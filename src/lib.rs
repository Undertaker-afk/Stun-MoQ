//! # Stun-MoQ
//!
//! A high-performance, P2P data transfer library for Rust, designed for real-time
//! streams (audio/video) and large file transfers.

use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Error as AeadError},
};
use rand::RngCore;
use thiserror::Error;
use std::sync::Arc;
use dashmap::DashMap;
use tracing::{info, debug, warn};

/// Re-exports from Iroh for easy access
pub use ::iroh::endpoint::Connection;
pub use ::iroh::SecretKey;
pub use ::iroh::EndpointAddr;

/// Re-exports from Nostr for identity management
pub use nostr_sdk::prelude::{Keys, PublicKey};

use anyhow::Result;

pub mod nostr;
pub mod iroh;
pub mod transport;

pub use crate::nostr::*;
pub use crate::iroh::node::*;
pub use crate::transport::stream::*;
pub use crate::transport::blob::*;

/// Defines the type of data being transferred to optimize transport parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StreamKind {
    /// Optimized for low-latency, small packets (e.g., VoIP)
    Audio,
    /// Optimized for high-throughput, sequential delivery (e.g., HD Video)
    Video,
    /// Optimized for reliable, bulk transfer (e.g., Zip files)
    File,
}

/// Configuration profile for a data stream.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamProfile {
    pub kind: StreamKind,
    pub target_latency_ms: u16,
    pub target_fps: u16,
    pub max_bitrate_kbps: u32,
}

impl StreamProfile {
    pub fn audio_low_latency() -> Self {
        Self {
            kind: StreamKind::Audio,
            target_latency_ms: 20,
            target_fps: 0,
            max_bitrate_kbps: 128,
        }
    }

    pub fn video_high_fps() -> Self {
        Self {
            kind: StreamKind::Video,
            target_latency_ms: 50,
            target_fps: 60,
            max_bitrate_kbps: 15_000,
        }
    }

    pub fn file_transfer() -> Self {
        Self {
            kind: StreamKind::File,
            target_latency_ms: 500,
            target_fps: 0,
            max_bitrate_kbps: 100_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EncryptedFrame {
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
}

#[derive(Clone)]
pub struct E2eeChannel {
    key: [u8; 32],
}

impl E2eeChannel {
    pub fn random() -> Self {
        let mut key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Self::from_key_bytes(key)
    }

    pub fn from_key_bytes(key: [u8; 32]) -> Self {
        Self { key }
    }

    pub fn key_bytes(&self) -> [u8; 32] {
        self.key
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> std::result::Result<EncryptedFrame, CryptoError> {
        let cipher = ChaCha20Poly1305::new(&self.key.into());
        let mut nonce = [0_u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(&nonce.into(), plaintext)
            .map_err(CryptoError::EncryptionFailed)?;

        Ok(EncryptedFrame { nonce, ciphertext })
    }

    pub fn decrypt(&self, frame: &EncryptedFrame) -> std::result::Result<Vec<u8>, CryptoError> {
        let cipher = ChaCha20Poly1305::new(&self.key.into());
        cipher
            .decrypt(&frame.nonce.into(), frame.ciphertext.as_ref())
            .map_err(CryptoError::DecryptionFailed)
    }
}

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("failed to encrypt frame")]
    EncryptionFailed(AeadError),
    #[error("failed to decrypt frame")]
    DecryptionFailed(AeadError),
}

pub struct StunMoq {
    iroh: IrohNetworking,
    nostr: NostrSignaling,
    peer_channels: Arc<DashMap<PublicKey, E2eeChannel>>,
    pending_conns: Arc<DashMap<::iroh::EndpointId, PublicKey>>,
}

impl StunMoq {
    pub async fn new(
        iroh_secret: Option<SecretKey>,
        nostr_keys: Option<Keys>,
        relays: Vec<String>,
    ) -> Result<Self> {
        info!("Creating new StunMoq instance...");
        let iroh_secret = iroh_secret.unwrap_or_else(SecretKey::generate);
        let nostr_keys = nostr_keys.unwrap_or_else(Keys::generate);

        let iroh = IrohNetworking::new(iroh_secret, vec![]).await?;
        let nostr = NostrSignaling::new(nostr_keys, relays).await?;

        Ok(Self {
            iroh,
            nostr,
            peer_channels: Arc::new(DashMap::new()),
            pending_conns: Arc::new(DashMap::new()),
        })
    }

    pub async fn listen(&self) -> Result<tokio::sync::mpsc::Receiver<(PublicKey, Connection)>> {
        info!("StunMoq started listening...");
        let mut signal_rx = self.nostr.listen_for_signals().await?;
        let my_addr = self.iroh.addr();
        let nostr = self.nostr.clone();
        let iroh_endpoint = self.iroh.endpoint().clone();
        let peer_channels = self.peer_channels.clone();
        let pending_conns = self.pending_conns.clone();

        let pending_conns_signal = pending_conns.clone();
        tokio::spawn(async move {
            while let Some((sender_pubkey, msg)) = signal_rx.recv().await {
                match msg {
                    SignalMessage::Handshake { node_addr, session_key } => {
                        debug!("Processing handshake signal from {}", sender_pubkey);
                        peer_channels.insert(sender_pubkey, E2eeChannel::from_key_bytes(session_key));
                        pending_conns_signal.insert(node_addr.id, sender_pubkey);

                        let _ = nostr.send_signal(sender_pubkey, SignalMessage::Handshake {
                            node_addr: my_addr.clone(),
                            session_key,
                        }).await;
                    }
                }
            }
        });

        let (conn_tx, conn_rx) = tokio::sync::mpsc::channel(10);

        let pending_conns_accept = pending_conns;
        tokio::spawn(async move {
            while let Some(incoming) = iroh_endpoint.accept().await {
                if let Ok(conn) = incoming.await {
                    let node_id = conn.remote_id();
                    info!("Accepted incoming Iroh connection from {}", node_id);

                    let pubkey = pending_conns_accept.remove(&node_id).map(|(_, v)| v)
                        .unwrap_or_else(|| {
                            warn!("Unknown peer connected: {}", node_id);
                            PublicKey::from_slice(&[0u8; 32]).unwrap()
                        });

                    let _ = conn_tx.send((pubkey, conn)).await;
                }
            }
        });

        Ok(conn_rx)
    }

    pub async fn connect(&self, peer_nostr_pubkey: PublicKey) -> Result<Connection> {
        info!("Connecting to peer {} via Nostr signaling...", peer_nostr_pubkey);
        let my_addr = self.iroh.addr();
        let mut session_key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut session_key);

        self.nostr.send_signal(peer_nostr_pubkey, SignalMessage::Handshake {
            node_addr: my_addr.clone(),
            session_key,
        }).await?;

        let mut signal_rx = self.nostr.listen_for_signals().await?;
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Ok(Some((sender, msg))) = tokio::time::timeout(std::time::Duration::from_secs(1), signal_rx.recv()).await {
                if sender == peer_nostr_pubkey {
                    match msg {
                        SignalMessage::Handshake { node_addr, session_key: _ } => {
                            info!("Handshake complete. Dialing Iroh node...");
                            self.peer_channels.insert(peer_nostr_pubkey, E2eeChannel::from_key_bytes(session_key));
                            let conn = self.iroh.endpoint().connect(node_addr, b"stun-moq/0.1").await?;
                            info!("Successfully connected to {}", peer_nostr_pubkey);
                            return Ok(conn);
                        }
                    }
                }
            }
        }

        warn!("Failed to connect to {} within timeout", peer_nostr_pubkey);
        Err(anyhow::anyhow!("Handshake timeout: Peer did not respond via Nostr within 30s"))
    }

    pub fn stream_transport(&self, peer_pubkey: PublicKey, conn: Connection) -> Result<StreamTransport> {
        let channel = self.peer_channels.get(&peer_pubkey)
            .ok_or_else(|| anyhow::anyhow!("No E2EE channel established for peer {}", peer_pubkey))?;
        Ok(StreamTransport::new(conn, E2eeChannel::from_key_bytes(channel.key_bytes())))
    }

    pub fn blob_transport(&self, peer_pubkey: PublicKey, conn: Connection) -> Result<BlobTransport> {
        let channel = self.peer_channels.get(&peer_pubkey)
            .ok_or_else(|| anyhow::anyhow!("No E2EE channel established for peer {}", peer_pubkey))?;
        Ok(BlobTransport::new(conn, E2eeChannel::from_key_bytes(channel.key_bytes())))
    }

    pub fn nostr_keys(&self) -> Keys {
        self.nostr.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_e2ee_encryption_roundtrip() {
        let channel = E2eeChannel::random();
        let original_data = b"Hello, secure world!";

        let encrypted = channel.encrypt(original_data).expect("Encryption failed");
        let decrypted = channel.decrypt(&encrypted).expect("Decryption failed");

        assert_eq!(original_data, decrypted.as_slice());
    }
}
