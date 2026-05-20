use iroh::endpoint::Connection;
use anyhow::Result;
use zstd;
use bytes::Bytes;
use crate::E2eeChannel;
use tracing::{debug, trace};

/// Optimized transport for real-time data streams.
pub struct StreamTransport {
    connection: Connection,
    crypto: E2eeChannel,
}

impl StreamTransport {
    /// Creates a new `StreamTransport` from an existing connection.
    pub fn new(connection: Connection, crypto: E2eeChannel) -> Self {
        Self { connection, crypto }
    }

    /// Returns the underlying Iroh connection.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Sends a single frame of data over a new unidirectional stream.
    /// This prevents Head-of-Line (HOL) blocking between independent frames.
    pub async fn send_frame(&self, data: &[u8]) -> Result<()> {
        trace!("Sending frame (len: {} bytes)...", data.len());

        // 1. Compress
        let compressed = zstd::encode_all(data, 3)?;
        trace!("Compressed frame ({} -> {} bytes)", data.len(), compressed.len());

        // 2. Encrypt
        let encrypted = self.crypto.encrypt(&compressed)?;

        // 3. Send over a fresh uni stream
        let mut send_stream = self.connection.open_uni().await?;

        // Protocol: Nonce (12 bytes) + Ciphertext
        send_stream.write_all(&encrypted.nonce).await?;
        send_stream.write_all(&encrypted.ciphertext).await?;
        send_stream.finish()?;

        debug!("Sent frame successfully");
        Ok(())
    }

    /// Waits for and receives the next incoming frame.
    pub async fn next_frame(&self) -> Result<Vec<u8>> {
        trace!("Waiting for next frame...");
        let mut recv_stream = self.connection.accept_uni().await?;

        // Read to end with a 10MB limit per frame
        let payload = recv_stream.read_to_end(10 * 1024 * 1024).await?;

        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Received invalid frame: too short"));
        }

        // 1. Decrypt
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&payload[..12]);
        let ciphertext = payload[12..].to_vec();

        let encrypted = crate::EncryptedFrame { nonce, ciphertext };
        let decrypted = self.crypto.decrypt(&encrypted)?;

        // 2. Decompress
        let decompressed = zstd::decode_all(&decrypted[..])?;
        trace!("Received and decompressed frame (len: {} bytes)", decompressed.len());

        Ok(decompressed)
    }

    /// Sends data via QUIC datagrams for lowest possible latency.
    /// Suitable for loss-tolerant data (e.g., non-reference video frames).
    pub async fn send_datagram(&self, data: &[u8]) -> Result<()> {
        trace!("Sending datagram (len: {} bytes)...", data.len());
        let compressed = zstd::encode_all(data, 3)?;
        let encrypted = self.crypto.encrypt(&compressed)?;

        let mut payload = Vec::with_capacity(12 + encrypted.ciphertext.len());
        payload.extend_from_slice(&encrypted.nonce);
        payload.extend_from_slice(&encrypted.ciphertext);

        // Note: Datagrams have a maximum size limited by MTU
        self.connection.send_datagram(Bytes::from(payload))?;
        trace!("Sent datagram successfully");
        Ok(())
    }

    /// Receives the next available datagram.
    pub async fn next_datagram(&self) -> Result<Vec<u8>> {
        let datagram = self.connection.read_datagram().await?;
        trace!("Received datagram (len: {} bytes)", datagram.len());

        if datagram.len() < 12 {
            return Err(anyhow::anyhow!("Received invalid datagram: too short"));
        }

        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&datagram[..12]);
        let ciphertext = datagram[12..].to_vec();

        let encrypted = crate::EncryptedFrame { nonce, ciphertext };
        let decrypted = self.crypto.decrypt(&encrypted)?;
        let decompressed = zstd::decode_all(&decrypted[..])?;

        Ok(decompressed)
    }
}
