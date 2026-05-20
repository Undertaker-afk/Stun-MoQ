use iroh::endpoint::Connection;
use anyhow::Result;
use tokio::sync::mpsc;
use crate::{E2eeChannel, EncryptedFrame};
use tracing::{debug, warn, error};
use std::sync::Arc;
use tokio::task;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Stream = 0x01,
    Blob = 0x02,
    Datagram = 0x03,
}

pub struct TransportDispatcher {
    connection: Connection,
    stream_tx: mpsc::Sender<Vec<u8>>,
    blob_tx: mpsc::Sender<Vec<u8>>,
    datagram_tx: mpsc::Sender<Vec<u8>>,
    crypto: E2eeChannel,
}

impl TransportDispatcher {
    pub fn new(
        connection: Connection,
        crypto: E2eeChannel,
    ) -> (Arc<Self>, mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>, mpsc::Receiver<Vec<u8>>) {
        let (stream_tx, stream_rx) = mpsc::channel(1000);
        let (blob_tx, blob_rx) = mpsc::channel(100);
        let (datagram_tx, datagram_rx) = mpsc::channel(1000);

        let dispatcher = Arc::new(Self {
            connection,
            stream_tx,
            blob_tx,
            datagram_tx,
            crypto,
        });

        let d_clone = dispatcher.clone();
        tokio::spawn(async move {
            if let Err(e) = d_clone.run_loop().await {
                debug!("Transport dispatcher stream loop stopped: {}", e);
            }
        });

        let d_dg = dispatcher.clone();
        tokio::spawn(async move {
            if let Err(e) = d_dg.run_datagram_loop().await {
                debug!("Transport dispatcher datagram loop stopped: {}", e);
            }
        });

        (dispatcher, stream_rx, blob_rx, datagram_rx)
    }

    async fn run_loop(&self) -> Result<()> {
        let conn = self.connection.clone();

        loop {
            let mut recv_stream = conn.accept_uni().await?;
            let blob_dispatcher = self.blob_tx.clone();
            let stream_dispatcher = self.stream_tx.clone();
            let crypto = self.crypto.clone();

            tokio::spawn(async move {
                // Enforce strict maximum size: 100 MB for streams
                const MAX_STREAM_SIZE: usize = 100 * 1024 * 1024;
                match recv_stream.read_to_end(MAX_STREAM_SIZE).await {
                    Ok(payload) => {
                        if payload.is_empty() { return; }
                        let header = payload[0];

                        if payload.len() < 14 {
                            warn!("Received invalid payload: too short (minimum 14 bytes required)");
                            return;
                        }

                        let crypto_inner = crypto.clone();
                        let result = task::spawn_blocking(move || -> Result<(u8, Vec<u8>)> {
                            let mut nonce = [0u8; 12];
                            nonce.copy_from_slice(&payload[1..13]);
                            let ciphertext = &payload[13..];

                            let encrypted = EncryptedFrame { nonce, ciphertext: ciphertext.to_vec() };
                            let decrypted = crypto_inner.decrypt(&encrypted)?;
                            let decompressed = zstd::decode_all(&decrypted[..])?;
                            Ok((header, decompressed))
                        }).await;

                        match result {
                            Ok(Ok((0x01, data))) => {
                                let _ = stream_dispatcher.send(data).await;
                            }
                            Ok(Ok((0x02, data))) => {
                                let _ = blob_dispatcher.send(data).await;
                            }
                            Ok(Ok((h, _))) => {
                                warn!("Unknown message type: 0x{:02x}", h);
                            }
                            Ok(Err(e)) => {
                                error!("Failed to process incoming stream: {}", e);
                            }
                            Err(e) => {
                                error!("Task join error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Stream read error: {}", e);
                    }
                }
            });
        }
    }

    async fn run_datagram_loop(&self) -> Result<()> {
        let conn = self.connection.clone();
        loop {
            let dg = conn.read_datagram().await?;
            if dg.len() < 14 { continue; } // 1 byte header + 12 byte nonce + at least some ciphertext

            let header = dg[0];
            if header != 0x03 {
                warn!("Received non-datagram header in datagram: 0x{:02x}", header);
                continue;
            }

            let dg_bytes = dg.to_vec();
            let crypto = self.crypto.clone();
            let datagram_dispatcher = self.datagram_tx.clone();

            tokio::spawn(async move {
                let crypto_inner = crypto.clone();
                let result = task::spawn_blocking(move || -> Result<Vec<u8>> {
                    let mut nonce = [0u8; 12];
                    nonce.copy_from_slice(&dg_bytes[1..13]);
                    let ciphertext = &dg_bytes[13..];

                    let encrypted = EncryptedFrame { nonce, ciphertext: ciphertext.to_vec() };
                    let decrypted = crypto_inner.decrypt(&encrypted)?;
                    let decompressed = zstd::decode_all(&decrypted[..])?;
                    Ok(decompressed)
                }).await;

                match result {
                    Ok(Ok(data)) => {
                        let _ = datagram_dispatcher.send(data).await;
                    }
                    Ok(Err(e)) => {
                        warn!("Failed to decrypt/decompress datagram: {}", e);
                    }
                    Err(e) => {
                        error!("Datagram processing task failed: {}", e);
                    }
                }
            });
        }
    }

    pub async fn send_raw(&self, msg_type: MessageType, data: Vec<u8>, compression_level: i32) -> Result<()> {
        let crypto = self.crypto.clone();
        let payload = task::spawn_blocking(move || -> Result<Vec<u8>> {
            let compressed = zstd::encode_all(&data[..], compression_level)?;
            let encrypted = crypto.encrypt(&compressed)?;

            let mut packet = Vec::with_capacity(1 + 12 + encrypted.ciphertext.len());
            packet.push(msg_type as u8);
            packet.extend_from_slice(&encrypted.nonce);
            packet.extend_from_slice(&encrypted.ciphertext);
            Ok(packet)
        }).await??;

        if matches!(msg_type, MessageType::Datagram) {
            // Check against QUIC MTU before sending
            let max_size = self.connection.max_datagram_size();
            if payload.len() > max_size {
                return Err(anyhow::anyhow!(
                    "Datagram size {} exceeds max_datagram_size {}",
                    payload.len(),
                    max_size
                ).into());
            }
            self.connection.send_datagram(payload.into())?;
        } else {
            let mut send_stream = self.connection.open_uni().await?;
            send_stream.write_all(&payload).await?;
            send_stream.finish()?;
        }
        Ok(())
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }
}
