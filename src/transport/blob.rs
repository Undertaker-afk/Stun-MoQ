use iroh::endpoint::Connection;
use anyhow::Result;
use crate::E2eeChannel;
use bytes::{Bytes, BytesMut, BufMut};
use tokio::task;

/// Specialized transport for large files or binary blobs.
pub struct BlobTransport {
    connection: Connection,
    crypto: E2eeChannel,
}

impl BlobTransport {
    pub fn new(connection: Connection, crypto: E2eeChannel) -> Self {
        Self { connection, crypto }
    }

    /// High-throughput blob sending.
    pub async fn send_blob(&self, data: Vec<u8>) -> Result<()> {
        let crypto = self.crypto.clone();

        let payload = task::spawn_blocking(move || -> Result<Bytes> {
            // Level 3 is a good balance for large files
            let compressed = zstd::encode_all(&data[..], 3)?;
            let encrypted = crypto.encrypt(&compressed)?;

            let mut packet = BytesMut::with_capacity(12 + encrypted.ciphertext.len());
            packet.put_slice(&encrypted.nonce);
            packet.put_slice(&encrypted.ciphertext);

            Ok(packet.freeze())
        }).await??;

        let mut send_stream = self.connection.open_uni().await?;
        send_stream.write_all(&payload).await?;
        send_stream.finish()?;

        Ok(())
    }

    pub async fn receive_blob(&self) -> Result<Vec<u8>> {
        let mut recv_stream = self.connection.accept_uni().await?;

        // 1GB limit for blobs
        let payload = recv_stream.read_to_end(1024 * 1024 * 1024).await?;

        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Received invalid blob: too short"));
        }

        let crypto = self.crypto.clone();
        let decompressed = task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut nonce = [0u8; 12];
            nonce.copy_from_slice(&payload[..12]);
            let ciphertext = &payload[12..];

            let encrypted = crate::EncryptedFrame { nonce, ciphertext: ciphertext.to_vec() };
            let decrypted = crypto.decrypt(&encrypted)?;
            let decompressed = zstd::decode_all(&decrypted[..])?;

            Ok(decompressed)
        }).await??;

        Ok(decompressed)
    }
}
