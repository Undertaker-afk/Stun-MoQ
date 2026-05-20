use iroh::endpoint::Connection;
use anyhow::Result;
use crate::E2eeChannel;
use tracing::{info, debug};

/// Specialized transport for large files or binary blobs.
pub struct BlobTransport {
    connection: Connection,
    crypto: E2eeChannel,
}

impl BlobTransport {
    /// Creates a new `BlobTransport` from an existing connection.
    pub fn new(connection: Connection, crypto: E2eeChannel) -> Self {
        Self { connection, crypto }
    }

    /// Sends a blob of data efficiently.
    pub async fn send_blob(&self, data: &[u8]) -> Result<()> {
        info!("Sending blob (len: {} bytes)...", data.len());

        let compressed = zstd::encode_all(data, 3)?;
        debug!("Compressed blob ({} -> {} bytes)", data.len(), compressed.len());

        let encrypted = self.crypto.encrypt(&compressed)?;

        let mut send_stream = self.connection.open_uni().await?;
        send_stream.write_all(&encrypted.nonce).await?;
        send_stream.write_all(&encrypted.ciphertext).await?;
        send_stream.finish()?;

        info!("Blob sent successfully");
        Ok(())
    }

    /// Receives a blob.
    pub async fn receive_blob(&self) -> Result<Vec<u8>> {
        debug!("Waiting for incoming blob...");
        let mut recv_stream = self.connection.accept_uni().await?;

        // 1GB limit for blobs
        let payload = recv_stream.read_to_end(1024 * 1024 * 1024).await?;

        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Received invalid blob: too short"));
        }

        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&payload[..12]);
        let ciphertext = payload[12..].to_vec();

        let encrypted = crate::EncryptedFrame { nonce, ciphertext };
        let decrypted = self.crypto.decrypt(&encrypted)?;
        let decompressed = zstd::decode_all(&decrypted[..])?;

        info!("Received blob successfully (len: {} bytes)", decompressed.len());
        Ok(decompressed)
    }
}
