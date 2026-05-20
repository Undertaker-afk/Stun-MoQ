use iroh::endpoint::Connection;
use anyhow::Result;
use crate::E2eeChannel;

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
        // Apply compression (useful for text, logs, etc.)
        let compressed = zstd::encode_all(data, 3)?;
        let encrypted = self.crypto.encrypt(&compressed)?;

        let mut send_stream = self.connection.open_uni().await?;

        // Protocol: Nonce (12 bytes) + Ciphertext
        send_stream.write_all(&encrypted.nonce).await?;
        send_stream.write_all(&encrypted.ciphertext).await?;
        send_stream.finish()?;

        Ok(())
    }

    /// Receives a blob.
    pub async fn receive_blob(&self) -> Result<Vec<u8>> {
        let mut recv_stream = self.connection.accept_uni().await?;

        // Read to end with a 1GB limit for blobs
        let payload = recv_stream.read_to_end(1024 * 1024 * 1024).await?;

        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Received invalid blob: too short"));
        }

        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&payload[..12]);
        let ciphertext = payload[12..].to_vec();

        let encrypted = crate::EncryptedFrame { nonce, ciphertext };
        let decrypted = self.crypto.decrypt(&encrypted)?;

        // Decompress to original size
        let decompressed = zstd::decode_all(&decrypted[..])?;

        Ok(decompressed)
    }
}
