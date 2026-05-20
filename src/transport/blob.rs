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

            // 3. Assemble packet: Header (1 byte) + Nonce (12 bytes) + Ciphertext
            // Header 0x02 = Blob
            let mut packet = BytesMut::with_capacity(1 + 12 + encrypted.ciphertext.len());
            packet.put_u8(0x02);
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

        if payload.len() < 13 {
            return Err(anyhow::anyhow!("Received invalid blob: too short"));
        }

        if payload[0] != 0x02 {
            return Err(anyhow::anyhow!("Received unexpected stream type: expected blob (0x02), got 0x{:02x}", payload[0]));
        }

        let crypto = self.crypto.clone();
        let decompressed = task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut nonce = [0u8; 12];
            nonce.copy_from_slice(&payload[1..13]);
            let ciphertext = &payload[13..];

            let encrypted = crate::EncryptedFrame { nonce, ciphertext: ciphertext.to_vec() };
            let decrypted = crypto.decrypt(&encrypted)?;
            let decompressed = zstd::decode_all(&decrypted[..])?;

            Ok(decompressed)
        }).await??;

        Ok(decompressed)
    }
}
