use iroh::endpoint::Connection;
use anyhow::Result;
use zstd;
use bytes::{Bytes, BytesMut, BufMut};
use crate::E2eeChannel;
use tokio::task;

/// Optimized transport for real-time data streams.
pub struct StreamTransport {
    connection: Connection,
    crypto: E2eeChannel,
}

impl StreamTransport {
    pub fn new(connection: Connection, crypto: E2eeChannel) -> Self {
        Self { connection, crypto }
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Optimized frame sending: offloads CPU-intensive tasks to blocking threads
    /// and uses Bytes for efficient memory management.
    pub async fn send_frame(&self, data: Vec<u8>) -> Result<()> {
        let crypto = self.crypto.clone();

        // Move CPU intensive work to a blocking thread
        let payload = task::spawn_blocking(move || -> Result<Bytes> {
            // 1. Compress (Level 1 for lowest latency)
            let compressed = zstd::encode_all(&data[..], 1)?;

            // 2. Encrypt
            let encrypted = crypto.encrypt(&compressed)?;

            // 3. Assemble packet: Header (1 byte) + Nonce (12 bytes) + Ciphertext
            // Header 0x01 = Stream Frame
            let mut packet = BytesMut::with_capacity(1 + 12 + encrypted.ciphertext.len());
            packet.put_u8(0x01);
            packet.put_slice(&encrypted.nonce);
            packet.put_slice(&encrypted.ciphertext);

            Ok(packet.freeze())
        }).await??;

        // 4. Send over a fresh uni stream (prevents HOL blocking)
        let mut send_stream = self.connection.open_uni().await?;
        send_stream.write_all(&payload).await?;
        send_stream.finish()?;

        Ok(())
    }

    pub async fn next_frame(&self) -> Result<Vec<u8>> {
        // In a real multiplexed system, we'd have a central dispatcher.
        // For this optimized prototype, we'll read the next uni stream and verify the header.
        let mut recv_stream = self.connection.accept_uni().await?;
        let payload = recv_stream.read_to_end(10 * 1024 * 1024).await?;

        if payload.len() < 13 {
            return Err(anyhow::anyhow!("Received invalid frame: too short"));
        }

        if payload[0] != 0x01 {
            return Err(anyhow::anyhow!("Received unexpected stream type: expected frame (0x01), got 0x{:02x}", payload[0]));
        }

        let crypto = self.crypto.clone();

        // Move decryption and decompression to blocking thread
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

    /// Lowest possible latency using QUIC datagrams.
    pub async fn send_datagram(&self, data: Vec<u8>) -> Result<()> {
        let crypto = self.crypto.clone();

        let payload = task::spawn_blocking(move || -> Result<Bytes> {
            let compressed = zstd::encode_all(&data[..], 1)?;
            let encrypted = crypto.encrypt(&compressed)?;

            let mut packet = BytesMut::with_capacity(1 + 12 + encrypted.ciphertext.len());
            packet.put_u8(0x01); // Datagrams also use 0x01 for data
            packet.put_slice(&encrypted.nonce);
            packet.put_slice(&encrypted.ciphertext);

            Ok(packet.freeze())
        }).await??;

        self.connection.send_datagram(payload)?;
        Ok(())
    }

    pub async fn next_datagram(&self) -> Result<Vec<u8>> {
        let datagram = self.connection.read_datagram().await?;

        if datagram.len() < 13 {
            return Err(anyhow::anyhow!("Received invalid datagram: too short"));
        }

        if datagram[0] != 0x01 {
            return Err(anyhow::anyhow!("Received unexpected datagram type"));
        }

        let crypto = self.crypto.clone();
        let decompressed = task::spawn_blocking(move || -> Result<Vec<u8>> {
            let mut nonce = [0u8; 12];
            nonce.copy_from_slice(&datagram[1..13]);
            let ciphertext = &datagram[13..];

            let encrypted = crate::EncryptedFrame { nonce, ciphertext: ciphertext.to_vec() };
            let decrypted = crypto.decrypt(&encrypted)?;
            let decompressed = zstd::decode_all(&decrypted[..])?;
            Ok(decompressed)
        }).await??;

        Ok(decompressed)
    }
}
