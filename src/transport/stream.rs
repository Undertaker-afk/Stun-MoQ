use crate::transport::dispatcher::{TransportDispatcher, MessageType};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use iroh::endpoint::Connection;

/// Optimized transport for real-time data streams.
pub struct StreamTransport {
    dispatcher: Arc<TransportDispatcher>,
    receiver: mpsc::Receiver<Vec<u8>>,
    datagram_receiver: mpsc::Receiver<Vec<u8>>,
}

impl StreamTransport {
    pub fn new(
        dispatcher: Arc<TransportDispatcher>,
        receiver: mpsc::Receiver<Vec<u8>>,
        datagram_receiver: mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self { dispatcher, receiver, datagram_receiver }
    }

    pub fn connection(&self) -> &Connection {
        self.dispatcher.connection()
    }

    /// Sends a frame via a reliable QUIC uni-directional stream.
    pub async fn send_frame(&self, data: Vec<u8>) -> Result<()> {
        // Level 1 for lowest latency
        self.dispatcher.send_raw(MessageType::Stream, data, 1).await
    }

    /// Receives the next reliable frame.
    pub async fn next_frame(&mut self) -> Result<Vec<u8>> {
        self.receiver.recv().await.ok_or_else(|| anyhow::anyhow!("Stream closed"))
    }

    /// Sends an unreliable datagram. Best for high-frequency, low-latency updates.
    pub async fn send_datagram(&self, data: Vec<u8>) -> Result<()> {
        self.dispatcher.send_raw(MessageType::Datagram, data, 1).await
    }

    /// Receives the next unreliable datagram.
    pub async fn next_datagram(&mut self) -> Result<Vec<u8>> {
        self.datagram_receiver.recv().await.ok_or_else(|| anyhow::anyhow!("Datagram channel closed"))
    }
}
