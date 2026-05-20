use crate::transport::dispatcher::{TransportDispatcher, MessageType};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Specialized transport for large files or binary blobs.
pub struct BlobTransport {
    dispatcher: Arc<TransportDispatcher>,
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl BlobTransport {
    pub fn new(dispatcher: Arc<TransportDispatcher>, receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self { dispatcher, receiver }
    }

    /// High-throughput blob sending.
    pub async fn send_blob(&self, data: Vec<u8>) -> Result<()> {
        // Level 3 balance
        self.dispatcher.send_raw(MessageType::Blob, data, 3).await
    }

    pub async fn receive_blob(&mut self) -> Result<Vec<u8>> {
        self.receiver.recv().await.ok_or_else(|| anyhow::anyhow!("Blob channel closed"))
    }
}
