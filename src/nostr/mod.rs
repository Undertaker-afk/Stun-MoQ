use iroh::EndpointAddr;
use nostr_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, debug, warn, error};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalMessage {
    Handshake {
        node_addr: EndpointAddr,
        session_key: [u8; 32],
    },
}

#[derive(Clone)]
pub struct NostrSignaling {
    client: Client,
    keys: Keys,
    running: Arc<AtomicBool>,
}

impl NostrSignaling {
    pub async fn new(keys: Keys, relays: Vec<String>) -> Result<Self, anyhow::Error> {
        info!("Initializing Nostr signaling with {} relays...", relays.len());
        let client = Client::new(keys.clone());

        for relay in relays {
            if let Err(e) = client.add_relay(relay.clone()).await {
                warn!("Failed to add Nostr relay {}: {}", relay, e);
            }
        }

        client.connect().await;

        // Wait for at least one relay to connect
        let mut connected = false;
        for _ in 0..10 {
            let relays = client.relays().await;
            if relays.values().any(|r| r.is_connected()) {
                connected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        if !connected {
            warn!("No Nostr relays connected yet, signaling may be delayed.");
        } else {
            info!("Nostr signaling connected. Identity: {}", keys.public_key());
        }

        Ok(Self {
            client,
            keys,
            running: Arc::new(AtomicBool::new(true)),
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn keys(&self) -> Keys {
        self.keys.clone()
    }

    pub async fn send_signal(&self, receiver_pubkey: PublicKey, message: SignalMessage) -> Result<(), anyhow::Error> {
        debug!("Sending signal to {}: {:?}", receiver_pubkey, message);
        let content = serde_json::to_string(&message)?;
        let builder = EventBuilder::new(Kind::PrivateDirectMessage, content);

        // Send with retry or to multiple relays is handled by nostr-sdk internally
        self.client.gift_wrap(&receiver_pubkey, builder, None).await?;
        Ok(())
    }

    pub async fn listen_for_signals(&self) -> Result<mpsc::Receiver<(PublicKey, SignalMessage)>, anyhow::Error> {
        debug!("Starting Nostr signal listener...");
        let (tx, rx) = mpsc::channel(100);
        let client = self.client.clone();
        let running = self.running.clone();
        let my_pubkey = self.keys.public_key();

        // Use a filter to subscribe only to GiftWrap events
        let filter = Filter::new().kind(Kind::GiftWrap).pubkey(my_pubkey);
        if let Err(e) = client.subscribe(vec![filter], None).await {
            error!("Failed to subscribe to Nostr events: {}", e);
            return Err(e.into());
        }

        tokio::spawn(async move {
            let mut notifications = client.notifications();
            while running.load(Ordering::SeqCst) {
                tokio::select! {
                    notification = notifications.recv() => {
                        match notification {
                            Ok(RelayPoolNotification::Event { event, .. }) => {
                                if event.kind == Kind::GiftWrap {
                                    if let Ok(unwrapped) = client.unwrap_gift_wrap(&event).await {
                                        if unwrapped.rumor.kind == Kind::PrivateDirectMessage {
                                            if let Ok(msg) = serde_json::from_str::<SignalMessage>(&unwrapped.rumor.content) {
                                                debug!("Received signal from {}: {:?}", unwrapped.sender, msg);
                                                if tx.send((unwrapped.sender, msg)).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            Ok(RelayPoolNotification::Shutdown) => break,
                            Err(_) => break,
                            _ => {}
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        // Periodic check for 'running' flag if recv is blocked
                    }
                }
            }
            debug!("Nostr signal listener stopped.");
        });

        Ok(rx)
    }
}

impl Drop for NostrSignaling {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}
