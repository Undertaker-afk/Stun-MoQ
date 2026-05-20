use iroh::EndpointAddr;
use nostr_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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
}

impl NostrSignaling {
    pub async fn new(keys: Keys, relays: Vec<String>) -> Result<Self, anyhow::Error> {
        let client = Client::new(keys.clone());
        for relay in relays {
            client.add_relay(relay).await?;
        }
        client.connect().await;

        Ok(Self { client, keys })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn keys(&self) -> Keys {
        self.keys.clone()
    }

    pub async fn send_signal(&self, receiver_pubkey: PublicKey, message: SignalMessage) -> Result<(), anyhow::Error> {
        let content = serde_json::to_string(&message)?;
        let builder = EventBuilder::new(Kind::PrivateDirectMessage, content);
        self.client.gift_wrap(&receiver_pubkey, builder, None).await?;
        Ok(())
    }

    pub async fn listen_for_signals(&self) -> Result<mpsc::Receiver<(PublicKey, SignalMessage)>, anyhow::Error> {
        let (tx, rx) = mpsc::channel(100);
        let client = self.client.clone();

        tokio::spawn(async move {
            let mut notifications = client.notifications();
            while let Ok(notification) = notifications.recv().await {
                if let RelayPoolNotification::Event { event, .. } = notification {
                    if event.kind == Kind::GiftWrap {
                        if let Ok(unwrapped) = client.unwrap_gift_wrap(&event).await {
                            if unwrapped.rumor.kind == Kind::PrivateDirectMessage {
                                if let Ok(msg) = serde_json::from_str::<SignalMessage>(&unwrapped.rumor.content) {
                                    let _ = tx.send((unwrapped.sender, msg)).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}
