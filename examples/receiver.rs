use stun_moq::{StunMoq, Keys};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize rich logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    println!("🚀 Starting Stun-MoQ Receiver...");

    // Use a public relay
    let relays = vec!["wss://relay.damus.io".to_string()];

    // In a real app, you'd load your persistent keys.
    let keys = Keys::generate();
    println!("🔑 Our Nostr Public Key: {}", keys.public_key());
    println!("📢 Share this key with the sender!");

    let stun = StunMoq::new(None, Some(keys), relays).await?;

    println!("📡 Listening for incoming connections via Nostr signaling...");
    let mut conn_rx = stun.listen().await?;

    while let Some((peer_pubkey, _conn)) = conn_rx.recv().await {
        println!("🤝 Accepted connection from peer: {}", peer_pubkey);

        let mut transport = stun.stream_transport(peer_pubkey)?;

        tokio::spawn(async move {
            println!("📥 Waiting for data frames...");
            loop {
                match transport.next_frame().await {
                    Ok(data) => {
                        println!("✅ Received frame: {}", String::from_utf8_lossy(&data));
                    }
                    Err(e) => {
                        println!("❌ Connection closed or error: {}", e);
                        break;
                    }
                }
            }
        });
    }

    Ok(())
}
