use stun_moq::{StunMoq, Keys, PublicKey};
use std::io::{self, BufRead};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize rich logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    println!("🚀 Starting Stun-MoQ Sender...");

    let relays = vec!["wss://relay.damus.io".to_string()];
    let keys = Keys::generate();
    println!("🔑 Our Nostr Public Key: {}", keys.public_key());

    let stun = StunMoq::new(None, Some(keys), relays).await?;

    println!("👉 Enter the receiver's Nostr Public Key (hex):");
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let receiver_pubkey = PublicKey::from_hex(input.trim())?;

    println!("📡 Dialing receiver via Nostr signaling...");
    let _conn = stun.connect(receiver_pubkey).await?;
    println!("🤝 Connected via P2P (Iroh)!");

    let transport = stun.stream_transport(receiver_pubkey)?;

    println!("💬 Enter messages to send (one per line):");
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() { break; }

        println!("📤 Sending: {}", line);
        transport.send_frame(line.into_bytes()).await?;
    }

    Ok(())
}
