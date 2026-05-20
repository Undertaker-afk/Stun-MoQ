use stun_moq::{StunMoq, Keys, PublicKey};
use std::io::{self, BufRead};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let relays = vec!["wss://relay.damus.io".to_string()];
    let keys = Keys::generate();
    println!("Our Nostr Public Key: {}", keys.public_key());

    let stun = StunMoq::new(None, Some(keys), relays).await?;

    println!("Enter the receiver's Nostr Public Key (hex):");
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let receiver_pubkey = PublicKey::from_hex(input.trim())?;

    println!("Connecting to {}...", receiver_pubkey);
    let conn = stun.connect(receiver_pubkey).await?;
    println!("Connected!");

    let transport = stun.stream_transport(receiver_pubkey, conn)?;

    println!("Enter messages to send (one per line):");
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() { break; }

        println!("Sending: {}", line);
        transport.send_frame(line.as_bytes()).await?;
    }

    Ok(())
}
