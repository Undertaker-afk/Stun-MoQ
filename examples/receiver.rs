use stun_moq::{StunMoq, Keys};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Use a public relay
    let relays = vec!["wss://relay.damus.io".to_string()];

    // In a real app, you'd load your persistent keys.
    let keys = Keys::generate();
    println!("Our Nostr Public Key: {}", keys.public_key());

    let stun = StunMoq::new(None, Some(keys), relays).await?;

    println!("Listening for incoming connections...");
    let mut conn_rx = stun.listen().await?;

    while let Some((peer_pubkey, conn)) = conn_rx.recv().await {
        println!("Accepted connection from peer!");
        let transport = stun.stream_transport(peer_pubkey, conn)?;

        tokio::spawn(async move {
            loop {
                match transport.next_frame().await {
                    Ok(data) => {
                        println!("Received frame: {}", String::from_utf8_lossy(&data));
                    }
                    Err(e) => {
                        println!("Connection closed or error: {}", e);
                        break;
                    }
                }
            }
        });
    }

    Ok(())
}
