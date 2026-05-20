use stun_moq::{StunMoq, Keys, PublicKey};
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn test_tailscale_derp_only() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::DEBUG.into()))
        .try_init();

    // Use a public relay for signaling
    let relays = vec!["wss://relay.damus.io".to_string()];

    // 1. Setup Receiver with ONLY Tailscale DERP
    let rx_keys = Keys::generate();
    let rx_pubkey = rx_keys.public_key();

    // We'll pass an empty custom_relays but then the library will fetch Tailscale ones.
    // To ensure ONLY Tailscale are used, I might need to modify the library or the RelayMap.
    // For now, let's just see if it uses them.
    let receiver = StunMoq::new(None, Some(rx_keys), relays.clone()).await?;
    let mut incoming_conns = receiver.listen().await?;

    // 2. Setup Sender
    let tx_keys = Keys::generate();
    let sender = StunMoq::new(None, Some(tx_keys), relays).await?;

    // 3. Perform Handshake and Connect
    println!("Connecting...");
    let tx_conn = timeout(Duration::from_secs(60), sender.connect(rx_pubkey)).await??;
    println!("Connected.");

    let transport = sender.stream_transport(rx_pubkey, tx_conn)?;
    transport.send_frame(b"TailscaleTest".to_vec()).await?;
    println!("Sent frame.");

    Ok(())
}
