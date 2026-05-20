use stun_moq::{StunMoq, Keys};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_p2p_connection_via_tailscale_derp() -> anyhow::Result<()> {
    // Check if network tests are enabled
    if std::env::var("RUN_NETWORK_TESTS").unwrap_or_default() != "true" {
        println!("Skipping network test (set RUN_NETWORK_TESTS=true to enable)");
        return Ok(());
    }

    // Get relay from environment
    let relay_url = std::env::var("TEST_TAILSCALE_RELAY")
        .map_err(|_| anyhow::anyhow!("TEST_TAILSCALE_RELAY environment variable not set"))?;
    println!("Using relay: {}", relay_url);

    // 1. Setup two peers with distinct keys
    let relays = vec![relay_url];

    let rx_keys = Keys::generate();
    let rx_pubkey = rx_keys.public_key();
    let receiver = StunMoq::new(None, Some(rx_keys), relays.clone()).await?;

    let tx_keys = Keys::generate();
    let sender = StunMoq::new(None, Some(tx_keys), relays.clone()).await?;

    // 2. Receiver starts listening
    let mut incoming_conns = receiver.listen().await?;

    // 3. Sender connects to receiver
    println!("Connecting from sender to receiver...");
    let _tx_conn = timeout(Duration::from_secs(60), sender.connect(rx_pubkey)).await??;
    println!("Connected.");

    // 4. Test Stream (Reliable)
    let mut tx_transport = sender.stream_transport(rx_pubkey)?;
    tx_transport.send_frame(b"ReliableStream".to_vec()).await?;
    println!("Sent reliable frame.");

    // Accepted on receiver side
    let (peer_pk, _rx_conn) = timeout(Duration::from_secs(10), incoming_conns.recv()).await?.unwrap();
    let mut rx_transport = receiver.stream_transport(peer_pk)?;

    let frame = timeout(Duration::from_secs(10), rx_transport.next_frame()).await??;
    assert_eq!(frame, b"ReliableStream");
    println!("Received reliable frame.");

    // 5. Test Datagram (Unreliable)
    tx_transport.send_datagram(b"UnreliableDatagram".to_vec()).await?;
    println!("Sent datagram.");

    let dg = timeout(Duration::from_secs(10), rx_transport.next_datagram()).await??;
    assert_eq!(dg, b"UnreliableDatagram");
    println!("Received datagram.");

    Ok(())
}
