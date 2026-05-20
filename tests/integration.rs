use stun_moq::{StunMoq, Keys};
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn test_integration_live_and_blob() -> anyhow::Result<()> {
    // Initialize logging for the test
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::DEBUG.into()))
        .try_init();

    let relays = vec!["wss://relay.damus.io".to_string()];

    // 1. Setup Receiver
    let rx_keys = Keys::generate();
    let rx_pubkey = rx_keys.public_key();
    let receiver = StunMoq::new(None, Some(rx_keys), relays.clone()).await?;
    let mut incoming_conns = receiver.listen().await?;

    // 2. Setup Sender
    let tx_keys = Keys::generate();
    let tx_pubkey = tx_keys.public_key();
    let sender = StunMoq::new(None, Some(tx_keys), relays).await?;

    // 3. Perform Handshake and Connect (Sender -> Receiver)
    println!("Connecting sender to receiver {}...", rx_pubkey);
    let tx_conn = timeout(Duration::from_secs(60), sender.connect(rx_pubkey)).await??;
    println!("Sender connected.");

    // 4. Accept connection on Receiver side
    let (peer_pk, rx_conn) = timeout(Duration::from_secs(30), incoming_conns.recv()).await?
        .ok_or_else(|| anyhow::anyhow!("No incoming connection"))?;
    println!("Receiver accepted connection from {}.", peer_pk);
    assert_eq!(peer_pk, tx_pubkey);

    // --- TEST A: LIVE DATA STREAM ---
    let tx_stream = sender.stream_transport(rx_pubkey, tx_conn)?;
    let rx_stream = receiver.stream_transport(tx_pubkey, rx_conn.clone())?;

    let test_data = b"LIVE_FRAME_DATA";
    for i in 0..5 {
        let frame = format!("{}_{}", String::from_utf8_lossy(test_data), i);
        tx_stream.send_frame(frame.as_bytes()).await?;

        let received = timeout(Duration::from_secs(10), rx_stream.next_frame()).await??;
        assert_eq!(received, frame.as_bytes());
        println!("Verified live frame {}", i);
    }

    // --- TEST B: BLOB TRANSFER ---
    let tx_blob = sender.blob_transport(rx_pubkey, tx_stream.connection().clone())?; // Reuse connection
    let rx_blob = receiver.blob_transport(tx_pubkey, rx_conn)?;

    let large_data = vec![0xAF; 1024 * 1024]; // 1MB blob
    tx_blob.send_blob(&large_data).await?;

    let received_blob = timeout(Duration::from_secs(30), rx_blob.receive_blob()).await??;
    assert_eq!(received_blob.len(), large_data.len());
    assert_eq!(received_blob, large_data);
    println!("Verified 1MB blob transfer integrity.");

    Ok(())
}
