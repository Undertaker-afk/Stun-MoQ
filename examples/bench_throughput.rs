use stun_moq::{StunMoq, Keys, PublicKey};
use std::time::Instant;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use std::env;
use rand::RngCore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    let args: Vec<String> = env::args().collect();
    let relays = vec!["wss://relay.damus.io".to_string()];

    if args.len() > 1 && args[1] == "receiver" {
        // Receiver mode
        let keys = Keys::generate();
        println!("BENCH_THROUGHPUT_RECEIVER_PUBKEY: {}", keys.public_key());
        let stun = StunMoq::new(None, Some(keys), relays).await?;
        let mut conn_rx = stun.listen().await?;

        if let Some((peer_pk, _conn)) = conn_rx.recv().await {
            let mut transport = stun.blob_transport(peer_pk)?;
            println!("Receiver ready, waiting for blob...");
            let start = Instant::now();
            let data = transport.receive_blob().await?;
            let duration = start.elapsed();

            let mb = data.len() as f64 / 1024.0 / 1024.0;
            let mbs = mb / duration.as_secs_f64();
            println!("Received {:.2} MB in {:.2?} ({:.2} MB/s)", mb, duration, mbs);

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    } else if args.len() > 2 && args[1] == "sender" {
        // Sender mode
        let receiver_pubkey = PublicKey::from_hex(&args[2])?;
        let stun = StunMoq::new(None, None, relays).await?;
        let _conn = stun.connect(receiver_pubkey).await?;
        let transport = stun.blob_transport(receiver_pubkey)?;

        let size_mb = 10;
        let mut data = vec![0u8; size_mb * 1024 * 1024];
        rand::thread_rng().fill_bytes(&mut data);
        println!("Sending {} MB random blob to {}...", size_mb, receiver_pubkey);

        let start = Instant::now();
        transport.send_blob(data).await?;
        let duration = start.elapsed();

        println!("Sent {} MB in {:.2?}", size_mb, duration);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    } else {
        println!("Usage:");
        println!("  Receiver: cargo run --example bench_throughput receiver");
        println!("  Sender:   cargo run --example bench_throughput sender <receiver_pubkey>");
    }

    Ok(())
}
