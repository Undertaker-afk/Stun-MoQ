use stun_moq::{StunMoq, Keys, PublicKey};
use std::time::{Instant, Duration};
use tracing::Level;
use tracing_subscriber::EnvFilter;
use std::env;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    let args: Vec<String> = env::args().collect();
    let relays = vec!["wss://relay.damus.io".to_string()];

    if args.len() > 1 && args[1] == "receiver" {
        // Receiver: Echoes back frames
        let keys = Keys::generate();
        println!("BENCH_LATENCY_RECEIVER_PUBKEY: {}", keys.public_key());
        let stun = StunMoq::new(None, Some(keys), relays).await?;
        let mut conn_rx = stun.listen().await?;

        while let Some((peer_pk, _conn)) = conn_rx.recv().await {
            let mut transport = stun.stream_transport(peer_pk)?;
            tokio::spawn(async move {
                loop {
                    if let Ok(data) = transport.next_frame().await {
                        let _ = transport.send_frame(data).await;
                    } else {
                        break;
                    }
                }
            });
        }
    } else if args.len() > 2 && args[1] == "sender" {
        // Sender: Measures RTT
        let receiver_pubkey = PublicKey::from_hex(&args[2])?;
        let stun = StunMoq::new(None, None, relays).await?;
        let _conn = stun.connect(receiver_pubkey).await?;
        let mut transport = stun.stream_transport(receiver_pubkey)?;

        println!("Measuring latency (RTT) for 50 frames...");
        let mut latencies = Vec::new();

        for i in 0..50 {
            let data = format!("PING_{}", i).into_bytes();
            let start = Instant::now();
            transport.send_frame(data).await?;
            let _ = timeout(Duration::from_secs(5), transport.next_frame()).await??;
            let rtt = start.elapsed();
            latencies.push(rtt);
            if i % 10 == 0 { println!("  Frame {}/50: {:.2?}", i, rtt); }
        }

        let avg: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let min = latencies.iter().min().unwrap();
        let max = latencies.iter().max().unwrap();
        println!("Latency results: Avg: {:.2?}, Min: {:.2?}, Max: {:.2?}", avg, min, max);
    } else {
        println!("Usage:");
        println!("  Receiver: cargo run --example bench_latency receiver");
        println!("  Sender:   cargo run --example bench_latency sender <receiver_pubkey>");
    }

    Ok(())
}
