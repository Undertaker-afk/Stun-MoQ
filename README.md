# Stun-MoQ (stun_moq)

`stun_moq` is a production-ready Rust library for peer-to-peer (P2P) data transfer, designed for both real-time data streams (video/audio) and large blob transfers. It provides out-of-the-box NAT traversal and hole-punching using [Iroh](https://iroh.computer/), [Nostr](https://nostr.com/) for signaling, and Tailscale's public DERP infrastructure.

## Features

- **Zero-Config P2P**: No server setup required. Uses public Nostr relays for signaling and Tailscale's public DERP servers for NAT traversal.
- **Multiplexed Transport**: Supports high-performance streaming (UDP-like via QUIC streams) and reliable blob transfers on the same connection.
- **Secure by Default**: End-to-end encrypted (E2EE) using ChaCha20-Poly1305.
- **Optimized Performance**:
  - Transparent Zstd compression (Level 1 for streams, Level 3 for blobs).
  - CPU-bound tasks (crypto/compression) are offloaded to background threads.
  - Tuned QUIC parameters for high-throughput (32MB+ stream windows).
- **Decentralized Signaling**: Uses Nostr encrypted DMs (NIP-17/NIP-59) for private, decentralized handshakes.

## Quick Start

### Receiver

```rust
use stun_moq::{StunMoq, Keys};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let relays = vec!["wss://relay.damus.io".to_string()];
    let keys = Keys::generate(); // Or load your existing keys
    println!("Our Public Key: {}", keys.public_key());

    let stun = StunMoq::new(None, Some(keys), relays).await?;
    let mut conn_rx = stun.listen().await?;

    while let Some((peer_pk, _conn)) = conn_rx.recv().await {
        println!("Accepted connection from {}", peer_pk);
        let mut transport = stun.stream_transport(peer_pk)?;

        while let Ok(frame) = transport.next_frame().await {
            println!("Received {} bytes", frame.len());
        }
    }
    Ok(())
}
```

### Sender

```rust
use stun_moq::{StunMoq, PublicKey};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let relays = vec!["wss://relay.damus.io".to_string()];
    let receiver_pk = PublicKey::from_hex("...")?;

    let stun = StunMoq::new(None, None, relays).await?;
    stun.connect(receiver_pk).await?;

    let mut transport = stun.stream_transport(receiver_pk)?;
    transport.send_frame(b"Hello P2P!".to_vec()).await?;

    Ok(())
}
```

## Architecture

- **Signaling**: Uses Nostr NIP-17/59 to exchange Iroh Node IDs and session keys.
- **Networking**: Powered by `iroh-net`. It automatically fetches the latest Tailscale DERP map and uses public STUN servers.
- **Multiplexing**: A 1-byte header distinguishes between `Stream` and `Blob` traffic over QUIC uni-directional streams.

## Development

Run benchmarks to verify performance in your environment:

```bash
# Throttled environment test
cargo run --example bench_throughput receiver
cargo run --example bench_throughput sender <pubkey>
```

## License

MIT / Apache-2.0
