# Using Stun-MoQ

Stun-MoQ is designed to be simple to integrate while providing high-level abstractions for complex P2P operations.

## Basic Setup

Add Stun-MoQ to your `Cargo.toml`.

```rust
use stun_moq::{StunMoq, Keys, PublicKey};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Define Nostr relays for signaling
    let relays = vec!["wss://relay.damus.io".to_string()];

    // 2. Load or generate your identity
    let keys = Keys::generate();

    // 3. Initialize the library
    let stun = StunMoq::new(
        None,        // Optional Iroh SecretKey
        Some(keys),  // Optional Nostr Keys
        relays       // List of relays
    ).await?;

    Ok(())
}
```

## Receiving Data

The receiver listens for incoming signals on Nostr and established Iroh connections.

```rust
let mut conn_rx = stun.listen().await?;

while let Some((peer_pubkey, conn)) = conn_rx.recv().await {
    println!("Connection from: {}", peer_pubkey);

    // Create a transport for real-time data
    let transport = stun.stream_transport(peer_pubkey, conn)?;

    // Receive frames in a loop
    tokio::spawn(async move {
        while let Ok(frame) = transport.next_frame().await {
            println!("Received {} bytes", frame.len());
        }
    });
}
```

## Sending Data

To send data, you just need the receiver's Nostr Public Key.

```rust
let receiver_pk = PublicKey::from_hex("...")?;

// Dial the peer
let conn = stun.connect(receiver_pk).await?;

// Send a real-time frame
let transport = stun.stream_transport(receiver_pk, conn)?;
transport.send_frame(b"Hello P2P!".to_vec()).await?;
```

## Large File (Blob) Transfer

For transferring files or large buffers, use `BlobTransport`.

```rust
let transport = stun.blob_transport(peer_pk, conn)?;

// Sender
let large_file = std::fs::read("video.mp4")?;
transport.send_blob(large_file).await?;

// Receiver
let data = transport.receive_blob().await?;
std::fs::write("received_video.mp4", data)?;
```

## Performance Tips

1. **Multiplexing:** You can create both `StreamTransport` and `BlobTransport` from the same `Connection`. The library handles the routing automatically.
2. **Context Switching:** The library handles CPU-heavy tasks (compression/encryption) in a background thread pool, so your async loops won't lag.
3. **Datagrams:** For loss-tolerant data (like VoIP), use `transport.send_datagram()` for the absolute lowest possible latency.
