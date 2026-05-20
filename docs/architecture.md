# Architecture Deep Dive

Stun-MoQ bridges the gap between decentralized identity and high-performance networking by layering three distinct protocols.

## Protocol Stack

1.  **Application Layer:** `StreamTransport` and `BlobTransport`
2.  **Encryption Layer:** ChaCha20-Poly1305 (AEAD)
3.  **Compression Layer:** Zstd
4.  **Multiplexing Layer:** 1-byte Custom Header (0x01/0x02)
5.  **Networking Layer:** QUIC (via Iroh/Quinn)
6.  **Signaling Layer:** Nostr (NIP-17/NIP-59)

## The Handshake Process

Stun-MoQ uses an "Out-of-Band" signaling approach to bypass the need for a central server or static IP addresses.

### Step 1: Peer Discovery
The Receiver starts by calling `listen()`. This initializes a `Client` and subscribes to `GiftWrap` events tagged with the receiver's public key.

### Step 2: The Handshake Signal
When the Sender calls `connect(receiver_pk)`, it:
- Generates its local Iroh `EndpointAddr` (contains NodeID, Relay URLs, and local IPs).
- Generates a random 32-byte `session_key`.
- Wraps these in a `SignalMessage::Handshake`.
- Sends this to the Receiver via Nostr.

### Step 3: Response & Key Establishment
The Receiver receives the signal, extracts the Sender's address and the session key. It then:
- Maps the Sender's Iroh NodeID to their Nostr Public Key.
- Stores the `session_key` for future E2EE operations.
- Sends its own `EndpointAddr` back to the Sender via Nostr.

### Step 4: QUIC Connection
Both peers now have each other's Iroh addresses. Iroh performs "Multipath Hole Punching":
- It sends UDP packets to all known local and public IPs of the peer.
- It concurrently connects to the best DERP relay.
- As soon as a UDP packet punches through the NAT, the connection migrates to a direct P2P path.

## High Performance Optimizations

### Asynchronous Concurrency
To maintain 60+ FPS in video streams, Stun-MoQ ensures the async runtime is never blocked. Compression and encryption are offloaded to a thread pool via `tokio::task::spawn_blocking`.

### Memory Management
The library uses `bytes::Bytes` for zero-copy data passing. Once a frame is encrypted/compressed in a blocking thread, the resulting immutable buffer is passed directly to the networking stack, minimizing allocations and copies.

### QUIC Parameter Tuning
We override the default QUIC settings to allow for "Fat Pipes":
- **Stream Receive Window:** 32MB (allows for large bursts of data without backpressure).
- **Connection Receive Window:** 128MB (aggregate across all streams).
- **Max Uni-Streams:** 2048 (supports high-frequency frame sending where each frame is its own stream).

## Secure Multiplexing
Since Iroh is a general-purpose networking tool, Stun-MoQ adds a 1-byte protocol discriminator.
- `0x01`: Real-time data. Handled by `StreamTransport`.
- `0x02`: Bulk data. Handled by `BlobTransport`.

This ensures that if you start a file transfer while streaming video, the receiver can correctly route the incoming bytes to the appropriate handler.
