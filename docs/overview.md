# Stun-MoQ: High-Performance P2P Data Transfer

Stun-MoQ is a Rust library designed for high-bandwidth, low-latency peer-to-peer (P2P) data transfer. It combines the decentralized signaling capabilities of **Nostr** with the robust NAT traversal and networking of **Iroh**.

## Features

- **Zero-Configuration:** Start transferring data instantly using public Nostr relays and Tailscale/Iroh DERP servers.
- **Robust NAT Traversal:** Built on Iroh, utilizing UDP hole-punching and global relay fallbacks (DERP).
- **Secure by Default:** End-to-end encryption (E2EE) using ChaCha20-Poly1305 with session keys exchanged over Nostr.
- **Optimized for Real-Time:** Built-in Zstd compression for low-latency live streams (audio/video).
- **High Throughput:** Tuned QUIC parameters and parallelized crypto/compression tasks.
- **Decentralized Discovery:** Find and connect to peers using only their Nostr Public Key.

## Architecture

### 1. Signaling Layer (Nostr)
Peers use Nostr to "find" each other. When you call `connect(peer_pubkey)`, the library:
1. Generates an ephemeral session key for E2EE.
2. Encapsulates its Iroh `EndpointAddr` and the session key into a `SignalMessage`.
3. Sends this message as a NIP-59 "Gift Wrapped" Private Direct Message to the peer.
4. The receiver responds with its own address, completing the handshake.

### 2. Networking Layer (Iroh)
Once addresses are exchanged, Iroh takes over:
- **Hole Punching:** Attempts to establish a direct UDP path between peers.
- **Relaying:** If direct connection fails (symmetric NATs), it automatically routes traffic through the lowest-latency DERP relay.
- **Tailscale Integration:** Dynamically fetches the latest DERP map from Tailscale to provide 90+ fallback nodes worldwide.

### 3. Transport Layer
- **StreamTransport:** Uses QUIC unidirectional streams for independent data frames. Each frame is compressed with Zstd (Level 1) and encrypted. This avoids Head-of-Line (HOL) blocking.
- **BlobTransport:** Optimized for large files, using larger receive windows and Level 3 compression.
- **Multiplexing:** A 1-byte header (0x01 for streams, 0x02 for blobs) allows multiple transport types to share the same connection safely.

## Technical Specifications

- **Encryption:** ChaCha20-Poly1305 (AEAD)
- **Compression:** Zstd
- **Protocol:** QUIC (via Iroh/Quinn)
- **Signaling:** Nostr (NIP-17, NIP-59)
- **Version Compatibility:** Iroh v1.0.0-rc.0
