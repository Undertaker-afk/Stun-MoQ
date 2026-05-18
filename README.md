# Stun-MoQ

Cross-platform Rust library primitives for low-latency NAT traversal and E2EE media/file streaming on Linux and Windows.

## Features

- Public ICE/STUN defaults based on free always-online style endpoints
- Stream presets for audio, high-FPS video, and generic file transfer
- End-to-end frame encryption/decryption using `ChaCha20-Poly1305`
- Simple configuration validation API for easy integration

## Quick start

```rust
use stun_moq::{E2eeChannel, SessionConfig, StreamProfile};

let config = SessionConfig::new("room-01", StreamProfile::video_high_fps());
config.validate().expect("valid config");

let crypto = E2eeChannel::random();
let encrypted = crypto.encrypt(b"frame-bytes").expect("encrypt");
let plain = crypto.decrypt(&encrypted).expect("decrypt");
assert_eq!(plain, b"frame-bytes");
```
