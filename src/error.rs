use thiserror::Error;
use nostr_sdk::prelude::PublicKey;
use iroh::endpoint::{ConnectionError, ConnectError};

#[derive(Debug, Error)]
pub enum StunMoqError {
    #[error("Iroh networking error: {0}")]
    Iroh(#[from] anyhow::Error),

    #[error("QUIC connection error: {0}")]
    Connection(#[from] ConnectionError),

    #[error("QUIC connect error: {0}")]
    Connect(#[from] ConnectError),

    #[error("Nostr signaling error: {0}")]
    Signaling(String),

    #[error("Handshake timeout with peer {0}")]
    HandshakeTimeout(PublicKey),

    #[error("Cryptography error: {0}")]
    Crypto(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StunMoqError>;
