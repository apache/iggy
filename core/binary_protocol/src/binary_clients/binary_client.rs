use crate::binary_clients::client::Client;
use crate::BinaryTransport;
use async_trait::async_trait;

/// A client that can send and receive binary messages.
#[async_trait]
pub trait BinaryClient: BinaryTransport + Client {}
