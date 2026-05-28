# Sink plugin skeleton

Boilerplate for a new `core/connectors/sinks/<name>_sink/`. Adapt the
`MySink` / `Client` types to the backend driver you're integrating.

## Cargo.toml

```toml
# Apache 2.0 header (copy verbatim from any existing sink Cargo.toml)
[package]
name = "iggy_connector_<name>_sink"
version = "0.4.1-edge.1"   # match the version most other sinks use
edition = "2024"
license = "Apache-2.0"
publish = false
# ...keywords, description, repository, homepage all identical to existing sinks

[package.metadata.cargo-machete]
ignored = ["dashmap", "once_cell"]   # used by sink_connector! macro

[lib]
crate-type = ["cdylib", "lib"]       # cdylib = runtime-loadable; lib = unit tests

[dependencies]
async-trait        = { workspace = true }
dashmap            = { workspace = true }
iggy_connector_sdk = { workspace = true }
once_cell          = { workspace = true }
serde              = { workspace = true }
tokio              = { workspace = true }
tracing            = { workspace = true }
# + your client crate (reqwest, sqlx, mongodb, ...)
```

Run `cargo sort --no-format --workspace` after edits.

## src/lib.rs

Code reads top to bottom. Public types first, then `impl`, then private
helpers.

```rust
/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements... */   // full Apache header

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(MySink);   // generates FFI symbols + version export

const CONNECTOR_NAME: &str = "My sink";

#[derive(Debug, Serialize, Deserialize)]
pub struct MySinkConfig {
    pub endpoint: String,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,        // humantime, e.g. "500ms"
    pub verbose_logging: Option<bool>,      // mirror runtime's `verbose` flag
    // Every runtime-tunable field is Option<T>; defaults applied in new()
}

#[derive(Debug)]
pub struct MySink {
    id: u32,
    config: MySinkConfig,
    batch_size: usize,
    max_retries: u32,
    retry_delay: Duration,
    verbose: bool,
    client: Option<Client>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    errors: u64,
}

impl MySink {
    pub fn new(id: u32, config: MySinkConfig) -> Self {
        let batch_size = config.batch_size.unwrap_or(100) as usize;
        let max_retries = config.max_retries.unwrap_or(3);
        let retry_delay = config
            .retry_delay
            .as_deref()
            .and_then(|raw| humantime::Duration::from_str(raw).ok().map(|d| *d))
            .unwrap_or_else(|| {
                warn!("Invalid retry_delay for {CONNECTOR_NAME} ID: {id}, defaulting to 500ms");
                Duration::from_millis(500)
            });
        let verbose = config.verbose_logging.unwrap_or(false);
        Self {
            id,
            config,
            batch_size,
            max_retries,
            retry_delay,
            verbose,
            client: None,
            state: Mutex::new(State { messages_processed: 0, errors: 0 }),
        }
    }
}

#[async_trait]
impl Sink for MySink {
    async fn open(&mut self) -> Result<(), Error> {
        let client = build_client(&self.config)
            .await
            .map_err(|e| Error::InitError(format!("client build failed: {e}")))?;
        client.ping()
            .await
            .map_err(|e| Error::InitError(format!("connectivity check failed: {e}")))?;
        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, endpoint: {}",
            self.id, self.config.endpoint
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(client) = self.client.as_ref() else {
            return Err(Error::InitError("client not initialized".into()));
        };

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages from stream: {}, topic: {}, offset: {}, current_offset: {}",
                self.id, messages.len(), topic_metadata.stream, topic_metadata.topic,
                messages_metadata.partition_id, messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages",
                self.id, messages.len()
            );
        }

        let mut last_err: Option<Error> = None;
        for batch in messages.chunks(self.batch_size) {
            match self.send_batch(client, batch).await {
                Ok(()) => { /* counter */ }
                Err(Error::PermanentHttpError(message)) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} dropping batch (permanent): {message}",
                        self.id
                    );
                }
                Err(error) => {
                    last_err = Some(error);
                }
            }
        }
        match last_err {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // sqlx pools have `.close().await`; reqwest/mongodb/elasticsearch just drop.
        if let Some(client) = self.client.take() {
            let _ = client; // or `client.close().await;` for sqlx
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, errors: {}",
            self.id, state.messages_processed, state.errors
        );
        Ok(())
    }
}

async fn build_client(config: &MySinkConfig) -> Result<Client, ClientError> { /* ... */ }
```
