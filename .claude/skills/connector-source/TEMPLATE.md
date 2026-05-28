# Source plugin skeleton

Boilerplate for a new `core/connectors/sources/<name>_source/`. Adapt
the `MySource` / `Client` / row types to the backend driver.

`Cargo.toml` is identical to a sink's (see
[connector-sink/TEMPLATE.md](../connector-sink/TEMPLATE.md)) - only
the crate name suffix changes (`iggy_connector_<name>_source`) and the
upstream client dep.

## src/lib.rs

Code reads top to bottom. Public types first, `impl Source` after,
helpers below.

```rust
/* Apache 2.0 header */

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

source_connector!(MySource);

const CONNECTOR_NAME: &str = "My source";

#[derive(Debug, Serialize, Deserialize)]
pub struct MySourceConfig {
    pub endpoint: String,
    pub poll_interval: Option<String>,      // humantime, "10s" default
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    cursor: Option<String>,                 // WAL LSN, scroll id, timestamp, ...
    last_offset: u64,
    messages_produced: u64,
}

#[derive(Debug)]
pub struct MySource {
    id: u32,
    config: MySourceConfig,
    poll_interval: Duration,
    verbose: bool,
    client: Option<Client>,
    state: Mutex<State>,
}

impl MySource {
    pub fn new(id: u32, config: MySourceConfig, state: Option<ConnectorState>) -> Self {
        let raw_interval = config.poll_interval.clone().unwrap_or_else(|| "10s".into());
        let poll_interval = humantime::Duration::from_str(&raw_interval)
            .map(|d| *d)
            .unwrap_or_else(|_| {
                warn!("Invalid poll_interval for {CONNECTOR_NAME} ID: {id}, defaulting to 10s");
                Duration::from_secs(10)
            });

        let verbose = config.verbose_logging.unwrap_or(false);

        let restored = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| info!(
                "Restored state for {CONNECTOR_NAME} ID: {id}, last_offset: {}, cursor: {:?}",
                s.last_offset, s.cursor
            ));

        Self {
            id,
            config,
            poll_interval,
            verbose,
            client: None,
            state: Mutex::new(restored.unwrap_or(State {
                cursor: None,
                last_offset: 0,
                messages_produced: 0,
            })),
        }
    }
}

#[async_trait]
impl Source for MySource {
    async fn open(&mut self) -> Result<(), Error> {
        let client = build_client(&self.config)
            .await
            .map_err(|e| Error::InitError(format!("client build failed: {e}")))?;
        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, endpoint: {}",
            self.id, self.config.endpoint
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.poll_interval).await;            // sleep first - backpressure

        let cursor = { self.state.lock().await.cursor.clone() };   // brief read

        let fetched = self.fetch_since(cursor.as_deref()).await?;  // no lock held

        let mut messages = Vec::with_capacity(fetched.len());
        let mut next_cursor = None;
        for row in fetched {
            let payload = simd_json::to_vec(&row).map_err(|e|
                Error::Serialization(format!("row serialize: {e}"))
            )?;
            messages.push(ProducedMessage {
                id: Some(row.id as u128),
                checksum: None,
                timestamp: None,
                origin_timestamp: Some(row.created_at_ns),
                headers: None,
                payload,
            });
            next_cursor = Some(row.cursor_value);
        }

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} produced {} messages, next cursor: {:?}",
                self.id, messages.len(), next_cursor
            );
        }

        let persisted = {                                  // brief write
            let mut state = self.state.lock().await;
            state.messages_produced += messages.len() as u64;
            if let Some(c) = next_cursor {
                state.cursor = Some(c);
            }
            ConnectorState::serialize(&*state, CONNECTOR_NAME, self.id)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(client) = self.client.take() {
            let _ = client; // or `client.close().await;` for sqlx pools
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, total produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

async fn build_client(config: &MySourceConfig) -> Result<Client, ClientError> { /* ... */ }
```
