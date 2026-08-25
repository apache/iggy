# Sink plugin fill-in-the-blank kit

Copy this kit into `core/connectors/sinks/<name>_sink/`. The scaffolding
covers config, secrets, retry, error classification, batching, logging,
and unit-test shape. **You only implement the marked `TODO(ConnectorDeveloper)`
sections:** build a client from the connection string, and push one
batch.

Also read [SKILL.md](SKILL.md) and pre-flight with
[connector-pr-review](../connector-pr-review/SKILL.md) before `/ready`.

## Files to create

```text
core/connectors/sinks/<name>_sink/
├── Cargo.toml
├── README.md
├── config.toml
└── src/lib.rs
```

Add a workspace member, a row in `sinks/README.md`, and a sample under
`runtime/example_config/connectors/`.

---

## Cargo.toml

```toml
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

[package]
name = "iggy_connector_<name>_sink"
version = "0.4.1-edge.1"
edition = "2024"
license = "Apache-2.0"
publish = false
description = "Apache Iggy <Name> sink connector"
repository = "https://github.com/apache/iggy"
homepage = "https://iggy.apache.org"

[package.metadata.cargo-machete]
ignored = ["dashmap", "once_cell"]

[lib]
crate-type = ["cdylib", "lib"]

[dependencies]
async-trait = { workspace = true }
dashmap = { workspace = true }
humantime = { workspace = true }
iggy_common = { workspace = true }
iggy_connector_sdk = { workspace = true }
once_cell = { workspace = true }
secrecy = { workspace = true }
serde = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
# TODO(ConnectorDeveloper): add your client crate as a workspace dependency
```

Run `cargo sort --no-format --workspace` after edits. Keep `Cargo.lock`
churn limited to this crate's deps.

---

## config.toml (example)

```toml
# Plugin path matches sibling sinks (relative to connectors runtime cwd).
path = "../../target/release/libiggy_connector_<name>_sink"

[[sinks]]
key = "<name>"
enabled = true
# path is also set via IGGY_CONNECTORS_SINK_<NAME>_PATH in integration tests

[sinks.<name>.plugin_config]
# Never commit real secrets. Use env overrides in tests/ops.
connection_string = "scheme://user:pass@host:port/db"
batch_size = 100
max_retries = 3
retry_delay = "500ms"
verbose_logging = false
```

---

## src/lib.rs

Replace `<Name>` / `<name>` and implement only the `TODO(ConnectorDeveloper)` blocks.

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(NameSink);

const CONNECTOR_NAME: &str = "Name sink";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_RETRIES: u32 = 3; // total attempts
const DEFAULT_RETRY_DELAY: &str = "500ms";

/// Backend client. Replace with the real driver type.
struct BackendClient {
    // TODO(ConnectorDeveloper): fields
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameSinkConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
    // TODO(ConnectorDeveloper): optional non-secret knobs (table, index, batch_mode, ...)
}

#[derive(Debug)]
pub struct NameSink {
    id: u32,
    config: NameSinkConfig,
    batch_size: usize,
    max_retries: u32,
    retry_delay: Duration,
    verbose: bool,
    client: Option<BackendClient>,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    messages_processed: u64,
    errors: u64,
}

impl NameSink {
    pub fn new(id: u32, config: NameSinkConfig) -> Self {
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE) as usize;
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let retry_delay = config
            .retry_delay
            .as_deref()
            .and_then(|raw| HumanDuration::from_str(raw).ok().map(|d| *d))
            .unwrap_or_else(|| {
                warn!(
                    "Invalid retry_delay for {CONNECTOR_NAME} ID: {id}, defaulting to {DEFAULT_RETRY_DELAY}"
                );
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
            state: Mutex::new(State::default()),
        }
    }

    async fn send_batch_with_retry(
        &self,
        client: &BackendClient,
        topic_metadata: &TopicMetadata,
        batch: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match push_batch(client, topic_metadata, batch).await {
                Ok(()) => return Ok(()),
                Err(error) if is_permanent(&error) => return Err(error),
                Err(error) if attempt >= self.max_retries => return Err(error),
                Err(error) => {
                    warn!(
                        "{CONNECTOR_NAME} ID: {} retry {attempt}/{}: {error}",
                        self.id, self.max_retries
                    );
                    tokio::time::sleep(self.retry_delay.saturating_mul(attempt)).await;
                }
            }
        }
    }
}

#[async_trait]
impl Sink for NameSink {
    async fn open(&mut self) -> Result<(), Error> {
        // Structural validation belongs here / in new(), not in consume().
        let client = build_client(&self.config)
            .await
            .map_err(|e| Error::InitError(format!("client build failed: {e}")))?;
        ping(&client)
            .await
            .map_err(|e| Error::InitError(format!("connectivity check failed: {e}")))?;
        self.client = Some(client);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, endpoint: <redacted>",
            self.id
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
                "{CONNECTOR_NAME} ID: {} consuming {} messages from stream: {}, topic: {}, partition_id: {}, current_offset: {}",
                self.id,
                messages.len(),
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages",
                self.id,
                messages.len()
            );
        }

        // Never swallow a failed batch as Ok(()) — offsets may still advance.
        let mut last_err: Option<Error> = None;
        for batch in messages.chunks(self.batch_size) {
            match self
                .send_batch_with_retry(client, topic_metadata, batch)
                .await
            {
                Ok(()) => {
                    let mut state = self.state.lock().await;
                    state.messages_processed += batch.len() as u64;
                }
                Err(Error::PermanentHttpError(message)) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} dropping batch (permanent): {message}",
                        self.id
                    );
                    let mut state = self.state.lock().await;
                    state.errors += 1;
                }
                Err(error) => {
                    let mut state = self.state.lock().await;
                    state.errors += 1;
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
        if let Some(client) = self.client.take() {
            close_client(client).await;
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, errors: {}",
            self.id, state.messages_processed, state.errors
        );
        Ok(())
    }
}

// ─── Backend surface: implement these ───────────────────────────────────────

/// TODO(ConnectorDeveloper): parse `config.connection_string.expose_secret()` and build the client.
async fn build_client(config: &NameSinkConfig) -> Result<BackendClient, String> {
    let _secret = config.connection_string.expose_secret();
    Err("TODO(ConnectorDeveloper): build_client".into())
}

/// TODO(ConnectorDeveloper): cheap connectivity probe used from open().
async fn ping(_client: &BackendClient) -> Result<(), String> {
    Ok(())
}

/// TODO(ConnectorDeveloper): push one batch. Prefer a stable dedup key from
/// `stream:topic:partition:message_id` (or backend natural key).
/// Use `message.payload.try_to_bytes()` — do not clone Payload::Json.
async fn push_batch(
    _client: &BackendClient,
    _topic_metadata: &TopicMetadata,
    _batch: &[ConsumedMessage],
) -> Result<(), Error> {
    Err(Error::InitError("TODO(ConnectorDeveloper): push_batch".into()))
}

/// Map driver errors. Never classify via `err.to_string()` substrings.
fn is_permanent(error: &Error) -> bool {
    matches!(
        error,
        Error::PermanentHttpError(_) | Error::SchemaMismatch(_) | Error::InvalidRecordValue(_)
    )
}

async fn close_client(_client: BackendClient) {
    // sqlx: pool.close().await; most HTTP clients: drop
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NameSinkConfig {
        NameSinkConfig {
            connection_string: SecretString::from("scheme://localhost/db"),
            batch_size: Some(10),
            max_retries: Some(2),
            retry_delay: Some("10ms".into()),
            verbose_logging: Some(false),
        }
    }

    #[test]
    fn given_defaults_should_apply_consts() {
        let sink = NameSink::new(
            1,
            NameSinkConfig {
                connection_string: SecretString::from("scheme://localhost/db"),
                batch_size: None,
                max_retries: None,
                retry_delay: None,
                verbose_logging: None,
            },
        );
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE as usize);
        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_invalid_retry_delay_should_fall_back_to_default() {
        let mut config = test_config();
        config.retry_delay = Some("not-a-duration".into());
        let sink = NameSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }
}
```

---

## README.md (required paragraphs)

Your README must include a **Delivery semantics** section answering:

1. Transient failure behavior (retry N times, then `Err`)
2. Permanent failure behavior (drop/skip vs fail)
3. Duplication window (usually at-least-once)
4. Dedup key (or "none — duplicates possible")

Diff README defaults against the `DEFAULT_*` consts before opening the PR.

---

## Before `/ready`

Run the pre-flight checklist in
[connector-pr-review](../connector-pr-review/SKILL.md#pre-flight-author-checklist).
