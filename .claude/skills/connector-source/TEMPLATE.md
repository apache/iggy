# Source plugin fill-in-the-blank kit

Copy this kit into `core/connectors/sources/<name>_source/`. The
scaffolding covers config, secrets, sleep-first poll, lock discipline,
staged/committed state ser·de (ACK/NACK batch acknowledgment, #3855),
retry classification, logging, and the six canonical state tests.
**You only implement the marked `TODO(ConnectorDeveloper)` sections:** build a
client from the connection string, and fetch the next batch (advancing
a cursor).

Also read [SKILL.md](SKILL.md) and pre-flight with
[connector-pr-review](../connector-pr-review/SKILL.md) before `/ready`.

Prefer starting from a compiling crate over copying this prose kit:
`core/connectors/sources/source_template/` implements the same shape as
real, tested code you can `cargo build`/`cargo test` immediately, with the
same `TODO(ConnectorDeveloper)` markers. Use this kit instead only when
copying a whole crate is more scaffolding than you need.

## Files to create

```text
core/connectors/sources/<name>_source/
├── Cargo.toml
├── README.md
├── config.toml
└── src/lib.rs
```

Add a workspace member, a row in `sources/README.md`, and a sample under
`runtime/example_config/connectors/`.

---

## Cargo.toml

Same shape as the sink kit (`cdylib` + `lib`, workspace deps, Apache
header). Only the package name suffix changes:

```toml
name = "iggy_connector_<name>_source"
# ... identical metadata / machete ignored / crate-type ...
# TODO(ConnectorDeveloper): add your client crate as a workspace dependency
```

---

## config.toml (example)

```toml
path = "../../target/release/libiggy_connector_<name>_source"

[[sources]]
key = "<name>"
enabled = true

[sources.<name>.plugin_config]
connection_string = "scheme://user:pass@host:port/db"
poll_interval = "5s"
batch_size = 100
max_retries = 3
retry_delay = "500ms"
verbose_logging = false
```

Defaults in this file must match `DEFAULT_*` consts in code.

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
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

source_connector!(NameSource);

const CONNECTOR_NAME: &str = "Name source";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_RETRIES: u32 = 3; // total attempts
const DEFAULT_RETRY_DELAY: &str = "500ms";

struct BackendClient {
    // TODO(ConnectorDeveloper): fields
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameSourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
    // TODO(ConnectorDeveloper): optional non-secret knobs (query, table, index, ...)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct State {
    /// Opaque backend cursor (LSN, scroll id, timestamp, PK, ...). Keep O(1).
    cursor: Option<String>,
    messages_produced: u64,
}

#[derive(Debug)]
pub struct NameSource {
    id: u32,
    config: NameSourceConfig,
    poll_interval: Duration,
    batch_size: usize,
    max_retries: u32,
    retry_delay: Duration,
    verbose: bool,
    client: Option<BackendClient>,
    state: Mutex<State>,
    /// Staged in `poll()`, committed to `state` on `Ack`, discarded on `Nack`
    /// (source batch acknowledgment, #3855). Only one batch is ever staged
    /// at a time - the SDK enforces one in-flight batch.
    pending_state: Mutex<Option<State>>,
}

struct FetchedBatch {
    messages: Vec<ProducedMessage>,
    /// Next cursor, staged via `pending_state` and committed only after
    /// `on_batch_result(SourceBatchResult::Ack)`.
    next_cursor: Option<String>,
}

impl NameSource {
    pub fn new(id: u32, config: NameSourceConfig, state: Option<ConnectorState>) -> Self {
        let raw_interval = config
            .poll_interval
            .clone()
            .unwrap_or_else(|| DEFAULT_POLL_INTERVAL.into());
        let poll_interval = HumanDuration::from_str(&raw_interval)
            .map(|d| *d)
            .unwrap_or_else(|_| {
                warn!(
                    "Invalid poll_interval for {CONNECTOR_NAME} ID: {id}, defaulting to {DEFAULT_POLL_INTERVAL}"
                );
                Duration::from_secs(5)
            });
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

        let restored = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} ID: {id}, cursor: {:?}, messages_produced: {}",
                    s.cursor, s.messages_produced
                );
            });

        Self {
            id,
            config,
            poll_interval,
            batch_size,
            max_retries,
            retry_delay,
            verbose,
            client: None,
            state: Mutex::new(restored.unwrap_or(State {
                cursor: None,
                messages_produced: 0,
            })),
            pending_state: Mutex::new(None),
        }
    }

    async fn fetch_with_retry(
        &self,
        client: &BackendClient,
        cursor: Option<String>,
    ) -> Result<FetchedBatch, Error> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match fetch_batch(client, cursor.as_deref(), self.batch_size).await {
                Ok(batch) => return Ok(batch),
                Err(error) if is_permanent(&error) => return Err(error),
                Err(error) if attempt >= self.max_retries => return Err(error),
                Err(error) => {
                    warn!(
                        "{CONNECTOR_NAME} ID: {} retry {attempt}/{}: {error}",
                        self.id, self.max_retries
                    );
                    sleep(self.retry_delay.saturating_mul(attempt)).await;
                }
            }
        }
    }
}

#[async_trait]
impl Source for NameSource {
    async fn open(&mut self) -> Result<(), Error> {
        // Validate query/cursor shape here — not on first poll after sleep.
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

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Sleep FIRST or an idle source spins the CPU.
        sleep(self.poll_interval).await;

        let Some(client) = self.client.as_ref() else {
            return Err(Error::InitError("client not initialized".into()));
        };

        // Brief lock read → drop → I/O. Never hold across upstream await.
        let cursor = { self.state.lock().await.cursor.clone() };

        let fetched = self.fetch_with_retry(client, cursor).await?;

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} polled {} messages, next_cursor: {:?}",
                self.id,
                fetched.messages.len(),
                fetched.next_cursor
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} polled {} messages",
                self.id,
                fetched.messages.len()
            );
        }

        // Stage the candidate state - do NOT commit it to `self.state` here, and do
        // NOT delete/mark upstream rows here either. The runtime sends the batch and
        // saves this staged state; `on_batch_result()` below commits it (Ack) or
        // discards it (Nack) before the next poll() runs. Committing directly in
        // poll() would leave nothing to roll back on a Nack.
        let messages_produced = self.state.lock().await.messages_produced;
        let candidate_state = State {
            cursor: fetched.next_cursor,
            messages_produced: messages_produced + fetched.messages.len() as u64,
        };
        let state_bytes = ConnectorState::serialize(&candidate_state, CONNECTOR_NAME, self.id);
        *self.pending_state.lock().await = Some(candidate_state);

        Ok(ProducedMessages {
            schema: Schema::Json, // TODO(ConnectorDeveloper): match actual payload bytes
            messages: fetched.messages,
            state: state_bytes,
        })
    }

    // Commits or discards the batch staged in poll() above. The default
    // trait impl is a no-op - override is mandatory whenever poll() stages
    // a cursor or destructive work, which this template always does.
    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate_state = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate_state) = candidate_state
        {
            *self.state.lock().await = candidate_state;
        }
        // Nack: candidate_state is dropped, self.state (committed) is untouched,
        // so the next poll() re-fetches from the same committed cursor.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(client) = self.client.take() {
            close_client(client).await;
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, total produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

// ─── Backend surface: implement these ───────────────────────────────────────

/// TODO(ConnectorDeveloper): parse `config.connection_string.expose_secret()` and build the client.
async fn build_client(config: &NameSourceConfig) -> Result<BackendClient, String> {
    let _secret = config.connection_string.expose_secret();
    Err("TODO(ConnectorDeveloper): build_client".into())
}

/// TODO(ConnectorDeveloper): cheap connectivity probe used from open().
async fn ping(_client: &BackendClient) -> Result<(), String> {
    Ok(())
}

/// TODO(ConnectorDeveloper): fetch up to `limit` rows after `cursor`.
/// Set `ProducedMessage.id` from a stable natural key (never random UUID).
/// Set `origin_timestamp` when the backend has event time (nanoseconds).
async fn fetch_batch(
    _client: &BackendClient,
    _cursor: Option<&str>,
    _limit: usize,
) -> Result<FetchedBatch, Error> {
    Err(Error::InitError("TODO(ConnectorDeveloper): fetch_batch".into()))
}

fn is_permanent(error: &Error) -> bool {
    matches!(
        error,
        Error::PermanentHttpError(_) | Error::SchemaMismatch(_) | Error::InvalidConfigValue(_)
    )
}

async fn close_client(_client: BackendClient) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NameSourceConfig {
        NameSourceConfig {
            connection_string: SecretString::from("scheme://localhost/db"),
            poll_interval: Some("100ms".into()),
            batch_size: Some(10),
            max_retries: Some(2),
            retry_delay: Some("10ms".into()),
            verbose_logging: Some(false),
        }
    }

    #[test]
    fn given_persisted_state_should_restore_cursor() {
        let state = State {
            cursor: Some("cursor-1".into()),
            messages_produced: 7,
        };
        let bytes = rmp_serde::to_vec(&state).expect("serialize");
        let source = NameSource::new(1, test_config(), Some(ConnectorState(bytes)));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.cursor.as_deref(), Some("cursor-1"));
            assert_eq!(restored.messages_produced, 7);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = NameSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert!(restored.cursor.is_none());
            assert_eq!(restored.messages_produced, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid = ConnectorState(b"not valid msgpack".to_vec());
        let source = NameSource::new(1, test_config(), Some(invalid));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert!(restored.cursor.is_none());
            assert_eq!(restored.messages_produced, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            cursor: Some("c".into()),
            messages_produced: 3,
        };
        let bytes = rmp_serde::to_vec(&original).unwrap();
        let restored: State = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn given_defaults_should_apply_consts() {
        let source = NameSource::new(
            1,
            NameSourceConfig {
                connection_string: SecretString::from("scheme://localhost/db"),
                poll_interval: None,
                batch_size: None,
                max_retries: None,
                retry_delay: None,
                verbose_logging: None,
            },
            None,
        );
        assert_eq!(source.batch_size, DEFAULT_BATCH_SIZE as usize);
        assert_eq!(source.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(source.poll_interval, Duration::from_secs(5));
    }

    // Stages `pending_state` directly rather than going through poll() - poll()'s
    // TODO(ConnectorDeveloper) fetch is unimplemented in this template, but on_batch_result's
    // commit/discard contract is independently testable and must stay covered once
    // the backend is filled in.

    #[test]
    fn given_ack_when_batch_is_staged_should_commit_candidate_state() {
        let source = NameSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let candidate = State {
                cursor: Some("cursor-2".into()),
                messages_produced: 5,
            };
            *source.pending_state.lock().await = Some(candidate.clone());

            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ack should be applied");

            assert_eq!(*source.state.lock().await, candidate);
            assert!(source.pending_state.lock().await.is_none());
        });
    }

    #[test]
    fn given_nack_when_batch_is_staged_should_keep_committed_state() {
        let source = NameSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let committed_before = source.state.lock().await.clone();
            *source.pending_state.lock().await = Some(State {
                cursor: Some("cursor-2".into()),
                messages_produced: 5,
            });

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("nack should be applied");

            assert_eq!(*source.state.lock().await, committed_before);
            assert!(source.pending_state.lock().await.is_none());
        });
    }
}
```

---

## README.md (required paragraphs)

Include a **Delivery semantics** section:

1. Transient fetch failure → retry N times, then `Err` (loop continues; see `connector-source`)
2. Cursor commits only on `on_batch_result(Ack)` - a Nack (send or state-save failure) discards the staged cursor and redelivers the batch
3. Whether destructive work (delete/mark-processed) is staged in `poll()` and applied only in `on_batch_result()` on Ack (standard - no loss window to document), or must happen earlier for some architectural reason (if so, document the loss window explicitly)
4. Dedup key for `ProducedMessage.id` (or "none")

---

## Before `/ready`

Run the pre-flight checklist in
[connector-pr-review](../connector-pr-review/SKILL.md#pre-flight-author-checklist).
Mandatory: the six canonical state tests above must stay green.
