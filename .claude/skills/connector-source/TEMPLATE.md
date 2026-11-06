# Source plugin fill-in-the-blank kit

Copy this kit into `core/connectors/sources/<name>_source/`. The
scaffolding covers config, secrets, sleep-first poll, lock discipline,
staged/committed state serde (ACK/NACK batch acknowledgment, #3855),
retry classification, logging, and the state and ACK/NACK tests.
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

One connector per file, in the runtime's format. The keys under
`[plugin_config]` are the plugin's own; the rest is the same for every
source (see `runtime/example_config/connectors/source_template.toml`).

```toml
type = "source"
key = "<name>"
enabled = true
version = 0
name = "<Name> source"
path = "target/release/libiggy_connector_<name>_source"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream = "example_stream"
topic = "example_topic"
schema = "json"
batch_length = 100
linger_time = "5ms"

[plugin_config]
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
The crate also needs `serde_json` and `rmp-serde` (state) as workspace dependencies.

```rust
/* Apache 2.0 header */

use async_trait::async_trait;
use iggy_connector_sdk::retry::parse_duration;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, warn};

source_connector!(NameSource);

const CONNECTOR_NAME: &str = "Name source";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_RETRIES: u32 = 3; // total attempts
const DEFAULT_RETRY_DELAY: &str = "500ms";

#[derive(Debug)]
struct BackendClient {
    // TODO(ConnectorDeveloper): fields
}

/// One fetched record before it becomes an Iggy message.
struct FetchedRecord {
    /// Where the cursor moves once this record's batch is acknowledged.
    cursor_value: String,
    /// Stable natural key (never a random UUID), so a redelivered record keeps its id.
    natural_id: Option<u128>,
    /// The record as JSON, or why it could not be converted
    /// (`Error::InvalidRecordValue`). `stage_batch()` never moves the cursor past an `Err`.
    payload: Result<serde_json::Value, Error>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Committed by `on_batch_result(Ack)` only.
    state: Mutex<State>,
    /// Staged by `poll()`, never written to `state` there.
    pending_state: Mutex<Option<State>>,
}

impl NameSource {
    pub fn new(id: u32, config: NameSourceConfig, state: Option<ConnectorState>) -> Self {
        let poll_interval = parse_duration(config.poll_interval.as_deref(), DEFAULT_POLL_INTERVAL);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize;
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES).max(1);
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

    /// Transient errors are retried up to `max_retries` total attempts. A permanent
    /// error (`Error::is_permanent()`) is returned at once.
    async fn fetch_with_retry(
        &self,
        client: &BackendClient,
        cursor: Option<&str>,
    ) -> Result<Vec<FetchedRecord>, Error> {
        let mut attempt = 1;
        loop {
            match fetch_batch(client, cursor, self.batch_size).await {
                Ok(records) => return Ok(records),
                Err(error) if error.is_permanent() || attempt >= self.max_retries => {
                    return Err(error);
                }
                Err(error) => {
                    warn!(
                        "{CONNECTOR_NAME} ID: {} fetch attempt {attempt}/{} failed: {error}",
                        self.id, self.max_retries
                    );
                    attempt += 1;
                    sleep(self.retry_delay).await;
                }
            }
        }
    }

    /// Converts records to messages and stages the cursor that follows them. The first
    /// record that cannot be converted ends the batch, so the cursor never passes it. When
    /// it is the first record there is nothing to send and the poll fails.
    async fn stage_batch(
        &self,
        cursor: Option<String>,
        records: Vec<FetchedRecord>,
    ) -> Result<ProducedMessages, Error> {
        let mut messages = Vec::with_capacity(records.len());
        let mut new_cursor = cursor;
        for record in records {
            let payload = record.payload.and_then(|value| {
                serde_json::to_vec(&value).map_err(|e| Error::Serialization(e.to_string()))
            });
            let payload = match payload {
                Ok(payload) => payload,
                Err(error) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} cannot convert the record at cursor {}, \
                         not moving past it: {error}",
                        self.id, record.cursor_value
                    );
                    if messages.is_empty() {
                        return Err(error);
                    }
                    break;
                }
            };
            new_cursor = Some(record.cursor_value);
            messages.push(ProducedMessage {
                id: record.natural_id,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload,
            });
        }

        let messages_produced = self.state.lock().await.messages_produced + messages.len() as u64;
        let candidate = State {
            cursor: new_cursor,
            messages_produced,
        };
        let persisted = ConnectorState::serialize(&candidate, CONNECTOR_NAME, self.id)
            .ok_or_else(|| Error::Serialization("failed to serialize state".into()))?;
        *self.pending_state.lock().await = Some(candidate);

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} staged {} messages",
                self.id,
                messages.len()
            );
        }

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(persisted),
        })
    }
}

#[async_trait]
impl Source for NameSource {
    async fn open(&mut self) -> Result<(), Error> {
        if self
            .config
            .connection_string
            .expose_secret()
            .trim()
            .is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "connection_string must not be empty".into(),
            ));
        }
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
        sleep(self.poll_interval).await; // sleep first: backpressure, and paces a failing poll

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("client not initialized, was open() called?".into())
        })?;
        let cursor = self.state.lock().await.cursor.clone(); // brief read, no lock across I/O

        let records = self.fetch_with_retry(client, cursor.as_deref()).await?;
        if records.is_empty() {
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: Vec::new(),
                state: None,
            });
        }
        self.stage_batch(cursor, records).await
    }

    /// The staged state becomes the committed state only on `Ack`. A `Nack` (send or
    /// state-save failure) drops it, so the same range is fetched again. Destructive work
    /// (delete, mark-processed) is staged in `poll()` and applied here on `Ack`.
    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let staged = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(staged) = staged
        {
            *self.state.lock().await = staged;
        }
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

/// TODO(ConnectorDeveloper): fetch up to `limit` rows after `cursor`, in cursor order.
/// Set `FetchedRecord.natural_id` from a stable natural key. Leave
/// `ProducedMessage.origin_timestamp` unset: the runtime drops it today.
async fn fetch_batch(
    _client: &BackendClient,
    _cursor: Option<&str>,
    _limit: usize,
) -> Result<Vec<FetchedRecord>, Error> {
    Err(Error::InitError(
        "TODO(ConnectorDeveloper): fetch_batch".into(),
    ))
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

    fn record(cursor_value: &str, payload: Result<serde_json::Value, Error>) -> FetchedRecord {
        FetchedRecord {
            cursor_value: cursor_value.into(),
            natural_id: Some(1),
            payload,
        }
    }

    #[tokio::test]
    async fn given_persisted_state_should_restore_cursor() {
        let state = State {
            cursor: Some("cursor-1".into()),
            messages_produced: 7,
        };
        let bytes = rmp_serde::to_vec(&state).expect("serialize");
        let source = NameSource::new(1, test_config(), Some(ConnectorState(bytes)));
        assert_eq!(*source.state.lock().await, state);
    }

    #[tokio::test]
    async fn given_no_state_should_start_fresh() {
        let source = NameSource::new(1, test_config(), None);
        let restored = source.state.lock().await;
        assert!(restored.cursor.is_none());
        assert_eq!(restored.messages_produced, 0);
    }

    #[tokio::test]
    async fn given_invalid_state_should_start_fresh() {
        let invalid = ConnectorState(b"not valid msgpack".to_vec());
        let source = NameSource::new(1, test_config(), Some(invalid));
        let restored = source.state.lock().await;
        assert!(restored.cursor.is_none());
        assert_eq!(restored.messages_produced, 0);
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

    // poll()'s fetch is a TODO, so these stage through stage_batch() directly. The
    // stage/commit/discard contract is independent of the backend and must stay covered.

    #[tokio::test]
    async fn given_staged_batch_before_ack_should_not_move_committed_cursor() {
        let source = NameSource::new(1, test_config(), None);

        source
            .stage_batch(None, vec![record("c1", Ok(serde_json::json!({})))])
            .await
            .expect("stage");

        assert!(source.state.lock().await.cursor.is_none());
        let staged = source.pending_state.lock().await.clone().expect("staged");
        assert_eq!(staged.cursor.as_deref(), Some("c1"));
    }

    #[tokio::test]
    async fn given_ack_when_batch_is_staged_should_commit_staged_state() {
        let source = NameSource::new(1, test_config(), None);
        source
            .stage_batch(None, vec![record("c1", Ok(serde_json::json!({})))])
            .await
            .expect("stage");

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack");

        let committed = source.state.lock().await.clone();
        assert_eq!(committed.cursor.as_deref(), Some("c1"));
        assert_eq!(committed.messages_produced, 1);
        assert!(source.pending_state.lock().await.is_none());
    }

    #[tokio::test]
    async fn given_nack_when_batch_is_staged_should_keep_committed_state() {
        let source = NameSource::new(1, test_config(), None);
        let before = source.state.lock().await.clone();
        source
            .stage_batch(None, vec![record("c1", Ok(serde_json::json!({})))])
            .await
            .expect("stage");

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack");

        assert_eq!(*source.state.lock().await, before);
        assert!(source.pending_state.lock().await.is_none());
    }

    #[tokio::test]
    async fn given_bad_record_mid_batch_should_stop_before_it() {
        let source = NameSource::new(1, test_config(), None);
        let records = vec![
            record("c1", Ok(serde_json::json!({}))),
            record("c2", Err(Error::InvalidRecordValue("bad".into()))),
            record("c3", Ok(serde_json::json!({}))),
        ];

        let produced = source.stage_batch(None, records).await.expect("stage");

        assert_eq!(produced.messages.len(), 1);
        let staged = source.pending_state.lock().await.clone().expect("staged");
        assert_eq!(staged.cursor.as_deref(), Some("c1"));
    }

    #[tokio::test]
    async fn given_bad_first_record_should_fail_and_stage_nothing() {
        let source = NameSource::new(1, test_config(), None);
        let records = vec![record("c1", Err(Error::InvalidRecordValue("bad".into())))];

        let result = source.stage_batch(None, records).await;

        assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
        assert!(source.pending_state.lock().await.is_none());
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
Mandatory: the state, ACK/NACK and bad-record tests above must stay green.
