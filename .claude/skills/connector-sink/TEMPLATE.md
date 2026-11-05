# Sink plugin fill-in-the-blank kit

Copy this kit into `core/connectors/sinks/<name>_sink/`. The scaffolding
covers config, secrets, retry, error classification, batching, logging,
and unit-test shape. **You only implement the marked `TODO(ConnectorDeveloper)`
sections:** build a client from the connection string, and push one
batch.

Also read [SKILL.md](SKILL.md) and pre-flight with
[connector-pr-review](../connector-pr-review/SKILL.md) before `/ready`.

Prefer starting from a compiling crate over copying this prose kit:
`core/connectors/sinks/sink_template/` implements the same shape as real,
tested code you can `cargo build`/`cargo test` immediately, with the same
`TODO(ConnectorDeveloper)` markers. It also has the circuit breaker, which this
kit leaves out to stay short. Use this kit instead only when copying a whole
crate is more scaffolding than you need.

## The delivery contract this kit is built on

By default the server stores a sink's consumer offset when it answers the poll,
before `consume()` runs, and the runtime never redelivers a batch after
`consume()` returns an error. So delivery from Iggy is at-most-once, and the
retrying the plugin does inside `consume()` is the only second chance a batch
gets. The skeleton below therefore keeps trying a batch within one time budget
(`max_delivery_time`), logs the offset range it gives up on, and leaves
idempotent writes to you.

The skeleton skips a chunk the backend rejects for good, so it does not
override `enable_at_least_once()`, and a sink configured with
`[delivery] guarantee = "at_least_once"` refuses to start. To support that
mode, copy `deliver_messages()` and `enable_at_least_once()` from
`sinks/sink_template`, which fail the call instead of skipping.

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
version = "0.5.0"   # match the version most other sinks use
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
iggy_common = { workspace = true }
iggy_connector_sdk = { workspace = true }
once_cell = { workspace = true }
secrecy = { workspace = true }
serde = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
# TODO(ConnectorDeveloper): add your client crate as a workspace dependency

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }  # for paused-time delivery tests
```

Run `cargo sort --no-format --workspace` after edits. Keep `Cargo.lock`
churn limited to this crate's deps.

---

## config.toml (example)

A sink config has a connector header, one or more `[[streams]]` entries that
tell the runtime what to consume and how to batch it, and a `[plugin_config]`
table for your own settings. `sinks/sink_template/config.toml` is the working
reference.

```toml
type = "sink"
key = "<name>"
enabled = true
version = 0
name = "<Name> sink"
path = "../../target/release/libiggy_connector_<name>_sink"
plugin_config_format = "toml"
verbose = false

[[streams]]
stream = "example_stream"
topics = ["example_topic"]
schema = "json"
batch_length = 100
# Long poll: a poll returns once `min_count` messages are ready or `max_wait`
# expires. These replace `poll_interval`, which the runtime refuses for sinks.
# `min_count` must not exceed `batch_length`; `max_wait` must be above zero.
max_wait = "2s"
min_count = 100
consumer_group = "<name>_sink_connector"

[plugin_config]
# Never commit real secrets. Use env overrides in tests/ops.
connection_string = "scheme://user:pass@host:port/db"
batch_size = 100
retry_delay = "500ms"
retry_max_delay = "5s"
# One budget for a whole consume() call. Keep it well under a minute: the call
# blocks a runtime worker thread, and a stop waits for it.
max_delivery_time = "20s"
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
use iggy_connector_sdk::retry::{parse_duration, retry_backoff};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, warn};

sink_connector!(NameSink);

const CONNECTOR_NAME: &str = "Name sink";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
// Well under the runtime's close budget (the poll timeout plus 60 s): consume() blocks a
// runtime worker thread and a stop waits for it.
const DEFAULT_MAX_DELIVERY_TIME: &str = "20s";

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
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub max_delivery_time: Option<String>,
    pub verbose_logging: Option<bool>,
    // TODO(ConnectorDeveloper): optional non-secret knobs (table, index, batch_mode, ...)
}

#[derive(Debug)]
pub struct NameSink {
    id: u32,
    config: NameSinkConfig,
    batch_size: usize,
    retry_delay: Duration,
    retry_max_delay: Duration,
    max_delivery_time: Duration,
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
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize;
        // `parse_duration` falls back to the default with a `warn!` on a bad value.
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_max_delay =
            parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY);
        let max_delivery_time = parse_duration(
            config.max_delivery_time.as_deref(),
            DEFAULT_MAX_DELIVERY_TIME,
        );
        let verbose = config.verbose_logging.unwrap_or(false);
        Self {
            id,
            config,
            batch_size,
            retry_delay,
            retry_max_delay,
            max_delivery_time,
            verbose,
            client: None,
            state: Mutex::new(State::default()),
        }
    }

    /// Retries transient failures with jittered backoff until `deadline`. The runtime never
    /// redelivers a batch after an error, so this loop is the batch's only second chance.
    async fn send_batch_with_retry(
        &self,
        client: &BackendClient,
        topic_metadata: &TopicMetadata,
        batch: &[ConsumedMessage],
        deadline: Instant,
    ) -> Result<(), Error> {
        let mut failed_rounds = 0u32;
        loop {
            match push_batch(client, topic_metadata, batch).await {
                Ok(()) => return Ok(()),
                Err(error) if !is_transient(&error) => return Err(error),
                Err(error) => {
                    failed_rounds += 1;
                    let pause =
                        retry_backoff(self.retry_delay, failed_rounds, self.retry_max_delay);
                    if Instant::now() + pause >= deadline {
                        return Err(error);
                    }
                    warn!(
                        "{CONNECTOR_NAME} ID: {} round {failed_rounds} failed, retrying in \
                         {pause:?}: {error}",
                        self.id
                    );
                    sleep(pause).await;
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

        // One budget for the whole call: with a dead backend every chunk would otherwise wait
        // out a budget of its own, and the runtime is blocked on this call for all of them.
        let deadline = Instant::now() + self.max_delivery_time;
        let mut last_err: Option<Error> = None;
        for (index, chunk) in messages.chunks(self.batch_size).enumerate() {
            match self
                .send_batch_with_retry(client, topic_metadata, chunk, deadline)
                .await
            {
                Ok(()) => {
                    let mut state = self.state.lock().await;
                    state.messages_processed += chunk.len() as u64;
                }
                Err(Error::PermanentHttpError(message)) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} skipping {} messages (permanent), offsets {}: {message}",
                        self.id,
                        chunk.len(),
                        offset_range(chunk)
                    );
                    let mut state = self.state.lock().await;
                    state.errors += chunk.len() as u64;
                }
                // The budget is spent, so the chunks after this one are not attempted either.
                Err(error) if is_transient(&error) => {
                    let undelivered = &messages[index * self.batch_size..];
                    error!(
                        "{CONNECTOR_NAME} ID: {} gave up on {} messages, stream: {}, topic: {}, \
                         partition: {}, offsets {}. The runtime does not redeliver them; replay \
                         that range from Iggy. Cause: {error}",
                        self.id,
                        undelivered.len(),
                        topic_metadata.stream,
                        topic_metadata.topic,
                        messages_metadata.partition_id,
                        offset_range(undelivered)
                    );
                    let mut state = self.state.lock().await;
                    state.errors += undelivered.len() as u64;
                    last_err = Some(error);
                    break;
                }
                Err(error) => {
                    let mut state = self.state.lock().await;
                    state.errors += chunk.len() as u64;
                    last_err = Some(error);
                }
            }
        }
        // Never swallow a failed batch as Ok(()): the error is what gets it logged and counted.
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

/// TODO(ConnectorDeveloper): push one batch. Make the write idempotent on a key built
/// from `stream:topic:partition:offset` (`message.offset`, with the partition from
/// `MessagesMetadata`), or a natural backend key: retries, replays and a server failover
/// all repeat messages. Do not key on `message.id` unless your producers guarantee unique
/// ids, because two different messages with the same id would overwrite each other.
/// Use `message.payload.try_to_bytes()` — do not clone Payload::Json.
///
/// Return a transient error (`HttpRequestFailed`, `Connection`, `CannotStoreData`) for what is
/// worth retrying, `PermanentHttpError` for what never will succeed.
async fn push_batch(
    _client: &BackendClient,
    _topic_metadata: &TopicMetadata,
    _batch: &[ConsumedMessage],
) -> Result<(), Error> {
    Err(Error::InitError("TODO(ConnectorDeveloper): push_batch".into()))
}

/// Map driver errors. Never classify via `err.to_string()` substrings.
fn is_transient(error: &Error) -> bool {
    matches!(
        error,
        Error::HttpRequestFailed(_) | Error::Connection(_) | Error::CannotStoreData(_)
    )
}

fn offset_range(messages: &[ConsumedMessage]) -> String {
    match (messages.first(), messages.last()) {
        (Some(first), Some(last)) => format!("{}..={}", first.offset, last.offset),
        _ => "none".to_string(),
    }
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
            retry_delay: Some("10ms".into()),
            retry_max_delay: Some("100ms".into()),
            max_delivery_time: Some("1s".into()),
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
                retry_delay: None,
                retry_max_delay: None,
                max_delivery_time: None,
                verbose_logging: None,
            },
        );
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE as usize);
        assert_eq!(sink.max_delivery_time, Duration::from_secs(20));
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

Add delivery tests once your `push_batch` returns real errors: recovery within the
budget, giving up with a bounded number of attempts, and no retry of permanent
errors. `sinks/sink_template/src/lib.rs` has all three. Its `deliver_chunk` takes the
attempt as a closure, so they run without a backend; keep that shape if you want to
copy them.

---

## README.md (required paragraphs)

Your README must include a **Delivery semantics** section answering:

1. Transient failure behavior (retried with backoff within `max_delivery_time`,
   then given up on, with the offset range logged)
2. Permanent failure behavior (skip the chunk and count it, or fail)
3. Duplication window (a sink is at-most-once from Iggy by default because the
   offset is stored when the poll is answered; say where your own retries,
   replays and failovers can duplicate)
4. Dedup key (`stream:topic:partition:offset`, a natural backend key, or "none —
   duplicates possible")

Also document the `[[streams]]` settings you recommend (`batch_length`,
`min_count`, `max_wait`) and the limits that follow from a blocking
`consume()`. Diff README defaults against the `DEFAULT_*` consts before opening
the PR.

---

## Before `/ready`

Run the pre-flight checklist in
[connector-pr-review](../connector-pr-review/SKILL.md#pre-flight-author-checklist).
