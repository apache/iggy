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

//! Template Apache Iggy **sink** connector.
//!
//! Everything in this file is already wired up and follows the patterns the
//! connector-review checklist expects: config validation happens in
//! `open()`, secrets are `SecretString`, the destination identifier is
//! validated before it's ever interpolated into a request, outbound calls
//! go through a retry-wrapped client plus a circuit breaker, and messages
//! are chunked by a configurable batch size instead of shipped as one
//! unbounded request.
//!
//! Delivery follows the runtime's contract. The server stores the offset when
//! it answers the poll, and the runtime never redelivers a batch after an
//! error, so `consume()` keeps trying a batch for up to `max_delivery_time`
//! before it gives up and logs the lost offset range. The circuit breaker only
//! paces those attempts; it never discards a batch. Writes must be idempotent
//! on `dedup_key()`, because replays and failovers can repeat a message.
//!
//! There is exactly **one** place you need to write code, marked
//! `TODO(ConnectorDeveloper)`: `TemplateSink::push_batch()` — build the
//! request/write that actually pushes one chunk of messages to your
//! destination, using `self.config.connection_string` (and
//! `self.config.target`, if your destination has a table/index/
//! collection-shaped name). A second `TODO(ConnectorDeveloper)` on the
//! `connection_string` field's doc comment just asks you to describe its
//! expected shape — not code, but worth personalizing too. `grep` for the
//! marker and you'll find both, plus one more on `build_raw_client()` that
//! only applies if your destination isn't HTTP (see below).
//!
//! This template assumes an HTTP-ish destination and uses `reqwest` wrapped
//! by the SDK's retry middleware, because that's what
//! `iggy_connector_sdk::retry` is built for and it covers the common case.
//! If your destination talks something else (a database, a queue, object
//! storage), swap the client type in `build_raw_client()`/`push_batch()` for
//! your driver of choice and lean on its own retry/pooling behavior — keep the
//! surrounding shape (validation in `open()`, circuit breaker, batching,
//! identifier validation) unchanged. See `core/connectors/sinks/postgres_sink`
//! or `core/connectors/sinks/s3_sink` in this repo for non-HTTP examples of
//! that same shape.

use async_trait::async_trait;
use iggy_connector_sdk::retry::{
    CircuitBreaker, RetryPolicy, build_retry_client, check_connectivity_with_retry, parse_duration,
    retry_backoff,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, warn};

sink_connector!(TemplateSink);

const CONNECTOR_NAME: &str = "Template sink";

const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
// Three failed rounds per cool-down cycle bound the attempts a struggling destination sees:
// the SDK breaker closes completely after its cool-down, so it takes `threshold` new failures
// to open again. The cool-down has to be shorter than the delivery budget, or a call that
// finds the breaker open could never reach a retry.
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 3;
const DEFAULT_CIRCUIT_BREAKER_COOL_DOWN: &str = "10s";
// Kept well under the runtime's close budget (the poll timeout plus 60 s): `consume()` blocks
// a runtime worker thread and a stop waits for it to return.
const DEFAULT_MAX_DELIVERY_TIME: &str = "20s";
const BREAKER_RECHECK_INTERVAL: Duration = Duration::from_millis(250);

// ── Configuration ───────────────────────────────────────────────────────────
//
// Every tunable except `connection_string` and `target` is optional with a
// sane default, and unknown keys are rejected outright so a typo in a TOML
// file fails at load time instead of silently doing nothing.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateSinkConfig {
    /// TODO(ConnectorDeveloper): document the exact shape this connector expects, e.g.
    /// "<https://api.example.com>" or "postgres://user:pass@host:5432/db".
    /// `SecretString` because DSNs commonly embed credentials — never plain
    /// `String` for this field, see `PostgresSinkConfig::connection_string`
    /// in `sinks/postgres_sink` for the same pattern.
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,

    /// The destination table/index/collection/bucket name. Kept as its own
    /// field (rather than folded into `connection_string`) specifically so
    /// it can be validated in `open()` before ever being interpolated into
    /// a query, path, or URL — see `validate_identifier` below. Delete this
    /// field if your destination has no such dynamic identifier.
    pub target: String,

    /// Example of a secret-shaped setting. `SecretString` keeps it out of
    /// `Debug`/log output; delete this field if `connection_string` already
    /// carries all required auth. Read it with `.expose_secret()` (from the
    /// `secrecy::ExposeSecret` trait) at the one place you actually need the
    /// plaintext — e.g. when building an auth header in `build_raw_client()`.
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub auth_token: Option<SecretString>,

    /// Optional path (e.g. "/health") probed with retry during `open()`
    /// before the connector is considered ready. Leave unset if your target
    /// has no health endpoint — the probe is skipped, not failed, in that case.
    pub health_check_path: Option<String>,

    pub batch_size: Option<usize>,
    pub timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,

    /// How long one `consume()` call may keep trying before it gives up on the
    /// messages it still holds. Must be longer than `circuit_breaker_cool_down`.
    pub max_delivery_time: Option<String>,

    /// Upgrades the per-batch log line from `debug!` to `info!`. Mirrors the
    /// runtime's own `verbose` flag; keep the field name `verbose_logging`
    /// (see `postgres_sink::PostgresSinkConfig::verbose_logging`).
    pub verbose_logging: Option<bool>,
}

/// Rejects anything that isn't a plain alphanumeric/underscore identifier.
/// Adjust the allowed character set to whatever your destination's naming
/// rules actually are, but always validate *something* before a
/// config-or-message-derived name is interpolated into a query, path, or
/// URL — see `doris_sink::validate_identifier` / `surrealdb_sink::validate_identifier`
/// in this repo for the same pattern applied to a real destination.
fn validate_identifier(field: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must be non-empty and contain only ASCII alphanumeric characters and \
             underscores, got: {value:?}"
        )));
    }
    Ok(())
}

// ── Internal state ──────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct State {
    invocations_count: u64,
    messages_written: u64,
    messages_failed: u64,
}

#[derive(Debug)]
pub struct TemplateSink {
    id: u32,
    config: TemplateSinkConfig,
    client: Option<ClientWithMiddleware>,
    circuit_breaker: Arc<CircuitBreaker>,
    batch_size_limit: usize,
    retry_delay: Duration,
    retry_max_delay: Duration,
    cool_down: Duration,
    max_delivery_time: Duration,
    verbose: bool,
    state: Mutex<State>,
    records_written_total: AtomicU64,
}

impl TemplateSink {
    pub fn new(id: u32, config: TemplateSinkConfig) -> Self {
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_max_delay =
            parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY);
        let cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CIRCUIT_BREAKER_COOL_DOWN,
        );
        let max_delivery_time = parse_duration(
            config.max_delivery_time.as_deref(),
            DEFAULT_MAX_DELIVERY_TIME,
        );
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config
                .circuit_breaker_threshold
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
            cool_down,
        ));
        let batch_size_limit = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let verbose = config.verbose_logging.unwrap_or(false);

        Self {
            id,
            config,
            client: None,
            circuit_breaker,
            batch_size_limit,
            retry_delay,
            retry_max_delay,
            cool_down,
            max_delivery_time,
            verbose,
            state: Mutex::new(State::default()),
            records_written_total: AtomicU64::new(0),
        }
    }

    /// TODO(ConnectorDeveloper): build your actual client/connection here using
    /// `self.config.connection_string` (and `self.config.auth_token`, if
    /// your destination needs it). This template builds a plain
    /// `reqwest::Client` to hand to `build_retry_client` — if you're not
    /// talking HTTP, replace this with e.g. a database connection pool or
    /// your driver's equivalent, store it on `self` (add a field, since
    /// `client` here is HTTP-specific), and skip the
    /// `check_connectivity_with_retry` call below in favor of whatever your
    /// driver offers (a ping, a test query).
    fn build_raw_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::Connection(format!("failed to build HTTP client: {e}")))
    }

    /// TODO(ConnectorDeveloper): push one chunk of already-batched messages to your
    /// destination using `self.config.connection_string` and
    /// `self.config.target`, via `self.client` (already retry-wrapped).
    ///
    /// The error you return decides what `consume()` does next:
    /// - `Error::HttpRequestFailed`, `Error::Connection` and
    ///   `Error::CannotStoreData` (network error, 5xx, timeout) are transient.
    ///   `consume()` retries the same chunk with backoff until
    ///   `max_delivery_time` runs out, and each failed round counts toward the
    ///   circuit breaker.
    /// - `Error::PermanentHttpError` (bad schema, the destination rejects the
    ///   payload shape) will never succeed. The chunk is skipped and counted,
    ///   and the breaker is left alone, because a data problem says nothing
    ///   about whether the destination is up. See the doc comment on that
    ///   variant in `iggy_connector_sdk::Error`.
    /// - Any other error is not retried. The chunk is counted as failed and
    ///   the error is returned once every chunk has been tried.
    ///
    /// The runtime never redelivers a batch after an error, because the server
    /// stored the offset when it answered the poll. The retries above are the
    /// only second chance a batch gets, so from Iggy to this plugin delivery is
    /// at-most-once. Duplicates come from those retries (a request that failed
    /// part way through a write), from replaying an offset range after a
    /// logged loss, and from a server failover before the stored offset
    /// replicated. Build a stable key with `dedup_key()` below for every
    /// message you write, and make the write idempotent on it (upsert /
    /// `ON CONFLICT` / unique index) — see `connector-pr-review` B5.
    ///
    /// If your destination's write call can succeed at the transport level
    /// while rejecting some or all rows individually (a bulk/batch API
    /// returning 200 with per-item errors, a stream-load response, a bulk
    /// index response), count accepted rows yourself and return `Err` when
    /// that count is zero for a non-empty `batch` — never `Ok(())`. Four
    /// unrelated sinks shipped exactly this bug in review (BigQuery,
    /// Elasticsearch, Doris, S3): the call "succeeded," `consume()` reported
    /// success, and the batch was gone with no signal anywhere. See
    /// `connector-pr-review` B9.
    async fn push_batch(
        &self,
        client: &ClientWithMiddleware,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        batch: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let _ = client; // remove once implemented
        for message in batch {
            // TODO(ConnectorDeveloper): use `key` as the identity your destination's
            // write is idempotent on (upsert key, `ON CONFLICT` target, document _id).
            let key = dedup_key(topic_metadata, messages_metadata, message);
            let _ = key;
        }
        Err(Error::InitError(
            "TemplateSink::push_batch is not implemented yet — see the TODO(ConnectorDeveloper) comment in \
             template_sink/src/lib.rs"
                .to_string(),
        ))
    }

    /// Runs `attempt` until it succeeds, fails for good, or `deadline` passes.
    ///
    /// A failed round counts once toward the circuit breaker, and the wait before the next
    /// round is jittered exponential backoff. While the breaker is open nothing reaches the
    /// destination, so a struggling destination sees at most `threshold` rounds per cool-down.
    async fn deliver_chunk<F, Fut>(&self, deadline: Instant, mut attempt: F) -> Result<(), Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let mut failed_rounds = 0;
        let mut last_err: Option<Error> = None;
        loop {
            // Report the failure that opened the breaker, not the wait that ran out.
            if let Err(still_open) = self.wait_for_closed_breaker(deadline).await {
                return Err(last_err.unwrap_or(still_open));
            }
            match attempt().await {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    return Ok(());
                }
                Err(err) if is_transient(&err) => {
                    self.circuit_breaker.record_failure().await;
                    failed_rounds += 1;
                    let pause =
                        retry_backoff(self.retry_delay, failed_rounds, self.retry_max_delay);
                    if Instant::now() + pause >= deadline {
                        return Err(err);
                    }
                    last_err = Some(err);
                    sleep(pause).await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Waits out an open breaker instead of refusing the batch. The runtime stored the offset
    /// when it polled, so a refused batch would be gone for good.
    async fn wait_for_closed_breaker(&self, deadline: Instant) -> Result<(), Error> {
        let mut warned = false;
        while self.circuit_breaker.is_open().await {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::CannotStoreData(
                    "circuit breaker stayed open for the whole delivery budget".to_string(),
                ));
            }
            if !warned {
                warn!(
                    "{CONNECTOR_NAME} connector with ID: {} circuit breaker is OPEN, holding the \
                     batch for up to {remaining:?}",
                    self.id
                );
                warned = true;
            }
            sleep(remaining.min(BREAKER_RECHECK_INTERVAL)).await;
        }
        Ok(())
    }
}

/// Stable per-message dedup key: `stream:topic:partition:offset`. Mirrors
/// `mongodb_sink::build_composite_document_id` — copy this, not a random UUID,
/// for whatever identity column/key your destination's upsert needs.
///
/// Partition plus offset names one position in one partition's log for good, so a
/// replayed or redelivered message always gets the same key and two distinct
/// messages never share one. `message.id` is not a safe default: producers can set
/// it, and two different messages with the same id would overwrite each other in the
/// destination. Swap it in only when your producers guarantee unique ids and you want
/// duplicates produced at different offsets collapsed too.
fn dedup_key(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message: &ConsumedMessage,
) -> String {
    format!(
        "{}:{}:{}:{}",
        topic_metadata.stream, topic_metadata.topic, messages_metadata.partition_id, message.offset
    )
}

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

// ── Sink trait ────────────────────────────────────────────────────────────────

#[async_trait]
impl Sink for TemplateSink {
    async fn open(&mut self) -> Result<(), Error> {
        // Structural validation happens here, not in `new()`, because only
        // `open()` can return an error — `new()` is a plain factory function
        // with nowhere to send a "this config is invalid" result.
        if self
            .config
            .connection_string
            .expose_secret()
            .trim()
            .is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "connection_string must not be empty".to_string(),
            ));
        }
        validate_identifier("target", &self.config.target)?;
        if self.cool_down >= self.max_delivery_time {
            return Err(Error::InvalidConfigValue(format!(
                "circuit_breaker_cool_down ({:?}) must be shorter than max_delivery_time ({:?}), \
                 or a batch that meets an open breaker could never be retried",
                self.cool_down, self.max_delivery_time
            )));
        }

        info!(
            "Opening {CONNECTOR_NAME} connector with ID: {}, target: {}, batch_size: {}",
            self.id, self.config.target, self.batch_size_limit
        );

        let raw_client = self.build_raw_client()?;

        if let Some(health_path) = &self.config.health_check_path {
            let base = Url::parse(self.config.connection_string.expose_secret()).map_err(|e| {
                Error::InvalidConfigValue(format!("connection_string is not a valid URL: {e}"))
            })?;
            let health_url = base.join(health_path).map_err(|e| {
                Error::InvalidConfigValue(format!("invalid health_check_path: {e}"))
            })?;
            check_connectivity_with_retry(
                &raw_client,
                health_url,
                CONNECTOR_NAME,
                self.id,
                RetryPolicy {
                    max_attempts: self
                        .config
                        .max_open_retries
                        .unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
                    base_delay: self.retry_delay,
                    max_delay: parse_duration(
                        self.config.open_retry_max_delay.as_deref(),
                        DEFAULT_OPEN_RETRY_MAX_DELAY,
                    ),
                },
            )
            .await?;
        } else {
            warn!(
                "{CONNECTOR_NAME} connector with ID: {} has no health_check_path configured — \
                 skipping the startup connectivity probe. Consider adding one.",
                self.id
            );
        }

        self.client = Some(build_retry_client(
            raw_client,
            self.config
                .max_retries
                .unwrap_or(DEFAULT_MAX_RETRIES)
                .max(1),
            self.retry_delay,
            parse_duration(
                self.config.retry_max_delay.as_deref(),
                DEFAULT_RETRY_MAX_DELAY,
            ),
            CONNECTOR_NAME,
        ));

        info!(
            "{CONNECTOR_NAME} connector with ID: {} opened successfully",
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
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} with ID: {} received: {} messages, schema: {}, stream: {}, \
                 topic: {}, partition: {}, offset: {}, invocation: {}",
                self.id,
                messages.len(),
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset,
                invocation
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} with ID: {} received: {} messages, schema: {}, stream: {}, \
                 topic: {}, partition: {}, offset: {}, invocation: {}",
                self.id,
                messages.len(),
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset,
                invocation
            );
        }

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("client not initialized -- was open() called?".into())
        })?;

        // One budget for the whole call. With a dead destination every chunk would otherwise wait
        // out a budget of its own, and the runtime is blocked on this call for all of them.
        let deadline = Instant::now() + self.max_delivery_time;
        let mut last_err: Option<Error> = None;
        let mut written = 0u64;
        let mut failed = 0u64;

        for (index, chunk) in messages.chunks(self.batch_size_limit).enumerate() {
            let result = self
                .deliver_chunk(deadline, || {
                    self.push_batch(client, topic_metadata, &messages_metadata, chunk)
                })
                .await;
            match result {
                Ok(()) => written += chunk.len() as u64,
                Err(Error::PermanentHttpError(message)) => {
                    failed += chunk.len() as u64;
                    error!(
                        "{CONNECTOR_NAME} connector with ID: {} skipping {} messages \
                         (permanent), offsets {}: {message}",
                        self.id,
                        chunk.len(),
                        offset_range(chunk)
                    );
                }
                // The budget is spent, so the chunks after this one are not attempted either.
                Err(err) if is_transient(&err) => {
                    let undelivered = &messages[index * self.batch_size_limit..];
                    failed += undelivered.len() as u64;
                    error!(
                        "{CONNECTOR_NAME} connector with ID: {} gave up on {} messages after \
                         {:?}, stream: {}, topic: {}, partition: {}, offsets {}. The runtime \
                         does not redeliver them; replay that range from Iggy to recover \
                         them. Cause: {err}",
                        self.id,
                        undelivered.len(),
                        self.max_delivery_time,
                        topic_metadata.stream,
                        topic_metadata.topic,
                        messages_metadata.partition_id,
                        offset_range(undelivered)
                    );
                    last_err = Some(err);
                    break;
                }
                Err(err) => {
                    failed += chunk.len() as u64;
                    error!(
                        "{CONNECTOR_NAME} connector with ID: {} failed {} messages, offsets {}: \
                         {err}",
                        self.id,
                        chunk.len(),
                        offset_range(chunk)
                    );
                    last_err = Some(err);
                }
            }
        }

        let mut state = self.state.lock().await;
        state.messages_written += written;
        state.messages_failed += failed;
        drop(state);
        self.records_written_total
            .fetch_add(written, Ordering::Relaxed);

        match last_err {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "{CONNECTOR_NAME} connector with ID: {} closing. Stats: {} invocations, {} messages written, \
             {} messages failed",
            self.id, state.invocations_count, state.messages_written, state.messages_failed
        );
        drop(state);
        self.client = None;
        info!("{CONNECTOR_NAME} connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TemplateSinkConfig {
        TemplateSinkConfig {
            connection_string: SecretString::from("https://api.example.com"),
            target: "events".to_string(),
            auth_token: None,
            health_check_path: None,
            batch_size: Some(10),
            timeout: Some("5s".to_string()),
            max_retries: Some(2),
            retry_delay: Some("10ms".to_string()),
            retry_max_delay: Some("100ms".to_string()),
            max_open_retries: Some(2),
            open_retry_max_delay: Some("100ms".to_string()),
            circuit_breaker_threshold: Some(3),
            circuit_breaker_cool_down: Some("50ms".to_string()),
            max_delivery_time: Some("1s".to_string()),
            verbose_logging: None,
        }
    }

    fn transient() -> Error {
        Error::HttpRequestFailed("destination unavailable".to_string())
    }

    async fn trip_breaker(sink: &TemplateSink) {
        for _ in 0..3 {
            sink.circuit_breaker.record_failure().await;
        }
        assert!(sink.circuit_breaker.is_open().await);
    }

    #[test]
    fn given_verbose_logging_enabled_should_set_verbose_flag() {
        let mut config = test_config();
        config.verbose_logging = Some(true);
        let sink = TemplateSink::new(1, config);
        assert!(sink.verbose);
    }

    #[test]
    fn given_verbose_logging_disabled_should_not_set_verbose_flag() {
        let sink = TemplateSink::new(1, test_config());
        assert!(!sink.verbose);
    }

    #[tokio::test]
    async fn given_empty_connection_string_should_reject_open() {
        let mut config = test_config();
        config.connection_string = SecretString::from("   ");
        let mut sink = TemplateSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn given_invalid_target_identifier_should_reject_open() {
        let mut config = test_config();
        config.target = "events; DROP TABLE users;--".to_string();
        let mut sink = TemplateSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn validate_identifier_should_accept_plain_names() {
        assert!(validate_identifier("target", "events_v2").is_ok());
    }

    #[test]
    fn validate_identifier_should_reject_empty() {
        assert!(validate_identifier("target", "").is_err());
    }

    #[test]
    fn validate_identifier_should_reject_path_and_query_characters() {
        for bad in [
            "../etc/passwd",
            "events?x=1",
            "events/../secrets",
            "events;drop",
        ] {
            assert!(
                validate_identifier("target", bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn dedup_key_should_join_stream_topic_partition_and_offset() {
        let (topic_metadata, messages_metadata) = topic_and_messages_metadata();
        let message = consumed_message(7);
        assert_eq!(
            dedup_key(&topic_metadata, &messages_metadata, &message),
            "s:t:1:7"
        );
    }

    #[test]
    fn given_messages_sharing_an_id_should_get_distinct_dedup_keys() {
        let (topic_metadata, messages_metadata) = topic_and_messages_metadata();
        let first = consumed_message(1);
        let second = consumed_message(2);
        assert_eq!(first.id, second.id);
        assert_ne!(
            dedup_key(&topic_metadata, &messages_metadata, &first),
            dedup_key(&topic_metadata, &messages_metadata, &second)
        );
    }

    #[test]
    fn given_same_batch_written_twice_should_leave_one_row_per_message() {
        let (topic_metadata, messages_metadata) = topic_and_messages_metadata();
        let batch: Vec<_> = (0..5).map(consumed_message).collect();
        let mut destination = std::collections::HashMap::new();

        for _ in 0..2 {
            for message in &batch {
                destination.insert(
                    dedup_key(&topic_metadata, &messages_metadata, message),
                    message.offset,
                );
            }
        }

        assert_eq!(destination.len(), batch.len());
    }

    #[test]
    fn given_cool_down_not_shorter_than_budget_should_reject_open() {
        let mut config = test_config();
        config.circuit_breaker_cool_down = Some("2s".to_string());
        config.max_delivery_time = Some("2s".to_string());
        let mut sink = TemplateSink::new(1, config);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(matches!(
            runtime.block_on(sink.open()),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn given_destination_recovers_within_budget_should_deliver_after_breaker_opens() {
        let sink = TemplateSink::new(1, test_config());
        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result = sink
            .deliver_chunk(Instant::now() + sink.max_delivery_time, || async {
                // Three failures open the breaker, so the fourth attempt has to wait it out.
                if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                    Err(transient())
                } else {
                    Ok(())
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert!(!sink.circuit_breaker.is_open().await);
    }

    #[tokio::test(start_paused = true)]
    async fn given_destination_down_for_whole_budget_should_give_up_with_bounded_attempts() {
        let mut config = test_config();
        config.circuit_breaker_cool_down = Some("400ms".to_string());
        let sink = TemplateSink::new(1, config);
        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result = sink
            .deliver_chunk(Instant::now() + sink.max_delivery_time, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(transient())
            })
            .await;

        assert!(matches!(result, Err(Error::HttpRequestFailed(_))));
        // The breaker closes completely after each 400 ms cool-down and needs three new
        // failures to open again, and a 1 s budget fits at most three such cycles. Backoff
        // alone would allow about thirteen attempts.
        assert!(attempts.load(Ordering::SeqCst) <= 9);
    }

    #[tokio::test(start_paused = true)]
    async fn given_breaker_open_past_budget_should_give_up_without_calling_destination() {
        let mut config = test_config();
        config.circuit_breaker_cool_down = Some("10s".to_string());
        let sink = TemplateSink::new(1, config);
        trip_breaker(&sink).await;
        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result = sink
            .deliver_chunk(Instant::now() + sink.max_delivery_time, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert!(matches!(result, Err(Error::CannotStoreData(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_permanent_error_should_not_retry_or_open_breaker() {
        let sink = TemplateSink::new(1, test_config());
        let attempts = std::sync::atomic::AtomicU32::new(0);

        for _ in 0..5 {
            let result = sink
                .deliver_chunk(Instant::now() + sink.max_delivery_time, || async {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(Error::PermanentHttpError("bad schema".to_string()))
                })
                .await;
            assert!(matches!(result, Err(Error::PermanentHttpError(_))));
        }

        assert_eq!(attempts.load(Ordering::SeqCst), 5);
        assert!(!sink.circuit_breaker.is_open().await);
    }

    #[tokio::test(start_paused = true)]
    async fn given_unclassified_error_should_not_retry() {
        let sink = TemplateSink::new(1, test_config());
        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result = sink
            .deliver_chunk(Instant::now() + sink.max_delivery_time, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(Error::InitError("not implemented".to_string()))
            })
            .await;

        assert!(matches!(result, Err(Error::InitError(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn given_breaker_open_past_budget_when_consuming_should_count_every_message_failed() {
        let mut config = test_config();
        config.circuit_breaker_cool_down = Some("10s".to_string());
        let mut sink = TemplateSink::new(1, config);
        sink.client = Some(build_retry_client(
            reqwest::Client::new(),
            1,
            sink.retry_delay,
            sink.retry_max_delay,
            CONNECTOR_NAME,
        ));
        trip_breaker(&sink).await;
        let (topic_metadata, messages_metadata) = topic_and_messages_metadata();
        let messages: Vec<_> = (0..25).map(consumed_message).collect();

        let result = sink
            .consume(&topic_metadata, messages_metadata, messages)
            .await;

        assert!(matches!(result, Err(Error::CannotStoreData(_))));
        assert_eq!(sink.state.lock().await.messages_failed, 25);
        assert_eq!(sink.state.lock().await.messages_written, 0);
    }

    fn consumed_message(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: iggy_connector_sdk::Payload::Raw(b"payload".to_vec()),
        }
    }

    fn topic_and_messages_metadata() -> (TopicMetadata, MessagesMetadata) {
        (
            TopicMetadata {
                stream: "s".to_string(),
                topic: "t".to_string(),
            },
            MessagesMetadata {
                partition_id: 1,
                current_offset: 0,
                schema: iggy_connector_sdk::Schema::Json,
            },
        )
    }

    // push_batch() is an unimplemented TODO(ConnectorDeveloper) stub that
    // always returns Error::InitError. That is neither transient nor a
    // PermanentHttpError, so a non-empty consume() call exercises the
    // "unclassified error" branch of the chunking loop end to end: the chunk
    // is counted as failed, not retried, and the error is returned.
    #[tokio::test]
    async fn given_unimplemented_push_batch_should_return_err_and_count_failure() {
        let mut sink = TemplateSink::new(1, test_config());
        sink.open().await.expect("open should succeed");
        let (topic_metadata, messages_metadata) = topic_and_messages_metadata();

        let result = sink
            .consume(
                &topic_metadata,
                messages_metadata,
                vec![consumed_message(0)],
            )
            .await;

        assert!(matches!(result, Err(Error::InitError(_))));
        assert_eq!(sink.state.lock().await.messages_failed, 1);
        assert_eq!(sink.state.lock().await.messages_written, 0);
    }
}
