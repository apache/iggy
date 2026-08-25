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

//! Template Apache Iggy **source** connector.
//!
//! Everything in this file is already wired up and follows the patterns the
//! connector-review checklist expects: config validation happens in
//! `open()`, secrets are `SecretString`, outbound calls go through a
//! retry-wrapped client plus a circuit breaker, and the read cursor is
//! staged in `poll()` and only committed in `on_batch_result()` on an ACK so
//! a dropped/nacked batch can be re-polled instead of silently lost.
//!
//! There are exactly **two** places you need to touch, each marked
//! `TODO(ConnectorDeveloper)`:
//!   1. `TemplateSource::connect()`  — build your actual client/connection
//!      from `config.connection_string` (and `config.auth_token`, if used).
//!   2. `TemplateSource::fetch_records()` — fetch up to `batch_size` new
//!      records from your external system, starting after `cursor`.
//!
//! This template assumes an HTTP-ish source and uses `reqwest` wrapped by
//! the SDK's retry middleware, because that's what `iggy_connector_sdk::retry`
//! is built for and it covers the common case. If your source talks to
//! something else (a database, a queue, a filesystem), swap the client type
//! in `connect()`/`fetch_records()` for your driver of choice and lean on
//! its own retry/pooling behavior — keep the surrounding shape (validation
//! in `open()`, circuit breaker, cursor staging, batching) unchanged. See
//! `core/connectors/sources/postgres_source` in this repo for a real
//! non-HTTP example of that same shape.

use async_trait::async_trait;
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

source_connector!(TemplateSource);

const CONNECTOR_NAME: &str = "Template source";

const DEFAULT_POLL_INTERVAL: &str = "1s";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
const DEFAULT_CIRCUIT_BREAKER_COOL_DOWN: &str = "30s";

// ── Configuration ───────────────────────────────────────────────────────────
//
// Every tunable except `connection_string` is optional with a sane default,
// and unknown keys are rejected outright so a typo in a TOML file fails at
// load time instead of silently doing nothing.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateSourceConfig {
    /// TODO(ConnectorDeveloper): document the exact shape this connector expects, e.g.
    /// "https://api.example.com" or "postgres://user:pass@host:5432/db".
    /// `SecretString` because DSNs commonly embed credentials — never plain
    /// `String` for this field, see `PostgresSinkConfig::connection_string`
    /// in `sinks/postgres_sink` for the same pattern.
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,

    /// Example of a secret-shaped setting. `SecretString` keeps it out of
    /// `Debug`/log output; delete this field if `connection_string` already
    /// carries all required auth. Read it with `.expose_secret()` (from the
    /// `secrecy::ExposeSecret` trait) at the one place you actually need the
    /// plaintext — e.g. when building an auth header in `connect()`.
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub auth_token: Option<SecretString>,

    /// Optional path (e.g. "/health") probed with retry during `open()`
    /// before the connector is considered ready. Leave unset if your target
    /// has no health endpoint — the probe is skipped, not failed, in that case.
    pub health_check_path: Option<String>,

    pub batch_size: Option<u32>,
    pub poll_interval: Option<String>,
    pub timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

// ── Internal state ──────────────────────────────────────────────────────────

/// Read cursor. Kept as a plain `Option<String>` so it fits whatever ordering
/// field your source uses (a timestamp, an auto-increment ID, an opaque
/// pagination token, ...) — stringify it however makes sense for your data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct State {
    cursor: Option<String>,
}

/// One record as fetched from the external system, before it's turned into
/// an Iggy message. Replace the `payload` type with whatever your
/// `fetch_records()` actually produces.
struct FetchedRecord {
    /// The value `cursor` should advance to once this record's batch is
    /// acknowledged — typically this record's timestamp/ID/token.
    cursor_value: String,
    payload: serde_json::Value,
}

#[derive(Debug)]
pub struct TemplateSource {
    id: u32,
    config: TemplateSourceConfig,
    client: Option<ClientWithMiddleware>,
    circuit_breaker: Arc<CircuitBreaker>,
    batch_size: u32,
    poll_interval: Duration,
    retry_delay: Duration,
    state: Mutex<State>,
    pending_state: Mutex<Option<State>>,
    records_produced: AtomicU64,
}

impl TemplateSource {
    pub fn new(id: u32, config: TemplateSourceConfig, state: Option<ConnectorState>) -> Self {
        let poll_interval = *humantime::Duration::from_str_lossy(
            config
                .poll_interval
                .as_deref()
                .unwrap_or(DEFAULT_POLL_INTERVAL),
        );
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config
                .circuit_breaker_threshold
                .unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
            parse_duration(
                config.circuit_breaker_cool_down.as_deref(),
                DEFAULT_CIRCUIT_BREAKER_COOL_DOWN,
            ),
        ));
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. Cursor: {:?}",
                    s.cursor
                );
            });

        Self {
            id,
            config,
            client: None,
            circuit_breaker,
            batch_size,
            poll_interval,
            retry_delay,
            state: Mutex::new(restored_state.unwrap_or_default()),
            pending_state: Mutex::new(None),
            records_produced: AtomicU64::new(0),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    /// TODO(ConnectorDeveloper): build your actual client/connection here using
    /// `self.config.connection_string` (and `self.config.auth_token`, if
    /// your system needs it). This template builds a plain `reqwest::Client`
    /// to hand to `build_retry_client` — if you're not talking HTTP, replace
    /// this with e.g. a `sqlx::PgPool::connect(...)` or your driver's
    /// equivalent, store it on `self` (add a field, since `client` here is
    /// HTTP-specific), and skip the `check_connectivity_with_retry` call
    /// below in favor of whatever your driver offers (a ping, a test query).
    fn build_raw_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::Connection(format!("failed to build HTTP client: {e}")))
    }

    /// TODO(ConnectorDeveloper): fetch up to `self.batch_size` new records from your
    /// external system, ordered after `cursor` (`None` means "from the
    /// beginning" or "from now" — whichever is right for your source).
    /// Use `self.config.connection_string` as the base address and
    /// `self.client` (already retry-wrapped) to make the request. Map each
    /// result row/document/event to a `FetchedRecord`, using something that
    /// monotonically increases (a timestamp, an ID, a page token) as
    /// `cursor_value` so the state-staging logic in `poll()` below can
    /// advance the cursor correctly.
    async fn fetch_records(
        &self,
        client: &ClientWithMiddleware,
        cursor: Option<&str>,
    ) -> Result<Vec<FetchedRecord>, Error> {
        let _ = (client, cursor); // remove once implemented
        Err(Error::InitError(
            "TemplateSource::fetch_records is not implemented yet — see the TODO(ConnectorDeveloper) comment \
             in template_source/src/lib.rs"
                .to_string(),
        ))
    }
}

// ── Source trait ────────────────────────────────────────────────────────────

#[async_trait]
impl Source for TemplateSource {
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

        info!(
            "Opening {CONNECTOR_NAME} connector with ID: {}, batch_size: {}, poll_interval: {:?}",
            self.id, self.batch_size, self.poll_interval
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
                &ConnectivityConfig {
                    max_open_retries: self
                        .config
                        .max_open_retries
                        .unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
                    open_retry_max_delay: parse_duration(
                        self.config.open_retry_max_delay.as_deref(),
                        DEFAULT_OPEN_RETRY_MAX_DELAY,
                    ),
                    retry_delay: self.retry_delay,
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

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // If the breaker is open, sleep for the normal poll interval and
        // return an empty (not an error) result. Returning `Err` here would
        // make the runtime retry `poll()` again immediately with no delay —
        // see `handle_messages` in the SDK's source container — so an empty
        // ACK-free result is how a source waits out a known-bad window
        // without busy-looping or counting against the NACK budget.
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} connector with ID: {} — circuit breaker OPEN, skipping poll",
                self.id
            );
            tokio::time::sleep(self.poll_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: Vec::new(),
                state: None,
            });
        }
        tokio::time::sleep(self.poll_interval).await;

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("client not initialized -- was open() called?".into())
        })?;
        let cursor = self.state.lock().await.cursor.clone();

        let records = match self.fetch_records(client, cursor.as_deref()).await {
            Ok(records) => {
                self.circuit_breaker.record_success();
                records
            }
            Err(err) => {
                if !matches!(err, Error::PermanentHttpError(_)) {
                    self.circuit_breaker.record_failure().await;
                }
                return Err(err);
            }
        };

        if records.is_empty() {
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: Vec::new(),
                state: None,
            });
        }

        let mut messages = Vec::with_capacity(records.len());
        let mut new_cursor = cursor;
        for record in records {
            new_cursor = Some(record.cursor_value);
            let Ok(payload) = serde_json::to_vec(&record.payload) else {
                error!(
                    "Failed to serialize a record fetched by {CONNECTOR_NAME} connector with ID: {}",
                    self.id
                );
                continue;
            };
            messages.push(ProducedMessage {
                id: None,
                headers: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                payload,
            });
        }

        let candidate_state = State { cursor: new_cursor };
        let persisted_state = self.serialize_state(&candidate_state).ok_or_else(|| {
            Error::Serialization(format!(
                "failed to serialize state for {CONNECTOR_NAME} connector with ID: {}",
                self.id
            ))
        })?;
        *self.pending_state.lock().await = Some(candidate_state);

        self.records_produced
            .fetch_add(messages.len() as u64, Ordering::Relaxed);

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(persisted_state),
        })
    }

    /// The staged cursor from `poll()` is only committed here, and only on
    /// an ACK. A NACK (delivery failed, batch timed out, runtime is
    /// shutting down) discards the candidate so the same range is re-polled
    /// next time — the cursor never moves past data that wasn't confirmed
    /// delivered. If `fetch_records()` ever needs to perform a destructive
    /// read against the source (delete-after-read, mark-as-processed),
    /// stage that side effect the same way and only apply it here on `Ack`.
    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate_state = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate_state) = candidate_state
        {
            *self.state.lock().await = candidate_state;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "{CONNECTOR_NAME} connector with ID: {} closed. Cursor: {:?}, total records produced: {}",
            self.id,
            state.cursor,
            self.records_produced.load(Ordering::Relaxed)
        );
        drop(state);
        self.client = None;
        Ok(())
    }
}

// Small local shim so `new()` doesn't need to pull in `humantime::Duration`'s
// `FromStr` (which returns `Result`) just to apply a default — mirrors the
// fallback-with-warning behavior of `iggy_connector_sdk::retry::parse_duration`
// for the one duration field (`poll_interval`) that isn't itself optional in
// spirit (there's always a poll interval, just maybe the default one).
trait DurationExt {
    fn from_str_lossy(s: &str) -> humantime::Duration;
}
impl DurationExt for humantime::Duration {
    fn from_str_lossy(s: &str) -> humantime::Duration {
        use std::str::FromStr;
        humantime::Duration::from_str(s).unwrap_or_else(|_| {
            warn!("Invalid poll_interval {s:?}, falling back to {DEFAULT_POLL_INTERVAL}");
            humantime::Duration::from_str(DEFAULT_POLL_INTERVAL)
                .expect("DEFAULT_POLL_INTERVAL must itself be a valid duration literal")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> TemplateSourceConfig {
        TemplateSourceConfig {
            connection_string: SecretString::from("https://api.example.com"),
            auth_token: None,
            health_check_path: None,
            batch_size: Some(50),
            poll_interval: Some("50ms".to_string()),
            timeout: Some("5s".to_string()),
            max_retries: Some(2),
            retry_delay: Some("10ms".to_string()),
            retry_max_delay: Some("100ms".to_string()),
            max_open_retries: Some(2),
            open_retry_max_delay: Some("100ms".to_string()),
            circuit_breaker_threshold: Some(3),
            circuit_breaker_cool_down: Some("50ms".to_string()),
        }
    }

    #[tokio::test]
    async fn open_rejects_empty_connection_string() {
        let mut config = test_config();
        config.connection_string = SecretString::from("   ");
        let mut source = TemplateSource::new(1, config, None);
        let result = source.open().await;
        assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
    }

    #[tokio::test]
    async fn given_no_state_should_start_with_no_cursor() {
        let source = TemplateSource::new(1, test_config(), None);
        assert_eq!(source.state.lock().await.cursor, None);
    }

    #[tokio::test]
    async fn given_persisted_state_should_restore_cursor() {
        let state = State {
            cursor: Some("2024-01-01T00:00:00Z".to_string()),
        };
        let serialized = rmp_serde::to_vec(&state).expect("failed to serialize state");
        let source = TemplateSource::new(1, test_config(), Some(ConnectorState(serialized)));
        assert_eq!(
            source.state.lock().await.cursor,
            Some("2024-01-01T00:00:00Z".to_string())
        );
    }

    #[tokio::test]
    async fn given_invalid_persisted_state_should_start_fresh() {
        let source = TemplateSource::new(
            1,
            test_config(),
            Some(ConnectorState(b"not valid msgpack".to_vec())),
        );
        assert_eq!(source.state.lock().await.cursor, None);
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            cursor: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let serialized = rmp_serde::to_vec(&original).expect("failed to serialize state");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("failed to deserialize state");

        assert_eq!(original.cursor, deserialized.cursor);
    }

    #[tokio::test]
    async fn given_ack_should_commit_staged_cursor() {
        let source = TemplateSource::new(1, test_config(), None);
        *source.pending_state.lock().await = Some(State {
            cursor: Some("next-cursor".to_string()),
        });

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ACK should be applied");

        assert_eq!(
            source.state.lock().await.cursor,
            Some("next-cursor".to_string())
        );
        assert!(source.pending_state.lock().await.is_none());
    }

    #[tokio::test]
    async fn given_nack_should_discard_staged_cursor() {
        let source = TemplateSource::new(1, test_config(), None);
        *source.pending_state.lock().await = Some(State {
            cursor: Some("next-cursor".to_string()),
        });

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("NACK should be applied");

        // The committed cursor is unchanged (still None) — the candidate is
        // simply discarded so the same range is polled again.
        assert_eq!(source.state.lock().await.cursor, None);
        assert!(source.pending_state.lock().await.is_none());
    }

    #[tokio::test]
    async fn poll_returns_empty_without_error_when_circuit_is_open() {
        let source = TemplateSource::new(1, test_config(), None);
        source.circuit_breaker.record_failure().await;
        source.circuit_breaker.record_failure().await;
        source.circuit_breaker.record_failure().await;
        assert!(source.circuit_breaker.is_open().await);

        let result = source
            .poll()
            .await
            .expect("open breaker should not error poll()");
        assert!(result.messages.is_empty());
        assert!(result.state.is_none());
    }
}
