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
//! There is exactly **one** place you need to touch, marked `TODO(ConnectorDeveloper)`:
//!   `TemplateSink::push_batch()` — build the request/write that actually
//!   pushes one chunk of messages to your destination, using
//!   `self.config.connection_string` (and `self.config.target`, if your
//!   destination has a table/index/collection-shaped name).
//!
//! This template assumes an HTTP-ish destination and uses `reqwest` wrapped
//! by the SDK's retry middleware, because that's what
//! `iggy_connector_sdk::retry` is built for and it covers the common case.
//! If your destination talks something else (a database, a queue, object
//! storage), swap the client type in `connect()`/`push_batch()` for your
//! driver of choice and lean on its own retry/pooling behavior — keep the
//! surrounding shape (validation in `open()`, circuit breaker, batching,
//! identifier validation) unchanged. See `core/connectors/sinks/postgres_sink`
//! or `core/connectors/sinks/s3_sink` in this repo for non-HTTP examples of
//! that same shape.

use async_trait::async_trait;
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
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

sink_connector!(TemplateSink);

const CONNECTOR_NAME: &str = "Template sink";

const DEFAULT_BATCH_SIZE: usize = 100;
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
// Every tunable except `connection_string` and `target` is optional with a
// sane default, and unknown keys are rejected outright so a typo in a TOML
// file fails at load time instead of silently doing nothing.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateSinkConfig {
    /// TODO(ConnectorDeveloper): document the exact shape this connector expects, e.g.
    /// "https://api.example.com" or "postgres://user:pass@host:5432/db".
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

    pub batch_size: Option<usize>,
    pub timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
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
    state: Mutex<State>,
    records_written_total: AtomicU64,
}

impl TemplateSink {
    pub fn new(id: u32, config: TemplateSinkConfig) -> Self {
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
        let batch_size_limit = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);

        Self {
            id,
            config,
            client: None,
            circuit_breaker,
            batch_size_limit,
            retry_delay,
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
    /// Distinguish permanent failures (bad schema, destination rejects the
    /// payload shape — will not succeed on retry) from transient ones
    /// (network error, 5xx, timeout — should retry) by returning
    /// `Error::PermanentHttpError` for the former; see the doc comment on
    /// that variant in `iggy_connector_sdk::Error` for why the distinction
    /// matters to the circuit breaker.
    async fn push_batch(
        &self,
        client: &ClientWithMiddleware,
        batch: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let _ = (client, batch); // remove once implemented
        Err(Error::InitError(
            "TemplateSink::push_batch is not implemented yet — see the TODO(ConnectorDeveloper) comment in \
             template_sink/src/lib.rs"
                .to_string(),
        ))
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

        info!(
            "{CONNECTOR_NAME} with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, \
             partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );

        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} connector with ID: {} — circuit breaker OPEN, refusing {} messages",
                self.id,
                messages.len()
            );
            return Err(Error::CannotStoreData(
                "Circuit breaker is open".to_string(),
            ));
        }

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("client not initialized -- was open() called?".into())
        })?;

        let mut first_error: Option<Error> = None;
        let mut written = 0u64;
        let mut failed = 0u64;

        for batch in messages.chunks(self.batch_size_limit) {
            match self.push_batch(client, batch).await {
                Ok(()) => written += batch.len() as u64,
                Err(err) => {
                    failed += batch.len() as u64;
                    error!(
                        "{CONNECTOR_NAME} connector with ID: {} failed a batch of {}: {err}",
                        self.id,
                        batch.len()
                    );
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        // Record the circuit breaker outcome once per `consume()` call, not
        // once per chunk — recording success partway through would reset
        // the failure counter mid-consume and prevent the breaker from
        // opening on a batch with sustained, mixed-success chunks.
        match &first_error {
            None => self.circuit_breaker.record_success(),
            Some(e) if !matches!(e, Error::PermanentHttpError(_)) => {
                self.circuit_breaker.record_failure().await;
            }
            Some(_) => {}
        }

        let mut state = self.state.lock().await;
        state.messages_written += written;
        state.messages_failed += failed;
        drop(state);
        self.records_written_total
            .fetch_add(written, Ordering::Relaxed);

        match first_error {
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
        }
    }

    #[tokio::test]
    async fn open_rejects_empty_connection_string() {
        let mut config = test_config();
        config.connection_string = SecretString::from("   ");
        let mut sink = TemplateSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn open_rejects_invalid_target_identifier() {
        let mut config = test_config();
        config.target = "events; DROP TABLE users;--".to_string();
        let mut sink = TemplateSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn validate_identifier_accepts_plain_names() {
        assert!(validate_identifier("target", "events_v2").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_empty() {
        assert!(validate_identifier("target", "").is_err());
    }

    #[test]
    fn validate_identifier_rejects_path_and_query_characters() {
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

    #[tokio::test]
    async fn consume_short_circuits_when_breaker_is_open() {
        let sink = TemplateSink::new(1, test_config());
        sink.circuit_breaker.record_failure().await;
        sink.circuit_breaker.record_failure().await;
        sink.circuit_breaker.record_failure().await;
        assert!(sink.circuit_breaker.is_open().await);

        let topic_metadata = TopicMetadata {
            stream: "s".to_string(),
            topic: "t".to_string(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Json,
        };

        let result = sink
            .consume(&topic_metadata, messages_metadata, Vec::new())
            .await;
        assert!(matches!(result, Err(Error::CannotStoreData(_))));
    }
}
