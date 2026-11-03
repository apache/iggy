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
use iggy_common::{DateTime, HeaderKey, HeaderValue, Utc};
use iggy_connector_sdk::retry::{CircuitBreaker, RetryPolicy, parse_duration, retry_async};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use opensearch::{
    OpenSearch, SearchParts,
    auth::Credentials,
    http::{StatusCode, Url, response::Response, transport::TransportBuilder},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

mod retry;
use crate::retry::{
    DEFAULT_CB_COOL_DOWN, DEFAULT_CB_THRESHOLD, DEFAULT_MAX_OPEN_RETRIES, DEFAULT_MAX_RETRIES,
    DEFAULT_OPEN_RETRY_MAX_DELAY, DEFAULT_RETRY_DELAY, DEFAULT_RETRY_MAX_DELAY, RequestFailure,
};

source_connector!(OpenSearchSource);

const CONNECTOR_NAME: &str = "OpenSearch source";
const DEFAULT_POLLING_INTERVAL: &str = "10s";
const DEFAULT_BATCH_SIZE: usize = 100;
// OpenSearch rejects `size` above the default `index.max_result_window` with HTTP 400.
const MAX_BATCH_SIZE: usize = 10_000;
const DOCUMENT_ID_HEADER: &str = "opensearch.document_id";
const INDEX_HEADER: &str = "opensearch.index";
const MAX_HEADER_VALUE_LENGTH: usize = 255;

static DOCUMENT_ID_HEADER_KEY: std::sync::LazyLock<HeaderKey> = std::sync::LazyLock::new(|| {
    HeaderKey::try_from(DOCUMENT_ID_HEADER).expect("document id header key is a valid literal")
});
static INDEX_HEADER_KEY: std::sync::LazyLock<HeaderKey> = std::sync::LazyLock::new(|| {
    HeaderKey::try_from(INDEX_HEADER).expect("index header key is a valid literal")
});

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    last_poll_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    total_documents_published: usize,
    #[serde(default)]
    poll_count: usize,
    /// OpenSearch `search_after` tuple from the last hit in the previous batch.
    #[serde(default)]
    search_after: Option<Vec<Value>>,
    #[serde(default)]
    error_count: usize,
    #[serde(default)]
    last_error: Option<String>,
    #[serde(default)]
    processing_stats: ProcessingStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProcessingStats {
    #[serde(default)]
    total_bytes_processed: u64,
    /// Running cumulative average over the connector's lifetime, persisted and accumulated
    /// across restarts. Reflects long-term throughput baseline, not session-only average.
    #[serde(default)]
    avg_batch_processing_time_ms: f64,
    #[serde(default)]
    last_successful_poll: Option<DateTime<Utc>>,
    #[serde(default)]
    empty_polls_count: usize,
    #[serde(default)]
    successful_polls_count: usize,
}

// `Deserialize` only: nothing re-serializes a plugin config, and omitting `Serialize`
// keeps `password` from ever being written out as plaintext.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
    #[serde(default)]
    pub verbose_logging: bool,
    /// Total poll-phase attempt count (not retry count); 1 = no retries, 3 = 2 retries.
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    /// Total open-phase attempt count (not retry count); 1 = no retries, 5 = 4 retries.
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug)]
pub struct OpenSearchSource {
    id: u32,
    config: OpenSearchSourceConfig,
    client: Option<OpenSearch>,
    polling_interval: Duration,
    /// Pre-built query body (query + size + sort). Set once in `open()` after config validation.
    /// `None` before `open()` and after a failed `open()`.
    search_body_base: Option<Value>,
    verbose: bool,
    poll_retry_policy: RetryPolicy,
    open_retry_policy: RetryPolicy,
    circuit_breaker: Arc<CircuitBreaker>,
    state: Mutex<State>,
    /// Candidate state of the batch in flight; committed on ACK, dropped on NACK.
    pending_state: Mutex<Option<State>>,
    /// `Some(cause)` when runtime state restore was rejected; `None` means restore succeeded.
    state_restore_error: Option<String>,
}

struct SearchOutcome {
    messages: Vec<ProducedMessage>,
    search_after: Option<Vec<Value>>,
    last_poll_timestamp: Option<DateTime<Utc>>,
    batch_bytes: u64,
}

impl OpenSearchSource {
    pub fn new(id: u32, config: OpenSearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval =
            parse_duration(config.polling_interval.as_deref(), DEFAULT_POLLING_INTERVAL);
        let verbose = config.verbose_logging;
        let (restored_state, state_restore_error) = restore_state(id, state);

        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CB_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CB_COOL_DOWN,
        );
        let circuit_breaker = Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down));
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let poll_retry_policy = RetryPolicy {
            max_attempts: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            base_delay: retry_delay,
            max_delay: parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY),
        };
        // The open phase has no base delay of its own: it shares `retry_delay` and only
        // `open_retry_max_delay` caps the growth independently.
        let open_retry_policy = RetryPolicy {
            max_attempts: config.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
            base_delay: retry_delay,
            max_delay: parse_duration(
                config.open_retry_max_delay.as_deref(),
                DEFAULT_OPEN_RETRY_MAX_DELAY,
            ),
        };

        OpenSearchSource {
            id,
            config,
            poll_retry_policy,
            open_retry_policy,
            circuit_breaker,
            client: None,
            polling_interval,
            search_body_base: None,
            verbose,
            state: Mutex::new(restored_state),
            pending_state: Mutex::new(None),
            state_restore_error,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE)
    }

    fn timestamp_field(&self) -> Result<&str, Error> {
        self.config
            .timestamp_field
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidConfigValue("timestamp_field is required".to_string()))
    }

    async fn create_client(&self) -> Result<OpenSearch, Error> {
        let url = Url::parse(&self.config.url).map_err(|error| {
            Error::InvalidConfigValue(format!("Invalid OpenSearch URL: {error}"))
        })?;

        let conn_pool = opensearch::http::transport::SingleNodeConnectionPool::new(url);
        let mut transport_builder = TransportBuilder::new(conn_pool);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|error| Error::InitError(format!("Failed to build transport: {error}")))?;

        Ok(OpenSearch::new(transport))
    }

    async fn check_index_exists_with_retry(&self, client: &OpenSearch) -> Result<(), Error> {
        let index = &self.config.index;
        retry_async(
            self.open_retry_policy,
            &format!("{CONNECTOR_NAME} ID {} index exists check", self.id),
            RequestFailure::is_transient,
            || async {
                let response = client
                    .indices()
                    .exists(opensearch::indices::IndicesExistsParts::Index(&[index]))
                    .send()
                    .await
                    .map_err(RequestFailure::transport)?;
                RequestFailure::check(response).await.map(drop)
            },
        )
        .await
        .map_err(|failure| match failure.into_error() {
            RequestFailure::Transport(message) => {
                Error::InitError(format!("Failed to check index existence: {message}"))
            }
            RequestFailure::Status { status, .. } if status == StatusCode::NOT_FOUND => {
                Error::InitError(format!(
                    "Index '{index}' does not exist or is not accessible"
                ))
            }
            failure @ RequestFailure::Status { .. } => {
                Error::InitError(format!("Index '{index}' check failed: {failure}"))
            }
        })
    }

    async fn send_search_with_retry(
        &self,
        client: &OpenSearch,
        search_body: &Value,
    ) -> Result<Response, Error> {
        retry_async(
            self.poll_retry_policy,
            &format!("{CONNECTOR_NAME} ID {} search", self.id),
            RequestFailure::is_transient,
            || async {
                let response = client
                    .search(SearchParts::Index(&[&self.config.index]))
                    .body(search_body)
                    .send()
                    .await
                    .map_err(RequestFailure::transport)?;
                RequestFailure::check(response).await
            },
        )
        .await
        .map_err(|failure| failure.into_error().into_search_error())
    }

    async fn search_documents(&self, client: &OpenSearch) -> Result<SearchOutcome, Error> {
        let state = self.state.lock().await;
        let search_after = state.search_after.clone();
        drop(state);

        let timestamp_field = self.timestamp_field()?;

        let mut search_body = self
            .search_body_base
            .as_ref()
            .ok_or_else(|| {
                Error::Connection("connector not initialized; call open() first".to_string())
            })?
            .clone();

        if let Some(cursor) = search_after {
            search_body["search_after"] = json!(cursor);
        }

        let response = self.send_search_with_retry(client, &search_body).await?;

        let mut response_body: Value = response.json().await.map_err(|e| {
            Error::PermanentHttpError(format!("Failed to parse search response: {e}"))
        })?;

        let hits: Vec<Value> = response_body
            .get_mut("hits")
            .and_then(|h| h.get_mut("hits"))
            .and_then(|arr| arr.as_array_mut())
            .map(std::mem::take)
            .unwrap_or_default();

        let mut messages = Vec::with_capacity(hits.len());
        let mut batch_bytes = 0u64;
        let mut last_sort = None;
        let mut last_poll_timestamp = None;

        for hit in &hits {
            let Some(sort) = hit
                .get("sort")
                .and_then(|s| s.as_array())
                .filter(|a| !a.is_empty())
            else {
                return Err(Error::SchemaMismatch(format!(
                    "OpenSearch hit {:?} has no sort tuple; the cursor cannot move past it \
                     without risking skipped documents",
                    hit.get("_id").and_then(Value::as_str)
                )));
            };

            let Some(source) = hit.get("_source") else {
                warn!(
                    connector_id = self.id,
                    hit_id = hit.get("_id").and_then(|v| v.as_str()),
                    hit_index = hit.get("_index").and_then(|v| v.as_str()),
                    "Skipping OpenSearch hit without _source; it has no body to publish"
                );
                last_sort = Some(sort);
                continue;
            };

            let document_timestamp = source
                .get(timestamp_field)
                .and_then(parse_document_timestamp);
            if let Some(timestamp_utc) = document_timestamp {
                last_poll_timestamp = Some(timestamp_utc);
            }

            let payload = serde_json::to_vec(source)
                .map_err(|e| Error::Serialization(format!("Failed to serialize document: {e}")))?;
            batch_bytes += payload.len() as u64;

            let document_id = hit.get("_id").and_then(Value::as_str);
            // `index` may be a pattern, so the concrete index of each hit keeps equal `_id`s
            // in different indices from sharing a message id.
            let reported_index = hit.get("_index").and_then(Value::as_str);
            let hit_index = reported_index.unwrap_or(&self.config.index);
            messages.push(ProducedMessage {
                id: document_id.map(|document_id| message_id(hit_index, document_id)),
                headers: document_headers(self.id, reported_index, document_id),
                checksum: None,
                timestamp: None,
                origin_timestamp: document_timestamp
                    .and_then(|timestamp| u64::try_from(timestamp.timestamp_micros()).ok()),
                payload,
            });
            // Cursor advances only after the document is queued for publish.
            last_sort = Some(sort);
        }

        Ok(SearchOutcome {
            messages,
            search_after: last_sort.map(ToOwned::to_owned),
            last_poll_timestamp,
            batch_bytes,
        })
    }

    async fn finalize_poll(
        &self,
        outcome: SearchOutcome,
        processing_time_ms: f64,
    ) -> Result<(Vec<ProducedMessage>, Option<ConnectorState>), Error> {
        let mut state = self.state.lock().await.clone();
        state.total_documents_published += outcome.messages.len();
        state.poll_count += 1;
        if let Some(cursor) = outcome.search_after {
            state.search_after = Some(cursor);
        }
        if let Some(timestamp) = outcome.last_poll_timestamp {
            state.last_poll_timestamp = Some(timestamp);
        }
        state.processing_stats.total_bytes_processed += outcome.batch_bytes;

        if outcome.messages.is_empty() {
            state.processing_stats.empty_polls_count += 1;
        } else {
            state.processing_stats.successful_polls_count += 1;
            state.processing_stats.last_successful_poll = Some(Utc::now());
        }

        let total_polls = state.processing_stats.successful_polls_count
            + state.processing_stats.empty_polls_count;
        // total_polls >= 1 because one of the two counters was just incremented above,
        // but use saturating_sub to make the invariant machine-checked.
        state.processing_stats.avg_batch_processing_time_ms =
            (state.processing_stats.avg_batch_processing_time_ms
                * total_polls.saturating_sub(1) as f64
                + processing_time_ms)
                / total_polls as f64;

        let produced_count = outcome.messages.len();
        let total_documents_published = state.total_documents_published;
        let messages = outcome.messages;
        // An empty batch moves no cursor, so nothing can be lost by committing its
        // counters directly and there is no batch result to wait for.
        let persisted_state = if messages.is_empty() {
            *self.state.lock().await = state;
            None
        } else {
            let persisted = self.serialize_state(&state).ok_or_else(|| {
                Error::Serialization("failed to serialize source state".to_string())
            })?;
            *self.pending_state.lock().await = Some(state);
            Some(persisted)
        };

        if self.verbose {
            info!(
                "OpenSearch source connector ID: {} produced {produced_count} messages. \
                 Total published: {total_documents_published}",
                self.id
            );
        } else {
            debug!(
                "OpenSearch source connector ID: {} produced {produced_count} messages. \
                 Total published: {total_documents_published}",
                self.id
            );
        }

        Ok((messages, persisted_state))
    }

    #[cfg(test)]
    fn client_initialized(&self) -> bool {
        self.client.is_some()
    }

    #[cfg(test)]
    async fn test_metrics(&self) -> (usize, usize, usize, usize) {
        let state = self.state.lock().await;
        (
            state.total_documents_published,
            state.poll_count,
            state.error_count,
            state.processing_stats.empty_polls_count,
        )
    }

    #[cfg(test)]
    async fn test_search_after(&self) -> Option<Vec<Value>> {
        self.state.lock().await.search_after.clone()
    }

    #[cfg(test)]
    async fn test_last_poll_timestamp(&self) -> Option<DateTime<Utc>> {
        self.state.lock().await.last_poll_timestamp
    }

    async fn handle_poll_error(&self, error: Error) -> Result<ProducedMessages, Error> {
        self.circuit_breaker.record_failure().await;
        let mut state = self.state.lock().await;
        // error_count and last_error accumulate in State and are captured by the next
        // finalize_poll call (success path). Both runtime ConnectorState and file state
        // preserve these values across restarts.
        state.error_count += 1;
        state.last_error = Some(error.to_string());
        drop(state);
        error!(
            "{CONNECTOR_NAME} connector ID: {} poll failed: {error}",
            self.id
        );
        Err(error)
    }
}

fn restore_state(id: u32, state: Option<ConnectorState>) -> (State, Option<String>) {
    let Some(connector_state) = state else {
        return (State::default(), None);
    };

    match connector_state.deserialize::<State>(CONNECTOR_NAME, id) {
        Some(restored) => {
            info!(
                "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                 Documents published: {}, poll count: {}",
                restored.total_documents_published, restored.poll_count
            );
            (restored, None)
        }
        None => {
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (State::default(), Some(cause))
        }
    }
}

fn validate_open_config(config: &OpenSearchSourceConfig) -> Result<(), Error> {
    if config.timestamp_field.as_deref().is_none_or(str::is_empty) {
        return Err(Error::InvalidConfigValue(
            "timestamp_field is required for incremental OpenSearch polling".to_string(),
        ));
    }

    if let Some(batch_size) = config.batch_size
        && !(1..=MAX_BATCH_SIZE).contains(&batch_size)
    {
        return Err(Error::InvalidConfigValue(format!(
            "batch_size must be between 1 and {MAX_BATCH_SIZE}, got {batch_size}"
        )));
    }

    Ok(())
}

/// Strips credentials and query from a URL before it reaches a log line, since a URL can
/// carry `user:password@` or an access token.
fn redacted_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.to_string()
        }
        Err(_) => "<invalid url>".to_string(),
    }
}

/// Stable 128-bit message id so a consumer can drop a document replayed after a NACK or a
/// lost state save. Length-prefixed parts keep distinct (index, id) pairs from aliasing.
fn message_id(index: &str, document_id: &str) -> u128 {
    let mut hasher = blake3::Hasher::new();
    for part in [index, document_id] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let mut id = [0u8; 16];
    hasher.finalize_xof().fill(&mut id);
    u128::from_le_bytes(id)
}

/// The hashed message id cannot be mapped back to the document, so the original `_id` and
/// concrete `_index` travel as headers. A value over the header limit is omitted, the message
/// id remains.
fn document_headers(
    connector_id: u32,
    index: Option<&str>,
    document_id: Option<&str>,
) -> Option<BTreeMap<HeaderKey, HeaderValue>> {
    let mut headers = BTreeMap::new();
    for (key, name, value) in [
        (&*INDEX_HEADER_KEY, INDEX_HEADER, index),
        (&*DOCUMENT_ID_HEADER_KEY, DOCUMENT_ID_HEADER, document_id),
    ] {
        let Some(value) = value else { continue };
        if value.len() > MAX_HEADER_VALUE_LENGTH {
            warn!(
                "{CONNECTOR_NAME} connector ID: {connector_id} omitted the {name} header: \
                 value is {} bytes, limit is {MAX_HEADER_VALUE_LENGTH}",
                value.len()
            );
            continue;
        }
        if let Ok(header_value) = HeaderValue::try_from(value) {
            headers.insert(key.clone(), header_value);
        }
    }
    (!headers.is_empty()).then_some(headers)
}

fn parse_document_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(text) => DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc)),
        Value::Number(number) => {
            let raw = number.as_i64()?;
            // Values above 1e12 are already milliseconds (Unix epoch seconds won't reach
            // 1e12 until year 33658). Values at or below are treated as seconds and
            // multiplied by 1000.
            let millis = if !(-1_000_000_000_000..=1_000_000_000_000).contains(&raw) {
                raw
            } else {
                raw.saturating_mul(1_000)
            };
            DateTime::from_timestamp_millis(millis).map(|timestamp| timestamp.with_timezone(&Utc))
        }
        _ => None,
    }
}

#[async_trait]
impl Source for OpenSearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(ref cause) = self.state_restore_error {
            return Err(Error::InitError(format!("state restore failed: {cause}")));
        }

        validate_open_config(&self.config)?;

        info!(
            "Opening OpenSearch source connector with ID: {} for URL: {}, index: {}",
            self.id,
            redacted_url(&self.config.url),
            self.config.index
        );

        let client = self.create_client().await?;

        self.check_index_exists_with_retry(&client).await?;

        let timestamp_field = self.timestamp_field()?;
        let batch_size = self.batch_size();
        self.search_body_base = Some(json!({
            "query": self.config.query.clone().unwrap_or_else(|| json!({ "match_all": {} })),
            "size": batch_size,
            "track_total_hits": false,
            "sort": [
                { timestamp_field: { "order": "asc" } },
                { "_id": { "order": "asc" } },
                { "_index": { "order": "asc" } }
            ]
        }));
        self.client = Some(client);

        info!(
            "Successfully opened OpenSearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} connector ID: {} circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            sleep(self.polling_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        sleep(self.polling_interval).await;

        let start_time = std::time::Instant::now();

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("OpenSearch client not initialized".to_string()))?;

        match self.search_documents(client).await {
            Ok(outcome) => {
                self.circuit_breaker.record_success();
                let processing_time = start_time.elapsed().as_millis() as f64;
                match self.finalize_poll(outcome, processing_time).await {
                    Ok((messages, persisted_state)) => Ok(ProducedMessages {
                        schema: Schema::Json,
                        messages,
                        state: persisted_state,
                    }),
                    Err(error) => self.handle_poll_error(error).await,
                }
            }
            Err(error) => self.handle_poll_error(error).await,
        }
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate) = candidate
        {
            *self.state.lock().await = candidate;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "OpenSearch source connector with ID: {} is closing. Stats: {} total documents published, {} polls executed, {} errors",
            self.id, state.total_documents_published, state.poll_count, state.error_count
        );
        drop(state);

        self.client = None;
        info!(
            "OpenSearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod http_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OpenSearchSourceConfig {
        OpenSearchSourceConfig {
            url: "http://localhost:9200".to_string(),
            index: "test_documents".to_string(),
            username: None,
            password: None,
            query: None,
            polling_interval: Some("100ms".to_string()),
            batch_size: Some(10),
            timestamp_field: Some("timestamp".to_string()),
            verbose_logging: false,
            max_retries: None,
            retry_delay: None,
            retry_max_delay: None,
            max_open_retries: None,
            open_retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        }
    }

    fn test_state() -> State {
        State {
            last_poll_timestamp: None,
            total_documents_published: 500,
            poll_count: 7,
            search_after: Some(vec![json!("2024-01-01T00:00:00Z"), json!("doc_42")]),
            error_count: 1,
            last_error: Some("connection reset".to_string()),
            processing_stats: ProcessingStats {
                total_bytes_processed: 1024,
                avg_batch_processing_time_ms: 12.5,
                last_successful_poll: None,
                empty_polls_count: 2,
                successful_polls_count: 5,
            },
        }
    }

    #[test]
    fn given_persisted_runtime_state_when_new_should_restore_counts() {
        let state = test_state();
        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let source = OpenSearchSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.total_documents_published, 500);
            assert_eq!(restored.poll_count, 7);
            assert_eq!(
                restored.search_after,
                Some(vec![json!("2024-01-01T00:00:00Z"), json!("doc_42")])
            );
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_no_runtime_state_when_new_should_start_fresh() {
        let source = OpenSearchSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_published, 0);
            assert_eq!(state.poll_count, 0);
            assert_eq!(state.search_after, None);
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_invalid_runtime_state_when_new_should_set_restore_error() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = OpenSearchSource::new(1, test_config(), Some(invalid_state));

        assert!(source.state_restore_error.is_some());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_published, 0);
            assert_eq!(state.poll_count, 0);
        });
    }

    #[test]
    fn given_invalid_state_when_open_should_fail() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let mut source = OpenSearchSource::new(1, test_config(), Some(invalid_state));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(_))),
            "open() must fail with InitError on restore failure"
        );
    }

    #[test]
    fn given_missing_timestamp_field_when_validate_should_fail() {
        let mut config = test_config();
        config.timestamp_field = None;
        let error = validate_open_config(&config).expect_err("missing timestamp_field");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_empty_timestamp_field_when_validate_should_fail() {
        let mut config = test_config();
        config.timestamp_field = Some(String::new());
        let error = validate_open_config(&config).expect_err("empty timestamp_field");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_unparsable_timestamp_when_parsed_should_return_none() {
        let value = json!("not-a-timestamp");
        assert!(parse_document_timestamp(&value).is_none());
    }

    #[test]
    fn given_invalid_url_when_open_should_return_invalid_config() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut config = test_config();
            config.url = "not-a-url".to_string();
            let mut source = OpenSearchSource::new(1, config, None);
            let error = source.open().await.expect_err("invalid url");
            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }

    #[test]
    fn given_url_with_credentials_and_query_when_redacted_should_drop_both() {
        let redacted = redacted_url("https://admin:s3cret@search.example.com:9200/?token=abc");
        assert_eq!(redacted, "https://search.example.com:9200/");
        assert!(!redacted.contains("s3cret") && !redacted.contains("abc"));
    }

    #[test]
    fn given_unparsable_url_when_redacted_should_not_echo_input() {
        assert_eq!(redacted_url("not a url s3cret"), "<invalid url>");
    }

    #[test]
    fn given_same_index_and_id_when_hashed_should_return_same_message_id() {
        assert_eq!(message_id("logs", "doc-1"), message_id("logs", "doc-1"));
    }

    #[test]
    fn given_different_id_or_index_when_hashed_should_return_different_message_id() {
        let base = message_id("logs", "doc-1");
        assert_ne!(base, message_id("logs", "doc-2"));
        assert_ne!(base, message_id("metrics", "doc-1"));
        assert_ne!(message_id("ab", "c"), message_id("a", "bc"));
    }

    #[test]
    fn given_index_and_id_when_building_headers_should_carry_both() {
        let headers = document_headers(1, Some("logs-a"), Some("doc-1")).expect("headers");
        let id = headers
            .get(&*DOCUMENT_ID_HEADER_KEY)
            .expect("document id header");
        let index = headers.get(&*INDEX_HEADER_KEY).expect("index header");
        assert_eq!(id.as_str().expect("string header"), "doc-1");
        assert_eq!(index.as_str().expect("string header"), "logs-a");
    }

    #[test]
    fn given_no_index_or_id_when_building_headers_should_return_none() {
        assert!(document_headers(1, None, None).is_none());
    }

    #[test]
    fn given_oversized_document_id_when_building_headers_should_omit_header() {
        let oversized = "x".repeat(MAX_HEADER_VALUE_LENGTH + 1);
        assert!(document_headers(1, None, Some(&oversized)).is_none());

        let headers = document_headers(1, Some("logs-a"), Some(&oversized)).expect("headers");
        assert!(headers.contains_key(&*INDEX_HEADER_KEY));
        assert!(!headers.contains_key(&*DOCUMENT_ID_HEADER_KEY));
    }

    #[test]
    fn given_zero_batch_size_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let error = validate_open_config(&config).expect_err("zero batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_batch_size_above_max_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(MAX_BATCH_SIZE + 1);
        let error = validate_open_config(&config).expect_err("oversized batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));

        config.batch_size = Some(MAX_BATCH_SIZE);
        validate_open_config(&config).expect("max batch_size is allowed");
    }

    #[test]
    fn given_rfc3339_timestamp_when_parsed_should_succeed() {
        let value = json!("2024-01-15T10:30:00Z");
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn given_epoch_millis_timestamp_when_parsed_should_succeed() {
        let value = json!(1_705_312_200_000_i64);
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn given_state_when_serialized_should_round_trip() {
        let original = test_state();

        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(
            original.total_documents_published,
            deserialized.total_documents_published
        );
        assert_eq!(original.poll_count, deserialized.poll_count);
        assert_eq!(original.search_after, deserialized.search_after);
        assert_eq!(original.error_count, deserialized.error_count);
    }

    #[test]
    fn given_state_when_serialized_should_produce_connector_state() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let state = test_state();

        let connector_state = source.serialize_state(&state);
        assert!(connector_state.is_some());

        let restored: State = connector_state
            .unwrap()
            .deserialize(CONNECTOR_NAME, 1)
            .expect("Failed to deserialize state");
        assert_eq!(restored.total_documents_published, 500);
    }
}
