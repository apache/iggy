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
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::retry::{CircuitBreaker, parse_duration};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use opensearch::{
    OpenSearch, SearchParts,
    auth::Credentials,
    http::{Url, transport::TransportBuilder},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

mod retry;
mod state_manager;
use crate::retry::{
    DEFAULT_CB_COOL_DOWN, DEFAULT_CB_THRESHOLD, DEFAULT_MAX_OPEN_RETRIES, DEFAULT_MAX_RETRIES,
    DEFAULT_OPEN_RETRY_MAX_DELAY, DEFAULT_RETRY_DELAY, DEFAULT_RETRY_MAX_DELAY, RetryBackoff,
    is_transient_status, normalized_max_attempts, sleep_before_retry,
};
use crate::state_manager::{SOURCE_STATE_VERSION, SourceState, validate_state_storage_config};

source_connector!(OpenSearchSource);

const CONNECTOR_NAME: &str = "OpenSearch source";
const DEFAULT_POLLING_INTERVAL: &str = "10s";
const DEFAULT_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    last_poll_timestamp: Option<DateTime<Utc>>,
    #[serde(default, alias = "total_documents_fetched")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default)]
    pub enabled: bool,
    pub storage_type: Option<String>,
    pub storage_config: Option<Value>,
    pub state_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenSearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
    #[serde(default)]
    pub verbose_logging: bool,
    pub state: Option<StateConfig>,
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
    max_retries: u32,
    retry_delay: Duration,
    retry_max_delay: Duration,
    max_open_retries: u32,
    open_retry_max_delay: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
    state: Mutex<State>,
    /// `Some(cause)` when runtime state restore was rejected; `None` means restore succeeded.
    state_restore_error: Option<String>,
    /// True when `new()` restored a valid runtime `ConnectorState`. File mirror must not override it.
    runtime_state_restored: bool,
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
        let (restored_state, state_restore_error, runtime_state_restored) =
            restore_state(id, state);

        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CB_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CB_COOL_DOWN,
        );
        let circuit_breaker = Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down));
        let max_retries =
            normalized_max_attempts(config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES));
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_max_delay =
            parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY);
        let max_open_retries =
            normalized_max_attempts(config.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES));
        let open_retry_max_delay = parse_duration(
            config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

        OpenSearchSource {
            id,
            config,
            max_retries,
            retry_delay,
            retry_max_delay,
            max_open_retries,
            open_retry_max_delay,
            circuit_breaker,
            client: None,
            polling_interval,
            search_body_base: None,
            verbose,
            state: Mutex::new(restored_state),
            state_restore_error,
            runtime_state_restored,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE)
    }

    fn timestamp_field(&self) -> &str {
        self.config
            .timestamp_field
            .as_deref()
            .expect("timestamp_field validated at open()")
    }

    fn get_state_id(&self) -> String {
        self.config
            .state
            .as_ref()
            .and_then(|s| s.state_id.clone())
            .unwrap_or_else(|| format!("opensearch_source_{}", self.id))
    }

    pub(crate) async fn internal_state_to_source_state(&self) -> Result<SourceState, Error> {
        let state = self.state.lock().await;
        let data = serde_json::to_value(&*state).map_err(|error| {
            Error::Serialization(format!("Failed to serialize connector state: {error}"))
        })?;

        Ok(SourceState {
            id: self.get_state_id(),
            last_updated: Utc::now(),
            version: SOURCE_STATE_VERSION,
            data,
            metadata: Some(json!({
                "connector_type": "opensearch_source",
                "connector_id": self.id,
                "index": self.config.index,
                "url": self.config.url,
            })),
        })
    }

    pub(crate) async fn source_state_to_internal_state(
        &mut self,
        source_state: SourceState,
    ) -> Result<(), Error> {
        if source_state.version != SOURCE_STATE_VERSION {
            return Err(Error::Serialization(format!(
                "unsupported file state version {}, expected {SOURCE_STATE_VERSION}",
                source_state.version
            )));
        }

        let restored: State = serde_json::from_value(source_state.data).map_err(|error| {
            Error::Serialization(format!("Failed to deserialize connector state: {error}"))
        })?;

        let mut state = self.state.lock().await;
        *state = restored;
        Ok(())
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
        let max_attempts = self.max_open_retries;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let response = match client
                .indices()
                .exists(opensearch::indices::IndicesExistsParts::Index(&[&self
                    .config
                    .index]))
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    if attempt < max_attempts {
                        sleep_before_retry(
                            "index_exists",
                            self.id,
                            attempt,
                            max_attempts,
                            // retry_delay is shared with the poll phase (no separate open-phase
                            // base delay); open_retry_max_delay caps the growth independently.
                            // Same pattern as the InfluxDB source connector.
                            &RetryBackoff {
                                delay: self.retry_delay,
                                max_delay: self.open_retry_max_delay,
                            },
                            None,
                            &error.to_string(),
                        )
                        .await;
                        continue;
                    }
                    return Err(Error::InitError(format!(
                        "Failed to check index existence: {error}"
                    )));
                }
            };

            if response.status_code().is_success() {
                return Ok(());
            }

            let status = response.status_code().as_u16();
            if status == 404 {
                return Err(Error::InitError(format!(
                    "Index '{}' does not exist or is not accessible",
                    self.config.index
                )));
            }

            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());

            if is_transient_status(status) && attempt < max_attempts {
                sleep_before_retry(
                    "index_exists",
                    self.id,
                    attempt,
                    max_attempts,
                    &RetryBackoff {
                        delay: self.retry_delay,
                        max_delay: self.open_retry_max_delay,
                    },
                    retry_after.as_deref(),
                    &format!("HTTP {status}: {error_text}"),
                )
                .await;
                continue;
            }

            return Err(Error::InitError(format!(
                "Index '{}' check failed: HTTP {status}: {error_text}",
                self.config.index
            )));
        }
    }

    async fn send_search_with_retry(
        &self,
        client: &OpenSearch,
        mut search_body: Value,
    ) -> Result<opensearch::http::response::Response, Error> {
        let max_attempts = self.max_retries;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let body = if attempt < max_attempts {
                search_body.clone()
            } else {
                std::mem::take(&mut search_body)
            };
            let response = match client
                .search(SearchParts::Index(&[&self.config.index]))
                .body(body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    if attempt < max_attempts {
                        sleep_before_retry(
                            "search",
                            self.id,
                            attempt,
                            max_attempts,
                            &RetryBackoff {
                                delay: self.retry_delay,
                                max_delay: self.retry_max_delay,
                            },
                            None,
                            &error.to_string(),
                        )
                        .await;
                        continue;
                    }
                    return Err(Error::Storage(format!("Failed to execute search: {error}")));
                }
            };

            if response.status_code().is_success() {
                return Ok(response);
            }

            let status = response.status_code().as_u16();
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());

            if is_transient_status(status) && attempt < max_attempts {
                sleep_before_retry(
                    "search",
                    self.id,
                    attempt,
                    max_attempts,
                    &RetryBackoff {
                        delay: self.retry_delay,
                        max_delay: self.retry_max_delay,
                    },
                    retry_after.as_deref(),
                    &format!("HTTP {status}: {error_text}"),
                )
                .await;
                continue;
            }

            return Err(Error::Storage(format!(
                "Search request failed: {error_text}"
            )));
        }
    }

    async fn search_documents(&self, client: &OpenSearch) -> Result<SearchOutcome, Error> {
        let state = self.state.lock().await;
        let search_after = state.search_after.clone();
        drop(state);

        let timestamp_field = self.timestamp_field();

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

        let response = self.send_search_with_retry(client, search_body).await?;

        let mut response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Storage(format!("Failed to parse search response: {e}")))?;

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
                warn!(
                    connector_id = self.id,
                    hit_id = hit.get("_id").and_then(|value| value.as_str()),
                    "Skipping OpenSearch hit without sort tuple; document will not be published"
                );
                continue;
            };

            last_sort = Some(sort);

            let Some(source) = hit.get("_source") else {
                warn!(
                    connector_id = self.id,
                    hit_id = hit.get("_id").and_then(|v| v.as_str()),
                    "Skipping OpenSearch hit without _source; document will not be published"
                );
                continue;
            };

            if let Some(timestamp_value) = source.get(timestamp_field)
                && let Some(timestamp_utc) = parse_document_timestamp(timestamp_value)
            {
                last_poll_timestamp = Some(timestamp_utc);
            }

            let payload = serde_json::to_vec(source)
                .map_err(|e| Error::Serialization(format!("Failed to serialize document: {e}")))?;
            batch_bytes += payload.len() as u64;

            messages.push(ProducedMessage {
                id: None,
                headers: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                payload,
            });
        }

        if !hits.is_empty() && last_sort.is_none() {
            return Err(Error::Storage(format!(
                "OpenSearch returned {} hit(s) but none had a sort tuple; \
                 index may be missing the sort field or using an incompatible mapping",
                hits.len()
            )));
        }

        // Guard: cursor would advance past the entire batch but nothing would be published.
        // This happens when _source is disabled on the index (all sort-bearing hits lack _source).
        if !hits.is_empty() && messages.is_empty() && last_sort.is_some() {
            return Err(Error::Storage(format!(
                "OpenSearch returned {} hit(s) with valid sort tuples but all were missing \
                 _source; index may have _source disabled. Refusing to advance cursor \
                 without publishing any messages.",
                hits.len()
            )));
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
    ) -> (Vec<ProducedMessage>, Option<ConnectorState>) {
        let mut state = self.state.lock().await;
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
        let state_snapshot = state.clone();
        let messages = outcome.messages;
        drop(state);
        let persisted_state = self.serialize_state(&state_snapshot);

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

        (messages, persisted_state)
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

fn restore_state(id: u32, state: Option<ConnectorState>) -> (State, Option<String>, bool) {
    let Some(connector_state) = state else {
        return (State::default(), None, false);
    };

    match connector_state.deserialize::<State>(CONNECTOR_NAME, id) {
        Some(restored) => {
            info!(
                "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                 Documents published: {}, poll count: {}",
                restored.total_documents_published, restored.poll_count
            );
            (restored, None, true)
        }
        None => {
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (State::default(), Some(cause), false)
        }
    }
}

fn validate_open_config(config: &OpenSearchSourceConfig) -> Result<(), Error> {
    if config.timestamp_field.as_deref().is_none_or(str::is_empty) {
        return Err(Error::InvalidConfigValue(
            "timestamp_field is required for incremental OpenSearch polling".to_string(),
        ));
    }

    if matches!(config.batch_size, Some(0)) {
        return Err(Error::InvalidConfigValue(
            "batch_size must be at least 1".to_string(),
        ));
    }

    if let Some(state) = &config.state
        && state.enabled
    {
        validate_state_storage_config(state)?;
    }

    Ok(())
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

        let timestamp_field = self.timestamp_field();
        let batch_size = self.batch_size();
        self.search_body_base = Some(json!({
            "query": self.config.query.clone().unwrap_or_else(|| json!({ "match_all": {} })),
            "size": batch_size,
            "sort": [
                { timestamp_field: { "order": "asc" } },
                { "_id": { "order": "asc" } }
            ]
        }));

        info!(
            "Opening OpenSearch source connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client().await?;

        self.check_index_exists_with_retry(&client).await?;

        self.client = Some(client);

        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
        {
            if self.runtime_state_restored {
                info!(
                    "Skipping file state load for OpenSearch source connector with ID: {} \
                     because runtime ConnectorState is authoritative",
                    self.id
                );
            } else {
                self.load_state().await.map_err(|error| {
                    Error::InitError(format!("file state load failed: {error}"))
                })?;
            }
        }

        info!(
            "Successfully opened OpenSearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} connector ID: {} — circuit breaker is OPEN. Skipping poll.",
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
                let (messages, persisted_state) =
                    self.finalize_poll(outcome, processing_time).await;
                Ok(ProducedMessages {
                    schema: Schema::Json,
                    messages,
                    state: persisted_state,
                })
            }
            Err(error) => self.handle_poll_error(error).await,
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "OpenSearch source connector with ID: {} is closing. Stats: {} total documents published, {} polls executed, {} errors",
            self.id, state.total_documents_published, state.poll_count, state.error_count
        );
        drop(state);

        if self.client.is_some()
            && self
                .config
                .state
                .as_ref()
                .map(|s| s.enabled)
                .unwrap_or(false)
        {
            self.save_state().await?;
        }

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
            state: None,
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
            assert!(source.runtime_state_restored);
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
            assert!(!source.runtime_state_restored);
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
    fn given_source_state_json_when_apply_should_restore_metrics() {
        use crate::state_manager::{SOURCE_STATE_VERSION, SourceState};

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut source = OpenSearchSource::new(1, test_config(), None);
            let source_state = SourceState {
                id: "opensearch_source_1".to_string(),
                last_updated: Utc::now(),
                version: SOURCE_STATE_VERSION,
                data: json!({
                    "total_documents_published": 9,
                    "poll_count": 4,
                    "search_after": ["2024-02-01T00:00:00Z", "doc-9"],
                    "error_count": 2,
                    "last_error": "timeout",
                    "processing_stats": {
                        "total_bytes_processed": 100,
                        "avg_batch_processing_time_ms": 1.5,
                        "last_successful_poll": null,
                        "empty_polls_count": 1,
                        "successful_polls_count": 3
                    }
                }),
                metadata: None,
            };

            source
                .source_state_to_internal_state(source_state)
                .await
                .expect("apply source state");

            let state = source.state.lock().await;
            assert_eq!(state.total_documents_published, 9);
            assert_eq!(state.poll_count, 4);
            assert_eq!(
                state.search_after,
                Some(vec![json!("2024-02-01T00:00:00Z"), json!("doc-9")])
            );
            assert_eq!(state.error_count, 2);
        });
    }

    #[test]
    fn given_unsupported_file_state_version_when_applied_should_fail() {
        use crate::state_manager::SourceState;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut source = OpenSearchSource::new(1, test_config(), None);
            let source_state = SourceState {
                id: "opensearch_source_1".to_string(),
                last_updated: Utc::now(),
                version: 99,
                data: json!({ "poll_count": 1 }),
                metadata: None,
            };

            let error = source
                .source_state_to_internal_state(source_state)
                .await
                .expect_err("unsupported version");
            assert!(matches!(error, Error::Serialization(_)));
        });
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
    fn given_internal_state_when_export_should_round_trip_source_state() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let source = OpenSearchSource::new(1, test_config(), None);
            {
                let mut runtime_state = source.state.lock().await;
                *runtime_state = test_state();
            }
            let exported = source
                .internal_state_to_source_state()
                .await
                .expect("export state");
            assert_eq!(exported.id, "opensearch_source_1");
            assert_eq!(exported.data["total_documents_published"], 500);
            assert_eq!(
                exported.metadata.as_ref().unwrap()["index"],
                "test_documents"
            );
        });
    }

    #[test]
    fn given_zero_batch_size_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let error = validate_open_config(&config).expect_err("zero batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
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
