/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use async_trait::async_trait;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::retry::parse_duration;
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
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

mod state_manager;
use crate::state_manager::{SourceState, create_state_storage};

source_connector!(OpenSearchSource);

const CONNECTOR_NAME: &str = "OpenSearch source";
const DEFAULT_POLLING_INTERVAL: &str = "10s";
const DEFAULT_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
    last_document_id: Option<String>,
    /// OpenSearch `search_after` tuple from the last hit in the previous batch.
    search_after: Option<Vec<Value>>,
    error_count: usize,
    last_error: Option<String>,
    processing_stats: ProcessingStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingStats {
    total_bytes_processed: u64,
    avg_batch_processing_time_ms: f64,
    last_successful_poll: Option<DateTime<Utc>>,
    empty_polls_count: usize,
    successful_polls_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default)]
    pub enabled: bool,
    pub storage_type: Option<String>,
    pub storage_config: Option<Value>,
    pub state_id: Option<String>,
    pub auto_save_interval: Option<String>,
    pub tracked_fields: Option<Vec<String>>,
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
    pub verbose_logging: Option<bool>,
    pub state: Option<StateConfig>,
}

#[derive(Debug)]
pub struct OpenSearchSource {
    id: u32,
    config: OpenSearchSourceConfig,
    client: Option<OpenSearch>,
    polling_interval: Duration,
    search_query: Value,
    verbose: bool,
    state: Mutex<State>,
    /// `Some(cause)` when runtime state restore was rejected; `None` means restore succeeded.
    state_restore_error: Option<String>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            last_poll_timestamp: None,
            total_documents_fetched: 0,
            poll_count: 0,
            last_document_id: None,
            search_after: None,
            error_count: 0,
            last_error: None,
            processing_stats: ProcessingStats {
                total_bytes_processed: 0,
                avg_batch_processing_time_ms: 0.0,
                last_successful_poll: None,
                empty_polls_count: 0,
                successful_polls_count: 0,
            },
        }
    }
}

impl OpenSearchSource {
    pub fn new(id: u32, config: OpenSearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval =
            parse_duration(config.polling_interval.as_deref(), DEFAULT_POLLING_INTERVAL);
        let search_query = config
            .query
            .clone()
            .unwrap_or_else(|| json!({ "match_all": {} }));
        let verbose = config.verbose_logging.unwrap_or(false);
        let (restored_state, state_restore_error) = restore_state(id, state);

        OpenSearchSource {
            id,
            config,
            client: None,
            polling_interval,
            search_query,
            verbose,
            state: Mutex::new(restored_state),
            state_restore_error,
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

    async fn internal_state_to_source_state(&self) -> Result<SourceState, Error> {
        let state = self.state.lock().await;

        let data = json!({
            "last_poll_timestamp": state.last_poll_timestamp,
            "total_documents_fetched": state.total_documents_fetched,
            "poll_count": state.poll_count,
            "last_document_id": state.last_document_id,
            "search_after": state.search_after,
            "error_count": state.error_count,
            "last_error": state.last_error,
            "processing_stats": state.processing_stats,
        });

        Ok(SourceState {
            id: self.get_state_id(),
            last_updated: Utc::now(),
            version: 1,
            data,
            metadata: Some(json!({
                "connector_type": "opensearch_source",
                "connector_id": self.id,
                "index": self.config.index,
                "url": self.config.url,
            })),
        })
    }

    async fn source_state_to_internal_state(
        &mut self,
        source_state: SourceState,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().await;

        if let Some(data) = source_state.data.as_object() {
            if let Some(timestamp) = data.get("last_poll_timestamp")
                && let Some(ts_str) = timestamp.as_str()
                && let Ok(dt) = DateTime::parse_from_rfc3339(ts_str)
            {
                state.last_poll_timestamp = Some(dt.with_timezone(&Utc));
            }

            if let Some(count) = data.get("total_documents_fetched")
                && let Some(count_val) = count.as_u64()
            {
                state.total_documents_fetched = count_val as usize;
            }

            if let Some(count) = data.get("poll_count")
                && let Some(count_val) = count.as_u64()
            {
                state.poll_count = count_val as usize;
            }

            if let Some(doc_id) = data.get("last_document_id") {
                state.last_document_id = doc_id.as_str().map(str::to_owned);
            }

            if let Some(search_after) = data.get("search_after")
                && let Ok(cursor) = serde_json::from_value(search_after.clone())
            {
                state.search_after = cursor;
            }

            if let Some(error_count) = data.get("error_count")
                && let Some(count_val) = error_count.as_u64()
            {
                state.error_count = count_val as usize;
            }

            if let Some(last_error) = data.get("last_error") {
                state.last_error = last_error.as_str().map(str::to_owned);
            }

            if let Some(stats) = data.get("processing_stats")
                && let Ok(processing_stats) = serde_json::from_value(stats.clone())
            {
                state.processing_stats = processing_stats;
            }
        }

        Ok(())
    }

    async fn create_client(&self) -> Result<OpenSearch, Error> {
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Storage(format!("Invalid OpenSearch URL: {error}")))?;

        let conn_pool = opensearch::http::transport::SingleNodeConnectionPool::new(url);
        let mut transport_builder = TransportBuilder::new(conn_pool);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|e| Error::Storage(format!("Failed to build transport: {e}")))?;

        Ok(OpenSearch::new(transport))
    }

    async fn search_documents(&self, client: &OpenSearch) -> Result<Vec<ProducedMessage>, Error> {
        let state = self.state.lock().await;
        let batch_size = self.batch_size();
        let timestamp_field = self.timestamp_field();
        let search_after = state.search_after.clone();
        drop(state);

        let mut search_body = json!({
            "query": self.search_query,
            "size": batch_size,
            "sort": [
                { timestamp_field: { "order": "asc" } },
                { "_id": { "order": "asc" } }
            ]
        });

        if let Some(cursor) = search_after {
            search_body["search_after"] = json!(cursor);
        }

        let response = client
            .search(SearchParts::Index(&[&self.config.index]))
            .body(search_body)
            .send()
            .await
            .map_err(|e| Error::Storage(format!("Failed to execute search: {e}")))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .map_err(|e| Error::Storage(format!("Failed to read search error body: {e}")))?;
            return Err(Error::Storage(format!(
                "Search request failed: {error_text}"
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Storage(format!("Failed to parse search response: {e}")))?;

        let hits = response_body
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
            .cloned()
            .unwrap_or_default();

        let mut messages = Vec::with_capacity(hits.len());
        let mut batch_bytes = 0u64;
        let mut last_search_after = None;
        let mut last_document_id = None;
        let mut last_poll_timestamp = None;

        for hit in &hits {
            if let Some(sort) = hit.get("sort").and_then(|s| s.as_array()) {
                last_search_after = Some(sort.clone());
            }

            if let Some(document_id) = hit.get("_id").and_then(|v| v.as_str()) {
                last_document_id = Some(document_id.to_string());
            }

            let Some(source) = hit.get("_source") else {
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

        let mut state = self.state.lock().await;
        state.total_documents_fetched += messages.len();
        state.poll_count += 1;
        state.search_after = last_search_after;
        state.last_document_id = last_document_id;
        if let Some(timestamp) = last_poll_timestamp {
            state.last_poll_timestamp = Some(timestamp);
        }
        state.processing_stats.total_bytes_processed += batch_bytes;

        Ok(messages)
    }
}

fn restore_state(id: u32, state: Option<ConnectorState>) -> (State, Option<String>) {
    let Some(connector_state) = state else {
        return (State::default(), None);
    };

    let bytes = connector_state.0;
    match ConnectorState(bytes.clone()).deserialize::<State>(CONNECTOR_NAME, id) {
        Some(restored) => {
            info!(
                "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                 Documents fetched: {}, poll count: {}",
                restored.total_documents_fetched, restored.poll_count
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

    if matches!(config.batch_size, Some(0)) {
        return Err(Error::InvalidConfigValue(
            "batch_size must be at least 1".to_string(),
        ));
    }

    if let Some(state) = &config.state
        && state.enabled
    {
        create_state_storage(state)?;
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
            let millis = if raw.abs() > 1_000_000_000_000 {
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
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client().await?;

        let response = client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[&self
                .config
                .index]))
            .send()
            .await
            .map_err(|e| Error::Storage(format!("Failed to check index existence: {e}")))?;

        if !response.status_code().is_success() {
            return Err(Error::Storage(format!(
                "Index '{}' does not exist or is not accessible",
                self.config.index
            )));
        }

        self.client = Some(client);

        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
            && let Err(e) = self.load_state().await
        {
            warn!(
                "Failed to load state for OpenSearch source connector with ID: {}: {}",
                self.id, e
            );
        }

        info!(
            "Successfully opened OpenSearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let start_time = std::time::Instant::now();

        sleep(self.polling_interval).await;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Storage("OpenSearch client not initialized".to_string()))?;

        let messages = match self.search_documents(client).await {
            Ok(msgs) => {
                let mut state = self.state.lock().await;
                state.processing_stats.successful_polls_count += 1;
                state.processing_stats.last_successful_poll = Some(Utc::now());

                let processing_time = start_time.elapsed().as_millis() as f64;
                let total_polls = state.processing_stats.successful_polls_count
                    + state.processing_stats.empty_polls_count;
                state.processing_stats.avg_batch_processing_time_ms =
                    (state.processing_stats.avg_batch_processing_time_ms
                        * (total_polls - 1) as f64
                        + processing_time)
                        / total_polls as f64;

                if msgs.is_empty() {
                    state.processing_stats.empty_polls_count += 1;
                }

                let produced_count = msgs.len();
                let total_documents_fetched = state.total_documents_fetched;
                drop(state);

                if self.verbose {
                    info!(
                        "OpenSearch source connector ID: {} produced {produced_count} messages. \
                         Total fetched: {total_documents_fetched}",
                        self.id
                    );
                } else {
                    debug!(
                        "OpenSearch source connector ID: {} produced {produced_count} messages. \
                         Total fetched: {total_documents_fetched}",
                        self.id
                    );
                }

                msgs
            }
            Err(e) => {
                let mut state = self.state.lock().await;
                state.error_count += 1;
                state.last_error = Some(e.to_string());
                drop(state);
                return Err(e);
            }
        };
        let persisted_state = {
            let state = self.state.lock().await;
            self.serialize_state(&state)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "OpenSearch source connector with ID: {} is closing. Stats: {} total documents fetched, {} polls executed, {} errors",
            self.id, state.total_documents_fetched, state.poll_count, state.error_count
        );
        drop(state);

        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
            && let Err(e) = self.save_state().await
        {
            warn!(
                "Failed to save final state for OpenSearch source connector with ID: {}: {}",
                self.id, e
            );
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
            verbose_logging: None,
            state: None,
        }
    }

    fn test_state() -> State {
        State {
            last_poll_timestamp: None,
            total_documents_fetched: 500,
            poll_count: 5,
            last_document_id: Some("doc_42".to_string()),
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
    fn given_persisted_state_should_restore_total_documents_fetched() {
        let state = test_state();
        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let source = OpenSearchSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.total_documents_fetched, 500);
            assert_eq!(restored.poll_count, 5);
            assert_eq!(restored.last_document_id, Some("doc_42".to_string()));
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = OpenSearchSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_fetched, 0);
            assert_eq!(state.poll_count, 0);
            assert_eq!(state.last_document_id, None);
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_invalid_state_should_set_state_restore_error() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = OpenSearchSource::new(1, test_config(), Some(invalid_state));

        assert!(source.state_restore_error.is_some());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_fetched, 0);
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
    fn given_zero_batch_size_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let error = validate_open_config(&config).expect_err("zero batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_rfc3339_timestamp_value_should_parse() {
        let value = json!("2024-01-15T10:30:00Z");
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn given_epoch_millis_timestamp_value_should_parse() {
        let value = json!(1_705_312_200_000_i64);
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn given_state_should_round_trip_serialization() {
        let original = test_state();

        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(
            original.total_documents_fetched,
            deserialized.total_documents_fetched
        );
        assert_eq!(original.poll_count, deserialized.poll_count);
        assert_eq!(original.last_document_id, deserialized.last_document_id);
        assert_eq!(original.search_after, deserialized.search_after);
        assert_eq!(original.error_count, deserialized.error_count);
    }

    #[test]
    fn given_state_when_serialize_helper_should_produce_connector_state() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let state = test_state();

        let connector_state = source.serialize_state(&state);
        assert!(connector_state.is_some());

        let restored: State = connector_state
            .unwrap()
            .deserialize(CONNECTOR_NAME, 1)
            .expect("Failed to deserialize state");
        assert_eq!(restored.total_documents_fetched, 500);
    }
}
