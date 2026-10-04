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
use elasticsearch::{
    Elasticsearch, SearchParts,
    auth::Credentials,
    http::{Url, transport::TransportBuilder},
};
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{info, warn};

mod state_manager;
use crate::state_manager::{FileStateStorage, SourceState, StateStorage};
pub use state_manager::{StateInfo, StateManager, StateStats};

source_connector!(ElasticsearchSource);

const CONNECTOR_NAME: &str = "Elasticsearch source";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
    /// Retained in snapshots; not used for pagination.
    last_document_id: Option<String>,
    /// Retained in snapshots; the connector does not use scroll.
    last_scroll_id: Option<String>,
    /// Retained in snapshots; not used as a polling cursor.
    last_offset: Option<u64>,
    /// Error count and last error
    error_count: usize,
    last_error: Option<String>,
    /// Processing statistics
    processing_stats: ProcessingStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingStats {
    /// Serialized source payload bytes fetched, including retries.
    total_bytes_processed: u64,
    /// Mean successful-poll elapsed milliseconds, including the polling delay.
    avg_batch_processing_time_ms: f64,
    /// Last successful processing timestamp
    last_successful_poll: Option<DateTime<Utc>>,
    /// Number of empty polls
    empty_polls_count: usize,
    /// Number of successful polls, including empty polls.
    successful_polls_count: usize,
}

impl ProcessingStats {
    fn record_success(&mut self, elapsed: Duration, empty: bool) {
        self.successful_polls_count += 1;
        self.last_successful_poll = Some(Utc::now());

        let total_polls = self.successful_polls_count;
        self.avg_batch_processing_time_ms = (self.avg_batch_processing_time_ms
            * (total_polls - 1) as f64
            + elapsed.as_millis() as f64)
            / total_polls as f64;
        if empty {
            self.empty_polls_count += 1;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Enable state persistence
    pub enabled: bool,
    /// Only "file" is implemented; other values fall back to the default file directory.
    pub storage_type: Option<String>,
    /// State storage configuration (depends on storage_type)
    pub storage_config: Option<Value>,
    /// State ID for this connector instance
    pub state_id: Option<String>,
    /// Interval for manually started StateManager tasks; unused by the runtime plugin.
    pub auto_save_interval: Option<String>,
    /// Reserved; does not filter the saved state.
    pub tracked_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
    /// Reserved; the connector does not use the scroll API.
    pub scroll_timeout: Option<String>,
    pub state: Option<StateConfig>,
}

#[derive(Debug)]
pub struct ElasticsearchSource {
    id: u32,
    config: ElasticsearchSourceConfig,
    client: Option<Elasticsearch>,
    polling_interval: Duration,
    state: Mutex<State>,
    pending_timestamp: Mutex<Option<DateTime<Utc>>>,
}

impl ElasticsearchSource {
    pub fn new(id: u32, config: ElasticsearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval = config
            .polling_interval
            .as_deref()
            .unwrap_or("10s")
            .parse::<humantime::Duration>()
            .unwrap_or_else(|_| humantime::Duration::from_str("10s").unwrap())
            .into();

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Documents fetched: {}, poll count: {}",
                    s.total_documents_fetched, s.poll_count
                );
            });

        ElasticsearchSource {
            id,
            config,
            client: None,
            polling_interval,
            pending_timestamp: Mutex::new(None),
            state: Mutex::new(restored_state.unwrap_or(State {
                last_poll_timestamp: None,
                total_documents_fetched: 0,
                poll_count: 0,
                last_document_id: None,
                last_scroll_id: None,
                last_offset: None,
                error_count: 0,
                last_error: None,
                processing_stats: ProcessingStats {
                    total_bytes_processed: 0,
                    avg_batch_processing_time_ms: 0.0,
                    last_successful_poll: None,
                    empty_polls_count: 0,
                    successful_polls_count: 0,
                },
            })),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    /// Create state storage based on configuration
    fn create_state_storage(&self) -> Option<Arc<dyn StateStorage>> {
        let state_config = self.config.state.as_ref()?;
        if !state_config.enabled {
            return None;
        }

        match state_config.storage_type.as_deref() {
            Some("file") | None => {
                let base_path = state_config
                    .storage_config
                    .as_ref()
                    .and_then(|c| c.get("base_path"))
                    .and_then(|p| p.as_str())
                    .unwrap_or("./connector_states");

                Some(Arc::new(FileStateStorage::new(base_path)))
            }
            Some("elasticsearch") => {
                // TODO: Implement Elasticsearch-based state storage
                warn!(
                    "Elasticsearch state storage not yet implemented, falling back to file storage"
                );
                Some(Arc::new(FileStateStorage::new("./connector_states")))
            }
            Some(storage_type) => {
                warn!(
                    "Unknown state storage type: {}, falling back to file storage",
                    storage_type
                );
                Some(Arc::new(FileStateStorage::new("./connector_states")))
            }
        }
    }

    /// Get state ID for this connector
    fn get_state_id(&self) -> String {
        self.config
            .state
            .as_ref()
            .and_then(|s| s.state_id.clone())
            .unwrap_or_else(|| format!("elasticsearch_source_{}", self.id))
    }

    /// Convert internal state to SourceState
    async fn internal_state_to_source_state(&self) -> Result<SourceState, Error> {
        let state = self.state.lock().await;

        let data = json!({
            "last_poll_timestamp": state.last_poll_timestamp,
            "total_documents_fetched": state.total_documents_fetched,
            "poll_count": state.poll_count,
            "last_document_id": state.last_document_id,
            "last_scroll_id": state.last_scroll_id,
            "last_offset": state.last_offset,
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
                "connector_type": "elasticsearch_source",
                "connector_id": self.id,
                "index": self.config.index,
                "url": self.config.url,
            })),
        })
    }

    /// Convert SourceState to internal state
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
                state.last_document_id = doc_id.as_str().map(|s| s.to_string());
            }

            if let Some(scroll_id) = data.get("last_scroll_id") {
                state.last_scroll_id = scroll_id.as_str().map(|s| s.to_string());
            }

            if let Some(offset) = data.get("last_offset") {
                state.last_offset = offset.as_u64();
            }

            if let Some(error_count) = data.get("error_count")
                && let Some(count_val) = error_count.as_u64()
            {
                state.error_count = count_val as usize;
            }

            if let Some(last_error) = data.get("last_error") {
                state.last_error = last_error.as_str().map(|s| s.to_string());
            }

            if let Some(stats) = data.get("processing_stats")
                && let Ok(processing_stats) = serde_json::from_value(stats.clone())
            {
                state.processing_stats = processing_stats;
            }
        }

        Ok(())
    }

    async fn create_client(&self) -> Result<Elasticsearch, Error> {
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Storage(format!("Invalid Elasticsearch URL: {error}")))?;

        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
        let mut transport_builder = TransportBuilder::new(conn_pool);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|e| Error::Storage(format!("Failed to build transport: {}", e)))?;

        Ok(Elasticsearch::new(transport))
    }

    async fn search_documents(
        &self,
        client: &Elasticsearch,
    ) -> Result<(Vec<ProducedMessage>, Option<DateTime<Utc>>), Error> {
        let state = self.state.lock().await;
        let batch_size = self.config.batch_size.unwrap_or(100);

        // Build query based on timestamp field if configured
        let mut query = self.config.query.clone().unwrap_or_else(|| {
            json!({
                "match_all": {}
            })
        });

        // Add timestamp filter for incremental polling
        if let Some(timestamp_field) = &self.config.timestamp_field
            && let Some(last_timestamp) = state.last_poll_timestamp
        {
            query = json!({
                "bool": {
                    "must": [
                        query,
                        {
                            "range": {
                                timestamp_field: {
                                    "gt": last_timestamp.to_rfc3339()
                                }
                            }
                        }
                    ]
                }
            });
        }

        let search_body = json!({
            "query": query,
            "size": batch_size,
            "sort": [
                {
                    self.config.timestamp_field.as_deref().unwrap_or("@timestamp"): {
                        "order": "asc"
                    }
                }
            ]
        });

        drop(state);

        let response = client
            .search(SearchParts::Index(&[&self.config.index]))
            .body(search_body)
            .send()
            .await
            .map_err(|e| Error::Storage(format!("Failed to execute search: {}", e)))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Storage(format!(
                "Search request failed: {}",
                error_text
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Storage(format!("Failed to parse search response: {}", e)))?;

        if response_body.get("timed_out").and_then(Value::as_bool) == Some(true) {
            return Err(Error::Storage("Elasticsearch search timed out".to_string()));
        }
        if let Some(failed_shards) = response_body
            .get("_shards")
            .and_then(|shards| shards.get("failed"))
            .and_then(Value::as_u64)
            .filter(|failed| *failed > 0)
        {
            return Err(Error::Storage(format!(
                "Elasticsearch search failed on {failed_shards} shards"
            )));
        }

        let mut messages = Vec::new();
        let mut latest_timestamp = None;
        let mut payload_bytes = 0;

        if let Some(hits) = response_body
            .get("hits")
            .and_then(|h| h.get("hits"))
            .and_then(|h| h.as_array())
        {
            for hit in hits {
                if let Some(source) = hit.get("_source") {
                    // Extract timestamp for incremental polling
                    if let Some(timestamp_field) = &self.config.timestamp_field
                        && let Some(timestamp_str) =
                            source.get(timestamp_field).and_then(|v| v.as_str())
                        && let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str)
                    {
                        let timestamp_utc = timestamp.with_timezone(&Utc);
                        if latest_timestamp.is_none() || timestamp_utc > latest_timestamp.unwrap() {
                            latest_timestamp = Some(timestamp_utc);
                        }
                    }

                    // Create message from document
                    let payload = serde_json::to_vec(source).map_err(|e| {
                        Error::Serialization(format!("Failed to serialize document: {}", e))
                    })?;

                    payload_bytes += payload.len() as u64;
                    let message = ProducedMessage {
                        id: None,
                        headers: None,
                        checksum: None,
                        timestamp: None,
                        origin_timestamp: None,
                        payload,
                    };
                    messages.push(message);
                }
            }
        }

        // Update state
        let mut state = self.state.lock().await;
        state.total_documents_fetched += messages.len();
        state.processing_stats.total_bytes_processed += payload_bytes;
        state.poll_count += 1;
        Ok((messages, latest_timestamp))
    }
}

#[async_trait]
impl Source for ElasticsearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Elasticsearch source connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client().await?;

        // Test connection by checking if index exists
        let response = client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self
                .config
                .index]))
            .send()
            .await
            .map_err(|e| Error::Storage(format!("Failed to check index existence: {}", e)))?;

        if !response.status_code().is_success() {
            return Err(Error::Storage(format!(
                "Index '{}' does not exist or is not accessible",
                self.config.index
            )));
        }

        self.client = Some(client);

        // Load state if state management is enabled
        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
            && let Err(e) = self.load_state().await
        {
            warn!(
                "Failed to load state for Elasticsearch source connector with ID: {}: {}",
                self.id, e
            );
        }

        info!(
            "Successfully opened Elasticsearch source connector with ID: {}",
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
            .ok_or_else(|| Error::Storage("Elasticsearch client not initialized".to_string()))?;

        let (messages, latest_timestamp) = match self.search_documents(client).await {
            Ok((msgs, latest_timestamp)) => {
                let mut state = self.state.lock().await;
                state
                    .processing_stats
                    .record_success(start_time.elapsed(), msgs.is_empty());

                drop(state);
                (msgs, latest_timestamp)
            }
            Err(e) => {
                // Update error statistics
                let mut state = self.state.lock().await;
                state.error_count += 1;
                state.last_error = Some(e.to_string());
                drop(state);
                return Err(e);
            }
        };
        let persisted_state = {
            let mut candidate_state = self.state.lock().await.clone();
            if let Some(timestamp) = latest_timestamp {
                candidate_state.last_poll_timestamp = Some(timestamp);
            }
            self.serialize_state(&candidate_state).ok_or_else(|| {
                Error::Serialization("Failed to serialize Elasticsearch source state".to_string())
            })?
        };
        *self.pending_timestamp.lock().await = latest_timestamp;

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(persisted_state),
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let pending_timestamp = self.pending_timestamp.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(timestamp) = pending_timestamp
        {
            self.state.lock().await.last_poll_timestamp = Some(timestamp);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch source connector with ID: {} is closing. Stats: {} total documents fetched, {} polls executed, {} errors",
            self.id, state.total_documents_fetched, state.poll_count, state.error_count
        );
        drop(state);

        // Save final state if state management is enabled
        if self
            .config
            .state
            .as_ref()
            .map(|s| s.enabled)
            .unwrap_or(false)
            && let Err(e) = self.save_state().await
        {
            warn!(
                "Failed to save final state for Elasticsearch source connector with ID: {}: {}",
                self.id, e
            );
        }

        self.client = None;
        info!(
            "Elasticsearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CONNECTOR_NAME, ElasticsearchSource, ElasticsearchSourceConfig, State};
    use iggy_connector_sdk::{ConnectorState, Source, source::SourceBatchResult};
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(url: &str) -> ElasticsearchSourceConfig {
        serde_json::from_value(json!({
            "url": url,
            "index": "logs",
            "polling_interval": "0s",
            "timestamp_field": "timestamp"
        }))
        .unwrap()
    }

    async fn source_backend() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/logs"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/logs/_search"))
            .and(body_partial_json(json!({"query": {"match_all": {}}})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "hits": {"hits": [{"_source": {"timestamp": "2026-01-01T00:00:00Z", "value": 1}}]}
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/logs/_search"))
            .and(body_partial_json(json!({"query": {"bool": {"must": [
                {"match_all": {}}, {"range": {"timestamp": {"gt": "2026-01-01T00:00:00+00:00"}}}
            ]}}})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"hits": {"hits": []}})))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn given_nack_should_retry_documents_before_advancing_cursor() {
        let server = source_backend().await;
        let mut source = ElasticsearchSource::new(1, test_config(&server.uri()), None);
        source.open().await.unwrap();
        let first = source.poll().await.unwrap();
        assert_eq!(first.messages.len(), 1);

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();
        let retry = source.poll().await.unwrap();
        assert_eq!(
            retry.messages.len(),
            1,
            "Nack must leave the document eligible for the next poll"
        );
        assert_eq!(retry.messages[0].payload, first.messages[0].payload);

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();
        assert!(source.poll().await.unwrap().messages.is_empty());
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn given_no_state_should_start_fresh() {
        let source = ElasticsearchSource::new(1, test_config("http://localhost:9200"), None);
        let state = source.state.lock().await;
        assert!(state.last_poll_timestamp.is_none());
        assert_eq!(state.total_documents_fetched, 0);
    }

    #[tokio::test]
    async fn given_invalid_state_should_start_fresh() {
        let source = ElasticsearchSource::new(
            1,
            test_config("http://localhost:9200"),
            Some(ConnectorState(b"invalid state".to_vec())),
        );
        let state = source.state.lock().await;
        assert!(state.last_poll_timestamp.is_none());
        assert_eq!(state.total_documents_fetched, 0);
    }

    #[tokio::test]
    async fn given_persisted_state_should_restore_cursor() {
        let source = ElasticsearchSource::new(1, test_config("http://localhost:9200"), None);
        let mut state = source.state.lock().await.clone();
        state.last_poll_timestamp = Some("2026-01-01T00:00:00Z".parse().unwrap());
        state.total_documents_fetched = 12;
        let restored = ElasticsearchSource::new(
            1,
            test_config("http://localhost:9200"),
            source.serialize_state(&state),
        );
        let restored = restored.state.lock().await;
        assert_eq!(restored.last_poll_timestamp, state.last_poll_timestamp);
        assert_eq!(restored.total_documents_fetched, 12);
    }

    #[tokio::test]
    async fn state_should_be_serializable_and_deserializable() {
        let source = ElasticsearchSource::new(1, test_config("http://localhost:9200"), None);
        let mut state = source.state.lock().await.clone();
        state.poll_count = 7;
        state.last_document_id = Some("record_1".to_string());
        state.error_count = 2;
        state.last_error = Some("backend unavailable".to_string());
        state.processing_stats.successful_polls_count = 5;
        let restored = source
            .serialize_state(&state)
            .unwrap()
            .deserialize::<State>(CONNECTOR_NAME, 1)
            .unwrap();
        assert_eq!(
            serde_json::to_value(restored).unwrap(),
            serde_json::to_value(state).unwrap()
        );
    }

    #[tokio::test]
    async fn given_empty_and_nonempty_polls_should_average_each_poll_once() {
        let source = ElasticsearchSource::new(1, test_config("http://localhost:9200"), None);
        let mut state = source.state.lock().await;
        let stats = &mut state.processing_stats;
        for (milliseconds, empty) in [(100, false), (200, true), (300, false), (400, true)] {
            stats.record_success(std::time::Duration::from_millis(milliseconds), empty);
        }
        assert_eq!(stats.successful_polls_count, 4);
        assert_eq!(stats.empty_polls_count, 2);
        assert_eq!(stats.avg_batch_processing_time_ms, 250.0);
    }

    #[tokio::test]
    async fn given_fetched_documents_should_count_payload_bytes_including_retries() {
        let server = source_backend().await;
        let mut source = ElasticsearchSource::new(1, test_config(&server.uri()), None);
        source.open().await.unwrap();
        let first = source.poll().await.unwrap();
        assert_eq!(first.messages.len(), 1);
        let payload_bytes = first.messages[0].payload.len() as u64;
        assert_eq!(
            source
                .state
                .lock()
                .await
                .processing_stats
                .total_bytes_processed,
            payload_bytes
        );

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .unwrap();
        let retry = source.poll().await.unwrap();
        assert_eq!(retry.messages.len(), 1);
        assert_eq!(
            source
                .state
                .lock()
                .await
                .processing_stats
                .total_bytes_processed,
            payload_bytes * 2
        );
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .unwrap();
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn given_incomplete_search_should_preserve_cursor_and_retry() {
        for mut response in [
            json!({"timed_out": true}),
            json!({"_shards": {"failed": 1}}),
        ] {
            let server = source_backend().await;
            response["hits"] =
                json!({"hits": [{"_source": {"timestamp": "2026-01-01T00:00:00Z", "value": 1}}]});
            Mock::given(method("POST"))
                .and(path("/logs/_search"))
                .respond_with(ResponseTemplate::new(200).set_body_json(&response))
                .with_priority(1)
                .up_to_n_times(1)
                .mount(&server)
                .await;
            let mut source = ElasticsearchSource::new(1, test_config(&server.uri()), None);
            source.open().await.unwrap();

            assert!(
                source.poll().await.is_err(),
                "Incomplete response must fail: {response}"
            );
            let state = source.state.lock().await;
            assert!(state.last_poll_timestamp.is_none());
            assert_eq!(state.total_documents_fetched, 0);
            assert_eq!(state.error_count, 1);
            drop(state);

            let retry = source.poll().await.unwrap();
            assert_eq!(retry.messages.len(), 1);
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .unwrap();
            source.close().await.unwrap();
        }
    }
}
