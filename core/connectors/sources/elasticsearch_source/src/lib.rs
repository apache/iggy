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
use chrono::{DateTime, Utc};
use elasticsearch::{
    Elasticsearch, SearchParts,
    auth::Credentials,
    http::{Url, transport::TransportBuilder},
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
    /// Last document ID processed (for cursor-based pagination)
    last_document_id: Option<String>,
    /// Last scroll ID (for scroll-based pagination)
    last_scroll_id: Option<String>,
    /// Last processed offset
    last_offset: Option<u64>,
    /// Error count and last error
    error_count: usize,
    last_error: Option<String>,
    /// Processing statistics
    processing_stats: ProcessingStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingStats {
    /// Total bytes processed
    total_bytes_processed: u64,
    /// Average processing time per batch
    avg_batch_processing_time_ms: f64,
    /// Last successful processing timestamp
    last_successful_poll: Option<DateTime<Utc>>,
    /// Number of empty polls
    empty_polls_count: usize,
    /// Number of successful polls
    successful_polls_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Enable state persistence
    pub enabled: bool,
    /// State storage type: "file", "elasticsearch", "redis", etc.
    pub storage_type: Option<String>,
    /// State storage configuration (depends on storage_type)
    pub storage_config: Option<Value>,
    /// State ID for this connector instance
    pub state_id: Option<String>,
    /// Auto-save state interval (e.g., "30s", "5m")
    pub auto_save_interval: Option<String>,
    /// Fields to track in state (e.g., ["last_timestamp", "last_document_id"])
    pub tracked_fields: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
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
}

impl ElasticsearchSource {
    pub fn new(id: u32, config: ElasticsearchSourceConfig, _state: Option<ConnectorState>) -> Self {
        let polling_interval = config
            .polling_interval
            .as_deref()
            .unwrap_or("10s")
            .parse::<humantime::Duration>()
            .unwrap_or_else(|_| humantime::Duration::from_str("10s").unwrap())
            .into();

        ElasticsearchSource {
            id,
            config,
            client: None,
            polling_interval,
            state: Mutex::new(State {
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
            }),
        }
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
            let credentials = Credentials::Basic(username.clone(), password.clone());
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
    ) -> Result<Vec<ProducedMessage>, Error> {
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

        let mut messages = Vec::new();
        let mut latest_timestamp = None;

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
        state.poll_count += 1;
        if let Some(timestamp) = latest_timestamp {
            state.last_poll_timestamp = Some(timestamp);
        }

        Ok(messages)
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

        let messages = match self.search_documents(client).await {
            Ok(msgs) => {
                // Update success statistics
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

                drop(state);
                msgs
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
        let state_value = {
            let state = self.state.lock().await;
            serde_json::to_vec(&*state).map_err(|err| {
                Error::Serialization(format!("Failed to serialize state: {}", err))
            })?
        };
        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(ConnectorState(state_value)),
        })
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
