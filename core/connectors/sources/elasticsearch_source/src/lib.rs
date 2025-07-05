/* Licensed to the Apache Software Foundation (ASF) under one
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
use elasticsearch::{Elasticsearch, http::transport::Transport, SearchParts};
use iggy_connector_sdk::{
    Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{error, info, warn};

source_connector!(ElasticsearchSource);

#[derive(Debug)]
struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
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
    pub fn new(id: u32, config: ElasticsearchSourceConfig) -> Self {
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
            }),
        }
    }

    async fn create_client(&self) -> Result<Elasticsearch, Error> {
        let mut transport_builder = Transport::single_node(&self.config.url)
            .map_err(|e| Error::Connection(format!("Failed to create transport: {}", e)))?;

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            transport_builder = transport_builder.auth(elasticsearch::auth::Credentials::Basic(
                username.clone(),
                password.clone(),
            ));
        }

        let transport = transport_builder
            .build()
            .map_err(|e| Error::Connection(format!("Failed to build transport: {}", e)))?;

        Ok(Elasticsearch::new(transport))
    }

    async fn search_documents(&self, client: &Elasticsearch) -> Result<Vec<ProducedMessage>, Error> {
        let mut state = self.state.lock().await;
        let batch_size = self.config.batch_size.unwrap_or(100);

        // Build query based on timestamp field if configured
        let mut query = self.config.query.clone().unwrap_or_else(|| json!({
            "match_all": {}
        }));

        // Add timestamp filter for incremental polling
        if let Some(timestamp_field) = &self.config.timestamp_field {
            if let Some(last_timestamp) = state.last_poll_timestamp {
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
            .map_err(|e| Error::Connection(format!("Failed to execute search: {}", e)))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Connection(format!(
                "Search request failed: {}",
                error_text
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Connection(format!("Failed to parse search response: {}", e)))?;

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
                    if let Some(timestamp_field) = &self.config.timestamp_field {
                        if let Some(timestamp_str) = source.get(timestamp_field).and_then(|v| v.as_str()) {
                            if let Ok(timestamp) = DateTime::parse_from_rfc3339(timestamp_str) {
                                let timestamp_utc = timestamp.with_timezone(&Utc);
                                if latest_timestamp.is_none() || timestamp_utc > latest_timestamp.unwrap() {
                                    latest_timestamp = Some(timestamp_utc);
                                }
                            }
                        }
                    }

                    // Create message from document
                    let payload = serde_json::to_vec(source)
                        .map_err(|e| Error::Serialization(format!("Failed to serialize document: {}", e)))?;

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
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self.config.index]))
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to check index existence: {}", e)))?;

        if !response.status_code().is_success() {
            return Err(Error::Connection(format!(
                "Index '{}' does not exist or is not accessible",
                self.config.index
            )));
        }

        self.client = Some(client);

        info!(
            "Successfully opened Elasticsearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.polling_interval).await;

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("Elasticsearch client not initialized".to_string())
        })?;

        let messages = self.search_documents(client).await?;

        let state = self.state.lock().await;
        info!(
            "Elasticsearch source connector with ID: {} polled {} documents (total: {}, polls: {})",
            self.id,
            messages.len(),
            state.total_documents_fetched,
            state.poll_count
        );
        drop(state);

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch source connector with ID: {} is closing. Stats: {} total documents fetched, {} polls executed",
            self.id, state.total_documents_fetched, state.poll_count
        );
        drop(state);

        self.client = None;
        info!("Elasticsearch source connector with ID: {} is closed.", self.id);
        Ok(())
    }
}