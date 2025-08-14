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
use elasticsearch::{Elasticsearch, http::transport::Transport, BulkParts, IndexParts};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use chrono::Utc;
use base64;

sink_connector!(ElasticsearchSink);

#[derive(Debug)]
struct State {
    invocations_count: usize,
    documents_indexed: usize,
    errors_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub batch_size: Option<usize>,
    pub timeout_seconds: Option<u64>,
    pub create_index_if_not_exists: Option<bool>,
    pub index_mapping: Option<Value>,
}

#[derive(Debug)]
pub struct ElasticsearchSink {
    id: u32,
    config: ElasticsearchSinkConfig,
    client: Option<Elasticsearch>,
    state: Mutex<State>,
}

impl ElasticsearchSink {
    pub fn new(id: u32, config: ElasticsearchSinkConfig) -> Self {
        ElasticsearchSink {
            id,
            config,
            client: None,
            state: Mutex::new(State {
                invocations_count: 0,
                documents_indexed: 0,
                errors_count: 0,
            }),
        }
    }

    async fn create_client(&self) -> Result<Elasticsearch, Error> {
        let url = elasticsearch::http::Url::parse(&self.config.url)
            .map_err(|e| Error::Connection(format!("Invalid Elasticsearch URL: {}", e)))?;

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

    async fn ensure_index_exists(&self, client: &Elasticsearch) -> Result<(), Error> {
        if !self.config.create_index_if_not_exists.unwrap_or(true) {
            return Ok(());
        }

        let response = client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self.config.index]))
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to check index existence: {}", e)))?;

        if response.status_code().is_success() {
            info!("Index '{}' already exists", self.config.index);
            return Ok(());
        }

        let mut create_request = client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(&self.config.index));

        if let Some(mapping) = &self.config.index_mapping {
            create_request = create_request.body(mapping.clone());
        }

        let response = create_request
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to create index: {}", e)))?;

        if response.status_code().is_success() {
            info!("Successfully created index '{}'", self.config.index);
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Connection(format!(
                "Failed to create index '{}': {}",
                self.config.index, error_text
            )));
        }

        Ok(())
    }

    async fn bulk_index_documents(&self, client: &Elasticsearch, documents: Vec<Value>) -> Result<(), Error> {
        if documents.is_empty() {
            return Ok(());
        }

        let mut body = Vec::new();
        for doc in documents {
            // Add index action
            body.push(json!({
                "index": {
                    "_index": self.config.index
                }
            }));
            // Add document
            body.push(doc);
        }

        let response = client
            .bulk(BulkParts::None)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to execute bulk request: {}", e)))?;

        if !response.status_code().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Connection(format!(
                "Bulk indexing failed: {}",
                error_text
            )));
        }

        let response_body: Value = response
            .json()
            .await
            .map_err(|e| Error::Connection(format!("Failed to parse bulk response: {}", e)))?;

        // Check for individual document errors
        if let Some(items) = response_body.get("items").and_then(|v| v.as_array()) {
            let mut errors = 0;
            for item in items {
                if let Some(index_result) = item.get("index") {
                    if let Some(error) = index_result.get("error") {
                        warn!("Document indexing error: {}", error);
                        errors += 1;
                    }
                }
            }

            let mut state = self.state.lock().await;
            state.errors_count += errors;
            state.documents_indexed += items.len() - errors;
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for ElasticsearchSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Elasticsearch sink connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client().await?;
        self.ensure_index_exists(&client).await?;
        self.client = Some(client);

        info!(
            "Successfully opened Elasticsearch sink connector with ID: {}",
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
            "Elasticsearch sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );

        let client = self.client.as_ref().ok_or_else(|| {
            Error::Connection("Elasticsearch client not initialized".to_string())
        })?;

        let mut documents = Vec::with_capacity(messages.len());
        for message in messages {
            let mut doc = match message.payload {
                Payload::Json(value) => value,
                Payload::Binary(bytes) => {
                    // Try to parse binary as JSON
                    match serde_json::from_slice::<Value>(&bytes) {
                        Ok(value) => value,
                        Err(_) => {
                            // If not JSON, create a document with the binary data as base64
                            json!({
                                "data": base64::encode(&bytes),
                                "data_type": "binary"
                            })
                        }
                    }
                }
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                    continue;
                }
            };

            // Add metadata fields
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_iggy_offset".to_string(), json!(message.offset));
                obj.insert("_iggy_stream".to_string(), json!(topic_metadata.stream));
                obj.insert("_iggy_topic".to_string(), json!(topic_metadata.topic));
                obj.insert("_iggy_partition".to_string(), json!(messages_metadata.partition_id));
                obj.insert("_iggy_timestamp".to_string(), json!(chrono::Utc::now().timestamp_millis()));

                if let Some(headers) = &message.headers {
                    obj.insert("_iggy_headers".to_string(), json!(headers));
                }
            }

            documents.push(doc);
        }

        if !documents.is_empty() {
            self.bulk_index_documents(client, documents).await?;
            info!(
                "Successfully indexed {} documents to Elasticsearch index '{}'",
                messages.len(),
                self.config.index
            );
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Elasticsearch sink connector with ID: {} is closing. Stats: {} invocations, {} documents indexed, {} errors",
            self.id, state.invocations_count, state.documents_indexed, state.errors_count
        );
        drop(state);

        self.client = None;
        info!("Elasticsearch sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}