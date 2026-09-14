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
use base64::{Engine as _, engine::general_purpose};
use elasticsearch::{
    BulkParts, Elasticsearch,
    auth::Credentials,
    http::{Url, request::JsonBody, transport::TransportBuilder},
};
use iggy_common::IggyTimestamp;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::json;
use simd_json::{OwnedValue, prelude::*};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

sink_connector!(ElasticsearchSink);

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

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
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub batch_size: Option<usize>,
    /// Client-wide HTTP timeout for open() and bulk consume(). Values of `0`
    /// are clamped to 1s (a zero duration would fail every request immediately).
    /// Raise this for slow bulk workloads; until runtime ack/retry (#2927/#2928),
    /// a timeout on consume drops the batch after the poll offset is already
    /// committed.
    pub timeout_seconds: Option<u64>,
    pub create_index_if_not_exists: Option<bool>,
    pub index_mapping: Option<serde_json::Value>,
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
        let url = Url::parse(&self.config.url)
            .map_err(|error| Error::Connection(format!("Invalid Elasticsearch URL: {error}")))?;

        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(url);
        // elasticsearch-rs defaults to no timeout. This client-global timeout is
        // an infinite-hang backstop for open() and for bulk consume() — not the
        // primary #3728 flake fix (that is the harness readiness gate, which
        // expires sooner than the 30s default). Values of 0 clamp to 1s.
        let timeout_seconds = self
            .config
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECONDS)
            .max(1);
        let mut transport_builder =
            TransportBuilder::new(conn_pool).timeout(Duration::from_secs(timeout_seconds));

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
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
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self
                .config
                .index]))
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to check index existence: {}", e)))?;

        if response.status_code().is_success() {
            info!("Index '{}' already exists", self.config.index);
            return Ok(());
        }

        let response = if let Some(mapping) = &self.config.index_mapping {
            client
                .indices()
                .create(elasticsearch::indices::IndicesCreateParts::Index(
                    &self.config.index,
                ))
                .body(mapping.clone())
                .send()
                .await
                .map_err(|e| Error::Connection(format!("Failed to create index: {}", e)))?
        } else {
            client
                .indices()
                .create(elasticsearch::indices::IndicesCreateParts::Index(
                    &self.config.index,
                ))
                .send()
                .await
                .map_err(|e| Error::Connection(format!("Failed to create index: {}", e)))?
        };

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

    async fn bulk_index_documents(
        &self,
        client: &Elasticsearch,
        documents: Vec<OwnedValue>,
    ) -> Result<usize, Error> {
        if documents.is_empty() {
            return Ok(0);
        }

        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(documents.len() * 2);
        for doc in documents {
            // Add index action
            body.push(
                json!({
                    "index": {
                        "_index": self.config.index
                    }
                })
                .into(),
            );
            let doc_json: serde_json::Value = owned_value_to_serde_json(&doc);
            body.push(doc_json.into());
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

        let response_body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| Error::Connection(format!("Failed to parse bulk response: {}", e)))?;

        // A 200 without an items array is not a bulk response this connector
        // can account for, so it must not be read as "nothing was indexed".
        let Some(items) = response_body.get("items").and_then(|v| v.as_array()) else {
            return Err(Error::Connection(format!(
                "Elasticsearch bulk response for index '{}' carried no items array",
                self.config.index
            )));
        };

        let mut errors = 0;
        let mut retryable_rejection = false;
        for item in items {
            if let Some(index_result) = item.get("index")
                && let Some(error) = index_result.get("error")
            {
                warn!("Document indexing error: {error}");
                errors += 1;
                retryable_rejection |= index_result
                    .get("status")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|status| status == 429 || status >= 500);
            }
        }

        let documents_indexed = items.len() - errors;
        {
            let mut state = self.state.lock().await;
            state.errors_count += errors;
            state.documents_indexed += documents_indexed;
        }

        if errors > 0 {
            // The runtime counts a batch, not a document, so a partial rejection
            // is invisible in `/stats`. This line is the only per-batch record.
            error!(
                "Elasticsearch rejected {errors} of {} documents in index '{}'",
                items.len(),
                self.config.index
            );
        }

        if documents_indexed == 0 {
            let reason = format!(
                "Elasticsearch bulk request indexed no documents in index '{}'",
                self.config.index
            );
            // Bad data must not look like a connectivity failure to a circuit
            // breaker, but a 429 or 5xx rejection is transient and is not
            // permanent. See `Error::PermanentHttpError`.
            return Err(if retryable_rejection {
                Error::CannotStoreData(reason)
            } else {
                Error::PermanentHttpError(reason)
            });
        }

        Ok(documents_indexed)
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

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Elasticsearch client not initialized".to_string()))?;

        let messages_count = messages.len();
        let mut documents = Vec::with_capacity(messages_count);
        for message in messages {
            let mut doc = match message.payload {
                Payload::Json(value) => value,
                Payload::Raw(bytes) => {
                    let mut bytes_copy = bytes.clone();
                    match simd_json::from_slice::<OwnedValue>(&mut bytes_copy) {
                        Ok(value) => value,
                        Err(_) => {
                            simd_json::json!({
                                "data": general_purpose::STANDARD.encode(&bytes),
                                "data_type": "raw"
                            })
                        }
                    }
                }
                Payload::Text(text) => simd_json::json!({
                    "text": text,
                    "data_type": "text"
                }),
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                    continue;
                }
            };

            // Add metadata fields
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("_iggy_offset".to_string(), OwnedValue::from(message.offset));
                obj.insert(
                    "_iggy_stream".to_string(),
                    OwnedValue::from(topic_metadata.stream.as_str()),
                );
                obj.insert(
                    "_iggy_topic".to_string(),
                    OwnedValue::from(topic_metadata.topic.as_str()),
                );
                obj.insert(
                    "_iggy_partition".to_string(),
                    OwnedValue::from(messages_metadata.partition_id),
                );
                obj.insert(
                    "_iggy_timestamp".to_string(),
                    OwnedValue::from(IggyTimestamp::now().as_millis() as i64),
                );

                if let Some(headers) = &message.headers {
                    // Convert headers to simd_json value
                    let headers_json = serde_json::to_string(headers).unwrap_or_default();
                    let mut headers_bytes = headers_json.into_bytes();
                    if let Ok(headers_value) =
                        simd_json::from_slice::<OwnedValue>(&mut headers_bytes)
                    {
                        obj.insert("_iggy_headers".to_string(), headers_value);
                    }
                }
            }

            documents.push(doc);
        }

        if !documents.is_empty() {
            let documents_indexed = self.bulk_index_documents(client, documents).await?;
            info!(
                "Successfully indexed {} documents to Elasticsearch index '{}'",
                documents_indexed, self.config.index
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
        info!(
            "Elasticsearch sink connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn test_config() -> ElasticsearchSinkConfig {
        ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            index: "test".to_string(),
            username: None,
            password: None,
            batch_size: None,
            timeout_seconds: Some(1),
            create_index_if_not_exists: Some(false),
            index_mapping: None,
        }
    }

    #[test]
    fn given_bulk_item_results_when_indexing_should_fail_only_fully_rejected_batches() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let accepted = json!({"index": {"status": 201}});
            let rejected = json!({"index": {
                "status": 400,
                "error": {"type": "document_parsing_exception", "reason": "invalid document"}
            }});
            for (items, expected_indexed) in [
                (vec![rejected.clone(), rejected.clone()], 0),
                (vec![accepted.clone(), rejected], 1),
                (vec![accepted.clone(), accepted], 2),
            ] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "errors": expected_indexed < items.len(),
                        "items": items,
                    })))
                    .expect(1)
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink
                    .create_client()
                    .await
                    .expect("client should initialize");
                let result = sink
                    .bulk_index_documents(
                        &client,
                        vec![simd_json::json!({"id": 1}), simd_json::json!({"id": 2})],
                    )
                    .await;

                if expected_indexed == 0 {
                    assert!(
                        matches!(result, Err(Error::PermanentHttpError(_))),
                        "{result:?}"
                    );
                } else {
                    assert_eq!(result, Ok(expected_indexed));
                }
                let state = sink.state.lock().await;
                assert_eq!(state.documents_indexed, expected_indexed);
                assert_eq!(state.errors_count, items.len() - expected_indexed);
            }
        });
    }

    #[test]
    fn given_transient_item_rejections_when_indexing_should_not_report_a_permanent_error() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            for status in [429, 503] {
                let server = MockServer::start().await;
                Mock::given(method("POST"))
                    .and(path("/_bulk"))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                        "errors": true,
                        "items": [json!({"index": {
                            "status": status,
                            "error": {"type": "es_rejected_execution_exception"}
                        }})],
                    })))
                    .expect(1)
                    .mount(&server)
                    .await;
                let mut config = test_config();
                config.url = server.uri();
                let sink = ElasticsearchSink::new(1, config);
                let client = sink
                    .create_client()
                    .await
                    .expect("client should initialize");

                let result = sink
                    .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                    .await;

                assert!(
                    matches!(result, Err(Error::CannotStoreData(_))),
                    "status {status}: {result:?}"
                );
            }
        });
    }

    #[test]
    fn given_a_bulk_response_without_items_when_indexing_should_report_a_connection_error() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/_bulk"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"took": 1})))
                .expect(1)
                .mount(&server)
                .await;
            let mut config = test_config();
            config.url = server.uri();
            let sink = ElasticsearchSink::new(1, config);
            let client = sink
                .create_client()
                .await
                .expect("client should initialize");

            let result = sink
                .bulk_index_documents(&client, vec![simd_json::json!({"id": 1})])
                .await;

            assert!(matches!(result, Err(Error::Connection(_))), "{result:?}");
            let state = sink.state.lock().await;
            assert_eq!(state.documents_indexed, 0);
            assert_eq!(state.errors_count, 0);
        });
    }

    #[test]
    fn given_empty_batch_when_indexing_should_succeed_without_a_request() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime should start");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut config = test_config();
            config.url = server.uri();
            let sink = ElasticsearchSink::new(1, config);
            let client = sink
                .create_client()
                .await
                .expect("client should initialize");

            assert_eq!(sink.bulk_index_documents(&client, Vec::new()).await, Ok(0));
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests should be recorded")
                    .is_empty()
            );
        });
    }
}
