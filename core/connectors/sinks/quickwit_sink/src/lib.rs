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
use iggy_connector_sdk::retry::{
    ConnectivityConfig, build_retry_client, check_connectivity_with_retry, parse_duration,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Schema, Sink, TopicMetadata, sink_connector,
};
use reqwest::StatusCode;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use tracing::{debug, error, info, warn};

sink_connector!(QuickwitSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "200ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "30s";
const DEFAULT_REQUEST_TIMEOUT: &str = "30s";

#[derive(Debug)]
pub struct QuickwitSink {
    id: u32,
    config: QuickwitSinkConfig,
    client: Option<ClientWithMiddleware>,
    verbose: bool,
    index_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSinkConfig {
    pub url: String,
    /// Full Quickwit index config YAML, passed to `POST /api/v1/indexes` on first open.
    /// `index_id` is extracted from this YAML to build ingest URLs.
    pub index: String,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub request_timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexIdExtract {
    index_id: String,
}

impl QuickwitSink {
    pub fn new(id: u32, config: QuickwitSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        Self {
            id,
            config,
            client: None,
            verbose,
            index_id: String::new(),
        }
    }

    fn client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::InitError("Quickwit sink client not initialized".into()))
    }

    async fn has_index(&self) -> Result<bool, Error> {
        let client = self.client()?;
        let url = format!("{}/api/v1/indexes/{}", self.config.url, self.index_id);
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::HttpRequestFailed(e.to_string()))?;
        let status = response.status();
        if status.is_success() {
            Ok(true)
        } else if status == StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            Err(Error::HttpRequestFailed(format!(
                "Unexpected status checking Quickwit index: {status}"
            )))
        }
    }

    async fn create_index(&self) -> Result<(), Error> {
        info!(
            "Creating Quickwit index: {} for connector ID: {}",
            self.index_id, self.id
        );
        let client = self.client()?;
        let url = format!("{}/api/v1/indexes", self.config.url);
        let response = client
            .post(&url)
            .header("Content-Type", "application/yaml")
            .body(self.config.index.clone())
            .send()
            .await
            .map_err(|e| {
                error!(
                    "Failed to create Quickwit index: {} for connector ID: {}. {e}",
                    self.index_id, self.id
                );
                Error::HttpRequestFailed(e.to_string())
            })?;

        let status = response.status();
        if status.is_success() {
            info!(
                "Created Quickwit index: {} for connector ID: {}",
                self.index_id, self.id
            );
            Ok(())
        } else if status == StatusCode::CONFLICT {
            // Another instance beat us to it; the index exists, which is what we want.
            info!(
                "Quickwit index already exists (409): {} for connector ID: {}",
                self.index_id, self.id
            );
            Ok(())
        } else if status.is_client_error() {
            let reason = response.text().await.unwrap_or_default();
            error!(
                "Permanent client error creating Quickwit index: {} for connector ID: {}. status: {status}, reason: {reason}",
                self.index_id, self.id
            );
            Err(Error::InitError(format!(
                "Failed to create index '{0}': {status} {reason}",
                self.index_id
            )))
        } else {
            let reason = response.text().await.unwrap_or_default();
            error!(
                "Server error creating Quickwit index: {} for connector ID: {}. status: {status}, reason: {reason}",
                self.index_id, self.id
            );
            Err(Error::InitError(format!(
                "Failed to create index '{0}': {status} {reason}",
                self.index_id
            )))
        }
    }

    pub async fn ingest(&self, messages: Vec<simd_json::OwnedValue>) -> Result<(), Error> {
        let client = self.client()?;
        // At-least-once: Quickwit ingest carries no dedup key, so a retry after a
        // 5xx/timeout that actually committed (commit=auto) double-writes those rows.
        let url = format!(
            "{}/api/v1/{}/ingest?commit=auto",
            self.config.url, self.index_id
        );
        let messages_count = messages.len();
        let mut ndjson = String::new();
        for record in messages {
            if let Ok(json_str) = simd_json::to_string(&record) {
                if !ndjson.is_empty() {
                    ndjson.push('\n');
                }
                ndjson.push_str(&json_str);
            }
        }

        let response = client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .map_err(|e| {
                error!(
                    "Failed to ingest {messages_count} messages into Quickwit index: {} for connector ID: {}. {e}",
                    self.index_id, self.id
                );
                Error::HttpRequestFailed(e.to_string())
            })?;

        let status = response.status();
        if status.is_success() {
            debug!(
                "Ingested {messages_count} messages into Quickwit index: {} for connector ID: {}",
                self.index_id, self.id
            );
            Ok(())
        } else if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
            let text = response.text().await.unwrap_or_default();
            error!(
                "Permanent error ingesting into Quickwit index: {} for connector ID: {}. status: {status}, reason: {text}",
                self.index_id, self.id
            );
            Err(Error::PermanentHttpError(format!(
                "status: {status}, reason: {text}"
            )))
        } else {
            let text = response.text().await.unwrap_or_default();
            error!(
                "Transient error ingesting into Quickwit index: {} for connector ID: {}. status: {status}, reason: {text}",
                self.index_id, self.id
            );
            Err(Error::HttpRequestFailed(format!(
                "status: {status}, reason: {text}"
            )))
        }
    }

    fn extract_json_payloads(
        &self,
        messages: Vec<ConsumedMessage>,
        schema: Schema,
    ) -> Vec<OwnedValue> {
        let mut json_payloads = Vec::with_capacity(messages.len());
        for message in messages {
            let val = match message.payload {
                Payload::Json(value) => value,
                Payload::Raw(bytes) => {
                    let mut bytes_copy = bytes.clone();
                    match simd_json::from_slice::<OwnedValue>(&mut bytes_copy) {
                        Ok(value) => value,
                        Err(_) => {
                            if let Ok(text) = String::from_utf8(bytes.clone()) {
                                simd_json::json!({
                                    "data": text,
                                    "data_type": "raw"
                                })
                            } else {
                                simd_json::json!({
                                    "data": general_purpose::STANDARD.encode(&bytes),
                                    "data_type": "raw"
                                })
                            }
                        }
                    }
                }
                Payload::Text(text) => simd_json::json!({
                    "text": text,
                    "data_type": "text"
                }),
                _ => {
                    warn!(
                        "Quickwit sink connector ID: {} unsupported payload schema: {}",
                        self.id, schema
                    );
                    continue;
                }
            };
            json_payloads.push(val);
        }
        json_payloads
    }
}

#[async_trait]
impl Sink for QuickwitSink {
    async fn open(&mut self) -> Result<(), Error> {
        let parsed: IndexIdExtract = serde_yaml_ng::from_str(&self.config.index)
            .map_err(|e| Error::InvalidConfigValue(format!("index: invalid YAML: {e}")))?;
        self.index_id = parsed.index_id;

        let retry_delay = parse_duration(self.config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay = parse_duration(
            self.config.max_retry_delay.as_deref(),
            DEFAULT_MAX_RETRY_DELAY,
        );
        let max_open_retries = self
            .config
            .max_open_retries
            .unwrap_or(DEFAULT_MAX_OPEN_RETRIES);
        let open_retry_max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

        let request_timeout = parse_duration(
            self.config.request_timeout.as_deref(),
            DEFAULT_REQUEST_TIMEOUT,
        );
        let raw_client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|e| Error::InitError(format!("reqwest client: {e}")))?;
        let health_url = Url::parse(&format!("{}/health/livez", self.config.url))
            .map_err(|e| Error::InvalidConfigValue(format!("url: {e}")))?;

        check_connectivity_with_retry(
            &raw_client,
            health_url,
            "Quickwit sink connector",
            self.id,
            &ConnectivityConfig {
                max_open_retries,
                open_retry_max_delay,
                retry_delay,
            },
        )
        .await?;

        self.client = Some(build_retry_client(
            raw_client,
            self.config
                .max_retries
                .unwrap_or(DEFAULT_MAX_RETRIES)
                .max(1),
            retry_delay,
            max_retry_delay,
            "Quickwit",
        ));

        if !self.has_index().await? {
            self.create_index().await?;
        }

        info!(
            "Opened Quickwit sink connector ID: {}, index: {}",
            self.id, self.index_id
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();
        if self.verbose {
            info!(
                "Quickwit sink connector ID: {} received {total} messages, schema: {}",
                self.id, messages_metadata.schema
            );
        } else {
            debug!(
                "Quickwit sink connector ID: {} received {total} messages, schema: {}",
                self.id, messages_metadata.schema
            );
        }

        let json_payloads = self.extract_json_payloads(messages, messages_metadata.schema);
        if json_payloads.is_empty() {
            return Ok(());
        }

        self.ingest(json_payloads).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        let _ = self.client.take();
        info!("Closed Quickwit sink connector ID: {}", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> QuickwitSinkConfig {
        QuickwitSinkConfig {
            url: "http://localhost:7280".to_string(),
            index: "index_id: test\nversion: 0.8\n".to_string(),
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            max_open_retries: None,
            open_retry_max_delay: None,
            request_timeout: None,
        }
    }

    #[test]
    fn given_default_config_verbose_should_be_false() {
        let sink = QuickwitSink::new(1, test_config());
        assert!(!sink.verbose);
    }

    #[test]
    fn given_verbose_logging_enabled_should_set_verbose_flag() {
        let mut config = test_config();
        config.verbose_logging = Some(true);
        let sink = QuickwitSink::new(1, config);
        assert!(sink.verbose);
    }

    #[test]
    fn given_verbose_logging_disabled_should_not_set_verbose_flag() {
        let mut config = test_config();
        config.verbose_logging = Some(false);
        let sink = QuickwitSink::new(1, config);
        assert!(!sink.verbose);
    }

    #[test]
    fn given_new_sink_client_should_not_be_initialized() {
        let sink = QuickwitSink::new(1, test_config());
        assert!(sink.client.is_none());
    }

    #[test]
    fn given_new_sink_index_id_should_be_empty() {
        let sink = QuickwitSink::new(1, test_config());
        assert!(sink.index_id.is_empty());
    }

    fn test_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_json_payload_should_extract_it_directly() {
        let sink = QuickwitSink::new(1, test_config());
        let val = simd_json::json!({"key": "value"});
        let msg = test_message(Payload::Json(val.clone()));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::Json);
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0], val);
    }

    #[test]
    fn given_raw_json_payload_should_parse_and_extract_it() {
        let sink = QuickwitSink::new(1, test_config());
        let val = simd_json::json!({"key": "value"});
        let raw_bytes = simd_json::to_vec(&val).unwrap();
        let msg = test_message(Payload::Raw(raw_bytes));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::Raw);
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0], val);
    }

    #[test]
    fn given_raw_invalid_json_text_should_wrap_in_raw_object() {
        let sink = QuickwitSink::new(1, test_config());
        let raw_text = "invalid json text";
        let msg = test_message(Payload::Raw(raw_text.as_bytes().to_vec()));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::Raw);
        assert_eq!(extracted.len(), 1);
        assert_eq!(
            extracted[0],
            simd_json::json!({
                "data": raw_text,
                "data_type": "raw"
            })
        );
    }

    #[test]
    fn given_raw_binary_should_encode_base64_in_raw_object() {
        let sink = QuickwitSink::new(1, test_config());
        let binary_data = vec![0, 15, 255];
        let msg = test_message(Payload::Raw(binary_data.clone()));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::Raw);
        assert_eq!(extracted.len(), 1);
        assert_eq!(
            extracted[0],
            simd_json::json!({
                "data": general_purpose::STANDARD.encode(&binary_data),
                "data_type": "raw"
            })
        );
    }

    #[test]
    fn given_text_payload_should_wrap_in_text_object() {
        let sink = QuickwitSink::new(1, test_config());
        let text = "hello quickwit";
        let msg = test_message(Payload::Text(text.to_string()));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::Text);
        assert_eq!(extracted.len(), 1);
        assert_eq!(
            extracted[0],
            simd_json::json!({
                "text": text,
                "data_type": "text"
            })
        );
    }

    #[test]
    fn given_unsupported_payload_should_ignore_it() {
        let sink = QuickwitSink::new(1, test_config());
        let msg = test_message(Payload::FlatBuffer(vec![1, 2, 3]));
        let extracted = sink.extract_json_payloads(vec![msg], Schema::FlatBuffer);
        assert!(extracted.is_empty());
    }
}
