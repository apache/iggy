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

use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use iggy_connector_sdk::retry::{
    RetryPolicy, build_retry_client, check_connectivity_with_retry, is_transient_status,
    retry_async,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::StatusCode;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use simd_json::OwnedValue;
use tracing::{debug, error, info};

sink_connector!(QuickwitSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "30s";
const DEFAULT_TIMEOUT: &str = "30s";
const MAX_INGEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const ESTIMATED_RECORD_BYTES: usize = 512;

#[derive(Debug)]
pub struct QuickwitSink {
    id: u32,
    config: QuickwitSinkConfig,
    client: Option<ClientWithMiddleware>,
    verbose: bool,
    index_id: String,
    indexes_url: String,
    index_url: String,
    ingest_url: String,
}

/// Configuration for the Quickwit sink connector, deserialized from `[plugin_config]` in config.toml.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuickwitSinkConfig {
    /// Target URL for the Quickwit service.
    pub url: String,
    /// Full Quickwit index config YAML, passed to `POST /api/v1/indexes` on first open.
    /// `index_id` is extracted from this YAML to build ingest URLs.
    pub index: String,
    /// Enable verbose logging for ingested messages (default: false).
    pub verbose_logging: Option<bool>,
    /// Total HTTP attempts including the first (default: 3). 1 disables retries.
    pub max_retries: Option<u32>,
    /// Initial retry delay as a human-readable duration string, e.g. "1s" (default: 1s).
    pub retry_delay: Option<String>,
    /// Maximum retry delay cap as a human-readable duration string, e.g. "5s" (default: 5s).
    pub retry_max_delay: Option<String>,
    /// Total attempts per readiness check including the first (default: 10). 1 disables retries.
    pub max_open_retries: Option<u32>,
    /// Maximum retry delay cap when opening the sink, e.g. "30s" (default: 30s).
    pub open_retry_max_delay: Option<String>,
    /// HTTP request timeout as a human-readable duration string, e.g. "30s" (default: 30s).
    pub timeout: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexConfig {
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
            indexes_url: String::new(),
            index_url: String::new(),
            ingest_url: String::new(),
        }
    }

    fn client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::InitError("Quickwit sink client not initialized".into()))
    }

    async fn has_index(&self) -> Result<bool, Error> {
        let client = self.client()?;
        let response = client
            .get(&self.index_url)
            .send()
            .await
            .map_err(|e| Error::HttpRequestFailed(e.to_string()))?;
        let status = response.status();
        if status.is_success() {
            Ok(true)
        } else if status == StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            let reason = response
                .text()
                .await
                .unwrap_or_else(|error| format!("failed to read response: {error}"));
            Err(Error::InitError(format!(
                "Checking Quickwit index '{}': {status}, {reason}",
                self.index_id
            )))
        }
    }

    async fn create_index(&self) -> Result<(), Error> {
        info!(
            "Creating Quickwit index: {} for connector ID: {}",
            self.index_id, self.id
        );
        let client = self.client()?;
        let response = client
            .post(&self.indexes_url)
            .header("Content-Type", "application/yaml")
            .body(self.config.index.clone())
            .send()
            .await
            .map_err(|error| Error::HttpRequestFailed(error.to_string()))?;

        let status = response.status();
        if status.is_success() {
            info!(
                "Created Quickwit index: {} for connector ID: {}",
                self.index_id, self.id
            );
            Ok(())
        } else {
            let reason = response
                .text()
                .await
                .unwrap_or_else(|error| format!("failed to read response: {error}"));
            // A competing creator or a retried POST may already have created the index.
            if self.has_index().await? {
                info!(
                    "Quickwit index already exists ({status}): {} for connector ID: {}",
                    self.index_id, self.id
                );
                Ok(())
            } else {
                Err(Error::InitError(format!(
                    "Failed to create index '{0}': {status} {reason}",
                    self.index_id
                )))
            }
        }
    }

    async fn wait_for_index(
        &self,
        client: &reqwest::Client,
        policy: RetryPolicy,
    ) -> Result<(), Error> {
        // Index metadata can exist before Quickwit's ingest queue accepts documents.
        retry_async(
            policy,
            &format!("Quickwit sink ID {} ingest readiness", self.id),
            |error: &reqwest::Error| {
                error.status().is_none_or(|status| {
                    status == StatusCode::NOT_FOUND || is_transient_status(status)
                })
            },
            || async {
                client
                    .post(&self.ingest_url)
                    .header(reqwest::header::CONTENT_LENGTH, "0")
                    .body("")
                    .send()
                    .await?
                    .error_for_status()
                    .map(|_| ())
            },
        )
        .await
        .map_err(|failure| {
            Error::InitError(format!(
                "Quickwit index '{}' ingest readiness: {failure}",
                self.index_id
            ))
        })
    }

    async fn ingest(&self, messages: Vec<OwnedValue>) -> Result<(), Error> {
        let capacity = messages
            .len()
            .saturating_mul(ESTIMATED_RECORD_BYTES)
            .min(MAX_INGEST_BODY_BYTES);
        let mut body = Vec::with_capacity(capacity);
        let mut messages_count = 0;
        let mut last_error = None;
        for (position, record) in messages.into_iter().enumerate() {
            let previous_len = body.len();
            if let Err(error) = simd_json::to_writer(&mut body, &record) {
                body.truncate(previous_len);
                error!(
                    "Quickwit sink connector ID: {} failed to serialize record {position}: {error}",
                    self.id
                );
                last_error = Some(Error::Serialization(error.to_string()));
                continue;
            }
            body.push(b'\n');
            let record_len = body.len() - previous_len;
            if record_len > MAX_INGEST_BODY_BYTES {
                body.truncate(previous_len);
                let reason = format!(
                    "record {position} is {record_len} bytes, exceeding the {MAX_INGEST_BODY_BYTES}-byte ingest limit"
                );
                error!("Quickwit sink connector ID: {}: {reason}", self.id);
                last_error = Some(Error::InvalidRecordValue(reason));
                continue;
            }
            if body.len() > MAX_INGEST_BODY_BYTES {
                let next_body = body.split_off(previous_len);
                let completed_body = std::mem::replace(&mut body, next_body);
                if let Err(error) = self.ingest_batch(completed_body, messages_count).await {
                    last_error = Some(error);
                }
                messages_count = 0;
            }
            messages_count += 1;
        }
        if messages_count > 0
            && let Err(error) = self.ingest_batch(body, messages_count).await
        {
            last_error = Some(error);
        }
        last_error.map_or(Ok(()), Err)
    }

    async fn ingest_batch(&self, body: Vec<u8>, messages_count: usize) -> Result<(), Error> {
        let client = self.client()?;
        // Retries can duplicate accepted documents. Final failures are logged but
        // not redelivered because the runtime commits offsets when polling.
        // Error classification remains diagnostic across the sink FFI boundary.
        let response = client
            .post(&self.ingest_url)
            .header("Content-Type", "application/x-ndjson")
            .body(body)
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
            if self.verbose {
                info!(
                    "Ingested {messages_count} messages into Quickwit index: {} for connector ID: {}",
                    self.index_id, self.id
                );
            } else {
                debug!(
                    "Ingested {messages_count} messages into Quickwit index: {} for connector ID: {}",
                    self.index_id, self.id
                );
            }
            return Ok(());
        }

        let reason = response
            .text()
            .await
            .unwrap_or_else(|error| format!("failed to read response: {error}"));
        let transient = is_transient_status(status);
        let category = if transient { "Transient" } else { "Permanent" };
        error!(
            "{category} error ingesting into Quickwit index: {} for connector ID: {}. status: {status}, reason: {reason}",
            self.index_id, self.id
        );
        let reason = format!("status: {status}, reason: {reason}");
        if transient {
            Err(Error::HttpRequestFailed(reason))
        } else {
            Err(Error::PermanentHttpError(reason))
        }
    }

    fn extract_json_payloads(&self, messages: Vec<ConsumedMessage>) -> Vec<OwnedValue> {
        let mut json_payloads = Vec::with_capacity(messages.len());
        for message in messages {
            let val = match message.payload {
                Payload::Json(value @ OwnedValue::Object(_)) => value,
                Payload::Json(value) => simd_json::json!({
                    "data": value,
                    "data_type": "json"
                }),
                Payload::Raw(bytes) | Payload::Avro(bytes) | Payload::FlatBuffer(bytes) => {
                    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) == Some(&b'{') {
                        // SIMD parsing mutates its input, even on failure. Preserve the fallback.
                        let mut json_bytes = bytes.clone();
                        if let Ok(value @ OwnedValue::Object(_)) =
                            simd_json::from_slice::<OwnedValue>(&mut json_bytes)
                        {
                            json_payloads.push(value);
                            continue;
                        }
                    }
                    let (data, encoding) = match String::from_utf8(bytes) {
                        Ok(text) => (text, "utf8"),
                        Err(error) => (
                            general_purpose::STANDARD.encode(error.into_bytes()),
                            "base64",
                        ),
                    };
                    simd_json::json!({
                        "data": data,
                        "data_type": "raw",
                        "data_encoding": encoding
                    })
                }
                Payload::Text(text) | Payload::Proto(text) => simd_json::json!({
                    "text": text,
                    "data_type": "text"
                }),
            };
            json_payloads.push(val);
        }
        json_payloads
    }
}

#[async_trait]
impl Sink for QuickwitSink {
    async fn open(&mut self) -> Result<(), Error> {
        let result = async {
            let index_config = serde_yaml_ng::from_str::<IndexConfig>(&self.config.index)
                .map_err(|error| Error::InvalidConfigValue(format!("index: {error}")))?;
            if index_config.index_id.trim().is_empty() {
                return Err(Error::InvalidConfigValue(
                    "index_id must not be empty".into(),
                ));
            }
            let base_url = Url::parse(self.config.url.trim_end_matches('/'))
                .map_err(|error| Error::InvalidConfigValue(format!("url: {error}")))?;
            if !matches!(base_url.scheme(), "http" | "https")
                || !base_url.has_host()
                || base_url.query().is_some()
                || base_url.fragment().is_some()
            {
                return Err(Error::InvalidConfigValue(
                    "url must be an HTTP(S) base URL with a host and no query or fragment".into(),
                ));
            }
            let retry_delay = parse_duration(
                self.config.retry_delay.as_deref(),
                DEFAULT_RETRY_DELAY,
                "retry_delay",
            )?;
            let retry_max_delay = parse_duration(
                self.config.retry_max_delay.as_deref(),
                DEFAULT_RETRY_MAX_DELAY,
                "retry_max_delay",
            )?;
            let open_retry_max_delay = parse_duration(
                self.config.open_retry_max_delay.as_deref(),
                DEFAULT_OPEN_RETRY_MAX_DELAY,
                "open_retry_max_delay",
            )?;
            let timeout =
                parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT, "timeout")?;

            self.index_id = index_config.index_id;
            self.indexes_url = endpoint_url(&base_url, &["api", "v1", "indexes"])?.into();
            self.index_url =
                endpoint_url(&base_url, &["api", "v1", "indexes", &self.index_id])?.into();
            let mut ingest_url = endpoint_url(&base_url, &["api", "v1", &self.index_id, "ingest"])?;
            ingest_url.set_query(Some("commit=auto"));
            self.ingest_url = ingest_url.into();

            let raw_client = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|error| Error::InitError(format!("reqwest client: {error}")))?;
            let open_retry_policy = RetryPolicy {
                max_attempts: self
                    .config
                    .max_open_retries
                    .unwrap_or(DEFAULT_MAX_OPEN_RETRIES)
                    .max(1),
                base_delay: retry_delay,
                max_delay: open_retry_max_delay,
            };
            check_connectivity_with_retry(
                &raw_client,
                endpoint_url(&base_url, &["health", "readyz"])?,
                "Quickwit sink",
                self.id,
                open_retry_policy,
            )
            .await?;

            self.client = Some(build_retry_client(
                raw_client.clone(),
                self.config
                    .max_retries
                    .unwrap_or(DEFAULT_MAX_RETRIES)
                    .max(1),
                retry_delay,
                retry_max_delay,
                "Quickwit",
            ));
            if !self.has_index().await? {
                self.create_index().await?;
            }
            self.wait_for_index(&raw_client, open_retry_policy).await
        }
        .await;
        if let Err(error) = result {
            self.client = None;
            error!(
                "Failed to open Quickwit sink connector ID: {}: {error}",
                self.id
            );
            return Err(error);
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

        let json_payloads = self.extract_json_payloads(messages);
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

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let duration = humantime::parse_duration(value.unwrap_or(default))
        .map_err(|error| Error::InvalidConfigValue(format!("{field}: {error}")))?;
    if duration.is_zero() {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(duration)
}

fn endpoint_url(base_url: &Url, segments: &[&str]) -> Result<Url, Error> {
    let mut url = base_url.clone();
    url.path_segments_mut()
        .map_err(|()| Error::InvalidConfigValue("url cannot be used as a base URL".into()))?
        .pop_if_empty()
        .extend(segments);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iggy_connector_sdk::Schema;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const HTTP_READ_BUFFER_BYTES: usize = 8192;

    fn test_config() -> QuickwitSinkConfig {
        QuickwitSinkConfig {
            url: "http://localhost:7280".to_string(),
            index: "index_id: test\nversion: 0.8\n".to_string(),
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            max_open_retries: Some(1),
            open_retry_max_delay: None,
            timeout: Some("1s".to_string()),
        }
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
    fn given_invalid_index_config_when_opened_should_return_config_error() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for index in [
                "",
                "index_id: [",
                "version: 0.8",
                "index_id: ''",
                "index_id: '  '",
            ] {
                let mut config = test_config();
                config.index = index.to_string();
                let mut sink = QuickwitSink::new(1, config);
                assert!(
                    matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
                    "invalid index config should fail open: {index:?}"
                );
                assert!(sink.client.is_none());
            }
        });
    }

    #[test]
    fn given_invalid_url_when_opened_should_return_config_error() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for url in [
                "localhost:7280",
                "ftp://localhost:7280",
                "http://",
                "http://localhost:7280/?token=value",
                "http://localhost:7280/#fragment",
            ] {
                let mut config = test_config();
                config.url = url.to_string();
                let mut sink = QuickwitSink::new(1, config);
                assert!(
                    matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
                    "invalid URL should fail before probing: {url}"
                );
            }
        });
    }

    #[test]
    fn given_invalid_duration_when_opened_should_return_config_error() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for field in ["retry_delay", "retry_max_delay", "open_retry_max_delay", "timeout"] {
                for value in ["30", "0s"] {
                    let mut config = test_config();
                    let setting = match field {
                        "retry_delay" => &mut config.retry_delay,
                        "retry_max_delay" => &mut config.retry_max_delay,
                        "open_retry_max_delay" => &mut config.open_retry_max_delay,
                        "timeout" => &mut config.timeout,
                        _ => unreachable!(),
                    };
                    *setting = Some(value.to_string());
                    let mut sink = QuickwitSink::new(1, config);
                    assert!(
                        matches!(sink.open().await, Err(Error::InvalidConfigValue(reason)) if reason.contains(field)),
                        "invalid {field} should fail before probing: {value}"
                    );
                }
            }
        });
    }

    #[test]
    fn given_unknown_config_key_when_deserialized_should_reject_it() {
        let config = "url: http://localhost:7280\nindex: 'index_id: test'\nmax_retires: 1";
        assert!(serde_yaml_ng::from_str::<QuickwitSinkConfig>(config).is_err());
    }

    #[test]
    fn given_payload_variants_when_extracted_should_preserve_object_and_text_documents() {
        let sink = QuickwitSink::new(1, test_config());
        let object = simd_json::json!({"key": "value"});
        let raw_object = b" \n{\"key\": \"value\"}".to_vec();
        let messages = vec![
            test_message(Payload::Json(object.clone())),
            test_message(Payload::Raw(raw_object.clone())),
            test_message(Payload::Avro(raw_object.clone())),
            test_message(Payload::FlatBuffer(raw_object)),
            test_message(Payload::Text("hello quickwit".to_string())),
            test_message(Payload::Proto("hello quickwit".to_string())),
        ];
        let extracted = sink.extract_json_payloads(messages);
        assert_eq!(
            &extracted[..4],
            &[object.clone(), object.clone(), object.clone(), object]
        );
        let text = simd_json::json!({"text": "hello quickwit", "data_type": "text"});
        assert_eq!(&extracted[4..], &[text.clone(), text]);
    }

    #[test]
    fn given_raw_nonobjects_when_extracted_should_preserve_original_bytes() {
        let sink = QuickwitSink::new(1, test_config());
        for raw_text in [
            "42",
            "\"text\"",
            "[1,2]",
            "null",
            "true",
            "",
            "plain text",
            r#"{"message":"escaped\ntext","broken":}"#,
        ] {
            let extracted = sink.extract_json_payloads(vec![test_message(Payload::Raw(
                raw_text.as_bytes().to_vec(),
            ))]);
            assert_eq!(
                extracted,
                vec![simd_json::json!({
                    "data": raw_text,
                    "data_type": "raw",
                    "data_encoding": "utf8"
                })],
                "raw input should remain unchanged: {raw_text:?}"
            );
        }
        let binary = vec![0, 15, 255];
        let extracted =
            sink.extract_json_payloads(vec![test_message(Payload::Raw(binary.clone()))]);
        assert_eq!(
            extracted,
            vec![simd_json::json!({
                "data": general_purpose::STANDARD.encode(binary),
                "data_type": "raw",
                "data_encoding": "base64"
            })]
        );
    }

    #[test]
    fn given_json_nonobjects_when_extracted_should_wrap_original_values() {
        let sink = QuickwitSink::new(1, test_config());
        for value in [
            simd_json::json!(42),
            simd_json::json!("text"),
            simd_json::json!([1, 2]),
            simd_json::json!(null),
            simd_json::json!(true),
        ] {
            let extracted =
                sink.extract_json_payloads(vec![test_message(Payload::Json(value.clone()))]);
            assert_eq!(
                extracted,
                vec![simd_json::json!({"data": value, "data_type": "json"})]
            );
        }
    }

    #[test]
    fn given_index_creation_race_when_opened_should_verify_the_existing_index() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for status in [400, 409, 503] {
                let (url, server) = start_test_server(vec![
                    ("GET /prefix/health/readyz HTTP/1.1", 200, ""),
                    ("GET /prefix/api/v1/indexes/test HTTP/1.1", 404, ""),
                    (
                        "POST /prefix/api/v1/indexes HTTP/1.1",
                        status,
                        "index `test` already exist(s)",
                    ),
                    ("GET /prefix/api/v1/indexes/test HTTP/1.1", 200, "{}"),
                    (
                        "POST /prefix/api/v1/test/ingest?commit=auto HTTP/1.1",
                        200,
                        "{}",
                    ),
                ])
                .await;
                let mut config = test_config();
                config.url = format!("{url}/prefix///");
                let mut sink = QuickwitSink::new(1, config);
                sink.open()
                    .await
                    .expect("index creation race should recover");
                assert_eq!(sink.index_id, "test");
                assert_eq!(
                    sink.ingest_url,
                    format!("{url}/prefix/api/v1/test/ingest?commit=auto")
                );
                let requests = server.await.expect("test server should finish");
                assert_eq!(requests[2], sink.config.index.as_bytes());
            }
        });
    }

    #[test]
    fn given_failed_index_creation_when_index_still_absent_should_fail_open() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let (url, server) = start_test_server(vec![
                ("GET /health/readyz HTTP/1.1", 200, ""),
                ("GET /api/v1/indexes/test HTTP/1.1", 404, ""),
                (
                    "POST /api/v1/indexes HTTP/1.1",
                    409,
                    "index creation failed",
                ),
                ("GET /api/v1/indexes/test HTTP/1.1", 404, ""),
            ])
            .await;
            let mut config = test_config();
            config.url = url;
            let mut sink = QuickwitSink::new(1, config);
            assert!(matches!(sink.open().await, Err(Error::InitError(_))));
            assert!(sink.client.is_none());
            server.await.expect("test server should finish");
        });
    }

    #[test]
    fn given_delayed_ingest_readiness_when_opened_should_deliver_the_first_batch() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for index_exists in [false, true] {
                let mut responses = vec![
                    ("GET /health/readyz HTTP/1.1", 200, ""),
                    (
                        "GET /api/v1/indexes/test HTTP/1.1",
                        if index_exists { 200 } else { 404 },
                        "{}",
                    ),
                ];
                if !index_exists {
                    responses.push(("POST /api/v1/indexes HTTP/1.1", 200, "{}"));
                }
                let readiness_start = responses.len();
                responses.extend([
                    (
                        "POST /api/v1/test/ingest?commit=auto HTTP/1.1",
                        404,
                        "index not found",
                    ),
                    ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
                    ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
                ]);
                let (url, server) = start_test_server(responses).await;
                let mut config = test_config();
                config.url = url;
                config.max_open_retries = Some(3);
                let mut sink = QuickwitSink::new(1, config);
                sink.open()
                    .await
                    .expect("sink should wait for its ingest queue");
                consume_payloads(&sink, vec![Payload::Text("first message".to_string())])
                    .await
                    .expect("first batch should reach the ready index");
                let requests = server.await.expect("test server should finish");
                assert!(
                    requests[readiness_start..requests.len() - 1]
                        .iter()
                        .all(Vec::is_empty)
                );
                let mut body = requests
                    .into_iter()
                    .last()
                    .expect("data request should exist");
                assert_eq!(
                    simd_json::from_slice::<OwnedValue>(&mut body)
                        .expect("data request should be JSON"),
                    simd_json::json!({"text": "first message", "data_type": "text"})
                );
            }
        });
    }

    #[test]
    fn given_unavailable_ingest_readiness_when_opened_should_stop_within_the_attempt_budget() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for (status, max_attempts) in
                [(404, 0), (404, 1), (404, 3), (429, 3), (503, 3), (401, 3)]
            {
                let expected_attempts = if status == 401 {
                    1
                } else {
                    max_attempts.max(1)
                };
                let mut responses = vec![
                    ("GET /health/readyz HTTP/1.1", 200, ""),
                    ("GET /api/v1/indexes/test HTTP/1.1", 200, "{}"),
                ];
                responses.extend(std::iter::repeat_n(
                    (
                        "POST /api/v1/test/ingest?commit=auto HTTP/1.1",
                        status,
                        "not ready",
                    ),
                    expected_attempts as usize,
                ));
                let (url, server) = start_test_server(responses).await;
                let mut config = test_config();
                config.url = url;
                config.max_open_retries = Some(max_attempts);
                let mut sink = QuickwitSink::new(1, config);
                assert!(
                    matches!(sink.open().await, Err(Error::InitError(_))),
                    "status {status}"
                );
                assert!(sink.client.is_none());
                let requests = server.await.expect("test server should finish");
                assert_eq!(requests.len(), expected_attempts as usize + 2);
                assert!(requests.iter().all(Vec::is_empty));
            }
        });
    }

    #[test]
    fn given_ingest_failure_when_consumed_should_classify_http_errors() {
        let runtime = test_runtime();
        runtime.block_on(async {
            for status in [400, 404, 429, 503] {
                let (url, server) = start_test_server(vec![
                    ("GET /health/readyz HTTP/1.1", 200, ""),
                    ("GET /api/v1/indexes/test HTTP/1.1", 200, "{}"),
                    ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
                    (
                        "POST /api/v1/test/ingest?commit=auto HTTP/1.1",
                        status,
                        "rejected",
                    ),
                ])
                .await;
                let mut config = test_config();
                config.url = url;
                let mut sink = QuickwitSink::new(1, config);
                sink.open().await.expect("sink should open");
                let result =
                    consume_payloads(&sink, vec![Payload::Text("message".to_string())]).await;
                if status == 400 || status == 404 {
                    assert!(matches!(result, Err(Error::PermanentHttpError(_))));
                } else {
                    assert!(matches!(result, Err(Error::HttpRequestFailed(_))));
                }
                let requests = server.await.expect("test server should finish");
                let mut body = requests
                    .into_iter()
                    .last()
                    .expect("ingest request should exist");
                assert_eq!(
                    simd_json::from_slice::<OwnedValue>(&mut body)
                        .expect("ingest body should be JSON"),
                    simd_json::json!({"text": "message", "data_type": "text"})
                );
            }
        });
    }

    #[test]
    fn given_full_chunk_when_consumed_should_split_and_continue_after_http_failure() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let (url, server) = start_test_server(vec![
                ("GET /health/readyz HTTP/1.1", 200, ""),
                ("GET /api/v1/indexes/test HTTP/1.1", 200, "{}"),
                ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
                (
                    "POST /api/v1/test/ingest?commit=auto HTTP/1.1",
                    503,
                    "unavailable",
                ),
                ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
            ])
            .await;
            let mut config = test_config();
            config.url = url;
            let mut sink = QuickwitSink::new(1, config);
            sink.open().await.expect("sink should open");
            let overhead = simd_json::to_vec(&simd_json::json!({"data": ""}))
                .expect("empty document should serialize")
                .len()
                + 1;
            let full_record =
                simd_json::json!({"data": "x".repeat(MAX_INGEST_BODY_BYTES - overhead)});
            let last_record = simd_json::json!({"sequence": 2});
            let result = consume_payloads(
                &sink,
                vec![
                    Payload::Json(full_record),
                    Payload::Json(last_record.clone()),
                ],
            )
            .await;
            assert!(matches!(result, Err(Error::HttpRequestFailed(_))));
            let requests = server.await.expect("test server should finish");
            assert_eq!(requests[3].len(), MAX_INGEST_BODY_BYTES);
            assert_eq!(requests[3].last(), Some(&b'\n'));
            let mut last_body = requests
                .into_iter()
                .last()
                .expect("last chunk should exist");
            assert_eq!(
                simd_json::from_slice::<OwnedValue>(&mut last_body)
                    .expect("last chunk should be JSON"),
                last_record
            );
        });
    }

    #[test]
    fn given_oversized_record_when_consumed_should_report_it_and_send_other_records() {
        let runtime = test_runtime();
        runtime.block_on(async {
            let (url, server) = start_test_server(vec![
                ("GET /health/readyz HTTP/1.1", 200, ""),
                ("GET /api/v1/indexes/test HTTP/1.1", 200, "{}"),
                ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
                ("POST /api/v1/test/ingest?commit=auto HTTP/1.1", 200, "{}"),
            ])
            .await;
            let mut config = test_config();
            config.url = url;
            let mut sink = QuickwitSink::new(1, config);
            sink.open().await.expect("sink should open");
            let result = consume_payloads(
                &sink,
                vec![
                    Payload::Json(simd_json::json!({"sequence": 1})),
                    Payload::Json(simd_json::json!({"data": "x".repeat(MAX_INGEST_BODY_BYTES)})),
                    Payload::Json(simd_json::json!({"sequence": 3})),
                ],
            )
            .await;
            assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
            let requests = server.await.expect("test server should finish");
            assert_eq!(requests[3], b"{\"sequence\":1}\n{\"sequence\":3}\n");
        });
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should start")
    }

    async fn consume_payloads(sink: &QuickwitSink, payloads: Vec<Payload>) -> Result<(), Error> {
        sink.consume(
            &TopicMetadata {
                stream: "test".to_string(),
                topic: "test".to_string(),
            },
            MessagesMetadata {
                partition_id: 1,
                current_offset: 0,
                schema: Schema::Raw,
            },
            payloads.into_iter().map(test_message).collect(),
        )
        .await
    }

    async fn start_test_server(
        responses: Vec<(&'static str, u16, &'static str)>,
    ) -> (String, JoinHandle<Vec<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("test server should have address")
        );
        let server = tokio::spawn(async move {
            let mut bodies = Vec::with_capacity(responses.len());
            for (expected_request, status, response_body) in responses {
                let (mut stream, _) = timeout(TEST_TIMEOUT, listener.accept())
                    .await
                    .expect("request should arrive before timeout")
                    .expect("request should connect");
                let (request_line, body) = timeout(TEST_TIMEOUT, read_http_request(&mut stream))
                    .await
                    .expect("request should finish before timeout");
                assert_eq!(request_line, expected_request);
                bodies.push(body);
                let response = format!(
                    "HTTP/1.1 {status} Test\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("response should be written");
            }
            bodies
        });
        (url, server)
    }

    async fn read_http_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; HTTP_READ_BUFFER_BYTES];
        let headers_end = loop {
            let read = stream.read(&mut chunk).await.expect("request should read");
            assert_ne!(read, 0, "request should include headers");
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(position) = buffer
                .windows(b"\r\n\r\n".len())
                .position(|window| window == b"\r\n\r\n")
            {
                break position;
            }
        };
        let headers = std::str::from_utf8(&buffer[..headers_end]).expect("headers should be UTF-8");
        let request_line = headers
            .lines()
            .next()
            .expect("request line should exist")
            .to_string();
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("content length should be valid")
            })
        });
        assert!(
            !request_line.starts_with("POST ") || content_length.is_some(),
            "Quickwit requires an explicit Content-Length for POST requests"
        );
        let content_length = content_length.unwrap_or(0);
        let body_start = headers_end + b"\r\n\r\n".len();
        while buffer.len() < body_start + content_length {
            let read = stream.read(&mut chunk).await.expect("body should read");
            assert_ne!(read, 0, "request should include declared body");
            buffer.extend_from_slice(&chunk[..read]);
        }
        (
            request_line,
            buffer[body_start..body_start + content_length].to_vec(),
        )
    }
}
