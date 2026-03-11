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
use base64::Engine;
use base64::engine::general_purpose;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

sink_connector!(HttpSink);

const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;
const DEFAULT_MAX_PAYLOAD_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
const DEFAULT_MAX_CONNECTIONS: usize = 10;

/// HTTP method enum — validated at deserialization, prevents invalid values like "DELET" or "GETS".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Head,
    #[default]
    Post,
    Put,
    Patch,
    Delete,
}

/// Payload formatting mode for HTTP requests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    /// One HTTP request per message (default). Note: with batch_length=50, this produces 50
    /// sequential HTTP round trips per poll cycle. Use ndjson or json_array for higher throughput.
    #[default]
    Individual,
    /// All messages in one request, newline-delimited JSON.
    Ndjson,
    /// All messages as a single JSON array.
    JsonArray,
    /// Raw bytes, one request per message (for non-JSON payloads).
    Raw,
}

/// Configuration for the HTTP sink connector, deserialized from [plugin_config] in config.toml.
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpSinkConfig {
    /// Target URL for HTTP requests (required).
    pub url: String,
    /// HTTP method (default: POST).
    pub method: Option<HttpMethod>,
    /// Request timeout as a human-readable duration string, e.g. "30s" (default: 30s).
    pub timeout: Option<String>,
    /// Maximum HTTP body size in bytes (default: 10MB). Set to 0 to disable.
    pub max_payload_size_bytes: Option<u64>,
    /// Custom HTTP headers.
    pub headers: Option<HashMap<String, String>>,
    /// Payload formatting mode (default: individual).
    pub batch_mode: Option<BatchMode>,
    /// Include Iggy metadata envelope in payload (default: true).
    pub include_metadata: Option<bool>,
    /// Include message checksum in metadata (default: false).
    pub include_checksum: Option<bool>,
    /// Include origin timestamp in metadata (default: false).
    pub include_origin_timestamp: Option<bool>,
    /// Enable health check request in open() (default: false).
    pub health_check_enabled: Option<bool>,
    /// HTTP method for health check (default: HEAD).
    pub health_check_method: Option<HttpMethod>,
    /// Maximum number of retries for transient errors (default: 3).
    pub max_retries: Option<u32>,
    /// Retry delay as a human-readable duration string, e.g. "1s" (default: 1s).
    pub retry_delay: Option<String>,
    /// Backoff multiplier for exponential retry delay (default: 2.0).
    pub retry_backoff_multiplier: Option<f64>,
    /// Maximum retry delay cap as a human-readable duration string (default: 30s).
    pub max_retry_delay: Option<String>,
    /// HTTP status codes considered successful (default: [200, 201, 202, 204]).
    pub success_status_codes: Option<Vec<u16>>,
    /// Accept invalid TLS certificates (default: false). Named to signal danger.
    pub tls_danger_accept_invalid_certs: Option<bool>,
    /// Maximum idle connections per host (default: 10).
    pub max_connections: Option<usize>,
    /// Enable verbose request/response logging (default: false).
    pub verbose_logging: Option<bool>,
}

/// HTTP sink connector that delivers consumed messages to any HTTP endpoint.
///
/// Lifecycle: `new()` → `open()` → `consume()` (repeated) → `close()`.
/// The `reqwest::Client` is built in `open()` (not `new()`) so that config-derived
/// settings (timeout, TLS, connection pool) are applied. This matches the
/// MongoDB/Elasticsearch/PostgreSQL sink initialization pattern.
#[derive(Debug)]
pub struct HttpSink {
    id: u32,
    url: String,
    method: HttpMethod,
    timeout: Duration,
    max_payload_size_bytes: u64,
    headers: HashMap<String, String>,
    batch_mode: BatchMode,
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    health_check_enabled: bool,
    health_check_method: HttpMethod,
    max_retries: u32,
    retry_delay: Duration,
    retry_backoff_multiplier: f64,
    max_retry_delay: Duration,
    success_status_codes: Vec<u16>,
    tls_danger_accept_invalid_certs: bool,
    max_connections: usize,
    verbose: bool,
    /// Initialized in `open()` with config-derived settings. `None` before `open()` is called.
    client: Option<reqwest::Client>,
    requests_sent: AtomicU64,
    messages_delivered: AtomicU64,
    errors_count: AtomicU64,
    retries_count: AtomicU64,
    /// Epoch seconds of last successful HTTP request.
    last_success_timestamp: AtomicU64,
}

/// Parse a human-readable duration string, falling back to a default on failure.
fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!(
                "Invalid duration '{}': {}, using default '{}'",
                raw, e, default
            );
            *HumanDuration::from_str(default).expect("default duration must be valid")
        })
}

impl HttpSink {
    pub fn new(id: u32, config: HttpSinkConfig) -> Self {
        let url = config.url;
        let method = config.method.unwrap_or_default();
        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let max_payload_size_bytes = config.max_payload_size_bytes.unwrap_or(DEFAULT_MAX_PAYLOAD_SIZE);
        let headers = config.headers.unwrap_or_default();
        let batch_mode = config.batch_mode.unwrap_or_default();
        let include_metadata = config.include_metadata.unwrap_or(true);
        let include_checksum = config.include_checksum.unwrap_or(false);
        let include_origin_timestamp = config.include_origin_timestamp.unwrap_or(false);
        let health_check_enabled = config.health_check_enabled.unwrap_or(false);
        let health_check_method = config.health_check_method.unwrap_or(HttpMethod::Head);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_backoff_multiplier = config
            .retry_backoff_multiplier
            .unwrap_or(DEFAULT_BACKOFF_MULTIPLIER)
            .max(1.0);
        let max_retry_delay = parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let success_status_codes = config
            .success_status_codes
            .unwrap_or_else(|| vec![200, 201, 202, 204]);
        let tls_danger_accept_invalid_certs =
            config.tls_danger_accept_invalid_certs.unwrap_or(false);
        let max_connections = config.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let verbose = config.verbose_logging.unwrap_or(false);

        if tls_danger_accept_invalid_certs {
            warn!(
                "HTTP sink ID: {} — tls_danger_accept_invalid_certs is enabled. \
                 TLS certificate validation is DISABLED.",
                id
            );
        }

        HttpSink {
            id,
            url,
            method,
            timeout,
            max_payload_size_bytes,
            headers,
            batch_mode,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            health_check_enabled,
            health_check_method,
            max_retries,
            retry_delay,
            retry_backoff_multiplier,
            max_retry_delay,
            success_status_codes,
            tls_danger_accept_invalid_certs,
            max_connections,
            verbose,
            client: None,
            requests_sent: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            retries_count: AtomicU64::new(0),
            last_success_timestamp: AtomicU64::new(0),
        }
    }

    /// Build the `reqwest::Client` from resolved config.
    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let builder = reqwest::Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(self.max_connections)
            .danger_accept_invalid_certs(self.tls_danger_accept_invalid_certs);

        builder.build().map_err(|e| {
            Error::InitError(format!("Failed to build HTTP client: {}", e))
        })
    }

    /// Apply the configured HTTP method to a `reqwest::Client` for the target URL.
    fn request_builder(&self, client: &reqwest::Client) -> reqwest::RequestBuilder {
        let builder = match self.method {
            HttpMethod::Get => client.get(&self.url),
            HttpMethod::Head => client.head(&self.url),
            HttpMethod::Post => client.post(&self.url),
            HttpMethod::Put => client.put(&self.url),
            HttpMethod::Patch => client.patch(&self.url),
            HttpMethod::Delete => client.delete(&self.url),
        };

        // Apply custom headers
        let mut builder = builder;
        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }
        builder
    }

    /// Determine the Content-Type header based on batch mode.
    fn content_type(&self) -> &'static str {
        match self.batch_mode {
            BatchMode::Individual | BatchMode::JsonArray => "application/json",
            BatchMode::Ndjson => "application/x-ndjson",
            BatchMode::Raw => "application/octet-stream",
        }
    }

    /// Convert a `Payload` to a JSON value for metadata wrapping.
    /// Non-JSON payloads are base64-encoded with a `iggy_payload_encoding` marker.
    fn payload_to_json(
        &self,
        payload: Payload,
    ) -> Result<serde_json::Value, Error> {
        match payload {
            Payload::Json(value) => {
                // simd_json::OwnedValue → serde_json::Value via serialization roundtrip
                let bytes = simd_json::to_vec(&value)
                    .map_err(|e| Error::Serialization(format!("JSON serialize: {}", e)))?;
                serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(format!("JSON re-parse: {}", e)))
            }
            Payload::Text(text) => Ok(serde_json::Value::String(text)),
            Payload::Raw(bytes) => Ok(serde_json::json!({
                "data": general_purpose::STANDARD.encode(&bytes),
                "iggy_payload_encoding": "base64"
            })),
            Payload::Proto(proto_str) => Ok(serde_json::json!({
                "data": general_purpose::STANDARD.encode(proto_str.as_bytes()),
                "iggy_payload_encoding": "base64"
            })),
            Payload::FlatBuffer(bytes) => Ok(serde_json::json!({
                "data": general_purpose::STANDARD.encode(&bytes),
                "iggy_payload_encoding": "base64"
            })),
        }
    }

    /// Build a message envelope with optional metadata wrapping.
    fn build_envelope(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        payload_json: serde_json::Value,
    ) -> serde_json::Value {
        if !self.include_metadata {
            return payload_json;
        }

        let mut metadata = serde_json::json!({
            "iggy_id": format_u128_as_uuid(message.id),
            "iggy_offset": message.offset,
            "iggy_timestamp": message.timestamp,
            "iggy_stream": topic_metadata.stream,
            "iggy_topic": topic_metadata.topic,
            "iggy_partition_id": messages_metadata.partition_id,
        });

        if self.include_checksum {
            metadata["iggy_checksum"] = serde_json::json!(message.checksum);
        }

        if self.include_origin_timestamp {
            metadata["iggy_origin_timestamp"] = serde_json::json!(message.origin_timestamp);
        }

        serde_json::json!({
            "metadata": metadata,
            "payload": payload_json,
        })
    }

    /// Classify whether an HTTP status code is transient (worth retrying).
    fn is_transient_status(status: reqwest::StatusCode) -> bool {
        matches!(
            status.as_u16(),
            429 | 500 | 502 | 503 | 504
        )
    }

    /// Extract `Retry-After` header value as a Duration (seconds).
    fn parse_retry_after(response: &reqwest::Response) -> Option<Duration> {
        response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
    }

    /// Compute the retry delay for a given attempt, applying exponential backoff
    /// capped at `max_retry_delay`.
    fn compute_retry_delay(&self, attempt: u32) -> Duration {
        let delay_secs = self.retry_delay.as_secs_f64()
            * self.retry_backoff_multiplier.powi(attempt as i32);
        Duration::from_secs_f64(delay_secs).min(self.max_retry_delay)
    }

    /// Record a successful request timestamp.
    fn record_success(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_success_timestamp.store(now, Ordering::Relaxed);
    }

    /// Send an HTTP request with retry logic. Returns Ok on success, Err after exhausting retries.
    async fn send_with_retry(
        &self,
        client: &reqwest::Client,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), Error> {
        let mut attempt = 0u32;

        loop {
            let request = self
                .request_builder(client)
                .header("content-type", content_type)
                .body(body.clone())
                .build()
                .map_err(|e| Error::HttpRequestFailed(format!("Request build error: {}", e)))?;

            if self.verbose {
                debug!(
                    "HTTP sink ID: {} — sending {} {} (attempt {}/{}, {} bytes)",
                    self.id,
                    request.method(),
                    request.url(),
                    attempt + 1,
                    self.max_retries + 1,
                    body.len(),
                );
            }

            self.requests_sent.fetch_add(1, Ordering::Relaxed);

            match client.execute(request).await {
                Ok(response) => {
                    let status = response.status();

                    // Check for Retry-After before consuming the response
                    let retry_after = Self::parse_retry_after(&response);

                    if self.success_status_codes.contains(&status.as_u16()) {
                        if self.verbose {
                            debug!(
                                "HTTP sink ID: {} — success (status {})",
                                self.id,
                                status.as_u16()
                            );
                        }
                        self.record_success();
                        return Ok(());
                    }

                    // Non-success status
                    let response_body = response.text().await.unwrap_or_default();

                    if Self::is_transient_status(status) && attempt < self.max_retries {
                        let delay = retry_after.unwrap_or_else(|| self.compute_retry_delay(attempt));
                        warn!(
                            "HTTP sink ID: {} — transient error (status {}, attempt {}/{}). \
                             Retrying in {:?}. Response: {}",
                            self.id,
                            status.as_u16(),
                            attempt + 1,
                            self.max_retries + 1,
                            delay,
                            truncate_response(&response_body, 200),
                        );
                        self.retries_count.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }

                    // Non-transient or retries exhausted
                    error!(
                        "HTTP sink ID: {} — request failed (status {}, attempt {}/{}). \
                         Response: {}",
                        self.id,
                        status.as_u16(),
                        attempt + 1,
                        self.max_retries + 1,
                        truncate_response(&response_body, 500),
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::HttpRequestFailed(format!(
                        "HTTP {} — status: {}",
                        self.url,
                        status.as_u16()
                    )));
                }
                Err(network_err) => {
                    if attempt < self.max_retries {
                        let delay = self.compute_retry_delay(attempt);
                        warn!(
                            "HTTP sink ID: {} — network error (attempt {}/{}): {}. \
                             Retrying in {:?}.",
                            self.id,
                            attempt + 1,
                            self.max_retries + 1,
                            network_err,
                            delay,
                        );
                        self.retries_count.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }

                    error!(
                        "HTTP sink ID: {} — network error after {} attempts: {}",
                        self.id,
                        attempt + 1,
                        network_err,
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::HttpRequestFailed(format!(
                        "Network error after {} attempts: {}",
                        attempt + 1,
                        network_err
                    )));
                }
            }
        }
    }

    /// Send messages in `individual` mode — one HTTP request per message.
    /// Continues processing remaining messages if one fails (partial delivery).
    async fn send_individual(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();
        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut last_error: Option<Error> = None;

        for message in &messages {
            let offset = message.offset;
            let payload_json = match self.payload_to_json(message.payload.clone()) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to serialize payload at offset {}: {}",
                        self.id, offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    failed += 1;
                    last_error = Some(e);
                    continue;
                }
            };

            let envelope = self.build_envelope(message, topic_metadata, messages_metadata, payload_json);
            let body = match serde_json::to_vec(&envelope) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to serialize envelope at offset {}: {}",
                        self.id, offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    failed += 1;
                    last_error = Some(Error::Serialization(format!("Envelope serialize: {}", e)));
                    continue;
                }
            };

            if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
                error!(
                    "HTTP sink ID: {} — payload at offset {} exceeds max size ({} > {} bytes). Skipping.",
                    self.id, offset, body.len(), self.max_payload_size_bytes,
                );
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                failed += 1;
                last_error = Some(Error::HttpRequestFailed(format!(
                    "Payload exceeds max size: {} bytes",
                    body.len()
                )));
                continue;
            }

            match self.send_with_retry(client, body, self.content_type()).await {
                Ok(()) => delivered += 1,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to deliver message at offset {} after retries: {}",
                        self.id, offset, e
                    );
                    failed += 1;
                    last_error = Some(e);
                }
            }
        }

        self.messages_delivered.fetch_add(delivered, Ordering::Relaxed);

        match last_error {
            Some(e) => {
                error!(
                    "HTTP sink ID: {} — partial delivery: {}/{} messages delivered, {} failed",
                    self.id, delivered, total, failed,
                );
                Err(e)
            }
            None => Ok(()),
        }
    }

    /// Send messages in `ndjson` mode — all messages in one request, newline-delimited.
    async fn send_ndjson(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let count = messages.len() as u64;
        let mut lines = Vec::with_capacity(messages.len());

        for message in &messages {
            let payload_json = self.payload_to_json(message.payload.clone())?;
            let envelope =
                self.build_envelope(message, topic_metadata, messages_metadata, payload_json);
            let line = serde_json::to_string(&envelope)
                .map_err(|e| Error::Serialization(format!("NDJSON line serialize: {}", e)))?;
            lines.push(line);
        }

        let body = lines.join("\n").into_bytes();

        if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
            error!(
                "HTTP sink ID: {} — NDJSON batch exceeds max payload size ({} > {} bytes)",
                self.id,
                body.len(),
                self.max_payload_size_bytes,
            );
            return Err(Error::HttpRequestFailed(format!(
                "NDJSON batch exceeds max size: {} bytes",
                body.len()
            )));
        }

        self.send_with_retry(client, body, self.content_type()).await?;
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Send messages in `json_array` mode — all messages as a single JSON array.
    async fn send_json_array(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let count = messages.len() as u64;
        let mut envelopes = Vec::with_capacity(messages.len());

        for message in &messages {
            let payload_json = self.payload_to_json(message.payload.clone())?;
            let envelope =
                self.build_envelope(message, topic_metadata, messages_metadata, payload_json);
            envelopes.push(envelope);
        }

        let body = serde_json::to_vec(&envelopes)
            .map_err(|e| Error::Serialization(format!("JSON array serialize: {}", e)))?;

        if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
            error!(
                "HTTP sink ID: {} — JSON array batch exceeds max payload size ({} > {} bytes)",
                self.id,
                body.len(),
                self.max_payload_size_bytes,
            );
            return Err(Error::HttpRequestFailed(format!(
                "JSON array batch exceeds max size: {} bytes",
                body.len()
            )));
        }

        self.send_with_retry(client, body, self.content_type()).await?;
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Send messages in `raw` mode — one HTTP request per message with raw bytes.
    /// Only meaningful for Raw/FlatBuffer/Proto payloads; JSON/Text are sent as UTF-8 bytes.
    async fn send_raw(
        &self,
        client: &reqwest::Client,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();
        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut last_error: Option<Error> = None;

        for message in &messages {
            let offset = message.offset;
            let body = match message.payload.clone().try_into_vec() {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to convert raw payload at offset {}: {}",
                        self.id, offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    failed += 1;
                    last_error = Some(Error::Serialization(format!("Raw payload convert: {}", e)));
                    continue;
                }
            };

            if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
                error!(
                    "HTTP sink ID: {} — raw payload at offset {} exceeds max size ({} > {} bytes). Skipping.",
                    self.id, offset, body.len(), self.max_payload_size_bytes,
                );
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                failed += 1;
                last_error = Some(Error::HttpRequestFailed(format!(
                    "Raw payload exceeds max size: {} bytes",
                    body.len()
                )));
                continue;
            }

            match self.send_with_retry(client, body, self.content_type()).await {
                Ok(()) => delivered += 1,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to deliver raw message at offset {}: {}",
                        self.id, offset, e
                    );
                    failed += 1;
                    last_error = Some(e);
                }
            }
        }

        self.messages_delivered.fetch_add(delivered, Ordering::Relaxed);

        match last_error {
            Some(e) => {
                error!(
                    "HTTP sink ID: {} — partial raw delivery: {}/{} messages delivered, {} failed",
                    self.id, delivered, total, failed,
                );
                Err(e)
            }
            None => Ok(()),
        }
    }
}

/// Format a u128 message ID as a UUID-style string (8-4-4-4-12 hex).
/// Avoids pulling in the `uuid` crate for a single formatting operation.
fn format_u128_as_uuid(id: u128) -> String {
    let hex = format!("{:032x}", id);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32],
    )
}

/// Truncate a response body string for log output, respecting UTF-8 char boundaries.
fn truncate_response(body: &str, max_len: usize) -> &str {
    if body.len() <= max_len {
        body
    } else {
        // Find the last valid UTF-8 char boundary at or before max_len
        let end = body.floor_char_boundary(max_len);
        &body[..end]
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn open(&mut self) -> Result<(), Error> {
        // Validate URL is non-empty
        if self.url.is_empty() {
            return Err(Error::InvalidConfig);
        }

        // Build the HTTP client with config-derived settings
        self.client = Some(self.build_client()?);

        // Optional health check
        if self.health_check_enabled {
            let client = self.client.as_ref().expect("client just built");
            let health_request = match self.health_check_method {
                HttpMethod::Get => client.get(&self.url),
                HttpMethod::Head => client.head(&self.url),
                HttpMethod::Post => client.post(&self.url),
                HttpMethod::Put => client.put(&self.url),
                HttpMethod::Patch => client.patch(&self.url),
                HttpMethod::Delete => client.delete(&self.url),
            };

            let response = health_request.send().await.map_err(|e| {
                Error::Connection(format!(
                    "Health check failed for URL '{}': {}",
                    self.url, e
                ))
            })?;

            if !response.status().is_success() {
                return Err(Error::Connection(format!(
                    "Health check returned non-success status {} for URL '{}'",
                    response.status(),
                    self.url,
                )));
            }

            info!(
                "HTTP sink ID: {} — health check passed (status {})",
                self.id,
                response.status().as_u16()
            );
        }

        info!(
            "Opened HTTP sink connector with ID: {} for URL: {} (method: {:?}, \
             batch_mode: {:?}, timeout: {:?}, max_retries: {})",
            self.id, self.url, self.method, self.batch_mode, self.timeout, self.max_retries,
        );
        Ok(())
    }

    /// Deliver messages to the configured HTTP endpoint.
    ///
    /// **Runtime note**: The connector runtime (`sink.rs:585`) currently discards the `Result`
    /// returned by `consume()`. All retry logic lives inside this method — returning `Err`
    /// does not trigger a runtime-level retry. This is a known upstream issue.
    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let messages_count = messages.len();
        if messages_count == 0 {
            return Ok(());
        }

        if self.verbose {
            info!(
                "HTTP sink ID: {} — received {} messages (schema: {}, stream: {}, topic: {})",
                self.id,
                messages_count,
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
            );
        }

        let client = self.client.as_ref().ok_or_else(|| {
            Error::InitError("HTTP client not initialized — was open() called?".to_string())
        })?;

        let result = match self.batch_mode {
            BatchMode::Individual => {
                self.send_individual(client, topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::Ndjson => {
                self.send_ndjson(client, topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::JsonArray => {
                self.send_json_array(client, topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::Raw => self.send_raw(client, messages).await,
        };

        if let Err(ref e) = result {
            error!(
                "HTTP sink ID: {} — consume() returning error (runtime will discard): {}",
                self.id, e
            );
        }

        result
    }

    async fn close(&mut self) -> Result<(), Error> {
        let requests = self.requests_sent.load(Ordering::Relaxed);
        let delivered = self.messages_delivered.load(Ordering::Relaxed);
        let errors = self.errors_count.load(Ordering::Relaxed);
        let retries = self.retries_count.load(Ordering::Relaxed);

        info!(
            "HTTP sink connector ID: {} closed. Stats: {} requests sent, \
             {} messages delivered, {} errors, {} retries.",
            self.id, requests, delivered, errors, retries,
        );

        self.client = None;
        Ok(())
    }
}
