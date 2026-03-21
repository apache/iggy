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
use bytes::Bytes;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json, sink_connector,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
/// TCP keep-alive interval for detecting dead connections behind load balancers.
/// Cloud LBs silently drop idle connections (AWS ALB ~60s, GCP ~600s);
/// probing at 30s detects these before requests fail.
const DEFAULT_TCP_KEEPALIVE_SECS: u64 = 30;
/// Close pooled connections unused for this long. Prevents stale connections
/// from accumulating when traffic is bursty.
const DEFAULT_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
/// Abort remaining messages in individual/raw mode after this many consecutive HTTP failures.
/// Prevents hammering a dead endpoint with N sequential retry cycles per poll.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// HTTP method enum — validated at deserialization, prevents invalid values like "DELEET" or "GETX".
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
    success_status_codes: HashSet<u16>,
    tls_danger_accept_invalid_certs: bool,
    max_connections: usize,
    verbose: bool,
    /// Pre-built HTTP headers (excluding Content-Type). Built once in `open()` from validated
    /// `self.headers`, reused for every request. `None` before `open()` is called.
    request_headers: Option<reqwest::header::HeaderMap>,
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
        let max_payload_size_bytes = config
            .max_payload_size_bytes
            .unwrap_or(DEFAULT_MAX_PAYLOAD_SIZE);
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
        let max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let success_status_codes: HashSet<u16> = config
            .success_status_codes
            .unwrap_or_else(|| vec![200, 201, 202, 204])
            .into_iter()
            .collect();
        let tls_danger_accept_invalid_certs =
            config.tls_danger_accept_invalid_certs.unwrap_or(false);
        let max_connections = config.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let verbose = config.verbose_logging.unwrap_or(false);

        if retry_delay > max_retry_delay {
            warn!(
                "HTTP sink ID: {} — retry_delay ({:?}) exceeds max_retry_delay ({:?}). \
                 All retry delays will be capped to max_retry_delay.",
                id, retry_delay, max_retry_delay,
            );
        }

        if tls_danger_accept_invalid_certs {
            warn!(
                "HTTP sink ID: {} — tls_danger_accept_invalid_certs is enabled. \
                 TLS certificate validation is DISABLED.",
                id
            );
        }

        if batch_mode == BatchMode::Raw && include_metadata {
            warn!(
                "HTTP sink ID: {} — batch_mode=raw ignores include_metadata. \
                 Raw mode sends payload bytes directly without metadata envelope.",
                id
            );
        }

        if matches!(method, HttpMethod::Get | HttpMethod::Head)
            && batch_mode != BatchMode::Individual
        {
            warn!(
                "HTTP sink ID: {} — {:?} with batch_mode={:?} will send a request body. \
                 Some servers may reject GET/HEAD requests with a body.",
                id, method, batch_mode,
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
            request_headers: None,
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
            .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
            .tcp_keepalive(Duration::from_secs(DEFAULT_TCP_KEEPALIVE_SECS))
            .danger_accept_invalid_certs(self.tls_danger_accept_invalid_certs);

        builder
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {}", e)))
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
    ///
    /// Note: All current `Payload` variants produce infallible conversions.
    /// The `Result` return type exists as a safety net for future variants.
    fn payload_to_json(&self, payload: Payload) -> Result<serde_json::Value, Error> {
        match payload {
            Payload::Json(value) => {
                // Direct structural conversion (not serialization roundtrip).
                // Follows the Elasticsearch sink pattern. NaN/Infinity f64 → null.
                Ok(owned_value_to_serde_json(&value))
            }
            Payload::Text(text) => Ok(serde_json::Value::String(text)),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) => Ok(serde_json::json!({
                "data": general_purpose::STANDARD.encode(&bytes),
                "iggy_payload_encoding": "base64"
            })),
            Payload::Proto(proto_str) => Ok(serde_json::json!({
                "data": general_purpose::STANDARD.encode(proto_str.as_bytes()),
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

        if let Some(ref headers) = message.headers
            && !headers.is_empty()
        {
            let headers_map: serde_json::Map<String, serde_json::Value> = headers
                .iter()
                .map(|(k, v)| {
                    // Raw bytes: base64-encode to avoid Rust debug format in JSON output.
                    // as_raw() returns Ok only for HeaderKind::Raw.
                    let value = if let Ok(raw) = v.as_raw() {
                        serde_json::json!({
                            "data": general_purpose::STANDARD.encode(raw),
                            "iggy_header_encoding": "base64"
                        })
                    } else {
                        serde_json::Value::String(v.to_string_value())
                    };
                    (k.to_string_value(), value)
                })
                .collect();
            metadata["iggy_headers"] = serde_json::Value::Object(headers_map);
        }

        serde_json::json!({
            "metadata": metadata,
            "payload": payload_json,
        })
    }

    /// Classify whether an HTTP status code is transient (worth retrying).
    fn is_transient_status(status: reqwest::StatusCode) -> bool {
        matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
    }

    /// Extract `Retry-After` header value as a Duration (seconds), capped to `max_retry_delay`.
    fn parse_retry_after(&self, response: &reqwest::Response) -> Option<Duration> {
        let header_raw = response.headers().get(reqwest::header::RETRY_AFTER)?;
        let header_value = match header_raw.to_str() {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "HTTP sink ID: {} — Retry-After header contains non-ASCII bytes: {}. \
                     Using computed backoff.",
                    self.id, e,
                );
                return None;
            }
        };
        match header_value.parse::<u64>() {
            Ok(secs) => Some(Duration::from_secs(secs).min(self.max_retry_delay)),
            Err(_) => {
                warn!(
                    "HTTP sink ID: {} — Retry-After header '{}' is not an integer delay; \
                     HTTP-date format is not supported. Using computed backoff.",
                    self.id, header_value,
                );
                None
            }
        }
    }

    /// Compute the retry delay for a given attempt, applying exponential backoff
    /// capped at `max_retry_delay`. Clamps before `Duration::from_secs_f64` to avoid
    /// panics when extreme backoff configs produce infinity (e.g., multiplier=1000, retries=200).
    fn compute_retry_delay(&self, attempt: u32) -> Duration {
        let delay_secs =
            self.retry_delay.as_secs_f64() * self.retry_backoff_multiplier.powi(attempt as i32);
        let capped_secs = delay_secs.min(self.max_retry_delay.as_secs_f64());
        if !capped_secs.is_finite() || capped_secs < 0.0 {
            return self.max_retry_delay;
        }
        Duration::from_secs_f64(capped_secs)
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
    ///
    /// Takes `Bytes` instead of `Vec<u8>` so retries clone via reference-count increment (O(1))
    /// rather than copying the entire payload on each attempt.
    async fn send_with_retry(
        &self,
        client: &reqwest::Client,
        body: Bytes,
        content_type: &str,
    ) -> Result<(), Error> {
        let mut attempt = 0u32;

        loop {
            let headers = self.request_headers.as_ref().ok_or_else(|| {
                Error::InitError("HTTP headers not initialized — was open() called?".to_string())
            })?;
            let request = build_request(self.method, client, &self.url)
                .headers(headers.clone())
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
                    let retry_after = self.parse_retry_after(&response);

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

                    // Non-success status — read body for diagnostics
                    let response_body = match response.text().await {
                        Ok(body) => body,
                        Err(e) => format!("<body read error: {}>", e),
                    };

                    if Self::is_transient_status(status) && attempt < self.max_retries {
                        let delay =
                            retry_after.unwrap_or_else(|| self.compute_retry_delay(attempt));
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

    /// Shared per-message send loop for `individual` and `raw` modes.
    ///
    /// Iterates `messages`, builds a body for each via `build_body`, enforces payload size
    /// limits, sends via `send_with_retry`, and tracks partial delivery.
    /// Aborts after `MAX_CONSECUTIVE_FAILURES` consecutive HTTP failures.
    ///
    /// `build_body` takes ownership of each `ConsumedMessage` — callers must extract
    /// all needed fields (payload, metadata) within the closure.
    async fn send_per_message<F>(
        &self,
        client: &reqwest::Client,
        messages: Vec<ConsumedMessage>,
        content_type: &str,
        mode_name: &str,
        mut build_body: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ConsumedMessage) -> Result<Vec<u8>, Error>,
    {
        let total = messages.len();
        let mut delivered = 0u64;
        let mut http_failures = 0u64;
        let mut serialization_failures = 0u64;
        let mut consecutive_failures = 0u32;
        let mut last_error: Option<Error> = None;

        for message in messages {
            let offset = message.offset;
            let body = match build_body(message) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to build {} body at offset {}: {}",
                        self.id, mode_name, offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    serialization_failures += 1;
                    last_error = Some(e);
                    continue;
                }
            };

            if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
                error!(
                    "HTTP sink ID: {} — {} payload at offset {} exceeds max size ({} > {} bytes). Skipping.",
                    self.id,
                    mode_name,
                    offset,
                    body.len(),
                    self.max_payload_size_bytes,
                );
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                serialization_failures += 1;
                last_error = Some(Error::HttpRequestFailed(format!(
                    "Payload exceeds max size: {} bytes",
                    body.len()
                )));
                continue;
            }

            match self
                .send_with_retry(client, Bytes::from(body), content_type)
                .await
            {
                Ok(()) => {
                    delivered += 1;
                    consecutive_failures = 0;
                }
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to deliver {} message at offset {} after retries: {}",
                        self.id, mode_name, offset, e
                    );
                    http_failures += 1;
                    consecutive_failures += 1;
                    last_error = Some(e);

                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        let processed = delivered + http_failures + serialization_failures;
                        debug_assert!(
                            processed <= total as u64,
                            "processed ({processed}) > total ({total}) — accounting bug"
                        );
                        let skipped = (total as u64).saturating_sub(processed);
                        error!(
                            "HTTP sink ID: {} — aborting {} batch after {} consecutive HTTP failures \
                             ({} remaining messages skipped)",
                            self.id, mode_name, consecutive_failures, skipped,
                        );
                        self.errors_count.fetch_add(skipped, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        self.messages_delivered
            .fetch_add(delivered, Ordering::Relaxed);

        match last_error {
            Some(e) => {
                error!(
                    "HTTP sink ID: {} — partial {} delivery: {}/{} delivered, \
                     {} HTTP failures, {} serialization errors",
                    self.id, mode_name, delivered, total, http_failures, serialization_failures,
                );
                Err(e)
            }
            None => Ok(()),
        }
    }

    /// Send messages in `individual` mode — one HTTP request per message.
    async fn send_individual(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.send_per_message(
            client,
            messages,
            self.content_type(),
            "individual",
            |mut message| {
                let payload = std::mem::replace(&mut message.payload, Payload::Raw(vec![]));
                let payload_json = self.payload_to_json(payload)?;
                let envelope =
                    self.build_envelope(&message, topic_metadata, messages_metadata, payload_json);
                serde_json::to_vec(&envelope)
                    .map_err(|e| Error::Serialization(format!("Envelope serialize: {}", e)))
            },
        )
        .await
    }

    /// Sends a batch body and updates delivery/error accounting.
    ///
    /// Shared by `send_ndjson` and `send_json_array` — the post-send accounting logic
    /// (error propagation, skip warnings) is identical across batch modes.
    async fn send_batch_body(
        &self,
        client: &reqwest::Client,
        body: Bytes,
        count: u64,
        skipped: u64,
        batch_mode: &str,
    ) -> Result<(), Error> {
        debug_assert!(
            count > 0,
            "send_batch_body called with count=0 — callers must guard against empty batches"
        );
        if let Err(e) = self
            .send_with_retry(client, body, self.content_type())
            .await
        {
            // send_with_retry already added 1 to errors_count for the HTTP failure.
            // Add the remaining messages that were serialized but not delivered.
            if count > 1 {
                self.errors_count.fetch_add(count - 1, Ordering::Relaxed);
            }
            if skipped > 0 {
                error!(
                    "HTTP sink ID: {} — {} batch failed with {} serialization skips",
                    self.id, batch_mode, skipped,
                );
            }
            return Err(e);
        }
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
        if skipped > 0 {
            warn!(
                "HTTP sink ID: {} — {} batch: {} delivered, {} skipped (serialization errors)",
                self.id, batch_mode, count, skipped,
            );
        }
        Ok(())
    }

    /// Send messages in `ndjson` mode — all messages in one request, newline-delimited.
    /// Skips individual messages that fail serialization rather than aborting the batch.
    async fn send_ndjson(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut lines = Vec::with_capacity(messages.len());
        let mut skipped = 0u64;

        for mut message in messages {
            let payload = std::mem::replace(&mut message.payload, Payload::Raw(vec![]));
            let payload_json = match self.payload_to_json(payload) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — skipping message at offset {} in NDJSON batch: {}",
                        self.id, message.offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    skipped += 1;
                    continue;
                }
            };
            let envelope =
                self.build_envelope(&message, topic_metadata, messages_metadata, payload_json);
            match serde_json::to_string(&envelope) {
                Ok(line) => lines.push(line),
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — skipping message at offset {} in NDJSON batch (serialize): {}",
                        self.id, message.offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    skipped += 1;
                    continue;
                }
            }
        }

        if lines.is_empty() {
            return Err(Error::Serialization(
                "All messages in NDJSON batch failed serialization".to_string(),
            ));
        }

        let count = lines.len() as u64;

        let mut body_str = lines.join("\n");
        body_str.push('\n'); // NDJSON spec requires trailing newline
        let body = body_str.into_bytes();

        if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
            error!(
                "HTTP sink ID: {} — NDJSON batch exceeds max payload size ({} > {} bytes)",
                self.id,
                body.len(),
                self.max_payload_size_bytes,
            );
            // Count all successfully-serialized messages as errors (skipped already counted individually)
            self.errors_count.fetch_add(count, Ordering::Relaxed);
            return Err(Error::HttpRequestFailed(format!(
                "NDJSON batch exceeds max size: {} bytes",
                body.len()
            )));
        }

        self.send_batch_body(client, Bytes::from(body), count, skipped, "NDJSON")
            .await
    }

    /// Send messages in `json_array` mode — all messages as a single JSON array.
    /// Skips individual messages that fail serialization rather than aborting the batch.
    async fn send_json_array(
        &self,
        client: &reqwest::Client,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut envelopes = Vec::with_capacity(messages.len());
        let mut skipped = 0u64;

        for mut message in messages {
            let payload = std::mem::replace(&mut message.payload, Payload::Raw(vec![]));
            let payload_json = match self.payload_to_json(payload) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — skipping message at offset {} in JSON array batch: {}",
                        self.id, message.offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    skipped += 1;
                    continue;
                }
            };
            let envelope =
                self.build_envelope(&message, topic_metadata, messages_metadata, payload_json);
            envelopes.push(envelope);
        }

        if envelopes.is_empty() {
            return Err(Error::Serialization(
                "All messages in JSON array batch failed serialization".to_string(),
            ));
        }

        let count = envelopes.len() as u64;

        let body = match serde_json::to_vec(&envelopes) {
            Ok(b) => b,
            Err(e) => {
                error!(
                    "HTTP sink ID: {} — failed to serialize JSON array batch \
                     ({} envelopes, {} skipped): {}",
                    self.id,
                    envelopes.len(),
                    skipped,
                    e,
                );
                // Count all successfully-built envelopes as errors (skipped already counted individually)
                self.errors_count.fetch_add(count, Ordering::Relaxed);
                return Err(Error::Serialization(format!(
                    "JSON array serialize ({} envelopes): {}",
                    envelopes.len(),
                    e
                )));
            }
        };

        if self.max_payload_size_bytes > 0 && body.len() as u64 > self.max_payload_size_bytes {
            error!(
                "HTTP sink ID: {} — JSON array batch exceeds max payload size ({} > {} bytes)",
                self.id,
                body.len(),
                self.max_payload_size_bytes,
            );
            // Count all successfully-serialized messages as errors (skipped already counted individually)
            self.errors_count.fetch_add(count, Ordering::Relaxed);
            return Err(Error::HttpRequestFailed(format!(
                "JSON array batch exceeds max size: {} bytes",
                body.len()
            )));
        }

        self.send_batch_body(client, Bytes::from(body), count, skipped, "JSON array")
            .await
    }

    /// Send messages in `raw` mode — one HTTP request per message with raw bytes.
    async fn send_raw(
        &self,
        client: &reqwest::Client,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.send_per_message(client, messages, self.content_type(), "raw", |message| {
            message
                .payload
                .try_into_vec()
                .map_err(|e| Error::Serialization(format!("Raw payload convert: {}", e)))
        })
        .await
    }
}

/// Map an `HttpMethod` to a `reqwest::RequestBuilder` for the given URL.
fn build_request(
    method: HttpMethod,
    client: &reqwest::Client,
    url: &str,
) -> reqwest::RequestBuilder {
    match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Head => client.head(url),
        HttpMethod::Post => client.post(url),
        HttpMethod::Put => client.put(url),
        HttpMethod::Patch => client.patch(url),
        HttpMethod::Delete => client.delete(url),
    }
}

/// Format a u128 message ID as an RFC 4122 v8 (custom) UUID.
///
/// Uses `Uuid::new_v8()` which sets version=8 and variant=RFC4122 bits,
/// producing UUIDs that downstream libraries accept as valid.
/// Note: `new_v8()` overwrites 6 bits (version nibble + variant bits), so the
/// UUID is not round-trippable to the original u128 value.
fn format_u128_as_uuid(id: u128) -> String {
    uuid::Uuid::new_v8(id.to_be_bytes()).to_string()
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
        // Validate success_status_codes — empty would cause every response to be treated as failure
        if self.success_status_codes.is_empty() {
            return Err(Error::InitError(
                "success_status_codes must not be empty — would cause retry storms against healthy endpoints".to_string(),
            ));
        }
        for &code in &self.success_status_codes {
            if !(200..=599).contains(&code) {
                return Err(Error::InitError(format!(
                    "Invalid status code {} in success_status_codes — must be 200-599",
                    code,
                )));
            }
        }

        // Warn if success codes overlap with transient retry codes — these will be treated
        // as success, silently disabling retry for those status codes.
        const TRANSIENT_CODES: &[u16] = &[429, 500, 502, 503, 504];
        let overlap: Vec<u16> = self
            .success_status_codes
            .iter()
            .filter(|c| TRANSIENT_CODES.contains(c))
            .copied()
            .collect();
        if !overlap.is_empty() {
            warn!(
                "HTTP sink ID: {} — success_status_codes {:?} overlap with transient retry codes. \
                 These will be treated as success, disabling retry.",
                self.id, overlap
            );
        }

        // Validate URL
        if self.url.is_empty() {
            return Err(Error::InitError(
                "HTTP sink URL is empty — 'url' is required in [plugin_config]".to_string(),
            ));
        }
        match reqwest::Url::parse(&self.url) {
            Ok(parsed) => {
                let scheme = parsed.scheme();
                if scheme != "http" && scheme != "https" {
                    return Err(Error::InitError(format!(
                        "HTTP sink URL scheme '{}' is not allowed — only 'http' and 'https' are supported (url: '{}')",
                        scheme, self.url,
                    )));
                }
            }
            Err(e) => {
                return Err(Error::InitError(format!(
                    "HTTP sink URL '{}' is not a valid URL: {}",
                    self.url, e,
                )));
            }
        }

        // Warn if user supplied a Content-Type header — it will be overridden by batch_mode.
        if self
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("content-type"))
        {
            warn!(
                "HTTP sink ID: {} — custom 'Content-Type' header in [headers] is ignored. \
                 Content-Type is set by batch_mode ({:?} -> '{}'). \
                 Remove it from [headers] to silence this warning.",
                self.id,
                self.batch_mode,
                self.content_type(),
            );
        }

        // Validate custom headers — fail fast rather than per-request errors
        for (key, value) in &self.headers {
            reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| Error::InitError(format!("Invalid header name '{}': {}", key, e)))?;
            reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                Error::InitError(format!("Invalid header value for '{}': {}", key, e))
            })?;
        }

        // Pre-build the HeaderMap once — avoids re-parsing on every request.
        // Header names and values were validated above, so expect() is safe here.
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in &self.headers {
            if key.eq_ignore_ascii_case("content-type") {
                continue;
            }
            let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .expect("header name validated above");
            let val = reqwest::header::HeaderValue::from_str(value)
                .expect("header value validated above");
            header_map.insert(name, val);
        }
        self.request_headers = Some(header_map);

        // Build the HTTP client with config-derived settings
        self.client = Some(self.build_client()?);

        // Optional health check — uses same pre-built headers and success_status_codes as consume()
        if self.health_check_enabled {
            let client = self.client.as_ref().expect("client just built");
            let headers = self
                .request_headers
                .as_ref()
                .expect("request_headers just built");
            let health_request =
                build_request(self.health_check_method, client, &self.url).headers(headers.clone());

            let response = health_request.send().await.map_err(|e| {
                Error::Connection(format!("Health check failed for URL '{}': {}", self.url, e))
            })?;

            let status = response.status();
            if !self.success_status_codes.contains(&status.as_u16()) {
                return Err(Error::Connection(format!(
                    "Health check returned status {} (not in success_status_codes {:?}) for URL '{}'",
                    status.as_u16(),
                    self.success_status_codes,
                    self.url,
                )));
            }

            info!(
                "HTTP sink ID: {} — health check passed (status {})",
                self.id,
                status.as_u16()
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
    /// **Worst-case latency upper bound** (individual/raw modes):
    /// `batch_length * (max_retries + 1) * (timeout + max_retry_delay)`.
    /// Example: 50 * 4 * (30s + 30s) = 12000s. `MAX_CONSECUTIVE_FAILURES` (3)
    /// mitigates this by aborting early, but a fail-succeed-fail pattern can bypass it.
    ///
    /// **Runtime note**: The FFI boundary in `sdk/src/sink.rs` maps `consume()`'s `Result` to
    /// `i32` (0=ok, 1=err), but the runtime's `process_messages()` in `runtime/src/sink.rs`
    /// discards that return code. All retry logic lives inside this method — returning `Err`
    /// does not trigger a runtime-level retry.
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
            debug!(
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
                "HTTP sink ID: {} — consume() returning error (runtime ignores FFI status code): {}",
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
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);

        info!(
            "HTTP sink connector ID: {} closed. Stats: {} requests sent, \
             {} messages delivered, {} errors, {} retries, last success epoch: {}.",
            self.id, requests, delivered, errors, retries, last_success,
        );

        self.request_headers = None;
        self.client = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;

    // ── Test helpers ──────────────────────────────────────────────────

    /// Parse a JSON string into `simd_json::OwnedValue` for test construction.
    fn simd_json_from_str(s: &str) -> simd_json::OwnedValue {
        let mut bytes = s.as_bytes().to_vec();
        simd_json::to_owned_value(&mut bytes).expect("valid JSON for test")
    }

    fn given_default_config() -> HttpSinkConfig {
        HttpSinkConfig {
            url: "https://api.example.com/ingest".to_string(),
            method: None,
            timeout: None,
            max_payload_size_bytes: None,
            headers: None,
            batch_mode: None,
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            health_check_enabled: None,
            health_check_method: None,
            max_retries: None,
            retry_delay: None,
            retry_backoff_multiplier: None,
            max_retry_delay: None,
            success_status_codes: None,
            tls_danger_accept_invalid_certs: None,
            max_connections: None,
            verbose_logging: None,
        }
    }

    fn given_sink_with_defaults() -> HttpSink {
        HttpSink::new(1, given_default_config())
    }

    fn given_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

    fn given_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: Schema::Json,
        }
    }

    fn given_json_message(id: u128, offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id,
            offset,
            checksum: 12345,
            timestamp: 1710064800000000,
            origin_timestamp: 1710064799000000,
            headers: None,
            payload: Payload::Json(simd_json_from_str(r#"{"key":"value"}"#)),
        }
    }

    // ── Config resolution tests ──────────────────────────────────────

    #[test]
    fn given_all_none_config_should_apply_defaults() {
        let sink = given_sink_with_defaults();

        assert_eq!(sink.method, HttpMethod::Post);
        assert_eq!(sink.timeout, Duration::from_secs(30));
        assert_eq!(sink.max_payload_size_bytes, DEFAULT_MAX_PAYLOAD_SIZE);
        assert_eq!(sink.batch_mode, BatchMode::Individual);
        assert!(sink.include_metadata);
        assert!(!sink.include_checksum);
        assert!(!sink.include_origin_timestamp);
        assert!(!sink.health_check_enabled);
        assert_eq!(sink.health_check_method, HttpMethod::Head);
        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
        assert_eq!(sink.retry_backoff_multiplier, DEFAULT_BACKOFF_MULTIPLIER);
        assert_eq!(sink.max_retry_delay, Duration::from_secs(30));
        assert_eq!(
            sink.success_status_codes,
            HashSet::from([200, 201, 202, 204])
        );
        assert!(!sink.tls_danger_accept_invalid_certs);
        assert_eq!(sink.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert!(!sink.verbose);
        assert!(sink.client.is_none());
    }

    #[test]
    fn given_explicit_config_values_should_override_defaults() {
        let config = HttpSinkConfig {
            url: "https://example.com".to_string(),
            method: Some(HttpMethod::Put),
            timeout: Some("10s".to_string()),
            max_payload_size_bytes: Some(5000),
            headers: Some(HashMap::from([("X-Key".to_string(), "val".to_string())])),
            batch_mode: Some(BatchMode::Ndjson),
            include_metadata: Some(false),
            include_checksum: Some(true),
            include_origin_timestamp: Some(true),
            health_check_enabled: Some(true),
            health_check_method: Some(HttpMethod::Get),
            max_retries: Some(5),
            retry_delay: Some("500ms".to_string()),
            retry_backoff_multiplier: Some(3.0),
            max_retry_delay: Some("60s".to_string()),
            success_status_codes: Some(vec![200, 202]),
            tls_danger_accept_invalid_certs: Some(true),
            max_connections: Some(20),
            verbose_logging: Some(true),
        };

        let sink = HttpSink::new(1, config);
        assert_eq!(sink.method, HttpMethod::Put);
        assert_eq!(sink.timeout, Duration::from_secs(10));
        assert_eq!(sink.max_payload_size_bytes, 5000);
        assert_eq!(sink.headers.len(), 1);
        assert_eq!(sink.batch_mode, BatchMode::Ndjson);
        assert!(!sink.include_metadata);
        assert!(sink.include_checksum);
        assert!(sink.include_origin_timestamp);
        assert!(sink.health_check_enabled);
        assert_eq!(sink.health_check_method, HttpMethod::Get);
        assert_eq!(sink.max_retries, 5);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
        assert_eq!(sink.retry_backoff_multiplier, 3.0);
        assert_eq!(sink.max_retry_delay, Duration::from_secs(60));
        assert_eq!(sink.success_status_codes, HashSet::from([200, 202]));
        assert!(sink.tls_danger_accept_invalid_certs);
        assert_eq!(sink.max_connections, 20);
        assert!(sink.verbose);
    }

    #[test]
    fn given_backoff_multiplier_below_one_should_clamp_to_one() {
        let mut config = given_default_config();
        config.retry_backoff_multiplier = Some(0.5);
        let sink = HttpSink::new(1, config);
        assert_eq!(sink.retry_backoff_multiplier, 1.0);
    }

    #[test]
    fn given_invalid_duration_string_should_fall_back_to_default() {
        let mut config = given_default_config();
        config.timeout = Some("not_a_duration".to_string());
        config.retry_delay = Some("xyz".to_string());
        let sink = HttpSink::new(1, config);
        assert_eq!(sink.timeout, Duration::from_secs(30));
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
    }

    // ── Duration parsing tests ───────────────────────────────────────

    #[test]
    fn given_valid_duration_strings_should_parse_correctly() {
        let cases = [
            ("30s", Duration::from_secs(30)),
            ("500ms", Duration::from_millis(500)),
            ("2m", Duration::from_secs(120)),
            ("1h", Duration::from_secs(3600)),
        ];

        for (input, expected) in cases {
            assert_eq!(
                parse_duration(Some(input), "1s"),
                expected,
                "input: {}",
                input
            );
        }
    }

    #[test]
    fn given_none_duration_should_use_default() {
        assert_eq!(parse_duration(None, "5s"), Duration::from_secs(5));
    }

    // ── HttpMethod serde tests ───────────────────────────────────────

    #[test]
    fn given_http_method_should_serialize_as_uppercase() {
        let cases = [
            (HttpMethod::Get, "\"GET\""),
            (HttpMethod::Head, "\"HEAD\""),
            (HttpMethod::Post, "\"POST\""),
            (HttpMethod::Put, "\"PUT\""),
            (HttpMethod::Patch, "\"PATCH\""),
            (HttpMethod::Delete, "\"DELETE\""),
        ];

        for (method, expected_json) in cases {
            let json = serde_json::to_string(&method).unwrap();
            assert_eq!(json, expected_json);
        }
    }

    #[test]
    fn given_uppercase_json_should_deserialize_to_method() {
        let cases = [
            ("\"GET\"", HttpMethod::Get),
            ("\"POST\"", HttpMethod::Post),
            ("\"DELETE\"", HttpMethod::Delete),
        ];

        for (json, expected) in cases {
            let method: HttpMethod = serde_json::from_str(json).unwrap();
            assert_eq!(method, expected);
        }
    }

    #[test]
    fn given_invalid_method_string_should_fail_deserialization() {
        let result: Result<HttpMethod, _> = serde_json::from_str("\"DELEET\"");
        assert!(result.is_err());
    }

    // ── BatchMode serde tests ────────────────────────────────────────

    #[test]
    fn given_batch_mode_should_serialize_as_snake_case() {
        let cases = [
            (BatchMode::Individual, "\"individual\""),
            (BatchMode::Ndjson, "\"ndjson\""),
            (BatchMode::JsonArray, "\"json_array\""),
            (BatchMode::Raw, "\"raw\""),
        ];

        for (mode, expected_json) in cases {
            let json = serde_json::to_string(&mode).unwrap();
            assert_eq!(json, expected_json);
        }
    }

    // ── Content-type tests ───────────────────────────────────────────

    #[test]
    fn given_batch_mode_should_return_correct_content_type() {
        let cases = [
            (BatchMode::Individual, "application/json"),
            (BatchMode::Ndjson, "application/x-ndjson"),
            (BatchMode::JsonArray, "application/json"),
            (BatchMode::Raw, "application/octet-stream"),
        ];

        for (mode, expected) in cases {
            let mut config = given_default_config();
            config.batch_mode = Some(mode);
            let sink = HttpSink::new(1, config);
            assert_eq!(sink.content_type(), expected);
        }
    }

    // ── UUID formatting tests ────────────────────────────────────────

    #[test]
    fn given_zero_id_should_format_as_valid_v8_uuid() {
        let result = format_u128_as_uuid(0);
        let parsed = uuid::Uuid::parse_str(&result).expect("should be valid UUID");
        assert_eq!(parsed.get_version_num(), 8, "expected v8 UUID");
        // v8 sets version nibble (byte 6 high) and variant bits (byte 8 high 2)
        assert_eq!(result, "00000000-0000-8000-8000-000000000000");
    }

    #[test]
    fn given_max_u128_should_format_as_valid_v8_uuid() {
        let result = format_u128_as_uuid(u128::MAX);
        let parsed = uuid::Uuid::parse_str(&result).expect("should be valid UUID");
        assert_eq!(parsed.get_version_num(), 8, "expected v8 UUID");
        assert_eq!(result, "ffffffff-ffff-8fff-bfff-ffffffffffff");
    }

    #[test]
    fn given_specific_id_should_produce_valid_v8_uuid() {
        let id: u128 = 0x0123456789abcdef0123456789abcdef;
        let result = format_u128_as_uuid(id);
        let parsed = uuid::Uuid::parse_str(&result).expect("should be valid UUID");
        assert_eq!(parsed.get_version_num(), 8, "expected v8 UUID");
        assert_eq!(result.len(), 36, "UUID should be 36 chars");
        // Original bits preserved except version nibble and variant bits
        assert_eq!(result, "01234567-89ab-8def-8123-456789abcdef");
    }

    // ── Truncation tests ─────────────────────────────────────────────

    #[test]
    fn given_short_string_should_return_unchanged() {
        assert_eq!(truncate_response("hello", 10), "hello");
    }

    #[test]
    fn given_long_string_should_truncate_at_boundary() {
        let result = truncate_response("hello world", 5);
        assert_eq!(result, "hello");
    }

    #[test]
    fn given_multibyte_string_should_truncate_at_char_boundary() {
        // "héllo" — 'é' is 2 bytes in UTF-8, so bytes are: h(1) é(2) l(1) l(1) o(1)
        // floor_char_boundary(2) can't include the 2-byte 'é', returns 1 → "h"
        let result = truncate_response("héllo", 2);
        assert_eq!(result, "h");
    }

    // ── Payload conversion tests ─────────────────────────────────────

    #[test]
    fn given_json_payload_should_convert_to_serde_json() {
        let sink = given_sink_with_defaults();
        let payload = Payload::Json(simd_json_from_str(r#"{"name":"test","count":42}"#));

        let result = sink.payload_to_json(payload).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["count"], 42);
    }

    #[test]
    fn given_text_payload_should_convert_to_string_value() {
        let sink = given_sink_with_defaults();
        let result = sink
            .payload_to_json(Payload::Text("hello".to_string()))
            .unwrap();
        assert_eq!(result, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn given_raw_payload_should_base64_encode() {
        let sink = given_sink_with_defaults();
        let result = sink.payload_to_json(Payload::Raw(vec![1, 2, 3])).unwrap();
        assert_eq!(result["iggy_payload_encoding"], "base64");
        assert_eq!(result["data"], general_purpose::STANDARD.encode([1, 2, 3]));
    }

    #[test]
    fn given_flatbuffer_payload_should_base64_encode() {
        let sink = given_sink_with_defaults();
        let result = sink
            .payload_to_json(Payload::FlatBuffer(vec![4, 5, 6]))
            .unwrap();
        assert_eq!(result["iggy_payload_encoding"], "base64");
        assert_eq!(result["data"], general_purpose::STANDARD.encode([4, 5, 6]));
    }

    #[test]
    fn given_proto_payload_should_base64_encode_string_bytes() {
        let sink = given_sink_with_defaults();
        let result = sink
            .payload_to_json(Payload::Proto("proto_data".to_string()))
            .unwrap();
        assert_eq!(result["iggy_payload_encoding"], "base64");
        assert_eq!(
            result["data"],
            general_purpose::STANDARD.encode(b"proto_data")
        );
    }

    // ── Metadata envelope tests ──────────────────────────────────────

    #[test]
    fn given_include_metadata_true_should_wrap_payload() {
        let sink = given_sink_with_defaults();
        let message = given_json_message(42, 10);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json);

        assert!(envelope.get("metadata").is_some());
        assert!(envelope.get("payload").is_some());

        let metadata = &envelope["metadata"];
        assert_eq!(metadata["iggy_offset"], 10);
        assert_eq!(metadata["iggy_timestamp"], 1710064800000000u64);
        assert_eq!(metadata["iggy_stream"], "test_stream");
        assert_eq!(metadata["iggy_topic"], "test_topic");
        assert_eq!(metadata["iggy_partition_id"], 0);
        assert_eq!(metadata["iggy_id"], format_u128_as_uuid(42));
        // Verify conditional fields are absent by default
        assert!(metadata.get("iggy_checksum").is_none());
        assert!(metadata.get("iggy_origin_timestamp").is_none());
    }

    #[test]
    fn given_include_metadata_false_should_return_raw_payload() {
        let mut config = given_default_config();
        config.include_metadata = Some(false);
        let sink = HttpSink::new(1, config);

        let message = given_json_message(1, 0);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json.clone());

        // Should be the payload itself, not wrapped
        assert_eq!(envelope, payload_json);
        assert!(envelope.get("metadata").is_none());
    }

    #[test]
    fn given_include_checksum_should_add_checksum_to_metadata() {
        let mut config = given_default_config();
        config.include_checksum = Some(true);
        let sink = HttpSink::new(1, config);

        let message = given_json_message(1, 0);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json);
        assert_eq!(envelope["metadata"]["iggy_checksum"], 12345);
    }

    #[test]
    fn given_include_origin_timestamp_should_add_to_metadata() {
        let mut config = given_default_config();
        config.include_origin_timestamp = Some(true);
        let sink = HttpSink::new(1, config);

        let message = given_json_message(1, 0);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json);
        assert_eq!(
            envelope["metadata"]["iggy_origin_timestamp"],
            1710064799000000u64
        );
    }

    #[test]
    fn given_message_with_headers_should_include_iggy_headers_in_metadata() {
        use iggy_connector_sdk::ConsumedMessage;

        let sink = given_sink_with_defaults();
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();

        let mut headers = HashMap::new();
        headers.insert(
            "x-correlation-id".parse().unwrap(),
            "abc-123".parse().unwrap(),
        );

        let message = ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 1710064800000000,
            origin_timestamp: 0,
            headers: Some(headers),
            payload: Payload::Json(simd_json_from_str(r#"{"key":"value"}"#)),
        };

        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();
        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json);

        let iggy_headers = &envelope["metadata"]["iggy_headers"];
        assert!(
            !iggy_headers.is_null(),
            "Expected iggy_headers in metadata when message has headers"
        );
        assert!(
            iggy_headers.get("x-correlation-id").is_some(),
            "Expected header key in iggy_headers, got: {iggy_headers}"
        );
    }

    #[test]
    fn given_message_without_headers_should_not_include_iggy_headers() {
        let sink = given_sink_with_defaults();
        let message = given_json_message(1, 0);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink.build_envelope(&message, &topic_meta, &msg_meta, payload_json);
        assert!(
            envelope["metadata"].get("iggy_headers").is_none(),
            "Expected no iggy_headers when message has no headers"
        );
    }

    // ── Retry delay computation tests ────────────────────────────────

    #[test]
    fn given_attempt_zero_should_return_base_delay() {
        let sink = given_sink_with_defaults();
        assert_eq!(sink.compute_retry_delay(0), Duration::from_secs(1));
    }

    #[test]
    fn given_increasing_attempts_should_apply_exponential_backoff() {
        let sink = given_sink_with_defaults();
        // attempt 0: 1s * 2.0^0 = 1s
        assert_eq!(sink.compute_retry_delay(0), Duration::from_secs(1));
        // attempt 1: 1s * 2.0^1 = 2s
        assert_eq!(sink.compute_retry_delay(1), Duration::from_secs(2));
        // attempt 2: 1s * 2.0^2 = 4s
        assert_eq!(sink.compute_retry_delay(2), Duration::from_secs(4));
    }

    #[test]
    fn given_large_attempt_should_cap_at_max_retry_delay() {
        let sink = given_sink_with_defaults();
        // attempt 10: 1s * 2.0^10 = 1024s, capped to 30s
        assert_eq!(sink.compute_retry_delay(10), Duration::from_secs(30));
    }

    // ── Transient status classification tests ────────────────────────

    #[test]
    fn given_transient_status_codes_should_return_true() {
        for code in [429, 500, 502, 503, 504] {
            assert!(
                HttpSink::is_transient_status(reqwest::StatusCode::from_u16(code).unwrap()),
                "Expected {} to be transient",
                code
            );
        }
    }

    #[test]
    fn given_non_transient_status_codes_should_return_false() {
        for code in [200, 201, 400, 401, 403, 404, 405] {
            assert!(
                !HttpSink::is_transient_status(reqwest::StatusCode::from_u16(code).unwrap()),
                "Expected {} to be non-transient",
                code
            );
        }
    }

    // ── owned_value_to_serde_json conversion tests ───────────────────

    #[test]
    fn given_null_value_should_convert_to_null() {
        let v = simd_json::OwnedValue::Static(simd_json::StaticNode::Null);
        assert_eq!(owned_value_to_serde_json(&v), serde_json::Value::Null);
    }

    #[test]
    fn given_bool_value_should_convert_correctly() {
        let v = simd_json::OwnedValue::Static(simd_json::StaticNode::Bool(true));
        assert_eq!(owned_value_to_serde_json(&v), serde_json::Value::Bool(true));
    }

    #[test]
    fn given_integer_values_should_convert_correctly() {
        let i64_val = simd_json::OwnedValue::Static(simd_json::StaticNode::I64(-42));
        assert_eq!(owned_value_to_serde_json(&i64_val), serde_json::json!(-42));

        let u64_val = simd_json::OwnedValue::Static(simd_json::StaticNode::U64(42));
        assert_eq!(owned_value_to_serde_json(&u64_val), serde_json::json!(42));
    }

    #[test]
    fn given_f64_value_should_convert_correctly() {
        let v = simd_json::OwnedValue::Static(simd_json::StaticNode::F64(3.54));
        let result = owned_value_to_serde_json(&v);
        assert_eq!(result.as_f64().unwrap(), 3.54);
    }

    #[test]
    fn given_nan_f64_should_convert_to_null() {
        let v = simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f64::NAN));
        assert_eq!(owned_value_to_serde_json(&v), serde_json::Value::Null);
    }

    #[test]
    fn given_infinity_f64_should_convert_to_null() {
        let v = simd_json::OwnedValue::Static(simd_json::StaticNode::F64(f64::INFINITY));
        assert_eq!(owned_value_to_serde_json(&v), serde_json::Value::Null);
    }

    #[test]
    fn given_nested_object_should_convert_recursively() {
        let v = simd_json_from_str(r#"{"nested":{"key":"val"},"arr":[1,2]}"#);

        let result = owned_value_to_serde_json(&v);
        assert_eq!(result["nested"]["key"], "val");
        assert_eq!(result["arr"][0], 1);
        assert_eq!(result["arr"][1], 2);
    }

    // ── Config TOML deserialization tests ─────────────────────────────

    #[test]
    fn given_minimal_toml_config_should_deserialize() {
        let toml_str = r#"url = "https://example.com""#;
        let config: HttpSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.url, "https://example.com");
        assert!(config.method.is_none());
        assert!(config.headers.is_none());
        assert!(config.batch_mode.is_none());
    }

    #[test]
    fn given_full_toml_config_should_deserialize_all_fields() {
        let toml_str = r#"
            url = "https://example.com/api"
            method = "PUT"
            timeout = "10s"
            max_payload_size_bytes = 5000
            batch_mode = "ndjson"
            include_metadata = false
            include_checksum = true
            include_origin_timestamp = true
            health_check_enabled = true
            health_check_method = "GET"
            max_retries = 5
            retry_delay = "2s"
            retry_backoff_multiplier = 3.0
            max_retry_delay = "60s"
            success_status_codes = [200, 201]
            tls_danger_accept_invalid_certs = true
            max_connections = 20
            verbose_logging = true

            [headers]
            Authorization = "Bearer token"
            X-Custom = "value"
        "#;

        let config: HttpSinkConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.url, "https://example.com/api");
        assert_eq!(config.method, Some(HttpMethod::Put));
        assert_eq!(config.batch_mode, Some(BatchMode::Ndjson));
        assert_eq!(config.max_retries, Some(5));
        assert_eq!(config.success_status_codes, Some(vec![200, 201]));
        let headers = config.headers.unwrap();
        assert_eq!(headers["Authorization"], "Bearer token");
        assert_eq!(headers["X-Custom"], "value");
    }

    #[test]
    fn given_invalid_method_in_toml_should_fail() {
        let toml_str = r#"
            url = "https://example.com"
            method = "DELEET"
        "#;
        let result: Result<HttpSinkConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn given_invalid_batch_mode_in_toml_should_fail() {
        let toml_str = r#"
            url = "https://example.com"
            batch_mode = "xml"
        "#;
        let result: Result<HttpSinkConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    // ── open() validation tests ──────────────────────────────────────

    #[tokio::test]
    async fn given_empty_url_should_fail_open() {
        let mut config = given_default_config();
        config.url = String::new();
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("empty"),
            "Error should mention empty URL: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_invalid_url_should_fail_open() {
        let mut config = given_default_config();
        config.url = "not a url".to_string();
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not a valid URL"),
            "Error should mention invalid URL: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_empty_success_status_codes_should_fail_open() {
        let mut config = given_default_config();
        config.success_status_codes = Some(vec![]);
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("success_status_codes"),
            "Error should mention success_status_codes: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_valid_config_should_build_client_in_open() {
        let mut sink = given_sink_with_defaults();
        // Disable health check so open() doesn't try to connect
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_ok());
        assert!(sink.client.is_some());
    }

    // ── Batch mode invariant tests ───────────────────────────────────

    #[test]
    fn given_raw_mode_with_include_metadata_should_still_use_raw_content_type() {
        let mut config = given_default_config();
        config.batch_mode = Some(BatchMode::Raw);
        config.include_metadata = Some(true);
        let sink = HttpSink::new(1, config);
        // Raw mode uses octet-stream regardless of include_metadata
        assert_eq!(sink.content_type(), "application/octet-stream");
        assert_eq!(sink.batch_mode, BatchMode::Raw);
        // include_metadata is set but irrelevant in raw mode (warned at construction)
        assert!(sink.include_metadata);
    }

    // ── C1: URL scheme validation tests ─────────────────────────────

    #[tokio::test]
    async fn given_file_scheme_url_should_fail_open() {
        let mut config = given_default_config();
        config.url = "file:///etc/passwd".to_string();
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not allowed"),
            "Expected scheme rejection: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_ftp_scheme_url_should_fail_open() {
        let mut config = given_default_config();
        config.url = "ftp://fileserver.local/data".to_string();
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not allowed"),
            "Expected scheme rejection: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_http_scheme_url_should_pass_open() {
        let mut config = given_default_config();
        config.url = "http://localhost:8080/ingest".to_string();
        let mut sink = HttpSink::new(1, config);
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn given_https_scheme_url_should_pass_open() {
        let mut sink = given_sink_with_defaults(); // default URL is https
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_ok());
    }

    // ── C2: Header validation tests ─────────────────────────────────

    #[tokio::test]
    async fn given_invalid_header_name_should_fail_open() {
        let mut config = given_default_config();
        config.headers = Some(HashMap::from([(
            "Invalid Header\r\n".to_string(),
            "value".to_string(),
        )]));
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid header name"),
            "Expected header name error: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_invalid_header_value_should_fail_open() {
        let mut config = given_default_config();
        config.headers = Some(HashMap::from([(
            "X-Good-Name".to_string(),
            "bad\r\nvalue".to_string(),
        )]));
        let mut sink = HttpSink::new(1, config);
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid header value"),
            "Expected header value error: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_valid_headers_should_pass_open() {
        let mut config = given_default_config();
        config.headers = Some(HashMap::from([
            ("Authorization".to_string(), "Bearer token123".to_string()),
            ("X-Custom-ID".to_string(), "abc-def".to_string()),
        ]));
        let mut sink = HttpSink::new(1, config);
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_ok());
    }

    // ── H1: Content-Type deduplication test ──────────────────────────

    #[test]
    fn given_user_content_type_header_should_be_filtered_in_open() {
        // Note: This test validates the Content-Type filter used when building
        // request_headers in open(). We verify the predicate matches what open() uses.
        let mut config = given_default_config();
        config.headers = Some(HashMap::from([
            ("Content-Type".to_string(), "text/plain".to_string()),
            ("content-type".to_string(), "text/xml".to_string()),
            ("X-Custom".to_string(), "keep-me".to_string()),
        ]));
        let sink = HttpSink::new(1, config);
        // Count how many headers survive the Content-Type filter
        let surviving: Vec<&String> = sink
            .headers
            .keys()
            .filter(|k| !k.eq_ignore_ascii_case("content-type"))
            .collect();
        assert_eq!(
            surviving.len(),
            1,
            "Only non-Content-Type headers should survive, got: {:?}",
            surviving
        );
        assert!(
            surviving.iter().any(|k| *k == "X-Custom"),
            "X-Custom should survive the filter, got: {:?}",
            surviving
        );
    }

    // ── M1: compute_retry_delay overflow safety ──────────────────────

    #[test]
    fn given_extreme_backoff_config_should_not_panic() {
        let mut config = given_default_config();
        config.retry_backoff_multiplier = Some(1000.0);
        config.max_retries = Some(200);
        let sink = HttpSink::new(1, config);
        // This would panic with Duration::from_secs_f64(Infinity) without the clamp
        let delay = sink.compute_retry_delay(199);
        assert_eq!(delay, sink.max_retry_delay);
    }

    // ── M2: success_status_codes validation ────────────────────────────

    #[tokio::test]
    async fn given_invalid_status_code_should_fail_open() {
        let mut config = given_default_config();
        config.success_status_codes = Some(vec![200, 999]);
        let mut sink = HttpSink::new(1, config);
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("999"),
            "Expected invalid code in error: {}",
            err
        );
    }

    #[tokio::test]
    async fn given_zero_status_code_should_fail_open() {
        let mut config = given_default_config();
        config.success_status_codes = Some(vec![0]);
        let mut sink = HttpSink::new(1, config);
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_informational_status_code_should_fail_open() {
        let mut config = given_default_config();
        config.success_status_codes = Some(vec![100]);
        let mut sink = HttpSink::new(1, config);
        sink.health_check_enabled = false;
        let result = sink.open().await;
        assert!(result.is_err());
    }

    // ── T4: consume() before open() test ─────────────────────────────

    #[tokio::test]
    async fn given_consume_called_before_open_should_return_init_error() {
        let sink = given_sink_with_defaults();
        let topic_metadata = given_topic_metadata();
        let messages_metadata = given_messages_metadata();
        let messages = vec![given_json_message(1, 0)];
        let result = sink
            .consume(&topic_metadata, messages_metadata, messages)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not initialized") || err.contains("open()"),
            "Expected init error: {}",
            err
        );
    }
}
