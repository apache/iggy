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
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    RetryTransientMiddleware, Retryable, RetryableStrategy, policies::ExponentialBackoff,
};
use reqwest_tracing::TracingMiddleware;
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
const DEFAULT_BACKOFF_MULTIPLIER: u32 = 2;
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

const ENCODING_BASE64: &str = "base64";

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
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum_macros::Display,
)]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    /// One HTTP request per message (default). Note: with batch_length=50, this produces 50
    /// sequential HTTP round trips per poll cycle. Use ndjson or json_array for higher throughput.
    #[default]
    #[strum(to_string = "individual")]
    Individual,
    /// All messages in one request, newline-delimited JSON.
    /// Note: `rename = "ndjson"` overrides the enum-level `rename_all = "snake_case"` which
    /// would produce "and_json". Industry standard is "ndjson" (one word, per ndjson.org).
    #[serde(rename = "ndjson")]
    #[strum(to_string = "NDJSON")]
    NdJson,
    /// All messages as a single JSON array.
    #[strum(to_string = "JSON array")]
    JsonArray,
    /// Raw bytes, one request per message (for non-JSON payloads).
    #[strum(to_string = "raw")]
    Raw,
}

impl BatchMode {
    /// Determine the Content-Type header based on batch mode.
    fn content_type(&self) -> &'static str {
        match self {
            BatchMode::Individual | BatchMode::JsonArray => "application/json",
            BatchMode::NdJson => "application/x-ndjson",
            BatchMode::Raw => "application/octet-stream",
        }
    }
}

/// Metadata envelope wrapping a payload with Iggy message metadata.
#[derive(Debug, Serialize)]
struct MetadataEnvelope {
    metadata: IggyMetadata,
    payload: serde_json::Value,
}

/// Iggy message metadata fields.
///
/// Field naming convention: `iggy_*` without leading underscore. The Elasticsearch sink
/// uses `_iggy_*` (leading underscore, ES convention for internal fields). This divergence
/// is intentional — HTTP endpoints follow standard JSON conventions, not ES-specific ones.
/// A follow-up to unify the convention across sinks may be worthwhile.
#[derive(Debug, Serialize)]
struct IggyMetadata {
    iggy_id: String,
    iggy_offset: u64,
    iggy_timestamp: u64,
    iggy_stream: String,
    iggy_topic: String,
    iggy_partition_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_checksum: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_origin_timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iggy_headers: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Binary payload with base64 encoding marker.
#[derive(Debug, Serialize)]
struct EncodedPayload {
    data: String,
    iggy_payload_encoding: &'static str,
}

/// Binary header value with base64 encoding marker.
#[derive(Debug, Serialize)]
struct EncodedHeader {
    data: String,
    iggy_header_encoding: &'static str,
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
    /// Backoff multiplier for exponential retry delay (default: 2).
    pub retry_backoff_multiplier: Option<u32>,
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
    /// URL with userinfo (user:password) stripped for safe logging.
    log_url: String,
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
    retry_backoff_multiplier: u32,
    max_retry_delay: Duration,
    success_status_codes: HashSet<u16>,
    tls_danger_accept_invalid_certs: bool,
    max_connections: usize,
    verbose: bool,
    /// Pre-built HTTP headers (excluding Content-Type). Built once in `open()` from validated
    /// `self.headers`, reused for every request. `None` before `open()` is called.
    request_headers: Option<reqwest::header::HeaderMap>,
    /// Initialized in `open()` with config-derived settings. `None` before `open()` is called.
    client: Option<ClientWithMiddleware>,
    send_attempts: AtomicU64,
    messages_delivered: AtomicU64,
    errors_count: AtomicU64,
    /// Epoch seconds of last successful HTTP request.
    last_success_timestamp: AtomicU64,
}

impl HttpSink {
    pub fn new(id: u32, config: HttpSinkConfig) -> Self {
        let url = config.url;
        let log_url = sanitize_url_for_log(&url);
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
        let mut retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_backoff_multiplier = config
            .retry_backoff_multiplier
            .unwrap_or(DEFAULT_BACKOFF_MULTIPLIER)
            .max(1);
        let mut max_retry_delay =
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
                 Swapping values to prevent ExponentialBackoff panic.",
                id, retry_delay, max_retry_delay,
            );
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
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
            log_url,
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
            send_attempts: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            last_success_timestamp: AtomicU64::new(0),
        }
    }

    /// Build the `reqwest::Client` wrapped with retry and tracing middleware.
    fn build_client(&self) -> Result<ClientWithMiddleware, Error> {
        let raw_client = reqwest::Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(self.max_connections)
            .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
            .tcp_keepalive(Duration::from_secs(DEFAULT_TCP_KEEPALIVE_SECS))
            .danger_accept_invalid_certs(self.tls_danger_accept_invalid_certs)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {}", e)))?;

        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(self.retry_delay, self.max_retry_delay)
            .base(self.retry_backoff_multiplier)
            .build_with_max_retries(self.max_retries);

        let retry_strategy = HttpSinkRetryStrategy {
            success_status_codes: self.success_status_codes.clone(),
        };

        let retry_middleware =
            RetryTransientMiddleware::new_with_policy_and_strategy(retry_policy, retry_strategy);

        Ok(ClientBuilder::new(raw_client)
            .with(TracingMiddleware::default())
            .with(retry_middleware)
            .build())
    }

    /// Returns the initialized HTTP client, or an error if `open()` was not called.
    fn client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client.as_ref().ok_or_else(|| {
            Error::InitError("HTTP client not initialized — was open() called?".to_string())
        })
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
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) => {
                let encoded = EncodedPayload {
                    data: general_purpose::STANDARD.encode(&bytes),
                    iggy_payload_encoding: ENCODING_BASE64,
                };
                serde_json::to_value(encoded)
                    .map_err(|e| Error::Serialization(format!("EncodedPayload: {}", e)))
            }
            Payload::Proto(proto_str) => {
                let encoded = EncodedPayload {
                    data: general_purpose::STANDARD.encode(proto_str.as_bytes()),
                    iggy_payload_encoding: ENCODING_BASE64,
                };
                serde_json::to_value(encoded)
                    .map_err(|e| Error::Serialization(format!("EncodedPayload: {}", e)))
            }
        }
    }

    /// Build the `IggyMetadata` struct from message and topic context.
    /// Shared by `build_envelope` (all batch modes call through this single entry point).
    fn build_iggy_metadata(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<IggyMetadata, Error> {
        let headers_map = if let Some(ref headers) = message.headers
            && !headers.is_empty()
        {
            let map: serde_json::Map<String, serde_json::Value> = headers
                .iter()
                .map(|(k, v)| {
                    let value = if let Ok(raw) = v.as_raw() {
                        let encoded = EncodedHeader {
                            data: general_purpose::STANDARD.encode(raw),
                            iggy_header_encoding: ENCODING_BASE64,
                        };
                        serde_json::to_value(encoded)
                            .map_err(|e| Error::Serialization(format!("EncodedHeader: {}", e)))?
                    } else {
                        serde_json::Value::String(v.to_string_value())
                    };
                    Ok((k.to_string_value(), value))
                })
                .collect::<Result<serde_json::Map<String, serde_json::Value>, Error>>()?;
            Some(map)
        } else {
            None
        };

        Ok(IggyMetadata {
            iggy_id: format_u128_as_hex(message.id),
            iggy_offset: message.offset,
            iggy_timestamp: message.timestamp,
            iggy_stream: topic_metadata.stream.clone(),
            iggy_topic: topic_metadata.topic.clone(),
            iggy_partition_id: messages_metadata.partition_id,
            iggy_checksum: if self.include_checksum {
                Some(message.checksum)
            } else {
                None
            },
            iggy_origin_timestamp: if self.include_origin_timestamp {
                Some(message.origin_timestamp)
            } else {
                None
            },
            iggy_headers: headers_map,
        })
    }

    /// Build a message envelope as `serde_json::Value`.
    /// Single entry point for all batch modes — callers choose serialization format:
    /// - `json_array`: uses the `Value` directly in `Vec<Value>`
    /// - `individual`: `serde_json::to_vec(&envelope)`
    /// - `ndjson`: `serde_json::to_writer(&mut body, &envelope)`
    fn build_envelope(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        payload_json: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        if !self.include_metadata {
            return Ok(payload_json);
        }

        let metadata = self.build_iggy_metadata(message, topic_metadata, messages_metadata)?;
        let envelope = MetadataEnvelope {
            metadata,
            payload: payload_json,
        };

        serde_json::to_value(envelope)
            .map_err(|e| Error::Serialization(format!("MetadataEnvelope: {}", e)))
    }

    /// Record a successful request timestamp.
    fn record_success(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_success_timestamp.store(now, Ordering::Relaxed);
    }

    /// Send an HTTP request with retry via reqwest-middleware. Returns Ok on success,
    /// Err after exhausting retries. Retry logic (backoff, transient classification)
    /// is handled by the middleware configured in `build_client()`.
    async fn send_with_retry(&self, body: Bytes, content_type: &str) -> Result<(), Error> {
        let client = self.client()?;
        let headers = self.request_headers.as_ref().ok_or_else(|| {
            Error::InitError("HTTP headers not initialized — was open() called?".to_string())
        })?;

        if self.verbose {
            debug!(
                "HTTP sink ID: {} — sending {:?} {} ({} bytes)",
                self.id,
                self.method,
                self.log_url,
                body.len(),
            );
        }

        self.send_attempts.fetch_add(1, Ordering::Relaxed);

        let response = build_request(self.method, client, &self.url)
            .headers(headers.clone())
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                error!(
                    "HTTP sink ID: {} — request to {} failed after middleware retries: {:#}",
                    self.id, self.log_url, e
                );
                Error::HttpRequestFailed(format!("HTTP {} — {}", self.log_url, e))
            })?;

        let status = response.status();
        // success_status_codes is checked in BOTH the retry strategy (to stop retrying)
        // AND here (to classify the final response). Both must use the same set.
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

        // Non-success status after middleware exhausted retries — read body for diagnostics
        let response_body = match response.text().await {
            Ok(body) => body,
            Err(e) => format!("<body read error: {}>", e),
        };

        error!(
            "HTTP sink ID: {} — request failed (status {}). Response: {}",
            self.id,
            status.as_u16(),
            truncate_response(&response_body, 500),
        );
        self.errors_count.fetch_add(1, Ordering::Relaxed);
        Err(Error::HttpRequestFailed(format!(
            "HTTP {} — status: {}",
            self.log_url,
            status.as_u16()
        )))
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
        messages: Vec<ConsumedMessage>,
        content_type: &str,
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
                        self.id, self.batch_mode, offset, e
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
                    self.batch_mode,
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

            match self.send_with_retry(Bytes::from(body), content_type).await {
                Ok(()) => {
                    delivered += 1;
                    consecutive_failures = 0;
                }
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — failed to deliver {} message at offset {} after retries: {}",
                        self.id, self.batch_mode, offset, e
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
                            self.id, self.batch_mode, consecutive_failures, skipped,
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
                    self.id,
                    self.batch_mode,
                    delivered,
                    total,
                    http_failures,
                    serialization_failures,
                );
                Err(e)
            }
            None => Ok(()),
        }
    }

    /// Send messages in `individual` mode — one HTTP request per message.
    async fn send_individual(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.send_per_message(messages, self.batch_mode.content_type(), |mut message| {
            let payload = std::mem::replace(&mut message.payload, Payload::Raw(vec![]));
            let payload_json = self.payload_to_json(payload)?;
            let envelope =
                self.build_envelope(&message, topic_metadata, messages_metadata, payload_json)?;
            serde_json::to_vec(&envelope)
                .map_err(|e| Error::Serialization(format!("Envelope serialize: {}", e)))
        })
        .await
    }

    /// Sends a batch body and updates delivery/error accounting.
    ///
    /// Shared by `send_ndjson` and `send_json_array` — the post-send accounting logic
    /// (error propagation, skip warnings) is identical across batch modes.
    async fn send_batch_body(&self, body: Bytes, count: u64, skipped: u64) -> Result<(), Error> {
        debug_assert!(
            count > 0,
            "send_batch_body called with count=0 — callers must guard against empty batches"
        );
        if let Err(e) = self
            .send_with_retry(body, self.batch_mode.content_type())
            .await
        {
            // INVARIANT: send_with_retry increments self.errors_count exactly once before
            // returning Err (at line ~568 for middleware errors, or ~603 for non-success status).
            // We add the remaining (count - 1) messages that were serialized but not delivered.
            // If this invariant changes, error accounting will silently miscount.
            self.errors_count
                .fetch_add(count.saturating_sub(1), Ordering::Relaxed);
            if skipped > 0 {
                error!(
                    "HTTP sink ID: {} — {} batch failed with {} serialization skips",
                    self.id, self.batch_mode, skipped,
                );
            }
            return Err(e);
        }
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
        if skipped > 0 {
            warn!(
                "HTTP sink ID: {} — {} batch: {} delivered, {} skipped (serialization errors)",
                self.id, self.batch_mode, count, skipped,
            );
        }
        Ok(())
    }

    /// Send messages in `ndjson` mode — all messages in one request, newline-delimited.
    /// Skips individual messages that fail serialization rather than aborting the batch.
    async fn send_ndjson(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        // Write directly to a single Vec<u8> — avoids the intermediate Vec<String>
        // and the join("\n") copy that doubles memory usage.
        let mut body = Vec::with_capacity(messages.len() * 256);
        let mut count = 0u64;
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
            let envelope = match self.build_envelope(
                &message,
                topic_metadata,
                messages_metadata,
                payload_json,
            ) {
                Ok(env) => env,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — skipping message at offset {} in NDJSON batch (envelope): {}",
                        self.id, message.offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    skipped += 1;
                    continue;
                }
            };
            let pos = body.len();
            if let Err(e) = serde_json::to_writer(&mut body, &envelope) {
                body.truncate(pos);
                error!(
                    "HTTP sink ID: {} — skipping message at offset {} in NDJSON batch (serialize): {}",
                    self.id, message.offset, e
                );
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                skipped += 1;
                continue;
            }
            body.push(b'\n');
            count += 1;
        }

        if count == 0 {
            return Err(Error::Serialization(
                "All messages in NDJSON batch failed serialization".to_string(),
            ));
        }

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

        self.send_batch_body(Bytes::from(body), count, skipped)
            .await
    }

    /// Send messages in `json_array` mode — all messages as a single JSON array.
    /// Skips individual messages that fail serialization rather than aborting the batch.
    async fn send_json_array(
        &self,
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
            let envelope = match self.build_envelope(
                &message,
                topic_metadata,
                messages_metadata,
                payload_json,
            ) {
                Ok(env) => env,
                Err(e) => {
                    error!(
                        "HTTP sink ID: {} — skipping message at offset {} in JSON array batch (envelope): {}",
                        self.id, message.offset, e
                    );
                    self.errors_count.fetch_add(1, Ordering::Relaxed);
                    skipped += 1;
                    continue;
                }
            };
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

        self.send_batch_body(Bytes::from(body), count, skipped)
            .await
    }

    /// Send messages in `raw` mode — one HTTP request per message with raw bytes.
    async fn send_raw(&self, messages: Vec<ConsumedMessage>) -> Result<(), Error> {
        self.send_per_message(messages, self.batch_mode.content_type(), |message| {
            message
                .payload
                .try_into_vec()
                .map_err(|e| Error::Serialization(format!("Raw payload convert: {}", e)))
        })
        .await
    }
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

/// Custom retry strategy that respects user-configured success_status_codes.
///
/// Codes in the success set are never retried (even if normally transient like 429).
/// Remaining 429/5xx are classified as transient for retry.
struct HttpSinkRetryStrategy {
    success_status_codes: HashSet<u16>,
}

impl RetryableStrategy for HttpSinkRetryStrategy {
    fn handle(&self, res: &reqwest_middleware::Result<reqwest::Response>) -> Option<Retryable> {
        match res {
            Ok(response) => {
                let status = response.status().as_u16();
                if self.success_status_codes.contains(&status) {
                    return None;
                }
                if let Some(retry_after) = response.headers().get(reqwest::header::RETRY_AFTER) {
                    let header_str = retry_after.to_str().unwrap_or("<non-ascii>");
                    warn!(
                        "Server returned {} with Retry-After: {} — middleware uses computed \
                         backoff which may be insufficient",
                        status, header_str,
                    );
                }
                match status {
                    429 | 500 | 502 | 503 | 504 => Some(Retryable::Transient),
                    _ => Some(Retryable::Fatal),
                }
            }
            Err(_) => Some(Retryable::Transient),
        }
    }
}

/// Map an `HttpMethod` to a `reqwest_middleware::RequestBuilder` for the given URL.
fn build_request(
    method: HttpMethod,
    client: &ClientWithMiddleware,
    url: &str,
) -> reqwest_middleware::RequestBuilder {
    match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Head => client.head(url),
        HttpMethod::Post => client.post(url),
        HttpMethod::Put => client.put(url),
        HttpMethod::Patch => client.patch(url),
        HttpMethod::Delete => client.delete(url),
    }
}

/// Format a u128 message ID as a 32-character lowercase hex string (no dashes).
fn format_u128_as_hex(id: u128) -> String {
    format!("{:032x}", id)
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

/// Strip userinfo (user:password) from a URL for safe logging.
/// Preserves the original string exactly when no userinfo is present (avoids URL normalization).
/// Falls back to stripping `://user:pass@` patterns when URL parsing fails.
fn sanitize_url_for_log(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) if parsed.username().is_empty() && parsed.password().is_none() => {
            url.to_string()
        }
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        Err(_) => {
            // Fallback for unparsable URLs: strip userinfo to prevent credential leak.
            if let Some(scheme_end) = url.find("://") {
                let after_scheme = &url[scheme_end + 3..];
                if let Some(at_pos) = after_scheme.find('@') {
                    // Only strip if '@' comes before the first '/' (it's userinfo, not a path segment)
                    let slash_pos = after_scheme.find('/').unwrap_or(after_scheme.len());
                    if at_pos < slash_pos {
                        return format!(
                            "{}{}",
                            &url[..scheme_end + 3],
                            &after_scheme[at_pos + 1..]
                        );
                    }
                }
            }
            url.to_string()
        }
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
                        scheme, self.log_url,
                    )));
                }
            }
            Err(e) => {
                return Err(Error::InitError(format!(
                    "HTTP sink URL '{}' is not a valid URL: {}",
                    self.log_url, e,
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
                self.batch_mode.content_type(),
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
                Error::Connection(format!(
                    "Health check failed for URL '{}': {}",
                    self.log_url, e
                ))
            })?;

            let status = response.status();
            if !self.success_status_codes.contains(&status.as_u16()) {
                return Err(Error::Connection(format!(
                    "Health check returned status {} (not in success_status_codes {:?}) for URL '{}'",
                    status.as_u16(),
                    self.success_status_codes,
                    self.log_url,
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
            self.id, self.log_url, self.method, self.batch_mode, self.timeout, self.max_retries,
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

        let result = match self.batch_mode {
            BatchMode::Individual => {
                self.send_individual(topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::NdJson => {
                self.send_ndjson(topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::JsonArray => {
                self.send_json_array(topic_metadata, &messages_metadata, messages)
                    .await
            }
            BatchMode::Raw => self.send_raw(messages).await,
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
        let requests = self.send_attempts.load(Ordering::Relaxed);
        let delivered = self.messages_delivered.load(Ordering::Relaxed);
        let errors = self.errors_count.load(Ordering::Relaxed);
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);

        info!(
            "HTTP sink connector ID: {} closed. Stats: {} send attempts, \
             {} messages delivered, {} errors, last success epoch: {}.",
            self.id, requests, delivered, errors, last_success,
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

    const FIELD_DATA: &str = "data";
    const FIELD_PAYLOAD_ENCODING: &str = "iggy_payload_encoding";
    const FIELD_METADATA: &str = "metadata";
    const FIELD_PAYLOAD: &str = "payload";
    const FIELD_ID: &str = "iggy_id";
    const FIELD_OFFSET: &str = "iggy_offset";
    const FIELD_TIMESTAMP: &str = "iggy_timestamp";
    const FIELD_STREAM: &str = "iggy_stream";
    const FIELD_TOPIC: &str = "iggy_topic";
    const FIELD_PARTITION_ID: &str = "iggy_partition_id";
    const FIELD_CHECKSUM: &str = "iggy_checksum";
    const FIELD_ORIGIN_TIMESTAMP: &str = "iggy_origin_timestamp";
    const FIELD_HEADERS: &str = "iggy_headers";

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
            batch_mode: Some(BatchMode::NdJson),
            include_metadata: Some(false),
            include_checksum: Some(true),
            include_origin_timestamp: Some(true),
            health_check_enabled: Some(true),
            health_check_method: Some(HttpMethod::Get),
            max_retries: Some(5),
            retry_delay: Some("500ms".to_string()),
            retry_backoff_multiplier: Some(3),
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
        assert_eq!(sink.batch_mode, BatchMode::NdJson);
        assert!(!sink.include_metadata);
        assert!(sink.include_checksum);
        assert!(sink.include_origin_timestamp);
        assert!(sink.health_check_enabled);
        assert_eq!(sink.health_check_method, HttpMethod::Get);
        assert_eq!(sink.max_retries, 5);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
        assert_eq!(sink.retry_backoff_multiplier, 3);
        assert_eq!(sink.max_retry_delay, Duration::from_secs(60));
        assert_eq!(sink.success_status_codes, HashSet::from([200, 202]));
        assert!(sink.tls_danger_accept_invalid_certs);
        assert_eq!(sink.max_connections, 20);
        assert!(sink.verbose);
    }

    #[test]
    fn given_backoff_multiplier_below_one_should_clamp_to_one() {
        let mut config = given_default_config();
        config.retry_backoff_multiplier = Some(0);
        let sink = HttpSink::new(1, config);
        assert_eq!(sink.retry_backoff_multiplier, 1);
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

    #[test]
    fn given_batch_mode_should_serialize_as_snake_case() {
        let cases = [
            (BatchMode::Individual, "\"individual\""),
            (BatchMode::NdJson, "\"ndjson\""),
            (BatchMode::JsonArray, "\"json_array\""),
            (BatchMode::Raw, "\"raw\""),
        ];

        for (mode, expected_json) in cases {
            let json = serde_json::to_string(&mode).unwrap();
            assert_eq!(json, expected_json);
        }
    }

    #[test]
    fn given_batch_mode_display_should_return_human_readable_name() {
        assert_eq!(BatchMode::Individual.to_string(), "individual");
        assert_eq!(BatchMode::NdJson.to_string(), "NDJSON");
        assert_eq!(BatchMode::JsonArray.to_string(), "JSON array");
        assert_eq!(BatchMode::Raw.to_string(), "raw");
    }

    #[test]
    fn given_batch_mode_should_return_correct_content_type() {
        let cases = [
            (BatchMode::Individual, "application/json"),
            (BatchMode::NdJson, "application/x-ndjson"),
            (BatchMode::JsonArray, "application/json"),
            (BatchMode::Raw, "application/octet-stream"),
        ];

        for (mode, expected) in cases {
            assert_eq!(mode.content_type(), expected);
        }
    }

    #[test]
    fn given_zero_id_should_format_as_32_char_hex() {
        let result = format_u128_as_hex(0);
        assert_eq!(result.len(), 32);
        assert_eq!(result, "00000000000000000000000000000000");
    }

    #[test]
    fn given_max_u128_should_format_as_32_char_hex() {
        let result = format_u128_as_hex(u128::MAX);
        assert_eq!(result.len(), 32);
        assert_eq!(result, "ffffffffffffffffffffffffffffffff");
    }

    #[test]
    fn given_specific_id_should_produce_correct_hex() {
        let id: u128 = 0x0123456789abcdef0123456789abcdef;
        let result = format_u128_as_hex(id);
        assert_eq!(result.len(), 32);
        assert_eq!(result, "0123456789abcdef0123456789abcdef");
    }

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
        assert_eq!(result[FIELD_PAYLOAD_ENCODING], "base64");
        assert_eq!(
            result[FIELD_DATA],
            general_purpose::STANDARD.encode([1, 2, 3])
        );
    }

    #[test]
    fn given_flatbuffer_payload_should_base64_encode() {
        let sink = given_sink_with_defaults();
        let result = sink
            .payload_to_json(Payload::FlatBuffer(vec![4, 5, 6]))
            .unwrap();
        assert_eq!(result[FIELD_PAYLOAD_ENCODING], "base64");
        assert_eq!(
            result[FIELD_DATA],
            general_purpose::STANDARD.encode([4, 5, 6])
        );
    }

    #[test]
    fn given_proto_payload_should_base64_encode_string_bytes() {
        let sink = given_sink_with_defaults();
        let result = sink
            .payload_to_json(Payload::Proto("proto_data".to_string()))
            .unwrap();
        assert_eq!(result[FIELD_PAYLOAD_ENCODING], "base64");
        assert_eq!(
            result[FIELD_DATA],
            general_purpose::STANDARD.encode(b"proto_data")
        );
    }

    #[test]
    fn given_include_metadata_true_should_wrap_payload() {
        let sink = given_sink_with_defaults();
        let message = given_json_message(42, 10);
        let topic_meta = given_topic_metadata();
        let msg_meta = given_messages_metadata();
        let payload_json = sink.payload_to_json(message.payload.clone()).unwrap();

        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json)
            .unwrap();

        assert!(envelope.get(FIELD_METADATA).is_some());
        assert!(envelope.get(FIELD_PAYLOAD).is_some());

        let metadata = &envelope[FIELD_METADATA];
        assert_eq!(metadata[FIELD_OFFSET], 10);
        assert_eq!(metadata[FIELD_TIMESTAMP], 1710064800000000u64);
        assert_eq!(metadata[FIELD_STREAM], "test_stream");
        assert_eq!(metadata[FIELD_TOPIC], "test_topic");
        assert_eq!(metadata[FIELD_PARTITION_ID], 0);
        assert_eq!(metadata[FIELD_ID], format_u128_as_hex(42));
        // Verify conditional fields are absent by default
        assert!(metadata.get(FIELD_CHECKSUM).is_none());
        assert!(metadata.get(FIELD_ORIGIN_TIMESTAMP).is_none());
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

        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json.clone())
            .unwrap();

        // Should be the payload itself, not wrapped
        assert_eq!(envelope, payload_json);
        assert!(envelope.get(FIELD_METADATA).is_none());
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

        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json)
            .unwrap();
        assert_eq!(envelope[FIELD_METADATA][FIELD_CHECKSUM], 12345);
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

        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json)
            .unwrap();
        assert_eq!(
            envelope[FIELD_METADATA][FIELD_ORIGIN_TIMESTAMP],
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
        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json)
            .unwrap();

        let iggy_headers = &envelope[FIELD_METADATA][FIELD_HEADERS];
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

        let envelope = sink
            .build_envelope(&message, &topic_meta, &msg_meta, payload_json)
            .unwrap();
        assert!(
            envelope[FIELD_METADATA].get(FIELD_HEADERS).is_none(),
            "Expected no iggy_headers when message has no headers"
        );
    }

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
            retry_backoff_multiplier = 3
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
        assert_eq!(config.batch_mode, Some(BatchMode::NdJson));
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

    #[test]
    fn given_raw_mode_with_include_metadata_should_still_use_raw_content_type() {
        let mut config = given_default_config();
        config.batch_mode = Some(BatchMode::Raw);
        config.include_metadata = Some(true);
        let sink = HttpSink::new(1, config);
        // Raw mode uses octet-stream regardless of include_metadata
        assert_eq!(sink.batch_mode.content_type(), "application/octet-stream");
        assert_eq!(sink.batch_mode, BatchMode::Raw);
        // include_metadata is set but irrelevant in raw mode (warned at construction)
        assert!(sink.include_metadata);
    }

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
}
