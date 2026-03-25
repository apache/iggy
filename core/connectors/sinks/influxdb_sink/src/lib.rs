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
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use iggy_connector_sdk::retry::{
    CircuitBreaker, build_retry_client, exponential_backoff, jitter, parse_duration,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tracing::{debug, error, info, warn};
sink_connector!(InfluxDbSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_PRECISION: &str = "us";
// Maximum attempts for open() connectivity retries
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
// Cap for exponential backoff in open() — never wait longer than this
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
// Cap for exponential backoff on per-write retries — kept short so a
// transient InfluxDB blip does not stall message delivery for too long
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
// How many consecutive batch failures open the circuit breaker
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
// How long the circuit stays open before allowing a probe attempt
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

// ---------------------------------------------------------------------------
// Main connector structs
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InfluxDbSink {
    pub id: u32,
    config: InfluxDbSinkConfig,
    /// `None` until `open()` is called. Wraps `reqwest::Client` with
    /// [`InfluxDbRetryMiddleware`] so retry/back-off/jitter is handled
    /// transparently by the middleware stack instead of a hand-rolled loop.
    client: Option<ClientWithMiddleware>,
    /// Cached once in `open()` — config fields never change at runtime.
    write_url: Option<Url>,
    messages_attempted: AtomicU64,
    write_success: AtomicU64,
    write_errors: AtomicU64,
    verbose: bool,
    retry_delay: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDbSinkConfig {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
    pub measurement: Option<String>,
    pub precision: Option<String>,
    pub batch_size: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub include_stream_tag: Option<bool>,
    pub include_topic_tag: Option<bool>,
    pub include_partition_tag: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub timeout: Option<String>,
    // How many times open() will retry before giving up
    pub max_open_retries: Option<u32>,
    // Upper cap on open() backoff delay — can be set high (e.g. "60s") for
    // patient startup without affecting per-write retry behaviour
    pub open_retry_max_delay: Option<String>,
    // Upper cap on per-write retry backoff — kept short so a transient blip
    // does not stall message delivery; independent of open_retry_max_delay
    pub retry_max_delay: Option<String>,
    // Circuit breaker configuration
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PayloadFormat {
    #[default]
    Json,
    Text,
    Base64,
}

impl PayloadFormat {
    fn from_config(value: Option<&str>) -> Self {
        match value.map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("text") | Some("utf8") => PayloadFormat::Text,
            Some("base64") | Some("raw") => PayloadFormat::Base64,
            Some("json") => PayloadFormat::Json,
            other => {
                warn!(
                    "Unrecognized payload_format value {:?}, falling back to JSON. \
                     Valid values are: \"json\", \"text\", \"utf8\", \"base64\", \"raw\".",
                    other
                );
                PayloadFormat::Json
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write an escaped measurement name into `buf`.
/// Escapes: `\` → `\\`, `,` → `\,`, ` ` → `\ `, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline (`\n`) and carriage-return (`\r`) are the InfluxDB line-protocol
/// record delimiters; a literal newline inside a measurement name would split
/// the line and corrupt the batch.
fn write_measurement(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            ' ' => buf.push_str("\\ "),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped tag key/value into `buf`.
/// Escapes: `\` → `\\`, `,` → `\,`, `=` → `\=`, ` ` → `\ `, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline and carriage-return are escaped for the same reason as in
/// [`write_measurement`]: they are InfluxDB line-protocol record delimiters.
fn write_tag_value(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            '=' => buf.push_str("\\="),
            ' ' => buf.push_str("\\ "),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped string field value (without surrounding quotes) into `buf`.
/// Escapes: `\` → `\\`, `"` → `\"`, `\n` → `\\n`, `\r` → `\\r`
///
/// Newline and carriage-return are the InfluxDB line-protocol record
/// delimiters; a literal newline inside a string field value (e.g. from a
/// multi-line text payload) would split the line and corrupt the batch.
fn write_field_string(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            '"' => buf.push_str("\\\""),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            _ => buf.push(ch),
        }
    }
}

// ---------------------------------------------------------------------------
// InfluxDbSink implementation
// ---------------------------------------------------------------------------

impl InfluxDbSink {
    pub fn new(id: u32, config: InfluxDbSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);

        // Build circuit breaker from config
        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CIRCUIT_COOL_DOWN,
        );

        InfluxDbSink {
            id,
            config,
            client: None,
            write_url: None,
            messages_attempted: AtomicU64::new(0),
            write_success: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            verbose,
            retry_delay,
            circuit_breaker: Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down)),
        }
    }

    fn build_raw_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn build_write_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        let mut url = Url::parse(&format!("{base}/api/v2/write"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        let precision = self
            .config
            .precision
            .as_deref()
            .unwrap_or(DEFAULT_PRECISION);
        url.query_pairs_mut()
            .append_pair("org", &self.config.org)
            .append_pair("bucket", &self.config.bucket)
            .append_pair("precision", precision);

        Ok(url)
    }

    fn build_health_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }

    /// Single connectivity probe using the provided raw client (no retry).
    /// The caller (`check_connectivity_with_retry`) is responsible for the
    /// outer retry loop, which uses different delay bounds than per-write retries.
    async fn check_connectivity(&self, client: &reqwest::Client) -> Result<(), Error> {
        let url = self.build_health_url()?;

        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("InfluxDB health check failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read response body".to_string());
            return Err(Error::Connection(format!(
                "InfluxDB health check returned status {status}: {body}"
            )));
        }

        Ok(())
    }

    /// Retry connectivity check with exponential backoff + jitter instead of
    /// failing hard on the first attempt.
    ///
    /// Uses a separate `max_open_retries` / `open_retry_max_delay` so startup
    /// can wait patiently for InfluxDB without affecting the per-write retry
    /// parameters used by the middleware during normal operation.
    async fn check_connectivity_with_retry(&self, client: &reqwest::Client) -> Result<(), Error> {
        let max_open_retries = self
            .config
            .max_open_retries
            .unwrap_or(DEFAULT_MAX_OPEN_RETRIES)
            .max(1);

        let max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

        let mut attempt = 0u32;
        loop {
            match self.check_connectivity(client).await {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            "InfluxDB connectivity established after {attempt} retries \
                             for sink connector ID: {}",
                            self.id
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_open_retries {
                        error!(
                            "InfluxDB connectivity check failed after {attempt} attempts \
                             for sink connector ID: {}. Giving up: {e}",
                            self.id
                        );
                        return Err(e);
                    }
                    // Exponential backoff, with jitter
                    let backoff = jitter(exponential_backoff(self.retry_delay, attempt, max_delay));
                    warn!(
                        "InfluxDB health check failed (attempt {attempt}/{max_open_retries}) \
                         for sink connector ID: {}. Retrying in {backoff:?}: {e}",
                        self.id
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client is not initialized".to_string()))
    }

    fn measurement(&self) -> &str {
        self.config
            .measurement
            .as_deref()
            .unwrap_or("iggy_messages")
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn timestamp_precision(&self) -> &str {
        self.config
            .precision
            .as_deref()
            .unwrap_or(DEFAULT_PRECISION)
    }

    fn get_max_retries(&self) -> u32 {
        self.config
            .max_retries
            .unwrap_or(DEFAULT_MAX_RETRIES)
            .max(1)
    }

    fn to_precision_timestamp(&self, micros: u64) -> u64 {
        match self.timestamp_precision() {
            "ns" => micros.saturating_mul(1_000),
            "us" => micros,
            "ms" => micros / 1_000,
            "s" => micros / 1_000_000,
            _ => micros,
        }
    }

    /// Serialise one message as a line-protocol line, appending directly into
    /// `buf` with no intermediate `Vec<String>` for tags or fields.
    ///
    /// # Allocation budget (per message, happy path)
    /// - Zero `Vec` allocations for tags or fields.
    /// - Zero per-tag/per-field `format!` allocations.
    /// - One `Vec<u8>` for `payload_bytes` (unavoidable — payload must be
    ///   decoded/serialised before it can be escaped into the buffer).
    /// - The caller's `buf` grows in place; if it was pre-allocated with
    ///   `with_capacity` it will not reallocate for typical message sizes.
    fn append_line(
        &self,
        buf: &mut String,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &ConsumedMessage,
    ) -> Result<(), Error> {
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let include_stream_tag = self.config.include_stream_tag.unwrap_or(true);
        let include_topic_tag = self.config.include_topic_tag.unwrap_or(true);
        let include_partition_tag = self.config.include_partition_tag.unwrap_or(true);

        // ── Measurement ──────────────────────────────────────────────────────
        write_measurement(buf, self.measurement());

        // ── Tag set ──────────────────────────────────────────────────────────
        // Tags are written as ",key=value" pairs directly into buf.
        // The offset tag is always present — it makes every point unique in
        // InfluxDB's deduplication key (measurement + tag set + timestamp),
        // regardless of precision or how many messages share a timestamp.
        if include_metadata && include_stream_tag {
            buf.push_str(",stream=");
            write_tag_value(buf, &topic_metadata.stream);
        }
        if include_metadata && include_topic_tag {
            buf.push_str(",topic=");
            write_tag_value(buf, &topic_metadata.topic);
        }
        if include_metadata && include_partition_tag {
            use std::fmt::Write as _;
            write!(buf, ",partition={}", messages_metadata.partition_id)
                .expect("write to String is infallible");
        }
        // offset tag — always written, ensures point uniqueness
        {
            use std::fmt::Write as _;
            write!(buf, ",offset={}", message.offset).expect("write to String is infallible");
        }

        // ── Field set ────────────────────────────────────────────────────────
        // First field: no leading comma.  All subsequent fields: leading comma.
        buf.push(' ');

        buf.push_str("message_id=\"");
        write_field_string(buf, &message.id.to_string());
        buf.push('"');

        // offset as a numeric field (queryable in Flux) in addition to the tag
        {
            use std::fmt::Write as _;
            write!(buf, ",offset={}u", message.offset).expect("write to String is infallible");
        }

        // Optional metadata fields written when the corresponding tag is
        // disabled (so the value is still queryable as a field).
        if include_metadata && !include_stream_tag {
            buf.push_str(",iggy_stream=\"");
            write_field_string(buf, &topic_metadata.stream);
            buf.push('"');
        }
        if include_metadata && !include_topic_tag {
            buf.push_str(",iggy_topic=\"");
            write_field_string(buf, &topic_metadata.topic);
            buf.push('"');
        }
        if include_metadata && !include_partition_tag {
            use std::fmt::Write as _;
            write!(
                buf,
                ",iggy_partition={}u",
                messages_metadata.partition_id as u64
            )
            .expect("write to String is infallible");
        }
        if include_checksum {
            use std::fmt::Write as _;
            write!(buf, ",iggy_checksum={}u", message.checksum)
                .expect("write to String is infallible");
        }
        if include_origin_timestamp {
            use std::fmt::Write as _;
            write!(buf, ",iggy_origin_timestamp={}u", message.origin_timestamp)
                .expect("write to String is infallible");
        }

        // ── Payload field ────────────────────────────────────────────────────
        // try_as_bytes() serialises in-place without cloning the Payload tree.
        let payload_bytes = message.payload.try_as_bytes().map_err(|e| {
            Error::CannotStoreData(format!("Failed to convert payload to bytes: {e}"))
        })?;

        match self.payload_format() {
            PayloadFormat::Json => {
                let value: serde_json::Value =
                    serde_json::from_slice(&payload_bytes).map_err(|e| {
                        Error::CannotStoreData(format!(
                            "Payload format is json but payload is invalid JSON: {e}"
                        ))
                    })?;
                let compact = serde_json::to_string(&value).map_err(|e| {
                    Error::CannotStoreData(format!("Failed to serialize JSON payload: {e}"))
                })?;
                buf.push_str(",payload_json=\"");
                write_field_string(buf, &compact);
                buf.push('"');
            }
            PayloadFormat::Text => {
                let text = String::from_utf8(payload_bytes).map_err(|e| {
                    Error::CannotStoreData(format!(
                        "Payload format is text but payload is invalid UTF-8: {e}"
                    ))
                })?;
                buf.push_str(",payload_text=\"");
                write_field_string(buf, &text);
                buf.push('"');
            }
            PayloadFormat::Base64 => {
                let encoded = general_purpose::STANDARD.encode(&payload_bytes);
                buf.push_str(",payload_base64=\"");
                write_field_string(buf, &encoded);
                buf.push('"');
            }
        }

        // ── Timestamp ────────────────────────────────────────────────────────
        // message.timestamp is microseconds since Unix epoch.
        // Fall back to now() when unset (0) so points are not stored at the
        // Unix epoch (year 1970), which falls outside every range(start:-1h).
        let base_micros = if message.timestamp == 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64
        } else {
            message.timestamp
        };
        let ts = self.to_precision_timestamp(base_micros);

        {
            use std::fmt::Write as _;
            write!(buf, " {ts}").expect("write to String is infallible");
        }

        debug!(
            "InfluxDB sink ID: {} point — offset={}, raw_ts={}, influx_ts={ts}",
            self.id, message.offset, message.timestamp
        );

        Ok(())
    }

    async fn process_batch(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        // Single buffer for the entire batch — reused across all messages.
        // Pre-allocate a generous estimate (256 bytes per message) to avoid
        // reallocation in the common case.  The buffer is passed into
        // append_line() which writes each line directly, with '\n' separators
        // between lines.  No per-message String is allocated.
        let mut body = String::with_capacity(messages.len() * 256);

        for (i, message) in messages.iter().enumerate() {
            if i > 0 {
                body.push('\n');
            }
            self.append_line(&mut body, topic_metadata, messages_metadata, message)?;
        }

        let client = self.get_client()?;
        let url = self.write_url.clone().ok_or_else(|| {
            Error::Connection("write_url not initialised — was open() called?".to_string())
        })?;
        let token = self.config.token.clone();

        // Convert once before sending — Bytes is reference-counted so any
        // retry inside the middleware clones the pointer, not the payload data.
        let body: Bytes = Bytes::from(body);

        let response = client
            .post(url)
            .header("Authorization", format!("Token {token}"))
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await
            .map_err(|e| Error::CannotStoreData(format!("InfluxDB write failed: {e}")))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body_text = response
            .text()
            .await
            .unwrap_or_else(|_| "failed to read response body".to_string());
        Err(Error::CannotStoreData(format!(
            "InfluxDB write failed with status {status}: {body_text}"
        )))
    }
}

// ---------------------------------------------------------------------------
// Sink trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Sink for InfluxDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening InfluxDB sink connector with ID: {}. Bucket: {}, org: {}",
            self.id, self.config.bucket, self.config.org
        );

        // Build the raw client first and use it for the startup connectivity
        // check. The connectivity retry loop uses separate delay bounds
        // (open_retry_max_delay) from the per-write middleware retries, so
        // we keep them independent rather than routing health checks through
        // the write-tuned middleware.
        let raw_client = self.build_raw_client()?;
        self.check_connectivity_with_retry(&raw_client).await?;

        // Wrap in the retry middleware for all subsequent write operations.
        // The middleware handles transient 429 / 5xx retries with
        // exponential back-off, jitter, and Retry-After header support.
        let max_retries = self.get_max_retries();
        let write_retry_max_delay = parse_duration(
            self.config.retry_max_delay.as_deref(),
            DEFAULT_RETRY_MAX_DELAY,
        );
        self.client = Some(build_retry_client(
            raw_client,
            max_retries,
            self.retry_delay,
            write_retry_max_delay,
        ));

        // Cache once — both are derived purely from config fields that
        // never change at runtime.
        self.write_url = Some(self.build_write_url()?);

        info!(
            "InfluxDB sink connector with ID: {} opened successfully",
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
        let batch_size = self.config.batch_size.unwrap_or(500) as usize;
        let total_messages = messages.len();

        // Skip writes entirely if circuit breaker is open
        if self.circuit_breaker.is_open().await {
            warn!(
                "InfluxDB sink ID: {} — circuit breaker is OPEN. \
                 Skipping {} messages to avoid hammering a down InfluxDB.",
                self.id, total_messages
            );
            // Return an error so the runtime knows messages were not written
            return Err(Error::CannotStoreData(
                "Circuit breaker is open — InfluxDB write skipped".to_string(),
            ));
        }

        // Collect the first batch error rather than silently dropping
        let mut first_error: Option<Error> = None;

        for batch in messages.chunks(batch_size.max(1)) {
            match self
                .process_batch(topic_metadata, &messages_metadata, batch)
                .await
            {
                Ok(()) => {
                    // Successful write — reset circuit breaker
                    self.circuit_breaker.record_success();
                    self.write_success
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    // Failed write — notify circuit breaker
                    self.circuit_breaker.record_failure().await;
                    self.write_errors
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    error!(
                        "InfluxDB sink ID: {} failed to write batch of {} messages: {e}",
                        self.id,
                        batch.len()
                    );

                    // Capture first error; continue attempting remaining
                    // batches to maximise data delivery, but record the failure.
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        let total_processed = self
            .messages_attempted
            .fetch_add(total_messages as u64, Ordering::Relaxed)
            + total_messages as u64;

        if self.verbose {
            info!(
                "InfluxDB sink ID: {} processed {} messages. \
                 Total processed: {}, Success: {}, write errors: {}",
                self.id,
                total_messages,
                total_processed,
                self.write_success.load(Ordering::Relaxed),
                self.write_errors.load(Ordering::Relaxed),
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} processed {} messages. \
                 Total processed: {}, Success: {}, write errors: {}",
                self.id,
                total_messages,
                total_processed,
                self.write_success.load(Ordering::Relaxed),
                self.write_errors.load(Ordering::Relaxed),
            );
        }

        // Propagate the first batch error to the runtime so it can
        // decide whether to retry, halt, or dead-letter — instead of returning Ok(())
        // and silently losing messages.
        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None; // release connection pool
        info!(
            "InfluxDB sink connector with ID: {} closed. Processed: {}, Success: {}, errors: {}",
            self.id,
            self.messages_attempted.load(Ordering::Relaxed),
            self.write_success.load(Ordering::Relaxed),
            self.write_errors.load(Ordering::Relaxed),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::{MessagesMetadata, Schema, TopicMetadata};

    fn make_config() -> InfluxDbSinkConfig {
        InfluxDbSinkConfig {
            url: "http://localhost:8086".to_string(),
            org: "test_org".to_string(),
            bucket: "test_bucket".to_string(),
            token: "test_token".to_string(),
            measurement: Some("test_measurement".to_string()),
            precision: Some("us".to_string()),
            batch_size: None,
            include_metadata: Some(true),
            include_checksum: Some(true),
            include_origin_timestamp: Some(true),
            include_stream_tag: Some(true),
            include_topic_tag: Some(true),
            include_partition_tag: Some(true),
            payload_format: Some("json".to_string()),
            verbose_logging: None,
            max_retries: Some(3),
            retry_delay: Some("100ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(3),
            open_retry_max_delay: Some("5s".to_string()),
            retry_max_delay: Some("1s".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
        }
    }

    fn make_sink() -> InfluxDbSink {
        InfluxDbSink::new(1, make_config())
    }

    fn make_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

    fn make_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: Schema::Json,
        }
    }

    fn make_message(payload: iggy_connector_sdk::Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 7,
            checksum: 12345,
            timestamp: 1_000_000,
            origin_timestamp: 1_000_000,
            headers: None,
            payload,
        }
    }

    // ── to_precision_timestamp ───────────────────────────────────────────

    #[test]
    fn precision_ns_multiplies_by_1000() {
        let mut config = make_config();
        config.precision = Some("ns".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(1_000_000), 1_000_000_000);
    }

    #[test]
    fn precision_us_is_identity() {
        let mut config = make_config();
        config.precision = Some("us".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(1_234_567), 1_234_567);
    }

    #[test]
    fn precision_ms_divides_by_1000() {
        let mut config = make_config();
        config.precision = Some("ms".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(5_000_000), 5_000);
    }

    #[test]
    fn precision_s_divides_by_1_000_000() {
        let mut config = make_config();
        config.precision = Some("s".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(7_000_000), 7);
    }

    #[test]
    fn precision_unknown_falls_back_to_us() {
        let mut config = make_config();
        config.precision = Some("xx".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(999), 999);
    }

    // ── line-protocol escaping ───────────────────────────────────────────

    #[test]
    fn measurement_escapes_comma_space_backslash() {
        let mut buf = String::new();
        write_measurement(&mut buf, "m\\eas,urea meant");
        assert_eq!(buf, "m\\\\eas\\,urea\\ meant");
    }

    #[test]
    fn measurement_escapes_newlines() {
        let mut buf = String::new();
        write_measurement(&mut buf, "meas\nurea\rment");
        assert_eq!(buf, "meas\\nurea\\rment");
    }
    #[test]
    fn tag_value_escapes_equals_sign() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "a=b,c d\\e");
        assert_eq!(buf, "a\\=b\\,c\\ d\\\\e");
    }

    #[test]
    fn tag_value_escapes_newlines() {
        let mut buf = String::new();
        write_tag_value(&mut buf, "line1\nline2\r");
        assert_eq!(buf, "line1\\nline2\\r");
    }

    #[test]
    fn field_string_escapes_quote_and_backslash() {
        let mut buf = String::new();
        write_field_string(&mut buf, r#"say "hello" \world\"#);
        assert_eq!(buf, r#"say \"hello\" \\world\\"#);
    }

    #[test]
    fn field_string_escapes_newlines() {
        // A \n inside a string field value would split the line-protocol record.
        let mut buf = String::new();
        write_field_string(&mut buf, "line1\nline2\r");
        assert_eq!(buf, "line1\\nline2\\r");
    }
    // ── append_line error paths ──────────────────────────────────────────

    #[test]
    fn append_line_invalid_json_payload_returns_error() {
        let mut config = make_config();
        config.payload_format = Some("json".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        // Raw bytes that are not valid JSON
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"not json!".to_vec()));

        let mut buf = String::new();
        let result = sink.append_line(&mut buf, &topic, &meta, &msg);
        assert!(result.is_err(), "invalid JSON payload should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid JSON") || err.contains("JSON"),
            "error should mention JSON: {err}"
        );
    }

    #[test]
    fn append_line_invalid_utf8_text_payload_returns_error() {
        let mut config = make_config();
        config.payload_format = Some("text".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        // Invalid UTF-8 sequence
        let msg = make_message(iggy_connector_sdk::Payload::Raw(vec![0xff, 0xfe, 0xfd]));

        let mut buf = String::new();
        let result = sink.append_line(&mut buf, &topic, &meta, &msg);
        assert!(result.is_err(), "invalid UTF-8 payload should fail");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("UTF-8") || err.contains("utf"),
            "error should mention UTF-8: {err}"
        );
    }

    #[test]
    fn append_line_valid_json_payload_succeeds() {
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec()));

        let mut buf = String::new();
        assert!(sink.append_line(&mut buf, &topic, &meta, &msg).is_ok());
        assert!(buf.contains("payload_json="), "should have json field");
    }

    #[test]
    fn append_line_base64_payload_succeeds() {
        let mut config = make_config();
        config.payload_format = Some("base64".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"binary data".to_vec()));

        let mut buf = String::new();
        assert!(sink.append_line(&mut buf, &topic, &meta, &msg).is_ok());
        assert!(buf.contains("payload_base64="), "should have base64 field");
    }

    #[test]
    fn append_line_offset_tag_always_present() {
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        // offset=7 should appear as a tag
        assert!(
            buf.contains(",offset=7"),
            "offset tag should always be present"
        );
    }

    #[test]
    fn append_line_includes_measurement_name() {
        let sink = make_sink(); // measurement = "test_measurement"
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(
            buf.starts_with("test_measurement"),
            "line should start with measurement name"
        );
    }

    #[test]
    fn append_line_partial_metadata_tags_suppressed() {
        let mut config = make_config();
        config.include_stream_tag = Some(false);
        config.include_topic_tag = Some(false);
        config.include_partition_tag = Some(false);
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(!buf.contains(",stream="), "stream tag should be suppressed");
        assert!(!buf.contains(",topic="), "topic tag should be suppressed");
        assert!(
            !buf.contains(",partition="),
            "partition tag should be suppressed"
        );
        // Values should appear as fields instead
        assert!(
            buf.contains("iggy_stream="),
            "stream should appear as field"
        );
        assert!(buf.contains("iggy_topic="), "topic should appear as field");
    }

    // ── circuit breaker integration ──────────────────────────────────────

    #[tokio::test]
    async fn consume_returns_error_when_circuit_is_open() {
        let mut config = make_config();
        // Threshold of 1 means circuit opens after first failure
        config.circuit_breaker_threshold = Some(1);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        let sink = InfluxDbSink::new(1, config);

        // Force the circuit open
        sink.circuit_breaker.record_failure().await;
        assert!(sink.circuit_breaker.is_open().await);

        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let result = sink.consume(&topic, meta, vec![]).await;

        assert!(result.is_err(), "consume should fail when circuit is open");
        let err = result.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("circuit breaker"),
            "error should mention circuit breaker: {err}"
        );
    }

    #[tokio::test]
    async fn consume_succeeds_with_empty_messages_when_circuit_closed() {
        // Open the connector so write_url is set (needed if non-empty batch)
        // With empty messages, process_batch returns Ok(()) immediately.
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        // Empty message list — no HTTP call needed, should succeed even without open()
        let result = sink.consume(&topic, meta, vec![]).await;
        assert!(result.is_ok(), "empty consume should succeed: {:?}", result);
    }

    // ── close() ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn close_drops_client_and_logs() {
        let mut sink = make_sink();
        // close() before open() should be safe
        let result = sink.close().await;
        assert!(result.is_ok());
        assert!(sink.client.is_none(), "client should be None after close");
    }

    // ── payload_format fallback ──────────────────────────────────────────

    #[test]
    fn unknown_payload_format_falls_back_to_json() {
        assert_eq!(
            PayloadFormat::from_config(Some("unknown_format")),
            PayloadFormat::Json
        );
    }

    #[test]
    fn payload_format_aliases() {
        assert_eq!(
            PayloadFormat::from_config(Some("utf8")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("raw")),
            PayloadFormat::Base64
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Json);
    }
}
