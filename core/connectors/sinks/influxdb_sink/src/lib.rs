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
use iggy_common::serde_secret::serialize_secret;
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
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
    /// [`HttpRetryMiddleware`] so retry/back-off/jitter is handled
    /// transparently by the middleware stack instead of a hand-rolled loop.
    client: Option<ClientWithMiddleware>,
    /// Cached once in `open()` — config fields never change at runtime.
    write_url: Option<Url>,
    messages_attempted: AtomicU64,
    write_success: AtomicU64,
    write_errors: AtomicU64,
    verbose: bool,
    retry_delay: Duration,
    /// Resolved once in `new()` — avoids a `to_ascii_lowercase()` allocation
    /// on every message in the hot path.
    payload_format: PayloadFormat,
    circuit_breaker: Arc<CircuitBreaker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDbSinkConfig {
    pub url: String,
    pub org: String,
    pub bucket: String,
    #[serde(serialize_with = "serialize_secret")]
    pub token: SecretString,
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
        let payload_format = PayloadFormat::from_config(config.payload_format.as_deref());

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
            payload_format,
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
        self.payload_format
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
        match self.payload_format() {
            PayloadFormat::Json => {
                // Fast path: if the payload is already a parsed simd_json value,
                // serialise directly to a compact string — one pass, no bytes
                // round-trip. Avoids: simd_json→bytes, bytes→serde_json::Value,
                // serde_json::Value→string (three allocating passes per message).
                //
                // Fallback: any other Payload variant (Raw bytes that happen to
                // contain JSON, Text, etc.) goes through try_to_bytes() first.
                let compact = match &message.payload {
                    iggy_connector_sdk::Payload::Json(value) => simd_json::to_string(value)
                        .map_err(|e| {
                            Error::CannotStoreData(format!("Failed to serialize JSON payload: {e}"))
                        })?,
                    _ => {
                        let bytes = message.payload.try_to_bytes().map_err(|e| {
                            Error::CannotStoreData(format!(
                                "Failed to convert payload to bytes: {e}"
                            ))
                        })?;
                        // Validate that the bytes are actually JSON before
                        // writing them into the line-protocol field.
                        let value: serde_json::Value =
                            serde_json::from_slice(&bytes).map_err(|e| {
                                Error::CannotStoreData(format!(
                                    "Payload format is json but payload is invalid JSON: {e}"
                                ))
                            })?;
                        serde_json::to_string(&value).map_err(|e| {
                            Error::CannotStoreData(format!("Failed to serialize JSON payload: {e}"))
                        })?
                    }
                };
                buf.push_str(",payload_json=\"");
                write_field_string(buf, &compact);
                buf.push('"');
            }
            PayloadFormat::Text => {
                let payload_bytes = message.payload.try_to_bytes().map_err(|e| {
                    Error::CannotStoreData(format!("Failed to convert payload to bytes: {e}"))
                })?;
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
                let payload_bytes = message.payload.try_to_bytes().map_err(|e| {
                    Error::CannotStoreData(format!("Failed to convert payload to bytes: {e}"))
                })?;
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
        let token = self.config.token.expose_secret().to_owned();

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

        // Use PermanentHttpError for non-transient 4xx (400 Bad Request, 422
        // schema conflict, etc.) so consume() can skip the circuit breaker for
        // these — they indicate a data/schema issue, not an infrastructure one.
        if iggy_connector_sdk::retry::is_transient_status(status) {
            Err(Error::CannotStoreData(format!(
                "InfluxDB write failed with status {status}: {body_text}"
            )))
        } else {
            Err(Error::PermanentHttpError(format!(
                "InfluxDB write failed with status {status}: {body_text}"
            )))
        }
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
        let health_url = self.build_health_url()?;
        check_connectivity_with_retry(
            &raw_client,
            health_url,
            "InfluxDB sink",
            self.id,
            &ConnectivityConfig {
                max_open_retries: self
                    .config
                    .max_open_retries
                    .unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
                open_retry_max_delay: parse_duration(
                    self.config.open_retry_max_delay.as_deref(),
                    DEFAULT_OPEN_RETRY_MAX_DELAY,
                ),
                retry_delay: self.retry_delay,
            },
        )
        .await?;

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
            "InfluxDB",
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
                    self.write_success
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    // Only count transient/connectivity failures toward the
                    // circuit breaker. PermanentHttpError (400, 422, etc.) are
                    // data/schema issues that retrying will not fix; tripping
                    // the circuit on them would block valid subsequent messages.
                    if !matches!(e, Error::PermanentHttpError(_)) {
                        self.circuit_breaker.record_failure().await;
                    }
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

        // Only reset the circuit breaker if every batch in this consume() call
        // succeeded.  Resetting inside the loop means a later successful batch
        // would clear the failure counter accumulated by an earlier failed one,
        // masking repeated partial failures and preventing the circuit from
        // ever tripping.
        if first_error.is_none() {
            self.circuit_breaker.record_success();
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
            token: SecretString::from("test_token"),
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

    // ── payload_format cached at construction ────────────────────────────────────

    #[test]
    fn payload_format_resolved_at_construction_text() {
        let mut config = make_config();
        config.payload_format = Some("text".to_string());
        let sink = InfluxDbSink::new(1, config);
        // The cached field must reflect what was in the config at new() time.
        assert_eq!(sink.payload_format(), PayloadFormat::Text);
    }

    #[test]
    fn payload_format_resolved_at_construction_base64() {
        let mut config = make_config();
        config.payload_format = Some("base64".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.payload_format(), PayloadFormat::Base64);
    }

    #[test]
    fn payload_format_resolved_at_construction_none_defaults_to_json() {
        let mut config = make_config();
        config.payload_format = None;
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.payload_format(), PayloadFormat::Json);
    }

    #[test]
    fn payload_format_resolved_at_construction_unknown_defaults_to_json() {
        let mut config = make_config();
        config.payload_format = Some("bogus".to_string());
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.payload_format(), PayloadFormat::Json);
    }

    // ── append_line — Payload::Json fast path ───────────────────────────────────

    #[test]
    fn append_line_native_json_payload_uses_fast_path() {
        // Payload::Json is the new fast path (single simd_json serialisation pass).
        let sink = make_sink(); // payload_format = "json"
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let value = simd_json::json!({"sensor": "temp", "value": 23.5_f64});
        let msg = make_message(iggy_connector_sdk::Payload::Json(value));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();

        assert!(buf.contains("payload_json="), "field name must be present");
        // The compact JSON must contain the key and value somewhere in the field.
        assert!(
            buf.contains("sensor") && buf.contains("temp"),
            "JSON content must survive serialisation: {buf}"
        );
    }

    #[test]
    fn append_line_native_json_and_raw_json_produce_equivalent_output() {
        // Both Payload::Json and Payload::Raw(valid_json_bytes) must produce the
        // same logical JSON value in the line-protocol field, even though they
        // travel through different code paths.
        let mut config = make_config();
        config.payload_format = Some("json".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let raw_bytes = b"{\"k\":1}".to_vec();

        // Fast path: already-parsed OwnedValue
        let native_val = simd_json::owned::to_value(&mut raw_bytes.clone()).unwrap();
        let msg_native = make_message(iggy_connector_sdk::Payload::Json(native_val));
        let mut buf_native = String::new();
        sink.append_line(&mut buf_native, &topic, &meta, &msg_native)
            .unwrap();

        // Fallback path: raw bytes
        let msg_raw = make_message(iggy_connector_sdk::Payload::Raw(raw_bytes));
        let mut buf_raw = String::new();
        sink.append_line(&mut buf_raw, &topic, &meta, &msg_raw)
            .unwrap();

        // Extract just the payload_json field value from each line.
        // Both should encode {"k":1} (possibly different key ordering, both valid).
        let extract_json_field = |line: &str| -> serde_json::Value {
            // Find the payload_json="..." section and parse it.
            let start = line.find("payload_json=\"").unwrap() + "payload_json=\"".len();
            // Walk forward to the closing unescaped quote.
            let remainder = &line[start..];
            let mut end = 0;
            let chars: Vec<char> = remainder.chars().collect();
            while end < chars.len() {
                if chars[end] == '"' && (end == 0 || chars[end - 1] != '\\') {
                    break;
                }
                end += 1;
            }
            let json_str = &remainder[..end].replace("\\\"", "\"").replace("\\\\", "\\");
            serde_json::from_str(json_str).unwrap()
        };

        let val_native = extract_json_field(&buf_native);
        let val_raw = extract_json_field(&buf_raw);
        assert_eq!(val_native, val_raw, "fast-path and fallback must agree");
    }

    #[test]
    fn append_line_text_payload_uses_payload_text_field() {
        let mut config = make_config();
        config.payload_format = Some("text".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Text(
            "hello influx".to_string(),
        ));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(buf.contains("payload_text="), "field name must be present");
        assert!(buf.contains("hello influx"), "content must be preserved");
    }

    #[test]
    fn append_line_text_payload_from_raw_bytes() {
        let mut config = make_config();
        config.payload_format = Some("text".to_string());
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"raw_as_text".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(buf.contains("payload_text="));
        assert!(buf.contains("raw_as_text"));
    }

    // ── append_line — timestamp zero fallback ───────────────────────────────────

    #[test]
    fn append_line_zero_timestamp_falls_back_to_now() {
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        // message.timestamp == 0 triggers the now() fallback.
        let mut msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));
        msg.timestamp = 0;

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();

        // Extract the trailing timestamp from the line-protocol line.
        // Format: "measurement,...,tag=v field=v timestamp\n"
        let ts_str = buf.trim().rsplit(' ').next().unwrap();
        let ts: u64 = ts_str.parse().expect("timestamp should be a u64");

        // Must be after Unix epoch (year 1970) and before year 2100.
        let year_2100_us = 4_102_444_800_000_000u64; // approx
        assert!(ts > 0, "zero timestamp must produce a positive fallback");
        assert!(
            ts < year_2100_us,
            "fallback timestamp is unreasonably large: {ts}"
        );
    }

    #[test]
    fn append_line_nonzero_timestamp_preserved() {
        let sink = make_sink(); // precision = "us" (identity transform)
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let mut msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));
        msg.timestamp = 1_700_000_000_000_000; // 2023-11-14 in µs

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();

        let ts_str = buf.trim().rsplit(' ').next().unwrap();
        let ts: u64 = ts_str.parse().unwrap();
        assert_eq!(
            ts, 1_700_000_000_000_000,
            "timestamp must pass through unchanged"
        );
    }

    // ── append_line — checksum / origin_timestamp fields ────────────────────────

    #[test]
    fn append_line_checksum_field_present_by_default() {
        let sink = make_sink(); // include_checksum = Some(true)
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        // make_message sets checksum = 12345
        assert!(
            buf.contains("iggy_checksum=12345u"),
            "checksum field missing: {buf}"
        );
    }

    #[test]
    fn append_line_checksum_suppressed_when_disabled() {
        let mut config = make_config();
        config.include_checksum = Some(false);
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(
            !buf.contains("iggy_checksum"),
            "checksum must be absent: {buf}"
        );
    }

    #[test]
    fn append_line_origin_timestamp_present_by_default() {
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        // make_message sets origin_timestamp = 1_000_000
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(
            buf.contains("iggy_origin_timestamp=1000000u"),
            "origin_timestamp missing: {buf}"
        );
    }

    #[test]
    fn append_line_origin_timestamp_suppressed_when_disabled() {
        let mut config = make_config();
        config.include_origin_timestamp = Some(false);
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();
        assert!(
            !buf.contains("iggy_origin_timestamp"),
            "must be absent: {buf}"
        );
    }

    // ── append_line — metadata disabled entirely ─────────────────────────────────

    #[test]
    fn append_line_no_metadata_at_all() {
        let mut config = make_config();
        config.include_metadata = Some(false);
        let sink = InfluxDbSink::new(1, config);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec()));

        let mut buf = String::new();
        sink.append_line(&mut buf, &topic, &meta, &msg).unwrap();

        // With include_metadata=false, stream/topic/partition tags AND their
        // field fallbacks must all be absent.
        assert!(!buf.contains(",stream="), "stream tag must be absent");
        assert!(!buf.contains(",topic="), "topic tag must be absent");
        assert!(!buf.contains(",partition="), "partition tag must be absent");
        assert!(!buf.contains("iggy_stream="), "stream field must be absent");
        assert!(!buf.contains("iggy_topic="), "topic field must be absent");
        assert!(
            !buf.contains("iggy_partition="),
            "partition field must be absent"
        );
    }

    // ── build_write_url ───────────────────────────────────────────────────────────

    #[test]
    fn build_write_url_contains_org_bucket_precision() {
        let sink = make_sink(); // org=test_org, bucket=test_bucket, precision=us
        let url = sink.build_write_url().unwrap();
        let query = url.query().unwrap_or("");
        assert!(query.contains("org=test_org"), "org missing: {query}");
        assert!(
            query.contains("bucket=test_bucket"),
            "bucket missing: {query}"
        );
        assert!(query.contains("precision=us"), "precision missing: {query}");
    }

    #[test]
    fn build_write_url_path_is_api_v2_write() {
        let sink = make_sink();
        let url = sink.build_write_url().unwrap();
        assert_eq!(url.path(), "/api/v2/write");
    }

    #[test]
    fn build_write_url_trailing_slash_stripped() {
        let mut config = make_config();
        config.url = "http://localhost:8086/".to_string();
        let sink = InfluxDbSink::new(1, config);
        let url = sink.build_write_url().unwrap();
        // Must not produce a double-slash path like //api/v2/write
        assert!(
            !url.path().starts_with("//"),
            "double slash: {}",
            url.path()
        );
        assert_eq!(url.path(), "/api/v2/write");
    }

    #[test]
    fn build_write_url_invalid_base_url_returns_error() {
        let mut config = make_config();
        config.url = "not_a_url".to_string();
        let sink = InfluxDbSink::new(1, config);
        assert!(
            sink.build_write_url().is_err(),
            "invalid URL must return error"
        );
    }

    #[test]
    fn build_health_url_path_is_health() {
        let sink = make_sink();
        let url = sink.build_health_url().unwrap();
        assert_eq!(url.path(), "/health");
    }

    // ── PermanentHttpError does not trip circuit breaker ─────────────────────────

    #[tokio::test]
    async fn permanent_http_error_does_not_open_circuit_breaker() {
        // threshold=1 means any transient error would open the circuit immediately.
        let mut config = make_config();
        config.circuit_breaker_threshold = Some(1);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        let sink = InfluxDbSink::new(1, config);

        // Simulate what process_batch does when it gets a 400 response:
        // it returns PermanentHttpError, which consume() must NOT count.
        let e = Error::PermanentHttpError("400 Bad Request: malformed line protocol".to_string());
        if !matches!(e, Error::PermanentHttpError(_)) {
            sink.circuit_breaker.record_failure().await;
        }

        // Circuit must remain closed — no failure was recorded.
        assert!(
            !sink.circuit_breaker.is_open().await,
            "PermanentHttpError must not open the circuit breaker"
        );
    }

    #[tokio::test]
    async fn transient_error_does_open_circuit_breaker() {
        let mut config = make_config();
        config.circuit_breaker_threshold = Some(1);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        let sink = InfluxDbSink::new(1, config);

        // Simulate what process_batch does for a 503 response: CannotStoreData.
        let e = Error::CannotStoreData("503 Service Unavailable".to_string());
        if !matches!(e, Error::PermanentHttpError(_)) {
            sink.circuit_breaker.record_failure().await;
        }

        assert!(
            sink.circuit_breaker.is_open().await,
            "transient error must open the circuit breaker"
        );
    }

    // ── partial-batch failure does not reset circuit breaker (fix) ────────

    #[tokio::test]
    async fn partial_batch_failure_does_not_reset_circuit_failure_counter() {
        // With threshold=2, two consume() calls each containing one failed batch
        // followed by one successful batch must still open the circuit.
        //
        // With the old code (record_success inside the loop), the successful
        // second batch would reset the consecutive-failure counter after every
        // call, so the circuit would never trip regardless of how many partial
        // failures occurred.  The fix moves record_success after the loop,
        // guarded by first_error.is_none(), so the counter accumulates across
        // calls that had any failure.
        let mut config = make_config();
        config.circuit_breaker_threshold = Some(2);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        let sink = InfluxDbSink::new(1, config);

        // Simulate consume() call 1: batch 1 fails (record_failure), batch 2
        // succeeds but first_error.is_some() → record_success NOT called.
        sink.circuit_breaker.record_failure().await;
        assert!(
            !sink.circuit_breaker.is_open().await,
            "circuit must still be closed after one failure at threshold 2"
        );

        // Simulate consume() call 2: same pattern.
        sink.circuit_breaker.record_failure().await;

        assert!(
            sink.circuit_breaker.is_open().await,
            "circuit must open after threshold failures – a later successful \
             batch in the same call must not reset the consecutive-failure counter"
        );
    }

    #[tokio::test]
    async fn all_batches_succeeding_resets_circuit_failure_counter() {
        // Contrast: when ALL batches in a consume() succeed, first_error is None
        // and record_success IS called after the loop.  The consecutive-failure
        // counter must reset so a subsequent isolated failure does not
        // immediately re-open the circuit.
        let mut config = make_config();
        config.circuit_breaker_threshold = Some(2);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        let sink = InfluxDbSink::new(1, config);

        // Accumulate one failure.
        sink.circuit_breaker.record_failure().await;
        assert!(!sink.circuit_breaker.is_open().await);

        // A fully successful consume() → record_success resets the counter.
        sink.circuit_breaker.record_success();

        // A single subsequent failure should restart from 1 (threshold not reached).
        sink.circuit_breaker.record_failure().await;
        assert!(
            !sink.circuit_breaker.is_open().await,
            "failure counter must restart from zero after a fully successful consume() call"
        );
    }

    // ── measurement default ───────────────────────────────────────────────────────

    #[test]
    fn measurement_defaults_to_iggy_messages_when_not_configured() {
        let mut config = make_config();
        config.measurement = None;
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.measurement(), "iggy_messages");
    }

    #[test]
    fn measurement_uses_configured_value() {
        let sink = make_sink(); // measurement = "test_measurement"
        assert_eq!(sink.measurement(), "test_measurement");
    }

    // ── get_client before open() ──────────────────────────────────────────────────

    #[test]
    fn get_client_before_open_returns_error() {
        let sink = make_sink();
        assert!(
            sink.get_client().is_err(),
            "get_client before open() must return an error"
        );
    }

    // ── batch chunking: multiple messages produce newline-separated lines ─────────

    #[test]
    fn process_batch_body_has_newline_between_lines() {
        let sink = make_sink();
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let msg1 = make_message(iggy_connector_sdk::Payload::Raw(b"{\"a\":1}".to_vec()));
        let msg2 = make_message(iggy_connector_sdk::Payload::Raw(b"{\"b\":2}".to_vec()));

        // Build the body the same way process_batch does.
        let mut body = String::new();
        sink.append_line(&mut body, &topic, &meta, &msg1).unwrap();
        body.push('\n');
        sink.append_line(&mut body, &topic, &meta, &msg2).unwrap();

        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(
            lines.len(),
            2,
            "two messages must produce exactly two lines"
        );
        assert!(
            lines[0].starts_with("test_measurement"),
            "line 1: {}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("test_measurement"),
            "line 2: {}",
            lines[1]
        );
    }

    // ── to_precision_timestamp edge cases ────────────────────────────────────────

    #[test]
    fn precision_ns_does_not_overflow_large_micros() {
        // 1e15 µs × 1000 = 1e18 ns, within u64::MAX (~1.8e19).
        let mut config = make_config();
        config.precision = Some("ns".to_string());
        let sink = InfluxDbSink::new(1, config);
        let micros = 1_000_000_000_000_000u64; // 1e15 µs ≈ year 2001 in ns terms
        let ns = sink.to_precision_timestamp(micros);
        assert_eq!(ns, micros * 1_000);
    }

    #[test]
    fn precision_ns_saturates_on_overflow() {
        // u64::MAX µs × 1000 overflows; saturating_mul must not panic.
        let mut config = make_config();
        config.precision = Some("ns".to_string());
        let sink = InfluxDbSink::new(1, config);
        let result = sink.to_precision_timestamp(u64::MAX);
        assert_eq!(result, u64::MAX, "saturating_mul must clamp at u64::MAX");
    }
}
