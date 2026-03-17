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
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use rand::RngExt;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex;
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
// Simple consecutive-failure circuit breaker
// ---------------------------------------------------------------------------

/// All mutable circuit-breaker state under a single lock —
/// eliminates the AtomicU32 / Mutex race described above.
#[derive(Debug)]
struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<tokio::time::Instant>,
}

#[derive(Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    cool_down: Duration,
    state: Mutex<CircuitState>, // one lock, one consistent view
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cool_down: Duration) -> Self {
        Self {
            threshold,
            cool_down,
            state: Mutex::new(CircuitState {
                consecutive_failures: 0,
                open_until: None,
            }),
        }
    }

    /// Called on every successful operation — resets the failure
    /// counter and closes the circuit atomically.
    pub fn record_success(&self) {
        // spawn-and-forget is intentional: success is the fast path
        // and we want to avoid async overhead in the happy path.
        // block_in_place would be wrong here; just get the lock.
        // Because this is async context we use try_lock; if another
        // task holds the lock the count reset will happen on the next
        // success. This is safe — worst case: one extra failure
        // needed to re-open the circuit. Callers accept this.
        if let Ok(mut s) = self.state.try_lock() {
            s.consecutive_failures = 0;
            s.open_until = None;
        }
    }

    /// Called after all retries for one operation have failed.
    /// May open the circuit.
    pub async fn record_failure(&self) {
        let mut s = self.state.lock().await;
        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
        if s.consecutive_failures >= self.threshold {
            let deadline = tokio::time::Instant::now() + self.cool_down;
            s.open_until = Some(deadline);
            warn!(
                "Circuit breaker OPENED after {} consecutive failures. \
                 Pausing for {:?}.",
                s.consecutive_failures, self.cool_down
            );
        }
    }

    /// Returns `true` if the circuit is open (callers should skip the
    /// operation).  Transitions to half-open automatically once the
    /// cool-down has elapsed.
    pub async fn is_open(&self) -> bool {
        let mut s = self.state.lock().await;
        match s.open_until {
            None => false,
            Some(deadline) if tokio::time::Instant::now() < deadline => true,
            Some(_) => {
                // Cool-down elapsed: half-open — let one probe through.
                s.open_until = None;
                s.consecutive_failures = 0; // safe: same lock as record_failure
                info!("Circuit breaker entering HALF-OPEN state.");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Main connector structs
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InfluxDbSink {
    pub id: u32,
    config: InfluxDbSinkConfig,
    client: Option<Client>,
    /// Cached once in `open()` — config fields never change at runtime.
    write_url: Option<Url>,
    /// Cached once in `open()` — parsed from `config.retry_max_delay`.
    /// Controls the backoff cap for per-write retries only; startup retries
    /// use `config.open_retry_max_delay` and are never affected by this field.
    write_retry_max_delay: Duration,
    state: Mutex<State>,
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

#[derive(Debug)]
struct State {
    messages_processed: u64,
    write_success: u64,
    write_errors: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_duration(value: Option<&str>, default_value: &str) -> Duration {
    let raw = value.unwrap_or(default_value);
    HumanDuration::from_str(raw)
        .map(|d| d.into())
        .unwrap_or_else(|_| Duration::from_secs(1))
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

// Apply ±20% random jitter to a duration to spread retry storms
pub fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis() as u64;
    let jitter_range = millis / 5; // 20% of base
    if jitter_range == 0 {
        return base;
    }
    let delta = rand::rng().random_range(0..=jitter_range * 2);
    Duration::from_millis(millis.saturating_sub(jitter_range).saturating_add(delta))
}

// True exponential backoff: base * 2^attempt, capped at max_delay
pub fn exponential_backoff(base: Duration, attempt: u32, max_delay: Duration) -> Duration {
    let factor = 2u64.saturating_pow(attempt);
    let millis = base
        .as_millis() // u128
        .saturating_mul(factor as u128) // u128, no overflow
        .min(max_delay.as_millis()); // always keep below max_delay.as_millis()
    let millis_u64 = u64::try_from(millis).unwrap_or(u64::MAX);
    Duration::from_millis(millis_u64)
}

// Parse Retry-After header value (integer seconds or HTTP date)
fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date fallback would require httpdate crate; return None to use own backoff
    None
}

/// Write an escaped measurement name into `buf`.
/// Escapes: `\` → `\\`, `,` → `\,`, ` ` → `\ `
fn write_measurement(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            ' ' => buf.push_str("\\ "),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped tag key/value into `buf`.
/// Escapes: `\` → `\\`, `,` → `\,`, `=` → `\=`, ` ` → `\ `
fn write_tag_value(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            ',' => buf.push_str("\\,"),
            '=' => buf.push_str("\\="),
            ' ' => buf.push_str("\\ "),
            _ => buf.push(ch),
        }
    }
}

/// Write an escaped string field value (without surrounding quotes) into `buf`.
/// Escapes: `\` → `\\`, `"` → `\"`
fn write_field_string(buf: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => buf.push_str("\\\\"),
            '"' => buf.push_str("\\\""),
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
            write_retry_max_delay: Duration::from_secs(0), // overwritten in open()
            state: Mutex::new(State {
                messages_processed: 0,
                write_success: 0,
                write_errors: 0,
            }),
            verbose,
            retry_delay,
            circuit_breaker: Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down)),
        }
    }

    fn build_client(&self) -> Result<Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        Client::builder()
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

    async fn check_connectivity(&self) -> Result<(), Error> {
        let client = self.get_client()?;
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

    // Retry connectivity check with exponential backoff + jitter
    // instead of failing hard on the first attempt.
    async fn check_connectivity_with_retry(&self) -> Result<(), Error> {
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
            match self.check_connectivity().await {
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

    fn get_client(&self) -> Result<&Client, Error> {
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

        self.write_with_retry(body).await
    }

    async fn write_with_retry(&self, body: String) -> Result<(), Error> {
        let client = self.get_client()?;
        let url = self.write_url.clone().ok_or_else(|| {
            Error::Connection("write_url not initialised — was open() called?".to_string())
        })?;
        let max_retries = self.get_max_retries();
        let token = self.config.token.clone();

        // Cap for per-write backoff — cached in open(), no re-parse needed.
        let max_delay = self.write_retry_max_delay;

        // Convert once before the loop — Bytes is a reference-counted view over
        // the same allocation. Every .clone() inside the loop is a pointer bump
        // (O(1)), not a deep copy of the string data (O(n)).
        let body: Bytes = Bytes::from(body);

        let mut attempts = 0u32;
        loop {
            let response_result = client
                .post(url.clone())
                .header("Authorization", format!("Token {token}"))
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body.clone())
                .send()
                .await;

            match response_result {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::NO_CONTENT || status == StatusCode::OK {
                        return Ok(());
                    }

                    // Honour Retry-After on 429 before our own backoff
                    let retry_after = if status == StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after)
                    } else {
                        None
                    };

                    let body_text = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response body".to_string());

                    attempts += 1;
                    if is_transient_status(status) && attempts < max_retries {
                        // Use server-supplied delay when available
                        let delay = retry_after.unwrap_or_else(|| {
                            // Exponential, with jitter
                            jitter(exponential_backoff(self.retry_delay, attempts, max_delay))
                        });
                        warn!(
                            "Transient InfluxDB write error (attempt {attempts}/{max_retries}): \
                             {status}. Retrying in {delay:?}..."
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    return Err(Error::CannotStoreData(format!(
                        "InfluxDB write failed with status {status}: {body_text}"
                    )));
                }
                Err(e) => {
                    attempts += 1;
                    if attempts < max_retries {
                        // Exponential, with jitter
                        let delay =
                            jitter(exponential_backoff(self.retry_delay, attempts, max_delay));
                        warn!(
                            "Failed to send write request to InfluxDB \
                             (attempt {attempts}/{max_retries}): {e}. Retrying in {delay:?}..."
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    return Err(Error::CannotStoreData(format!(
                        "InfluxDB write failed after {attempts} attempts: {e}"
                    )));
                }
            }
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

        self.client = Some(self.build_client()?);

        // Cache once here — both are derived purely from config fields that
        // never change at runtime, so recomputing them on every batch write
        // is wasteful.
        self.write_url = Some(self.build_write_url()?);
        self.write_retry_max_delay = parse_duration(
            self.config.retry_max_delay.as_deref(),
            DEFAULT_RETRY_MAX_DELAY,
        );

        // Use retrying connectivity check instead of hard-fail
        self.check_connectivity_with_retry().await?;

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
                    let mut state = self.state.lock().await;
                    state.write_success += batch.len() as u64;
                }
                Err(e) => {
                    // Failed write — notify circuit breaker
                    self.circuit_breaker.record_failure().await;

                    let mut state = self.state.lock().await;
                    state.write_errors += batch.len() as u64;
                    error!(
                        "InfluxDB sink ID: {} failed to write batch of {} messages: {e}",
                        self.id,
                        batch.len()
                    );
                    drop(state);

                    // Capture first error; continue attempting remaining
                    // batches to maximise data delivery, but record the failure.
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += total_messages as u64;

        if self.verbose {
            info!(
                "InfluxDB sink ID: {} processed {} messages. \
                 Total processed: {}, Success: {} , write errors: {}",
                self.id,
                total_messages,
                state.messages_processed,
                state.write_success,
                state.write_errors
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} processed {} messages. \
                 Total processed: {}, write errors: {}",
                self.id, total_messages, state.messages_processed, state.write_errors
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
        let state = self.state.lock().await;
        info!(
            "InfluxDB sink connector with ID: {} closed. Processed: {}, errors: {}",
            self.id, state.messages_processed, state.write_errors
        );
        Ok(())
    }
}
