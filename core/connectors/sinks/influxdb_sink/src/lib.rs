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

// =============================================================================
// CHANGES FROM ORIGINAL — all fixes are marked with [FIX-SINK-N] comments:
//
// [FIX-SINK-1] open() now retries connectivity with exponential backoff+jitter
//              instead of failing hard when InfluxDB is unavailable at startup.
// [FIX-SINK-2] write_with_retry() uses true exponential backoff (2^attempt)
//              instead of linear (delay * attempt).
// [FIX-SINK-3] Added random jitter (±20%) to every retry delay to avoid
//              thundering herd across multiple connector instances.
// [FIX-SINK-4] On HTTP 429 Too Many Requests, the Retry-After response header
//              is parsed and honoured instead of using the fixed retry_delay.
// [FIX-SINK-5] Added a circuit breaker (ConsecutiveFailureBreaker) that opens
//              after max_retries consecutive batch failures, pausing writes for
//              a configurable cool-down before attempting again.
// [FIX-SINK-6] consume() now propagates batch write errors to the runtime
//              instead of silently dropping messages with Ok(()). Individual
//              batch errors are collected and the first failure is returned,
//              which prevents silent data loss.
// [FIX-SINK-7] Added DEFAULT_MAX_OPEN_RETRIES / max_open_retries config field
//              to control how many times open() retries before giving up.
// [FIX-SINK-8] Added DEFAULT_OPEN_RETRY_MAX_DELAY cap so backoff in open()
//              doesn't grow unboundedly.
// =============================================================================

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use rand::Rng;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(InfluxDbSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_PRECISION: &str = "ms";
// [FIX-SINK-7] Maximum attempts for open() connectivity retries
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
// [FIX-SINK-8] Cap for exponential backoff in open() — never wait longer than this
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
// [FIX-SINK-5] How many consecutive batch failures open the circuit breaker
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
// [FIX-SINK-5] How long the circuit stays open before allowing a probe attempt
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

// ---------------------------------------------------------------------------
// Simple consecutive-failure circuit breaker
// ---------------------------------------------------------------------------
#[derive(Debug)]
struct CircuitBreaker {
    threshold: u32,
    consecutive_failures: AtomicU32,
    open_until: Mutex<Option<tokio::time::Instant>>,
    cool_down: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, cool_down: Duration) -> Self {
        CircuitBreaker {
            threshold,
            consecutive_failures: AtomicU32::new(0),
            open_until: Mutex::new(None),
            cool_down,
        }
    }

    /// Call when a batch write succeeds — resets failure count and closes circuit.
    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
    }

    /// Call when a batch write fails after all retries — may open the circuit.
    async fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= self.threshold {
            let mut guard = self.open_until.lock().await;
            let deadline = tokio::time::Instant::now() + self.cool_down;
            *guard = Some(deadline);
            warn!(
                "Circuit breaker OPENED after {failures} consecutive batch failures. \
                 Pausing writes for {:?}.",
                self.cool_down
            );
        }
    }

    /// Returns true if the circuit is currently open (writes should be skipped).
    async fn is_open(&self) -> bool {
        let mut guard = self.open_until.lock().await;
        if let Some(deadline) = *guard {
            if tokio::time::Instant::now() < deadline {
                return true;
            }
            // Cool-down elapsed — allow one probe attempt (half-open state)
            *guard = None;
            self.consecutive_failures.store(0, Ordering::SeqCst);
            info!("Circuit breaker entering HALF-OPEN state — probing InfluxDB.");
        }
        false
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
    // Upper cap on open() backoff delay
    pub open_retry_max_delay: Option<String>,
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
            _ => PayloadFormat::Json,
        }
    }
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
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
fn jitter(base: Duration) -> Duration {
    let millis = base.as_millis() as u64;
    let jitter_range = millis / 5; // 20% of base
    if jitter_range == 0 {
        return base;
    }
    let delta = rand::rng().random_range(0..=jitter_range * 2);
    Duration::from_millis(millis.saturating_sub(jitter_range) + delta)
}

// [FIX-SINK-2] True exponential backoff: base * 2^attempt, capped at max_delay
fn exponential_backoff(base: Duration, attempt: u32, max_delay: Duration) -> Duration {
    let factor = 2u64.saturating_pow(attempt);
    let raw = Duration::from_millis(base.as_millis().saturating_mul(factor as u128) as u64);
    raw.min(max_delay)
}

// [FIX-SINK-4] Parse Retry-After header value (integer seconds or HTTP date)
fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date fallback would require httpdate crate; return None to use own backoff
    None
}

fn escape_measurement(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace(' ', "\\ ")
}

fn escape_tag_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

fn escape_field_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

// ---------------------------------------------------------------------------
// InfluxDbSink implementation
// ---------------------------------------------------------------------------

impl InfluxDbSink {
    pub fn new(id: u32, config: InfluxDbSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);

        // [FIX-SINK-5] Build circuit breaker from config
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
            state: Mutex::new(State {
                messages_processed: 0,
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

    // [FIX-SINK-1] Retry connectivity check with exponential backoff + jitter
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
                    // [FIX-SINK-2] Exponential backoff, [FIX-SINK-3] with jitter
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

    fn to_precision_timestamp(&self, millis: u64) -> u64 {
        match self.timestamp_precision() {
            "ns" => millis.saturating_mul(1_000_000),
            "us" => millis.saturating_mul(1_000),
            "s" => millis / 1_000,
            _ => millis,
        }
    }

    fn line_from_message(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &ConsumedMessage,
    ) -> Result<String, Error> {
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);

        let mut tags = Vec::new();
        if include_metadata && self.config.include_stream_tag.unwrap_or(true) {
            tags.push(format!(
                "stream={}",
                escape_tag_value(&topic_metadata.stream)
            ));
        }
        if include_metadata && self.config.include_topic_tag.unwrap_or(true) {
            tags.push(format!("topic={}", escape_tag_value(&topic_metadata.topic)));
        }
        if include_metadata && self.config.include_partition_tag.unwrap_or(true) {
            tags.push(format!("partition={}", messages_metadata.partition_id));
        }

        let mut fields = vec![
            format!(
                "message_id=\"{}\"",
                escape_field_string(&message.id.to_string())
            ),
            format!("offset={}i", message.offset as i64),
        ];

        if include_metadata && !self.config.include_stream_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_stream=\"{}\"",
                escape_field_string(&topic_metadata.stream)
            ));
        }
        if include_metadata && !self.config.include_topic_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_topic=\"{}\"",
                escape_field_string(&topic_metadata.topic)
            ));
        }
        if include_metadata && !self.config.include_partition_tag.unwrap_or(true) {
            fields.push(format!(
                "iggy_partition={}i",
                messages_metadata.partition_id as i64
            ));
        }

        if include_checksum {
            fields.push(format!("iggy_checksum={}i", message.checksum as i64));
        }
        if include_origin_timestamp {
            fields.push(format!(
                "iggy_origin_timestamp={}i",
                message.origin_timestamp as i64
            ));
        }

        let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
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
                fields.push(format!(
                    "payload_json=\"{}\"",
                    escape_field_string(&compact)
                ));
            }
            PayloadFormat::Text => {
                let text = String::from_utf8(payload_bytes).map_err(|e| {
                    Error::CannotStoreData(format!(
                        "Payload format is text but payload is invalid UTF-8: {e}"
                    ))
                })?;
                fields.push(format!("payload_text=\"{}\"", escape_field_string(&text)));
            }
            PayloadFormat::Base64 => {
                let encoded = general_purpose::STANDARD.encode(payload_bytes);
                fields.push(format!(
                    "payload_base64=\"{}\"",
                    escape_field_string(&encoded)
                ));
            }
        }

        let measurement = escape_measurement(self.measurement());
        let tags_fragment = if tags.is_empty() {
            String::new()
        } else {
            format!(",{}", tags.join(","))
        };

        let ts = self.to_precision_timestamp(message.timestamp);
        Ok(format!(
            "{measurement}{tags_fragment} {} {ts}",
            fields.join(",")
        ))
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

        let mut lines = Vec::with_capacity(messages.len());
        for message in messages {
            lines.push(self.line_from_message(topic_metadata, messages_metadata, message)?);
        }

        let body = lines.join("\n");
        self.write_with_retry(body).await
    }

    async fn write_with_retry(&self, body: String) -> Result<(), Error> {
        let client = self.get_client()?;
        let url = self.build_write_url()?;
        let max_retries = self.get_max_retries();
        let token = self.config.token.clone();

        // [FIX-SINK-8] Cap for per-write backoff
        let max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

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

                    // [FIX-SINK-4] Honour Retry-After on 429 before our own backoff
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
                        // [FIX-SINK-4] Use server-supplied delay when available
                        let delay = retry_after.unwrap_or_else(|| {
                            // [FIX-SINK-2] Exponential, [FIX-SINK-3] with jitter
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
                        // [FIX-SINK-2] Exponential, [FIX-SINK-3] with jitter
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

        // [FIX-SINK-1] Use retrying connectivity check instead of hard-fail
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

        // [FIX-SINK-5] Skip writes entirely if circuit breaker is open
        if self.circuit_breaker.is_open().await {
            warn!(
                "InfluxDB sink ID: {} — circuit breaker is OPEN. \
                 Skipping {} messages to avoid hammering a down InfluxDB.",
                self.id, total_messages
            );
            // [FIX-SINK-6] Return an error so the runtime knows messages were not written
            return Err(Error::CannotStoreData(
                "Circuit breaker is open — InfluxDB write skipped".to_string(),
            ));
        }

        // [FIX-SINK-6] Collect the first batch error rather than silently dropping
        let mut first_error: Option<Error> = None;

        for batch in messages.chunks(batch_size.max(1)) {
            match self
                .process_batch(topic_metadata, &messages_metadata, batch)
                .await
            {
                Ok(()) => {
                    // [FIX-SINK-5] Successful write — reset circuit breaker
                    self.circuit_breaker.record_success();
                }
                Err(e) => {
                    // [FIX-SINK-5] Failed write — notify circuit breaker
                    self.circuit_breaker.record_failure().await;

                    let mut state = self.state.lock().await;
                    state.write_errors += batch.len() as u64;
                    error!(
                        "InfluxDB sink ID: {} failed to write batch of {} messages: {e}",
                        self.id,
                        batch.len()
                    );
                    drop(state);

                    // [FIX-SINK-6] Capture first error; continue attempting remaining
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
                 Total processed: {}, write errors: {}",
                self.id, total_messages, state.messages_processed, state.write_errors
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} processed {} messages. \
                 Total processed: {}, write errors: {}",
                self.id, total_messages, state.messages_processed, state.write_errors
            );
        }

        // [FIX-SINK-6] Propagate the first batch error to the runtime so it can
        // decide whether to retry, halt, or dead-letter — instead of returning Ok(())
        // and silently losing messages.
        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "InfluxDB sink connector with ID: {} closed. Processed: {}, errors: {}",
            self.id, state.messages_processed, state.write_errors
        );
        Ok(())
    }
}
