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

mod protocol;

use crate::protocol::{write_field_string, write_measurement, write_tag_value};
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
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

sink_connector!(InfluxDbSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_PRECISION: &str = "us";
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

// ── Configuration ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2SinkConfig {
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
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3SinkConfig {
    pub url: String,
    pub db: String,
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
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "version")]
pub enum InfluxDbSinkConfig {
    #[serde(rename = "v2")]
    V2(V2SinkConfig),
    #[serde(rename = "v3")]
    V3(V3SinkConfig),
}

impl<'de> serde::Deserialize<'de> for InfluxDbSinkConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = serde_json::Value::deserialize(d)?;
        let version = raw.get("version").and_then(|v| v.as_str()).unwrap_or("v2");
        match version {
            "v2" => serde_json::from_value::<V2SinkConfig>(raw)
                .map(Self::V2)
                .map_err(serde::de::Error::custom),
            "v3" => serde_json::from_value::<V3SinkConfig>(raw)
                .map(Self::V3)
                .map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::custom(format!(
                "unknown InfluxDB version {other:?}; expected \"v2\" or \"v3\""
            ))),
        }
    }
}

/// Map a short precision string to InfluxDB 3's long-form equivalent.
///
/// InfluxDB 3 rejects the short forms (`"ns"`, `"us"`, `"ms"`, `"s"`) on the
/// `/api/v3/write_lp` endpoint and expects full English words. Returns an error
/// for unrecognised values rather than silently defaulting.
fn map_precision_v3(p: &str) -> Result<&'static str, Error> {
    match p {
        "ns" => Ok("nanosecond"),
        "us" => Ok("microsecond"),
        "ms" => Ok("millisecond"),
        "s" => Ok("second"),
        other => Err(Error::InvalidConfigValue(format!(
            "unknown precision {other:?}; valid values are \"ns\", \"us\", \"ms\", \"s\""
        ))),
    }
}

// Eliminates the repetitive "match self { V2(c) => …, V3(c) => … }" pattern for
// fields that are identical across all config variants. Methods with version-specific
// logic (auth_header, build_write_url, build_health_url, version_label) remain explicit.
macro_rules! delegate {
    // &T field reference  →  fn foo(&self) -> &T
    (ref $self:ident . $field:ident) => {
        match $self {
            Self::V2(c) => &c.$field,
            Self::V3(c) => &c.$field,
        }
    };
    // Option<String>  →  Option<&str>
    (opt $self:ident . $field:ident) => {
        match $self {
            Self::V2(c) => c.$field.as_deref(),
            Self::V3(c) => c.$field.as_deref(),
        }
    };
    // Option<String>  →  &str with fallback
    (str_or $self:ident . $field:ident, $default:expr) => {
        match $self {
            Self::V2(c) => c.$field.as_deref().unwrap_or($default),
            Self::V3(c) => c.$field.as_deref().unwrap_or($default),
        }
    };
    // Option<T: Copy>  →  T with fallback
    (unwrap $self:ident . $field:ident, $default:expr) => {
        match $self {
            Self::V2(c) => c.$field.unwrap_or($default),
            Self::V3(c) => c.$field.unwrap_or($default),
        }
    };
}

impl InfluxDbSinkConfig {
    fn url(&self) -> &str {
        delegate!(ref     self.url)
    }
    fn measurement(&self) -> Option<&str> {
        delegate!(opt     self.measurement)
    }
    fn precision(&self) -> &str {
        delegate!(str_or  self.precision, DEFAULT_PRECISION)
    }
    fn batch_size(&self) -> u32 {
        delegate!(unwrap  self.batch_size, 500)
    }
    fn include_metadata(&self) -> bool {
        delegate!(unwrap  self.include_metadata, true)
    }
    fn include_checksum(&self) -> bool {
        delegate!(unwrap  self.include_checksum, true)
    }
    fn include_origin_timestamp(&self) -> bool {
        delegate!(unwrap  self.include_origin_timestamp, true)
    }
    fn include_stream_tag(&self) -> bool {
        delegate!(unwrap  self.include_stream_tag, true)
    }
    fn include_topic_tag(&self) -> bool {
        delegate!(unwrap  self.include_topic_tag, true)
    }
    fn include_partition_tag(&self) -> bool {
        delegate!(unwrap  self.include_partition_tag, true)
    }
    fn payload_format(&self) -> Option<&str> {
        delegate!(opt     self.payload_format)
    }
    fn verbose_logging(&self) -> bool {
        delegate!(unwrap  self.verbose_logging, false)
    }
    fn max_retries(&self) -> u32 {
        delegate!(unwrap  self.max_retries, DEFAULT_MAX_RETRIES)
    }
    fn retry_delay(&self) -> Option<&str> {
        delegate!(opt     self.retry_delay)
    }
    fn timeout(&self) -> Option<&str> {
        delegate!(opt     self.timeout)
    }
    fn max_open_retries(&self) -> u32 {
        delegate!(unwrap  self.max_open_retries, DEFAULT_MAX_OPEN_RETRIES)
    }
    fn open_retry_max_delay(&self) -> Option<&str> {
        delegate!(opt   self.open_retry_max_delay)
    }
    fn retry_max_delay(&self) -> Option<&str> {
        delegate!(opt     self.retry_max_delay)
    }
    fn circuit_breaker_threshold(&self) -> u32 {
        delegate!(unwrap  self.circuit_breaker_threshold, DEFAULT_CIRCUIT_BREAKER_THRESHOLD)
    }
    fn circuit_breaker_cool_down(&self) -> Option<&str> {
        delegate!(opt self.circuit_breaker_cool_down)
    }

    fn auth_header(&self) -> String {
        match self {
            Self::V2(c) => format!("Token {}", c.token.expose_secret()),
            Self::V3(c) => format!("Bearer {}", c.token.expose_secret()),
        }
    }

    fn build_write_url(&self) -> Result<Url, Error> {
        let precision = self.precision();
        match self {
            Self::V2(c) => {
                let mut url = Url::parse(&format!("{}/api/v2/write", c.url.trim_end_matches('/')))
                    .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
                url.query_pairs_mut()
                    .append_pair("org", &c.org)
                    .append_pair("bucket", &c.bucket)
                    .append_pair("precision", precision);
                Ok(url)
            }
            Self::V3(c) => {
                let mut url =
                    Url::parse(&format!("{}/api/v3/write_lp", c.url.trim_end_matches('/')))
                        .map_err(|e| {
                            Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}"))
                        })?;
                url.query_pairs_mut()
                    .append_pair("db", &c.db)
                    .append_pair("precision", map_precision_v3(precision)?);
                Ok(url)
            }
        }
    }

    fn build_health_url(&self) -> Result<Url, Error> {
        let base = self.url().trim_end_matches('/');
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }

    fn version_label(&self) -> &'static str {
        match self {
            Self::V2(_) => "v2",
            Self::V3(_) => "v3",
        }
    }
}

// ── Sink struct ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct InfluxDbSink {
    id: u32,
    config: InfluxDbSinkConfig,
    client: Option<ClientWithMiddleware>,
    write_url: Option<Url>,
    auth_header: Option<String>,
    circuit_breaker: Arc<CircuitBreaker>,
    messages_attempted: AtomicU64,
    write_success: AtomicU64,
    write_errors: AtomicU64,
    verbose: bool,
    retry_delay: Duration,
    payload_format: PayloadFormat,
    measurement: String,
    precision: String,
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    include_stream_tag: bool,
    include_topic_tag: bool,
    include_partition_tag: bool,
    batch_size_limit: usize,
}

// ── PayloadFormat ─────────────────────────────────────────────────────────────

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
            Some("text") | Some("utf8") => Self::Text,
            Some("base64") | Some("raw") => Self::Base64,
            _ => Self::Json,
        }
    }
}

// ── InfluxDbSink impl ─────────────────────────────────────────────────────────

impl InfluxDbSink {
    pub fn new(id: u32, config: InfluxDbSinkConfig) -> Self {
        let verbose = config.verbose_logging();
        let retry_delay = parse_duration(config.retry_delay(), DEFAULT_RETRY_DELAY);
        let payload_format = PayloadFormat::from_config(config.payload_format());
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_threshold(),
            parse_duration(
                config.circuit_breaker_cool_down(),
                DEFAULT_CIRCUIT_COOL_DOWN,
            ),
        ));
        let measurement = config.measurement().unwrap_or("iggy_messages").to_string();
        let precision = config.precision().to_string();
        let include_metadata = config.include_metadata();
        let include_checksum = config.include_checksum();
        let include_origin_timestamp = config.include_origin_timestamp();
        let include_stream_tag = config.include_stream_tag();
        let include_topic_tag = config.include_topic_tag();
        let include_partition_tag = config.include_partition_tag();
        let batch_size_limit = config.batch_size().max(1) as usize;

        Self {
            id,
            config,
            client: None,
            write_url: None,
            auth_header: None,
            circuit_breaker,
            messages_attempted: AtomicU64::new(0),
            write_success: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
            verbose,
            retry_delay,
            payload_format,
            measurement,
            precision,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            include_stream_tag,
            include_topic_tag,
            include_partition_tag,
            batch_size_limit,
        }
    }

    fn build_raw_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_duration(self.config.timeout(), DEFAULT_TIMEOUT);
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client.as_ref().ok_or_else(|| {
            Error::Connection("InfluxDB client not initialized — call open() first".to_string())
        })
    }

    fn to_precision_timestamp(&self, micros: u64) -> u64 {
        match self.precision.as_str() {
            "ns" => micros.saturating_mul(1_000),
            "us" => micros,
            "ms" => micros / 1_000,
            "s" => micros / 1_000_000,
            _ => unreachable!("precision validated in open()"),
        }
    }

    fn append_line(
        &self,
        buf: &mut String,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &ConsumedMessage,
    ) -> Result<(), Error> {
        write_measurement(buf, &self.measurement);

        if self.include_metadata && self.include_stream_tag {
            buf.push_str(",stream=");
            write_tag_value(buf, &topic_metadata.stream);
        }
        if self.include_metadata && self.include_topic_tag {
            buf.push_str(",topic=");
            write_tag_value(buf, &topic_metadata.topic);
        }
        if self.include_metadata && self.include_partition_tag {
            write!(buf, ",partition={}", messages_metadata.partition_id).expect("infallible");
        }
        // `offset` is always written as a tag regardless of `include_metadata`.
        // It forms the deduplication key for idempotent writes: without it, two
        // messages at the same timestamp in the same measurement+tag-set would
        // silently overwrite each other in InfluxDB's last-write-wins model.
        write!(buf, ",offset={}", message.offset).expect("infallible");

        buf.push(' ');
        buf.push_str("message_id=\"");
        write_field_string(buf, &message.id.to_string());
        buf.push('"');

        if self.include_metadata && !self.include_stream_tag {
            buf.push_str(",iggy_stream=\"");
            write_field_string(buf, &topic_metadata.stream);
            buf.push('"');
        }
        if self.include_metadata && !self.include_topic_tag {
            buf.push_str(",iggy_topic=\"");
            write_field_string(buf, &topic_metadata.topic);
            buf.push('"');
        }
        if self.include_metadata && !self.include_partition_tag {
            write!(
                buf,
                ",iggy_partition={}u",
                messages_metadata.partition_id as u64
            )
            .expect("infallible");
        }
        if self.include_checksum {
            write!(buf, ",iggy_checksum={}u", message.checksum).expect("infallible");
        }
        if self.include_origin_timestamp {
            write!(buf, ",iggy_origin_timestamp={}u", message.origin_timestamp)
                .expect("infallible");
        }

        match self.payload_format {
            PayloadFormat::Json => {
                let compact = match &message.payload {
                    iggy_connector_sdk::Payload::Json(value) => simd_json::to_string(value)
                        .map_err(|e| {
                            Error::CannotStoreData(format!("JSON serialization failed: {e}"))
                        })?,
                    _ => {
                        let bytes = message.payload.try_to_bytes().map_err(|e| {
                            Error::CannotStoreData(format!("Payload conversion failed: {e}"))
                        })?;
                        let value: serde_json::Value =
                            serde_json::from_slice(&bytes).map_err(|e| {
                                Error::CannotStoreData(format!("Payload is not valid JSON: {e}"))
                            })?;
                        serde_json::to_string(&value).map_err(|e| {
                            Error::CannotStoreData(format!("JSON serialization failed: {e}"))
                        })?
                    }
                };
                buf.push_str(",payload_json=\"");
                write_field_string(buf, &compact);
                buf.push('"');
            }
            PayloadFormat::Text => {
                let bytes = message.payload.try_to_bytes().map_err(|e| {
                    Error::CannotStoreData(format!("Payload conversion failed: {e}"))
                })?;
                let text = String::from_utf8(bytes).map_err(|e| {
                    Error::CannotStoreData(format!("Payload is not valid UTF-8: {e}"))
                })?;
                buf.push_str(",payload_text=\"");
                write_field_string(buf, &text);
                buf.push('"');
            }
            PayloadFormat::Base64 => {
                let bytes = message.payload.try_to_bytes().map_err(|e| {
                    Error::CannotStoreData(format!("Payload conversion failed: {e}"))
                })?;
                let encoded = general_purpose::STANDARD.encode(&bytes);
                buf.push_str(",payload_base64=\"");
                write_field_string(buf, &encoded);
                buf.push('"');
            }
        }

        let base_micros = if message.timestamp == 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64
        } else {
            message.timestamp
        };
        let ts = self.to_precision_timestamp(base_micros);
        write!(buf, " {ts}").expect("infallible");

        debug!("sink ID: {} — offset={}, ts={ts}", self.id, message.offset);
        Ok(())
    }

    /// Build the newline-separated line-protocol body for a batch of messages.
    /// Pure function — no I/O; extracted for testability.
    fn build_body(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<String, Error> {
        // 1 KiB per message is a conservative estimate that accommodates JSON
        // payloads without excessive reallocation.
        let mut body = String::with_capacity(messages.len() * 1024);
        for (i, msg) in messages.iter().enumerate() {
            self.append_line(&mut body, topic_metadata, messages_metadata, msg)?;
            if i + 1 < messages.len() {
                body.push('\n');
            }
        }
        Ok(body)
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

        let body = self.build_body(topic_metadata, messages_metadata, messages)?;

        let client = self.get_client()?;
        let url = self.write_url.as_ref().ok_or_else(|| {
            Error::Connection("write_url not initialized — call open() first".to_string())
        })?;
        let auth = self.auth_header.as_deref().ok_or_else(|| {
            Error::Connection("auth_header not initialized — call open() first".to_string())
        })?;

        let response = client
            .post(url.as_str())
            .header("Authorization", auth)
            .header("Content-Type", "text/plain; charset=utf-8")
            // into_bytes() hands the Vec<u8> directly to Bytes without copying.
            .body(Bytes::from(body.into_bytes()))
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

        if iggy_connector_sdk::retry::is_transient_status(status) {
            Err(Error::CannotStoreData(format!(
                "InfluxDB write failed {status}: {body_text}"
            )))
        } else {
            Err(Error::PermanentHttpError(format!(
                "InfluxDB write failed {status}: {body_text}"
            )))
        }
    }
}

// ── Sink trait ────────────────────────────────────────────────────────────────

#[async_trait]
impl Sink for InfluxDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        const VALID_PRECISIONS: &[&str] = &["ns", "us", "ms", "s"];
        if !VALID_PRECISIONS.contains(&self.precision.as_str()) {
            return Err(Error::InvalidConfigValue(format!(
                "unknown precision {:?}; valid values are \"ns\", \"us\", \"ms\", \"s\"",
                self.precision
            )));
        }

        if let InfluxDbSinkConfig::V2(c) = &self.config
            && c.org.trim().is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "V2 sink config requires a non-empty 'org'".to_string(),
            ));
        }

        info!(
            "Opening InfluxDB sink ID: {} (version={})",
            self.id,
            self.config.version_label()
        );

        let raw_client = self.build_raw_client()?;
        check_connectivity_with_retry(
            &raw_client,
            self.config.build_health_url()?,
            "InfluxDB sink",
            self.id,
            &ConnectivityConfig {
                max_open_retries: self.config.max_open_retries(),
                open_retry_max_delay: parse_duration(
                    self.config.open_retry_max_delay(),
                    DEFAULT_OPEN_RETRY_MAX_DELAY,
                ),
                retry_delay: self.retry_delay,
            },
        )
        .await?;

        self.client = Some(build_retry_client(
            raw_client,
            self.config.max_retries().max(1),
            self.retry_delay,
            parse_duration(self.config.retry_max_delay(), DEFAULT_RETRY_MAX_DELAY),
            "InfluxDB",
        ));

        self.write_url = Some(self.config.build_write_url()?);
        self.auth_header = Some(self.config.auth_header());

        info!("InfluxDB sink ID: {} opened successfully", self.id);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let total = messages.len();

        if self.circuit_breaker.is_open().await {
            warn!(
                "InfluxDB sink ID: {} — circuit breaker OPEN, skipping {} messages",
                self.id, total
            );
            return Err(Error::CannotStoreData(
                "Circuit breaker is open".to_string(),
            ));
        }

        let mut first_error: Option<Error> = None;

        for batch in messages.chunks(self.batch_size_limit) {
            match self
                .process_batch(topic_metadata, &messages_metadata, batch)
                .await
            {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    self.write_success
                        .fetch_add(batch.len() as u64, Ordering::AcqRel);
                }
                Err(e) => {
                    if !matches!(e, Error::PermanentHttpError(_)) {
                        self.circuit_breaker.record_failure().await;
                    }
                    self.write_errors
                        .fetch_add(batch.len() as u64, Ordering::AcqRel);
                    error!(
                        "InfluxDB sink ID: {} failed batch of {}: {e}",
                        self.id,
                        batch.len()
                    );
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        let total_processed = self
            .messages_attempted
            .fetch_add(total as u64, Ordering::AcqRel)
            + total as u64;

        if self.verbose {
            info!(
                "InfluxDB sink ID: {} — processed={total}, cumulative={total_processed}, \
                 success={}, errors={}",
                self.id,
                self.write_success.load(Ordering::Acquire),
                self.write_errors.load(Ordering::Acquire),
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} — processed={total}, cumulative={total_processed}, \
                 success={}, errors={}",
                self.id,
                self.write_success.load(Ordering::Acquire),
                self.write_errors.load(Ordering::Acquire),
            );
        }

        first_error.map_or(Ok(()), Err)
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        info!(
            "InfluxDB sink ID: {} closed — processed={}, success={}, errors={}",
            self.id,
            self.messages_attempted.load(Ordering::Acquire),
            self.write_success.load(Ordering::Acquire),
            self.write_errors.load(Ordering::Acquire),
        );
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::{MessagesMetadata, Schema, TopicMetadata};

    fn make_v2_config() -> InfluxDbSinkConfig {
        InfluxDbSinkConfig::V2(V2SinkConfig {
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
        })
    }

    fn make_v3_config() -> InfluxDbSinkConfig {
        InfluxDbSinkConfig::V3(V3SinkConfig {
            url: "http://localhost:8181".to_string(),
            db: "test_db".to_string(),
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
        })
    }

    fn make_sink() -> InfluxDbSink {
        InfluxDbSink::new(1, make_v2_config())
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

    // ── config deserialization ────────────────────────────────────────────

    #[test]
    fn sink_config_without_version_defaults_to_v2() {
        let json = r#"{"url":"http://localhost:8086","org":"o","bucket":"b","token":"t"}"#;
        let raw: serde_json::Value = serde_json::from_str(json).unwrap();
        let config: InfluxDbSinkConfig = serde_json::from_value(raw).unwrap();
        assert!(matches!(config, InfluxDbSinkConfig::V2(_)));
    }

    #[test]
    fn sink_config_with_explicit_v2_version_deserializes_v2() {
        let json =
            r#"{"version":"v2","url":"http://localhost:8086","org":"o","bucket":"b","token":"t"}"#;
        let config: InfluxDbSinkConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, InfluxDbSinkConfig::V2(_)));
    }

    #[test]
    fn sink_config_with_version_v3_deserializes_v3() {
        let json = r#"{"version":"v3","url":"http://localhost:8181","db":"d","token":"t"}"#;
        let config: InfluxDbSinkConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, InfluxDbSinkConfig::V3(_)));
    }

    #[test]
    fn sink_config_unknown_version_returns_error() {
        let json = r#"{"version":"v9","url":"http://x","org":"o","bucket":"b","token":"t"}"#;
        let result = serde_json::from_str::<InfluxDbSinkConfig>(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown InfluxDB version")
        );
    }

    // ── config ────────────────────────────────────────────────────────────

    #[test]
    fn v2_auth_header_uses_token_scheme() {
        let config = make_v2_config();
        assert_eq!(config.auth_header(), "Token test_token");
    }

    #[test]
    fn v3_auth_header_uses_bearer_scheme() {
        let config = make_v3_config();
        assert_eq!(config.auth_header(), "Bearer test_token");
    }

    #[test]
    fn v2_write_url_contains_org_bucket_precision() {
        let config = make_v2_config();
        let url = config.build_write_url().unwrap();
        let q = url.query().unwrap_or("");
        assert!(url.path().ends_with("/api/v2/write"));
        assert!(q.contains("org=test_org"));
        assert!(q.contains("bucket=test_bucket"));
        assert!(q.contains("precision=us"));
    }

    #[test]
    fn v3_write_url_contains_db_and_mapped_precision() {
        let config = make_v3_config();
        let url = config.build_write_url().unwrap();
        let q = url.query().unwrap_or("");
        assert!(url.path().ends_with("/api/v3/write_lp"));
        assert!(q.contains("db=test_db"));
        assert!(q.contains("precision=microsecond"));
        assert!(!q.contains("org="));
        assert!(!q.contains("bucket="));
    }

    // ── to_precision_timestamp ────────────────────────────────────────────

    #[test]
    fn precision_ns_multiplies_by_1000() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            precision: Some("ns".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(1_000_000), 1_000_000_000);
    }

    #[test]
    fn precision_us_is_identity() {
        assert_eq!(make_sink().to_precision_timestamp(1_234_567), 1_234_567);
    }

    #[test]
    fn precision_ms_divides_by_1000() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            precision: Some("ms".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(5_000_000), 5_000);
    }

    #[test]
    fn precision_s_divides_by_1_000_000() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            precision: Some("s".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(7_000_000), 7);
    }

    #[tokio::test]
    async fn open_rejects_unknown_precision() {
        // Unknown precision must fail at open() rather than silently defaulting
        // to microseconds, which would timestamp data at the wrong precision.
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            url: "http://localhost:18086".to_string(),
            precision: Some("xx".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let mut sink = InfluxDbSink::new(1, config);
        let err = sink.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_empty_org_in_v2() {
        // An empty org generates `?org=` in the write URL, which InfluxDB V2
        // rejects at runtime with a 400. Catch it eagerly at open().
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            url: "http://localhost:18086".to_string(),
            org: "".to_string(),
            ..make_v2_config().into_v2().unwrap()
        });
        let mut sink = InfluxDbSink::new(1, config);
        let err = sink.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue for empty org, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_whitespace_only_org_in_v2() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            url: "http://localhost:18086".to_string(),
            org: "   ".to_string(),
            ..make_v2_config().into_v2().unwrap()
        });
        let mut sink = InfluxDbSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    // ── line-protocol escaping ────────────────────────────────────────────

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
        let mut buf = String::new();
        write_field_string(&mut buf, "line1\nline2\r");
        assert_eq!(buf, "line1\\nline2\\r");
    }

    // ── append_line ───────────────────────────────────────────────────────

    #[test]
    fn append_line_invalid_json_payload_returns_error() {
        let sink = make_sink();
        let mut buf = String::new();
        let result = sink.append_line(
            &mut buf,
            &make_topic_metadata(),
            &make_messages_metadata(),
            &make_message(iggy_connector_sdk::Payload::Raw(b"not json!".to_vec())),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .to_lowercase()
                .contains("json")
        );
    }

    #[test]
    fn append_line_invalid_utf8_text_payload_returns_error() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            payload_format: Some("text".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        let mut buf = String::new();
        let result = sink.append_line(
            &mut buf,
            &make_topic_metadata(),
            &make_messages_metadata(),
            &make_message(iggy_connector_sdk::Payload::Raw(vec![0xff, 0xfe, 0xfd])),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .to_uppercase()
                .contains("UTF")
        );
    }

    #[test]
    fn append_line_valid_json_payload_succeeds() {
        let sink = make_sink();
        let mut buf = String::new();
        assert!(
            sink.append_line(
                &mut buf,
                &make_topic_metadata(),
                &make_messages_metadata(),
                &make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec())),
            )
            .is_ok()
        );
        assert!(buf.contains("payload_json="));
    }

    #[test]
    fn append_line_base64_payload_succeeds() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            payload_format: Some("base64".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        let mut buf = String::new();
        assert!(
            sink.append_line(
                &mut buf,
                &make_topic_metadata(),
                &make_messages_metadata(),
                &make_message(iggy_connector_sdk::Payload::Raw(b"binary data".to_vec())),
            )
            .is_ok()
        );
        assert!(buf.contains("payload_base64="));
    }

    #[test]
    fn append_line_offset_tag_always_present() {
        let sink = make_sink();
        let mut buf = String::new();
        sink.append_line(
            &mut buf,
            &make_topic_metadata(),
            &make_messages_metadata(),
            &make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec())),
        )
        .unwrap();
        assert!(buf.contains(",offset=7"));
    }

    #[test]
    fn append_line_includes_measurement_name() {
        let sink = make_sink();
        let mut buf = String::new();
        sink.append_line(
            &mut buf,
            &make_topic_metadata(),
            &make_messages_metadata(),
            &make_message(iggy_connector_sdk::Payload::Raw(b"{\"x\":1}".to_vec())),
        )
        .unwrap();
        assert!(buf.starts_with("test_measurement"));
    }

    // ── V3 append_line parity ─────────────────────────────────────────────

    #[test]
    fn v3_append_line_produces_same_line_protocol_as_v2() {
        // Only URL, auth header, and write endpoint differ between V2 and V3.
        // The line-protocol body itself must be identical so existing tests
        // cover both configs implicitly.
        let v2_sink = InfluxDbSink::new(1, make_v2_config());
        let v3_sink = InfluxDbSink::new(2, make_v3_config());
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec()));
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let mut v2_buf = String::new();
        let mut v3_buf = String::new();
        v2_sink
            .append_line(&mut v2_buf, &topic, &meta, &msg)
            .unwrap();
        v3_sink
            .append_line(&mut v3_buf, &topic, &meta, &msg)
            .unwrap();

        assert_eq!(v2_buf, v3_buf);
    }

    // ── build_body batching logic ─────────────────────────────────────────

    #[test]
    fn build_body_empty_messages_returns_empty_string() {
        let sink = make_sink();
        let body = sink
            .build_body(&make_topic_metadata(), &make_messages_metadata(), &[])
            .unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn build_body_single_message_no_leading_or_trailing_newline() {
        let sink = make_sink();
        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec()));
        let body = sink
            .build_body(&make_topic_metadata(), &make_messages_metadata(), &[msg])
            .unwrap();
        assert!(!body.is_empty());
        assert!(!body.starts_with('\n'));
        assert!(!body.ends_with('\n'));
        assert_eq!(body.lines().count(), 1);
    }

    #[test]
    fn build_body_multiple_messages_newline_separated() {
        let sink = make_sink();
        let msgs: Vec<_> = (0..3)
            .map(|_| make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec())))
            .collect();
        let body = sink
            .build_body(&make_topic_metadata(), &make_messages_metadata(), &msgs)
            .unwrap();
        // 3 records → 2 separating newlines
        assert_eq!(body.lines().count(), 3);
        assert_eq!(body.chars().filter(|&c| c == '\n').count(), 2);
    }

    #[test]
    fn build_body_batch_size_one_produces_single_line() {
        // When batch_size=1, consume() calls process_batch with a single-element
        // slice. Verify that build_body returns exactly one line (no newlines).
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            batch_size: Some(1),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.batch_size_limit, 1);

        let msg = make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec()));
        let body = sink
            .build_body(&make_topic_metadata(), &make_messages_metadata(), &[msg])
            .unwrap();
        assert_eq!(body.lines().count(), 1);
        assert!(!body.contains('\n'));
    }

    #[test]
    fn build_body_exactly_batch_size_limit_messages() {
        // Edge case: exactly batch_size messages in one call.
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            batch_size: Some(3),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        let msgs: Vec<_> = (0..3)
            .map(|_| make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec())))
            .collect();
        let body = sink
            .build_body(&make_topic_metadata(), &make_messages_metadata(), &msgs)
            .unwrap();
        assert_eq!(body.lines().count(), 3);
    }
}

// ── Helper for tests: destructure config variants ─────────────────────────────

impl InfluxDbSinkConfig {
    #[cfg(test)]
    fn into_v2(self) -> Option<V2SinkConfig> {
        match self {
            Self::V2(c) => Some(c),
            Self::V3(_) => None,
        }
    }
}

// ── HTTP integration tests ────────────────────────────────────────────────────

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::Router;
    use axum::extract::Request;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use iggy_connector_sdk::{MessagesMetadata, Schema, Sink, TopicMetadata};
    use secrecy::SecretString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Mutex;

    // ── test helpers ─────────────────────────────────────────────────────────

    async fn start_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://127.0.0.1:{port}")
    }

    /// Minimal V2 config that points at `url`, has batch_size=2,
    /// and uses 1-retry / fast timeouts to keep tests quick.
    fn v2_config(url: &str) -> InfluxDbSinkConfig {
        InfluxDbSinkConfig::V2(V2SinkConfig {
            url: url.to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token: SecretString::from("tok"),
            measurement: Some("m".to_string()),
            precision: Some("us".to_string()),
            batch_size: Some(2),
            include_metadata: Some(false),
            include_checksum: Some(false),
            include_origin_timestamp: Some(false),
            include_stream_tag: Some(false),
            include_topic_tag: Some(false),
            include_partition_tag: Some(false),
            payload_format: Some("json".to_string()),
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: Some("1ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("10ms".to_string()),
            retry_max_delay: Some("10ms".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
        })
    }

    fn v3_config(url: &str) -> InfluxDbSinkConfig {
        InfluxDbSinkConfig::V3(V3SinkConfig {
            url: url.to_string(),
            db: "db".to_string(),
            token: SecretString::from("tok"),
            measurement: Some("m".to_string()),
            precision: Some("us".to_string()),
            batch_size: Some(2),
            include_metadata: Some(false),
            include_checksum: Some(false),
            include_origin_timestamp: Some(false),
            include_stream_tag: Some(false),
            include_topic_tag: Some(false),
            include_partition_tag: Some(false),
            payload_format: Some("json".to_string()),
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: Some("1ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("10ms".to_string()),
            retry_max_delay: Some("10ms".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
        })
    }

    /// Build a mock app that responds 200 to GET /health and `write_status` to
    /// POST on the V2 write endpoint.
    fn v2_app(write_status: StatusCode) -> Router {
        Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route("/api/v2/write", post(move || async move { write_status }))
    }

    fn v3_app(write_status: StatusCode) -> Router {
        Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v3/write_lp",
                post(move || async move { write_status }),
            )
    }

    async fn open_sink(config: InfluxDbSinkConfig) -> InfluxDbSink {
        let mut sink = InfluxDbSink::new(1, config);
        sink.open()
            .await
            .expect("open() should succeed against mock");
        sink
    }

    fn topic() -> TopicMetadata {
        TopicMetadata {
            stream: "s".to_string(),
            topic: "t".to_string(),
        }
    }

    fn meta() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: Schema::Json,
        }
    }

    fn msg() -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 1_000_000,
            origin_timestamp: 1_000_000,
            headers: None,
            payload: iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec()),
        }
    }

    // ── open() ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_v2_succeeds_when_health_returns_200() {
        let base = start_server(v2_app(StatusCode::NO_CONTENT)).await;
        let mut sink = InfluxDbSink::new(1, v2_config(&base));
        assert!(sink.open().await.is_ok());
    }

    #[tokio::test]
    async fn open_v3_succeeds_when_health_returns_200() {
        let base = start_server(v3_app(StatusCode::NO_CONTENT)).await;
        let mut sink = InfluxDbSink::new(1, v3_config(&base));
        assert!(sink.open().await.is_ok());
    }

    #[tokio::test]
    async fn open_fails_when_health_returns_503() {
        let app = Router::new().route("/health", get(|| async { StatusCode::SERVICE_UNAVAILABLE }));
        let base = start_server(app).await;
        let mut sink = InfluxDbSink::new(1, v2_config(&base));
        assert!(sink.open().await.is_err());
    }

    // ── process_batch() ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn process_batch_204_returns_ok() {
        let base = start_server(v2_app(StatusCode::NO_CONTENT)).await;
        let sink = open_sink(v2_config(&base)).await;
        assert!(
            sink.process_batch(&topic(), &meta(), &[msg()])
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn process_batch_v3_204_returns_ok() {
        let base = start_server(v3_app(StatusCode::NO_CONTENT)).await;
        let sink = open_sink(v3_config(&base)).await;
        assert!(
            sink.process_batch(&topic(), &meta(), &[msg()])
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn process_batch_500_returns_can_not_store_data_error() {
        let base = start_server(v2_app(StatusCode::INTERNAL_SERVER_ERROR)).await;
        let sink = open_sink(v2_config(&base)).await;
        let err = sink
            .process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::CannotStoreData(_)));
    }

    #[tokio::test]
    async fn process_batch_400_returns_permanent_http_error() {
        let base = start_server(v2_app(StatusCode::BAD_REQUEST)).await;
        let sink = open_sink(v2_config(&base)).await;
        let err = sink
            .process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PermanentHttpError(_)));
    }

    #[tokio::test]
    async fn process_batch_sends_token_authorization_header() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move |headers: HeaderMap| {
                    let cap = cap2.clone();
                    async move {
                        *cap.lock().await = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();
        assert_eq!(*captured.lock().await, "Token tok");
    }

    #[tokio::test]
    async fn process_batch_v3_sends_bearer_authorization_header() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v3/write_lp",
                post(move |headers: HeaderMap| {
                    let cap = cap2.clone();
                    async move {
                        *cap.lock().await = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v3_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();
        assert_eq!(*captured.lock().await, "Bearer tok");
    }

    #[tokio::test]
    async fn process_batch_sends_line_protocol_content_type() {
        let captured: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cap2 = captured.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move |headers: HeaderMap| {
                    let cap = cap2.clone();
                    async move {
                        *cap.lock().await = headers
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();
        assert!(captured.lock().await.starts_with("text/plain"));
    }

    #[tokio::test]
    async fn process_batch_body_is_valid_line_protocol() {
        let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let cap2 = captured.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move |request: Request| {
                    let cap = cap2.clone();
                    async move {
                        let b = axum::body::to_bytes(request.into_body(), usize::MAX)
                            .await
                            .unwrap();
                        *cap.lock().await = b.to_vec();
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();

        let body = String::from_utf8(captured.lock().await.clone()).unwrap();
        // measurement name is "m" from config
        assert!(body.starts_with("m,"), "expected measurement tag: {body}");
        // offset tag is always written
        assert!(body.contains(",offset=0"), "expected offset tag: {body}");
        // JSON payload field
        assert!(
            body.contains("payload_json="),
            "expected payload field: {body}"
        );
        // ends with a timestamp
        let last_token = body.split_whitespace().last().unwrap();
        assert!(
            last_token.parse::<u64>().is_ok(),
            "expected numeric ts: {body}"
        );
    }

    // ── consume() chunking ───────────────────────────────────────────────────

    #[tokio::test]
    async fn consume_chunks_into_batches_of_batch_size() {
        // batch_size=2 with 5 messages → 3 HTTP calls: (2, 2, 1)
        let call_count = Arc::new(AtomicU32::new(0));
        let cc2 = call_count.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move || {
                    let cc = cc2.clone();
                    async move {
                        cc.fetch_add(1, Ordering::AcqRel);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        let msgs: Vec<_> = (0..5).map(|_| msg()).collect();
        sink.consume(&topic(), meta(), msgs).await.unwrap();
        assert_eq!(call_count.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn consume_single_message_batch_size_one_makes_one_call() {
        let call_count = Arc::new(AtomicU32::new(0));
        let cc2 = call_count.clone();
        // Override batch_size to 1
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            batch_size: Some(1),
            ..v2_config("placeholder").into_v2().unwrap()
        });
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move || {
                    let cc = cc2.clone();
                    async move {
                        cc.fetch_add(1, Ordering::AcqRel);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        // Patch the url in after server started
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            url: base.clone(),
            ..config.into_v2().unwrap()
        });
        let sink = open_sink(config).await;
        sink.consume(&topic(), meta(), vec![msg()]).await.unwrap();
        assert_eq!(call_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn consume_returns_first_error_after_all_batches_attempt() {
        // First batch fails (500), second batch succeeds.
        // consume() should return an error but still attempt the second batch.
        let call_count = Arc::new(AtomicU32::new(0));
        let cc2 = call_count.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move || {
                    let cc = cc2.clone();
                    async move {
                        let n = cc.fetch_add(1, Ordering::AcqRel);
                        if n == 0 {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::NO_CONTENT
                        }
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        // 4 messages, batch_size=2 → 2 batches; first fails, second succeeds
        let msgs: Vec<_> = (0..4).map(|_| msg()).collect();
        let result = sink.consume(&topic(), meta(), msgs).await;
        assert!(result.is_err()); // error from the first batch is returned
        assert_eq!(call_count.load(Ordering::Acquire), 2); // both batches were attempted
    }

    #[tokio::test]
    async fn consume_records_success_per_successful_batch() {
        // With 2 batches where the first fails and the second succeeds, the circuit
        // breaker must record 1 failure AND 1 success — not 1 failure and 0 successes.
        // If only failures are recorded, the breaker will trip after enough intermittent
        // errors even when most batches succeed.
        let call_count = Arc::new(AtomicU32::new(0));
        let cc2 = call_count.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move || {
                    let cc = cc2.clone();
                    async move {
                        let n = cc.fetch_add(1, Ordering::AcqRel);
                        if n == 0 {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::NO_CONTENT
                        }
                    }
                }),
            );
        let base = start_server(app).await;
        // Use a threshold of 2 so the breaker trips after 2 failures.
        // If the success of batch 2 is not recorded, successive calls that have
        // 1 failure each would trip the breaker after 2 invocations.
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            circuit_breaker_threshold: Some(2),
            circuit_breaker_cool_down: Some("60s".to_string()),
            ..v2_config(&base).into_v2().unwrap()
        });
        let sink = open_sink(config).await;
        let msgs: Vec<_> = (0..4).map(|_| msg()).collect();
        let _ = sink.consume(&topic(), meta(), msgs).await; // first fails, second succeeds
        // Circuit breaker should NOT be open: 1 failure + 1 success → not tripped.
        assert!(
            !sink.circuit_breaker.is_open().await,
            "circuit breaker must not trip when at least one batch succeeded"
        );
    }

    // ── write URL routing ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn v2_writes_to_api_v2_write_endpoint() {
        let hit = Arc::new(AtomicU32::new(0));
        let hit2 = hit.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v2/write",
                post(move || {
                    let h = hit2.clone();
                    async move {
                        h.fetch_add(1, Ordering::AcqRel);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v2_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();
        assert_eq!(hit.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn v3_writes_to_api_v3_write_lp_endpoint() {
        let hit = Arc::new(AtomicU32::new(0));
        let hit2 = hit.clone();
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route(
                "/api/v3/write_lp",
                post(move || {
                    let h = hit2.clone();
                    async move {
                        h.fetch_add(1, Ordering::AcqRel);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let base = start_server(app).await;
        let sink = open_sink(v3_config(&base)).await;
        sink.process_batch(&topic(), &meta(), &[msg()])
            .await
            .unwrap();
        assert_eq!(hit.load(Ordering::Acquire), 1);
    }

    // ── map_precision_v3 ──────────────────────────────────────────────────

    #[test]
    fn map_precision_v3_maps_all_short_forms() {
        assert_eq!(map_precision_v3("ns").unwrap(), "nanosecond");
        assert_eq!(map_precision_v3("us").unwrap(), "microsecond");
        assert_eq!(map_precision_v3("ms").unwrap(), "millisecond");
        assert_eq!(map_precision_v3("s").unwrap(), "second");
    }

    #[test]
    fn map_precision_v3_rejects_unknown_values() {
        assert!(map_precision_v3("xx").is_err());
        assert!(map_precision_v3("").is_err());
        assert!(map_precision_v3("nanosecond").is_err());
    }

    #[test]
    fn v3_write_url_invalid_base_returns_error() {
        let config = InfluxDbSinkConfig::V3(V3SinkConfig {
            url: "not-a-url".to_string(),
            ..v3_config("http://placeholder").into_v3().unwrap()
        });
        assert!(config.build_write_url().is_err());
    }

    #[test]
    fn v2_write_url_invalid_base_returns_error() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            url: "not-a-url".to_string(),
            ..v2_config("http://placeholder").into_v2().unwrap()
        });
        assert!(config.build_write_url().is_err());
    }

    impl InfluxDbSinkConfig {
        fn into_v3(self) -> Option<V3SinkConfig> {
            match self {
                Self::V3(c) => Some(c),
                Self::V2(_) => None,
            }
        }
    }
}
