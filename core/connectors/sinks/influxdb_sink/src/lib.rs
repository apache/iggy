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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum InfluxDbSinkConfig {
    #[serde(rename = "v2")]
    V2(V2SinkConfig),
    #[serde(rename = "v3")]
    V3(V3SinkConfig),
}

impl InfluxDbSinkConfig {
    fn url(&self) -> &str {
        match self {
            Self::V2(c) => &c.url,
            Self::V3(c) => &c.url,
        }
    }

    fn auth_header(&self) -> String {
        match self {
            Self::V2(c) => format!("Token {}", c.token.expose_secret()),
            Self::V3(c) => format!("Bearer {}", c.token.expose_secret()),
        }
    }

    fn measurement(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.measurement.as_deref(),
            Self::V3(c) => c.measurement.as_deref(),
        }
    }

    fn precision(&self) -> &str {
        match self {
            Self::V2(c) => c.precision.as_deref().unwrap_or(DEFAULT_PRECISION),
            Self::V3(c) => c.precision.as_deref().unwrap_or(DEFAULT_PRECISION),
        }
    }

    fn batch_size(&self) -> u32 {
        match self {
            Self::V2(c) => c.batch_size.unwrap_or(500),
            Self::V3(c) => c.batch_size.unwrap_or(500),
        }
    }

    fn include_metadata(&self) -> bool {
        match self {
            Self::V2(c) => c.include_metadata.unwrap_or(true),
            Self::V3(c) => c.include_metadata.unwrap_or(true),
        }
    }

    fn include_checksum(&self) -> bool {
        match self {
            Self::V2(c) => c.include_checksum.unwrap_or(true),
            Self::V3(c) => c.include_checksum.unwrap_or(true),
        }
    }

    fn include_origin_timestamp(&self) -> bool {
        match self {
            Self::V2(c) => c.include_origin_timestamp.unwrap_or(true),
            Self::V3(c) => c.include_origin_timestamp.unwrap_or(true),
        }
    }

    fn include_stream_tag(&self) -> bool {
        match self {
            Self::V2(c) => c.include_stream_tag.unwrap_or(true),
            Self::V3(c) => c.include_stream_tag.unwrap_or(true),
        }
    }

    fn include_topic_tag(&self) -> bool {
        match self {
            Self::V2(c) => c.include_topic_tag.unwrap_or(true),
            Self::V3(c) => c.include_topic_tag.unwrap_or(true),
        }
    }

    fn include_partition_tag(&self) -> bool {
        match self {
            Self::V2(c) => c.include_partition_tag.unwrap_or(true),
            Self::V3(c) => c.include_partition_tag.unwrap_or(true),
        }
    }

    fn payload_format(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.payload_format.as_deref(),
            Self::V3(c) => c.payload_format.as_deref(),
        }
    }

    fn verbose_logging(&self) -> bool {
        match self {
            Self::V2(c) => c.verbose_logging.unwrap_or(false),
            Self::V3(c) => c.verbose_logging.unwrap_or(false),
        }
    }

    fn max_retries(&self) -> u32 {
        match self {
            Self::V2(c) => c.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            Self::V3(c) => c.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        }
    }

    fn retry_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.retry_delay.as_deref(),
            Self::V3(c) => c.retry_delay.as_deref(),
        }
    }

    fn timeout(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.timeout.as_deref(),
            Self::V3(c) => c.timeout.as_deref(),
        }
    }

    fn max_open_retries(&self) -> u32 {
        match self {
            Self::V2(c) => c.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
            Self::V3(c) => c.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
        }
    }

    fn open_retry_max_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.open_retry_max_delay.as_deref(),
            Self::V3(c) => c.open_retry_max_delay.as_deref(),
        }
    }

    fn retry_max_delay(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.retry_max_delay.as_deref(),
            Self::V3(c) => c.retry_max_delay.as_deref(),
        }
    }

    fn circuit_breaker_threshold(&self) -> u32 {
        match self {
            Self::V2(c) => c.circuit_breaker_threshold.unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
            Self::V3(c) => c.circuit_breaker_threshold.unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD),
        }
    }

    fn circuit_breaker_cool_down(&self) -> Option<&str> {
        match self {
            Self::V2(c) => c.circuit_breaker_cool_down.as_deref(),
            Self::V3(c) => c.circuit_breaker_cool_down.as_deref(),
        }
    }

    fn build_write_url(&self) -> Result<Url, Error> {
        let precision = self.precision();
        match self {
            Self::V2(c) => {
                let mut url =
                    Url::parse(&format!("{}/api/v2/write", c.url.trim_end_matches('/')))
                        .map_err(|e| Error::InvalidConfigValue(format!("Invalid URL: {e}")))?;
                url.query_pairs_mut()
                    .append_pair("org", &c.org)
                    .append_pair("bucket", &c.bucket)
                    .append_pair("precision", precision);
                Ok(url)
            }
            Self::V3(c) => {
                let v3_precision = map_precision_v3(precision);
                let mut url =
                    Url::parse(&format!("{}/api/v3/write_lp", c.url.trim_end_matches('/')))
                        .map_err(|e| Error::InvalidConfigValue(format!("Invalid URL: {e}")))?;
                url.query_pairs_mut()
                    .append_pair("db", &c.db)
                    .append_pair("precision", v3_precision);
                Ok(url)
            }
        }
    }

    fn build_health_url(&self) -> Result<Url, Error> {
        Url::parse(&format!("{}/health", self.url().trim_end_matches('/')))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid URL: {e}")))
    }

    fn version_label(&self) -> &'static str {
        match self {
            Self::V2(_) => "v2",
            Self::V3(_) => "v3",
        }
    }
}

fn map_precision_v3(p: &str) -> &'static str {
    match p {
        "ns" => "nanosecond",
        "ms" => "millisecond",
        "s" => "second",
        _ => "microsecond",
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

// ── Line-protocol escaping ────────────────────────────────────────────────────

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

// ── InfluxDbSink impl ─────────────────────────────────────────────────────────

impl InfluxDbSink {
    pub fn new(id: u32, config: InfluxDbSinkConfig) -> Self {
        let verbose = config.verbose_logging();
        let retry_delay = parse_duration(config.retry_delay(), DEFAULT_RETRY_DELAY);
        let payload_format = PayloadFormat::from_config(config.payload_format());
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_threshold(),
            parse_duration(config.circuit_breaker_cool_down(), DEFAULT_CIRCUIT_COOL_DOWN),
        ));
        let measurement = config
            .measurement()
            .unwrap_or("iggy_messages")
            .to_string();
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
            _ => micros,
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
            write!(buf, ",iggy_partition={}u", messages_metadata.partition_id as u64)
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

    async fn process_batch(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let mut body = String::with_capacity(messages.len() * 256);
        for (i, msg) in messages.iter().enumerate() {
            if i > 0 {
                body.push('\n');
            }
            self.append_line(&mut body, topic_metadata, messages_metadata, msg)?;
        }

        let client = self.get_client()?;
        let url = self.write_url.clone().ok_or_else(|| {
            Error::Connection("write_url not initialized — call open() first".to_string())
        })?;
        let auth = self.auth_header.as_deref().ok_or_else(|| {
            Error::Connection("auth_header not initialized — call open() first".to_string())
        })?;

        let response = client
            .post(url)
            .header("Authorization", auth)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Bytes::from(body))
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
            return Err(Error::CannotStoreData("Circuit breaker is open".to_string()));
        }

        let mut first_error: Option<Error> = None;

        for batch in messages.chunks(self.batch_size_limit) {
            match self
                .process_batch(topic_metadata, &messages_metadata, batch)
                .await
            {
                Ok(()) => {
                    self.write_success
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    if !matches!(e, Error::PermanentHttpError(_)) {
                        self.circuit_breaker.record_failure().await;
                    }
                    self.write_errors
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
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

        if first_error.is_none() {
            self.circuit_breaker.record_success();
        }

        let total_processed = self
            .messages_attempted
            .fetch_add(total as u64, Ordering::Relaxed)
            + total as u64;

        if self.verbose {
            info!(
                "InfluxDB sink ID: {} — processed={total}, cumulative={total_processed}, \
                 success={}, errors={}",
                self.id,
                self.write_success.load(Ordering::Relaxed),
                self.write_errors.load(Ordering::Relaxed),
            );
        } else {
            debug!(
                "InfluxDB sink ID: {} — processed={total}, cumulative={total_processed}, \
                 success={}, errors={}",
                self.id,
                self.write_success.load(Ordering::Relaxed),
                self.write_errors.load(Ordering::Relaxed),
            );
        }

        first_error.map_or(Ok(()), Err)
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        info!(
            "InfluxDB sink ID: {} closed — processed={}, success={}, errors={}",
            self.id,
            self.messages_attempted.load(Ordering::Relaxed),
            self.write_success.load(Ordering::Relaxed),
            self.write_errors.load(Ordering::Relaxed),
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

    #[test]
    fn map_precision_v3_all_values() {
        assert_eq!(map_precision_v3("ns"), "nanosecond");
        assert_eq!(map_precision_v3("ms"), "millisecond");
        assert_eq!(map_precision_v3("s"), "second");
        assert_eq!(map_precision_v3("us"), "microsecond");
        assert_eq!(map_precision_v3("xx"), "microsecond");
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

    #[test]
    fn precision_unknown_falls_back_to_us() {
        let config = InfluxDbSinkConfig::V2(V2SinkConfig {
            precision: Some("xx".to_string()),
            ..make_v2_config().into_v2().unwrap()
        });
        let sink = InfluxDbSink::new(1, config);
        assert_eq!(sink.to_precision_timestamp(999), 999);
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
        assert!(result.unwrap_err().to_string().to_lowercase().contains("json"));
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
        assert!(result.unwrap_err().to_string().to_uppercase().contains("UTF"));
    }

    #[test]
    fn append_line_valid_json_payload_succeeds() {
        let sink = make_sink();
        let mut buf = String::new();
        assert!(sink
            .append_line(
                &mut buf,
                &make_topic_metadata(),
                &make_messages_metadata(),
                &make_message(iggy_connector_sdk::Payload::Raw(b"{\"k\":1}".to_vec())),
            )
            .is_ok());
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
        assert!(sink
            .append_line(
                &mut buf,
                &make_topic_metadata(),
                &make_messages_metadata(),
                &make_message(iggy_connector_sdk::Payload::Raw(b"binary data".to_vec())),
            )
            .is_ok());
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
