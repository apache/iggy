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

use std::borrow::Cow;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::retry::{RetryPolicy, retry_async};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, Pipeline, RedisError, RedisResult};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize, de::IgnoredAny};
use thiserror::Error as ThisError;
use tracing::{debug, error, info, warn};

sink_connector!(RedisSink);

const CONNECTOR_NAME: &str = "Redis sink";
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const DUPLICATE_ENTRY_DETAIL: &str = "equal or smaller than the target stream top item";
const HEADER_ENCODING_BASE64: &str = "base64";

// `Deserialize` only: nothing re-serializes a plugin config, and leaving
// `Serialize` off keeps the URL, which may carry a password, unserializable.
#[derive(Debug, Clone, Deserialize)]
pub struct RedisSinkConfig {
    #[serde(alias = "url")]
    pub connection_url: SecretString,
    pub mode: Option<RedisMode>,
    pub stream_key: String,
    pub batch_size: Option<u32>,
    pub payload_format: Option<String>,
    pub include_metadata: Option<bool>,
    pub deterministic_ids: Option<bool>,
    pub max_len: Option<u64>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub response_timeout: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RedisMode {
    #[default]
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Binary,
    Text,
    Json,
}

impl From<Option<&str>> for PayloadFormat {
    fn from(value: Option<&str>) -> Self {
        match value.map(str::to_ascii_lowercase).as_deref() {
            None | Some("binary") => PayloadFormat::Binary,
            Some("text") => PayloadFormat::Text,
            Some("json") => PayloadFormat::Json,
            Some(unknown) => {
                warn!("Unknown {CONNECTOR_NAME} payload_format '{unknown}', defaulting to binary");
                PayloadFormat::Binary
            }
        }
    }
}

#[derive(Debug, ThisError)]
enum EntryError {
    #[error("failed to serialize JSON payload: {0}")]
    JsonPayload(Error),
    #[error("payload is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("payload is not valid JSON: {0}")]
    InvalidJson(serde_json::Error),
    #[error("failed to encode headers: {0}")]
    Headers(serde_json::Error),
}

#[derive(Serialize)]
struct EncodedHeader {
    data: String,
    iggy_header_encoding: &'static str,
}

struct RedisEntry<'a> {
    message: &'a ConsumedMessage,
    payload: &'a [u8],
    headers: Option<&'a [u8]>,
}

#[derive(Debug)]
pub struct RedisSink {
    pub id: u32,
    connection: Option<ConnectionManager>,
    config: RedisSinkConfig,
    verbose: bool,
    batch_size: usize,
    payload_format: PayloadFormat,
    include_metadata: bool,
    deterministic_ids: bool,
    retry_policy: RetryPolicy,
    response_timeout: Duration,
    messages_processed: AtomicU64,
    duplicates_skipped: AtomicU64,
    errors: AtomicU64,
}

impl RedisSink {
    pub fn new(id: u32, config: RedisSinkConfig) -> Self {
        let retry_delay = parse_duration(
            config.retry_delay.as_deref(),
            "retry_delay",
            DEFAULT_RETRY_DELAY,
            id,
        );
        let response_timeout = parse_duration(
            config.response_timeout.as_deref(),
            "response_timeout",
            DEFAULT_RESPONSE_TIMEOUT,
            id,
        );
        RedisSink {
            id,
            connection: None,
            verbose: config.verbose_logging.unwrap_or(false),
            batch_size: config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize,
            payload_format: PayloadFormat::from(config.payload_format.as_deref()),
            include_metadata: config.include_metadata.unwrap_or(true),
            deterministic_ids: config.deterministic_ids.unwrap_or(true),
            retry_policy: RetryPolicy {
                max_attempts: config
                    .max_retries
                    .unwrap_or(DEFAULT_MAX_RETRIES)
                    .clamp(1, DEFAULT_MAX_RETRIES),
                base_delay: retry_delay,
                max_delay: MAX_RETRY_DELAY.max(retry_delay),
            },
            response_timeout,
            config,
            messages_processed: AtomicU64::new(0),
            duplicates_skipped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Sink for RedisSink {
    async fn open(&mut self) -> Result<(), Error> {
        let client = Client::open(self.config.connection_url.expose_secret())
            .map_err(|error| Error::InitError(format!("invalid Redis URL: {error}")))?;
        let manager_config =
            ConnectionManagerConfig::new().set_response_timeout(Some(self.response_timeout));
        let mut connection = ConnectionManager::new_with_config(client, manager_config)
            .await
            .map_err(|error| Error::InitError(format!("Redis connection failed: {error}")))?;
        redis::cmd("PING")
            .query_async::<String>(&mut connection)
            .await
            .map_err(|error| Error::InitError(format!("Redis PING failed: {error}")))?;
        self.connection = Some(connection);
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, stream_key: {}",
            self.id, self.config.stream_key
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(connection) = self.connection.as_ref() else {
            return Err(Error::InitError("Redis connection not initialized".into()));
        };

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages from stream: {}, topic: {}, partition_id: {}, current_offset: {}",
                self.id,
                messages.len(),
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages",
                self.id,
                messages.len()
            );
        }

        let stream_key = self.target_key(topic_metadata, messages_metadata.partition_id);
        for batch in messages.chunks(self.batch_size) {
            if let Err(error) = self
                .write_batch(
                    connection,
                    &stream_key,
                    topic_metadata,
                    &messages_metadata,
                    batch,
                )
                .await
            {
                error!(
                    "{CONNECTOR_NAME} ID: {} failed to write batch to {stream_key}: {error}",
                    self.id
                );
                return Err(error);
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.connection.take();
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, duplicates skipped: {}, errors: {}",
            self.id,
            self.messages_processed.load(Ordering::Relaxed),
            self.duplicates_skipped.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

impl RedisSink {
    // Explicit entry IDs must grow monotonically per key, and offsets only do
    // within one partition, so each (stream, topic, partition) gets its own key.
    fn target_key(&self, topic_metadata: &TopicMetadata, partition_id: u32) -> String {
        if self.deterministic_ids {
            format!(
                "{}:{}:{}:{partition_id}",
                self.config.stream_key, topic_metadata.stream, topic_metadata.topic
            )
        } else {
            self.config.stream_key.clone()
        }
    }

    async fn write_batch(
        &self,
        connection: &ConnectionManager,
        stream_key: &str,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        batch: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let mut pipeline = redis::pipe();
        pipeline.ignore_errors();
        let mut queued = 0u64;
        let mut invalid = 0u64;
        for message in batch {
            let (payload, headers) = match self
                .payload_bytes(message)
                .and_then(|payload| Ok((payload, self.headers_bytes(message)?)))
            {
                Ok(encoded) => encoded,
                Err(reason) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} skipping message offset: {}: {reason}",
                        self.id, message.offset
                    );
                    invalid += 1;
                    continue;
                }
            };
            self.queue_entry(
                &mut pipeline,
                stream_key,
                topic_metadata,
                messages_metadata,
                RedisEntry {
                    message,
                    payload: &payload,
                    headers: headers.as_deref(),
                },
            );
            queued += 1;
        }
        if invalid > 0 {
            self.errors.fetch_add(invalid, Ordering::Relaxed);
        }
        if queued == 0 {
            return Ok(());
        }

        let context = format!("{CONNECTOR_NAME} ID: {} XADD batch", self.id);
        let results = retry_async(self.retry_policy, &context, is_transient_error, || {
            let mut connection = connection.clone();
            let pipeline = &pipeline;
            async move {
                pipeline
                    .query_async::<Vec<RedisResult<String>>>(&mut connection)
                    .await
            }
        })
        .await
        .map_err(|failure| {
            self.errors.fetch_add(queued, Ordering::Relaxed);
            map_redis_error("XADD", failure.error)
        })?;

        let mut written = 0u64;
        let mut duplicates = 0u64;
        let mut failed = 0u64;
        let mut first_failure: Option<RedisError> = None;
        for result in results {
            match result {
                Ok(_) => written += 1,
                Err(error) if is_duplicate_entry(&error) => duplicates += 1,
                Err(error) => {
                    failed += 1;
                    first_failure.get_or_insert(error);
                }
            }
        }
        self.messages_processed
            .fetch_add(written, Ordering::Relaxed);
        self.duplicates_skipped
            .fetch_add(duplicates, Ordering::Relaxed);
        if duplicates > 0 {
            // A retry after a lost response is the expected cause. Offsets
            // restarting below the stream's last ID (for example a recreated
            // topic) skip entries the same way, so this must stay visible.
            warn!(
                "{CONNECTOR_NAME} ID: {} skipped {duplicates} entries already present in {stream_key}",
                self.id
            );
        }
        self.errors.fetch_add(failed, Ordering::Relaxed);

        match first_failure {
            Some(error) => Err(map_redis_error(
                &format!("{failed} of {queued} XADD entries rejected"),
                error,
            )),
            None => Ok(()),
        }
    }

    fn queue_entry(
        &self,
        pipeline: &mut Pipeline,
        stream_key: &str,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        entry: RedisEntry<'_>,
    ) {
        let message = entry.message;
        let command = pipeline.cmd("XADD").arg(stream_key);
        if let Some(max_len) = self.config.max_len {
            command.arg("MAXLEN").arg("~").arg(max_len);
        }
        if self.deterministic_ids {
            command.arg(entry_id(message.offset));
        } else {
            command.arg("*");
        }
        command.arg("payload").arg(entry.payload);
        if self.include_metadata {
            command
                .arg("message_id")
                .arg(message.id.to_string())
                .arg("offset")
                .arg(message.offset)
                .arg("partition_id")
                .arg(messages_metadata.partition_id)
                .arg("stream")
                .arg(&topic_metadata.stream)
                .arg("topic")
                .arg(&topic_metadata.topic)
                .arg("timestamp")
                .arg(message.timestamp)
                .arg("origin_timestamp")
                .arg(message.origin_timestamp)
                .arg("checksum")
                .arg(message.checksum);
            if let Some(headers) = entry.headers {
                command.arg("headers").arg(headers);
            }
        }
    }

    fn payload_bytes<'a>(&self, message: &'a ConsumedMessage) -> Result<Cow<'a, [u8]>, EntryError> {
        let bytes: Cow<'a, [u8]> = match &message.payload {
            Payload::Json(_) => Cow::Owned(
                message
                    .payload
                    .try_to_bytes()
                    .map_err(EntryError::JsonPayload)?,
            ),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                Cow::Borrowed(bytes)
            }
            Payload::Text(text) | Payload::Proto(text) => Cow::Borrowed(text.as_bytes()),
        };
        match self.payload_format {
            PayloadFormat::Binary => {}
            PayloadFormat::Text => {
                std::str::from_utf8(&bytes)?;
            }
            PayloadFormat::Json if !matches!(message.payload, Payload::Json(_)) => {
                serde_json::from_slice::<IgnoredAny>(&bytes).map_err(EntryError::InvalidJson)?;
            }
            PayloadFormat::Json => {}
        }
        Ok(bytes)
    }

    fn headers_bytes(&self, message: &ConsumedMessage) -> Result<Option<Vec<u8>>, EntryError> {
        if !self.include_metadata {
            return Ok(None);
        }
        let Some(headers) = message
            .headers
            .as_ref()
            .filter(|headers| !headers.is_empty())
        else {
            return Ok(None);
        };

        let encoded = headers
            .iter()
            .map(|(key, value)| {
                let value = if let Ok(raw) = value.as_raw() {
                    serde_json::to_value(EncodedHeader {
                        data: general_purpose::STANDARD.encode(raw),
                        iggy_header_encoding: HEADER_ENCODING_BASE64,
                    })
                    .map_err(EntryError::Headers)?
                } else {
                    serde_json::Value::String(value.to_string_value())
                };
                Ok((key.to_string_value(), value))
            })
            .collect::<Result<serde_json::Map<String, serde_json::Value>, EntryError>>()?;

        serde_json::to_vec(&encoded)
            .map(Some)
            .map_err(EntryError::Headers)
    }
}

fn parse_duration(raw: Option<&str>, field: &str, default: Duration, id: u32) -> Duration {
    let Some(raw) = raw else {
        return default;
    };
    match HumanDuration::from_str(raw) {
        Ok(duration) => *duration,
        Err(_) => {
            warn!(
                "Invalid {field} '{raw}' for {CONNECTOR_NAME} ID: {id}, defaulting to {default:?}"
            );
            default
        }
    }
}

// Redis rejects the all-zero ID, so the offset is shifted by one.
fn entry_id(offset: u64) -> String {
    format!("{}-0", offset.saturating_add(1))
}

fn is_duplicate_entry(error: &RedisError) -> bool {
    error
        .detail()
        .is_some_and(|detail| detail.contains(DUPLICATE_ENTRY_DETAIL))
}

fn is_transient_error(error: &RedisError) -> bool {
    error.is_io_error()
        || error.is_timeout()
        || error.is_connection_dropped()
        || error.is_connection_refusal()
}

fn map_redis_error(operation: &str, error: RedisError) -> Error {
    if is_transient_error(&error) {
        Error::Connection(format!("{operation} failed: {error}"))
    } else {
        Error::PermanentHttpError(format!("{operation} failed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{HeaderKey, HeaderValue};
    use std::collections::BTreeMap;

    fn given_default_config() -> RedisSinkConfig {
        RedisSinkConfig {
            connection_url: SecretString::from("redis://localhost:6379"),
            mode: Some(RedisMode::Stream),
            stream_key: "iggy:events".to_string(),
            batch_size: None,
            payload_format: None,
            include_metadata: None,
            deterministic_ids: None,
            max_len: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
            response_timeout: None,
        }
    }

    fn given_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 1000,
            origin_timestamp: 1000,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_offsets_should_build_nonzero_monotonic_entry_ids() {
        assert_eq!(entry_id(0), "1-0");
        assert_eq!(entry_id(41), "42-0");
        assert_eq!(entry_id(u64::MAX), format!("{}-0", u64::MAX));
    }

    #[test]
    fn given_default_config_should_apply_defaults() {
        let sink = RedisSink::new(1, given_default_config());
        assert_eq!(sink.batch_size, 100);
        assert_eq!(sink.payload_format, PayloadFormat::Binary);
        assert!(sink.include_metadata);
        assert!(sink.deterministic_ids);
        assert!(!sink.verbose);
        assert_eq!(sink.retry_policy.max_attempts, 3);
        assert_eq!(sink.retry_policy.base_delay, Duration::from_secs(1));
    }

    #[test]
    fn given_zero_batch_size_should_clamp_to_one() {
        let mut config = given_default_config();
        config.batch_size = Some(0);
        assert_eq!(RedisSink::new(1, config).batch_size, 1);
    }

    #[test]
    fn given_retry_attempts_outside_bounds_should_clamp() {
        let mut no_retry = given_default_config();
        no_retry.max_retries = Some(0);
        assert_eq!(RedisSink::new(1, no_retry).retry_policy.max_attempts, 1);

        let mut excessive_retries = given_default_config();
        excessive_retries.max_retries = Some(u32::MAX);
        assert_eq!(
            RedisSink::new(1, excessive_retries)
                .retry_policy
                .max_attempts,
            DEFAULT_MAX_RETRIES
        );
    }

    #[test]
    fn given_invalid_retry_delay_should_fall_back_to_default() {
        let mut config = given_default_config();
        config.retry_delay = Some("soon".to_string());
        let sink = RedisSink::new(1, config);
        assert_eq!(sink.retry_policy.base_delay, DEFAULT_RETRY_DELAY);
    }

    #[test]
    fn given_response_timeout_values_should_parse_or_fall_back() {
        let sink = RedisSink::new(1, given_default_config());
        assert_eq!(sink.response_timeout, DEFAULT_RESPONSE_TIMEOUT);

        let mut config = given_default_config();
        config.response_timeout = Some("10s".to_string());
        assert_eq!(
            RedisSink::new(1, config).response_timeout,
            Duration::from_secs(10)
        );

        let mut config = given_default_config();
        config.response_timeout = Some("soon".to_string());
        assert_eq!(
            RedisSink::new(1, config).response_timeout,
            DEFAULT_RESPONSE_TIMEOUT
        );
    }

    #[test]
    fn given_payload_format_values_should_select_variant() {
        assert_eq!(PayloadFormat::from(None), PayloadFormat::Binary);
        assert_eq!(PayloadFormat::from(Some("TEXT")), PayloadFormat::Text);
        assert_eq!(PayloadFormat::from(Some("JSON")), PayloadFormat::Json);
        assert_eq!(PayloadFormat::from(Some("surprise")), PayloadFormat::Binary);
    }

    #[test]
    fn given_deterministic_ids_should_scope_key_per_partition() {
        let sink = RedisSink::new(1, given_default_config());
        let topic = TopicMetadata {
            stream: "s".to_string(),
            topic: "t".to_string(),
        };
        assert_eq!(sink.target_key(&topic, 3), "iggy:events:s:t:3");
    }

    #[test]
    fn given_auto_ids_should_use_configured_key_unchanged() {
        let mut config = given_default_config();
        config.deterministic_ids = Some(false);
        let sink = RedisSink::new(1, config);
        let topic = TopicMetadata {
            stream: "s".to_string(),
            topic: "t".to_string(),
        };
        assert_eq!(sink.target_key(&topic, 3), "iggy:events");
    }

    #[test]
    fn given_payload_variants_should_borrow_without_copy_except_json() {
        let sink = RedisSink::new(1, given_default_config());
        let raw = given_message(Payload::Raw(vec![1, 2, 3]));
        assert!(matches!(sink.payload_bytes(&raw), Ok(Cow::Borrowed(_))));
        let text = given_message(Payload::Text("hello".to_string()));
        assert!(matches!(sink.payload_bytes(&text), Ok(Cow::Borrowed(_))));
        let proto = given_message(Payload::Proto("message".to_string()));
        assert!(matches!(sink.payload_bytes(&proto), Ok(Cow::Borrowed(_))));
        let flat_buffer = given_message(Payload::FlatBuffer(vec![1, 2, 3]));
        assert!(matches!(
            sink.payload_bytes(&flat_buffer),
            Ok(Cow::Borrowed(_))
        ));
        let avro = given_message(Payload::Avro(vec![1, 2, 3]));
        assert!(matches!(sink.payload_bytes(&avro), Ok(Cow::Borrowed(_))));
        let json = given_message(Payload::Json(simd_json::OwnedValue::Static(
            simd_json::StaticNode::Null,
        )));
        assert!(matches!(sink.payload_bytes(&json), Ok(Cow::Owned(_))));
    }

    #[test]
    fn given_text_format_and_invalid_utf8_should_reject_payload() {
        let mut config = given_default_config();
        config.payload_format = Some("text".to_string());
        let sink = RedisSink::new(1, config);
        let message = given_message(Payload::Raw(vec![0xff, 0xfe]));
        assert!(sink.payload_bytes(&message).is_err());
    }

    #[test]
    fn given_binary_format_and_invalid_utf8_should_accept_payload() {
        let sink = RedisSink::new(1, given_default_config());
        let message = given_message(Payload::Raw(vec![0xff, 0xfe]));
        assert!(sink.payload_bytes(&message).is_ok());
    }

    #[test]
    fn given_json_format_should_accept_json_and_reject_other_payloads() {
        let mut config = given_default_config();
        config.payload_format = Some("json".to_string());
        let sink = RedisSink::new(1, config);
        let valid = given_message(Payload::Text(r#"{"value":42}"#.to_string()));
        let invalid = given_message(Payload::Text("not-json".to_string()));
        assert!(sink.payload_bytes(&valid).is_ok());
        assert!(sink.payload_bytes(&invalid).is_err());
    }

    #[test]
    fn given_connection_url_and_legacy_url_should_both_deserialize() {
        for field in ["connection_url", "url"] {
            let config_json =
                format!(r#"{{"{field}":"redis://localhost:6379","stream_key":"iggy:events"}}"#);
            let config: RedisSinkConfig =
                serde_json::from_str(&config_json).expect("Redis config should deserialize");
            assert_eq!(
                config.connection_url.expose_secret(),
                "redis://localhost:6379"
            );
        }
    }

    #[test]
    fn given_unsupported_mode_should_fail_deserialization() {
        let result = serde_json::from_str::<RedisSinkConfig>(
            r#"{"connection_url":"redis://localhost:6379","mode":"cache","stream_key":"iggy:events"}"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn given_user_headers_should_preserve_text_and_encode_binary() {
        let mut message = given_message(Payload::Raw(vec![1]));
        message.headers = Some(BTreeMap::from([
            (
                HeaderKey::try_from("content-type").expect("valid header key"),
                HeaderValue::try_from("application/json").expect("valid text header"),
            ),
            (
                HeaderKey::try_from("trace-id").expect("valid header key"),
                HeaderValue::try_from(vec![0xde, 0xad, 0xbe, 0xef]).expect("valid binary header"),
            ),
        ]));
        let sink = RedisSink::new(1, given_default_config());

        let bytes = sink
            .headers_bytes(&message)
            .expect("headers should encode")
            .expect("headers should be present");
        let headers: serde_json::Value =
            serde_json::from_slice(&bytes).expect("headers should be JSON");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["trace-id"]["data"], "3q2+7w==");
        assert_eq!(
            headers["trace-id"]["iggy_header_encoding"],
            HEADER_ENCODING_BASE64
        );
    }

    #[test]
    fn given_xadd_id_conflict_should_be_treated_as_duplicate() {
        let conflict = RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "ERR",
            "The ID specified in XADD is equal or smaller than the target stream top item"
                .to_string(),
        ));
        assert!(is_duplicate_entry(&conflict));
    }

    #[test]
    fn given_connection_error_should_be_transient() {
        let error = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(is_transient_error(&error));
        assert!(matches!(
            map_redis_error("XADD", error),
            Error::Connection(_)
        ));
    }

    #[test]
    fn given_wrongtype_error_should_be_permanent() {
        let error = RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value".to_string(),
        ));
        assert!(!is_transient_error(&error));
        assert!(matches!(
            map_redis_error("XADD", error),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_authentication_error_should_be_permanent() {
        let error = RedisError::from((
            redis::ErrorKind::AuthenticationFailed,
            "Authentication failed",
        ));
        assert!(!is_transient_error(&error));
        assert!(matches!(
            map_redis_error("XADD", error),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_unreachable_server_when_opening_should_not_leak_password() {
        let mut config = given_default_config();
        config.connection_url = SecretString::from("redis://user:hunter2@127.0.0.1:1");
        let mut sink = RedisSink::new(1, config);
        let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime should start");
        let error = runtime.block_on(sink.open()).unwrap_err().to_string();
        assert!(!error.contains("hunter2"), "password leaked: {error}");
    }

    #[test]
    fn given_no_connection_should_return_error_not_silent_ok() {
        let sink = RedisSink::new(1, given_default_config());
        let topic = TopicMetadata {
            stream: "s".to_string(),
            topic: "t".to_string(),
        };
        let metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Raw,
        };
        let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime should start");
        let result = runtime.block_on(sink.consume(
            &topic,
            metadata,
            vec![given_message(Payload::Raw(vec![1]))],
        ));
        assert!(result.is_err());
        assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 0);
    }
}
