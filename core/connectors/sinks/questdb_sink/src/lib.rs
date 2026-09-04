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

mod mapping;

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use questdb::ErrorCode;
use questdb::QuestDb;
use questdb::ingress::AckLevel;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub use mapping::{Mapping, RowContext, RowError, TimestampSource, TimestampUnit};

sink_connector!(QuestDbSink);

const CONNECTOR_NAME: &str = "QuestDB sink";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const DEFAULT_FLUSH_TIMEOUT: &str = "30s";

/// Rejections are logged one line per record so a bad message can be traced
/// back to its offset. A batch where every record is malformed would otherwise
/// emit one line per message, so the tail is collapsed into a single summary.
const MAX_LOGGED_REJECTIONS_PER_BATCH: usize = 20;

/// Upper bound on the payload text included when `log_rejected_payload` is on.
const REJECTED_PAYLOAD_PREVIEW_BYTES: usize = 512;

/// Deserialize only. Nothing re-serializes a plugin config, and leaving
/// `Serialize` off is what keeps `connection_string` unserializable rather than
/// merely un-serialized.
#[derive(Debug, Clone, Deserialize)]
pub struct QuestDbSinkConfig {
    /// QuestDB connect string, `ws::` or `wss::`. May carry credentials, so it
    /// is treated as a secret throughout.
    pub connection_string: SecretString,
    pub table: String,
    pub timestamp_source: Option<String>,
    pub timestamp_field: Option<String>,
    pub timestamp_unit: Option<String>,
    pub symbol_columns: Option<Vec<String>>,
    pub uuid_columns: Option<Vec<String>>,
    pub include_stream_column: Option<bool>,
    pub include_topic_column: Option<bool>,
    pub include_partition_column: Option<bool>,
    pub include_offset_column: Option<bool>,
    pub include_headers: Option<bool>,
    pub ack_level: Option<String>,
    pub flush_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Include a truncated payload in the log line for a rejected message.
    /// Off by default: a rejected payload is still user data and may carry
    /// personal or otherwise sensitive fields.
    pub log_rejected_payload: Option<bool>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug)]
pub struct QuestDbSink {
    pub id: u32,
    connection_string: SecretString,
    mapping: Arc<Mapping>,
    ack_level: AckLevel,
    flush_timeout: Duration,
    batch_size: usize,
    verbose: bool,
    log_rejected_payload: bool,
    /// The `sink_connector!` macro requires `new` to be infallible, so a bad
    /// config is recorded here and surfaced from `open` instead.
    init_error: Option<Error>,
    db: Option<Arc<QuestDb>>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    rows_written: u64,
    rejected_rows: u64,
}

impl QuestDbSink {
    pub fn new(id: u32, config: QuestDbSinkConfig) -> Self {
        let mut init_error = None;
        let mut reject = |error: Error| {
            if init_error.is_none() {
                init_error = Some(error);
            }
        };

        let timestamp_source = TimestampSource::parse(config.timestamp_source.as_deref())
            .unwrap_or_else(|| {
                reject(Error::InvalidConfigValue("timestamp_source".to_owned()));
                TimestampSource::default()
            });
        let timestamp_unit =
            TimestampUnit::parse(config.timestamp_unit.as_deref()).unwrap_or_else(|| {
                reject(Error::InvalidConfigValue("timestamp_unit".to_owned()));
                TimestampUnit::default()
            });
        if timestamp_source == TimestampSource::Payload && config.timestamp_field.is_none() {
            reject(Error::InvalidConfigValue("timestamp_field".to_owned()));
        }
        let ack_level = parse_ack_level(config.ack_level.as_deref()).unwrap_or_else(|| {
            reject(Error::InvalidConfigValue("ack_level".to_owned()));
            AckLevel::Ok
        });

        let raw_timeout = config
            .flush_timeout
            .as_deref()
            .unwrap_or(DEFAULT_FLUSH_TIMEOUT);
        let flush_timeout = HumanDuration::from_str(raw_timeout)
            .map(|duration| *duration)
            .unwrap_or_else(|_| {
                warn!(
                    "Invalid flush_timeout for {CONNECTOR_NAME} ID: {id}, defaulting to {DEFAULT_FLUSH_TIMEOUT}"
                );
                Duration::from_secs(30)
            });

        let mapping = Mapping {
            table: config.table,
            symbol_columns: config
                .symbol_columns
                .unwrap_or_default()
                .into_iter()
                .collect(),
            uuid_columns: config
                .uuid_columns
                .unwrap_or_default()
                .into_iter()
                .collect(),
            timestamp_source,
            timestamp_field: config.timestamp_field,
            timestamp_unit,
            include_stream_column: config.include_stream_column.unwrap_or(true),
            include_topic_column: config.include_topic_column.unwrap_or(true),
            include_partition_column: config.include_partition_column.unwrap_or(false),
            include_offset_column: config.include_offset_column.unwrap_or(false),
            include_headers: config.include_headers.unwrap_or(false),
        };
        if let Err(error) = validate_column_overlap(&mapping) {
            reject(error);
        }

        Self {
            id,
            connection_string: config.connection_string,
            mapping: Arc::new(mapping),
            ack_level,
            flush_timeout,
            batch_size: config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE) as usize,
            verbose: config.verbose_logging.unwrap_or(false),
            log_rejected_payload: config.log_rejected_payload.unwrap_or(false),
            init_error,
            db: None,
            state: Mutex::new(State {
                messages_processed: 0,
                rows_written: 0,
                rejected_rows: 0,
            }),
        }
    }

    async fn write_batch(
        &self,
        db: Arc<QuestDb>,
        messages: Vec<ConsumedMessage>,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
    ) -> Result<BatchOutcome, Error> {
        let mapping = Arc::clone(&self.mapping);
        let stream = topic_metadata.stream.clone();
        let topic = topic_metadata.topic.clone();
        let ack_level = self.ack_level;
        let flush_timeout = self.flush_timeout;
        let id = self.id;
        let log_payload = self.log_rejected_payload;

        // `questdb-rs` is synchronous: the QWP driver owns its own I/O thread
        // and `flush` / `wait` block. Row building is cheap but is done here
        // too, so the whole batch crosses the boundary exactly once.
        let handle = tokio::task::spawn_blocking(move || -> Result<BatchOutcome, FlushError> {
            let mut sender = db.borrow_sender().map_err(FlushError::publishing)?;
            let mut buffer = sender.new_buffer();
            let context = RowContext {
                stream: &stream,
                topic: &topic,
                partition_id,
            };

            let mut outcome = BatchOutcome::default();
            for message in &messages {
                let reason = match mapping.append_row(&mut buffer, message, context) {
                    Ok(()) => {
                        outcome.rows_written += 1;
                        continue;
                    }
                    Err(RowError::Invalid(reason)) => reason,
                    Err(RowError::Client(error)) if !is_transient(&error) => error.to_string(),
                    Err(RowError::Client(error)) => {
                        return Err(FlushError::publishing(error));
                    }
                };

                // A rejected record is unrecoverable: the runtime commits
                // the offset before `consume` runs (#2928) and discards its
                // result (#2927), so this log line is the only trace that
                // survives. Log the identity needed to find the message,
                // one line per record rather than a batch summary.
                outcome.rejected_rows += 1;
                if outcome.rejected_rows <= MAX_LOGGED_REJECTIONS_PER_BATCH {
                    let payload = if log_payload {
                        format!(", payload: {}", payload_preview(&message.payload))
                    } else {
                        String::new()
                    };
                    error!(
                        "{CONNECTOR_NAME} ID: {id} rejected message, stream: {stream}, topic: {topic}, partition_id: {partition_id}, offset: {}, message_id: {}, reason: {reason}{payload}",
                        message.offset, message.id
                    );
                }
                outcome.last_rejection = Some(reason);
            }

            if outcome.rejected_rows > MAX_LOGGED_REJECTIONS_PER_BATCH {
                error!(
                    "{CONNECTOR_NAME} ID: {id} rejected {} messages in this batch, stream: {stream}, topic: {topic}, partition_id: {partition_id}; {} further rejections were not logged individually",
                    outcome.rejected_rows,
                    outcome.rejected_rows - MAX_LOGGED_REJECTIONS_PER_BATCH
                );
            }

            if outcome.rows_written > 0 {
                sender
                    .flush_buffer(&mut buffer)
                    .map_err(FlushError::publishing)?;
                if ack_level != AckLevel::Ok || flush_timeout > Duration::ZERO {
                    sender
                        .wait(ack_level, flush_timeout)
                        .map_err(FlushError::awaiting)?;
                }
            }
            Ok(outcome)
        });

        handle
            .await
            .map_err(|error| {
                Error::CannotStoreData(format!("blocking flush task failed: {error}"))
            })?
            .map_err(|error| self.map_client_error(error))
    }

    fn map_client_error(&self, failure: FlushError) -> Error {
        let FlushError { phase, error } = failure;
        let code = error.code();
        let message = format!("{CONNECTOR_NAME} ID: {}: {error}", self.id);
        if phase == FlushPhase::Publishing && is_transient(&error) {
            return Error::CannotStoreData(message);
        }
        match code {
            ErrorCode::ServerSchemaMismatch => Error::SchemaMismatch(message),
            _ => Error::PermanentHttpError(message),
        }
    }
}

#[async_trait]
impl Sink for QuestDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(error) = self.init_error.take() {
            error!(
                "{CONNECTOR_NAME} ID: {} has an invalid config: {error}",
                self.id
            );
            return Err(error);
        }
        let connection_string = self.connection_string.expose_secret().to_owned();
        // `QuestDb::connect` dials the server, so this doubles as the
        // connectivity check the sink contract asks for in `open`.
        let db = tokio::task::spawn_blocking(move || QuestDb::connect(&connection_string))
            .await
            .map_err(|error| Error::InitError(format!("connect task failed: {error}")))?
            .map_err(|error| Error::InitError(format!("cannot connect to QuestDB: {}", error)))?;
        self.db = Some(Arc::new(db));
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, table: {}, ack_level: {:?}",
            self.id, self.mapping.table, self.ack_level
        );
        if self.ack_level == AckLevel::Durable {
            // A node without WAL shipping accepts the rows and simply never
            // advances the durable watermark, so the failure shows up as a
            // flush timeout rather than a connect error.
            warn!(
                "{CONNECTOR_NAME} ID: {} requests durable acks; this needs QuestDB Enterprise with \
                 replication configured, otherwise every flush will time out after {:?}",
                self.id, self.flush_timeout
            );
        }
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        mut messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let Some(db) = self.db.as_ref() else {
            return Err(Error::InitError(
                "QuestDB pool is not initialized".to_owned(),
            ));
        };
        if messages.is_empty() {
            return Ok(());
        }

        let received = messages.len();
        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {received} messages, stream: {}, topic: {}, partition_id: {}, current_offset: {}",
                self.id,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {received} messages, current_offset: {}",
                self.id, messages_metadata.current_offset
            );
        }

        let mut total = BatchOutcome::default();
        while !messages.is_empty() {
            let take = self.batch_size.min(messages.len());
            let batch: Vec<ConsumedMessage> = messages.drain(..take).collect();
            let outcome = self
                .write_batch(
                    Arc::clone(db),
                    batch,
                    topic_metadata,
                    messages_metadata.partition_id,
                )
                .await?;
            total.merge(outcome);
        }

        if total.rejected_rows > 0 {
            warn!(
                "{CONNECTOR_NAME} ID: {} rejected {} of {received} messages, last reason: {}",
                self.id,
                total.rejected_rows,
                total.last_rejection.as_deref().unwrap_or("unknown")
            );
        }

        let mut state = self.state.lock().await;
        state.messages_processed += received as u64;
        state.rows_written += total.rows_written as u64;
        state.rejected_rows += total.rejected_rows as u64;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(db) = self.db.take() {
            // Dropping the pool drains buffered frames, which blocks for up to
            // the client's `close_flush_timeout_millis`.
            if let Err(error) = tokio::task::spawn_blocking(move || drop(db)).await {
                warn!(
                    "{CONNECTOR_NAME} ID: {} close task failed: {error}",
                    self.id
                );
            }
        }
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, rows written: {}, rejected: {}",
            self.id, state.messages_processed, state.rows_written, state.rejected_rows
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct BatchOutcome {
    rows_written: usize,
    rejected_rows: usize,
    last_rejection: Option<String>,
}

impl BatchOutcome {
    fn merge(&mut self, other: BatchOutcome) {
        self.rows_written += other.rows_written;
        self.rejected_rows += other.rejected_rows;
        if other.last_rejection.is_some() {
            self.last_rejection = other.last_rejection;
        }
    }
}

/// Renders a rejected payload for the log, truncated on a char boundary so a
/// large or binary message cannot blow up the log line.
fn payload_preview(payload: &iggy_connector_sdk::Payload) -> String {
    use iggy_connector_sdk::Payload;

    let rendered = match payload {
        Payload::Json(value) => {
            simd_json::to_string(value).unwrap_or_else(|_| "<unrenderable>".to_owned())
        }
        Payload::Text(text) | Payload::Proto(text) => text.clone(),
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
            format!("<{} bytes>", bytes.len())
        }
    };
    if rendered.len() <= REJECTED_PAYLOAD_PREVIEW_BYTES {
        return rendered;
    }
    let mut end = REJECTED_PAYLOAD_PREVIEW_BYTES;
    while end > 0 && !rendered.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes total)", &rendered[..end], rendered.len())
}

fn parse_ack_level(raw: Option<&str>) -> Option<AckLevel> {
    match raw.map(str::to_ascii_lowercase).as_deref() {
        None | Some("ok") => Some(AckLevel::Ok),
        Some("durable") => Some(AckLevel::Durable),
        Some(_) => None,
    }
}

/// A column cannot be both a `SYMBOL` and a `UUID`, and neither can double as
/// the designated timestamp. Catching it here beats a per-row failure later.
fn validate_column_overlap(mapping: &Mapping) -> Result<(), Error> {
    let overlap: HashSet<_> = mapping
        .symbol_columns
        .intersection(&mapping.uuid_columns)
        .collect();
    if let Some(column) = overlap.into_iter().next() {
        return Err(Error::InvalidConfigValue(format!(
            "column {column} is listed in both symbol_columns and uuid_columns"
        )));
    }
    if let Some(field) = mapping.timestamp_field.as_deref()
        && (mapping.symbol_columns.contains(field) || mapping.uuid_columns.contains(field))
    {
        return Err(Error::InvalidConfigValue(format!(
            "timestamp_field {field} cannot also be a symbol or uuid column"
        )));
    }
    Ok(())
}

/// Which half of the flush failed.
///
/// `flush_buffer` either appends the frame to the local publication log or it
/// does not, so a failure there can be safe to re-send. By the time `wait` runs
/// the frame is already published and the rows may already be committed, so a
/// failure there is never safe to re-send. A durable-ACK stall against a server
/// without replication configured is exactly this case: the rows land, only the
/// watermark never advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushPhase {
    Publishing,
    Awaiting,
}

#[derive(Debug)]
struct FlushError {
    phase: FlushPhase,
    error: questdb::Error,
}

impl FlushError {
    fn publishing(error: questdb::Error) -> Self {
        Self {
            phase: FlushPhase::Publishing,
            error,
        }
    }

    fn awaiting(error: questdb::Error) -> Self {
        Self {
            phase: FlushPhase::Awaiting,
            error,
        }
    }
}

/// Transport-level failures are worth retrying; server rejections are not,
/// because a rejection is deterministic. The server refused the frame, so the
/// same bytes will be refused again and the rows were never stored.
///
/// A retryable code alone is not enough. The client sets `in_doubt` when
/// delivery is unknown, and documents that `FailoverRetry` in particular can
/// carry it, so re-sending would duplicate rows the server may already hold.
/// Both must hold for a failure to count as transient.
///
/// Note that the runtime currently discards `Sink::consume`'s return value at
/// the FFI boundary (#2927), so this classification only shapes logging today.
/// It becomes load-bearing once the return value is honoured.
fn is_transient(error: &questdb::Error) -> bool {
    is_retryable(error.code(), error.in_doubt())
}

/// Split from [`is_transient`] so the rule can be exercised directly: the
/// client keeps `with_in_doubt` crate-private, so a test cannot build an
/// in-doubt `questdb::Error`.
fn is_retryable(code: ErrorCode, in_doubt: bool) -> bool {
    if in_doubt {
        return false;
    }
    matches!(
        code,
        ErrorCode::SocketError
            | ErrorCode::ConnectTimeout
            | ErrorCode::FailoverRetry
            | ErrorCode::RoleMismatch
            | ErrorCode::ServerFlushError
            | ErrorCode::ServerInternalError
            | ErrorCode::CouldNotResolveAddr
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QuestDbSinkConfig {
        QuestDbSinkConfig {
            connection_string: SecretString::from("ws::addr=localhost:9000;"),
            table: "iggy_events".to_owned(),
            timestamp_source: None,
            timestamp_field: None,
            timestamp_unit: None,
            symbol_columns: None,
            uuid_columns: None,
            include_stream_column: None,
            include_topic_column: None,
            include_partition_column: None,
            include_offset_column: None,
            include_headers: None,
            ack_level: None,
            flush_timeout: None,
            batch_size: None,
            log_rejected_payload: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_minimal_config_when_constructed_should_apply_defaults() {
        let sink = QuestDbSink::new(1, config());
        assert!(sink.init_error.is_none());
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE as usize);
        assert_eq!(sink.flush_timeout, Duration::from_secs(30));
        assert_eq!(sink.ack_level, AckLevel::Ok);
        assert_eq!(sink.mapping.timestamp_source, TimestampSource::Message);
        assert!(sink.mapping.include_stream_column);
        assert!(sink.mapping.include_topic_column);
        assert!(!sink.mapping.include_partition_column);
    }

    #[tokio::test]
    async fn given_payload_timestamp_without_field_when_opened_should_fail() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_payload_timestamp_with_field_when_constructed_should_succeed() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        config.timestamp_field = Some("event_time".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.mapping.timestamp_source, TimestampSource::Payload);
        assert_eq!(sink.mapping.timestamp_field.as_deref(), Some("event_time"));
    }

    #[test]
    fn given_unknown_enum_values_when_constructed_should_record_init_error() {
        for (field, value) in [
            ("timestamp_source", "yesterday"),
            ("timestamp_unit", "fortnights"),
            ("ack_level", "maybe"),
        ] {
            let mut config = config();
            match field {
                "timestamp_source" => config.timestamp_source = Some(value.to_owned()),
                "timestamp_unit" => config.timestamp_unit = Some(value.to_owned()),
                _ => config.ack_level = Some(value.to_owned()),
            }
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(sink.init_error, Some(Error::InvalidConfigValue(_))),
                "{field} should reject {value}"
            );
        }
    }

    #[test]
    fn given_column_in_symbol_and_uuid_lists_when_constructed_should_record_init_error() {
        let mut config = config();
        config.symbol_columns = Some(vec!["id".to_owned()]);
        config.uuid_columns = Some(vec!["id".to_owned()]);
        let sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.init_error,
            Some(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_timestamp_field_also_listed_as_symbol_when_constructed_should_record_init_error() {
        let mut config = config();
        config.timestamp_source = Some("payload".to_owned());
        config.timestamp_field = Some("event_time".to_owned());
        config.symbol_columns = Some(vec!["event_time".to_owned()]);
        let sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.init_error,
            Some(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_invalid_flush_timeout_when_constructed_should_fall_back_to_default() {
        let mut config = config();
        config.flush_timeout = Some("not-a-duration".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.flush_timeout, Duration::from_secs(30));
    }

    #[test]
    fn given_toml_config_when_deserialized_should_populate_fields() {
        let raw = r#"
            connection_string = "ws::addr=questdb:9000;"
            table = "trades"
            timestamp_source = "payload"
            timestamp_field = "ts"
            symbol_columns = ["symbol", "side"]
            batch_size = 500
        "#;
        let config: QuestDbSinkConfig = toml::from_str(raw).unwrap();
        let sink = QuestDbSink::new(7, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.batch_size, 500);
        assert_eq!(sink.mapping.table, "trades");
        assert!(sink.mapping.symbol_columns.contains("side"));
    }

    #[test]
    fn given_retryable_code_when_not_in_doubt_should_be_retryable() {
        assert!(is_retryable(ErrorCode::FailoverRetry, false));
        assert!(is_retryable(ErrorCode::SocketError, false));
    }

    #[test]
    fn given_retryable_code_when_in_doubt_should_not_be_retryable() {
        // The client documents that `FailoverRetry` in particular can carry
        // `in_doubt`, so the code alone must not authorise a re-send.
        assert!(!is_retryable(ErrorCode::FailoverRetry, true));
        assert!(!is_retryable(ErrorCode::SocketError, true));
    }

    #[test]
    fn given_server_rejection_when_classified_should_not_be_retryable() {
        assert!(!is_retryable(ErrorCode::ServerSchemaMismatch, false));
        assert!(!is_retryable(ErrorCode::ServerSecurityError, false));
        assert!(!is_retryable(ErrorCode::ServerParseError, false));
    }

    #[test]
    fn given_ack_wait_failure_when_mapped_should_be_permanent() {
        // The rows are already published by the time `wait` runs, so even a
        // retryable code must not produce a retryable sink error: re-sending
        // would duplicate rows QuestDB may already hold. This is the
        // durable-ACK stall against a server without replication.
        let sink = QuestDbSink::new(1, config());
        let failure = FlushError::awaiting(questdb::Error::new(
            ErrorCode::FailoverRetry,
            "wait(durable) timed out with no ack progress",
        ));
        assert!(matches!(
            sink.map_client_error(failure),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn given_publish_failure_when_mapped_should_stay_retryable() {
        let sink = QuestDbSink::new(1, config());
        let failure = FlushError::publishing(questdb::Error::new(
            ErrorCode::SocketError,
            "connection reset",
        ));
        assert!(matches!(
            sink.map_client_error(failure),
            Error::CannotStoreData(_)
        ));
    }

    #[test]
    fn given_schema_mismatch_when_mapped_should_be_schema_error() {
        let sink = QuestDbSink::new(1, config());
        let failure = FlushError::publishing(questdb::Error::new(
            ErrorCode::ServerSchemaMismatch,
            "long arrays are not supported",
        ));
        assert!(matches!(
            sink.map_client_error(failure),
            Error::SchemaMismatch(_)
        ));
    }

    #[test]
    fn given_debug_output_when_formatted_should_not_leak_connection_string() {
        let mut config = config();
        config.connection_string =
            SecretString::from("ws::addr=questdb:9000;username=admin;password=hunter2;");
        let sink = QuestDbSink::new(1, config);
        let rendered = format!("{sink:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }
}
