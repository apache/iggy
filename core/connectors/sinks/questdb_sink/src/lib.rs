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

use std::borrow::Cow;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use questdb::ConnectHandlers;
use questdb::ErrorCode;
use questdb::QuestDb;
use questdb::ingress::AckLevel;
use questdb::ingress::TableName;
use questdb::ingress::{QwpWsErrorHandler, QwpWsErrorPolicy, QwpWsSenderError};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

pub use mapping::{Mapping, RowContext, RowError, TimestampSource, TimestampUnit};

sink_connector!(QuestDbSink);

const CONNECTOR_NAME: &str = "QuestDB sink";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const DEFAULT_FLUSH_TIMEOUT: &str = "30s";
/// `DEFAULT_FLUSH_TIMEOUT` parsed, so the text and the fallback cannot drift.
const DEFAULT_FLUSH_TIMEOUT_DURATION: Duration = Duration::from_secs(30);

/// Rejections are logged one line per record so a bad message can be traced
/// back to its offset. A batch where every record is malformed would otherwise
/// emit one line per message, so the tail is collapsed into a single summary.
const MAX_LOGGED_REJECTIONS_PER_BATCH: usize = 20;

/// Upper bound on the payload text included when `log_rejected_payload` is on.
const REJECTED_PAYLOAD_PREVIEW_BYTES: usize = 512;

/// A sustained lack of acknowledgement stops looking like lag and starts
/// looking like a sink that is quietly committing nothing, so after this many
/// consecutive unacknowledged batches the connector reports a failure to make
/// the condition visible in the runtime's metrics.
const PENDING_ACK_BATCHES_BEFORE_ESCALATION: u64 = 10;

/// Installs a process-wide rustls crypto provider exactly once.
///
/// `rustls` picks a provider from crate features only when exactly one of
/// `ring` and `aws-lc-rs` is linked. The connectors runtime links both, so
/// `ClientConfig::builder()` cannot choose and **panics** instead of returning
/// an error, taking down any `wss://` connection attempt. Installing one up
/// front makes the choice explicit.
///
/// The slot is process-wide and shared. The Apache Iggy client installs into it
/// too, `aws-lc-rs` from `core/sdk/src/tcp/tcp_client.rs` and `ring` from
/// `core/sdk/src/quic/quic_client.rs`, so whichever runs first decides for the
/// whole process. Every one of those providers is correct, and the first install
/// wins rather than failing. This runs only for a connect string that needs TLS,
/// which keeps a plain `ws://` sink from deciding TLS for a component that does
/// use it.
static INSTALL_CRYPTO_PROVIDER: Once = Once::new();

fn ensure_crypto_provider() {
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Flush once the encoded buffer reaches this many bytes, independently of
/// `batch_size`.
///
/// QWP caps a single frame at the smallest of `max_buf_size`, the server's
/// advertised maximum batch size, and the store-and-forward segment payload
/// capacity, which is about 4 MiB with the default 4 MiB segments. The row API
/// does not split an oversized buffer, so without a byte bound a batch of wide
/// rows is rejected outright and every row in it is lost.
///
/// The margin between this default and that cap is deliberate, and larger than
/// one row's worth. `Buffer::len` is a local estimate on this transport rather
/// than the encoded frame size: it does not model the connection-scoped symbol
/// dictionary, which a frame can have to re-ship in full. A server advertising a
/// smaller batch size than this bound would also be refused before the bound
/// ever fires, which the caller then recovers from as a rejected window.
const DEFAULT_MAX_FLUSH_BYTES: usize = 1_000_000;

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
    /// Payload fields to store as `LONG` while `numbers_as_double` is on.
    pub integer_columns: Option<Vec<String>>,
    pub include_stream_column: Option<bool>,
    pub include_topic_column: Option<bool>,
    pub include_partition_column: Option<bool>,
    pub include_offset_column: Option<bool>,
    pub include_headers: Option<bool>,
    pub ack_level: Option<String>,
    pub flush_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Flush once the encoded buffer reaches this many bytes, regardless of
    /// `batch_size`. Guards the QWP per-frame cap.
    pub max_flush_bytes: Option<usize>,
    /// Include a truncated payload in the log line for a rejected message.
    /// Off by default: a rejected payload is still user data and may carry
    /// personal or otherwise sensitive fields.
    pub log_rejected_payload: Option<bool>,
    /// Write every JSON number as a `DOUBLE`, so a column holding a measurement
    /// is always a `DOUBLE`. On by default. Name the columns that must stay
    /// exact past 2^53 in `integer_columns` rather than turning this off.
    pub numbers_as_double: Option<bool>,
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
    max_flush_bytes: usize,
    verbose: bool,
    log_rejected_payload: bool,
    /// The `sink_connector!` macro requires `new` to be infallible, so a bad
    /// config is recorded here and surfaced from `open` instead.
    init_error: Option<Error>,
    /// Terminal rejections the handler has reported and no batch has accounted
    /// for yet. The handler is the only report of a rejection the sink never
    /// asks `wait` about, so each batch spends one and fails.
    server_rejections: Arc<AtomicU64>,
    db: Option<Arc<QuestDb>>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    rows_written: u64,
    rejected_rows: u64,
    pending_acks: u64,
    /// Consecutive batches per stream and topic that finished with at least one
    /// flush still unacknowledged. An acknowledged batch clears its own entry.
    consecutive_pending_ack_batches: HashMap<(String, String), u64>,
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
                DEFAULT_FLUSH_TIMEOUT_DURATION
            });
        // The client reads a zero timeout as "no deadline". At the default ack
        // level the sink treats it as fire and forget and never calls `wait`,
        // but a durable ack level has to wait, so the pair would block the FFI
        // call for good with no way to stop the connector.
        if ack_level == AckLevel::Durable && flush_timeout.is_zero() {
            reject(Error::InvalidConfigValue(
                "flush_timeout must be greater than zero when ack_level is \"durable\", because a zero timeout never expires".to_owned(),
            ));
        }

        // A bad table name fails on every row of every batch, so it is caught
        // once at construction instead of being reported per record forever.
        if let Err(error) = TableName::new(config.table.as_str()) {
            reject(Error::InvalidConfigValue(format!(
                "table {} is not a valid QuestDB table name: {error}",
                config.table
            )));
        }

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
            integer_columns: config
                .integer_columns
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
            numbers_as_double: config.numbers_as_double.unwrap_or(true),
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
            // Zero would make the chunking loop take nothing on every pass and
            // never finish, holding the FFI call open for good.
            batch_size: config
                .batch_size
                .filter(|rows| *rows > 0)
                .unwrap_or(DEFAULT_BATCH_SIZE) as usize,
            max_flush_bytes: config
                .max_flush_bytes
                .filter(|bytes| *bytes > 0)
                .unwrap_or(DEFAULT_MAX_FLUSH_BYTES),
            verbose: config.verbose_logging.unwrap_or(false),
            log_rejected_payload: config.log_rejected_payload.unwrap_or(false),
            init_error,
            server_rejections: Arc::new(AtomicU64::new(0)),
            db: None,
            state: Mutex::new(State {
                messages_processed: 0,
                rows_written: 0,
                rejected_rows: 0,
                pending_acks: 0,
                consecutive_pending_ack_batches: HashMap::new(),
            }),
        }
    }

    /// `already_logged` is the number of records earlier chunks of the same
    /// runtime batch rejected, so the per-batch log cap spans the batch rather
    /// than restarting for each chunk.
    async fn write_batch(
        &self,
        db: Arc<QuestDb>,
        messages: Vec<ConsumedMessage>,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        already_logged: usize,
    ) -> Result<BatchOutcome, Error> {
        let mapping = Arc::clone(&self.mapping);
        let stream = topic_metadata.stream.clone();
        let topic = topic_metadata.topic.clone();
        let ack_level = self.ack_level;
        let flush_timeout = self.flush_timeout;
        let id = self.id;
        let log_payload = self.log_rejected_payload;
        let max_flush_bytes = self.max_flush_bytes;

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
            // How many rows are sitting in the current buffer, unflushed.
            let mut buffered = 0usize;
            let flush_context = FlushContext {
                id,
                stream: &stream,
                topic: &topic,
                partition_id,
            };
            for message in &messages {
                // Flush before the buffer can outgrow the QWP per-frame cap.
                // `batch_size` bounds rows, not bytes, so a batch of wide rows
                // would otherwise be rejected whole and lose every row in it.
                if buffered > 0 && buffer.len() >= max_flush_bytes {
                    flush_window(
                        &mut sender,
                        &mut buffer,
                        ack_level,
                        flush_timeout,
                        &mut outcome,
                        &flush_context,
                    )?;
                    buffered = 0;
                }

                let reason = match mapping.append_row(&mut buffer, message, context) {
                    Ok(()) => {
                        outcome.rows_written += 1;
                        buffered += 1;
                        continue;
                    }
                    Err(RowError::Invalid(reason)) => reason,
                    // The record reached the buffer and was refused there, by
                    // a rule only the buffer knows: a column's type is pinned by
                    // whichever row defined it first, so a later row disagreeing
                    // with it cannot be judged from that row alone. The client
                    // rolls the half-written row back before returning the error,
                    // on every column and symbol setter, so the buffer is still
                    // usable and only this record is lost.
                    Err(RowError::Client(error)) => error.to_string(),
                    // `Buffer::at` refused the row without rolling it back, so
                    // the buffer holds a partial row and nothing can remove it.
                    // The rows already accepted are still good, so flush them
                    // and start a fresh buffer rather than losing the window.
                    // `Mapping::prepare_timestamp` rejects every value `at`
                    // would refuse, which is what keeps this unreachable.
                    Err(RowError::Unrecoverable(error)) => {
                        // The partial row cannot be removed, and publishing the
                        // buffer would risk carrying it along, so the whole
                        // window goes. The rows already counted as written are
                        // uncounted again rather than reported as delivered.
                        error!(
                            "{CONNECTOR_NAME} ID: {id} discarded {buffered} unflushed rows after the client refused a designated timestamp, stream: {stream}, topic: {topic}, partition_id: {partition_id}: {error}"
                        );
                        outcome.rows_written -= buffered;
                        outcome.rejected_rows += buffered;
                        buffered = 0;
                        buffer = sender.new_buffer();
                        error.to_string()
                    }
                };

                // A rejected record is unrecoverable: the runtime commits
                // the offset before `consume` runs (#2928) and never replays a
                // batch (#2927), so this log line is the only trace that
                // survives. Log the identity needed to find the message,
                // one line per record rather than a batch summary.
                outcome.rejected_rows += 1;
                if should_log_rejection(already_logged, outcome.rejected_rows) {
                    let payload = if log_payload {
                        format!(", payload: {}", payload_preview(&message.payload))
                    } else {
                        String::new()
                    };
                    error!(
                        "{CONNECTOR_NAME} ID: {id} rejected message, stream: {stream}, topic: {topic}, partition_id: {partition_id}, offset: {}, message_id: {}, reason: {reason}{payload}",
                        message.offset, message.id
                    );
                } else {
                    // Past the cap the line drops to `debug` rather than
                    // disappearing. The record is unrecoverable either way, so
                    // an operator tracing a bad producer still needs its
                    // identity, while `error` stays bounded for alerting.
                    debug!(
                        "{CONNECTOR_NAME} ID: {id} rejected message, stream: {stream}, topic: {topic}, partition_id: {partition_id}, offset: {}, message_id: {}, reason: {reason}",
                        message.offset, message.id
                    );
                }
                outcome.last_rejection = Some(reason);
            }

            if buffered > 0 {
                flush_window(
                    &mut sender,
                    &mut buffer,
                    ack_level,
                    flush_timeout,
                    &mut outcome,
                    &flush_context,
                )?;
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

    /// Takes one token from the terminal-rejection count, if the handler left
    /// one. Returns whether a token was taken.
    fn spend_server_rejection(&self) -> bool {
        self.server_rejections
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    }

    /// Names the messages a transient failure left unattempted.
    ///
    /// Their offsets are already committed and the runtime does not replay a
    /// failed batch (#2927), so this line is the only record that they existed.
    fn log_abandoned(
        &self,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        abandoned: &[ConsumedMessage],
    ) {
        let (Some(first), Some(last)) = (abandoned.first(), abandoned.last()) else {
            return;
        };
        error!(
            "{CONNECTOR_NAME} ID: {} abandoned {} messages without a write attempt after a transient failure, stream: {}, topic: {}, partition_id: {partition_id}, offsets: [{}, {}], message_ids: [{}, {}]",
            self.id,
            abandoned.len(),
            topic_metadata.stream,
            topic_metadata.topic,
            first.offset,
            last.offset,
            first.id,
            last.id
        );
    }

    fn map_client_error(&self, failure: FlushError) -> Error {
        let FlushError { phase, error } = failure;
        let message = format!("{CONNECTOR_NAME} ID: {}: {error}", self.id);
        if phase == FlushPhase::Publishing && is_transient(&error) {
            return Error::CannotStoreData(message);
        }
        // `ServerSchemaMismatch` is a query-path code and never reaches a
        // sender, so a schema rejection arrives as `ServerRejection` carrying a
        // schema category the sink cannot read. Both are reported the same way,
        // so the code is not read at all.
        Error::PermanentHttpError(message)
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
        let uses_tls = connection_string
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("wss:");
        if uses_tls {
            ensure_crypto_provider();
        }
        // A connect string that disables verification is a test convenience, and
        // the client only honours it when this crate was built with the
        // `insecure-skip-verify` feature. Say which of the two applies rather
        // than letting a production deployment carry it silently, or letting a
        // default build fail with the client's generic configuration error.
        if connection_string
            .to_ascii_lowercase()
            .contains("tls_verify=unsafe_off")
        {
            if cfg!(feature = "insecure-skip-verify") {
                warn!(
                    "{CONNECTOR_NAME} ID: {} has TLS certificate verification disabled through tls_verify=unsafe_off; use this only against a test server",
                    self.id
                );
            } else {
                return Err(Error::InvalidConfigValue(
                    "the connection string sets tls_verify=unsafe_off, which this build of \
                     the connector does not accept; rebuild it with the \
                     insecure-skip-verify feature if a test server with a self-signed \
                     certificate needs it"
                        .to_owned(),
                ));
            }
        }
        // Durable acks are refused outright unless the connect string asks the
        // server for them, so a config that sets one without the other fails
        // every batch while the rows themselves land. Catch it here rather than
        // letting the operator read that as data loss.
        if self.ack_level == AckLevel::Durable
            && !connection_string
                .to_ascii_lowercase()
                .contains("request_durable_ack=on")
        {
            return Err(Error::InvalidConfigValue(
                "ack_level is \"durable\" but the connection string does not set \
                 request_durable_ack=on, which QuestDB requires for durable acknowledgement"
                    .to_owned(),
            ));
        }

        let id = self.id;
        // Without a handler the client reports server rejections through the
        // `log` crate, which nothing in the connectors runtime bridges to
        // `tracing`, so every retriable rejection and all of the structured
        // detail would be discarded. This is the only place that detail exists:
        // the `questdb::Error` the sink sees carries none of it.
        let mut handlers = ConnectHandlers::default();
        let rejections = Arc::clone(&self.server_rejections);
        handlers.error_handler = Some(QwpWsErrorHandler::new(move |error| {
            report_server_rejection(id, &rejections, error)
        }));

        // `QuestDb::connect_with_handlers` dials the server, so this doubles as
        // the connectivity check the sink contract asks for in `open`.
        let db = tokio::task::spawn_blocking(move || {
            QuestDb::connect_with_handlers(&connection_string, handlers)
        })
        .await
        .map_err(|error| Error::InitError(format!("connect task failed: {error}")))?
        .map_err(|error| Error::InitError(format!("cannot connect to QuestDB: {}", error)))?;
        self.db = Some(Arc::new(db));
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, table: {}, ack_level: {:?}, numbers_as_double: {}, symbol_columns: {:?}, uuid_columns: {:?}, integer_columns: {:?}",
            self.id,
            self.mapping.table,
            self.ack_level,
            self.mapping.numbers_as_double,
            self.mapping.symbol_columns.iter().collect::<Vec<_>>(),
            self.mapping.uuid_columns.iter().collect::<Vec<_>>(),
            self.mapping.integer_columns.iter().collect::<Vec<_>>()
        );
        if self.ack_level == AckLevel::Durable {
            // A node without WAL shipping accepts the rows and simply never
            // advances the durable watermark, so the failure shows up as
            // acknowledgement lag rather than a connect error.
            warn!(
                "{CONNECTOR_NAME} ID: {} requests durable acks; this needs QuestDB Enterprise with \
                 replication configured, otherwise no flush is ever acknowledged within {:?}",
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
        let mut failure: Option<Error> = None;
        while !messages.is_empty() {
            let take = self.batch_size.min(messages.len());
            let batch: Vec<ConsumedMessage> = messages.drain(..take).collect();
            match self
                .write_batch(
                    Arc::clone(db),
                    batch,
                    topic_metadata,
                    messages_metadata.partition_id,
                    total.rejected_rows,
                )
                .await
            {
                Ok(outcome) => total.merge(outcome),
                Err(error) => {
                    // Returning here would discard every chunk still queued
                    // behind this one, at offsets the runtime committed before
                    // `consume` ran (#2928). A permanent failure belongs to the
                    // frame that carried it, so the chunks behind it are still
                    // worth attempting. A transient one is a connection fault,
                    // so they would fail the same way and each would spend its
                    // own `flush_timeout` doing it.
                    let transient = matches!(error, Error::CannotStoreData(_));
                    failure.get_or_insert(error);
                    if transient {
                        self.log_abandoned(
                            topic_metadata,
                            messages_metadata.partition_id,
                            &messages,
                        );
                        break;
                    }
                }
            }
        }

        if total.rejected_rows > 0 {
            warn!(
                "{CONNECTOR_NAME} ID: {} rejected {} of {received} messages, last reason: {}",
                self.id,
                total.rejected_rows,
                total.last_rejection.as_deref().unwrap_or("unknown")
            );
        }
        if total.rejected_rows > MAX_LOGGED_REJECTIONS_PER_BATCH {
            error!(
                "{CONNECTOR_NAME} ID: {} logged {} of those rejections at debug level only, to keep the error log bounded",
                self.id,
                total.rejected_rows - MAX_LOGGED_REJECTIONS_PER_BATCH
            );
        }

        let mut state = self.state.lock().await;
        state.messages_processed += received as u64;
        state.rows_written += total.rows_written as u64;
        state.rejected_rows += total.rejected_rows as u64;
        state.pending_acks += total.pending_acks as u64;
        // Keyed by stream and topic, because the runtime runs one `consume` per
        // topic against this one instance. A shared counter would let a healthy
        // topic reset the run belonging to a stalled one, and the escalation
        // would never fire. `partition_id` is not part of the key: it varies
        // between batches inside one task, which would reset the run just as
        // wrongly.
        let stall_key = (topic_metadata.stream.clone(), topic_metadata.topic.clone());
        let stalled = if total.pending_acks > 0 {
            let run = state
                .consecutive_pending_ack_batches
                .entry(stall_key)
                .or_insert(0);
            *run += 1;
            *run
        } else {
            state.consecutive_pending_ack_batches.remove(&stall_key);
            0
        };
        drop(state);

        // One unacknowledged flush is lag. A run of them means the server has
        // stopped acknowledging altogether, and reporting every one of those
        // batches as a success would leave the runtime's metrics showing a
        // healthy connector while nothing is being committed. Escalate so the
        // error counter moves; the rows stay queued either way, and the runtime
        // does not replay a failed batch, so nothing is lost by saying so.
        if let Some(error) = failure {
            // The flush that failed reported its own rejection, and the handler
            // counted the same one. Spend that token here so it does not go on
            // to fail a second batch for a rejection already reported.
            self.spend_server_rejection();
            return Err(error);
        }
        // A terminal rejection reported through the handler has to fail a batch.
        // The handler runs on the client's thread and cannot know which batch
        // caused it, so this counts them and spends one per batch. A flag would
        // collapse several rejections into one failure when batches for
        // different topics run at the same time. The count stays per instance
        // rather than per topic, because the handler sees a connection and a
        // frame range and has no way to name the topic that filled it; a
        // rejection raised with no flush error therefore fails whichever
        // topic's batch observes it next.
        if self.spend_server_rejection() {
            return Err(Error::PermanentHttpError(format!(
                "{CONNECTOR_NAME} ID: {}: QuestDB terminally rejected a frame from this connector; the preceding log line carries the category and the frame range",
                self.id
            )));
        }
        if should_escalate_stall(stalled) {
            return Err(Error::PermanentHttpError(format!(
                "{CONNECTOR_NAME} ID: {}: the last {stalled} batches were flushed without QuestDB acknowledging them at ack_level {:?}; the frames are queued but nothing is being committed",
                self.id, self.ack_level
            )));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(db) = self.db.take() {
            // Dropping the pool drains what it still holds, bounded by the
            // connect string's `close_flush_timeout`, and discards the rest.
            //
            // There is no useful wait to add here. `borrow_sender` rebases the
            // new lease onto the connection's current published frame number,
            // so a wait on it returns at once and proves nothing about the
            // frames earlier batches left queued. The drop is the drain:
            // `Drop for QuestDb` walks every idle store-and-forward connection
            // under one shared `close_flush_timeout` deadline. What it cannot
            // deliver it reports through the `log` crate, which the connectors
            // runtime does not bridge, so that warning never reaches an
            // operator. Set `sf_dir` in the connect string for a tail that
            // survives both the drain window and a restart; the README says so
            // under Store-and-forward.
            if let Err(error) = tokio::task::spawn_blocking(move || drop(db)).await {
                warn!(
                    "{CONNECTOR_NAME} ID: {} close task failed: {error}",
                    self.id
                );
            }
        }
        let state = self.state.lock().await;
        // `rows buffered` rather than `rows written`: the count is taken when a
        // row is added to a buffer, so it leads what the server has accepted and
        // excludes any batch that failed before its final flush.
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, processed: {}, rows buffered: {}, rejected: {}, flushes awaiting ack: {}",
            self.id,
            state.messages_processed,
            state.rows_written,
            state.rejected_rows,
            state.pending_acks
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct BatchOutcome {
    rows_written: usize,
    rejected_rows: usize,
    /// Flushes whose acknowledgement had not arrived before the timeout. The
    /// rows are persisted and still being delivered; this is lag, not loss.
    pending_acks: usize,
    last_rejection: Option<String>,
}

impl BatchOutcome {
    fn merge(&mut self, other: BatchOutcome) {
        self.rows_written += other.rows_written;
        self.rejected_rows += other.rejected_rows;
        self.pending_acks += other.pending_acks;
        if other.last_rejection.is_some() {
            self.last_rejection = other.last_rejection;
        }
    }
}

/// Renders a rejected payload for the log, truncated on a char boundary so a
/// large or binary message cannot blow up the log line.
fn payload_preview(payload: &Payload) -> String {
    // A text payload is borrowed rather than copied, because the common case is
    // a payload short enough to need no truncation at all.
    let rendered: Cow<'_, str> = match payload {
        Payload::Json(value) => {
            Cow::Owned(simd_json::to_string(value).unwrap_or_else(|_| "<unrenderable>".to_owned()))
        }
        Payload::Text(text) | Payload::Proto(text) => Cow::Borrowed(text.as_str()),
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
            Cow::Owned(format!("<{} bytes>", bytes.len()))
        }
    };
    if rendered.len() <= REJECTED_PAYLOAD_PREVIEW_BYTES {
        return rendered.into_owned();
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

/// A column has one type. It cannot be two of `SYMBOL`, `UUID` and `LONG`, and
/// none of them can double as the designated timestamp. Catching it here beats a
/// per-row failure later.
fn validate_column_overlap(mapping: &Mapping) -> Result<(), Error> {
    let lists = [
        ("symbol_columns", &mapping.symbol_columns),
        ("uuid_columns", &mapping.uuid_columns),
        ("integer_columns", &mapping.integer_columns),
    ];
    for (index, (name, columns)) in lists.iter().enumerate() {
        for (other_name, other) in &lists[index + 1..] {
            if let Some(column) = columns.iter().find(|column| other.contains(column)) {
                return Err(Error::InvalidConfigValue(format!(
                    "column {column} is listed in both {name} and {other_name}"
                )));
            }
        }
    }
    if let Some(field) = mapping.timestamp_field.as_deref()
        && let Some((name, _)) = lists.iter().find(|(_, columns)| columns.contains(field))
    {
        return Err(Error::InvalidConfigValue(format!(
            "timestamp_field {field} cannot also be listed in {name}"
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

/// Whether this rejection still fits the batch's `error` budget.
///
/// `already_logged` carries the rejections of earlier chunks of the same runtime
/// batch, so the budget spans the batch rather than restarting for each chunk.
/// Past it the record is logged at `debug` instead, which bounds the error log
/// without losing the record's identity.
fn should_log_rejection(already_logged: usize, rejected_in_chunk: usize) -> bool {
    already_logged + rejected_in_chunk <= MAX_LOGGED_REJECTIONS_PER_BATCH
}

/// Whether a run of unacknowledged batches has gone on long enough to report.
///
/// One unacknowledged flush is delivery lag. A run of them means the server has
/// stopped acknowledging, and reporting every one of those batches as a success
/// would leave the runtime's metrics showing a healthy connector while nothing
/// is committed.
fn should_escalate_stall(consecutive_batches: u64) -> bool {
    consecutive_batches >= PENDING_ACK_BATCHES_BEFORE_ESCALATION
}

/// Identity of the batch being flushed, for the log lines only.
struct FlushContext<'a> {
    id: u32,
    stream: &'a str,
    topic: &'a str,
    partition_id: u32,
}

/// Publishes the buffer and waits for the configured acknowledgement.
///
/// A pending acknowledgement is lag rather than loss, so it is counted and
/// warned about instead of failing the batch: the frames are queued and the
/// client keeps delivering them. `wait` already polls until `flush_timeout` is
/// exhausted, so there is nothing to retry here; a stall that outlasts several
/// batches is escalated by the caller instead, where the run of them is
/// visible.
fn flush_window(
    sender: &mut questdb::BorrowedSender<'_>,
    buffer: &mut questdb::ingress::Buffer,
    ack_level: AckLevel,
    flush_timeout: Duration,
    outcome: &mut BatchOutcome,
    context: &FlushContext<'_>,
) -> Result<(), FlushError> {
    sender
        .flush_buffer(buffer)
        .map_err(FlushError::publishing)?;

    // `Duration::ZERO` means "no deadline" to the client, not "do not wait", so
    // with the default ack level it is treated as fire and forget here instead
    // of being handed to `wait`, where it would block without bound.
    if ack_level == AckLevel::Ok && flush_timeout == Duration::ZERO {
        return Ok(());
    }

    let FlushContext {
        id,
        stream,
        topic,
        partition_id,
    } = *context;
    match sender.wait(ack_level, flush_timeout) {
        Ok(()) => Ok(()),
        Err(error) if is_pending_ack(&error) => {
            outcome.pending_acks += 1;
            warn!(
                "{CONNECTOR_NAME} ID: {id} flushed rows whose acknowledgement has not arrived yet, stream: {stream}, topic: {topic}, partition_id: {partition_id}. The frames are queued and delivery continues in the background, so this is lag rather than a failed write, but it is only durable across a restart when the connect string sets `sf_dir`: {error}"
            );
            Ok(())
        }
        Err(error) => Err(FlushError::awaiting(error)),
    }
}

/// Re-emits a QuestDB server rejection through `tracing`.
///
/// The client's own reporting goes to `log`, which the connectors runtime does
/// not bridge, so without this the whole category is invisible: a terminal
/// rejection reaches the sink as an error code with none of this detail
/// attached, and a retriable one is never surfaced at all even though it tells
/// an operator the server is pushing back.
fn report_server_rejection(id: u32, rejections: &Arc<AtomicU64>, error: &QwpWsSenderError) {
    let status = error
        .status
        .map_or_else(|| "none".to_owned(), |status| format!("0x{status:02x}"));
    let sequence = error
        .message_sequence
        .map_or_else(|| "none".to_owned(), |sequence| sequence.to_string());
    let message = error.message.as_deref().unwrap_or("");
    let terminal = error.applied_policy == QwpWsErrorPolicy::Terminal;
    if terminal {
        // The handler runs on the client's own thread and is the only report of
        // a rejection the sink never asks `wait` about, which happens whenever
        // the ack level and the timeout make the sink skip the wait. Record it
        // so the batch can fail rather than reporting success.
        rejections.fetch_add(1, Ordering::Relaxed);
        error!(
            "{CONNECTOR_NAME} ID: {id} QuestDB rejected a batch terminally, category: {:?}, status: {status}, frames: [{}, {}], sequence: {sequence}, message: {message}",
            error.category, error.from_fsn, error.to_fsn
        );
    } else {
        // The client replays these itself, so they are back-pressure rather
        // than loss. Still worth seeing: a stream of them is the only warning
        // before the queue stops draining.
        warn!(
            "{CONNECTOR_NAME} ID: {id} QuestDB rejected a batch and the client will retry it, category: {:?}, policy: {:?}, status: {status}, frames: [{}, {}], sequence: {sequence}, message: {message}",
            error.category, error.applied_policy, error.from_fsn, error.to_fsn
        );
    }
}

/// A `wait` that timed out without the server advancing its watermark.
///
/// The frames are already in the publication log, and the client keeps
/// delivering them in the background, so this is delivery lag rather than a
/// failure: the rows arrive once the server catches up or a reconnect
/// completes. Re-flushing would duplicate them, and reporting it as a failure
/// tells an operator they lost data they did not lose. Only `FailoverRetry`
/// carries this meaning in the awaiting phase; a schema, parse or security
/// rejection there is a genuine terminal error.
fn is_pending_ack(error: &questdb::Error) -> bool {
    error.code() == ErrorCode::FailoverRetry
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
/// The runtime logs a failed batch and counts it in `iggy_connector_errors_total`
/// but does not replay it (#2927), so this classification shapes reporting
/// today. It becomes load-bearing once failed batches are retried.
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
            // A full symbol dictionary belongs to one connection. The frame is
            // refused before any byte reaches the wire and the buffer is rolled
            // back, and returning the sender retires that connection, so the
            // next batch borrows a fresh one and succeeds.
            | ErrorCode::SymbolDictFull
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
            integer_columns: None,
            include_stream_column: None,
            include_topic_column: None,
            include_partition_column: None,
            include_offset_column: None,
            include_headers: None,
            ack_level: None,
            flush_timeout: None,
            batch_size: None,
            max_flush_bytes: None,
            log_rejected_payload: None,
            numbers_as_double: None,
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

    #[test]
    fn given_durable_acks_with_a_zero_flush_timeout_when_constructed_should_record_init_error() {
        // The client reads a zero timeout as "no deadline", so a durable ack
        // level would wait for good inside the FFI call and the connector could
        // not be stopped.
        let mut config = config();
        config.ack_level = Some("durable".to_owned());
        config.flush_timeout = Some("0s".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.init_error,
            Some(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_zero_flush_timeout_at_the_default_ack_level_when_constructed_should_be_accepted() {
        // At the default ack level a zero timeout is fire and forget, which is a
        // legitimate choice, so the guard above must not reject it.
        let mut config = config();
        config.flush_timeout = Some("0s".to_owned());
        let sink = QuestDbSink::new(1, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.flush_timeout, Duration::ZERO);
    }

    #[test]
    fn given_rejections_spread_over_chunks_when_deciding_should_cap_across_the_batch() {
        // The budget spans the runtime batch, so a later chunk inherits what the
        // earlier ones spent rather than starting over.
        assert!(should_log_rejection(0, 1));
        assert!(should_log_rejection(0, MAX_LOGGED_REJECTIONS_PER_BATCH));
        assert!(!should_log_rejection(
            0,
            MAX_LOGGED_REJECTIONS_PER_BATCH + 1
        ));
        assert!(should_log_rejection(MAX_LOGGED_REJECTIONS_PER_BATCH - 1, 1));
        assert!(!should_log_rejection(
            MAX_LOGGED_REJECTIONS_PER_BATCH - 1,
            2
        ));
        assert!(!should_log_rejection(MAX_LOGGED_REJECTIONS_PER_BATCH, 1));
    }

    #[test]
    fn given_a_run_of_unacknowledged_batches_when_deciding_should_escalate_at_the_threshold() {
        assert!(!should_escalate_stall(0));
        assert!(!should_escalate_stall(
            PENDING_ACK_BATCHES_BEFORE_ESCALATION - 1
        ));
        assert!(should_escalate_stall(PENDING_ACK_BATCHES_BEFORE_ESCALATION));
        assert!(should_escalate_stall(
            PENDING_ACK_BATCHES_BEFORE_ESCALATION + 1
        ));
    }

    #[test]
    fn given_a_short_payload_when_previewing_should_return_it_whole() {
        let preview = payload_preview(&Payload::Text("hello".to_owned()));
        assert_eq!(preview, "hello");
    }

    #[test]
    fn given_a_long_payload_when_previewing_should_truncate_and_report_the_size() {
        let text = "a".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES * 2);
        let preview = payload_preview(&Payload::Text(text.clone()));
        assert!(preview.len() < text.len(), "{preview}");
        assert!(
            preview.ends_with(&format!("({} bytes total)", text.len())),
            "{preview}"
        );
    }

    #[test]
    fn given_a_multibyte_boundary_when_previewing_should_not_split_a_character() {
        // Three bytes per character puts the boundaries on multiples of three.
        // The cut is at 512, which is not one of them, so the walk has to step
        // back rather than slicing a character in half. A two-byte character
        // would not test this: every boundary would be even, and so is the cut.
        assert_ne!(
            REJECTED_PAYLOAD_PREVIEW_BYTES % "€".len(),
            0,
            "the cut has to land inside a character for this test to mean anything"
        );
        let text = "€".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES);
        let preview = payload_preview(&Payload::Text(text));
        assert!(preview.contains('€'));
        assert!(
            preview.starts_with(&"€".repeat(REJECTED_PAYLOAD_PREVIEW_BYTES / "€".len())),
            "the preview keeps every whole character before the cut"
        );
    }

    #[test]
    fn given_a_binary_payload_when_previewing_should_report_the_byte_count_only() {
        let preview = payload_preview(&Payload::Raw(vec![0xff, 0xfe, 0xfd]));
        assert_eq!(preview, "<3 bytes>");
    }

    #[tokio::test]
    async fn given_durable_acks_without_the_connect_string_flag_when_opened_should_fail() {
        // QuestDB refuses `AckLevel::Durable` unless the connect string asked
        // for it, which would otherwise fail every batch while the rows landed
        // anyway, reading to an operator as total data loss.
        let mut config = config();
        config.ack_level = Some("durable".to_owned());
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn given_durable_acks_with_the_connect_string_flag_when_opened_should_pass_validation() {
        // The pairing is accepted; only the connect itself then fails, which
        // proves the guard above is not rejecting a valid configuration.
        let mut config = config();
        config.ack_level = Some("durable".to_owned());
        config.connection_string = "ws::addr=127.0.0.1:1;request_durable_ack=on;"
            .to_owned()
            .into();
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
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

    #[tokio::test]
    async fn given_malformed_connection_string_when_opened_should_fail() {
        let mut config = config();
        config.connection_string = SecretString::from("not-a-connect-string");
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[cfg(not(feature = "insecure-skip-verify"))]
    #[tokio::test]
    async fn given_the_tls_bypass_without_the_feature_when_opened_should_fail() {
        // The default build must not accept a connect string that would skip
        // certificate verification, and must say why rather than reporting the
        // client's generic configuration error.
        let mut config = config();
        config.connection_string =
            SecretString::from("wss::addr=localhost:9000;tls_verify=unsafe_off;");
        let mut sink = QuestDbSink::new(1, config);
        let Err(Error::InvalidConfigValue(message)) = sink.open().await else {
            panic!("a default build must refuse tls_verify=unsafe_off");
        };
        assert!(message.contains("insecure-skip-verify"), "{message}");
    }

    #[tokio::test]
    async fn given_unsupported_transport_when_opened_should_fail() {
        // The sink is QWP-only. An ILP connect string parses but must not be
        // accepted, since the QWP-only column types would fail per row later.
        let mut config = config();
        config.connection_string = SecretString::from("http::addr=localhost:9000;");
        let mut sink = QuestDbSink::new(1, config);
        let Err(Error::InitError(message)) = sink.open().await else {
            panic!("an ILP connect string must not be accepted");
        };
        // Without this the test would pass on a failed dial to a free port,
        // which reports the same variant.
        assert!(
            message.contains("QWP/WebSocket"),
            "the error must name the protocol the sink requires: {message}"
        );
    }

    #[tokio::test]
    async fn given_unreachable_server_when_opened_should_fail_fast() {
        // Port 1 has no listener. `open` doubles as the connectivity check, so
        // a dead endpoint must surface at startup rather than at first flush.
        let mut config = config();
        config.connection_string = SecretString::from("ws::addr=127.0.0.1:1;");
        let mut sink = QuestDbSink::new(1, config);
        assert!(matches!(sink.open().await, Err(Error::InitError(_))));
    }

    #[tokio::test]
    async fn given_never_opened_sink_when_consuming_should_report_missing_pool() {
        let sink = QuestDbSink::new(1, config());
        let topic_metadata = TopicMetadata {
            stream: "s".to_owned(),
            topic: "t".to_owned(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Json,
        };
        let result = sink
            .consume(&topic_metadata, messages_metadata, Vec::new())
            .await;
        assert!(matches!(result, Err(Error::InitError(_))));
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
    fn given_minimal_config_when_constructed_should_write_numbers_as_doubles() {
        // The default has to be the one that does not depend on where the flush
        // boundary fell, so a mixed-number producer loses nothing.
        let sink = QuestDbSink::new(1, config());
        assert!(sink.mapping.numbers_as_double);
    }

    #[test]
    fn given_a_column_in_two_type_lists_when_constructed_should_record_init_error() {
        // A column has one type, so overlapping declarations are a config error
        // rather than a per-record failure forever.
        for (first, second) in [
            ("symbol_columns", "uuid_columns"),
            ("symbol_columns", "integer_columns"),
            ("uuid_columns", "integer_columns"),
        ] {
            let mut config = config();
            for name in [first, second] {
                let list = Some(vec!["trade_id".to_owned()]);
                match name {
                    "symbol_columns" => config.symbol_columns = list,
                    "uuid_columns" => config.uuid_columns = list,
                    _ => config.integer_columns = list,
                }
            }
            let sink = QuestDbSink::new(1, config);
            assert!(
                matches!(sink.init_error, Some(Error::InvalidConfigValue(_))),
                "{first} and {second} must not be allowed to overlap"
            );
        }
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
    fn given_no_max_flush_bytes_when_constructed_should_use_the_frame_safe_default() {
        let sink = QuestDbSink::new(1, config());
        assert_eq!(sink.max_flush_bytes, DEFAULT_MAX_FLUSH_BYTES);
    }

    #[test]
    fn given_zero_batch_size_when_constructed_should_fall_back_to_default() {
        // Zero would make the chunking loop drain nothing on every pass, so
        // `consume` would never return and the FFI call would never complete.
        let mut config = config();
        config.batch_size = Some(0);
        let sink = QuestDbSink::new(1, config);
        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE as usize);
    }

    #[test]
    fn given_zero_max_flush_bytes_when_constructed_should_fall_back_to_default() {
        // Zero would flush after every row, or never, depending on how the
        // comparison is read. Neither is useful, so it falls back.
        let mut config = config();
        config.max_flush_bytes = Some(0);
        let sink = QuestDbSink::new(1, config);
        assert_eq!(sink.max_flush_bytes, DEFAULT_MAX_FLUSH_BYTES);
    }

    #[test]
    fn given_explicit_max_flush_bytes_when_constructed_should_honour_it() {
        let mut config = config();
        config.max_flush_bytes = Some(4096);
        let sink = QuestDbSink::new(1, config);
        assert_eq!(sink.max_flush_bytes, 4096);
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
            integer_columns = ["trade_id"]
            numbers_as_double = false
            batch_size = 500
        "#;
        let config: QuestDbSinkConfig = toml::from_str(raw).unwrap();
        let sink = QuestDbSink::new(7, config);
        assert!(sink.init_error.is_none());
        assert_eq!(sink.batch_size, 500);
        assert_eq!(sink.mapping.table, "trades");
        assert!(sink.mapping.symbol_columns.contains("side"));
        assert!(sink.mapping.integer_columns.contains("trade_id"));
        assert!(!sink.mapping.numbers_as_double);
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
    fn given_ack_timeout_when_classified_should_be_pending_not_failed() {
        // The frames are already published and delivery continues, so this
        // must not be reported as a lost batch.
        let error = questdb::Error::new(
            ErrorCode::FailoverRetry,
            "wait(ok) timed out with no ack progress",
        );
        assert!(is_pending_ack(&error));
    }

    #[test]
    fn given_terminal_rejection_when_classified_should_not_be_pending() {
        for code in [
            ErrorCode::ServerSchemaMismatch,
            ErrorCode::ServerSecurityError,
            ErrorCode::ServerParseError,
        ] {
            let error = questdb::Error::new(code, "rejected");
            assert!(!is_pending_ack(&error), "{code:?} must stay terminal");
        }
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
    fn given_server_rejection_when_mapped_should_be_permanent() {
        // A schema rejection reaches a sender as `ServerRejection`, not as the
        // query path's `ServerSchemaMismatch`, so there is nothing to map it to
        // `Error::SchemaMismatch` from and every rejection is permanent.
        let sink = QuestDbSink::new(1, config());
        for code in [ErrorCode::ServerRejection, ErrorCode::ServerSchemaMismatch] {
            let failure = FlushError::publishing(questdb::Error::new(code, "rejected"));
            assert!(
                matches!(sink.map_client_error(failure), Error::PermanentHttpError(_)),
                "{code:?} should map to a permanent error"
            );
        }
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
