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

use async_trait::async_trait;
use fluss::client::{EARLIEST_OFFSET, FlussConnection, LogScanner};
use fluss::config::Config;
use fluss::metadata::{DataField, RowType, TablePath};
use fluss::record::ScanRecords;
use fluss::row::InternalRow;
use fluss::rpc::message::OffsetSpec;
use iggy_connector_sdk::retry::{RetryPolicy, retry_async};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

source_connector!(FlussSource);

const CONNECTOR_NAME: &str = "Apache Fluss source";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_TABLE_TYPE: &str = "log";
const JSON_PAYLOAD_FORMAT: &str = "json";
const METADATA_BUCKET: &str = "_fluss_bucket";
const METADATA_OFFSET: &str = "_fluss_offset";
const METADATA_TIMESTAMP: &str = "_fluss_timestamp";
const METADATA_PREFIX: &str = "_fluss_";
const NANOS_PER_MILLI: u64 = 1_000_000;
/// A rewind is local scanner bookkeeping unless the client has to refresh the table's
/// metadata first. An error from `on_batch_result` stops the source, so that refresh gets a
/// few attempts before the failure is reported.
const REWIND_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 3,
    base_delay: Duration::from_millis(200),
    max_delay: Duration::from_secs(1),
};

/// Deliberately not `Serialize`. The runtime hands plugin configuration over as raw JSON and
/// never serializes this struct, so leaving it out keeps `sasl_password` unserializable.
#[derive(Debug, Deserialize)]
pub struct FlussSourceConfig {
    pub bootstrap_servers: String,
    pub database: String,
    pub table: String,
    /// Only `log` is accepted today. A primary-key table streams a changelog whose change
    /// types need their own mapping onto messages, so the value is validated rather than
    /// silently ignored.
    pub table_type: Option<String>,
    /// `earliest` (default), `latest`, or an explicit numeric offset applied to every bucket.
    pub starting_offset: Option<String>,
    /// Column projection pushed down to the server. Omit to read every column. An empty list or
    /// a repeated name is rejected.
    pub columns: Option<Vec<String>>,
    pub poll_interval: Option<String>,
    pub poll_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Only `json` is accepted today. `arrow_ipc` needs the batch scanner and a different
    /// offset-tracking path, so it is rejected rather than quietly downgraded.
    pub payload_format: Option<String>,
    /// Adds the bucket, offset and timestamp of each row under the `_fluss_` prefix, so a column
    /// under that prefix is rejected while it is on.
    pub include_metadata: Option<bool>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<SecretString>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    /// Next offset to read per bucket. Absent buckets fall back to the configured start.
    bucket_offsets: HashMap<i32, i64>,
    messages_produced: u64,
    /// Unconvertible rows this run dropped, reported when the connector closes. It moves with
    /// the offsets on ACK, so rows read again after a NACK are not counted twice, and it is
    /// not persisted.
    #[serde(skip)]
    rows_skipped: u64,
}

#[derive(Debug, Clone, Copy)]
enum StartingOffset {
    Earliest,
    Latest,
    Explicit(i64),
}

impl FromStr for StartingOffset {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "earliest" => Ok(StartingOffset::Earliest),
            "latest" => Ok(StartingOffset::Latest),
            other => match other.parse::<i64>() {
                Ok(offset) if offset >= 0 => Ok(StartingOffset::Explicit(offset)),
                _ => Err(Error::InitError(format!(
                    "invalid starting_offset '{other}' for {CONNECTOR_NAME}, expected 'earliest', \
                     'latest' or a non-negative offset"
                ))),
            },
        }
    }
}

pub struct FlussSource {
    id: u32,
    config: FlussSourceConfig,
    table_path: TablePath,
    poll_interval: Duration,
    poll_timeout: Duration,
    include_metadata: bool,
    verbose_logging: bool,
    connection: Option<FlussConnection>,
    scanner: Option<LogScanner>,
    fields: Vec<DataField>,
    state: Mutex<State>,
    pending_state: Mutex<Option<State>>,
    /// Set when `open()` resolved start offsets that are not on disk yet. The next poll carries
    /// them through the ACK handshake even without rows, so a restart before the first row does
    /// not resolve `latest` again and skip what was written in between.
    start_offsets_unsaved: AtomicBool,
}

/// `FlussConnection` and `LogScanner` do not implement `Debug`, so the derive is replaced by
/// a hand-written one that reports connection presence instead of client internals.
impl std::fmt::Debug for FlussSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlussSource")
            .field("id", &self.id)
            .field("table_path", &self.table_path)
            .field("poll_interval", &self.poll_interval)
            .field("poll_timeout", &self.poll_timeout)
            .field("include_metadata", &self.include_metadata)
            .field("columns", &self.fields.len())
            .field("opened", &self.scanner.is_some())
            .finish_non_exhaustive()
    }
}

impl FlussSource {
    pub fn new(id: u32, config: FlussSourceConfig, state: Option<ConnectorState>) -> Self {
        let poll_interval = parse_duration(
            config.poll_interval.as_deref(),
            DEFAULT_POLL_INTERVAL,
            "poll_interval",
            id,
        );
        let poll_timeout = parse_duration(
            config.poll_timeout.as_deref(),
            DEFAULT_POLL_TIMEOUT,
            "poll_timeout",
            id,
        );
        let include_metadata = config.include_metadata.unwrap_or(false);
        let verbose_logging = config.verbose_logging.unwrap_or(false);
        let table_path = TablePath::new(config.database.clone(), config.table.clone());

        let restored_state = state
            .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|state| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Buckets tracked: {}, messages produced: {}",
                    state.bucket_offsets.len(),
                    state.messages_produced
                );
            });

        FlussSource {
            id,
            config,
            table_path,
            poll_interval,
            poll_timeout,
            include_metadata,
            verbose_logging,
            connection: None,
            scanner: None,
            fields: Vec::new(),
            state: Mutex::new(restored_state.unwrap_or_default()),
            pending_state: Mutex::new(None),
            start_offsets_unsaved: AtomicBool::new(false),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn client_config(&self) -> Config {
        let mut config = Config {
            bootstrap_servers: self.config.bootstrap_servers.clone(),
            ..Config::default()
        };
        if let Some(batch_size) = self.config.batch_size {
            config.scanner_log_max_poll_records = batch_size as usize;
        }
        if let (Some(username), Some(password)) =
            (&self.config.sasl_username, &self.config.sasl_password)
        {
            config.security_protocol = "sasl".to_owned();
            config.security_sasl_mechanism = "PLAIN".to_owned();
            config.security_sasl_username = username.clone();
            config.security_sasl_password = password.expose_secret().to_owned();
        }
        config
    }

    fn validate_config(&self) -> Result<StartingOffset, Error> {
        for (field, value) in [
            ("bootstrap_servers", &self.config.bootstrap_servers),
            ("database", &self.config.database),
            ("table", &self.config.table),
        ] {
            if value.trim().is_empty() {
                return Err(Error::InitError(format!(
                    "{field} for {CONNECTOR_NAME} must not be empty"
                )));
            }
        }

        let table_type = self.config.table_type.as_deref().unwrap_or(LOG_TABLE_TYPE);
        if table_type != LOG_TABLE_TYPE {
            return Err(Error::InitError(format!(
                "{CONNECTOR_NAME} supports only table_type '{LOG_TABLE_TYPE}', got '{table_type}'. \
                 Primary-key changelog tables are not supported yet"
            )));
        }

        let payload_format = self
            .config
            .payload_format
            .as_deref()
            .unwrap_or(JSON_PAYLOAD_FORMAT);
        if payload_format != JSON_PAYLOAD_FORMAT {
            return Err(Error::InitError(format!(
                "{CONNECTOR_NAME} supports only payload_format '{JSON_PAYLOAD_FORMAT}', got '{payload_format}'"
            )));
        }

        if let Some(columns) = &self.config.columns {
            if columns.is_empty() {
                return Err(Error::InitError(format!(
                    "columns for {CONNECTOR_NAME} must name at least one column, or be left out to \
                     read every column"
                )));
            }
            let mut seen = HashSet::with_capacity(columns.len());
            if let Some(duplicate) = columns.iter().find(|column| !seen.insert(column.as_str())) {
                return Err(Error::InitError(format!(
                    "column '{duplicate}' is listed more than once in columns for {CONNECTOR_NAME}"
                )));
            }
        }

        // The client accepts a zero limit, which makes every poll come back empty.
        if self.config.batch_size == Some(0) {
            return Err(Error::InitError(format!(
                "batch_size for {CONNECTOR_NAME} must be greater than 0"
            )));
        }
        // One without the other would connect without SASL and only fail at the server.
        if self.config.sasl_username.is_some() != self.config.sasl_password.is_some() {
            return Err(Error::InitError(format!(
                "sasl_username and sasl_password for {CONNECTOR_NAME} must be set together"
            )));
        }
        // With neither a pause before the poll nor a server-side wait, an idle table would be
        // polled in a tight loop.
        if self.poll_interval.is_zero() && self.poll_timeout.is_zero() {
            return Err(Error::InitError(format!(
                "poll_interval and poll_timeout for {CONNECTOR_NAME} cannot both be zero"
            )));
        }

        self.config
            .starting_offset
            .as_deref()
            .unwrap_or("earliest")
            .parse()
    }

    /// Only the table's current buckets are subscribed. A bucket already present in the restored
    /// state keeps its offset and every other one starts at the given default, so a widened
    /// bucket count does not rewind buckets that were already consumed, and an offset saved for
    /// a bucket the table no longer has is dropped.
    fn resolve_start_offsets(
        bucket_count: i32,
        start_offset: i64,
        tracked: &HashMap<i32, i64>,
    ) -> HashMap<i32, i64> {
        (0..bucket_count)
            .map(|bucket| {
                (
                    bucket,
                    tracked.get(&bucket).copied().unwrap_or(start_offset),
                )
            })
            .collect()
    }

    /// Same rule as [`Self::resolve_start_offsets`], with each untracked bucket starting at its
    /// current tail.
    async fn resolve_latest_offsets(
        &self,
        connection: &FlussConnection,
        bucket_count: i32,
        tracked: &HashMap<i32, i64>,
    ) -> Result<HashMap<i32, i64>, Error> {
        let missing: Vec<i32> = (0..bucket_count)
            .filter(|bucket| !tracked.contains_key(bucket))
            .collect();
        let tails = if missing.is_empty() {
            HashMap::new()
        } else {
            let admin = connection.get_admin().map_err(connection_error)?;
            admin
                .list_offsets(&self.table_path, &missing, OffsetSpec::Latest)
                .await
                .map_err(connection_error)?
        };
        (0..bucket_count)
            .map(|bucket| {
                let offset = match tracked.get(&bucket) {
                    Some(offset) => *offset,
                    None => tails.get(&bucket).copied().ok_or_else(|| {
                        Error::InitError(format!(
                            "Apache Fluss returned no latest offset for bucket {bucket} of table '{}'",
                            self.table_path
                        ))
                    })?,
                };
                Ok((bucket, offset))
            })
            .collect()
    }

    /// A row that cannot be converted fails the same way on every read, so failing the batch
    /// would read it again forever. That includes a failed column read, since the scanner hands
    /// rows over already decoded in memory. Such a row is dropped and logged with its position
    /// instead, and the offsets still move past it.
    async fn build_batch(
        &self,
        records: &ScanRecords,
    ) -> Result<(Vec<ProducedMessage>, Option<ConnectorState>), Error> {
        let buckets = records.records_by_buckets();
        let mut messages = Vec::with_capacity(records.count());
        let mut polled_offsets = HashMap::with_capacity(buckets.len());
        let mut skipped = 0;
        for (bucket, bucket_records) in buckets {
            let bucket_id = bucket.bucket_id();
            for record in bucket_records {
                match self.build_message(
                    bucket_id,
                    record.offset(),
                    record.timestamp(),
                    record.row(),
                ) {
                    Ok(message) => messages.push(message),
                    Err(error) => {
                        skipped += 1;
                        error!(
                            "{CONNECTOR_NAME} connector with ID: {} skipped the row at bucket \
                             {bucket_id}, offset {}, which it cannot convert: {error}",
                            self.id,
                            record.offset()
                        );
                    }
                }
            }
            if let Some(last) = bucket_records.last() {
                polled_offsets.insert(bucket_id, last.offset() + 1);
            }
        }

        let state = self
            .stage_batch_state(polled_offsets, messages.len(), skipped)
            .await?;
        Ok((messages, state))
    }

    fn build_message(
        &self,
        bucket: i32,
        offset: i64,
        timestamp_millis: i64,
        row: &dyn InternalRow,
    ) -> Result<ProducedMessage, Error> {
        let record_metadata = [
            (METADATA_BUCKET, i64::from(bucket)),
            (METADATA_OFFSET, offset),
            (METADATA_TIMESTAMP, timestamp_millis),
        ];
        let metadata: &[(&str, i64)] = if self.include_metadata {
            &record_metadata
        } else {
            &[]
        };

        let payload = serde_json::to_vec(&mapping::JsonRow::new(row, &self.fields, metadata))
            .map_err(|error| Error::InvalidRecordValue(error.to_string()))?;

        Ok(ProducedMessage {
            id: Some(message_id(bucket, offset)),
            headers: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: origin_timestamp_nanos(timestamp_millis),
            payload,
        })
    }

    /// Stages the state a polled batch would leave behind until the runtime reports its
    /// result. A poll that read no rows changes no offset, so it carries no state unless the
    /// start offsets from `open()` still have to reach disk. A poll whose rows were all skipped
    /// still moves the offsets past them.
    async fn stage_batch_state(
        &self,
        polled_offsets: HashMap<i32, i64>,
        produced: usize,
        skipped: usize,
    ) -> Result<Option<ConnectorState>, Error> {
        if polled_offsets.is_empty() && !self.start_offsets_unsaved.load(Ordering::Acquire) {
            return Ok(None);
        }

        let mut candidate = self.state.lock().await.clone();
        candidate.bucket_offsets.extend(polled_offsets);
        candidate.messages_produced += produced as u64;
        candidate.rows_skipped += skipped as u64;
        let persisted = self.serialize_state(&candidate).ok_or_else(|| {
            Error::Serialization(format!(
                "failed to serialize state for {CONNECTOR_NAME} connector with ID: {}",
                self.id
            ))
        })?;
        *self.pending_state.lock().await = Some(candidate);
        Ok(Some(persisted))
    }

    /// The scanner moves past records as soon as it returns them, so a batch that is not
    /// acknowledged is only read again once the scanner points back at the committed offsets.
    /// A fetch still buffered from the old position is dropped by the scanner's own
    /// expected-offset check.
    async fn rewind(&self, scanner: &LogScanner) -> Result<(), Error> {
        let committed = { self.state.lock().await.bucket_offsets.clone() };
        let context = format!("{CONNECTOR_NAME} connector with ID: {} rewind", self.id);
        retry_async(
            REWIND_RETRY,
            &context,
            fluss::error::Error::is_retriable,
            || scanner.subscribe_buckets(&committed),
        )
        .await
        .map_err(|failure| {
            Error::Connection(format!(
                "failed to rewind the Apache Fluss scanner to the last acknowledged offsets: \
                 {failure}"
            ))
        })
    }
}

#[async_trait]
impl Source for FlussSource {
    async fn open(&mut self) -> Result<(), Error> {
        let start = self.validate_config()?;

        let connection = FlussConnection::new(self.client_config())
            .await
            .map_err(connection_error)?;

        // The scanner aligns every row to the schema it was created with, so the columns used to
        // decode rows come from the same table snapshot rather than from a second lookup that a
        // schema change could land between.
        let (scanner, fields, bucket_count) = {
            let table = connection
                .get_table(&self.table_path)
                .await
                .map_err(connection_error)?;
            let table_info = table.get_table_info();

            if table_info.has_primary_key() {
                return Err(Error::InitError(format!(
                    "table '{}' is a primary-key table. {CONNECTOR_NAME} supports log tables only",
                    self.table_path
                )));
            }
            if table_info.is_partitioned() {
                return Err(Error::InitError(format!(
                    "table '{}' is partitioned, which {CONNECTOR_NAME} does not support yet",
                    self.table_path
                )));
            }

            let table_row_type = table_info.get_row_type();
            let projection = self
                .config
                .columns
                .as_deref()
                .map(|columns| projection_indices(table_row_type, columns, &self.table_path))
                .transpose()?;
            let projection_error = |error: fluss::error::Error| {
                Error::InitError(format!(
                    "failed to project the columns of table '{}': {error}",
                    self.table_path
                ))
            };
            let row_type = match projection.as_deref() {
                Some(indices) => table_row_type.project(indices).map_err(projection_error)?,
                None => table_row_type.clone(),
            };
            mapping::ensure_supported_types(row_type.fields())?;
            if self.include_metadata {
                ensure_no_metadata_collision(row_type.fields())?;
            }

            let scan = match projection.as_deref() {
                Some(indices) => table
                    .new_scan()
                    .project(indices)
                    .map_err(projection_error)?,
                None => table.new_scan(),
            };
            let scanner = scan.create_log_scanner().map_err(|error| {
                Error::InitError(format!("failed to create log scanner: {error}"))
            })?;
            (
                scanner,
                row_type.fields().clone(),
                table_info.get_num_buckets(),
            )
        };

        let restored = { self.state.lock().await.bucket_offsets.clone() };
        let mut stale: Vec<i32> = restored
            .keys()
            .copied()
            .filter(|bucket| !(0..bucket_count).contains(bucket))
            .collect();
        if !stale.is_empty() {
            stale.sort_unstable();
            warn!(
                "{CONNECTOR_NAME} connector with ID: {} ignores the saved offsets of buckets \
                 {stale:?}, which table '{}' no longer has",
                self.id, self.table_path
            );
        }
        let offsets = match start {
            StartingOffset::Earliest => {
                Self::resolve_start_offsets(bucket_count, EARLIEST_OFFSET, &restored)
            }
            StartingOffset::Explicit(offset) => {
                Self::resolve_start_offsets(bucket_count, offset, &restored)
            }
            StartingOffset::Latest => {
                self.resolve_latest_offsets(&connection, bucket_count, &restored)
                    .await?
            }
        };
        scanner
            .subscribe_buckets(&offsets)
            .await
            .map_err(connection_error)?;

        self.start_offsets_unsaved
            .store(offsets != restored, Ordering::Release);
        {
            let mut state = self.state.lock().await;
            state.bucket_offsets = offsets;
        }

        self.fields = fields;
        self.connection = Some(connection);
        self.scanner = Some(scanner);

        info!(
            "Opened {CONNECTOR_NAME} connector with ID: {}, table: {}, buckets: {bucket_count}, \
             columns: {}, poll interval: {:?}",
            self.id,
            self.table_path,
            self.fields.len(),
            self.poll_interval
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.poll_interval).await;

        let Some(scanner) = self.scanner.as_ref() else {
            return Err(Error::InitError(format!(
                "{CONNECTOR_NAME} connector with ID: {} polled before it was opened",
                self.id
            )));
        };

        let records = scanner
            .poll(self.poll_timeout)
            .await
            .map_err(|error| Error::Connection(format!("failed to poll Apache Fluss: {error}")))?;

        let (messages, state) = match self.build_batch(&records).await {
            Ok(batch) => batch,
            Err(error) => {
                // The scanner is already past these records, so they are read again on the next
                // poll rather than skipped along with the error.
                warn!(
                    "Rewinding {CONNECTOR_NAME} connector with ID: {} after a batch it could not \
                     build: {error}",
                    self.id
                );
                self.rewind(scanner).await?;
                return Err(error);
            }
        };

        let produced = messages.len();
        if produced > 0 {
            if self.verbose_logging {
                info!(
                    "{CONNECTOR_NAME} connector with ID: {} produced {produced} messages from table: {}",
                    self.id, self.table_path
                );
            } else {
                debug!(
                    "{CONNECTOR_NAME} connector with ID: {} produced {produced} messages from table: {}",
                    self.id, self.table_path
                );
            }
        }

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state,
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let Some(candidate_state) = self.pending_state.lock().await.take() else {
            return Ok(());
        };
        match result {
            SourceBatchResult::Ack => {
                *self.state.lock().await = candidate_state;
                self.start_offsets_unsaved.store(false, Ordering::Release);
                Ok(())
            }
            SourceBatchResult::Nack => {
                let Some(scanner) = self.scanner.as_ref() else {
                    return Ok(());
                };
                warn!(
                    "Rewinding {CONNECTOR_NAME} connector with ID: {} to the last acknowledged \
                     offsets after a rejected batch",
                    self.id
                );
                self.rewind(scanner).await
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // `FlussConnection::close` only drains the writer client, which a source never creates,
        // so dropping the scanner and the connection is what releases them.
        self.scanner = None;
        self.connection = None;
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector with ID: {}, total messages produced: {}, rows \
             skipped: {}",
            self.id, state.messages_produced, state.rows_skipped
        );
        Ok(())
    }
}

/// Buckets are per-table and offsets are per-bucket, so the pair is unique within a table and
/// stable across restarts. A consumer can use it to spot a record replayed after an
/// at-least-once redelivery. The Apache Iggy server does not dedupe on it.
fn message_id(bucket: i32, offset: i64) -> u128 {
    ((bucket as u32 as u128) << 64) | (offset as u64 as u128)
}

fn origin_timestamp_nanos(timestamp_millis: i64) -> Option<u64> {
    u64::try_from(timestamp_millis)
        .ok()
        .and_then(|millis| millis.checked_mul(NANOS_PER_MILLI))
}

/// `new()` cannot fail (the FFI macro fixes its signature), so an unparsable duration falls
/// back to the default. It is logged at warn so a typo does not silently change the cadence.
fn parse_duration(value: Option<&str>, default: Duration, field: &str, id: u32) -> Duration {
    let Some(raw) = value else {
        return default;
    };
    match humantime::Duration::from_str(raw) {
        Ok(duration) => *duration,
        Err(error) => {
            warn!(
                "Invalid {field} '{raw}' for {CONNECTOR_NAME} connector with ID: {id}, \
                 falling back to {default:?}. {error}"
            );
            default
        }
    }
}

fn connection_error(error: fluss::error::Error) -> Error {
    Error::Connection(format!("Apache Fluss client failure: {error}"))
}

/// Resolves the configured names once, so the row decoder and the scanner use the same
/// positions in the same order.
fn projection_indices(
    row_type: &RowType,
    columns: &[String],
    table_path: &TablePath,
) -> Result<Vec<usize>, Error> {
    columns
        .iter()
        .map(|column| {
            row_type.get_field_index(column).ok_or_else(|| {
                Error::InitError(format!(
                    "column '{column}' from columns does not exist in table '{table_path}'"
                ))
            })
        })
        .collect()
}

/// `include_metadata` writes its fields into the same object as the columns, so a column under
/// the reserved prefix would be overwritten.
fn ensure_no_metadata_collision(fields: &[DataField]) -> Result<(), Error> {
    match fields
        .iter()
        .find(|field| field.name().starts_with(METADATA_PREFIX))
    {
        Some(field) => Err(Error::InitError(format!(
            "column '{}' starts with '{METADATA_PREFIX}', which include_metadata reserves for its \
             own fields. Rename the column, leave it out of columns, or turn include_metadata off",
            field.name()
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::DataTypes;
    use fluss::row::GenericRow;
    use serde_json::Value;

    fn test_config() -> FlussSourceConfig {
        FlussSourceConfig {
            bootstrap_servers: "localhost:9123".to_owned(),
            database: "analytics".to_owned(),
            table: "events".to_owned(),
            table_type: None,
            starting_offset: None,
            columns: None,
            poll_interval: Some("100ms".to_owned()),
            poll_timeout: Some("1s".to_owned()),
            batch_size: Some(500),
            payload_format: None,
            include_metadata: None,
            sasl_username: None,
            sasl_password: None,
            verbose_logging: None,
        }
    }

    fn state_with(offsets: &[(i32, i64)], produced: u64) -> State {
        State {
            bucket_offsets: offsets.iter().copied().collect(),
            messages_produced: produced,
            ..State::default()
        }
    }

    #[test]
    fn given_persisted_state_should_restore_bucket_offsets() {
        let serialized = rmp_serde::to_vec(&state_with(&[(0, 42), (1, 7)], 500))
            .expect("Failed to serialize state");

        let source = FlussSource::new(1, test_config(), Some(ConnectorState(serialized)));

        let runtime = tokio::runtime::Runtime::new().expect("Failed to build runtime");
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.messages_produced, 500);
            assert_eq!(restored.bucket_offsets.get(&0), Some(&42));
            assert_eq!(restored.bucket_offsets.get(&1), Some(&7));
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = FlussSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().expect("Failed to build runtime");
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.messages_produced, 0);
            assert!(state.bucket_offsets.is_empty());
        });
    }

    /// The state is only a per-bucket cursor. An unreadable one falls back to
    /// `starting_offset`, which with the default `earliest` re-reads the table: duplicates that
    /// at-least-once delivery already allows, not lost rows.
    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid = ConnectorState(b"not valid msgpack".to_vec());

        let source = FlussSource::new(1, test_config(), Some(invalid));

        let runtime = tokio::runtime::Runtime::new().expect("Failed to build runtime");
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.messages_produced, 0);
            assert!(state.bucket_offsets.is_empty());
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = state_with(&[(0, 100), (3, 250)], 1000);

        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(original.messages_produced, deserialized.messages_produced);
        assert_eq!(original.bucket_offsets, deserialized.bucket_offsets);
    }

    #[test]
    fn given_default_config_should_accept_log_table_and_earliest_offset() {
        let source = FlussSource::new(1, test_config(), None);

        let start = source
            .validate_config()
            .expect("Default config should be valid");

        assert!(matches!(start, StartingOffset::Earliest));
    }

    #[test]
    fn given_primary_key_table_type_should_be_rejected() {
        let mut config = test_config();
        config.table_type = Some("primary_key".to_owned());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("Primary key tables are not supported yet");

        assert!(matches!(error, Error::InitError(message) if message.contains("primary_key")));
    }

    #[test]
    fn given_arrow_ipc_payload_format_should_be_rejected() {
        let mut config = test_config();
        config.payload_format = Some("arrow_ipc".to_owned());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("arrow_ipc is not supported yet");

        assert!(matches!(error, Error::InitError(message) if message.contains("arrow_ipc")));
    }

    #[test]
    fn given_latest_starting_offset_should_be_parsed() {
        let mut config = test_config();
        config.starting_offset = Some("latest".to_owned());
        let source = FlussSource::new(1, config, None);

        let start = source.validate_config().expect("latest should be accepted");

        assert!(matches!(start, StartingOffset::Latest));
    }

    #[test]
    fn given_explicit_starting_offset_should_be_parsed() {
        let mut config = test_config();
        config.starting_offset = Some("128".to_owned());
        let source = FlussSource::new(1, config, None);

        let start = source
            .validate_config()
            .expect("Explicit offset should parse");

        assert!(matches!(start, StartingOffset::Explicit(128)));
    }

    #[test]
    fn given_unparsable_starting_offset_should_be_rejected() {
        let mut config = test_config();
        config.starting_offset = Some("beginning".to_owned());
        let source = FlussSource::new(1, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_negative_starting_offset_should_be_rejected() {
        let mut config = test_config();
        config.starting_offset = Some("-1".to_owned());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("A negative offset should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("-1")));
    }

    #[test]
    fn given_zero_batch_size_should_be_rejected() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("A zero batch size should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("batch_size")));
    }

    #[test]
    fn given_sasl_username_without_password_should_be_rejected() {
        let mut config = test_config();
        config.sasl_username = Some("user".to_owned());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("Half of the SASL credentials should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("sasl_password")));
    }

    #[test]
    fn given_sasl_password_without_username_should_be_rejected() {
        let mut config = test_config();
        config.sasl_password = Some(SecretString::from("secret"));
        let source = FlussSource::new(1, config, None);

        assert!(source.validate_config().is_err());
    }

    #[test]
    fn given_sasl_credentials_should_build_a_client_config_the_client_accepts() {
        let mut config = test_config();
        config.sasl_username = Some("user".to_owned());
        config.sasl_password = Some(SecretString::from("secret"));
        let source = FlussSource::new(1, config, None);

        let client_config = source.client_config();

        assert!(client_config.is_sasl_enabled());
        assert_eq!(client_config.validate_security(), Ok(()));
        assert_eq!(client_config.security_sasl_username, "user");
        assert_eq!(client_config.scanner_log_max_poll_records, 500);
    }

    #[test]
    fn given_no_sasl_credentials_should_build_a_plaintext_client_config() {
        let source = FlussSource::new(1, test_config(), None);

        assert!(!source.client_config().is_sasl_enabled());
    }

    #[test]
    fn given_tracked_buckets_should_keep_their_offsets_and_fill_the_rest() {
        let tracked = HashMap::from([(0, 42)]);

        let offsets = FlussSource::resolve_start_offsets(3, EARLIEST_OFFSET, &tracked);

        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[&0], 42);
        assert_eq!(offsets[&1], EARLIEST_OFFSET);
        assert_eq!(offsets[&2], EARLIEST_OFFSET);
    }

    #[test]
    fn given_explicit_start_should_apply_to_untracked_buckets_only() {
        let tracked = HashMap::from([(1, 900)]);

        let offsets = FlussSource::resolve_start_offsets(2, 50, &tracked);

        assert_eq!(offsets[&0], 50);
        assert_eq!(offsets[&1], 900);
    }

    #[test]
    fn message_id_should_be_unique_per_bucket_and_offset() {
        assert_ne!(message_id(0, 1), message_id(1, 0));
        assert_ne!(message_id(0, 1), message_id(0, 2));
        assert_eq!(message_id(2, 5), message_id(2, 5));
    }

    #[test]
    fn origin_timestamp_should_convert_milliseconds_to_nanoseconds() {
        assert_eq!(
            origin_timestamp_nanos(1_785_655_133_842),
            Some(1_785_655_133_842_000_000)
        );
        assert_eq!(origin_timestamp_nanos(0), Some(0));
        assert_eq!(origin_timestamp_nanos(-1), None);
    }

    #[test]
    fn given_invalid_duration_should_fall_back_to_default() {
        assert_eq!(
            parse_duration(Some("nonsense"), DEFAULT_POLL_INTERVAL, "poll_interval", 1),
            DEFAULT_POLL_INTERVAL
        );
        assert_eq!(
            parse_duration(None, DEFAULT_POLL_TIMEOUT, "poll_timeout", 1),
            DEFAULT_POLL_TIMEOUT
        );
        assert_eq!(
            parse_duration(Some("250ms"), DEFAULT_POLL_INTERVAL, "poll_interval", 1),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn given_ack_when_batch_is_staged_should_commit_candidate_state() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.pending_state.lock().await = Some(state_with(&[(0, 42)], 42));

            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ACK should be applied");

            let state = source.state.lock().await;
            assert_eq!(state.messages_produced, 42);
            assert_eq!(state.bucket_offsets.get(&0), Some(&42));
            assert!(source.pending_state.lock().await.is_none());
        });
    }

    #[test]
    fn given_nack_when_batch_is_staged_should_keep_committed_state() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.pending_state.lock().await = Some(state_with(&[(0, 42)], 42));

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("NACK should be applied");

            let state = source.state.lock().await;
            assert_eq!(state.messages_produced, 0);
            assert!(state.bucket_offsets.is_empty());
            assert!(source.pending_state.lock().await.is_none());
        });
    }

    #[test]
    fn given_empty_poll_when_start_offsets_are_saved_should_carry_no_state() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.state.lock().await = state_with(&[(0, 42)], 42);

            let state = source
                .stage_batch_state(HashMap::new(), 0, 0)
                .await
                .expect("An empty batch should stage");

            assert!(state.is_none());
            assert!(source.pending_state.lock().await.is_none());
        });
    }

    #[test]
    fn given_empty_poll_when_start_offsets_are_unsaved_should_stage_them() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.state.lock().await = state_with(&[(0, 42)], 0);
            source.start_offsets_unsaved.store(true, Ordering::Release);

            let state = source
                .stage_batch_state(HashMap::new(), 0, 0)
                .await
                .expect("An empty batch should stage");

            let persisted = state
                .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, 1))
                .expect("Unsaved start offsets should ride the empty batch");
            assert_eq!(persisted.bucket_offsets.get(&0), Some(&42));
            assert!(source.pending_state.lock().await.is_some());
        });
    }

    #[test]
    fn given_polled_records_should_stage_advanced_offsets_without_committing_them() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.state.lock().await = state_with(&[(0, 42), (1, 7)], 42);

            let state = source
                .stage_batch_state(HashMap::from([(0, 45)]), 3, 0)
                .await
                .expect("A polled batch should stage");

            let persisted = state
                .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, 1))
                .expect("A batch with rows should carry state");
            assert_eq!(persisted.bucket_offsets.get(&0), Some(&45));
            assert_eq!(persisted.bucket_offsets.get(&1), Some(&7));
            assert_eq!(persisted.messages_produced, 45);

            let committed = source.state.lock().await;
            assert_eq!(committed.bucket_offsets.get(&0), Some(&42));
            assert_eq!(committed.messages_produced, 42);
        });
    }

    #[test]
    fn given_unsaved_start_offsets_should_stay_unsaved_until_a_batch_is_acked() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            source.start_offsets_unsaved.store(true, Ordering::Release);

            *source.pending_state.lock().await = Some(state_with(&[(0, 42)], 0));
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("NACK should be applied");
            assert!(source.start_offsets_unsaved.load(Ordering::Acquire));

            *source.pending_state.lock().await = Some(state_with(&[(0, 42)], 0));
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ACK should be applied");
            assert!(!source.start_offsets_unsaved.load(Ordering::Acquire));
        });
    }

    #[test]
    fn given_skipped_rows_read_again_after_a_nack_should_count_them_once() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            source
                .stage_batch_state(HashMap::from([(0, 3)]), 1, 2)
                .await
                .expect("A batch with skipped rows should stage");
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("NACK should be applied");
            assert_eq!(source.state.lock().await.rows_skipped, 0);

            source
                .stage_batch_state(HashMap::from([(0, 3)]), 1, 2)
                .await
                .expect("The rewound batch should stage again");
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("ACK should be applied");
            assert_eq!(source.state.lock().await.rows_skipped, 2);
        });
    }

    fn field(name: &str) -> DataField {
        DataField::new(name, DataTypes::int(), None)
    }

    fn source_with_id_column(include_metadata: bool) -> (FlussSource, GenericRow<'static>) {
        let mut config = test_config();
        config.include_metadata = Some(include_metadata);
        let mut source = FlussSource::new(1, config, None);
        source.fields = vec![field("id")];
        let mut row = GenericRow::new(1);
        row.set_field(0, 7i32);
        (source, row)
    }

    #[test]
    fn given_empty_connection_setting_should_be_rejected() {
        let mut blank_servers = test_config();
        blank_servers.bootstrap_servers.clear();
        let mut blank_database = test_config();
        blank_database.database = "  ".to_owned();
        let mut blank_table = test_config();
        blank_table.table.clear();

        for (setting, config) in [
            ("bootstrap_servers", blank_servers),
            ("database", blank_database),
            ("table", blank_table),
        ] {
            let source = FlussSource::new(1, config, None);

            let error = source
                .validate_config()
                .expect_err("An empty connection setting should be rejected");

            assert!(
                matches!(&error, Error::InitError(message) if message.contains(setting)),
                "{setting}: {error:?}"
            );
        }
    }

    #[test]
    fn given_empty_columns_should_be_rejected() {
        let mut config = test_config();
        config.columns = Some(Vec::new());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("An empty projection should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("columns")));
    }

    #[test]
    fn given_repeated_column_should_be_rejected() {
        let mut config = test_config();
        config.columns = Some(vec!["id".to_owned(), "payload".to_owned(), "id".to_owned()]);
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("A repeated column should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("'id'")));
    }

    #[test]
    fn given_zero_poll_interval_and_timeout_should_be_rejected() {
        let mut config = test_config();
        config.poll_interval = Some("0s".to_owned());
        config.poll_timeout = Some("0s".to_owned());
        let source = FlussSource::new(1, config, None);

        let error = source
            .validate_config()
            .expect_err("A tight poll loop should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("poll_timeout")));
    }

    #[test]
    fn given_zero_poll_interval_with_a_server_wait_should_be_accepted() {
        let mut config = test_config();
        config.poll_interval = Some("0s".to_owned());
        let source = FlussSource::new(1, config, None);

        assert!(source.validate_config().is_ok());
    }

    #[test]
    fn given_saved_offset_for_a_bucket_the_table_no_longer_has_should_drop_it() {
        let tracked = HashMap::from([(0, 42), (5, 9)]);

        let offsets = FlussSource::resolve_start_offsets(2, EARLIEST_OFFSET, &tracked);

        assert_eq!(offsets, HashMap::from([(0, 42), (1, EARLIEST_OFFSET)]));
    }

    #[test]
    fn given_projected_columns_should_resolve_positions_in_the_requested_order() {
        let row_type = RowType::new(vec![field("id"), field("payload"), field("amount")]);
        let columns = vec!["amount".to_owned(), "id".to_owned()];

        let indices = projection_indices(&row_type, &columns, &TablePath::new("db", "events"))
            .expect("Known columns should resolve");

        assert_eq!(indices, vec![2, 0]);
    }

    #[test]
    fn given_unknown_projected_column_should_be_rejected() {
        let row_type = RowType::new(vec![field("id")]);
        let columns = vec!["missing".to_owned()];

        let error = projection_indices(&row_type, &columns, &TablePath::new("db", "events"))
            .expect_err("An unknown column should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("'missing'")));
    }

    #[test]
    fn given_include_metadata_should_add_bucket_offset_and_timestamp() {
        let (source, row) = source_with_id_column(true);

        let message = source
            .build_message(2, 41, 1_700_000_000_123, &row)
            .expect("The row should build");

        let payload: Value =
            serde_json::from_slice(&message.payload).expect("The payload should be JSON");
        assert_eq!(
            payload,
            serde_json::json!({
                "id": 7,
                "_fluss_bucket": 2,
                "_fluss_offset": 41,
                "_fluss_timestamp": 1_700_000_000_123_i64,
            })
        );
        assert_eq!(message.id, Some(message_id(2, 41)));
        assert_eq!(message.origin_timestamp, Some(1_700_000_000_123_000_000));
    }

    #[test]
    fn given_metadata_left_off_should_emit_only_the_columns() {
        let (source, row) = source_with_id_column(false);

        let message = source
            .build_message(2, 41, 1_700_000_000_123, &row)
            .expect("The row should build");

        let payload: Value =
            serde_json::from_slice(&message.payload).expect("The payload should be JSON");
        assert_eq!(payload, serde_json::json!({ "id": 7 }));
    }

    #[test]
    fn given_column_under_the_metadata_prefix_should_be_rejected() {
        let fields = [field("id"), field("_fluss_offset")];

        let error = ensure_no_metadata_collision(&fields)
            .expect_err("A column under the reserved prefix should be rejected");

        assert!(matches!(error, Error::InitError(message) if message.contains("_fluss_offset")));
    }

    #[test]
    fn given_columns_outside_the_metadata_prefix_should_be_accepted() {
        let fields = [field("id"), field("fluss_offset")];

        assert!(ensure_no_metadata_collision(&fields).is_ok());
    }

    #[test]
    fn metadata_fields_should_sit_under_the_reserved_prefix() {
        for name in [METADATA_BUCKET, METADATA_OFFSET, METADATA_TIMESTAMP] {
            assert!(name.starts_with(METADATA_PREFIX), "{name}");
        }
    }

    #[test]
    fn given_polled_rows_that_were_all_skipped_should_still_stage_the_offsets_past_them() {
        let source = FlussSource::new(1, test_config(), None);
        let runtime = tokio::runtime::Runtime::new().expect("failed to create test runtime");
        runtime.block_on(async {
            *source.state.lock().await = state_with(&[(0, 42)], 42);

            let state = source
                .stage_batch_state(HashMap::from([(0, 45)]), 0, 3)
                .await
                .expect("A batch of skipped rows should stage");

            let persisted = state
                .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, 1))
                .expect("Offsets past skipped rows should be staged");
            assert_eq!(persisted.bucket_offsets.get(&0), Some(&45));
            assert_eq!(persisted.messages_produced, 42);
            let pending_skipped = source
                .pending_state
                .lock()
                .await
                .as_ref()
                .map(|pending| pending.rows_skipped);
            assert_eq!(pending_skipped, Some(3));
        });
    }
}
