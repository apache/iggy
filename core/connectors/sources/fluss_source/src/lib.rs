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
use fluss::metadata::{DataField, TablePath};
use fluss::rpc::message::OffsetSpec;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

source_connector!(FlussSource);

const CONNECTOR_NAME: &str = "Apache Fluss source";
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_TABLE_TYPE: &str = "log";
const JSON_PAYLOAD_FORMAT: &str = "json";
const METADATA_BUCKET: &str = "_fluss_bucket";
const METADATA_OFFSET: &str = "_fluss_offset";
const METADATA_TIMESTAMP: &str = "_fluss_timestamp";
const NANOS_PER_MILLI: u64 = 1_000_000;

#[derive(Debug, Serialize, Deserialize)]
pub struct FlussSourceConfig {
    pub bootstrap_servers: String,
    pub database: String,
    pub table: String,
    /// Only `log` is accepted today. Primary-key changelog scanning is not in the released
    /// `fluss-rs`, so the value is validated rather than silently ignored.
    pub table_type: Option<String>,
    /// `earliest` (default), `latest`, or an explicit numeric offset applied to every bucket.
    pub starting_offset: Option<String>,
    /// Column projection pushed down to the server. Omit to read every column.
    pub columns: Option<Vec<String>>,
    pub poll_interval: Option<String>,
    pub poll_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Only `json` is accepted today. `arrow_ipc` needs the batch scanner and a different
    /// offset-tracking path, so it is rejected rather than quietly downgraded.
    pub payload_format: Option<String>,
    pub include_metadata: Option<bool>,
    pub sasl_username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub sasl_password: Option<SecretString>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    /// Next offset to read per bucket. Absent buckets fall back to the configured start.
    bucket_offsets: HashMap<i32, i64>,
    messages_produced: u64,
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
            other => other.parse::<i64>().map(StartingOffset::Explicit).map_err(|_| {
                Error::InitError(format!(
                    "invalid starting_offset '{other}' for {CONNECTOR_NAME}, expected 'earliest', 'latest' or a number"
                ))
            }),
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
        let table_type = self.config.table_type.as_deref().unwrap_or(LOG_TABLE_TYPE);
        if table_type != LOG_TABLE_TYPE {
            return Err(Error::InitError(format!(
                "{CONNECTOR_NAME} supports only table_type '{LOG_TABLE_TYPE}', got '{table_type}'. \
                 Primary-key changelog scanning is not available in the released fluss-rs"
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

        self.config
            .starting_offset
            .as_deref()
            .unwrap_or("earliest")
            .parse()
    }

    /// Buckets already present in the restored state keep their offset. everything else
    /// starts at the given default, so a widened bucket count does not rewind buckets
    /// that were already consumed.
    fn resolve_start_offsets(
        bucket_count: i32,
        start_offset: i64,
        tracked: &HashMap<i32, i64>,
    ) -> HashMap<i32, i64> {
        let mut offsets = tracked.clone();
        for bucket in 0..bucket_count {
            offsets.entry(bucket).or_insert(start_offset);
        }
        offsets
    }

    fn build_message(
        &self,
        bucket: i32,
        offset: i64,
        timestamp_millis: i64,
        mut record: serde_json::Map<String, Value>,
    ) -> Result<ProducedMessage, Error> {
        if self.include_metadata {
            record.insert(METADATA_BUCKET.to_owned(), Value::from(bucket));
            record.insert(METADATA_OFFSET.to_owned(), Value::from(offset));
            record.insert(METADATA_TIMESTAMP.to_owned(), Value::from(timestamp_millis));
        }

        let payload = simd_json::to_vec(&Value::Object(record)).map_err(|error| {
            Error::Serialization(format!(
                "failed to serialize Apache Fluss row at bucket {bucket}, offset {offset}: {error}"
            ))
        })?;

        Ok(ProducedMessage {
            id: Some(message_id(bucket, offset)),
            headers: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: origin_timestamp_nanos(timestamp_millis),
            payload,
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

        let admin = connection.get_admin().map_err(connection_error)?;
        let table_info = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(connection_error)?;

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

        let row_type = match self.config.columns.as_ref() {
            Some(columns) => table_info
                .get_row_type()
                .project_with_field_names(columns)
                .map_err(|error| {
                    Error::InitError(format!(
                        "invalid columns projection {columns:?} for table '{}': {error}",
                        self.table_path
                    ))
                })?,
            None => table_info.get_row_type().clone(),
        };
        mapping::ensure_supported_types(row_type.fields())?;

        let bucket_count = table_info.get_num_buckets();
        let tracked = { self.state.lock().await.bucket_offsets.clone() };
        let offsets = match start {
            StartingOffset::Earliest => {
                Self::resolve_start_offsets(bucket_count, EARLIEST_OFFSET, &tracked)
            }
            StartingOffset::Explicit(offset) => {
                Self::resolve_start_offsets(bucket_count, offset, &tracked)
            }
            StartingOffset::Latest => {
                let missing: Vec<i32> = (0..bucket_count)
                    .filter(|bucket| !tracked.contains_key(bucket))
                    .collect();
                let mut offsets = tracked.clone();
                if !missing.is_empty() {
                    let tails = admin
                        .list_offsets(&self.table_path, &missing, OffsetSpec::Latest)
                        .await
                        .map_err(connection_error)?;
                    for bucket in missing {
                        let tail = tails.get(&bucket).copied().ok_or_else(|| {
                            Error::InitError(format!(
                                "Apache Fluss returned no latest offset for bucket {bucket} of table '{}'",
                                self.table_path
                            ))
                        })?;
                        offsets.insert(bucket, tail);
                    }
                }
                offsets
            }
        };

        let scanner = {
            let table = connection
                .get_table(&self.table_path)
                .await
                .map_err(connection_error)?;
            let scan = match self.config.columns.as_ref() {
                Some(columns) => {
                    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
                    table.new_scan().project_by_name(&names).map_err(|error| {
                        Error::InitError(format!("failed to project columns: {error}"))
                    })?
                }
                None => table.new_scan(),
            };
            scan.create_log_scanner().map_err(|error| {
                Error::InitError(format!("failed to create log scanner: {error}"))
            })?
        };
        scanner
            .subscribe_buckets(&offsets)
            .await
            .map_err(connection_error)?;

        {
            let mut state = self.state.lock().await;
            state.bucket_offsets = offsets;
        }

        self.fields = row_type.fields().clone();
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

        let mut messages = Vec::with_capacity(records.count());
        let mut latest_offsets: HashMap<i32, i64> = HashMap::new();
        for (bucket, bucket_records) in records.records_by_buckets() {
            let bucket_id = bucket.bucket_id();
            for record in bucket_records {
                let row = mapping::row_to_json(record.row(), &self.fields)?;
                messages.push(self.build_message(
                    bucket_id,
                    record.offset(),
                    record.timestamp(),
                    row,
                )?);
                latest_offsets.insert(bucket_id, record.offset() + 1);
            }
        }

        let produced = messages.len();
        let candidate_state = {
            let state = self.state.lock().await;
            let mut bucket_offsets = state.bucket_offsets.clone();
            bucket_offsets.extend(latest_offsets);
            State {
                bucket_offsets,
                messages_produced: state.messages_produced + produced as u64,
            }
        };
        let persisted_state = self.serialize_state(&candidate_state);
        *self.pending_state.lock().await = Some(candidate_state);

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
            state: persisted_state,
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate_state = self.pending_state.lock().await.take();
        match result {
            SourceBatchResult::Ack => {
                if let Some(candidate_state) = candidate_state {
                    *self.state.lock().await = candidate_state;
                }
                Ok(())
            }
            SourceBatchResult::Nack => {
                let Some(scanner) = self.scanner.as_ref() else {
                    return Ok(());
                };
                let committed = { self.state.lock().await.bucket_offsets.clone() };
                if committed.is_empty() {
                    return Ok(());
                }
                // The scanner advanced past the rejected records internally, so rewind it to
                // the acknowledged offsets or they would be skipped until a restart. Fetches
                // buffered from the old position are discarded by the scanner's own
                // expected-offset check.
                warn!(
                    "Rewinding {CONNECTOR_NAME} connector with ID: {} to the last acknowledged \
                     offsets across {} buckets after a rejected batch",
                    self.id,
                    committed.len()
                );
                scanner
                    .subscribe_buckets(&committed)
                    .await
                    .map_err(|error| {
                        Error::Connection(format!(
                            "failed to rewind Apache Fluss scanner after a rejected batch: {error}"
                        ))
                    })
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // fluss-rs 0.1.0 exposes no explicit connection shutdown, so dropping is the only
        // way to release the client.
        self.scanner = None;
        self.connection = None;
        let state = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector with ID: {}, total messages produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

/// Buckets are per-table and offsets are per-bucket, so the pair is unique within a table and
/// stable across restarts. Apache Iggy can dedupe on it after an at-least-once replay.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
