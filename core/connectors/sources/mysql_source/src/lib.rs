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

use async_trait::async_trait;
use base64::Engine;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use humantime::Duration as HumanDuration;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySqlDatabaseError, MySqlRow};
use sqlx::{Column, MySql, Pool, Row, TypeInfo, mysql::MySqlPoolOptions};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

source_connector!(MySqlSource);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

/// Fixed namespace so a given (table, key) always hashes to the same message id
/// across restarts. This is what makes a replayed row idempotent for downstream
/// dedup, so the value must never change once connectors are in the field.
const MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_u128(0x8f3b1e6a4c9d4f2a9b7c0d1e2f3a4b5c);

/// A disabled table is skipped before anything is logged, so without a reminder the
/// only evidence a connector went dark is the single line written when it happened.
/// At the default 10s poll interval this repeats it every ten minutes.
const DISABLED_TABLES_LOG_INTERVAL: u64 = 60;

/// How many consecutive polls a table may end in `CursorRegression` before the repeat
/// stops reading as transient. Nothing is published in the meantime, so past this it
/// is a stall the operator has to see.
const CURSOR_REGRESSION_ERROR_THRESHOLD: u32 = 3;

#[derive(Debug)]
pub struct MySqlSource {
    pub id: u32,
    pool: Option<Pool<MySql>>,
    config: MySqlSourceConfig,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
    poll_interval: Duration,
    last_batch_full: AtomicBool,
    poisoned_tables: Mutex<HashSet<String>>,
    tracking_kinds: Mutex<HashMap<String, OffsetKind>>,
    polls: AtomicU64,
    cursor_regressions: Mutex<HashMap<String, u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlSourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub tables: Vec<String>,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub tracking_column: Option<String>,
    pub initial_offset: Option<String>,
    pub max_connections: Option<u32>,
    pub custom_query: Option<String>,
    pub snake_case_columns: Option<bool>,
    pub include_metadata: Option<bool>,
    pub delete_after_read: Option<bool>,
    pub processed_column: Option<String>,
    pub primary_key_column: Option<String>,
    pub payload_column: Option<String>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Json,
    Bytea,
    Text,
    JsonDirect,
}

struct ProcessedRow {
    message: ProducedMessage,
    tracking_value: Option<String>,
    row_pk: Option<PkValue>,
}

/// Why a table's batch could not be produced.
enum FetchError {
    Ordering(String),
    /// The batch opened below the cursor it was told to resume from, so the query's
    /// filter and the cursor disagree about what "past the offset" means. Retried
    /// rather than latched, because unlike a within-batch decrease it depends on the
    /// rows as much as on the query: a `custom_query` whose WHERE admits rows the
    /// cursor does not gate (an OR branch is the usual one) only fails while such a
    /// row is there, so it can clear without a config change.
    CursorRegression(String),
    MissingTrackingColumn(String),
    UnusableTrackingValue(String),
    StaleOffsetKind(String),
    Other(Error),
}

impl From<Error> for FetchError {
    fn from(error: Error) -> Self {
        FetchError::Other(error)
    }
}

/// How MySQL ordered a tracking column, which offsets alone cannot reveal: they are
/// carried as strings, so an `INT` 10 and a `VARCHAR` "10" are indistinguishable by
/// content while the server orders them differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OffsetKind {
    Numeric,
    Lexical,
    Unknown,
}

/// Maps an `information_schema.columns.data_type` to the order MySQL sorts it in.
/// `decimal` belongs with the numbers even though `extract_column_value` renders it
/// as a string to preserve precision.
fn offset_kind_for_data_type(data_type: &str) -> OffsetKind {
    const NUMERIC: &[&str] = &[
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "bit",
        "year",
    ];
    if NUMERIC
        .iter()
        .any(|numeric| data_type.eq_ignore_ascii_case(numeric))
    {
        OffsetKind::Numeric
    } else {
        OffsetKind::Lexical
    }
}

/// Whether MySQL orders the column by its collation. The other lexically compared
/// types are the temporal ones, which render fixed-width and so compare the same way
/// as text no matter what the collation is.
fn is_collation_ordered(data_type: &str) -> bool {
    const TEXT: &[&str] = &[
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
    ];
    TEXT.iter().any(|text| data_type.eq_ignore_ascii_case(text))
}

fn offset_kind_for_type_name(type_name: &str) -> OffsetKind {
    match type_name {
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" | "TINYINT UNSIGNED"
        | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED" | "BIGINT UNSIGNED"
        | "FLOAT" | "DOUBLE" | "DECIMAL" | "BIT" | "YEAR" => OffsetKind::Numeric,
        "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT"
        | "MEDIUMTEXT" | "LONGTEXT" => OffsetKind::Lexical,
        _ => OffsetKind::Unknown,
    }
}

fn unusable_tracking_type(data_type: &str) -> Option<&'static str> {
    const BINARY: &[&str] = &[
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
    ];

    if data_type.eq_ignore_ascii_case("json") {
        return Some("it yields no ordered scalar cursor");
    }
    // Judged on `data_type` rather than on the `tinyint(1)` display width that spells
    // BOOLEAN, because that width is not reliably reported: MySQL 8.0.19 and later drop
    // it for unsigned columns while MariaDB keeps it.
    if data_type.eq_ignore_ascii_case("tinyint") {
        return Some(
            "at most 256 distinct values cannot be unique per row, and at display width 1 MySQL \
             spells the type BOOLEAN, which reaches the connector as true/false with no ordered \
             scalar form",
        );
    }
    if BINARY
        .iter()
        .any(|binary| data_type.eq_ignore_ascii_case(binary))
    {
        return Some(
            "binary values are carried as base64 text, which does not order the way the raw \
             bytes do, and that text is what gets compared against the column",
        );
    }
    if data_type.eq_ignore_ascii_case("enum") || data_type.eq_ignore_ascii_case("set") {
        return Some(
            "MySQL orders these by declaration index while only their labels reach the \
             connector, and a bounded set of labels cannot be unique per row either",
        );
    }
    None
}

/// What a `custom_query` may begin with. `TABLE` is MySQL 8.0.19's table statement,
/// and `(` opens a parenthesized query such as `(SELECT ...) UNION (SELECT ...)`.
const READ_STATEMENT_STARTS: &[&str] = &["SELECT", "WITH", "TABLE", "("];

/// The token that decides which statement a query is, skipping leading comments in
/// all three MySQL spellings. Returns a single character when the statement does not
/// open with a word, so a parenthesized query is still recognisable.
fn leading_statement_token(query: &str) -> Option<String> {
    let mut rest = query.trim_start();
    loop {
        rest = if let Some(after) = rest.strip_prefix("/*") {
            after.split_once("*/")?.1.trim_start()
        } else if rest.starts_with("--") || rest.starts_with('#') {
            rest.split_once('\n')?.1.trim_start()
        } else {
            break;
        };
    }

    let word: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if word.is_empty() {
        return rest.chars().next().map(String::from);
    }
    Some(word.to_uppercase())
}

/// The order the tracking column sorts in according to the result set that just
/// came back, or `None` when the query did not project the column under that name.
fn observed_tracking_kind(row: &MySqlRow, tracking_column: &str) -> Option<OffsetKind> {
    row.columns()
        .iter()
        .find(|column| column.name().eq_ignore_ascii_case(tracking_column))
        .map(|column| offset_kind_for_type_name(column.type_info().name()))
        .filter(|kind| *kind != OffsetKind::Unknown)
}

/// `Clone` so the passing case, which is every batch, walks the names without collecting.
fn ensure_tracking_column_projected<'a>(
    projected: impl IntoIterator<Item = &'a str> + Clone,
    tracking_column: &str,
) -> Result<(), FetchError> {
    if projected
        .clone()
        .into_iter()
        .any(|column| column.eq_ignore_ascii_case(tracking_column))
    {
        return Ok(());
    }
    Err(FetchError::MissingTrackingColumn(format!(
        "the result set has no column '{tracking_column}', it returned [{}]. Every row's cursor \
         comes from that column, so the same rows would be re-published on every poll while the \
         stored offset never moved.",
        projected.into_iter().collect::<Vec<_>>().join(", ")
    )))
}

fn unusable_tracking_value(tracking_column: &str, pk: &str) -> FetchError {
    FetchError::UnusableTrackingValue(format!(
        "row (pk {pk}) has no usable value for tracking_column '{tracking_column}': it is NULL, or \
         of a type with no ordered scalar form (tinyint(1), JSON). The cursor is taken from that \
         value, so publishing the row would advance nothing and the same rows would be re-read on \
         every poll."
    ))
}

/// Verifies rows arrive non-decreasing in the tracking column, which is what makes
/// "cursor = last row's value" correct. The built polling query orders ascending by
/// construction, so this only ever fires on a `custom_query`, where the operator
/// owns the `ORDER BY`.
struct OrderingGuard<'a> {
    tracking_column: &'a str,
    kind: OffsetKind,
    last: Option<String>,
    /// True while `last` still holds the cursor carried in from the previous poll
    /// rather than a value from this batch. Distinguishes the two violations, and
    /// keeps an empty batch from re-persisting the offset it started with.
    at_cursor: bool,
}

enum OrderingViolation {
    WithinBatch(String),
    BelowCursor(String),
}

impl<'a> OrderingGuard<'a> {
    fn new(tracking_column: &'a str, kind: OffsetKind, cursor: Option<String>) -> Self {
        OrderingGuard {
            tracking_column,
            kind,
            at_cursor: cursor.is_some(),
            last: cursor,
        }
    }

    fn accept(&mut self, value: String) -> Result<(), OrderingViolation> {
        if let Some(previous) = &self.last
            && offset_decreased(previous, &value, self.kind)
        {
            let column = self.tracking_column;
            return Err(if self.at_cursor {
                OrderingViolation::BelowCursor(format!(
                    "a row arrived below the offset the poll resumed from by tracking_column \
                     '{column}': stored offset '{previous}', first row '{value}'. Taking the \
                     cursor from the last row would move it backwards. Filter the query on \
                     '{column}' with $offset so a batch cannot reach behind its own cursor."
                ))
            } else {
                OrderingViolation::WithinBatch(format!(
                    "rows arrived out of order by tracking_column '{column}': '{previous}' \
                     preceded '{value}'. The cursor is taken from the last row, so it would \
                     move backwards and rows would be skipped permanently. Order the query \
                     ascending by '{column}'."
                ))
            });
        }
        self.last = Some(value);
        self.at_cursor = false;
        Ok(())
    }

    /// `None` when no row was accepted, so an empty batch leaves the stored offset
    /// untouched rather than rewriting the cursor it was seeded with.
    fn into_cursor(self) -> Option<String> {
        if self.at_cursor { None } else { self.last }
    }
}

struct TableBatch {
    table: String,
    messages: Vec<ProducedMessage>,
    processed_ids: Vec<PkValue>,
    max_offset: Option<String>,
}

impl PayloadFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("bytea") | Some("raw") => PayloadFormat::Bytea,
            Some("text") => PayloadFormat::Text,
            Some("json_direct") | Some("jsonb") => PayloadFormat::JsonDirect,
            _ => PayloadFormat::Json,
        }
    }
}

/// A primary-key value in a form the mark/delete `WHERE pk IN (...)` can bind
/// faithfully. `Text` covers ints/strings/dates/decimals (MySQL implicitly
/// converts the bound string to the column type). `Bytes` carries raw binary
/// (BINARY/VARBINARY/BLOB) so a UUID stored as bytes matches the row instead of
/// being compared as its base64 text, which never matches.
enum PkValue {
    Text(String),
    Bytes(Vec<u8>),
}

impl PkValue {
    fn as_key(&self) -> String {
        match self {
            PkValue::Text(text) => text.clone(),
            PkValue::Bytes(bytes) => base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRecord {
    pub table_name: String,
    pub operation_type: String,
    pub timestamp: DateTime<Utc>,
    pub data: serde_json::Value,
    pub old_data: Option<serde_json::Value>,
}
#[derive(Clone, Copy)]
struct RowProcessingConfig<'a> {
    table: &'a str,
    tracking_column: &'a str,
    pk_column: &'a str,
    payload_format: PayloadFormat,
    payload_col: &'a str,
    snake_case_columns: bool,
    include_metadata: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    tracking_offsets: HashMap<String, String>,
    processed_rows: u64,
}

const CONNECTOR_NAME: &str = "MySQL source";

#[async_trait]
impl Source for MySqlSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening MySQL source connector with ID: {}, Tables: {:?}",
            self.id, self.config.tables
        );

        // Every poll iterates `tables`, so an empty list makes the connector a
        // silent no-op rather than an obvious misconfiguration.
        if self.config.tables.is_empty() {
            return Err(Error::InitError(
                "tables must not be empty. Even when custom_query hardcodes the table, list it \
                 here: the table name supplies the offset key, the deterministic message ID, \
                 and the table_name metadata field"
                    .to_string(),
            ));
        }

        // A zero batch would make every fetch look like a full batch, so `poll`
        // would drop its pacing sleep and spin on `LIMIT 0` queries forever.
        if self.config.batch_size == Some(0) {
            return Err(Error::InitError(
                "batch_size must be greater than 0; omit it to use the default of 1000".to_string(),
            ));
        }

        // Only when the operator owns the ORDER BY. The built query sorts ascending by
        // construction, so a one-row batch is safe there and legitimate for a slow table.
        if self.config.batch_size == Some(1)
            && self
                .config
                .custom_query
                .as_deref()
                .is_some_and(|query| query.contains("$offset"))
        {
            return Err(Error::InitError(
                "batch_size must be greater than 1 when custom_query filters on $offset: a \
                 one-row batch offers no pair to compare, so the ordering check reduces to \
                 the row against the offset it resumed from, and a query ordered descending \
                 opens above that offset on every poll. The cursor would walk to the table's \
                 maximum and skip every row below it with nothing reporting it"
                    .to_string(),
            ));
        }

        if self
            .config
            .tracking_column
            .as_deref()
            .is_none_or(str::is_empty)
        {
            let guidance = if self.config.custom_query.is_some() {
                "the connector does not parse custom_query, so it has to be told which projected \
                 column supplies the cursor: name the one the query orders by and filters with \
                 $offset"
            } else {
                "it has to be unique and monotonically increasing under the order MySQL sorts it \
                 in."
            };
            return Err(Error::InitError(format!(
                "tracking_column must be set: every row's cursor is taken from it, and {guidance}."
            )));
        }

        if let Some(ref col) = self.config.payload_column
            && !col.is_empty()
            && PayloadFormat::from_config(self.config.payload_format.as_deref())
                == PayloadFormat::Json
        {
            return Err(Error::InitError(
                "payload_format must be 'bytea', 'text', or 'json_direct' when payload_column is set"
                    .to_string(),
            ));
        }

        // custom_query never changes after startup, so validate it once here
        if let Some(ref query) = self.config.custom_query {
            self.validate_custom_query(query)?;

            if query.contains("$offset") && self.config.initial_offset.is_none() {
                let state = self.state.lock().await;
                let missing: Vec<&str> = self
                    .config
                    .tables
                    .iter()
                    .filter(|t| !state.tracking_offsets.contains_key(*t))
                    .map(String::as_str)
                    .collect();
                if !missing.is_empty() {
                    return Err(Error::InitError(format!(
                        "custom_query uses $offset but initial_offset is not set and no stored offset exists for table(s): {}",
                        missing.join(", ")
                    )));
                }
            }
        }

        if self.config.delete_after_read.unwrap_or(false) && self.config.processed_column.is_some()
        {
            warn!(
                "both delete_after_read and processed_column are set; delete_after_read takes precedence, so rows are deleted and processed_column only acts as a poll filter"
            );
        }

        self.connect().await?;
        *self.tracking_kinds.get_mut() = self.resolve_tracking_columns().await?;

        info!(
            "MySQL source connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Skip the pacing delay while draining a backlog
        if !self.last_batch_full.load(Ordering::Relaxed) {
            tokio::time::sleep(self.poll_interval).await;
        }

        let messages = self.poll_tables().await?;

        let state = self.state.lock().await;
        if self.verbose {
            info!(
                "MySQL source connector ID: {} produced {} messages. Total processed: {}",
                self.id,
                messages.len(),
                state.processed_rows
            );
        } else {
            debug!(
                "MySQL source connector ID: {} produced {} messages. Total processed: {}",
                self.id,
                messages.len(),
                state.processed_rows
            );
        }

        let schema = match self.payload_format() {
            PayloadFormat::Bytea => Schema::Raw,
            PayloadFormat::Text => Schema::Text,
            PayloadFormat::JsonDirect | PayloadFormat::Json => Schema::Json,
        };

        // Idle cycles produce no messages and leave offsets/processed_rows
        // untouched, so return None to let the runtime skip the state fsync.
        let persisted_state = if messages.is_empty() {
            None
        } else {
            self.serialize_state(&state)
        };

        Ok(ProducedMessages {
            schema,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!("MySQL connection pool closed for connector ID: {}", self.id);
        }

        let state = self.state.lock().await;
        info!(
            "MySQL source connector ID: {} closed. Total rows processed: {}",
            self.id, state.processed_rows
        );
        Ok(())
    }
}

impl MySqlSource {
    pub fn new(id: u32, config: MySqlSourceConfig, state: Option<ConnectorState>) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Tracking offsets: {:?}, processed rows: {}",
                    s.tracking_offsets, s.processed_rows
                );
            });

        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = HumanDuration::from_str(delay_str)
            .map(|duration| duration.into())
            .unwrap_or_else(|_| Duration::from_secs(1));
        let interval_str = config.poll_interval.as_deref().unwrap_or("10s");
        let poll_interval = HumanDuration::from_str(interval_str)
            .map(|duration| duration.into())
            .unwrap_or_else(|_| Duration::from_secs(10));
        MySqlSource {
            id,
            pool: None,
            config,
            state: Mutex::new(restored_state.unwrap_or(State {
                tracking_offsets: HashMap::new(),
                processed_rows: 0,
            })),
            verbose,
            retry_delay,
            poll_interval,
            last_batch_full: AtomicBool::new(false),
            poisoned_tables: Mutex::new(HashSet::new()),
            tracking_kinds: Mutex::new(HashMap::new()),
            polls: AtomicU64::new(0),
            cursor_regressions: Mutex::new(HashMap::new()),
        }
    }

    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(10);
        let redacted = redact_connection_string(self.config.connection_string.expose_secret());

        info!("Connecting to MySQL with max {max_connections} connections: {redacted}");

        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.config.connection_string.expose_secret())
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to MySQL: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!("Connected to MySQL database with {max_connections} max connections");
        Ok(())
    }

    /// The column every cursor is read from. `open()` refuses a config that leaves it
    /// unset, so the fallback is unreachable in a running connector.
    fn tracking_column(&self) -> &str {
        self.config.tracking_column.as_deref().unwrap_or("id")
    }

    /// Fail fast if a tracking column can't yield a stable, ordered scalar
    /// cursor. `value_as_string` drops NULL, tinyint(1) (decoded as BOOLEAN),
    /// and JSON to `None`, which silently stalls offset advancement and re-reads
    /// rows every cycle.
    async fn resolve_tracking_columns(&self) -> Result<HashMap<String, OffsetKind>, Error> {
        // Whether the connector owns the query, and can therefore hold the schema to
        // what that query will do with it.
        let strict = self.config.custom_query.is_none();
        let pool = self.get_pool()?;
        let tracking_column = self.tracking_column();
        let mut kinds = HashMap::with_capacity(self.config.tables.len());

        for table in &self.config.tables {
            let (schema, table_name) = table
                .split_once('.')
                .map(|(schema, name)| (schema.to_string(), name.to_string()))
                .unwrap_or_else(|| (String::new(), table.clone()));

            let row = sqlx::query(
                "SELECT is_nullable, data_type, column_type \
                 FROM information_schema.columns \
                 WHERE table_schema = IF(? = '', DATABASE(), ?) \
                   AND table_name = ? AND column_name = ?",
            )
            .bind(&schema)
            .bind(&schema)
            .bind(&table_name)
            .bind(tracking_column)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                Error::InitError(format!(
                    "failed to probe schema for tracking_column '{tracking_column}' on table '{table}': {e}"
                ))
            })?;

            let Some(row) = row else {
                // A missing columns row means either the table doesn't exist yet
                // or the column is absent. Only the latter is a misconfiguration:
                // a table created after the connector starts is legitimate, so
                // defer validation rather than permanently disabling the source.
                let table_exists = sqlx::query(
                    "SELECT 1 FROM information_schema.tables \
                     WHERE table_schema = IF(? = '', DATABASE(), ?) \
                       AND table_name = ?",
                )
                .bind(&schema)
                .bind(&schema)
                .bind(&table_name)
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    Error::InitError(format!("failed to probe existence of table '{table}': {e}"))
                })?
                .is_some();

                if !table_exists {
                    warn!(
                        "table '{table}' does not exist yet; deferring tracking_column \
                         '{tracking_column}' validation until the table is created"
                    );
                    kinds.insert(table.clone(), OffsetKind::Unknown);
                    continue;
                }

                if strict {
                    return Err(Error::InitError(format!(
                        "tracking_column '{tracking_column}' not found on table '{table}'"
                    )));
                }

                warn!(
                    "tracking_column '{tracking_column}' is not a column of table '{table}'; \
                     assuming custom_query projects it. Its comparison order is resolved from \
                     the first result set instead."
                );
                kinds.insert(table.clone(), OffsetKind::Unknown);
                continue;
            };

            // Read by position: MySQL returns information_schema column names
            // uppercased (IS_NULLABLE, ...), so try_get by lowercase name misses.
            let is_nullable: String = row.try_get(0).map_err(|e| {
                Error::InitError(format!(
                    "failed to read is_nullable for table '{table}': {e}"
                ))
            })?;
            let data_type: String = row.try_get(1).map_err(|e| {
                Error::InitError(format!("failed to read data_type for table '{table}': {e}"))
            })?;
            let column_type: String = row.try_get(2).map_err(|e| {
                Error::InitError(format!(
                    "failed to read column_type for table '{table}': {e}"
                ))
            })?;

            // The column's type survives a plain projection, so it is worth rejecting
            // whatever the query looks like. A custom_query aliasing an expression onto
            // a name the table also uses is the one false positive, hence the hint.
            if let Some(reason) = unusable_tracking_type(&data_type) {
                let aliasing_hint = if self.config.custom_query.is_some() {
                    " If custom_query aliases a different expression to this name, rename the \
                      alias so it does not collide with the column."
                } else {
                    ""
                };
                return Err(Error::InitError(format!(
                    "tracking_column '{tracking_column}' on table '{table}' has type \
                     '{column_type}': {reason}. Use an integer, timestamp, or string \
                     column.{aliasing_hint}"
                )));
            }

            if is_nullable.eq_ignore_ascii_case("YES") {
                if strict {
                    return Err(Error::InitError(format!(
                        "tracking_column '{tracking_column}' on table '{table}' is nullable; \
                         a NULL value stalls offset tracking. Declare it NOT NULL."
                    )));
                }
                warn!(
                    "tracking_column '{tracking_column}' on table '{table}' is nullable; a row \
                     whose value is NULL cannot produce a cursor and will fail that table's batch. \
                     Have custom_query exclude those rows, or declare the column NOT NULL."
                );
            }

            if is_collation_ordered(&data_type) {
                warn!(
                    "tracking_column '{tracking_column}' on table '{table}' has type \
                     '{column_type}', so its cursor is compared in the column's collation and has \
                     to be unique and monotonically increasing in that order. Unpadded digit \
                     strings are not: '17' sorts below '9', after which the connector idles at \
                     the collation maximum forever. Zero-padded numbers, ULIDs and KSUIDs are. A \
                     case- or accent-insensitive collation such as the default utf8mb4_0900_ai_ci \
                     also breaks uniqueness, since 'ORD-1a' equals 'ORD-1A' and one of the two is \
                     dropped, so prefer a _bin collation."
                );
            }

            kinds.insert(table.clone(), offset_kind_for_data_type(&data_type));
        }
        Ok(kinds)
    }

    fn payload_format(&self) -> PayloadFormat {
        if let Some(ref payload_col) = self.config.payload_column
            && !payload_col.is_empty()
        {
            return PayloadFormat::from_config(self.config.payload_format.as_deref());
        }
        PayloadFormat::Json
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn get_pool(&self) -> Result<&Pool<MySql>, Error> {
        self.pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }

    fn extract_payload_column(
        &self,
        row: &MySqlRow,
        column_index: usize,
        format: PayloadFormat,
    ) -> Result<Vec<u8>, Error> {
        let column_name = row.columns()[column_index].name();
        match format {
            PayloadFormat::Bytea => {
                let bytes: Option<Vec<u8>> = row.try_get(column_index).map_err(|e| {
                    Error::InvalidRecordValue(format!(
                        "payload column '{column_name}' as bytea: {e}"
                    ))
                })?;
                Ok(bytes.unwrap_or_default())
            }
            PayloadFormat::Text => {
                let text: Option<String> = row.try_get(column_index).map_err(|e| {
                    Error::InvalidRecordValue(format!(
                        "payload column '{column_name}' as text (invalid UTF-8?): {e}"
                    ))
                })?;
                Ok(text.unwrap_or_default().into_bytes())
            }
            PayloadFormat::JsonDirect => {
                let json_value: Option<serde_json::Value> =
                    row.try_get(column_index).map_err(|e| {
                        Error::InvalidRecordValue(format!(
                            "payload column '{column_name}' as json_direct (invalid JSON?): {e}"
                        ))
                    })?;
                simd_json::to_vec(&json_value.unwrap_or(serde_json::Value::Null)).map_err(|e| {
                    Error::InvalidRecordValue(format!(
                        "payload column '{column_name}': failed to serialize JSON: {e}"
                    ))
                })
            }
            PayloadFormat::Json => Err(Error::InvalidConfig), // unreachable! if payload_column is there then payload_format can never be json
        }
    }

    /// Best current knowledge of how a table's tracking values compare.
    async fn tracking_kind(&self, table: &str) -> OffsetKind {
        self.tracking_kinds
            .lock()
            .await
            .get(table)
            .copied()
            .unwrap_or(OffsetKind::Unknown)
    }

    /// Whether the stored cursor is what limits the rows a poll can see. The built
    /// polling query always filters on it; a `custom_query` does so only if it
    /// interpolates `$offset`.
    fn cursor_gates_query(&self) -> bool {
        match &self.config.custom_query {
            Some(query) => query.contains("$offset"),
            None => true,
        }
    }

    /// The offset a poll actually resumes from: the stored cursor, or the configured
    /// starting point before anything has been stored.
    fn effective_offset(&self, last_offset: &Option<String>) -> Option<String> {
        last_offset
            .clone()
            .or_else(|| self.config.initial_offset.clone())
    }

    fn substitute_query_params(
        &self,
        query: &str,
        table: &str,
        last_offset: &Option<String>,
        batch_size: u32,
        kind: OffsetKind,
    ) -> Result<String, Error> {
        let offset_value = self.effective_offset(last_offset).unwrap_or_default();
        let offset = format_offset_value(&offset_value, kind);
        let now = Utc::now();

        let q = query
            .replace("$table", &quote_qualified_identifier(table)?)
            .replace("$offset", &offset)
            .replace("$limit", &batch_size.to_string())
            .replace("$now_unix", &now.timestamp().to_string())
            .replace("$now", &now.to_rfc3339());

        Ok(q)
    }

    /// The query is substituted as text and re-executed once per configured table on
    /// every poll for the life of the process, so a shape that cannot terminate does
    /// not fail, it repeats. What can be judged from the text alone is judged here,
    /// at startup, rather than left to the first poll.
    fn validate_custom_query(&self, query: &str) -> Result<(), Error> {
        let tracking_column = self.tracking_column();

        match leading_statement_token(query) {
            None => {
                return Err(Error::InitError(
                    "custom_query has no statement: it is empty, or nothing but comments"
                        .to_string(),
                ));
            }
            Some(token) if !READ_STATEMENT_STARTS.contains(&token.as_str()) => {
                return Err(Error::InitError(format!(
                    "custom_query must be a read, but it starts with '{token}'. It is executed \
                     once per configured table on every poll for the life of the process, so a \
                     statement that writes would re-run forever. Start it with SELECT, WITH or \
                     TABLE."
                )));
            }
            Some(_) => {}
        }

        // Collapsed so `ORDER\nBY` and `ORDER  BY` read the same as `ORDER BY`; the
        // check hard-fails, so a formatting difference must not decide it.
        let normalized = query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();

        // Two shapes, two failures. With `$offset` the stored cursor is what limits
        // the next poll, and it is only the batch's maximum when the rows arrive
        // ascending, so an unordered query skips rows. Without `$offset` nothing
        // limits the poll at all: the cursor is written but never read back, so the
        // same rows return forever unless the rows leave the result set. Only
        // `delete_after_read` guarantees that on its own - `processed_column` marks
        // rows the connector never filters on outside the query it builds itself, so
        // that arm rests on the operator's WHERE and is the weaker of the two.
        if query.contains("$offset") {
            if !normalized.contains("ORDER BY") {
                return Err(Error::InitError(format!(
                    "custom_query filters on $offset but has no ORDER BY. The next offset is the \
                     tracking value of the last row returned, which is only the highest one when \
                     the batch arrives ascending by '{tracking_column}'; without that, rows below \
                     it are skipped permanently. Add `ORDER BY {tracking_column}`."
                )));
            }
        } else if !self.config.delete_after_read.unwrap_or(false)
            && self.config.processed_column.is_none()
        {
            return Err(Error::InitError(format!(
                "custom_query does not filter on $offset, so every poll returns the same rows and \
                 publishes them again. Interpolate $offset (`WHERE {tracking_column} > $offset`, \
                 which also needs an ORDER BY), or take the rows out of the next poll by setting \
                 delete_after_read, which deletes them, or processed_column, which marks them - \
                 with processed_column the query has to exclude the marked rows itself, since the \
                 connector only appends that filter to the query it builds."
            )));
        }

        if query.contains("$table") && self.config.tables.is_empty() {
            return Err(Error::InitError(
                "custom_query uses $table but no tables are configured, so the placeholder \
                 can never resolve; list the tables to poll in `tables`"
                    .to_string(),
            ));
        }
        // Without $table the same SQL is executed once per table, so identical rows
        // would be emitted repeatedly, each carrying a different table's metadata,
        // message ID, and tracking offset. Table identity is not recoverable from a
        // static query, so reject the combination instead of guessing one.
        if !query.contains("$table") && self.config.tables.len() > 1 {
            return Err(Error::InitError(format!(
                "custom_query has no $table placeholder but {} tables are configured ({}); \
                 the same query would run once per table and emit its rows multiple times. \
                 Use $table in the query, or configure a single table.",
                self.config.tables.len(),
                self.config.tables.join(", ")
            )));
        }
        Ok(())
    }

    fn build_polling_query(
        &self,
        table: &str,
        tracking_column: &str,
        last_offset: &Option<String>,
        batch_size: u32,
        kind: OffsetKind,
    ) -> Result<String, Error> {
        let quoted_table = quote_qualified_identifier(table)?;
        let quoted_tracking = quote_identifier(tracking_column)?;

        let base_query = format!("SELECT * FROM {quoted_table}");

        let mut conditions = Vec::new();

        if let Some(offset) = self.effective_offset(last_offset) {
            conditions.push(format!(
                "{quoted_tracking} > {}",
                format_offset_value(&offset, kind)
            ));
        }

        if let Some(processed_col) = &self.config.processed_column {
            let quoted_processed = quote_identifier(processed_col)?;
            conditions.push(format!("{quoted_processed} = FALSE"));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let order_clause = format!(" ORDER BY {quoted_tracking} ASC");
        let limit_clause = format!(" LIMIT {batch_size}");

        Ok(format!(
            "{base_query}{where_clause}{order_clause}{limit_clause}"
        ))
    }

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    async fn mark_or_delete_processed_rows(
        &self,
        pool: &Pool<MySql>,
        table: &str,
        pk_column: &str,
        ids: &[PkValue],
    ) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let quoted_table = quote_qualified_identifier(table)?;
        let quoted_pk = quote_identifier(pk_column)?;
        let placeholders = vec!["?"; ids.len()].join(", ");

        let query = if self.config.delete_after_read.unwrap_or(false) {
            if self.verbose {
                info!("Deleting {} processed rows from '{table}'", ids.len());
            } else {
                debug!("Deleting {} processed rows from '{table}'", ids.len());
            }
            format!("DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({placeholders})")
        } else if let Some(processed_col) = &self.config.processed_column {
            let quoted_processed = quote_identifier(processed_col)?;
            if self.verbose {
                info!("Marking {} rows as processed in '{table}'", ids.len());
            } else {
                debug!("Marking {} rows as processed in '{table}'", ids.len());
            }
            format!(
                "UPDATE {quoted_table} SET {quoted_processed} = TRUE WHERE {quoted_pk} IN ({placeholders})"
            )
        } else {
            // Offset-tracking only: nothing to mark or delete.
            return Ok(());
        };

        // The query and its binds are rebuilt on every attempt because with_retry
        // calls the closure once per try and each `execute` consumes the query.
        with_retry(
            || {
                let mut prepared = sqlx::query(sqlx::AssertSqlSafe(query.as_str()));
                for id in ids {
                    prepared = match id {
                        PkValue::Text(text) => prepared.bind(text),
                        PkValue::Bytes(bytes) => prepared.bind(bytes),
                    };
                }
                prepared.execute(pool)
            },
            self.get_max_retries(),
            self.retry_delay.as_millis() as u64,
        )
        .await
        .map(|_| ())
    }

    async fn poll_tables(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self.get_pool()?;

        let batch_size = self.config.batch_size.unwrap_or(1000);
        let tracking_column = self.tracking_column();
        let pk_column = self
            .config
            .primary_key_column
            .as_deref()
            .unwrap_or(tracking_column);

        let row_config = RowProcessingConfig {
            table: "",
            tracking_column,
            pk_column,
            payload_format: self.payload_format(),
            payload_col: self.config.payload_column.as_deref().unwrap_or(""),
            snake_case_columns: self.config.snake_case_columns.unwrap_or(false),
            include_metadata: self.config.include_metadata.unwrap_or(true),
        };

        // Phase 1: fetch and process every table into its own buffer without any
        // side effect. A `process_row` failure is deterministic (a row that cannot
        // be decoded fails identically every cycle), so a failing table is logged
        // with its resume offset and skipped rather than aborting the whole poll:
        // nothing has been marked yet, its rows stay in MySQL, and the remaining
        // tables still make progress. The operator sees exactly which table and
        // offset to fix.
        //
        // An ordering violation is handled one step further. Retrying it would
        // re-run the same query, re-decode the same rows, and hit the same failure
        // forever, so the table is latched off instead of being retried each cycle.
        let mut batches: Vec<TableBatch> = Vec::with_capacity(self.config.tables.len());
        let mut any_batch_full = false;

        let poll_number = self.polls.fetch_add(1, Ordering::Relaxed) + 1;
        let poisoned = { self.poisoned_tables.lock().await.clone() };
        let mut newly_poisoned: Vec<String> = Vec::new();

        for table in &self.config.tables {
            if poisoned.contains(table) {
                continue;
            }

            // Get last offset with minimal lock time
            let last_offset = {
                let state = self.state.lock().await;
                state.tracking_offsets.get(table).cloned()
            };

            match self
                .fetch_table_batch(
                    table,
                    &row_config,
                    tracking_column,
                    batch_size,
                    &last_offset,
                )
                .await
            {
                Ok(batch) => {
                    if self.verbose {
                        info!("Fetched {} rows from table '{table}'", batch.messages.len());
                    } else {
                        debug!("Fetched {} rows from table '{table}'", batch.messages.len());
                    }
                    if !batch.messages.is_empty() {
                        self.cursor_regressions.lock().await.remove(table);
                    }
                    // A full batch means the LIMIT was hit, so more rows are waiting.
                    any_batch_full |= batch.messages.len() as u32 >= batch_size;
                    batches.push(batch);
                }
                Err(FetchError::Ordering(reason) | FetchError::MissingTrackingColumn(reason)) => {
                    let remedy = if self.config.custom_query.is_some() {
                        "with a corrected query"
                    } else {
                        "with a corrected tracking_column, since the connector wrote this query \
                         itself"
                    };
                    error!(
                        "Table '{table}' at offset {}: {reason} No rows were published and the \
                         offset was not advanced. This is deterministic, so the table is disabled \
                         until the connector is restarted {remedy}.",
                        last_offset.as_deref().unwrap_or("<start>")
                    );
                    newly_poisoned.push(table.clone());
                }
                Err(FetchError::CursorRegression(reason)) => {
                    // Cleared only by a batch that carried rows, so the run counts polls
                    // since the table last published anything. An idle poll produces as
                    // little as a rejected one does and must not read as recovery.
                    let consecutive = {
                        let mut regressions = self.cursor_regressions.lock().await;
                        let consecutive = regressions.entry(table.clone()).or_insert(0);
                        *consecutive += 1;
                        *consecutive
                    };
                    let offset = last_offset.as_deref().unwrap_or("<start>");
                    if consecutive >= CURSOR_REGRESSION_ERROR_THRESHOLD {
                        error!(
                            "Table '{table}' at offset {offset}: {reason} No rows were published \
                             and the offset was not advanced. This is the {consecutive}th \
                             consecutive poll to end this way, so it is no longer resolving on \
                             its own: the table stays enabled but has published nothing since."
                        );
                    } else {
                        warn!(
                            "Table '{table}' at offset {offset}: {reason} No rows were published \
                             and the offset was not advanced. The table stays enabled, since this \
                             can resolve without a config change."
                        );
                    }
                }
                Err(FetchError::StaleOffsetKind(reason)) => {
                    warn!(
                        "Table '{table}' at offset {}: {reason} No rows were published and the \
                         offset was not advanced. The next poll rebuilds the query from the type \
                         the result set reported.",
                        last_offset.as_deref().unwrap_or("<start>")
                    );
                }
                Err(FetchError::UnusableTrackingValue(reason)) => {
                    error!(
                        "Table '{table}' at offset {}: {reason} No rows were published and the \
                         offset was not advanced. The table stays enabled so a corrected row is \
                         picked up without a restart, but nothing will be published until then - \
                         this repeats every poll.",
                        last_offset.as_deref().unwrap_or("<start>")
                    );
                }
                Err(FetchError::Other(error)) => {
                    error!(
                        "Failed to process table '{table}' at offset {}, skipping this cycle: {error}",
                        last_offset.as_deref().unwrap_or("<start>")
                    );
                }
            }
        }

        if !newly_poisoned.is_empty() {
            let mut poisoned_tables = self.poisoned_tables.lock().await;
            poisoned_tables.extend(newly_poisoned);
            if self
                .config
                .tables
                .iter()
                .all(|table| poisoned_tables.contains(table))
            {
                error!(
                    "All configured tables are disabled because their batches cannot yield a \
                     cursor; {CONNECTOR_NAME} connector with ID: {} is idle until restarted",
                    self.id
                );
            }
        }

        // From the next poll on a disabled table is skipped before anything is logged,
        // so the one line written when it was disabled can scroll out of the operator's
        // log window and leave a dark connector looking healthy.
        if poll_number.is_multiple_of(DISABLED_TABLES_LOG_INTERVAL) {
            let poisoned_tables = self.poisoned_tables.lock().await;
            if !poisoned_tables.is_empty() {
                let mut disabled: Vec<&str> = poisoned_tables.iter().map(String::as_str).collect();
                disabled.sort_unstable();
                error!(
                    "{} of {} configured table(s) are still disabled and are not being polled by \
                     {CONNECTOR_NAME} connector with ID: {}, and will stay that way until it is \
                     restarted: {}",
                    disabled.len(),
                    self.config.tables.len(),
                    self.id,
                    disabled.join(", ")
                );
            }
        }

        // Phase 2: commit each table independently. If the mark/delete call fails
        // for a table we just skip its messages this cycle instead of queuing them
        // - `mark_or_delete_processed_rows` already retries transient errors, so a
        // failure here is permanent and the table's rows stay in MySQL to be
        // retried next cycle, without blocking the others.
        //
        // Doesn't cover the case where mark/delete succeeds but the runtime never
        // gets to publish the batch (crash, send failure, etc) - those rows are
        // gone from MySQL with nothing delivered.
        let mut messages = Vec::new();
        let mut state_updates: Vec<(String, String)> = Vec::new();
        let mut total_processed: u64 = 0;

        let mark_or_delete = self.config.delete_after_read.unwrap_or(false)
            || self.config.processed_column.is_some();

        for mut batch in batches {
            if mark_or_delete && !batch.messages.is_empty() && batch.processed_ids.is_empty() {
                error!(
                    "Table '{}': mark/delete is configured but no primary keys were extracted \
             from {} row(s), skipping this cycle so rows are not published without being \
             marked or deleted (check that '{pk_column}' is projected and scalar)",
                    batch.table,
                    batch.messages.len()
                );
                continue;
            }

            if !batch.processed_ids.is_empty()
                && let Err(error) = self
                    .mark_or_delete_processed_rows(
                        pool,
                        &batch.table,
                        pk_column,
                        &batch.processed_ids,
                    )
                    .await
            {
                error!(
                    "Failed to mark or delete processed rows for table '{}', skipping this cycle: {error}",
                    batch.table
                );
                continue;
            }

            total_processed += batch.messages.len() as u64;
            messages.append(&mut batch.messages);
            if let Some(offset) = batch.max_offset {
                state_updates.push((batch.table, offset));
            }
        }

        // Apply all state updates with a single lock acquisition
        {
            let mut state = self.state.lock().await;
            state.processed_rows += total_processed;
            for (table, offset) in state_updates {
                state.tracking_offsets.insert(table, offset);
            }
        }

        self.last_batch_full
            .store(any_batch_full, Ordering::Relaxed);
        Ok(messages)
    }

    async fn fetch_table_batch(
        &self,
        table: &str,
        row_config: &RowProcessingConfig<'_>,
        tracking_column: &str,
        batch_size: u32,
        last_offset: &Option<String>,
    ) -> Result<TableBatch, FetchError> {
        let pool = self.get_pool()?;
        let table_config = RowProcessingConfig {
            table,
            ..*row_config
        };

        let query_kind = self.tracking_kind(table).await;
        let query = if let Some(custom_query) = &self.config.custom_query {
            self.substitute_query_params(custom_query, table, last_offset, batch_size, query_kind)?
        } else {
            self.build_polling_query(table, tracking_column, last_offset, batch_size, query_kind)?
        };

        // Database I/O without holding the lock
        let rows = with_retry(
            || sqlx::query(sqlx::AssertSqlSafe(query.as_str())).fetch_all(pool),
            self.get_max_retries(),
            self.retry_delay.as_millis() as u64,
        )
        .await?;

        // Settled once for the whole batch rather than per row, since the column list
        // is a property of the result set.
        if let Some(first) = rows.first() {
            ensure_tracking_column_projected(
                first.columns().iter().map(|column| column.name()),
                tracking_column,
            )?;
        }

        let resume_from = if self.cursor_gates_query() {
            self.effective_offset(last_offset)
        } else {
            None
        };

        // The result set describes what this query actually returned, so it outranks
        // whatever `information_schema` said at startup and is the only answer
        // available for a computed column.
        let kind = match rows.first().and_then(|row| {
            observed_tracking_kind(row, tracking_column).filter(|observed| *observed != query_kind)
        }) {
            Some(observed) => {
                self.tracking_kinds
                    .lock()
                    .await
                    .insert(table.to_string(), observed);
                // The query was already sent with the stale kind, so its offset literal
                // may be quoted for the wrong type.
                if let Some(offset) = &resume_from
                    && offset_literal_diverged(offset, query_kind, observed)
                {
                    return Err(FetchError::StaleOffsetKind(format!(
                        "the poll filtered on '{tracking_column}' with {}, written for the \
                         comparison order known before this poll, but the result set reports an \
                         order whose literal is {}. A filter that disagrees with the query's own \
                         ordering strands rows below the cursor permanently, so the batch was \
                         discarded.",
                        format_offset_value(offset, query_kind),
                        format_offset_value(offset, observed)
                    )));
                }
                observed
            }
            None => query_kind,
        };

        let mut batch = TableBatch {
            table: table.to_string(),
            messages: Vec::with_capacity(rows.len()),
            processed_ids: Vec::new(),
            max_offset: None,
        };
        let mut ordering = OrderingGuard::new(tracking_column, kind, resume_from);
        for row in rows {
            let processed = self.process_row(&row, &table_config).map_err(|e| {
                error!(
                    "Failed to decode row in table '{table}' (pk {}): {e}",
                    pk_for_log(&row, table_config.pk_column)
                );
                e
            })?;

            let Some(value) = processed.tracking_value else {
                return Err(unusable_tracking_value(
                    tracking_column,
                    &pk_for_log(&row, table_config.pk_column),
                ));
            };
            ordering
                .accept(value)
                .map_err(|violation| match violation {
                    OrderingViolation::WithinBatch(reason) => FetchError::Ordering(reason),
                    OrderingViolation::BelowCursor(reason) => FetchError::CursorRegression(reason),
                })?;

            if let Some(pk) = processed.row_pk {
                batch.processed_ids.push(pk);
            }

            batch.messages.push(processed.message);
        }
        batch.max_offset = ordering.into_cursor();

        Ok(batch)
    }

    fn process_row(
        &self,
        row: &MySqlRow,
        config: &RowProcessingConfig,
    ) -> Result<ProcessedRow, Error> {
        let mut row_pk: Option<PkValue> = None;
        let mut tracking_value: Option<String> = None;

        // Payload column set: only extract it plus tracking/pk columns.
        // Avoids extract_column_value on every other column since the data map
        // built below would be discarded anyway.
        if !config.payload_col.is_empty() {
            let mut extracted_payload: Option<Vec<u8>> = None;
            for (i, column) in row.columns().iter().enumerate() {
                let name = column.name();
                if name.eq_ignore_ascii_case(config.payload_col) {
                    extracted_payload =
                        Some(self.extract_payload_column(row, i, config.payload_format)?);
                }
                if name.eq_ignore_ascii_case(config.tracking_column) {
                    let value = extract_column_value(row, i)?;
                    tracking_value = column_value_as_string(row, i, &value)?;
                }
                if name.eq_ignore_ascii_case(config.pk_column) {
                    row_pk = extract_pk_value(row, i)?;
                }
            }

            // Every extract_payload_column arm yields Ok for a NULL column, so None
            // here means the column was absent. Falling back to the whole-row JSON
            // would publish bytes contradicting the schema derived from payload_format.
            let payload = extracted_payload.ok_or_else(|| {
                Error::InvalidRecordValue(format!(
                    "table '{}': payload_column '{}' not present in the result set",
                    config.table, config.payload_col
                ))
            })?;

            return Ok(build_processed_row(
                config.table,
                payload,
                tracking_value,
                row_pk,
            ));
        }

        let mut data = serde_json::Map::new();
        for (i, column) in row.columns().iter().enumerate() {
            let name = column.name();
            let column_name = if config.snake_case_columns {
                to_snake_case(name)
            } else {
                name.to_string()
            };
            let value = extract_column_value(row, i)?;
            if name.eq_ignore_ascii_case(config.tracking_column) {
                tracking_value = column_value_as_string(row, i, &value)?;
            }
            if name.eq_ignore_ascii_case(config.pk_column) {
                row_pk = extract_pk_value(row, i)?;
            }
            data.insert(column_name, value);
        }

        let payload = if config.include_metadata {
            let record = DatabaseRecord {
                table_name: config.table.to_string(),
                operation_type: "SELECT".to_string(),
                timestamp: Utc::now(),
                data: serde_json::Value::Object(data),
                old_data: None,
            };
            simd_json::to_vec(&record).map_err(|e| {
                Error::InvalidRecordValue(format!(
                    "table '{}': failed to serialize row to JSON: {e}",
                    config.table
                ))
            })?
        } else {
            simd_json::to_vec(&data).map_err(|e| {
                Error::InvalidRecordValue(format!(
                    "table '{}': failed to serialize row to JSON: {e}",
                    config.table
                ))
            })?
        };

        Ok(build_processed_row(
            config.table,
            payload,
            tracking_value,
            row_pk,
        ))
    }
}

/// Type-faithful primary-key extraction. Binary columns are taken as raw bytes
/// rather than routed through the base64 path in `extract_column_value`, so the
/// value can be bound back into `WHERE pk IN (...)` and match the row. Everything
/// else takes the string form MySQL parses back to the same value. Returns None
/// only for a NULL pk, which a real primary key cannot be.
fn extract_pk_value(row: &MySqlRow, column_index: usize) -> Result<Option<PkValue>, Error> {
    let column = &row.columns()[column_index];
    let type_name = column.type_info().name();
    match type_name {
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            let bytes: Option<Vec<u8>> = row.try_get(column_index).map_err(|e| {
                Error::InvalidRecordValue(format!(
                    "primary key column '{}' (MySQL type '{type_name}'): {e}",
                    column.name()
                ))
            })?;
            Ok(bytes.map(PkValue::Bytes))
        }
        _ => {
            let value = extract_column_value(row, column_index)?;
            Ok(column_value_as_string(row, column_index, &value)?.map(PkValue::Text))
        }
    }
}

fn extract_column_value(row: &MySqlRow, column_index: usize) -> Result<serde_json::Value, Error> {
    let column = &row.columns()[column_index];
    let type_name = column.type_info().name();

    let to_err = |e: sqlx::Error| {
        Error::InvalidRecordValue(format!(
            "column '{}' (MySQL type '{type_name}'): {e}",
            column.name()
        ))
    };

    match type_name {
        "BOOLEAN" => {
            let value: Option<bool> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null))
        }
        "TINYINT" => {
            let value: Option<i8> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "SMALLINT" => {
            let value: Option<i16> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "MEDIUMINT" | "INT" => {
            let value: Option<i32> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "BIGINT" => {
            let value: Option<i64> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "TINYINT UNSIGNED" => {
            let value: Option<u8> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "SMALLINT UNSIGNED" => {
            let value: Option<u16> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            let value: Option<u32> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "BIGINT UNSIGNED" => {
            let value: Option<u64> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT" => {
            let value: Option<f32> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as f64))
                .unwrap_or(serde_json::Value::Null))
        }
        "DOUBLE" => {
            let value: Option<f64> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "DECIMAL" => {
            let value: Option<rust_decimal::Decimal> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|d| serde_json::Value::String(d.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "BIT" => {
            let value: Option<u64> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "YEAR" => {
            let value: Option<u16> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "DATE" => {
            let value: Option<NaiveDate> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|d| serde_json::Value::String(d.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIME" => {
            let value: Option<NaiveTime> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|t| serde_json::Value::String(t.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "DATETIME" => {
            let value: Option<NaiveDateTime> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|dt| serde_json::Value::String(dt.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMESTAMP" => {
            let value: Option<DateTime<Utc>> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|dt| serde_json::Value::String(dt.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => {
            let value: Option<String> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null))
        }
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            let value: Option<Vec<u8>> = row.try_get(column_index).map_err(to_err)?;
            Ok(value
                .map(|bytes| {
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(&bytes),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "JSON" => {
            let value: Option<serde_json::Value> = row.try_get(column_index).map_err(to_err)?;
            Ok(value.unwrap_or(serde_json::Value::Null))
        }
        "NULL" => Ok(serde_json::Value::Null),
        _ => {
            let column_name = column.name();
            warn!(
                "Column '{column_name}' has unrecognized MySQL type '{type_name}', \
                 attempting text extraction"
            );
            if let Ok(text) = row.try_get::<Option<String>, _>(column_index) {
                return Ok(text
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null));
            }
            if let Ok(bytes) = row.try_get::<Option<Vec<u8>>, _>(column_index) {
                return Ok(bytes
                    .map(|b| {
                        serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(&b),
                        )
                    })
                    .unwrap_or(serde_json::Value::Null));
            }
            error!(
                "Column '{column_name}' has unsupported MySQL type '{type_name}', \
                 returning null"
            );
            Ok(serde_json::Value::Null)
        }
    }
}

/// Best-effort primary-key value for diagnostics when a row fails to decode.
/// Falls back to "unknown" if the pk column is absent or itself fails to extract,
/// so logging a decode failure never masks it with a second error.
fn pk_for_log(row: &MySqlRow, pk_column: &str) -> String {
    row.columns()
        .iter()
        .position(|column| column.name().eq_ignore_ascii_case(pk_column))
        .and_then(|index| extract_column_value(row, index).ok())
        .and_then(|value| value_as_string(&value))
        .unwrap_or_else(|| "unknown".to_string())
}

fn value_as_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The string form of a column value that MySQL parses back to the value it came
/// from, which for `TIMESTAMP` is not its payload form: that one carries chrono's
/// trailing ` UTC`. A `SELECT` only warns about the superfluous delimiter, but the
/// same conversion inside the mark/delete DML is an error under the default
/// `STRICT_TRANS_TABLES`, so both the cursor and the `WHERE pk IN (...)` binding
/// take this form. Only the payload keeps chrono's rendering.
fn column_value_as_string(
    row: &MySqlRow,
    column_index: usize,
    value: &serde_json::Value,
) -> Result<Option<String>, Error> {
    let column = &row.columns()[column_index];
    if column.type_info().name() != "TIMESTAMP" {
        return Ok(value_as_string(value));
    }

    let timestamp: Option<DateTime<Utc>> = row.try_get(column_index).map_err(|e| {
        Error::InvalidRecordValue(format!(
            "tracking column '{}' (MySQL type 'TIMESTAMP'): {e}",
            column.name()
        ))
    })?;
    Ok(timestamp.map(timestamp_cursor))
}

/// Fixed width, so the collation comparison a temporal cursor is ordered by agrees
/// with the instants the values stand for.
fn timestamp_cursor(timestamp: DateTime<Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn build_processed_row(
    table: &str,
    payload: Vec<u8>,
    tracking_value: Option<String>,
    row_pk: Option<PkValue>,
) -> ProcessedRow {
    let now = Utc::now().timestamp_micros() as u64;
    // The key is the pk's string form, so changing how a column type renders re-keys
    // every row of that type. Once connectors are in the field that breaks dedup for
    // any row replayed across the change, so weigh it before touching
    // `column_value_as_string`.
    let pk_key = row_pk.as_ref().map(PkValue::as_key);
    let id = message_id(table, pk_key.as_deref().or(tracking_value.as_deref()));
    ProcessedRow {
        message: ProducedMessage {
            id: Some(id),
            headers: None,
            checksum: None,
            timestamp: Some(now),
            origin_timestamp: Some(now),
            payload,
        },
        tracking_value,
        row_pk,
    }
}

/// Deterministic message id so a row replayed after a restart keeps the same id
/// and downstream can dedup it. Derived from the table plus the row's stable key
/// (its primary key, or the unique/monotonic tracking value). The random branch
/// is unreachable for a contract-compliant tracking column - it only avoids a
/// panic if a row yields no usable key, at the cost of that one row not being
/// idempotent on replay.
fn message_id(table: &str, key: Option<&str>) -> u128 {
    match key {
        Some(key) => {
            let name = format!("{table}\0{key}");
            Uuid::new_v5(&MESSAGE_ID_NAMESPACE, name.as_bytes()).as_u128()
        }
        None => {
            warn!(
                "Row in table '{table}' has no primary-key or tracking value; \
                 using a random message id (replays will not be idempotent)"
            );
            Uuid::new_v4().as_u128()
        }
    }
}

fn to_snake_case(input: &str) -> String {
    let mut result = String::new();
    let mut prev_was_uppercase = false;
    for (i, ch) in input.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 && !prev_was_uppercase {
                result.push('_');
            }
            if let Some(lc) = ch.to_lowercase().next() {
                result.push(lc);
            } else {
                result.push(ch);
            }
            prev_was_uppercase = true;
        } else {
            result.push(ch);
            prev_was_uppercase = false;
        }
    }
    result
}

fn redact_connection_string(conn_str: &str) -> String {
    if let Some(scheme_end) = conn_str.find("://") {
        let scheme = &conn_str[..scheme_end + 3];
        let rest = &conn_str[scheme_end + 3..];
        let preview: String = rest.chars().take(3).collect();
        return format!("{scheme}{preview}***");
    }
    let preview: String = conn_str.chars().take(3).collect();
    format!("{preview}***")
}

fn quote_identifier(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InvalidConfigValue(
            "identifier must not be empty".to_string(),
        ));
    }
    if name.contains('\0') {
        return Err(Error::InvalidConfigValue(format!(
            "identifier '{name}' contains NUL byte"
        )));
    }
    let escaped = name.replace('`', "``");
    Ok(format!("`{escaped}`"))
}

fn quote_qualified_identifier(name: &str) -> Result<String, Error> {
    if !name.contains('.') {
        return quote_identifier(name);
    }
    let parts: Result<Vec<_>, _> = name.split('.').map(quote_identifier).collect();
    Ok(parts?.join("."))
}

/// Renders the cursor as a SQL literal, quoted by the column's kind rather than by the
/// value's text: MySQL compares a string column against a bare numeric literal as
/// doubles, so `code > 42` on a `VARCHAR` filters numerically while `ORDER BY code`
/// sorts by collation, and every row below the numeric high-water mark is stranded. The
/// implicit cast also costs the column's index.
fn format_offset_value(value: &str, kind: OffsetKind) -> String {
    let bare = kind != OffsetKind::Lexical
        && (value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok_and(|v| v.is_finite()));
    if bare {
        value.to_string()
    } else {
        // NUL is escaped, not dropped: a shortened literal is a smaller literal, so
        // MySQL would return the row the cursor already passed and the guard would
        // read that as a regression. Ordered after the backslash pass so the escape
        // this one writes is not doubled.
        let escaped = value
            .replace('\\', "\\\\")
            .replace('\'', "''")
            .replace('\0', "\\0");
        format!("'{escaped}'")
    }
}

/// Whether a query built with `applied` filtered on a different literal than one
/// built with `observed` would have. The kind reaches the SQL only through
/// `format_offset_value` and the `ORDER BY` does not depend on it, so equal literals
/// mean the two queries are the same query and a batch fetched under the stale kind
/// is still the right batch.
fn offset_literal_diverged(offset: &str, applied: OffsetKind, observed: OffsetKind) -> bool {
    format_offset_value(offset, applied) != format_offset_value(offset, observed)
}

/// Compares two offsets as numbers, mirroring the numeric/string split in
/// `format_offset_value`. `None` when either side is not numeric. Integers are
/// tried before floats so large `BIGINT` values keep full precision, and unsigned
/// values above `i64::MAX` are handled before falling back to `f64`.
fn compare_offsets_numeric(left: &str, right: &str) -> Option<CmpOrdering> {
    if let (Ok(left), Ok(right)) = (left.parse::<i64>(), right.parse::<i64>()) {
        return Some(left.cmp(&right));
    }
    if let (Ok(left), Ok(right)) = (left.parse::<u64>(), right.parse::<u64>()) {
        return Some(left.cmp(&right));
    }
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) if left.is_finite() && right.is_finite() => {
            Some(left.total_cmp(&right))
        }
        _ => None,
    }
}

/// Whether the cursor would move backwards. Numeric offsets compare numerically,
/// since a lexical compare reports "9" > "10" and would reject every correctly
/// ordered integer batch.
///
/// String offsets are reported as decreasing only when a case-sensitive and a
/// case-insensitive comparison agree, because the server-side collation is not
/// known here: `utf8mb4_0900_ai_ci` (the MySQL 8 default) orders 'a' before 'B'
/// while `utf8mb4_bin` orders 'B' first. Firing on that disagreement would disable
/// a table whose rows are correctly ordered, so ambiguous pairs are allowed
/// through. Erring toward a missed violation is the safe direction: a false
/// positive would stop a healthy table.
fn offset_decreased(previous: &str, next: &str, kind: OffsetKind) -> bool {
    match kind {
        OffsetKind::Numeric => {
            compare_offsets_numeric(previous, next) == Some(CmpOrdering::Greater)
        }
        OffsetKind::Lexical => lexically_decreased(previous, next),
        // Both readings must agree before a table whose column type is unresolved is
        // disabled. ("100", "99") is a real decrease numerically and an increase
        // lexically, so it passes: a missed violation is recoverable, a table
        // disabled in error is not.
        OffsetKind::Unknown => match compare_offsets_numeric(previous, next) {
            Some(ordering) => {
                ordering == CmpOrdering::Greater && lexically_decreased(previous, next)
            }
            None => lexically_decreased(previous, next),
        },
    }
}

/// Whether a collation-ordered value decreased. Reported only when case-sensitive
/// and case-insensitive comparisons agree, because the column's collation is not
/// known here: `utf8mb4_0900_ai_ci` orders 'a' before 'B', `utf8mb4_bin` orders 'B'
/// first, and one of those readings would disable a correctly ordered table.
///
/// Non-ASCII values are always treated as ambiguous. Accent-insensitive and
/// language-specific collations sort them in ways no byte comparison reproduces
/// (under `utf8mb4_0900_ai_ci`, 'é' precedes 'f' while its bytes do not).
fn lexically_decreased(previous: &str, next: &str) -> bool {
    if !previous.is_ascii() || !next.is_ascii() {
        return false;
    }
    let case_sensitive = previous.cmp(next);
    let case_insensitive = previous
        .bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .cmp(next.bytes().map(|byte| byte.to_ascii_lowercase()));
    case_sensitive == CmpOrdering::Greater && case_insensitive == CmpOrdering::Greater
}

fn is_transient_error(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_) => true,
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::PoolClosed => false,
        sqlx::Error::Protocol(_) => false,
        // MySQL surfaces a numeric error code (e.g. 1213) and a SQLSTATE (e.g. "40001").
        // `DatabaseError::code()` returns the SQLSTATE, so matching it against MySQL error
        // numbers never fires. Downcast to the driver error and compare `number()` instead.
        sqlx::Error::Database(db_err) => db_err
            .try_downcast_ref::<MySqlDatabaseError>()
            .is_some_and(|mysql_err| {
                matches!(
                    mysql_err.number(),
                    // concurrency
                    1213 | 1205 |
                    // server unavailability
                    1053 | 1152 | 1080 |
                    // connection/network
                    1158 | 1159 | 1160 | 1161 |
                    // resource exhaustion
                    1040 | 1041
                )
            }),
        _ => false,
    }
}

async fn with_retry<T, F, Fut>(operation: F, max_retries: u32, delay_ms: u64) -> Result<T, Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let max_attempts = max_retries + 1;
    let mut attempts = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                let transient = is_transient_error(&e);
                if attempts >= max_attempts || !transient {
                    error!("Database operation failed after {attempts} attempts: {e}");
                    return Err(if transient {
                        // exhausted retries on a connectivity/availability failure
                        Error::Connection(format!("after {attempts} attempts: {e}"))
                    } else {
                        // DB rejected the operation itself (syntax, missing table, ...)
                        Error::InvalidRecordValue(format!("{e}"))
                    });
                }
                warn!(
                    "Transient database error (attempt {attempts}/{max_attempts}): {e}. Retrying in {delay_ms}ms..."
                );
                tokio::time::sleep(Duration::from_millis(delay_ms * attempts as u64)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Baseline polling config; individual tests override only the fields they exercise.
    fn test_config() -> MySqlSourceConfig {
        MySqlSourceConfig {
            connection_string: SecretString::from("mysql://localhost/db"),
            tables: vec!["users".to_string()],
            poll_interval: Some("5s".to_string()),
            batch_size: Some(500),
            tracking_column: Some("id".to_string()),
            initial_offset: None,
            max_connections: None,
            custom_query: None,
            snake_case_columns: None,
            include_metadata: None,
            delete_after_read: None,
            processed_column: None,
            primary_key_column: None,
            payload_column: None,
            payload_format: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
        }
    }

    #[test]
    fn given_persisted_state_should_restore_tracking_offsets() {
        // A connector restarted with prior state must resume from the saved
        // per-table offsets and processed-row count, not re-poll from scratch.
        let state = State {
            tracking_offsets: HashMap::from([
                ("users".to_string(), "100".to_string()),
                ("orders".to_string(), "2024-01-15T10:30:00Z".to_string()),
            ]),
            processed_rows: 500,
        };
        let connector_state =
            ConnectorState::serialize(&state, "test", 1).expect("Failed to serialize state");

        let source = MySqlSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(
                restored.tracking_offsets.get("users"),
                Some(&"100".to_string())
            );
            assert_eq!(
                restored.tracking_offsets.get("orders"),
                Some(&"2024-01-15T10:30:00Z".to_string())
            );
            assert_eq!(restored.processed_rows, 500);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        // First-ever run (no persisted state) starts with empty offsets so the
        // first poll picks up everything from the initial_offset / table start.
        let source = MySqlSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert!(state.tracking_offsets.is_empty());
            assert_eq!(state.processed_rows, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        // Corrupt/unreadable persisted state must degrade to a fresh start
        // rather than panicking and crash-looping the connector.
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = MySqlSource::new(1, test_config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert!(state.tracking_offsets.is_empty());
            assert_eq!(state.processed_rows, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        // The full State (offsets + count) must survive a MessagePack
        // round-trip unchanged, since that is what gets persisted.
        let original = State {
            tracking_offsets: HashMap::from([("table1".to_string(), "42".to_string())]),
            processed_rows: 1000,
        };

        let connector_state =
            ConnectorState::serialize(&original, "test", 1).expect("Failed to serialize state");
        let deserialized: State = connector_state
            .deserialize("test", 1)
            .expect("Failed to deserialize state");

        assert_eq!(original.tracking_offsets, deserialized.tracking_offsets);
        assert_eq!(original.processed_rows, deserialized.processed_rows);
    }

    #[test]
    fn given_last_offset_should_filter_order_and_limit() {
        // With a known last offset, the query must fetch only newer rows,
        // ordered ascending by the tracking column, capped at the batch size.
        let source = MySqlSource::new(1, test_config(), None);
        let query = source
            .build_polling_query(
                "users",
                "id",
                &Some("100".to_string()),
                500,
                OffsetKind::Numeric,
            )
            .expect("Failed to build query");
        assert_eq!(
            query,
            "SELECT * FROM `users` WHERE `id` > 100 ORDER BY `id` ASC LIMIT 500"
        );
    }

    #[test]
    fn given_initial_offset_and_no_last_offset_should_use_initial() {
        // On the first poll (no last offset yet) the configured initial_offset
        // seeds the WHERE clause so we skip rows the operator wants ignored.
        let mut config = test_config();
        config.initial_offset = Some("1000".to_string());
        let source = MySqlSource::new(1, config, None);
        let query = source
            .build_polling_query("users", "id", &None, 500, OffsetKind::Numeric)
            .expect("Failed to build query");
        assert_eq!(
            query,
            "SELECT * FROM `users` WHERE `id` > 1000 ORDER BY `id` ASC LIMIT 500"
        );
    }

    #[test]
    fn given_no_offset_should_omit_where_clause() {
        // No last offset and no initial_offset means "read from the beginning":
        // no WHERE filter, but ordering + limit still bound the batch.
        let source = MySqlSource::new(1, test_config(), None);
        let query = source
            .build_polling_query("users", "id", &None, 500, OffsetKind::Numeric)
            .expect("Failed to build query");
        assert_eq!(query, "SELECT * FROM `users` ORDER BY `id` ASC LIMIT 500");
    }

    #[test]
    fn given_processed_column_should_append_unprocessed_filter() {
        // When a processed_column is configured, each poll must also exclude
        // already-handled rows (`col` = FALSE) so they are not re-emitted.
        let mut config = test_config();
        config.processed_column = Some("is_processed".to_string());
        let source = MySqlSource::new(1, config, None);
        let query = source
            .build_polling_query("events", "id", &None, 100, OffsetKind::Numeric)
            .expect("Failed to build query");
        assert!(query.contains("`is_processed` = FALSE"));
    }

    #[test]
    fn given_offset_value_should_quote_only_non_numeric() {
        // Numeric offsets are emitted bare (correct comparison + no cast),
        // while string offsets (e.g. timestamps) must be single-quoted literals.
        let source = MySqlSource::new(1, test_config(), None);

        let numeric = source
            .build_polling_query(
                "users",
                "id",
                &Some("42".to_string()),
                100,
                OffsetKind::Numeric,
            )
            .expect("Failed to build query");
        assert!(numeric.contains("`id` > 42"));
        assert!(!numeric.contains("'42'"));

        let string = source
            .build_polling_query(
                "users",
                "updated_at",
                &Some("2024-01-01".to_string()),
                100,
                OffsetKind::Lexical,
            )
            .expect("Failed to build query");
        assert!(string.contains("`updated_at` > '2024-01-01'"));
    }

    #[test]
    fn given_qualified_table_should_backtick_each_segment() {
        // A `db.table` target must quote each segment independently so the
        // dot stays a schema separator, not part of a single quoted name.
        let source = MySqlSource::new(1, test_config(), None);
        let query = source
            .build_polling_query("mydb.users", "id", &None, 100, OffsetKind::Numeric)
            .expect("Failed to build query");
        assert!(query.contains("FROM `mydb`.`users`"));
    }

    #[test]
    fn given_custom_query_should_substitute_table_offset_limit() {
        // Placeholders in an operator-provided query must be filled with the
        // current table, resolved offset, and batch size before execution.
        let source = MySqlSource::new(1, test_config(), None);
        let query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit";
        let result = source
            .substitute_query_params(
                query,
                "events",
                &Some("100".to_string()),
                50,
                OffsetKind::Numeric,
            )
            .unwrap();
        assert!(result.contains("FROM `events`"));
        assert!(result.contains("id > 100"));
        assert!(result.contains("LIMIT 50"));
    }

    #[test]
    fn given_custom_query_with_time_params_should_substitute_now() {
        // Time placeholders must be expanded to a concrete timestamp so no
        // literal `$now` reaches the database.
        let source = MySqlSource::new(1, test_config(), None);
        let query = "SELECT * FROM $table WHERE created_at < '$now' OR ts < $now_unix";
        let result = source
            .substitute_query_params(query, "logs", &None, 100, OffsetKind::Numeric)
            .unwrap();
        println!("{}", result);
        assert!(result.contains("FROM `logs`"));
        assert!(!result.contains("$now"));
        assert!(!result.contains("_unix"));
    }

    #[test]
    fn given_no_last_offset_should_fall_back_to_initial_offset() {
        // In the custom-query path too, a missing last offset must fall back to
        // the configured initial_offset rather than substituting an empty value.
        let mut config = test_config();
        config.initial_offset = Some("500".to_string());
        let source = MySqlSource::new(1, config, None);
        let result = source
            .substitute_query_params(
                "SELECT * FROM $table WHERE id > $offset",
                "data",
                &None,
                100,
                OffsetKind::Numeric,
            )
            .unwrap();
        assert!(result.contains("id > 500"));
    }

    #[test]
    fn given_table_placeholder_and_no_tables_should_fail() {
        // A $table placeholder with no configured tables can never resolve,
        // so validation must reject it instead of querying a literal "$table".
        let mut config = test_config();
        config.tables = vec![];
        let source = MySqlSource::new(1, config, None);
        let result =
            source.validate_custom_query("SELECT * FROM $table WHERE id > $offset ORDER BY id");
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("$table")),
            "expected an InitError naming the placeholder, got {result:?}"
        );
    }

    #[test]
    fn given_valid_custom_query_should_pass() {
        // A well-formed SELECT with tables configured passes validation.
        let source = MySqlSource::new(1, test_config(), None);
        let result =
            source.validate_custom_query("SELECT * FROM $table WHERE id > $offset ORDER BY id");
        assert!(result.is_ok());
    }

    #[test]
    fn given_static_custom_query_and_multiple_tables_should_fail() {
        // Without $table the identical SQL would run once per table, emitting the
        // same rows under each table's metadata, message ID, and offset key.
        let mut config = test_config();
        config.tables = vec!["users".to_string(), "orders".to_string()];
        let source = MySqlSource::new(1, config, None);
        match source.validate_custom_query("SELECT * FROM events WHERE id > $offset ORDER BY id") {
            Err(Error::InitError(message)) => {
                assert!(message.contains("users, orders"), "message was: {message}")
            }
            other => panic!("expected InitError, got {other:?}"),
        }
    }

    #[test]
    fn given_table_placeholder_and_multiple_tables_should_pass() {
        // The supported multi-table shape: $table resolves per table, so each one
        // gets its own SQL, metadata, message IDs, and offset.
        let mut config = test_config();
        config.tables = vec!["users".to_string(), "orders".to_string()];
        let source = MySqlSource::new(1, config, None);
        let result =
            source.validate_custom_query("SELECT * FROM $table WHERE id > $offset ORDER BY id");
        assert!(result.is_ok());
    }

    #[test]
    fn given_static_custom_query_and_single_table_should_pass() {
        // One table leaves no ambiguity: the query runs once and its rows are
        // attributed to the only configured table, so hardcoding it stays valid.
        let mut config = test_config();
        config.tables = vec!["users".to_string()];
        let source = MySqlSource::new(1, config, None);
        let result =
            source.validate_custom_query("SELECT * FROM users WHERE id > $offset ORDER BY id");
        assert!(result.is_ok());
    }

    #[test]
    fn given_backtick_in_identifier_should_escape() {
        // An embedded backtick must be doubled so it cannot terminate the
        // quoted identifier and inject trailing SQL.
        let result = quote_identifier("col`name").expect("Failed to quote");
        assert_eq!(result, "`col``name`");
    }

    #[test]
    fn given_empty_or_nul_identifier_should_fail() {
        // Empty names and NUL bytes are never valid identifiers and must be
        // rejected rather than producing malformed/unsafe SQL.
        assert!(quote_identifier("").is_err());
        assert!(quote_identifier("bad\0name").is_err());
    }

    #[test]
    fn given_qualified_identifier_should_quote_each_segment_and_reject_empty() {
        // Each segment of a db.table name is quoted independently; an empty
        // segment (leading/trailing dot) is rejected.
        let quoted = quote_qualified_identifier("mydb.users").expect("Failed to quote");
        assert_eq!(quoted, "`mydb`.`users`");
        assert!(quote_qualified_identifier("mydb.").is_err());
        assert!(quote_qualified_identifier(".users").is_err());
    }

    #[test]
    fn given_string_offset_value_should_escape_sql_metacharacters() {
        // A non-numeric offset is interpolated into the WHERE clause, so quotes
        // and backslashes must be escaped to prevent breaking out of the literal.
        assert_eq!(
            format_offset_value("O'Brien", OffsetKind::Lexical),
            "'O''Brien'"
        );
        assert_eq!(format_offset_value("a\\b", OffsetKind::Lexical), "'a\\\\b'");
        assert_eq!(format_offset_value("42", OffsetKind::Numeric), "42");
        // Dropping the NUL would compare against a shorter, smaller value and hand
        // back the row the cursor already passed.
        assert_eq!(format_offset_value("a\0b", OffsetKind::Lexical), "'a\\0b'");
    }

    #[test]
    fn given_digits_in_a_collation_ordered_column_should_still_quote() {
        // The whole point of threading the kind through: a bare literal makes MySQL
        // coerce a VARCHAR column and the numeric literal to double, so the filter
        // compares numerically while ORDER BY compares by collation. Values below
        // the numeric high-water mark then become unreachable for good.
        assert_eq!(format_offset_value("42", OffsetKind::Lexical), "'42'");
        assert_eq!(format_offset_value("007", OffsetKind::Lexical), "'007'");
        assert_eq!(format_offset_value("1.5", OffsetKind::Lexical), "'1.5'");
    }

    #[test]
    fn given_unparsable_value_should_quote_whatever_the_kind_claims() {
        // A Numeric kind is a claim about the column, not a guarantee about the
        // value. Nothing unparsed may reach the query as a bare literal.
        assert_eq!(
            format_offset_value("not-a-number", OffsetKind::Numeric),
            "'not-a-number'"
        );
        assert_eq!(format_offset_value("1e400", OffsetKind::Numeric), "'1e400'");
    }

    #[test]
    fn given_unknown_kind_should_keep_content_based_quoting() {
        // Before the first result set resolves the column, the value's own text is
        // all there is to go on.
        assert_eq!(format_offset_value("42", OffsetKind::Unknown), "42");
        assert_eq!(
            format_offset_value("2024-01-01", OffsetKind::Unknown),
            "'2024-01-01'"
        );
    }

    #[test]
    fn given_stale_kind_rendering_a_different_literal_should_report_divergence() {
        // The first fetch on an unresolved table builds its filter from the cached
        // kind. A numeric-looking offset against a column the result set then reports
        // as collation-ordered is the case that strands rows: the filter ran
        // numerically while ORDER BY sorted by collation.
        assert!(offset_literal_diverged(
            "200",
            OffsetKind::Unknown,
            OffsetKind::Lexical
        ));
        assert!(offset_literal_diverged(
            "200",
            OffsetKind::Numeric,
            OffsetKind::Lexical
        ));
    }

    #[test]
    fn given_stale_kind_rendering_the_same_literal_should_not_report_divergence() {
        // Both kinds emit the same literal, so the query was byte-identical to the one
        // the corrected kind would have built and its batch is sound. Discarding it
        // would drop the first batch of every table that was created after open().
        assert!(!offset_literal_diverged(
            "200",
            OffsetKind::Unknown,
            OffsetKind::Numeric
        ));
        assert!(!offset_literal_diverged(
            "2024-01-01",
            OffsetKind::Unknown,
            OffsetKind::Lexical
        ));
    }

    #[test]
    fn given_pk_value_should_produce_stable_message_key() {
        // as_key feeds the deterministic message id. Text passes through and
        // Bytes is base64, both matching the pre-typing value_as_string output,
        // so message ids downstream dedups on stay stable across this change.
        assert_eq!(PkValue::Text("123".to_string()).as_key(), "123");
        assert_eq!(
            PkValue::Bytes(vec![
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ])
            .as_key(),
            "VQ6EAOKbQdSnFkRmVUQAAA=="
        );
    }

    #[test]
    fn given_payload_format_strings_should_map_to_variants() {
        // Operator-facing aliases (and casing) must map to the right variant;
        // unknown/missing values default to Json.
        assert_eq!(
            PayloadFormat::from_config(Some("bytea")),
            PayloadFormat::Bytea
        );
        assert_eq!(
            PayloadFormat::from_config(Some("RAW")),
            PayloadFormat::Bytea
        );
        assert_eq!(
            PayloadFormat::from_config(Some("text")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("json_direct")),
            PayloadFormat::JsonDirect
        );
        assert_eq!(
            PayloadFormat::from_config(Some("unknown")),
            PayloadFormat::Json
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Json);
    }

    #[test]
    fn given_empty_payload_column_should_force_json() {
        // payload_format only takes effect when a payload_column is set; without
        // one the source always builds the full JSON record regardless of config.
        let mut config = test_config();
        config.payload_column = None;
        config.payload_format = Some("bytea".to_string());
        let source = MySqlSource::new(1, config, None);
        assert_eq!(source.payload_format(), PayloadFormat::Json);

        let mut config = test_config();
        config.payload_column = Some("data".to_string());
        config.payload_format = Some("bytea".to_string());
        let source = MySqlSource::new(1, config, None);
        assert_eq!(source.payload_format(), PayloadFormat::Bytea);
    }

    #[test]
    fn given_zero_batch_size_should_fail_open() {
        // Zero would make every fetch count as a full batch, so poll would never
        // sleep again and would spin on empty `LIMIT 0` queries.
        let mut config = test_config();
        config.batch_size = Some(0);
        let mut source = MySqlSource::new(1, config, None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("batch_size")),
            "expected an InitError naming batch_size, got {result:?}"
        );
    }

    #[test]
    fn given_single_row_batches_under_an_offset_filtered_custom_query_should_fail_open() {
        // With one row per batch the ordering check has only the resume cursor to
        // compare against, and a descending $offset query opens above that cursor on
        // every poll, so the rows it skips are never seen by anything.
        let mut config = test_config();
        config.batch_size = Some(1);
        config.custom_query =
            Some("SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit".to_string());
        let mut source = MySqlSource::new(1, config, None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("batch_size")),
            "expected an InitError naming batch_size, got {result:?}"
        );
    }

    #[test]
    fn given_single_row_batches_on_the_built_query_should_pass_the_batch_size_gate() {
        // The connector writes that query's ORDER BY itself, so one row per batch is
        // safe there and legitimate for a slow table. `payload_column` is set to trip
        // the next gate, which keeps the assertion off the network.
        let mut config = test_config();
        config.batch_size = Some(1);
        config.payload_column = Some("data".to_string());
        let mut source = MySqlSource::new(1, config, None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("payload_format")),
            "batch_size 1 must survive the gate without an $offset custom_query, got {result:?}"
        );
    }

    #[test]
    fn given_empty_tables_should_fail_open() {
        // Every poll iterates `tables`, so an empty list would leave the connector
        // running and producing nothing instead of reporting the misconfiguration.
        let mut config = test_config();
        config.tables = vec![];
        let mut source = MySqlSource::new(1, config, None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("tables")),
            "expected an InitError naming tables, got {result:?}"
        );
    }

    #[test]
    fn given_unset_tracking_column_should_fail_open() {
        // The column decides whether polling is lossless and the connector cannot
        // verify a guess, so the operator has to name it. An empty string is the same
        // omission spelled differently.
        for unset in [None, Some(String::new())] {
            let mut config = test_config();
            config.tracking_column = unset.clone();
            let mut source = MySqlSource::new(1, config, None);

            let runtime = tokio::runtime::Runtime::new().unwrap();
            let result = runtime.block_on(source.open());
            assert!(
                matches!(result, Err(Error::InitError(ref message)) if message.contains("tracking_column must be set")),
                "expected an InitError naming tracking_column for {unset:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn given_unset_tracking_column_with_custom_query_should_say_the_query_is_not_parsed() {
        // On the built path the operator can restore the old default in one line; under
        // a custom_query nothing can check that guess against the query's ORDER BY, so
        // the message has to say why.
        let mut config = test_config();
        config.tracking_column = None;
        config.custom_query =
            Some("SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit".to_string());
        let mut source = MySqlSource::new(1, config, None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        let Err(Error::InitError(message)) = result else {
            panic!("expected an InitError, got {result:?}");
        };
        assert!(message.contains("does not parse custom_query"), "{message}");
    }

    #[test]
    fn given_valid_poll_interval_and_retry_delay_should_parse() {
        // Valid humantime strings are parsed into the corresponding Durations.
        let mut config = test_config();
        config.poll_interval = Some("5s".to_string());
        config.retry_delay = Some("2s".to_string());
        let source = MySqlSource::new(1, config, None);
        assert_eq!(source.poll_interval, Duration::from_secs(5));
        assert_eq!(source.retry_delay, Duration::from_secs(2));
    }

    #[test]
    fn given_invalid_or_missing_cadence_should_fall_back_to_defaults() {
        // Unparsable or absent values fall back to the documented defaults
        // (10s poll interval, 1s retry delay) so the connector still runs.
        let mut config = test_config();
        config.poll_interval = Some("not-a-duration".to_string());
        config.retry_delay = None;
        let source = MySqlSource::new(1, config, None);
        assert_eq!(source.poll_interval, Duration::from_secs(10));
        assert_eq!(source.retry_delay, Duration::from_secs(1));
    }

    #[test]
    fn given_pool_errors_should_classify_transience() {
        // A pool timeout is worth retrying (likely transient contention); a
        // closed pool is terminal and must not be retried.
        assert!(is_transient_error(&sqlx::Error::PoolTimedOut));
        assert!(!is_transient_error(&sqlx::Error::PoolClosed));
    }

    #[test]
    fn given_same_table_and_key_should_produce_same_message_id() {
        // The whole point of the deterministic id: a row replayed after a
        // restart must hash to the same message id so downstream can dedup it.
        assert_eq!(
            message_id("users", Some("42")),
            message_id("users", Some("42"))
        );
    }

    #[test]
    fn given_different_key_or_table_should_produce_different_message_id() {
        // Distinct rows (different key, or same key in a different table) must
        // not collide, or dedup would drop genuinely different messages.
        assert_ne!(
            message_id("users", Some("42")),
            message_id("users", Some("43"))
        );
        assert_ne!(
            message_id("users", Some("42")),
            message_id("orders", Some("42"))
        );
    }

    #[test]
    fn given_key_boundary_should_not_collide_across_table_join() {
        // The NUL separator keeps `table="ab", key="c"` distinct from
        // `table="a", key="bc"`; a bare concatenation would collide them.
        assert_ne!(message_id("ab", Some("c")), message_id("a", Some("bc")));
    }

    #[test]
    fn given_no_key_should_still_yield_an_id() {
        // The contract-unreachable fallback must return an id rather than
        // panic; it is random, so we only assert it produces a value.
        let _ = message_id("users", None);
    }

    #[test]
    fn given_produced_row_should_stamp_timestamps_in_microseconds() {
        let before = Utc::now().timestamp_micros() as u64;
        let row = build_processed_row("users", b"{}".to_vec(), Some("1".to_owned()), None);
        let after = Utc::now().timestamp_micros() as u64;

        let timestamp = row.message.timestamp.expect("timestamp should be set");
        let origin_timestamp = row
            .message
            .origin_timestamp
            .expect("origin timestamp should be set");
        assert_eq!(timestamp, origin_timestamp);
        assert!(
            (before..=after).contains(&timestamp),
            "timestamp {timestamp} is outside the micros range [{before}, {after}]"
        );
    }

    fn run_guard(
        tracking_column: &str,
        kind: OffsetKind,
        values: &[&str],
    ) -> Result<Option<String>, String> {
        resume_guard(tracking_column, kind, None, values)
    }

    /// Drives the guard as a poll would, resuming from `cursor`. The violation is
    /// flattened to a string tagged with its variant so a test can assert which of
    /// the two it got.
    fn resume_guard(
        tracking_column: &str,
        kind: OffsetKind,
        cursor: Option<&str>,
        values: &[&str],
    ) -> Result<Option<String>, String> {
        let mut guard = OrderingGuard::new(tracking_column, kind, cursor.map(str::to_string));
        for value in values {
            guard
                .accept((*value).to_string())
                .map_err(|violation| match violation {
                    OrderingViolation::WithinBatch(reason) => format!("within_batch: {reason}"),
                    OrderingViolation::BelowCursor(reason) => format!("below_cursor: {reason}"),
                })?;
        }
        Ok(guard.into_cursor())
    }

    #[test]
    fn given_numeric_offsets_should_compare_numerically_not_lexically() {
        // The regression that would break every integer tracking column: "9" sorts
        // after "10" lexically, so a string compare would flag a correctly ordered
        // batch as a violation.
        assert_eq!(compare_offsets_numeric("9", "10"), Some(CmpOrdering::Less));
        assert_eq!(
            compare_offsets_numeric("100", "99"),
            Some(CmpOrdering::Greater)
        );
        assert_eq!(compare_offsets_numeric("7", "7"), Some(CmpOrdering::Equal));
        assert_eq!(compare_offsets_numeric("-5", "3"), Some(CmpOrdering::Less));
    }

    #[test]
    fn given_bigint_unsigned_offsets_should_compare_without_precision_loss() {
        // Values above i64::MAX must take the u64 branch; f64 cannot distinguish
        // adjacent integers up there and would report them equal.
        assert_eq!(
            compare_offsets_numeric("18446744073709551614", "18446744073709551615"),
            Some(CmpOrdering::Less)
        );
    }

    #[test]
    fn given_non_integer_numeric_offsets_should_compare_as_floats() {
        // A DECIMAL or FLOAT tracking column still orders numerically.
        assert_eq!(
            compare_offsets_numeric("1.5", "10.25"),
            Some(CmpOrdering::Less)
        );
        assert_eq!(compare_offsets_numeric("abc", "10"), None);
    }

    #[test]
    fn given_case_only_difference_should_not_report_decrease() {
        // The server-side collation is unknown: utf8mb4_0900_ai_ci orders 'a'
        // before 'B', utf8mb4_bin orders 'B' first. Neither ordering may disable a
        // table, so a pair the two comparisons disagree on is allowed through.
        assert!(!offset_decreased("B", "a", OffsetKind::Lexical));
        assert!(!offset_decreased("a", "B", OffsetKind::Lexical));
    }

    #[test]
    fn given_unambiguously_reversed_strings_should_report_decrease() {
        // Both comparisons agree, so this is a real reversal regardless of collation.
        assert!(offset_decreased("zebra", "apple", OffsetKind::Lexical));
        assert!(!offset_decreased("apple", "zebra", OffsetKind::Lexical));
    }

    #[test]
    fn given_non_ascii_values_should_not_report_decrease() {
        // Accent-insensitive collations order 'é' before 'f' while its bytes do not,
        // so non-ASCII pairs are ambiguous and must never disable a table.
        assert!(!offset_decreased("é", "f", OffsetKind::Lexical));
    }

    #[test]
    fn given_string_typed_tracking_column_should_accept_mysql_lexical_order() {
        // The regression a purely numeric comparison would cause: on a VARCHAR
        // column MySQL's ORDER BY ASC really does return "10" before "5", so
        // treating that as a decrease would disable a healthy table on the default
        // polling path.
        assert!(!offset_decreased("10", "5", OffsetKind::Lexical));
        assert!(run_guard("id", OffsetKind::Lexical, &["10", "5"]).is_ok());
    }

    #[test]
    fn given_numeric_typed_tracking_column_should_reject_lexically_ordered_rows() {
        // The mirror case: on an INT column "10" then "5" is a genuine decrease.
        assert!(offset_decreased("10", "5", OffsetKind::Numeric));
        assert!(run_guard("id", OffsetKind::Numeric, &["10", "5"]).is_err());
    }

    #[test]
    fn given_unresolved_column_type_should_reject_only_unambiguous_decreases() {
        // Without the column's SQL type, only a pair that decreases under both
        // readings may disable a table. ("100", "99") decreases numerically but
        // increases lexically, so it is allowed through.
        assert!(offset_decreased("30", "20", OffsetKind::Unknown));
        assert!(!offset_decreased("100", "99", OffsetKind::Unknown));
        assert!(offset_decreased("zebra", "apple", OffsetKind::Unknown));
    }

    #[test]
    fn given_data_type_should_map_to_mysql_sort_order() {
        // information_schema data types decide how MySQL sorted the column.
        for numeric in ["int", "BIGINT", "tinyint", "decimal", "double", "year"] {
            assert_eq!(
                offset_kind_for_data_type(numeric),
                OffsetKind::Numeric,
                "{numeric} should sort numerically"
            );
        }
        for lexical in ["varchar", "char", "text", "datetime", "timestamp", "date"] {
            assert_eq!(
                offset_kind_for_data_type(lexical),
                OffsetKind::Lexical,
                "{lexical} should sort by collation"
            );
        }
    }

    #[test]
    fn given_declared_type_without_a_usable_cursor_should_be_rejected() {
        // Each family for a different reason: no scalar form at all, a domain too small
        // to hold one value per row, base64 text that does not order like the bytes it
        // stands for, and labels MySQL sorts by declaration index instead. The whole
        // `data_type` decides it, so a `tinyint(1) unsigned` that MariaDB reports with
        // its width and MySQL 8.0.19+ reports without one is rejected either way.
        for data_type in [
            "json",
            "tinyint",
            "TINYINT",
            "binary",
            "varbinary",
            "blob",
            "longblob",
            "enum",
            "set",
        ] {
            assert!(
                unusable_tracking_type(data_type).is_some(),
                "{data_type} cannot carry a cursor and must be rejected"
            );
        }
    }

    #[test]
    fn given_declared_type_with_a_usable_cursor_should_be_accepted() {
        for data_type in [
            "int",
            "bigint",
            "smallint",
            "varchar",
            "timestamp",
            "decimal",
        ] {
            assert_eq!(
                unusable_tracking_type(data_type),
                None,
                "{data_type} should be accepted as a tracking column"
            );
        }
    }

    #[test]
    fn given_a_batch_whose_values_decrease_should_be_rejected() {
        // The guard reads the values, never the SQL, so a query whose result order
        // cannot be judged from its text - an ORDER BY confined to a derived table or
        // a CTE, a window clause, an ordering by some other column - is caught here
        // whenever it does return decreasing values, and left alone when it does not.
        let batches: &[(&str, &[&str])] = &[
            ("decreasing from the first pair", &["30", "20", "10"]),
            ("decreasing only at the last pair", &["10", "30", "20"]),
            ("two rows, the second below the first", &["2", "1"]),
        ];

        for (batch, values) in batches {
            let result = run_guard("id", OffsetKind::Numeric, values);
            assert!(result.is_err(), "expected rows to be rejected: {batch}");
        }
    }

    #[test]
    fn given_a_batch_whose_values_never_decrease_should_be_accepted() {
        // The mirror of the above: none of these may be rejected, or a working
        // deployment would be disabled.
        let batches: &[(&str, &[&str])] = &[
            ("ascending", &["10", "20", "30"]),
            (
                "ascending across the 9 to 10 lexical boundary",
                &["8", "9", "10", "11"],
            ),
            (
                "duplicate tracking values inside one batch",
                &["5", "5", "5"],
            ),
            ("single row", &["42"]),
        ];

        for (batch, values) in batches {
            let result = run_guard("id", OffsetKind::Numeric, values);
            assert!(result.is_ok(), "expected rows to be accepted: {batch}");
        }

        // Collation-ordered columns, where MySQL's ascending order is lexical.
        let lexical_batches: &[(&str, &[&str])] = &[
            (
                "timestamp tracking column ascending",
                &["2024-01-15T10:30:00Z", "2024-01-15T10:31:00Z"],
            ),
            (
                "string tracking column ascending",
                &["alpha", "beta", "gamma"],
            ),
            (
                "numeric-looking values in a VARCHAR column, ordered by collation",
                &["1", "10", "100", "2"],
            ),
        ];

        for (batch, values) in lexical_batches {
            let result = run_guard("id", OffsetKind::Lexical, values);
            assert!(result.is_ok(), "expected rows to be accepted: {batch}");
        }
    }

    #[test]
    fn given_accepted_batch_should_use_last_row_as_cursor() {
        // With ordering enforced the last row's value is the maximum, which is what
        // makes the existing last-row-wins cursor correct.
        assert_eq!(
            run_guard("id", OffsetKind::Numeric, &["10", "20", "30"]).unwrap(),
            Some("30".to_string())
        );
    }

    #[test]
    fn given_empty_batch_should_yield_no_cursor() {
        // An idle poll must leave the stored offset untouched.
        assert_eq!(run_guard("id", OffsetKind::Numeric, &[]).unwrap(), None);
    }

    #[test]
    fn given_a_single_row_below_its_resume_cursor_should_be_rejected() {
        // The only pair a one-row batch offers is the row against the cursor it
        // resumed from, so that is the whole of what the seed adds: a row reaching
        // behind the cursor. A row above it is indistinguishable from progress, which
        // is why a DESC query filtered on $offset walks past at batch_size=1.
        assert!(run_guard("id", OffsetKind::Numeric, &["30"]).is_ok());
        assert!(resume_guard("id", OffsetKind::Numeric, Some("30"), &["40"]).is_ok());
        let error = resume_guard("id", OffsetKind::Numeric, Some("30"), &["20"]).unwrap_err();
        assert!(error.starts_with("below_cursor:"), "{error}");
    }

    #[test]
    fn given_timestamp_tracking_value_should_render_a_cursor_without_a_zone_suffix() {
        // chrono renders DateTime<Utc> as "... UTC", which MySQL accepts only by
        // prefix-parsing and warns is a deprecated delimiter. Fixed-width fractional
        // digits keep the collation comparison the cursor is ordered by agreeing with
        // the instants.
        let whole_second = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let fractional = DateTime::from_timestamp(1_700_000_000, 500_000_000).unwrap();
        assert_eq!(timestamp_cursor(whole_second), "2023-11-14 22:13:20.000000");
        assert_eq!(timestamp_cursor(fractional), "2023-11-14 22:13:20.500000");
        assert!(!offset_decreased(
            &timestamp_cursor(whole_second),
            &timestamp_cursor(fractional),
            OffsetKind::Lexical
        ));
        assert!(offset_decreased(
            &timestamp_cursor(fractional),
            &timestamp_cursor(whole_second),
            OffsetKind::Lexical
        ));
    }

    #[test]
    fn given_batch_opening_below_its_cursor_should_report_regression_not_disorder() {
        // Ascending within the batch, but the batch reaches behind the offset it
        // resumed from. That is a different fault from a misordered query and must
        // not latch the table off.
        let error = resume_guard("id", OffsetKind::Numeric, Some("30"), &["15", "25"]).unwrap_err();
        assert!(error.starts_with("below_cursor:"), "{error}");

        // Disorder inside the batch stays the latching variant even when resumed.
        let error = resume_guard("id", OffsetKind::Numeric, Some("10"), &["30", "20"]).unwrap_err();
        assert!(error.starts_with("within_batch:"), "{error}");
    }

    #[test]
    fn given_batch_resuming_at_its_cursor_should_accept_rows() {
        // A query filtering with >= legitimately re-reads the boundary row, and
        // equality is not a decrease.
        assert_eq!(
            resume_guard("id", OffsetKind::Numeric, Some("30"), &["30", "40"]).unwrap(),
            Some("40".to_string())
        );
    }

    #[test]
    fn given_empty_batch_should_not_rewrite_the_cursor_it_resumed_from() {
        // An idle poll must leave the stored offset untouched rather than
        // re-persisting the seed as though it were this batch's maximum.
        assert_eq!(
            resume_guard("id", OffsetKind::Numeric, Some("30"), &[]).unwrap(),
            None
        );
    }

    #[test]
    fn given_result_set_type_should_resolve_comparison_order() {
        // Types come off the result set as sqlx names, which carry the unsigned
        // widths that information_schema reports as a separate column.
        for numeric in ["INT", "BIGINT UNSIGNED", "DECIMAL", "YEAR", "BIT"] {
            assert_eq!(
                offset_kind_for_type_name(numeric),
                OffsetKind::Numeric,
                "{numeric} should sort numerically"
            );
        }
        for lexical in ["VARCHAR", "TEXT", "DATETIME", "TIMESTAMP"] {
            assert_eq!(
                offset_kind_for_type_name(lexical),
                OffsetKind::Lexical,
                "{lexical} should sort by collation"
            );
        }
        // No usable cursor comes out of these, so they get no claimed order: the first
        // three yield no scalar at all, ENUM/SET sort by declaration index rather than
        // by the labels that reach the connector, and binary values are compared as
        // base64, which does not order the way their bytes do.
        for unknown in [
            "BOOLEAN",
            "JSON",
            "NULL",
            "GEOMETRY",
            "ENUM",
            "SET",
            "BINARY",
            "VARBINARY",
            "BLOB",
            "LONGBLOB",
        ] {
            assert_eq!(
                offset_kind_for_type_name(unknown),
                OffsetKind::Unknown,
                "{unknown} should not claim an order"
            );
        }
    }

    #[test]
    fn given_computed_tracking_column_typed_by_the_result_set_should_reject_descending_rows() {
        // The case information_schema cannot answer: a custom_query aliasing a
        // computed column. Left Unknown, ("100", "99") passes because the two
        // readings disagree; typed Numeric from the result set, it is caught.
        assert!(!offset_decreased("100", "99", OffsetKind::Unknown));
        assert_eq!(offset_kind_for_type_name("BIGINT"), OffsetKind::Numeric);
        assert!(run_guard("tracking_id", OffsetKind::Numeric, &["100", "99"]).is_err());
    }

    #[test]
    fn given_ordering_violation_should_name_column_and_both_values() {
        // The operator has to be able to fix the query from the log line alone.
        let error = run_guard("updated_at", OffsetKind::Numeric, &["30", "20"]).unwrap_err();
        assert!(error.contains("updated_at"));
        assert!(error.contains("'30'"));
        assert!(error.contains("'20'"));
    }

    #[test]
    fn given_result_set_projecting_the_tracking_column_should_accept_the_batch() {
        assert!(ensure_tracking_column_projected(["id", "name"], "id").is_ok());
    }

    #[test]
    fn given_result_set_without_the_tracking_column_should_reject_the_batch() {
        // The replay loop this closes: no column means no row yields a cursor, so
        // every poll re-publishes the same rows and the offset never moves.
        let error = ensure_tracking_column_projected(["row_id", "name"], "id")
            .expect_err("a result set missing the tracking column must not produce a batch");
        assert!(matches!(error, FetchError::MissingTrackingColumn(_)));
    }

    #[test]
    fn given_case_mismatched_tracking_column_should_accept_the_batch() {
        // MySQL matches column identifiers case-insensitively, and so does the
        // information_schema probe at open(), so `tracking_column = "ID"` against a
        // column declared `id` starts up. An exact match here would then disable the
        // table on every poll, telling the operator to fix an alias that never existed.
        assert!(ensure_tracking_column_projected(["id", "name"], "ID").is_ok());
        assert!(ensure_tracking_column_projected(["ID", "name"], "id").is_ok());
    }

    #[test]
    fn given_aliased_tracking_column_should_reject_the_batch() {
        // `SELECT id AS row_id` projects `row_id`. The alias is what the result set
        // carries, so matching on the underlying column name would be wrong.
        assert!(ensure_tracking_column_projected(["row_id"], "id").is_err());
    }

    #[test]
    fn given_missing_tracking_column_should_name_it_and_the_projected_columns() {
        // Both halves of the fix are in the message: what was looked for, and what
        // the query actually returned.
        let FetchError::MissingTrackingColumn(reason) =
            ensure_tracking_column_projected(["row_id", "payload"], "id").unwrap_err()
        else {
            panic!("expected a missing-tracking-column violation");
        };
        assert!(reason.contains("'id'"), "{reason}");
        assert!(reason.contains("row_id, payload"), "{reason}");
    }

    #[test]
    fn given_unusable_tracking_value_should_name_the_column_and_the_row() {
        // A single bad row stalls the table, so the log has to identify which one.
        let FetchError::UnusableTrackingValue(reason) = unusable_tracking_value("updated_at", "42")
        else {
            panic!("expected an unusable-tracking-value violation");
        };
        assert!(reason.contains("updated_at"), "{reason}");
        assert!(reason.contains("42"), "{reason}");
    }

    #[test]
    fn given_offset_filtered_custom_query_without_order_by_should_fail_open() {
        // The cursor is the last row's tracking value, which is only the batch maximum
        // when the rows arrive ascending. Nothing downstream can recover the rows an
        // unordered batch skipped, so this cannot be left to the guard.
        let source = MySqlSource::new(1, test_config(), None);
        let result =
            source.validate_custom_query("SELECT * FROM $table WHERE id > $offset LIMIT $limit");
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("ORDER BY")),
            "expected an InitError naming ORDER BY, got {result:?}"
        );
    }

    #[test]
    fn given_order_by_split_across_lines_should_satisfy_the_check() {
        // The check hard-fails, so how the clause is wrapped and indented must not
        // decide whether the connector starts.
        let source = MySqlSource::new(1, test_config(), None);
        let result = source.validate_custom_query(
            "SELECT * FROM $table\n  WHERE id > $offset\n  ORDER\n    BY id\n  LIMIT $limit",
        );
        assert!(result.is_ok(), "got {result:?}");
    }

    #[test]
    fn given_custom_query_without_offset_or_a_consuming_mode_should_fail_open() {
        // Nothing limits such a poll: the offset is written but never read back, so
        // the same rows are republished on every cycle forever.
        let source = MySqlSource::new(1, test_config(), None);
        let result = source.validate_custom_query("SELECT * FROM $table WHERE status = 'new'");
        assert!(
            matches!(result, Err(Error::InitError(ref message)) if message.contains("$offset")),
            "expected an InitError naming $offset, got {result:?}"
        );
    }

    #[test]
    fn given_custom_query_without_offset_should_pass_when_rows_are_consumed() {
        // delete_after_read and processed_column both take the rows out of the next
        // poll's result set, which is what $offset would otherwise have done.
        for consuming in [0, 1] {
            let mut config = test_config();
            if consuming == 0 {
                config.delete_after_read = Some(true);
            } else {
                config.processed_column = Some("is_processed".to_string());
            }
            let source = MySqlSource::new(1, config, None);
            let result = source.validate_custom_query("SELECT * FROM $table WHERE status = 'new'");
            assert!(result.is_ok(), "got {result:?}");
        }
    }

    #[test]
    fn given_custom_query_that_is_not_a_read_should_fail_open() {
        // The query re-executes once per table on every poll for the life of the
        // process, so a mistyped statement that writes runs forever.
        let source = MySqlSource::new(1, test_config(), None);
        for query in [
            "DELETE FROM $table WHERE id IN (SELECT id FROM stale) ORDER BY id",
            "INSERT INTO archive SELECT * FROM $table WHERE id > $offset ORDER BY id",
            "UPDATE $table SET flag = 1 WHERE id > $offset ORDER BY id",
        ] {
            let result = source.validate_custom_query(query);
            assert!(
                matches!(result, Err(Error::InitError(ref message)) if message.contains("must be a read")),
                "expected an InitError for {query}, got {result:?}"
            );
        }
    }

    #[test]
    fn given_read_statement_shapes_should_pass_the_leading_token_check() {
        // Comments and a parenthesized union are all legitimate ways to open a read,
        // and the check hard-fails, so none of them may be mistaken for a write.
        let source = MySqlSource::new(1, test_config(), None);
        for query in [
            "  \n SELECT * FROM $table WHERE id > $offset ORDER BY id",
            "-- resume from the cursor\nSELECT * FROM $table WHERE id > $offset ORDER BY id",
            "# resume from the cursor\nSELECT * FROM $table WHERE id > $offset ORDER BY id",
            "/* resume from\n the cursor */ SELECT * FROM $table WHERE id > $offset ORDER BY id",
            "WITH recent AS (SELECT * FROM $table WHERE id > $offset) \
             SELECT * FROM recent ORDER BY id",
            "(SELECT * FROM $table WHERE id > $offset) UNION (SELECT * FROM $table WHERE id > \
             $offset) ORDER BY id",
        ] {
            let result = source.validate_custom_query(query);
            assert!(result.is_ok(), "expected {query} to pass, got {result:?}");
        }
    }

    #[test]
    fn given_a_query_that_is_only_comments_should_fail_open() {
        // An unterminated block comment leaves no statement at all, which MySQL would
        // reject on the first poll rather than at startup.
        let source = MySqlSource::new(1, test_config(), None);
        for query in ["   ", "-- nothing here", "/* never closed"] {
            let result = source.validate_custom_query(query);
            assert!(
                matches!(result, Err(Error::InitError(ref message)) if message.contains("no statement")),
                "expected an InitError for {query:?}, got {result:?}"
            );
        }
    }
}
