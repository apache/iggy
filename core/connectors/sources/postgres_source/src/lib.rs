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
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use humantime::Duration as HumanDuration;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::types::{Oid, PgInterval, PgTimeTz};
use sqlx::{Column, Pool, Postgres, Row, TypeInfo, ValueRef};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

source_connector!(PostgresSource);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

#[derive(Debug)]
pub struct PostgresSource {
    pub id: u32,
    pool: Option<Pool<Postgres>>,
    config: PostgresSourceConfig,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
    poll_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_string: SecretString,
    pub mode: String,
    pub tables: Vec<String>,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub tracking_column: Option<String>,
    pub initial_offset: Option<String>,
    pub max_connections: Option<u32>,
    pub custom_query: Option<String>,
    pub snake_case_columns: Option<bool>,
    pub include_metadata: Option<bool>,
    pub replication_slot: Option<String>,
    pub capture_operations: Option<Vec<String>>,
    pub cdc_backend: Option<String>,
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

impl PayloadFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("bytea") | Some("raw") => PayloadFormat::Bytea,
            Some("text") => PayloadFormat::Text,
            Some("json_direct") | Some("jsonb") | Some("jsonb_direct") => PayloadFormat::JsonDirect,
            _ => PayloadFormat::Json,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    last_poll_time: DateTime<Utc>,
    tracking_offsets: HashMap<String, String>,
    processed_rows: u64,
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

struct ProcessedRow {
    message: ProducedMessage,
    max_offset: Option<String>,
    row_pk: Option<String>,
}

const CONNECTOR_NAME: &str = "PostgreSQL source";

impl PostgresSource {
    pub fn new(id: u32, config: PostgresSourceConfig, state: Option<ConnectorState>) -> Self {
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
        PostgresSource {
            id,
            pool: None,
            config,
            state: Mutex::new(restored_state.unwrap_or(State {
                last_poll_time: Utc::now(),
                tracking_offsets: HashMap::new(),
                processed_rows: 0,
            })),
            verbose,
            retry_delay,
            poll_interval,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening PostgreSQL source connector with ID: {}. Mode: {}, Tables: {:?}",
            self.id, self.config.mode, self.config.tables
        );

        self.connect().await?;

        match self.config.mode.as_str() {
            "cdc" => {
                self.setup_cdc().await?;
                let backend = self.config.cdc_backend.as_deref().unwrap_or("builtin");
                info!(
                    "PostgreSQL CDC mode enabled (backend: {backend}) for connector ID: {}",
                    self.id
                );
            }
            "polling" => {
                info!(
                    "PostgreSQL polling mode enabled for connector ID: {}",
                    self.id
                );
                info!("Poll interval: {:?}", self.poll_interval);
            }
            _ => {
                return Err(Error::InitError(format!(
                    "Invalid mode '{}'. Supported modes: 'polling', 'cdc'",
                    self.config.mode
                )));
            }
        }

        info!(
            "PostgreSQL source connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let poll_interval = self.poll_interval;
        tokio::time::sleep(poll_interval).await;

        let messages = match self.config.mode.as_str() {
            "polling" => self.poll_tables().await?,
            "cdc" => self.poll_cdc().await?,
            _ => {
                error!("Invalid mode: {}", self.config.mode);
                return Err(Error::InvalidConfig);
            }
        };

        let state = self.state.lock().await;
        if self.verbose {
            info!(
                "PostgreSQL source connector ID: {} produced {} messages. Total processed: {}",
                self.id,
                messages.len(),
                state.processed_rows
            );
        } else {
            debug!(
                "PostgreSQL source connector ID: {} produced {} messages. Total processed: {}",
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

        let persisted_state = self.serialize_state(&state);

        Ok(ProducedMessages {
            schema,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!(
                "PostgreSQL connection pool closed for connector ID: {}",
                self.id
            );
        }

        let state = self.state.lock().await;
        info!(
            "PostgreSQL source connector ID: {} closed. Total rows processed: {}",
            self.id, state.processed_rows
        );
        Ok(())
    }
}

impl PostgresSource {
    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(10);
        let redacted = redact_connection_string(self.config.connection_string.expose_secret());

        info!("Connecting to PostgreSQL with max {max_connections} connections: {redacted}");

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.config.connection_string.expose_secret())
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to PostgreSQL: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!("Connected to PostgreSQL database with {max_connections} max connections");
        Ok(())
    }

    async fn setup_cdc(&self) -> Result<(), Error> {
        let pool = self.get_pool()?;

        let wal_level: String = sqlx::query_scalar("SHOW wal_level")
            .fetch_one(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to check WAL level: {e}")))?;

        if wal_level != "logical" {
            return Err(Error::InitError(
                "WAL level must be 'logical' for CDC. Please set wal_level = logical in postgresql.conf".to_string()
            ));
        }

        for table in &self.config.tables {
            let exists: bool = if let Some((schema, name)) = table.split_once('.') {
                sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
                     WHERE table_schema = $1 AND table_name = $2)",
                )
                .bind(schema)
                .bind(name)
                .fetch_one(pool)
                .await
            } else {
                sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                )
                .bind(table)
                .fetch_one(pool)
                .await
            }
            .map_err(|e| Error::InitError(format!("Failed to validate table '{table}': {e}")))?;

            // Not fatal - the table may just not exist yet (e.g. a migration
            // runs shortly after startup).
            if !exists {
                warn!(
                    "Configured table '{table}' does not exist yet. If this is unexpected, \
                     check config.tables for typos."
                );
            }
        }

        let slot_name = self
            .config
            .replication_slot
            .as_deref()
            .unwrap_or("iggy_slot");

        let existing_plugin: Option<String> =
            sqlx::query_scalar("SELECT plugin FROM pg_replication_slots WHERE slot_name = $1")
                .bind(slot_name)
                .fetch_optional(pool)
                .await
                .map_err(|e| Error::InitError(format!("Failed to check replication slot: {e}")))?;

        match existing_plugin.as_deref() {
            Some("test_decoding") => {}
            Some(other) => {
                return Err(Error::InitError(format!(
                    "Replication slot '{slot_name}' already exists with plugin '{other}', \
                     expected 'test_decoding' (likely created by an older version). Drop it \
                     with SELECT pg_drop_replication_slot('{slot_name}') or set \
                     replication_slot to a new name."
                )));
            }
            None => {
                // test_decoding ignores publications entirely, so none is created
                // here. A future pgoutput-based backend would need one, since
                // that's the only way to get server-side table filtering under
                // that plugin.
                sqlx::query("SELECT pg_create_logical_replication_slot($1, 'test_decoding')")
                    .bind(slot_name)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        Error::InitError(format!("Failed to create replication slot: {e}"))
                    })?;
            }
        }

        info!("PostgreSQL CDC setup completed. Slot: {slot_name}");
        Ok(())
    }

    async fn poll_cdc(&self) -> Result<Vec<ProducedMessage>, Error> {
        let backend = self.config.cdc_backend.as_deref().unwrap_or("builtin");
        match backend {
            "builtin" => self.poll_cdc_builtin().await,
            "pg_replicate" => {
                #[cfg(feature = "cdc_pg_replicate")]
                {
                    Err(Error::InitError(
                        "pg_replicate backend not yet implemented".to_string(),
                    ))
                }
                #[cfg(not(feature = "cdc_pg_replicate"))]
                {
                    Err(Error::InitError(
                        "cdc_backend 'pg_replicate' requested but feature 'cdc_pg_replicate' is not enabled at build time".to_string(),
                    ))
                }
            }
            other => Err(Error::InitError(format!(
                "Unsupported cdc_backend '{other}'. Use 'builtin' or 'pg_replicate'"
            ))),
        }
    }

    async fn poll_cdc_builtin(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self.get_pool()?;

        let slot_name = self
            .config
            .replication_slot
            .as_deref()
            .unwrap_or("iggy_slot");
        let capture_ops = self
            .config
            .capture_operations
            .as_ref()
            .map(|ops| ops.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            .unwrap_or_else(|| vec!["INSERT", "UPDATE", "DELETE"]);
        let captured_tables =
            (!self.config.tables.is_empty()).then_some(self.config.tables.as_slice());
        let batch_size = self.config.batch_size.unwrap_or(1000) as i32;

        // Database I/O without holding the lock. upto_nchanges is only
        // checked at transaction-commit boundaries (a single huge transaction
        // can still exceed it), so this isn't a hard per-call cap - but it
        // stops the backlog from growing unbounded across many transactions
        // the way NULL (no limit at all) did.
        let rows =
            sqlx::query("SELECT lsn, xid, data FROM pg_logical_slot_get_changes($1, NULL, $2)")
                .bind(slot_name)
                .bind(batch_size)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to fetch CDC changes: {e}");
                    Error::InvalidRecord
                })?;

        let mut messages = Vec::new();

        for row in rows {
            let data: String = match row.try_get("data") {
                Ok(data) => data,
                Err(e) => {
                    error!("Skipping CDC row with unreadable data column: {e}");
                    continue;
                }
            };

            if let Some(change_record) =
                self.parse_logical_replication_message(&data, &capture_ops, captured_tables)
            {
                let payload = match simd_json::to_vec(&change_record) {
                    Ok(payload) => payload,
                    Err(e) => {
                        error!("Skipping CDC row that failed to serialize: {e}");
                        continue;
                    }
                };

                let message = ProducedMessage {
                    id: Some(Uuid::new_v4().as_u128()),
                    headers: None,
                    checksum: None,
                    timestamp: Some(Utc::now().timestamp_millis() as u64),
                    origin_timestamp: Some(Utc::now().timestamp_millis() as u64),
                    payload,
                };

                messages.push(message);
            }
        }

        // Update state with minimal lock time
        if !messages.is_empty() {
            let mut state = self.state.lock().await;
            state.processed_rows += messages.len() as u64;
        }

        if self.verbose {
            info!("CDC: Fetched {} change records", messages.len());
        } else {
            debug!("CDC: Fetched {} change records", messages.len());
        }
        Ok(messages)
    }

    async fn poll_tables(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self.get_pool()?;
        let mut messages = Vec::new();

        let batch_size = self.config.batch_size.unwrap_or(1000);
        let tracking_column = self.config.tracking_column.as_deref().unwrap_or("id");
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

        // Collect state updates to apply after processing
        let mut state_updates: Vec<(String, String)> = Vec::new();
        let mut total_processed: u64 = 0;

        for table in &self.config.tables {
            let table_config = RowProcessingConfig {
                table,
                ..row_config
            };

            // Get last offset with minimal lock time
            let last_offset = {
                let state = self.state.lock().await;
                state.tracking_offsets.get(table).cloned()
            };

            let query = if let Some(custom_query) = &self.config.custom_query {
                self.validate_custom_query(custom_query)?;
                self.substitute_query_params(custom_query, table, &last_offset, batch_size)
            } else {
                self.build_polling_query(table, tracking_column, &last_offset, batch_size)?
            };

            // Database I/O without holding the lock
            let rows = with_retry(
                || sqlx::query(sqlx::AssertSqlSafe(query.as_str())).fetch_all(pool),
                self.get_max_retries(),
                self.retry_delay.as_millis() as u64,
            )
            .await?;

            let mut max_offset: Option<String> = None;
            let mut processed_ids: Vec<String> = Vec::new();

            for row in rows {
                let processed = self.process_row(&row, &table_config)?;

                if let Some(pk) = processed.row_pk {
                    processed_ids.push(pk);
                }
                if let Some(offset) = processed.max_offset {
                    max_offset = Some(offset);
                }

                messages.push(processed.message);
                total_processed += 1;
            }

            // Database I/O without holding the lock
            if !processed_ids.is_empty() {
                self.mark_or_delete_processed_rows(pool, table, pk_column, &processed_ids)
                    .await?;
            }

            // Collect offset update for later
            if let Some(offset) = max_offset {
                state_updates.push((table.clone(), offset));
            }

            if self.verbose {
                info!("Fetched {} rows from table '{table}'", messages.len());
            } else {
                debug!("Fetched {} rows from table '{table}'", messages.len());
            }
        }

        // Apply all state updates with a single lock acquisition
        {
            let mut state = self.state.lock().await;
            state.processed_rows += total_processed;
            for (table, offset) in state_updates {
                state.tracking_offsets.insert(table, offset);
            }
            state.last_poll_time = Utc::now();
        }

        Ok(messages)
    }

    async fn mark_or_delete_processed_rows(
        &self,
        pool: &Pool<Postgres>,
        table: &str,
        pk_column: &str,
        ids: &[String],
    ) -> Result<(), Error> {
        if ids.is_empty() {
            return Ok(());
        }

        let quoted_table = quote_qualified_identifier(table)?;
        let quoted_pk = quote_identifier(pk_column)?;

        let ids_list = ids
            .iter()
            .map(|id| {
                if id.parse::<i64>().is_ok() {
                    id.clone()
                } else {
                    format!("'{}'", id.replace('\'', "''"))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        if self.config.delete_after_read.unwrap_or(false) {
            let delete_query =
                format!("DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({ids_list})");

            if self.verbose {
                info!("Deleting {} processed rows from '{table}'", ids.len());
            } else {
                debug!("Deleting {} processed rows from '{table}'", ids.len());
            }

            sqlx::query(sqlx::AssertSqlSafe(delete_query))
                .execute(pool)
                .await
                .map_err(|e| {
                    error!("Failed to delete processed rows: {e}");
                    Error::InvalidRecord
                })?;
        } else if let Some(processed_col) = &self.config.processed_column {
            let quoted_processed = quote_identifier(processed_col)?;
            let update_query = format!(
                "UPDATE {quoted_table} SET {quoted_processed} = TRUE WHERE {quoted_pk} IN ({ids_list})"
            );

            if self.verbose {
                info!("Marking {} rows as processed in '{table}'", ids.len());
            } else {
                debug!("Marking {} rows as processed in '{table}'", ids.len());
            }

            sqlx::query(sqlx::AssertSqlSafe(update_query))
                .execute(pool)
                .await
                .map_err(|e| {
                    error!("Failed to mark rows as processed: {e}");
                    Error::InvalidRecord
                })?;
        }

        Ok(())
    }

    fn get_pool(&self) -> Result<&Pool<Postgres>, Error> {
        self.pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }

    fn payload_format(&self) -> PayloadFormat {
        if let Some(ref payload_col) = self.config.payload_column
            && !payload_col.is_empty()
        {
            return PayloadFormat::from_config(self.config.payload_format.as_deref());
        }
        PayloadFormat::Json
    }

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    fn build_polling_query(
        &self,
        table: &str,
        tracking_column: &str,
        last_offset: &Option<String>,
        batch_size: u32,
    ) -> Result<String, Error> {
        let quoted_table = quote_qualified_identifier(table)?;
        let quoted_tracking = quote_identifier(tracking_column)?;

        let base_query = format!("SELECT * FROM {quoted_table}");

        let mut conditions = Vec::new();

        if let Some(offset) = last_offset {
            conditions.push(format!(
                "{quoted_tracking} > {}",
                format_offset_value(offset)
            ));
        } else if let Some(initial) = &self.config.initial_offset {
            conditions.push(format!(
                "{quoted_tracking} > {}",
                format_offset_value(initial)
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

    fn validate_custom_query(&self, query: &str) -> Result<(), Error> {
        let query_upper = query.to_uppercase();
        if !query_upper.contains("SELECT") {
            warn!("Custom query should contain SELECT statement");
        }
        if query.contains("$table") && self.config.tables.is_empty() {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }

    fn substitute_query_params(
        &self,
        query: &str,
        table: &str,
        last_offset: &Option<String>,
        batch_size: u32,
    ) -> String {
        let offset_value = last_offset
            .clone()
            .or_else(|| self.config.initial_offset.clone())
            .unwrap_or_default();

        let now = Utc::now();

        query
            .replace("$table", table)
            .replace("$offset", &offset_value)
            .replace("$limit", &batch_size.to_string())
            .replace("$now", &now.to_rfc3339())
            .replace("$now_unix", &now.timestamp().to_string())
    }

    fn parse_logical_replication_message(
        &self,
        data: &str,
        capture_ops: &[&str],
        captured_tables: Option<&[String]>,
    ) -> Option<DatabaseRecord> {
        if data.starts_with("BEGIN") || data.starts_with("COMMIT") {
            return None;
        }

        let rest = data.strip_prefix("table ")?;
        let (qualified_table, rest) = rest.split_once(": ")?;
        let (operation, rest) = rest.split_once(": ")?;

        if !matches!(operation, "INSERT" | "UPDATE" | "DELETE") || !capture_ops.contains(&operation)
        {
            return None;
        }

        let table_name = unquote_pg_identifier(
            qualified_table
                .rsplit('.')
                .next()
                .unwrap_or(qualified_table),
        );
        let unquoted_qualified_table = qualified_table
            .split('.')
            .map(unquote_pg_identifier)
            .collect::<Vec<_>>()
            .join(".");

        // test_decoding has no server-side table filter, so Postgres already
        // sent us every table's changes - config.tables scoping happens here.
        if let Some(tables) = captured_tables
            && !tables.iter().any(|t| {
                if t.contains('.') {
                    t == &unquoted_qualified_table
                } else {
                    t == &table_name
                }
            })
        {
            return None;
        }

        let rest = rest
            .strip_prefix("old-key: ")
            .and_then(|old_key| old_key.split_once("new-tuple: "))
            .map_or(rest, |(_, new_tuple)| new_tuple);

        Some(DatabaseRecord {
            table_name,
            operation_type: operation.to_string(),
            timestamp: Utc::now(),
            data: serde_json::Value::Object(parse_record_columns(rest)),
            old_data: None,
        })
    }

    fn process_row(
        &self,
        row: &sqlx::postgres::PgRow,
        config: &RowProcessingConfig,
    ) -> Result<ProcessedRow, Error> {
        let mut row_pk: Option<String> = None;
        let mut max_offset: Option<String> = None;
        let mut extracted_payload: Option<Vec<u8>> = None;
        let mut data = serde_json::Map::new();

        for (i, column) in row.columns().iter().enumerate() {
            let column_name = if config.snake_case_columns {
                to_snake_case(column.name())
            } else {
                column.name().to_string()
            };

            if !config.payload_col.is_empty() && column.name() == config.payload_col {
                extracted_payload =
                    Some(self.extract_payload_column(row, i, config.payload_format)?);
                continue;
            }

            let value = extract_column_value(row, i)?;
            data.insert(column_name.clone(), value.clone());

            if column.name() == config.tracking_column {
                if let serde_json::Value::String(ref s) = value {
                    max_offset = Some(s.clone());
                } else if let serde_json::Value::Number(ref n) = value {
                    max_offset = Some(n.to_string());
                }
            }

            if column.name() == config.pk_column {
                if let serde_json::Value::String(ref s) = value {
                    row_pk = Some(s.clone());
                } else if let serde_json::Value::Number(ref n) = value {
                    row_pk = Some(n.to_string());
                }
            }
        }

        let payload = if let Some(bytes) = extracted_payload {
            bytes
        } else {
            let record = if config.include_metadata {
                DatabaseRecord {
                    table_name: config.table.to_string(),
                    operation_type: "SELECT".to_string(),
                    timestamp: Utc::now(),
                    data: serde_json::Value::Object(data),
                    old_data: None,
                }
            } else {
                let mut simple_record = serde_json::Map::new();
                simple_record.insert("data".to_string(), serde_json::Value::Object(data));
                DatabaseRecord {
                    table_name: config.table.to_string(),
                    operation_type: "SELECT".to_string(),
                    timestamp: Utc::now(),
                    data: serde_json::Value::Object(simple_record),
                    old_data: None,
                }
            };
            simd_json::to_vec(&record).map_err(|_| Error::InvalidRecord)?
        };

        let message = ProducedMessage {
            id: Some(Uuid::new_v4().as_u128()),
            headers: None,
            checksum: None,
            timestamp: Some(Utc::now().timestamp_millis() as u64),
            origin_timestamp: Some(Utc::now().timestamp_millis() as u64),
            payload,
        };

        Ok(ProcessedRow {
            message,
            max_offset,
            row_pk,
        })
    }

    fn extract_payload_column(
        &self,
        row: &sqlx::postgres::PgRow,
        column_index: usize,
        format: PayloadFormat,
    ) -> Result<Vec<u8>, Error> {
        match format {
            PayloadFormat::Bytea => {
                let bytes: Option<Vec<u8>> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(bytes.unwrap_or_default())
            }
            PayloadFormat::Text => {
                let text: Option<String> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(text.unwrap_or_default().into_bytes())
            }
            PayloadFormat::JsonDirect => {
                let json_value: Option<serde_json::Value> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                simd_json::to_vec(&json_value.unwrap_or(serde_json::Value::Null))
                    .map_err(|_| Error::InvalidRecord)
            }
            PayloadFormat::Json => {
                let bytes: Option<Vec<u8>> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(bytes.unwrap_or_default())
            }
        }
    }
}

fn extract_column_value(
    row: &sqlx::postgres::PgRow,
    column_index: usize,
) -> Result<serde_json::Value, Error> {
    let column = &row.columns()[column_index];
    let type_name = column.type_info().name();

    match type_name {
        "BOOL" => {
            let value: Option<bool> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null))
        }
        "INT2" => {
            let value: Option<i16> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "INT4" => {
            let value: Option<i32> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "OID" => {
            let value: Option<Oid> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v.0 as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "INT8" => {
            let value: Option<i64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT4" => {
            let value: Option<f32> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as f64))
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT8" => {
            let value: Option<f64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "NUMERIC" => {
            let value: Option<String> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .and_then(|s| s.parse::<f64>().ok())
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "VARCHAR" | "TEXT" | "CHAR" | "NAME" | "BPCHAR" => {
            let value: Option<String> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null))
        }
        "DATE" => {
            let value: Option<NaiveDate> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|d| serde_json::Value::String(d.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIME" => {
            let value: Option<NaiveTime> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|t| serde_json::Value::String(t.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMETZ" => {
            let value: Option<PgTimeTz> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|tz| serde_json::Value::String(format!("{}{}", tz.time, tz.offset)))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMESTAMP" => {
            let value: Option<NaiveDateTime> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|dt| serde_json::Value::String(dt.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMESTAMPTZ" => {
            let value: Option<DateTime<Utc>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|dt| serde_json::Value::String(dt.to_rfc3339()))
                .unwrap_or(serde_json::Value::Null))
        }
        "INTERVAL" => {
            let value: Option<PgInterval> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|iv| serde_json::Value::String(format_pg_interval(&iv)))
                .unwrap_or(serde_json::Value::Null))
        }
        "UUID" => {
            let value: Option<Uuid> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|u| serde_json::Value::String(u.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "JSON" | "JSONB" => {
            let value: Option<serde_json::Value> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value.unwrap_or(serde_json::Value::Null))
        }
        "BYTEA" => {
            let value: Option<Vec<u8>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|bytes| {
                    use base64::Engine;
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(&bytes),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "BOOL[]" => {
            let value: Option<Vec<Option<bool>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(serde_json::Value::Bool)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "INT2[]" => {
            let value: Option<Vec<Option<i16>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|n| serde_json::Value::from(n as i64))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "INT4[]" => {
            let value: Option<Vec<Option<i32>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|n| serde_json::Value::from(n as i64))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "OID[]" => {
            let value: Option<Vec<Option<Oid>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|n| serde_json::Value::from(n.0 as u64))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "INT8[]" => {
            let value: Option<Vec<Option<i64>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(serde_json::Value::from)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT4[]" => {
            let value: Option<Vec<Option<f32>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|n| serde_json::Value::from(n as f64))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT8[]" => {
            let value: Option<Vec<Option<f64>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(serde_json::Value::from)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "TEXT[]" | "VARCHAR[]" | "CHAR[]" => {
            let value: Option<Vec<Option<String>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(serde_json::Value::String)
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "UUID[]" => {
            let value: Option<Vec<Option<Uuid>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|u| serde_json::Value::String(u.to_string()))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "JSON[]" | "JSONB[]" => {
            let value: Option<Vec<Option<serde_json::Value>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| v.unwrap_or(serde_json::Value::Null))
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "DATE[]" => {
            let value: Option<Vec<Option<NaiveDate>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|d| serde_json::Value::String(d.to_string()))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "TIME[]" => {
            let value: Option<Vec<Option<NaiveTime>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|t| serde_json::Value::String(t.to_string()))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMESTAMP[]" => {
            let value: Option<Vec<Option<NaiveDateTime>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|dt| serde_json::Value::String(dt.to_string()))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "TIMESTAMPTZ[]" => {
            let value: Option<Vec<Option<DateTime<Utc>>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|dt| serde_json::Value::String(dt.to_rfc3339()))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "INTERVAL[]" => {
            let value: Option<Vec<Option<PgInterval>>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|arr| {
                    serde_json::Value::Array(
                        arr.into_iter()
                            .map(|v| {
                                v.map(|iv| serde_json::Value::String(format_pg_interval(&iv)))
                                    .unwrap_or(serde_json::Value::Null)
                            })
                            .collect(),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        _ => {
            let column_name = column.name();
            warn!(
                "Column '{column_name}' has unrecognized Postgres type '{type_name}', \
                 attempting raw text extraction"
            );
            let raw = row.try_get_raw(column_index).map_err(|e| {
                error!("Failed to read column '{column_name}' (type '{type_name}'): {e}");
                Error::InvalidRecordValue(format!(
                    "column '{column_name}' has unsupported Postgres type '{type_name}'"
                ))
            })?;
            if raw.is_null() {
                return Ok(serde_json::Value::Null);
            }
            match raw.as_str() {
                Ok(text) => Ok(serde_json::Value::String(text.to_owned())),
                Err(_) => {
                    use base64::Engine;
                    let bytes = raw.as_bytes().map_err(|e| {
                        error!(
                            "Failed to read column '{column_name}' \
                             (type '{type_name}') as bytes: {e}"
                        );
                        Error::InvalidRecordValue(format!(
                            "column '{column_name}' has unsupported Postgres type '{type_name}'"
                        ))
                    })?;
                    Ok(serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(bytes),
                    ))
                }
            }
        }
    }
}

fn format_pg_interval(interval: &PgInterval) -> String {
    let mut parts = Vec::new();

    let years = interval.months / 12;
    let months = interval.months % 12;

    if years != 0 {
        parts.push(format!(
            "{years} year{}",
            if years.unsigned_abs() != 1 { "s" } else { "" }
        ));
    }
    if months != 0 {
        parts.push(format!(
            "{months} mon{}",
            if months.unsigned_abs() != 1 { "s" } else { "" }
        ));
    }
    if interval.days != 0 {
        parts.push(format!(
            "{} day{}",
            interval.days,
            if interval.days.unsigned_abs() != 1 {
                "s"
            } else {
                ""
            }
        ));
    }
    if interval.microseconds != 0 || parts.is_empty() {
        let negative = interval.microseconds < 0;
        let abs_us = interval.microseconds.unsigned_abs();
        let total_secs = abs_us / 1_000_000;
        let remaining_us = abs_us % 1_000_000;
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        let sign = if negative { "-" } else { "" };
        if remaining_us != 0 {
            parts.push(format!(
                "{sign}{:02}:{:02}:{:02}.{:06}",
                hours, mins, secs, remaining_us
            ));
        } else {
            parts.push(format!("{sign}{hours:02}:{mins:02}:{secs:02}"));
        }
    }

    parts.join(" ")
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
    let escaped = name.replace('"', "\"\"");
    Ok(format!("\"{escaped}\""))
}

/// Quote a possibly schema-qualified identifier like `public.users` as
/// `"public"."users"`. Each dot-separated segment is validated and quoted
/// independently so that schema-qualified table names survive intact.
fn quote_qualified_identifier(name: &str) -> Result<String, Error> {
    if !name.contains('.') {
        return quote_identifier(name);
    }
    let parts: Result<Vec<_>, _> = name.split('.').map(quote_identifier).collect();
    Ok(parts?.join("."))
}

fn format_offset_value(value: &str) -> String {
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
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
            if let Some(lowercase_ch) = ch.to_lowercase().next() {
                result.push(lowercase_ch);
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

fn unquote_pg_identifier(segment: &str) -> String {
    match segment.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => segment.to_string(),
    }
}

fn parse_record_columns(data: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut result = serde_json::Map::new();
    let bytes = data.as_bytes();
    let len = bytes.len();
    let mut pos = 0;

    while pos < len {
        while pos < len && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= len {
            break;
        }

        let Some(bracket_offset) = data[pos..].find('[') else {
            break;
        };
        let name_end = pos + bracket_offset;
        let column_name = unquote_pg_identifier(&data[pos..name_end]);

        let mut depth = 0;
        let mut type_end = name_end;
        loop {
            match bytes.get(type_end) {
                Some(b'[') => depth += 1,
                Some(b']') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                Some(_) => {}
                None => return result,
            }
            type_end += 1;
        }
        if bytes.get(type_end + 1) != Some(&b':') {
            break;
        }

        let (value, next_pos) = parse_column_value(data, type_end + 2);
        result.insert(column_name, value);
        pos = next_pos;
    }

    result
}

fn parse_column_value(data: &str, start: usize) -> (serde_json::Value, usize) {
    let bytes = data.as_bytes();

    if bytes.get(start) != Some(&b'\'') {
        let end = data[start..]
            .find(' ')
            .map_or(data.len(), |offset| start + offset);
        return (parse_bare_scalar(&data[start..end]), end);
    }

    let mut value = String::new();
    let mut pos = start + 1;
    while pos < bytes.len() {
        if bytes[pos] == b'\'' {
            if bytes.get(pos + 1) == Some(&b'\'') {
                value.push('\'');
                pos += 2;
                continue;
            }
            pos += 1;
            break;
        }
        let ch = data[pos..].chars().next().unwrap_or('\u{FFFD}');
        value.push(ch);
        pos += ch.len_utf8();
    }
    (serde_json::Value::String(value), pos)
}

fn parse_bare_scalar(token: &str) -> serde_json::Value {
    // test_decoding emits this sentinel for TOASTed columns the UPDATE didn't touch;
    // treating it as null avoids leaking the literal token as a fake column value.
    match token {
        "null" | "unchanged-toast-datum" => serde_json::Value::Null,
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        _ => {
            if let Ok(i) = token.parse::<i64>() {
                serde_json::Value::Number(serde_json::Number::from(i))
            } else if let Ok(f) = token.parse::<f64>()
                && let Some(num) = serde_json::Number::from_f64(f)
            {
                serde_json::Value::Number(num)
            } else {
                serde_json::Value::String(token.to_string())
            }
        }
    }
}

async fn with_retry<T, F, Fut>(operation: F, max_retries: u32, delay_ms: u64) -> Result<T, Error>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let mut attempts = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries || !is_transient_error(&e) {
                    error!("Database operation failed after {attempts} attempts: {e}");
                    return Err(Error::InvalidRecord);
                }
                warn!(
                    "Transient database error (attempt {attempts}/{max_retries}): {e}. Retrying in {delay_ms}ms..."
                );
                tokio::time::sleep(Duration::from_millis(delay_ms * attempts as u64)).await;
            }
        }
    }
}

fn is_transient_error(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_) => true,
        sqlx::Error::PoolTimedOut => true,
        sqlx::Error::PoolClosed => false,
        sqlx::Error::Protocol(_) => false,
        sqlx::Error::Database(db_err) => db_err.code().is_some_and(|code| {
            matches!(
                code.as_ref(),
                "40001" | "40P01" | "57P01" | "57P02" | "57P03" | "08000" | "08003" | "08006"
            )
        }),
        _ => false,
    }
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

#[cfg(test)]
#[path = "cdc_fixtures.rs"]
mod cdc_fixtures;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PostgresSourceConfig {
        PostgresSourceConfig {
            connection_string: SecretString::from("postgres://localhost/db"),
            mode: "polling".to_string(),
            tables: vec!["users".to_string()],
            poll_interval: Some("5s".to_string()),
            batch_size: Some(500),
            tracking_column: Some("updated_at".to_string()),
            initial_offset: None,
            max_connections: None,
            custom_query: None,
            snake_case_columns: None,
            include_metadata: None,
            replication_slot: None,
            capture_operations: None,
            cdc_backend: None,
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
    fn given_last_offset_polling_query_should_be_built() {
        let src = PostgresSource::new(1, test_config(), None);
        let query = src
            .build_polling_query("users", "updated_at", &Some("2024-01-01".to_string()), 500)
            .expect("Failed to build query");
        assert_eq!(
            query,
            "SELECT * FROM \"users\" WHERE \"updated_at\" > '2024-01-01' ORDER BY \"updated_at\" ASC LIMIT 500"
        );
    }

    #[test]
    fn given_initial_offset_polling_query_should_be_built() {
        let mut config = test_config();
        config.tracking_column = Some("id".to_string());
        config.initial_offset = Some("100".to_string());
        let src = PostgresSource::new(1, config, None);
        let query = src
            .build_polling_query("users", "id", &None, 1000)
            .expect("Failed to build query");
        assert_eq!(
            query,
            "SELECT * FROM \"users\" WHERE \"id\" > 100 ORDER BY \"id\" ASC LIMIT 1000"
        );
    }

    #[test]
    fn given_processed_column_polling_query_should_include_filter() {
        let mut config = test_config();
        config.processed_column = Some("is_processed".to_string());
        let src = PostgresSource::new(1, config, None);
        let query = src
            .build_polling_query("events", "id", &None, 100)
            .expect("Failed to build query");
        assert!(query.contains("\"is_processed\" = FALSE"));
    }

    #[test]
    fn given_numeric_offset_should_not_quote_value() {
        let src = PostgresSource::new(1, test_config(), None);
        let query = src
            .build_polling_query("users", "id", &Some("42".to_string()), 100)
            .expect("Failed to build query");
        assert!(query.contains("\"id\" > 42"));
        assert!(!query.contains("'42'"));
    }

    #[test]
    fn given_special_chars_in_identifier_should_escape() {
        let result = quote_identifier("table\"name").expect("Failed to quote");
        assert_eq!(result, "\"table\"\"name\"");
    }

    #[test]
    fn given_empty_identifier_should_fail() {
        let result = quote_identifier("");
        assert!(result.is_err());
    }

    #[test]
    fn given_unqualified_name_should_quote_as_single_identifier() {
        let result = quote_qualified_identifier("users").expect("Failed to quote");
        assert_eq!(result, "\"users\"");
    }

    #[test]
    fn given_schema_qualified_name_should_quote_each_segment() {
        let result = quote_qualified_identifier("public.users").expect("Failed to quote");
        assert_eq!(result, "\"public\".\"users\"");
    }

    #[test]
    fn given_qualified_name_with_quote_chars_should_escape_each_segment() {
        let result = quote_qualified_identifier("my\"schema.my\"table").expect("Failed to quote");
        assert_eq!(result, "\"my\"\"schema\".\"my\"\"table\"");
    }

    #[test]
    fn given_qualified_name_with_empty_segment_should_fail() {
        assert!(quote_qualified_identifier("public.").is_err());
        assert!(quote_qualified_identifier(".users").is_err());
    }

    fn cdc_source() -> PostgresSource {
        let mut config = test_config();
        config.mode = "cdc".to_string();
        PostgresSource::new(1, config, None)
    }

    #[test]
    fn given_insert_single_row_all_types_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_SINGLE_ROW_ALL_TYPES,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.table_name, "probe_events");
        assert_eq!(rec.operation_type, "INSERT");
        assert_eq!(rec.data["id"], serde_json::json!(2));
        assert_eq!(rec.data["name"], serde_json::json!("alice"));
        assert_eq!(rec.data["note"], serde_json::json!("first note"));
        assert_eq!(rec.data["amount"], serde_json::json!(12.50));
        assert_eq!(rec.data["active"], serde_json::json!(true));
        assert_eq!(rec.data["tags"], serde_json::json!("{a,b}"));
        assert_eq!(rec.data["payload"], serde_json::json!(r#"{"k": 1}"#));
        assert_eq!(rec.data["small_int"], serde_json::Value::Null);
    }

    #[test]
    fn given_insert_with_nulls_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::INSERT_WITH_NULLS, &["INSERT"], None)
            .unwrap();

        assert_eq!(rec.data["name"], serde_json::json!("bob"));
        assert_eq!(rec.data["note"], serde_json::Value::Null);
        assert_eq!(rec.data["amount"], serde_json::Value::Null);
        assert_eq!(rec.data["active"], serde_json::Value::Null);
    }

    #[test]
    fn given_multi_row_insert_statement_should_parse_each_row() {
        let src = cdc_source();
        for (fixture, name) in cdc_fixtures::INSERT_MULTI_ROW_SINGLE_STATEMENT
            .iter()
            .zip(["carol", "dave"])
        {
            let rec = src
                .parse_logical_replication_message(fixture, &["INSERT"], None)
                .unwrap();
            assert_eq!(rec.data["name"], serde_json::json!(name));
        }
    }

    #[test]
    fn given_multi_statement_transaction_should_parse_each_insert() {
        let src = cdc_source();
        for (fixture, name) in cdc_fixtures::INSERT_MULTI_STATEMENT_ONE_TRANSACTION
            .iter()
            .zip(["eve", "frank"])
        {
            let rec = src
                .parse_logical_replication_message(fixture, &["INSERT"], None)
                .unwrap();
            assert_eq!(rec.data["name"], serde_json::json!(name));
        }
    }

    #[test]
    fn given_update_single_column_should_keep_other_columns() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::UPDATE_SINGLE_COLUMN,
                &["UPDATE"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["note"], serde_json::json!("only note changed"));
        assert_eq!(rec.data["name"], serde_json::json!("bob"));
    }

    #[test]
    fn given_delete_multiple_rows_should_parse_each_row() {
        let src = cdc_source();
        for (fixture, id) in cdc_fixtures::DELETE_MULTIPLE_ROWS.iter().zip([6, 7]) {
            let rec = src
                .parse_logical_replication_message(fixture, &["DELETE"], None)
                .unwrap();
            assert_eq!(rec.data["id"], serde_json::json!(id));
        }
    }

    #[test]
    fn given_update_array_column_should_parse_new_array() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::UPDATE_ARRAY_COLUMN, &["UPDATE"], None)
            .unwrap();

        assert_eq!(rec.data["int_array"], serde_json::json!("{9,8,7}"));
    }

    #[test]
    fn given_negative_zero_float_should_parse_as_zero() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_NEGATIVE_ZERO_FLOAT,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["real_val"], serde_json::json!(0));
        assert_eq!(rec.data["double_val"], serde_json::json!(0));
    }

    #[test]
    fn given_quoted_mixed_case_column_should_strip_quotes_from_key() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_QUOTED_MIXED_CASE_COLUMN,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["user"], serde_json::json!("quoted_row"));
        assert!(rec.data.get("createdAt").is_some());
        assert!(rec.data.get("\"user\"").is_none());
    }

    #[test]
    fn given_unchanged_toast_datum_should_parse_as_null() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::UPDATE_UNCHANGED_TOAST_COLUMN,
                &["UPDATE"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["note"], serde_json::Value::Null);
        assert_eq!(rec.data["payload"], serde_json::Value::Null);
        assert_eq!(rec.data["name"], serde_json::json!("toast_row"));
    }

    #[test]
    fn given_update_full_row_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::UPDATE_FULL_ROW, &["UPDATE"], None)
            .unwrap();

        assert_eq!(rec.table_name, "probe_events");
        assert_eq!(rec.operation_type, "UPDATE");
        assert_eq!(rec.data["name"], serde_json::json!("alice2"));
        assert_eq!(rec.data["amount"], serde_json::json!(99.99));
        assert_eq!(rec.data["active"], serde_json::json!(false));
    }

    #[test]
    fn given_update_to_null_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::UPDATE_TO_NULL, &["UPDATE"], None)
            .unwrap();

        assert_eq!(rec.data["note"], serde_json::Value::Null);
        assert_eq!(rec.data["amount"], serde_json::json!(99.99));
    }

    #[test]
    fn given_update_primary_key_should_parse_new_tuple_only() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::UPDATE_PRIMARY_KEY, &["UPDATE"], None)
            .unwrap();

        assert_eq!(rec.data["id"], serde_json::json!(1004));
        assert_eq!(rec.data["name"], serde_json::json!("carol"));
    }

    #[test]
    fn given_delete_row_should_parse_replica_identity_columns() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::DELETE_ROW, &["DELETE"], None)
            .unwrap();

        assert_eq!(rec.table_name, "probe_events");
        assert_eq!(rec.operation_type, "DELETE");
        assert_eq!(rec.data["id"], serde_json::json!(5));
        assert_eq!(rec.data.as_object().unwrap().len(), 1);
    }

    #[test]
    fn given_structurally_broken_rows_should_return_none() {
        let src = cdc_source();
        let capture_ops = ["INSERT", "UPDATE", "DELETE"];
        let bad_rows = [
            "",
            "garbage",
            "table",
            "table public.probe_events",
            "table public.probe_events:",
            "table public.probe_events: INSERT",
            "table public.probe_events: INSERT:",
        ];

        for bad_row in bad_rows {
            assert!(
                src.parse_logical_replication_message(bad_row, &capture_ops, None)
                    .is_none(),
                "Expected None for structurally broken row: {bad_row:?}"
            );
        }
    }

    #[test]
    fn given_broken_column_syntax_should_not_panic() {
        let src = cdc_source();
        let capture_ops = ["INSERT", "UPDATE", "DELETE"];
        let bad_rows = [
            "table : INSERT: id[integer]:1",
            "table public.probe_events: INSERT: id[integer",
            "table public.probe_events: INSERT: id[integer]",
            "table public.probe_events: INSERT: id[integer]:",
            "table public.probe_events: INSERT: id[integer]:'unterminated",
        ];

        for bad_row in bad_rows {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                src.parse_logical_replication_message(bad_row, &capture_ops, None)
            }));
            assert!(
                result.is_ok(),
                "Parsing must not panic on malformed column data: {bad_row:?}"
            );
        }
    }

    #[test]
    fn given_truncate_message_should_be_ignored() {
        let src = cdc_source();
        assert!(
            src.parse_logical_replication_message(
                cdc_fixtures::TRUNCATE_TABLE,
                &["INSERT", "UPDATE", "DELETE"],
                None,
            )
            .is_none()
        );
    }

    #[test]
    fn given_operation_not_in_capture_ops_should_be_ignored() {
        let src = cdc_source();
        assert!(
            src.parse_logical_replication_message(
                cdc_fixtures::DELETE_ROW,
                &["INSERT", "UPDATE"],
                None
            )
            .is_none()
        );
    }

    #[test]
    fn given_schema_qualified_captured_table_should_match_exact_schema_only() {
        let src = cdc_source();
        let matching = vec!["public.probe_events".to_string()];
        let other_schema = vec!["other_schema.probe_events".to_string()];

        assert!(
            src.parse_logical_replication_message(
                cdc_fixtures::INSERT_SINGLE_ROW_ALL_TYPES,
                &["INSERT"],
                Some(&matching),
            )
            .is_some()
        );
        assert!(
            src.parse_logical_replication_message(
                cdc_fixtures::INSERT_SINGLE_ROW_ALL_TYPES,
                &["INSERT"],
                Some(&other_schema),
            )
            .is_none()
        );
    }

    #[test]
    fn given_bare_captured_table_should_match_regardless_of_schema() {
        let src = cdc_source();
        let bare = vec!["probe_events".to_string()];

        assert!(
            src.parse_logical_replication_message(
                cdc_fixtures::INSERT_SINGLE_ROW_ALL_TYPES,
                &["INSERT"],
                Some(&bare),
            )
            .is_some()
        );
    }

    #[test]
    fn given_mixed_case_table_name_should_unquote_for_table_name_and_filter() {
        let src = cdc_source();
        let data = r#"table public."MyTable": INSERT: id[integer]:1 name[text]:'alice'"#;

        let rec = src
            .parse_logical_replication_message(data, &["INSERT"], None)
            .unwrap();
        assert_eq!(rec.table_name, "MyTable");

        let bare = vec!["MyTable".to_string()];
        assert!(
            src.parse_logical_replication_message(data, &["INSERT"], Some(&bare))
                .is_some(),
            "Unquoted config entry must match the unquoted table name"
        );

        let qualified = vec!["public.MyTable".to_string()];
        assert!(
            src.parse_logical_replication_message(data, &["INSERT"], Some(&qualified))
                .is_some(),
            "Schema-qualified config entry must match against the unquoted qualified name"
        );

        let wrong_case = vec!["mytable".to_string()];
        assert!(
            src.parse_logical_replication_message(data, &["INSERT"], Some(&wrong_case))
                .is_none(),
            "Postgres identifiers are case-sensitive once quoted - must not fuzzy-match"
        );
    }

    #[test]
    fn given_quoted_identifier_with_embedded_quote_should_unescape() {
        let src = cdc_source();
        let data = r#"table public."Weird""Table": INSERT: id[integer]:1"#;

        let rec = src
            .parse_logical_replication_message(data, &["INSERT"], None)
            .unwrap();
        assert_eq!(rec.table_name, "Weird\"Table");
    }

    #[test]
    fn given_new_tuple_substring_in_value_should_not_truncate_ordinary_update() {
        let src = cdc_source();
        let data = "table public.probe_events: UPDATE: id[integer]:2 \
                     note[text]:'see new-tuple: format docs' amount[numeric]:99.99";

        let rec = src
            .parse_logical_replication_message(data, &["UPDATE"], None)
            .unwrap();

        assert_eq!(rec.data["id"], serde_json::json!(2));
        assert_eq!(
            rec.data["note"],
            serde_json::json!("see new-tuple: format docs")
        );
        assert_eq!(rec.data["amount"], serde_json::json!(99.99));
    }

    #[test]
    fn given_unicode_and_escaped_quote_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::UNICODE_AND_SPECIAL_CHARS,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(
            rec.data["note"],
            serde_json::json!("emoji \u{1F680} quote' backslash\\ newline\nend")
        );
    }

    #[test]
    fn given_extended_types_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_EXTENDED_TYPES_NORMAL_VALUES,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["small_int"], serde_json::json!(42));
        assert_eq!(rec.data["big_int"], serde_json::json!(9000000000i64));
        assert_eq!(
            rec.data["real_val"],
            serde_json::json!("3.14".parse::<f64>().unwrap())
        );
        assert_eq!(
            rec.data["uuid_val"],
            serde_json::json!("11111111-1111-1111-1111-111111111111")
        );
        assert_eq!(rec.data["bytea_val"], serde_json::json!("\\xdeadbeef"));
        assert_eq!(rec.data["date_val"], serde_json::json!("2024-01-15"));
        assert_eq!(rec.data["int_array"], serde_json::json!("{1,2,3}"));
        assert_eq!(rec.data["char_val"], serde_json::json!("ab        "));
    }

    #[test]
    fn given_nan_should_parse_as_string() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::INSERT_NUMERIC_NAN, &["INSERT"], None)
            .unwrap();

        assert_eq!(rec.data["real_val"], serde_json::json!("NaN"));
        assert_eq!(rec.data["numeric_val"], serde_json::json!("NaN"));
    }

    #[test]
    fn given_infinity_should_parse_as_string() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_NUMERIC_INFINITY,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["real_val"], serde_json::json!("Infinity"));
        assert_eq!(rec.data["double_val"], serde_json::json!("-Infinity"));
    }

    #[test]
    fn given_negative_and_boundary_numbers_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_NEGATIVE_AND_BOUNDARY_NUMBERS,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["small_int"], serde_json::json!(-32768));
        assert_eq!(
            rec.data["big_int"],
            serde_json::json!(-9223372036854775808i64)
        );
    }

    #[test]
    fn given_max_boundary_numbers_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_MAX_BOUNDARY_NUMBERS,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["small_int"], serde_json::json!(32767));
        assert_eq!(
            rec.data["big_int"],
            serde_json::json!(9223372036854775807i64)
        );
    }

    #[test]
    fn given_empty_string_vs_null_should_be_distinct() {
        let src = cdc_source();
        let empty_row = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_EMPTY_STRING_VS_NULL[0],
                &["INSERT"],
                None,
            )
            .unwrap();
        let null_row = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_EMPTY_STRING_VS_NULL[1],
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(empty_row.data["note"], serde_json::json!(""));
        assert_eq!(null_row.data["note"], serde_json::Value::Null);
    }

    #[test]
    fn given_array_with_null_element_should_parse_as_raw_string() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_ARRAY_WITH_NULL_ELEMENT,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["int_array"], serde_json::json!("{1,2,NULL,4}"));
    }

    #[test]
    fn given_empty_array_should_parse_correctly() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(cdc_fixtures::INSERT_EMPTY_ARRAY, &["INSERT"], None)
            .unwrap();

        assert_eq!(rec.data["int_array"], serde_json::json!("{}"));
    }

    #[test]
    fn given_char_padding_vs_varchar_should_preserve_padding() {
        let src = cdc_source();
        let rec = src
            .parse_logical_replication_message(
                cdc_fixtures::INSERT_CHAR_PADDING_VS_VARCHAR,
                &["INSERT"],
                None,
            )
            .unwrap();

        assert_eq!(rec.data["char_val"], serde_json::json!("ab        "));
        assert_eq!(rec.data["varchar_val"], serde_json::json!("ab"));
    }

    #[test]
    fn given_custom_query_params_should_substitute_correctly() {
        let mut config = test_config();
        config.initial_offset = Some("0".to_string());
        let src = PostgresSource::new(1, config, None);

        let query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit";
        let result = src.substitute_query_params(query, "events", &Some("100".to_string()), 50);

        assert!(result.contains("FROM events"));
        assert!(result.contains("id > 100"));
        assert!(result.contains("LIMIT 50"));
    }

    #[test]
    fn given_custom_query_with_time_params_should_substitute_correctly() {
        let src = PostgresSource::new(1, test_config(), None);

        let query = "SELECT * FROM $table WHERE created_at < '$now'";
        let result = src.substitute_query_params(query, "logs", &None, 100);

        assert!(result.contains("FROM logs"));
        assert!(!result.contains("$now"));
    }

    #[test]
    fn given_no_last_offset_should_use_initial_offset() {
        let mut config = test_config();
        config.initial_offset = Some("500".to_string());
        let src = PostgresSource::new(1, config, None);

        let query = "SELECT * FROM $table WHERE id > $offset";
        let result = src.substitute_query_params(query, "data", &None, 100);

        assert!(result.contains("id > 500"));
    }

    #[test]
    fn given_connection_string_with_credentials_should_redact() {
        let conn = "postgres://user:password@localhost:5432/db";
        let redacted = redact_connection_string(conn);
        assert_eq!(redacted, "postgres://use***");
    }

    #[test]
    fn given_connection_string_without_scheme_should_redact() {
        let conn = "localhost:5432/db";
        let redacted = redact_connection_string(conn);
        assert_eq!(redacted, "loc***");
    }

    #[test]
    fn given_postgresql_scheme_should_redact() {
        let conn = "postgresql://admin:secret123@db.example.com:5432/mydb";
        let redacted = redact_connection_string(conn);
        assert_eq!(redacted, "postgresql://adm***");
    }

    #[test]
    fn given_persisted_state_should_restore_tracking_offsets() {
        let state = State {
            last_poll_time: Utc::now(),
            tracking_offsets: HashMap::from([
                ("users".to_string(), "100".to_string()),
                ("orders".to_string(), "2024-01-15T10:30:00Z".to_string()),
            ]),
            processed_rows: 500,
        };

        let connector_state =
            ConnectorState::serialize(&state, "test", 1).expect("Failed to serialize state");

        let src = PostgresSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = src.state.lock().await;
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
        let src = PostgresSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert!(state.tracking_offsets.is_empty());
            assert_eq!(state.processed_rows, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid_state = ConnectorState(b"not valid json".to_vec());
        let src = PostgresSource::new(1, test_config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert!(state.tracking_offsets.is_empty());
            assert_eq!(state.processed_rows, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            last_poll_time: DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
            tracking_offsets: HashMap::from([("table1".to_string(), "42".to_string())]),
            processed_rows: 1000,
        };

        let connector_state =
            ConnectorState::serialize(&original, "test", 1).expect("Failed to serialize state");
        let deserialized: State = connector_state
            .deserialize("test", 1)
            .expect("Failed to deserialize state");

        assert_eq!(original.last_poll_time, deserialized.last_poll_time);
        assert_eq!(original.tracking_offsets, deserialized.tracking_offsets);
        assert_eq!(original.processed_rows, deserialized.processed_rows);
    }

    #[test]
    fn given_zero_interval_should_format_as_zero_time() {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: 0,
        };
        assert_eq!(format_pg_interval(&interval), "00:00:00");
    }

    #[test]
    fn given_interval_with_months_and_days_should_format_correctly() {
        let interval = PgInterval {
            months: 14,
            days: 3,
            microseconds: 0,
        };
        assert_eq!(format_pg_interval(&interval), "1 year 2 mons 3 days");
    }

    #[test]
    fn given_interval_with_time_should_format_correctly() {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: 3_661_000_000,
        };
        assert_eq!(format_pg_interval(&interval), "01:01:01");
    }

    #[test]
    fn given_interval_with_microseconds_should_format_fractional_seconds() {
        let interval = PgInterval {
            months: 0,
            days: 1,
            microseconds: 500_000,
        };
        assert_eq!(format_pg_interval(&interval), "1 day 00:00:00.500000");
    }

    #[test]
    fn given_singular_units_should_omit_plural_suffix() {
        let interval = PgInterval {
            months: 13,
            days: 1,
            microseconds: 0,
        };
        assert_eq!(format_pg_interval(&interval), "1 year 1 mon 1 day");
    }

    #[test]
    fn given_negative_microseconds_should_format_with_sign() {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: -1_500_000,
        };
        assert_eq!(format_pg_interval(&interval), "-00:00:01.500000");
    }

    #[test]
    fn given_negative_hours_should_format_with_sign() {
        let interval = PgInterval {
            months: 0,
            days: 0,
            microseconds: -3_600_000_000,
        };
        assert_eq!(format_pg_interval(&interval), "-01:00:00");
    }
}
