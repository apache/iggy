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
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

source_connector!(MySqlSource);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

#[derive(Debug)]
pub struct MySqlSource {
    pub id: u32,
    pool: Option<Pool<MySql>>,
    config: MySqlSourceConfig,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
    poll_interval: Duration,
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
    max_offset: Option<String>,
    row_pk: Option<String>,
}

/// One table's fully processed but not-yet-committed work. Built in the
/// side-effect-free first phase of `poll_tables`, then marked/deleted and
/// published in the second phase so a table's messages are emitted only once
/// its rows are marked.
struct TableBatch {
    table: String,
    messages: Vec<ProducedMessage>,
    processed_ids: Vec<String>,
    max_offset: Option<String>,
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
    last_poll_time: DateTime<Utc>,
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
        self.connect().await?;

        info!(
            "MySQL source connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let poll_interval = self.poll_interval;
        tokio::time::sleep(poll_interval).await;

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
                last_poll_time: Utc::now(),
                tracking_offsets: HashMap::new(),
                processed_rows: 0,
            })),
            verbose,
            retry_delay,
            poll_interval,
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
            PayloadFormat::Json => Err(Error::InvalidConfig), // unreachable! if payload_column is there then payload_format can never be json
        }
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

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    async fn mark_or_delete_processed_rows(
        &self,
        pool: &Pool<MySql>,
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
                    format!(
                        "'{}'",
                        id.replace('\\', "\\\\")
                            .replace('\'', "''")
                            .replace('\0', "")
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let query = if self.config.delete_after_read.unwrap_or(false) {
            if self.verbose {
                info!("Deleting {} processed rows from '{table}'", ids.len());
            } else {
                debug!("Deleting {} processed rows from '{table}'", ids.len());
            }
            format!("DELETE FROM {quoted_table} WHERE {quoted_pk} IN ({ids_list})")
        } else if let Some(processed_col) = &self.config.processed_column {
            let quoted_processed = quote_identifier(processed_col)?;
            if self.verbose {
                info!("Marking {} rows as processed in '{table}'", ids.len());
            } else {
                debug!("Marking {} rows as processed in '{table}'", ids.len());
            }
            format!(
                "UPDATE {quoted_table} SET {quoted_processed} = TRUE WHERE {quoted_pk} IN ({ids_list})"
            )
        } else {
            // Offset-tracking only: nothing to mark or delete.
            return Ok(());
        };

        with_retry(
            || sqlx::query(sqlx::AssertSqlSafe(query.as_str())).execute(pool),
            self.get_max_retries(),
            self.retry_delay.as_millis() as u64,
        )
        .await
        .map(|_| ())
    }

    async fn poll_tables(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self.get_pool()?;

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

        // Phase 1: fetch and process every table into its own buffer without any
        // side effect. A `process_row` failure is deterministic (a row that cannot
        // be decoded fails identically every cycle), so a failing table is logged
        // with its resume offset and skipped rather than aborting the whole poll:
        // nothing has been marked yet, its rows stay in MySQL, and the remaining
        // tables still make progress. The operator sees exactly which table and
        // offset to fix.
        let mut batches: Vec<TableBatch> = Vec::with_capacity(self.config.tables.len());

        for table in &self.config.tables {
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
                    batches.push(batch);
                }
                Err(error) => {
                    error!(
                        "Failed to process table '{table}' at offset {}, skipping this cycle: {error}",
                        last_offset.as_deref().unwrap_or("<start>")
                    );
                }
            }
        }

        // Phase 2: commit each table independently. A table's messages are emitted
        // only after its rows are marked or deleted, so a failure here can never
        // leave a table marked-but-unpublished. `mark_or_delete_processed_rows`
        // retries transient errors; a permanent failure isolates that one table
        // (its rows stay in MySQL and are retried next cycle) while the others
        // still flow.
        let mut messages = Vec::new();
        let mut state_updates: Vec<(String, String)> = Vec::new();
        let mut total_processed: u64 = 0;

        for mut batch in batches {
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
            state.last_poll_time = Utc::now();
        }

        Ok(messages)
    }

    async fn fetch_table_batch(
        &self,
        table: &str,
        row_config: &RowProcessingConfig<'_>,
        tracking_column: &str,
        batch_size: u32,
        last_offset: &Option<String>,
    ) -> Result<TableBatch, Error> {
        let pool = self.get_pool()?;
        let table_config = RowProcessingConfig {
            table,
            ..*row_config
        };

        let query = if let Some(custom_query) = &self.config.custom_query {
            self.validate_custom_query(custom_query)?;
            self.substitute_query_params(custom_query, table, last_offset, batch_size)
        } else {
            self.build_polling_query(table, tracking_column, last_offset, batch_size)?
        };

        // Database I/O without holding the lock
        let rows = with_retry(
            || sqlx::query(sqlx::AssertSqlSafe(query.as_str())).fetch_all(pool),
            self.get_max_retries(),
            self.retry_delay.as_millis() as u64,
        )
        .await?;

        let mut batch = TableBatch {
            table: table.to_string(),
            messages: Vec::with_capacity(rows.len()),
            processed_ids: Vec::new(),
            max_offset: None,
        };
        for row in rows {
            let processed = self.process_row(&row, &table_config)?;

            if let Some(pk) = processed.row_pk {
                batch.processed_ids.push(pk);
            }
            if let Some(offset) = processed.max_offset {
                batch.max_offset = Some(offset);
            }

            batch.messages.push(processed.message);
        }

        Ok(batch)
    }

    fn process_row(
        &self,
        row: &MySqlRow,
        config: &RowProcessingConfig,
    ) -> Result<ProcessedRow, Error> {
        let mut row_pk: Option<String> = None;
        let mut max_offset: Option<String> = None;
        let mut extracted_payload: Option<Vec<u8>> = None;

        // Payload column set: only extract it plus tracking/pk columns.
        // Avoids extract_column_value on every other column since the data map
        // built below would be discarded anyway.
        if !config.payload_col.is_empty() {
            for (i, column) in row.columns().iter().enumerate() {
                let name = column.name();
                if name == config.payload_col {
                    extracted_payload =
                        Some(self.extract_payload_column(row, i, config.payload_format)?);
                }
                if name == config.tracking_column {
                    max_offset = value_as_string(&extract_column_value(row, i)?);
                }
                if name == config.pk_column {
                    row_pk = value_as_string(&extract_column_value(row, i)?);
                }
            }
        }

        if extracted_payload.is_none() {
            let mut data = serde_json::Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                let name = column.name();
                let column_name = if config.snake_case_columns {
                    to_snake_case(name)
                } else {
                    name.to_string()
                };
                let value = extract_column_value(row, i)?;
                if name == config.tracking_column {
                    max_offset = value_as_string(&value);
                }
                if name == config.pk_column {
                    row_pk = value_as_string(&value);
                }
                data.insert(column_name, value);
            }

            extracted_payload = Some(if config.include_metadata {
                let record = DatabaseRecord {
                    table_name: config.table.to_string(),
                    operation_type: "SELECT".to_string(),
                    timestamp: Utc::now(),
                    data: serde_json::Value::Object(data),
                    old_data: None,
                };
                simd_json::to_vec(&record).map_err(|_| Error::InvalidRecord)?
            } else {
                simd_json::to_vec(&data).map_err(|_| Error::InvalidRecord)?
            });
        }

        // Both paths above always assign extracted_payload before reaching here.
        Ok(build_processed_row(
            extracted_payload.unwrap(),
            max_offset,
            row_pk,
        ))
    }
}

fn extract_column_value(row: &MySqlRow, column_index: usize) -> Result<serde_json::Value, Error> {
    let column = &row.columns()[column_index];
    let type_name = column.type_info().name();

    match type_name {
        "BOOLEAN" => {
            let value: Option<bool> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null))
        }
        "TINYINT" => {
            let value: Option<i8> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "SMALLINT" => {
            let value: Option<i16> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "MEDIUMINT" | "INT" => {
            let value: Option<i32> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as i64))
                .unwrap_or(serde_json::Value::Null))
        }
        "BIGINT" => {
            let value: Option<i64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "TINYINT UNSIGNED" => {
            let value: Option<u8> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "SMALLINT UNSIGNED" => {
            let value: Option<u16> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => {
            let value: Option<u32> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
                .unwrap_or(serde_json::Value::Null))
        }
        "BIGINT UNSIGNED" => {
            let value: Option<u64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "FLOAT" => {
            let value: Option<f32> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as f64))
                .unwrap_or(serde_json::Value::Null))
        }
        "DOUBLE" => {
            let value: Option<f64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "DECIMAL" => {
            let value: Option<String> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null))
        }
        "BIT" => {
            let value: Option<u64> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null))
        }
        "YEAR" => {
            let value: Option<u16> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|v| serde_json::Value::from(v as u64))
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
        "DATETIME" | "TIMESTAMP" => {
            let value: Option<NaiveDateTime> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|dt| serde_json::Value::String(dt.to_string()))
                .unwrap_or(serde_json::Value::Null))
        }
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET" => {
            let value: Option<String> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null))
        }
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "GEOMETRY" => {
            let value: Option<Vec<u8>> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
            Ok(value
                .map(|bytes| {
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(&bytes),
                    )
                })
                .unwrap_or(serde_json::Value::Null))
        }
        "JSON" => {
            let value: Option<serde_json::Value> = row
                .try_get(column_index)
                .map_err(|_| Error::InvalidRecord)?;
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

fn value_as_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn build_processed_row(
    payload: Vec<u8>,
    max_offset: Option<String>,
    row_pk: Option<String>,
) -> ProcessedRow {
    let now = Utc::now().timestamp_millis() as u64;
    ProcessedRow {
        message: ProducedMessage {
            id: Some(Uuid::new_v4().as_u128()),
            headers: None,
            checksum: None,
            timestamp: Some(now),
            origin_timestamp: Some(now),
            payload,
        },
        max_offset,
        row_pk,
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

fn format_offset_value(value: &str) -> String {
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok_and(|v| v.is_finite()) {
        value.to_string()
    } else {
        let escaped = value
            .replace('\\', "\\\\")
            .replace('\'', "''")
            .replace('\0', "");
        format!("'{escaped}'")
    }
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
                    2006 | 2013 | 1158 | 1159 | 1160 | 1161 |
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
            last_poll_time: Utc::now(),
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
        // The full State (timestamp + offsets + count) must survive a
        // MessagePack round-trip unchanged, since that is what gets persisted.
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
    fn given_last_offset_should_filter_order_and_limit() {
        // With a known last offset, the query must fetch only newer rows,
        // ordered ascending by the tracking column, capped at the batch size.
        let source = MySqlSource::new(1, test_config(), None);
        let query = source
            .build_polling_query("users", "id", &Some("100".to_string()), 500)
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
            .build_polling_query("users", "id", &None, 500)
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
            .build_polling_query("users", "id", &None, 500)
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
            .build_polling_query("events", "id", &None, 100)
            .expect("Failed to build query");
        assert!(query.contains("`is_processed` = FALSE"));
    }

    #[test]
    fn given_offset_value_should_quote_only_non_numeric() {
        // Numeric offsets are emitted bare (correct comparison + no cast),
        // while string offsets (e.g. timestamps) must be single-quoted literals.
        let source = MySqlSource::new(1, test_config(), None);

        let numeric = source
            .build_polling_query("users", "id", &Some("42".to_string()), 100)
            .expect("Failed to build query");
        assert!(numeric.contains("`id` > 42"));
        assert!(!numeric.contains("'42'"));

        let string = source
            .build_polling_query("users", "updated_at", &Some("2024-01-01".to_string()), 100)
            .expect("Failed to build query");
        assert!(string.contains("`updated_at` > '2024-01-01'"));
    }

    #[test]
    fn given_qualified_table_should_backtick_each_segment() {
        // A `db.table` target must quote each segment independently so the
        // dot stays a schema separator, not part of a single quoted name.
        let source = MySqlSource::new(1, test_config(), None);
        let query = source
            .build_polling_query("mydb.users", "id", &None, 100)
            .expect("Failed to build query");
        assert!(query.contains("FROM `mydb`.`users`"));
    }

    #[test]
    fn given_custom_query_should_substitute_table_offset_limit() {
        // Placeholders in an operator-provided query must be filled with the
        // current table, resolved offset, and batch size before execution.
        let source = MySqlSource::new(1, test_config(), None);
        let query = "SELECT * FROM $table WHERE id > $offset ORDER BY id LIMIT $limit";
        let result = source.substitute_query_params(query, "events", &Some("100".to_string()), 50);
        assert!(result.contains("FROM events"));
        assert!(result.contains("id > 100"));
        assert!(result.contains("LIMIT 50"));
    }

    #[test]
    fn given_custom_query_with_time_params_should_substitute_now() {
        // Time placeholders must be expanded to a concrete timestamp so no
        // literal `$now` reaches the database.
        let source = MySqlSource::new(1, test_config(), None);
        let query = "SELECT * FROM $table WHERE created_at < '$now' OR ts < $now_unix";
        let result = source.substitute_query_params(query, "logs", &None, 100);
        assert!(result.contains("FROM logs"));
        assert!(!result.contains("$now"));
        assert!(!result.contains("$now_unix"));
    }

    #[test]
    fn given_no_last_offset_should_fall_back_to_initial_offset() {
        // In the custom-query path too, a missing last offset must fall back to
        // the configured initial_offset rather than substituting an empty value.
        let mut config = test_config();
        config.initial_offset = Some("500".to_string());
        let source = MySqlSource::new(1, config, None);
        let result = source.substitute_query_params(
            "SELECT * FROM $table WHERE id > $offset",
            "data",
            &None,
            100,
        );
        assert!(result.contains("id > 500"));
    }

    #[test]
    fn given_table_placeholder_and_no_tables_should_fail() {
        // A $table placeholder with no configured tables can never resolve,
        // so validation must reject it instead of querying a literal "$table".
        let mut config = test_config();
        config.tables = vec![];
        let source = MySqlSource::new(1, config, None);
        let result = source.validate_custom_query("SELECT * FROM $table");
        assert!(matches!(result, Err(Error::InvalidConfig)));
    }

    #[test]
    fn given_valid_custom_query_should_pass() {
        // A well-formed SELECT with tables configured passes validation.
        let source = MySqlSource::new(1, test_config(), None);
        let result = source.validate_custom_query("SELECT * FROM $table WHERE id > $offset");
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
        assert_eq!(format_offset_value("O'Brien"), "'O''Brien'");
        assert_eq!(format_offset_value("a\\b"), "'a\\\\b'");
        assert_eq!(format_offset_value("42"), "42");
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
            PayloadFormat::from_config(Some("jsonb")),
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
}
