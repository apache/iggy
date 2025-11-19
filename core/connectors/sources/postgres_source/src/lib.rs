/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use std::{collections::HashMap, str::FromStr, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use sqlx::{Column, Pool, Postgres, Row, TypeInfo, postgres::PgPoolOptions};
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

source_connector!(PostgresSource);

#[derive(Debug)]
pub struct PostgresSource {
    pub id: u32,
    pool: Option<Pool<Postgres>>,
    config: PostgresSourceConfig,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub connection_string: String,
    pub mode: String,
    pub tables: Vec<String>,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub tracking_column: Option<String>,
    pub initial_offset: Option<String>,
    pub max_connections: Option<u32>,
    pub enable_wal_cdc: Option<bool>,
    pub custom_query: Option<String>,
    pub snake_case_columns: Option<bool>,
    pub include_metadata: Option<bool>,
    pub replication_slot: Option<String>,
    pub publication_name: Option<String>,
    pub capture_operations: Option<Vec<String>>, // INSERT, UPDATE, DELETE
    pub cdc_backend: Option<String>,             // "builtin" | "pg_replicate"
}

#[derive(Debug)]
struct State {
    last_poll_time: DateTime<Utc>,
    tracking_offsets: HashMap<String, String>,
    processed_rows: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRecord {
    pub table_name: String,
    pub operation_type: String, // INSERT, UPDATE, DELETE
    pub timestamp: DateTime<Utc>,
    pub data: serde_json::Value,
    pub old_data: Option<serde_json::Value>,
}

impl PostgresSource {
    pub fn new(id: u32, config: PostgresSourceConfig, _state: Option<ConnectorState>) -> Self {
        PostgresSource {
            id,
            pool: None,
            config,
            state: Mutex::new(State {
                last_poll_time: Utc::now(),
                tracking_offsets: HashMap::new(),
                processed_rows: 0,
            }),
        }
    }

    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(10);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(&self.config.connection_string)
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to PostgreSQL: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!(
            "Connected to PostgreSQL database with {} max connections",
            max_connections
        );
        Ok(())
    }

    async fn setup_cdc(&self) -> Result<(), Error> {
        if !self.config.enable_wal_cdc.unwrap_or(false) {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;

        let wal_level: String = sqlx::query_scalar("SHOW wal_level")
            .fetch_one(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to check WAL level: {e}")))?;

        if wal_level != "logical" {
            return Err(Error::InitError(
                "WAL level must be 'logical' for CDC. Please set wal_level = logical in postgresql.conf".to_string()
            ));
        }

        let publication_name = self
            .config
            .publication_name
            .as_deref()
            .unwrap_or("iggy_publication");
        let tables_clause = if self.config.tables.is_empty() {
            "FOR ALL TABLES".to_string()
        } else {
            format!("FOR TABLE {}", self.config.tables.join(", "))
        };

        let create_publication_sql =
            format!("CREATE PUBLICATION IF NOT EXISTS {publication_name} {tables_clause}");

        sqlx::query(&create_publication_sql)
            .execute(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create publication: {e}")))?;

        let slot_name = self
            .config
            .replication_slot
            .as_deref()
            .unwrap_or("iggy_slot");
        let create_slot_sql = format!(
            "SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        );

        sqlx::query(&create_slot_sql)
            .fetch_optional(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create replication slot: {e}")))?;

        info!(
            "PostgreSQL CDC setup completed. Publication: {}, Slot: {}",
            publication_name, slot_name
        );
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
                "Unsupported cdc_backend '{}'. Use 'builtin' or 'pg_replicate'",
                other
            ))),
        }
    }

    async fn poll_cdc_builtin(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;

        let slot_name = self
            .config
            .replication_slot
            .as_deref()
            .unwrap_or("iggy_slot");
        let publication_name = self
            .config
            .publication_name
            .as_deref()
            .unwrap_or("iggy_publication");
        let capture_ops = self
            .config
            .capture_operations
            .as_ref()
            .map(|ops| ops.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            .unwrap_or_else(|| vec!["INSERT", "UPDATE", "DELETE"]);

        let logical_repl_sql = format!(
            "SELECT lsn, xid, data FROM pg_logical_slot_get_changes('{slot_name}', NULL, NULL, 'proto_version', '1', 'publication_names', '{publication_name}')"
        );

        let rows = sqlx::query(&logical_repl_sql)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to fetch CDC changes: {}", e);
                Error::InvalidRecord
            })?;

        let mut messages = Vec::new();
        let mut state = self.state.lock().await;

        for row in rows {
            let data: String = row.try_get("data").map_err(|_| Error::InvalidRecord)?;

            if let Some(change_record) = self.parse_logical_replication_message(&data, &capture_ops)
            {
                let payload =
                    simd_json::to_vec(&change_record).map_err(|_| Error::InvalidRecord)?;

                let message = ProducedMessage {
                    id: Some(Uuid::new_v4().as_u128()),
                    headers: None,
                    checksum: None,
                    timestamp: Some(chrono::Utc::now().timestamp_millis() as u64),
                    origin_timestamp: Some(chrono::Utc::now().timestamp_millis() as u64),
                    payload,
                };

                messages.push(message);
                state.processed_rows += 1;
            }
        }

        info!("CDC: Fetched {} change records", messages.len());
        Ok(messages)
    }

    fn parse_logical_replication_message(
        &self,
        data: &str,
        capture_ops: &[&str],
    ) -> Option<DatabaseRecord> {
        if data.starts_with("BEGIN") || data.starts_with("COMMIT") {
            return None;
        }

        if data.starts_with("INSERT:") && capture_ops.contains(&"INSERT") {
            return self.parse_insert_message(data);
        }

        if data.starts_with("UPDATE:") && capture_ops.contains(&"UPDATE") {
            return self.parse_update_message(data);
        }

        if data.starts_with("DELETE:") && capture_ops.contains(&"DELETE") {
            return self.parse_delete_message(data);
        }

        None
    }

    fn parse_insert_message(&self, data: &str) -> Option<DatabaseRecord> {
        if let Some(table_start) = data.find("table ")
            && let Some(colon_pos) = data[table_start..].find(':')
        {
            let table_part = &data[table_start + 6..table_start + colon_pos];
            let table_name = table_part
                .split('.')
                .next_back()
                .unwrap_or(table_part)
                .to_string();

            let data_part = &data[table_start + colon_pos + 1..];
            let parsed_data = self.parse_record_data(data_part);

            return Some(DatabaseRecord {
                table_name,
                operation_type: "INSERT".to_string(),
                timestamp: Utc::now(),
                data: serde_json::Value::Object(parsed_data),
                old_data: None,
            });
        }
        None
    }

    fn parse_update_message(&self, data: &str) -> Option<DatabaseRecord> {
        if let Some(table_start) = data.find("table ")
            && let Some(colon_pos) = data[table_start..].find(':')
        {
            let table_part = &data[table_start + 6..table_start + colon_pos];
            let table_name = table_part
                .split('.')
                .next_back()
                .unwrap_or(table_part)
                .to_string();

            let data_part = &data[table_start + colon_pos + 1..];
            let parsed_data = self.parse_record_data(data_part);

            return Some(DatabaseRecord {
                table_name,
                operation_type: "UPDATE".to_string(),
                timestamp: Utc::now(),
                data: serde_json::Value::Object(parsed_data),
                old_data: None,
            });
        }
        None
    }

    fn parse_delete_message(&self, data: &str) -> Option<DatabaseRecord> {
        if let Some(table_start) = data.find("table ")
            && let Some(colon_pos) = data[table_start..].find(':')
        {
            let table_part = &data[table_start + 6..table_start + colon_pos];
            let table_name = table_part
                .split('.')
                .next_back()
                .unwrap_or(table_part)
                .to_string();

            let data_part = &data[table_start + colon_pos + 1..];
            let parsed_data = self.parse_record_data(data_part);

            return Some(DatabaseRecord {
                table_name,
                operation_type: "DELETE".to_string(),
                timestamp: Utc::now(),
                data: serde_json::Value::Object(parsed_data),
                old_data: None,
            });
        }
        None
    }

    fn parse_record_data(&self, data: &str) -> serde_json::Map<String, serde_json::Value> {
        let mut result = serde_json::Map::new();

        let parts = data.split_whitespace();

        for part in parts {
            if let Some(bracket_pos) = part.find('[')
                && let Some(_close_bracket) = part.find(']')
                && let Some(colon_pos) = part.find(':')
            {
                let column_name = &part[..bracket_pos];
                let value_str = &part[colon_pos + 1..];

                let cleaned_value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
                    &value_str[1..value_str.len() - 1]
                } else {
                    value_str
                };

                let value = if let Ok(num) = cleaned_value.parse::<i64>() {
                    serde_json::Value::Number(serde_json::Number::from(num))
                } else if let Ok(float) = cleaned_value.parse::<f64>() {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(float).unwrap_or(serde_json::Number::from(0)),
                    )
                } else if cleaned_value.eq_ignore_ascii_case("true") {
                    serde_json::Value::Bool(true)
                } else if cleaned_value.eq_ignore_ascii_case("false") {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::String(cleaned_value.to_string())
                };

                result.insert(column_name.to_string(), value);
            }
        }

        result
    }

    async fn poll_tables(&self) -> Result<Vec<ProducedMessage>, Error> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;
        let mut state = self.state.lock().await;
        let mut messages = Vec::new();

        let batch_size = self.config.batch_size.unwrap_or(1000);
        let tracking_column = self.config.tracking_column.as_deref().unwrap_or("id");

        for table in &self.config.tables {
            let last_offset = state.tracking_offsets.get(table).cloned();

            let query = if let Some(custom_query) = &self.config.custom_query {
                custom_query.clone()
            } else {
                self.build_polling_query(table, tracking_column, &last_offset, batch_size)
            };

            info!("Executing query for table '{}': {}", table, query);

            let rows = sqlx::query(&query)
                .fetch_all(pool)
                .await
                .map_err(|_| Error::InvalidRecord)?;

            let mut max_offset: Option<String> = None;

            for row in rows {
                let mut data = serde_json::Map::new();

                for (i, column) in row.columns().iter().enumerate() {
                    let column_name = if self.config.snake_case_columns.unwrap_or(false) {
                        self.to_snake_case(column.name())
                    } else {
                        column.name().to_string()
                    };

                    let value = self.extract_column_value(&row, i)?;
                    data.insert(column_name.clone(), value);

                    if column.name() == tracking_column {
                        if let Some(serde_json::Value::String(offset_str)) = data.get(&column_name)
                        {
                            max_offset = Some(offset_str.clone());
                        } else if let Some(serde_json::Value::Number(offset_num)) =
                            data.get(&column_name)
                        {
                            max_offset = Some(offset_num.to_string());
                        }
                    }
                }

                let record = if self.config.include_metadata.unwrap_or(true) {
                    DatabaseRecord {
                        table_name: table.clone(),
                        operation_type: "SELECT".to_string(),
                        timestamp: Utc::now(),
                        data: serde_json::Value::Object(data),
                        old_data: None,
                    }
                } else {
                    let mut simple_record = serde_json::Map::new();
                    simple_record.insert("data".to_string(), serde_json::Value::Object(data));
                    DatabaseRecord {
                        table_name: table.clone(),
                        operation_type: "SELECT".to_string(),
                        timestamp: Utc::now(),
                        data: serde_json::Value::Object(simple_record),
                        old_data: None,
                    }
                };

                let payload = simd_json::to_vec(&record).map_err(|_| Error::InvalidRecord)?;

                let message = ProducedMessage {
                    id: Some(Uuid::new_v4().as_u128()),
                    headers: None,
                    checksum: None,
                    timestamp: Some(chrono::Utc::now().timestamp_millis() as u64),
                    origin_timestamp: Some(chrono::Utc::now().timestamp_millis() as u64),
                    payload,
                };

                messages.push(message);
                state.processed_rows += 1;
            }

            if let Some(offset) = max_offset {
                state.tracking_offsets.insert(table.clone(), offset);
            }

            info!("Fetched {} rows from table '{}'", messages.len(), table);
        }

        state.last_poll_time = Utc::now();
        Ok(messages)
    }

    fn build_polling_query(
        &self,
        table: &str,
        tracking_column: &str,
        last_offset: &Option<String>,
        batch_size: u32,
    ) -> String {
        let base_query = format!("SELECT * FROM {table}");

        let where_clause = if let Some(offset) = last_offset {
            format!(" WHERE {tracking_column} > '{offset}'")
        } else if let Some(initial) = &self.config.initial_offset {
            format!(" WHERE {tracking_column} > '{initial}'")
        } else {
            String::new()
        };

        let order_clause = format!(" ORDER BY {tracking_column} ASC");
        let limit_clause = format!(" LIMIT {batch_size}");

        format!("{base_query}{where_clause}{order_clause}{limit_clause}")
    }

    fn extract_column_value(
        &self,
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
            "INT2" | "INT4" | "INT8" => {
                let value: Option<i64> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(value
                    .map(serde_json::Value::from)
                    .unwrap_or(serde_json::Value::Null))
            }
            "FLOAT4" | "FLOAT8" | "NUMERIC" => {
                let value: Option<f64> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(value
                    .map(serde_json::Value::from)
                    .unwrap_or(serde_json::Value::Null))
            }
            "VARCHAR" | "TEXT" | "CHAR" => {
                let value: Option<String> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(value
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null))
            }
            "TIMESTAMP" | "TIMESTAMPTZ" => {
                let value: Option<DateTime<Utc>> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(value
                    .map(|dt| serde_json::Value::String(dt.to_rfc3339()))
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
            _ => {
                let value: Option<String> = row
                    .try_get(column_index)
                    .map_err(|_| Error::InvalidRecord)?;
                Ok(value
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null))
            }
        }
    }

    fn to_snake_case(&self, input: &str) -> String {
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

    fn get_poll_interval(&self) -> Duration {
        let interval_str = self.config.poll_interval.as_deref().unwrap_or("10s");
        humantime::Duration::from_str(interval_str)
            .unwrap_or_else(|_| std::time::Duration::from_secs(10).into())
            .into()
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
                    "PostgreSQL CDC mode enabled (backend: {}) for connector ID: {}",
                    backend, self.id
                );
            }
            "polling" => {
                info!(
                    "PostgreSQL polling mode enabled for connector ID: {}",
                    self.id
                );
                info!("Poll interval: {:?}", self.get_poll_interval());
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
        let poll_interval = self.get_poll_interval();
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
        info!(
            "PostgreSQL source connector ID: {} produced {} messages. Total processed: {}",
            self.id,
            messages.len(),
            state.processed_rows
        );

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_last_offset_polling_query_should_be_built() {
        let src = PostgresSource::new(
            1,
            PostgresSourceConfig {
                connection_string: "postgres://localhost/db".to_string(),
                mode: "polling".to_string(),
                tables: vec!["users".to_string()],
                poll_interval: Some("5s".to_string()),
                batch_size: Some(500),
                tracking_column: Some("updated_at".to_string()),
                initial_offset: None,
                max_connections: None,
                enable_wal_cdc: None,
                custom_query: None,
                snake_case_columns: None,
                include_metadata: None,
                replication_slot: None,
                publication_name: None,
                capture_operations: None,
                cdc_backend: None,
            },
            None,
        );
        let query =
            src.build_polling_query("users", "updated_at", &Some("2024-01-01".to_string()), 500);
        assert_eq!(
            query,
            "SELECT * FROM users WHERE updated_at > '2024-01-01' ORDER BY updated_at ASC LIMIT 500"
        );
    }

    #[test]
    fn given_initial_offset_polling_query_should_be_built() {
        let src = PostgresSource::new(
            1,
            PostgresSourceConfig {
                connection_string: "postgres://localhost/db".to_string(),
                mode: "polling".to_string(),
                tables: vec!["users".to_string()],
                poll_interval: Some("5s".to_string()),
                batch_size: Some(1000),
                tracking_column: Some("id".to_string()),
                initial_offset: Some("100".to_string()),
                max_connections: None,
                enable_wal_cdc: None,
                custom_query: None,
                snake_case_columns: None,
                include_metadata: None,
                replication_slot: None,
                publication_name: None,
                capture_operations: None,
                cdc_backend: None,
            },
            None,
        );
        let query = src.build_polling_query("users", "id", &None, 1000);
        assert_eq!(
            query,
            "SELECT * FROM users WHERE id > '100' ORDER BY id ASC LIMIT 1000"
        );
    }

    #[test]
    fn given_insert_message_should_parse_correctly() {
        let src = PostgresSource::new(
            1,
            PostgresSourceConfig {
                connection_string: String::new(),
                mode: "cdc".to_string(),
                tables: vec![],
                poll_interval: None,
                batch_size: None,
                tracking_column: None,
                initial_offset: None,
                max_connections: None,
                enable_wal_cdc: None,
                custom_query: None,
                snake_case_columns: None,
                include_metadata: None,
                replication_slot: None,
                publication_name: None,
                capture_operations: None,
                cdc_backend: None,
            },
            None,
        );

        let data = "INSERT: table public.users: id[1] name['Alice'] active[true]";
        let rec = src
            .parse_logical_replication_message(data, &["INSERT"])
            .unwrap();
        assert_eq!(rec.table_name, "users");
        assert_eq!(rec.operation_type, "INSERT");
    }

    #[test]
    fn given_update_message_should_parse_correctly() {
        let src = PostgresSource::new(
            1,
            PostgresSourceConfig {
                connection_string: String::new(),
                mode: "cdc".to_string(),
                tables: vec![],
                poll_interval: None,
                batch_size: None,
                tracking_column: None,
                initial_offset: None,
                max_connections: None,
                enable_wal_cdc: None,
                custom_query: None,
                snake_case_columns: None,
                include_metadata: None,
                replication_slot: None,
                publication_name: None,
                capture_operations: None,
                cdc_backend: None,
            },
            None,
        );

        let data = "UPDATE: table public.orders: id[42] total[99.5]";
        let rec = src
            .parse_logical_replication_message(data, &["UPDATE"])
            .unwrap();
        assert_eq!(rec.table_name, "orders");
        assert_eq!(rec.operation_type, "UPDATE");
    }

    #[test]
    fn given_delete_message_should_parse_correctly() {
        let src = PostgresSource::new(
            1,
            PostgresSourceConfig {
                connection_string: String::new(),
                mode: "cdc".to_string(),
                tables: vec![],
                poll_interval: None,
                batch_size: None,
                tracking_column: None,
                initial_offset: None,
                max_connections: None,
                enable_wal_cdc: None,
                custom_query: None,
                snake_case_columns: None,
                include_metadata: None,
                replication_slot: None,
                publication_name: None,
                capture_operations: None,
                cdc_backend: None,
            },
            None,
        );

        let data = "DELETE: table public.products: id[7]";
        let rec = src
            .parse_logical_replication_message(data, &["DELETE"])
            .unwrap();
        assert_eq!(rec.table_name, "products");
        assert_eq!(rec.operation_type, "DELETE");
    }
}
