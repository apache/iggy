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
use humantime::Duration as HumanDuration;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(PostgresSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

#[derive(Debug)]
pub struct PostgresSink {
    pub id: u32,
    pool: Option<Pool<Postgres>>,
    config: PostgresSinkConfig,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSinkConfig {
    pub connection_string: String,
    pub target_table: String,
    pub batch_size: Option<u32>,
    pub max_connections: Option<u32>,
    pub auto_create_table: Option<bool>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Bytea,
    Json,
    Text,
}

impl PayloadFormat {
    fn from_config(s: Option<&str>) -> Self {
        match s.map(|s| s.to_lowercase()).as_deref() {
            Some("json") | Some("jsonb") => PayloadFormat::Json,
            Some("text") => PayloadFormat::Text,
            _ => PayloadFormat::Bytea,
        }
    }

    fn sql_type(&self) -> &'static str {
        match self {
            PayloadFormat::Bytea => "BYTEA",
            PayloadFormat::Json => "JSONB",
            PayloadFormat::Text => "TEXT",
        }
    }
}

#[derive(Debug)]
struct State {
    messages_processed: u64,
    insertion_errors: u64,
}

impl PostgresSink {
    pub fn new(id: u32, config: PostgresSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = HumanDuration::from_str(delay_str)
            .map(|duration| duration.into())
            .unwrap_or_else(|_| Duration::from_secs(1));
        PostgresSink {
            id,
            pool: None,
            config,
            state: Mutex::new(State {
                messages_processed: 0,
                insertion_errors: 0,
            }),
            verbose,
            retry_delay,
        }
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening PostgreSQL sink connector with ID: {}. Target table: {}",
            self.id, self.config.target_table
        );
        self.connect().await?;
        self.ensure_table_exists().await?;
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.process_messages(topic_metadata, &messages_metadata, &messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing PostgreSQL sink connector with ID: {}", self.id);

        if let Some(pool) = self.pool.take() {
            pool.close().await;
            info!(
                "PostgreSQL connection pool closed for sink connector ID: {}",
                self.id
            );
        }

        let state = self.state.lock().await;
        info!(
            "PostgreSQL sink ID: {} processed {} messages with {} errors",
            self.id, state.messages_processed, state.insertion_errors
        );
        Ok(())
    }
}

impl PostgresSink {
    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(10);
        let redacted = redact_connection_string(&self.config.connection_string);

        info!("Connecting to PostgreSQL with max {max_connections} connections: {redacted}");

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
        info!("Connected to PostgreSQL database with {max_connections} max connections");
        Ok(())
    }

    async fn ensure_table_exists(&self) -> Result<(), Error> {
        if !self.config.auto_create_table.unwrap_or(false) {
            return Ok(());
        }

        let pool = self.get_pool()?;
        let table_name = &self.config.target_table;
        let quoted_table = quote_identifier(table_name)?;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_type = self.payload_format().sql_type();

        let mut sql = format!("CREATE TABLE IF NOT EXISTS {quoted_table} (");
        sql.push_str("id DECIMAL(39, 0) PRIMARY KEY");

        if include_metadata {
            sql.push_str(", iggy_offset BIGINT");
            sql.push_str(", iggy_timestamp TIMESTAMP WITH TIME ZONE");
            sql.push_str(", iggy_stream TEXT");
            sql.push_str(", iggy_topic TEXT");
            sql.push_str(", iggy_partition_id INTEGER");
        }

        if include_checksum {
            sql.push_str(", iggy_checksum BIGINT");
        }

        if include_origin_timestamp {
            sql.push_str(", iggy_origin_timestamp TIMESTAMP WITH TIME ZONE");
        }

        sql.push_str(&format!(", payload {payload_type}"));
        sql.push_str(", created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())");

        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create table '{table_name}': {e}")))?;

        info!("Ensured table '{table_name}' exists with payload type {payload_type}");
        Ok(())
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let pool = self.get_pool()?;
        let batch_size = self.config.batch_size.unwrap_or(100) as usize;

        for batch in messages.chunks(batch_size) {
            if let Err(e) = self
                .insert_batch(batch, topic_metadata, messages_metadata, pool)
                .await
            {
                let mut state = self.state.lock().await;
                state.insertion_errors += batch.len() as u64;
                error!("Failed to insert batch: {e}");
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;

        let msg_count = messages.len();
        let table = &self.config.target_table;
        if self.verbose {
            info!(
                "PostgreSQL sink ID: {} processed {msg_count} messages to table '{table}'",
                self.id
            );
        } else {
            debug!(
                "PostgreSQL sink ID: {} processed {msg_count} messages to table '{table}'",
                self.id
            );
        }

        Ok(())
    }

    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        pool: &Pool<Postgres>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let table_name = &self.config.target_table;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_format = self.payload_format();

        let (query, _params_per_row) = self.build_batch_insert_query(
            table_name,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            messages.len(),
        )?;

        self.execute_batch_insert_with_retry(
            pool,
            &query,
            messages,
            topic_metadata,
            messages_metadata,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            payload_format,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_batch_insert_with_retry(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        include_metadata: bool,
        include_checksum: bool,
        include_origin_timestamp: bool,
        payload_format: PayloadFormat,
    ) -> Result<(), Error> {
        let max_retries = self.get_max_retries();
        let retry_delay = self.retry_delay;
        let mut attempts = 0u32;

        loop {
            let result = self
                .bind_and_execute_batch(
                    pool,
                    query,
                    messages,
                    topic_metadata,
                    messages_metadata,
                    include_metadata,
                    include_checksum,
                    include_origin_timestamp,
                    payload_format,
                )
                .await;

            match result {
                Ok(_) => return Ok(()),
                Err((e, is_transient)) => {
                    attempts += 1;
                    if !is_transient || attempts >= max_retries {
                        error!("Batch insert failed after {attempts} attempts: {e}");
                        return Err(Error::CannotStoreData(format!(
                            "Batch insert failed after {attempts} attempts: {e}"
                        )));
                    }
                    warn!(
                        "Transient database error (attempt {attempts}/{max_retries}): {e}. Retrying..."
                    );
                    tokio::time::sleep(retry_delay * attempts).await;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_and_execute_batch(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        include_metadata: bool,
        include_checksum: bool,
        include_origin_timestamp: bool,
        payload_format: PayloadFormat,
    ) -> Result<(), (sqlx::Error, bool)> {
        let mut query_builder = sqlx::query(query);

        for message in messages {
            let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
                let err_msg = format!("Failed to convert payload to bytes: {e}");
                (sqlx::Error::Protocol(err_msg), false)
            })?;

            let timestamp = self.parse_timestamp(message.timestamp);
            let origin_timestamp_val = self.parse_timestamp(message.origin_timestamp);

            query_builder = query_builder.bind(message.id.to_string());

            if include_metadata {
                query_builder = query_builder
                    .bind(message.offset as i64)
                    .bind(timestamp)
                    .bind(topic_metadata.stream.clone())
                    .bind(topic_metadata.topic.clone())
                    .bind(messages_metadata.partition_id as i32);
            }

            if include_checksum {
                query_builder = query_builder.bind(message.checksum as i64);
            }

            if include_origin_timestamp {
                query_builder = query_builder.bind(origin_timestamp_val);
            }

            query_builder = match payload_format {
                PayloadFormat::Bytea => query_builder.bind(payload_bytes),
                PayloadFormat::Json => {
                    let json_value: serde_json::Value = serde_json::from_slice(&payload_bytes)
                        .map_err(|e| {
                            let err_msg = format!("Failed to parse payload as JSON: {e}");
                            error!("{err_msg}");
                            (sqlx::Error::Protocol(err_msg), false)
                        })?;
                    query_builder.bind(json_value)
                }
                PayloadFormat::Text => {
                    let text_value = String::from_utf8(payload_bytes).map_err(|e| {
                        let err_msg = format!("Failed to parse payload as UTF-8 text: {e}");
                        error!("{err_msg}");
                        (sqlx::Error::Protocol(err_msg), false)
                    })?;
                    query_builder.bind(text_value)
                }
            };
        }

        query_builder.execute(pool).await.map_err(|e| {
            let is_transient = is_transient_error(&e);
            (e, is_transient)
        })?;

        Ok(())
    }

    fn get_pool(&self) -> Result<&Pool<Postgres>, Error> {
        self.pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    fn parse_timestamp(&self, micros: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(
            (micros / 1_000_000) as i64,
            ((micros % 1_000_000) * 1_000) as u32,
        )
        .unwrap_or_else(Utc::now)
    }

    fn build_batch_insert_query(
        &self,
        table_name: &str,
        include_metadata: bool,
        include_checksum: bool,
        include_origin_timestamp: bool,
        row_count: usize,
    ) -> Result<(String, u32), Error> {
        let quoted_table = quote_identifier(table_name)?;
        let mut query = format!("INSERT INTO {quoted_table} (id");

        let mut params_per_row: u32 = 1; // id

        if include_metadata {
            query.push_str(
                ", iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id",
            );
            params_per_row += 5;
        }

        if include_checksum {
            query.push_str(", iggy_checksum");
            params_per_row += 1;
        }

        if include_origin_timestamp {
            query.push_str(", iggy_origin_timestamp");
            params_per_row += 1;
        }

        query.push_str(", payload");
        params_per_row += 1;

        query.push_str(") VALUES ");

        let mut value_groups = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let base_param = (row_idx as u32) * params_per_row;
            let mut values = format!("(${}::numeric", base_param + 1);
            for i in 2..=params_per_row {
                values.push_str(&format!(", ${}", base_param + i));
            }
            values.push(')');
            value_groups.push(values);
        }

        query.push_str(&value_groups.join(", "));

        Ok((query, params_per_row))
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

fn quote_identifier(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InitError("Table name cannot be empty".to_string()));
    }
    if name.contains('\0') {
        return Err(Error::InitError(
            "Table name cannot contain null characters".to_string(),
        ));
    }
    let escaped = name.replace('"', "\"\"");
    Ok(format!("\"{escaped}\""))
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
mod tests {
    use super::*;

    fn test_config() -> PostgresSinkConfig {
        PostgresSinkConfig {
            connection_string: "postgres://localhost/db".to_string(),
            target_table: "messages".to_string(),
            batch_size: Some(100),
            max_connections: None,
            auto_create_table: None,
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            payload_format: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
        }
    }

    #[test]
    fn given_json_format_should_return_json() {
        assert_eq!(
            PayloadFormat::from_config(Some("json")),
            PayloadFormat::Json
        );
        assert_eq!(
            PayloadFormat::from_config(Some("jsonb")),
            PayloadFormat::Json
        );
        assert_eq!(
            PayloadFormat::from_config(Some("JSON")),
            PayloadFormat::Json
        );
    }

    #[test]
    fn given_text_format_should_return_text() {
        assert_eq!(
            PayloadFormat::from_config(Some("text")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("TEXT")),
            PayloadFormat::Text
        );
    }

    #[test]
    fn given_bytea_or_unknown_format_should_return_bytea() {
        assert_eq!(
            PayloadFormat::from_config(Some("bytea")),
            PayloadFormat::Bytea
        );
        assert_eq!(
            PayloadFormat::from_config(Some("unknown")),
            PayloadFormat::Bytea
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Bytea);
    }

    #[test]
    fn given_payload_format_should_return_correct_sql_type() {
        assert_eq!(PayloadFormat::Bytea.sql_type(), "BYTEA");
        assert_eq!(PayloadFormat::Json.sql_type(), "JSONB");
        assert_eq!(PayloadFormat::Text.sql_type(), "TEXT");
    }

    #[test]
    fn given_all_options_enabled_should_build_full_insert_query() {
        let sink = PostgresSink::new(1, test_config());
        let (query, param_count) = sink
            .build_batch_insert_query("messages", true, true, true, 1)
            .expect("Failed to build query");

        assert!(query.contains("INSERT INTO \"messages\""));
        assert!(query.contains("iggy_offset"));
        assert!(query.contains("iggy_timestamp"));
        assert!(query.contains("iggy_stream"));
        assert!(query.contains("iggy_topic"));
        assert!(query.contains("iggy_partition_id"));
        assert!(query.contains("iggy_checksum"));
        assert!(query.contains("iggy_origin_timestamp"));
        assert!(query.contains("payload"));
        assert_eq!(param_count, 9);
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_insert_query() {
        let sink = PostgresSink::new(1, test_config());
        let (query, param_count) = sink
            .build_batch_insert_query("messages", false, false, false, 1)
            .expect("Failed to build query");

        assert!(query.contains("INSERT INTO \"messages\""));
        assert!(!query.contains("iggy_offset"));
        assert!(!query.contains("iggy_checksum"));
        assert!(!query.contains("iggy_origin_timestamp"));
        assert!(query.contains("payload"));
        assert_eq!(param_count, 2);
    }

    #[test]
    fn given_only_checksum_enabled_should_include_checksum() {
        let sink = PostgresSink::new(1, test_config());
        let (query, param_count) = sink
            .build_batch_insert_query("messages", false, true, false, 1)
            .expect("Failed to build query");

        assert!(!query.contains("iggy_offset"));
        assert!(query.contains("iggy_checksum"));
        assert!(!query.contains("iggy_origin_timestamp"));
        assert_eq!(param_count, 3);
    }

    #[test]
    fn given_microseconds_should_parse_timestamp_correctly() {
        let sink = PostgresSink::new(1, test_config());
        let micros: u64 = 1_767_225_600_000_000; // 2026-01-01 00:00:00 UTC
        let dt = sink.parse_timestamp(micros);

        assert_eq!(dt.timestamp(), 1_767_225_600);
    }

    #[test]
    fn given_default_config_should_use_default_retries() {
        let sink = PostgresSink::new(1, test_config());
        assert_eq!(sink.get_max_retries(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_custom_retries_should_use_custom_value() {
        let mut config = test_config();
        config.max_retries = Some(5);
        let sink = PostgresSink::new(1, config);
        assert_eq!(sink.get_max_retries(), 5);
    }

    #[test]
    fn given_default_config_should_use_default_retry_delay() {
        let sink = PostgresSink::new(1, test_config());
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config();
        config.retry_delay = Some("500ms".to_string());
        let sink = PostgresSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }

    #[test]
    fn given_verbose_logging_enabled_should_set_verbose_flag() {
        let mut config = test_config();
        config.verbose_logging = Some(true);
        let sink = PostgresSink::new(1, config);
        assert!(sink.verbose);
    }

    #[test]
    fn given_verbose_logging_disabled_should_not_set_verbose_flag() {
        let sink = PostgresSink::new(1, test_config());
        assert!(!sink.verbose);
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
    fn given_null_char_in_identifier_should_fail() {
        let result = quote_identifier("table\0name");
        assert!(result.is_err());
    }

    #[test]
    fn given_normal_identifier_should_quote() {
        let result = quote_identifier("my_table").expect("Failed to quote");
        assert_eq!(result, "\"my_table\"");
    }

    #[test]
    fn given_identifier_with_spaces_should_quote() {
        let result = quote_identifier("my table").expect("Failed to quote");
        assert_eq!(result, "\"my table\"");
    }

    #[test]
    fn given_identifier_with_sql_injection_should_escape() {
        let result = quote_identifier("messages\"; DROP TABLE users; --").expect("Failed to quote");
        assert_eq!(result, "\"messages\"\"; DROP TABLE users; --\"");
    }

    #[test]
    fn given_batch_of_3_rows_should_build_multi_row_insert_query() {
        let sink = PostgresSink::new(1, test_config());
        let (query, params_per_row) = sink
            .build_batch_insert_query("messages", true, true, true, 3)
            .expect("Failed to build batch query");

        // With all options: id + 5 metadata + checksum + origin_timestamp + payload = 9 params per row
        assert_eq!(params_per_row, 9);

        // Should have 3 value groups
        assert!(query.contains("($1::numeric, $2, $3, $4, $5, $6, $7, $8, $9)"));
        assert!(query.contains("($10::numeric, $11, $12, $13, $14, $15, $16, $17, $18)"));
        assert!(query.contains("($19::numeric, $20, $21, $22, $23, $24, $25, $26, $27)"));
    }

    #[test]
    fn given_batch_of_2_rows_minimal_should_build_correct_query() {
        let sink = PostgresSink::new(1, test_config());
        let (query, params_per_row) = sink
            .build_batch_insert_query("messages", false, false, false, 2)
            .expect("Failed to build batch query");

        // With minimal options: id + payload = 2 params per row
        assert_eq!(params_per_row, 2);

        // Should have 2 value groups
        assert!(query.contains("($1::numeric, $2)"));
        assert!(query.contains("($3::numeric, $4)"));
        assert!(!query.contains("$5"));
    }
}
