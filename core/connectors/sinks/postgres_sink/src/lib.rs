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
use chrono::{DateTime, Utc};
use humantime::Duration as HumanDuration;
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
        PostgresSink {
            id,
            pool: None,
            config,
            state: Mutex::new(State {
                messages_processed: 0,
                insertion_errors: 0,
            }),
            verbose,
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
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_type = self.payload_format().sql_type();

        let mut sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (");
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
        let table_name = &self.config.target_table;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_format = self.payload_format();

        for message in messages {
            let payload_bytes = message.payload.clone().try_into_vec().map_err(|e| {
                error!("Failed to convert payload to bytes: {e}");
                Error::InvalidRecord
            })?;

            let json_value = self.parse_json_payload(&payload_bytes, payload_format)?;
            let text_value = self.parse_text_payload(&payload_bytes, payload_format)?;

            let (query, _) = self.build_insert_query(
                table_name,
                include_metadata,
                include_checksum,
                include_origin_timestamp,
            );

            let timestamp = self.parse_timestamp(message.timestamp);
            let origin_timestamp = self.parse_timestamp(message.origin_timestamp);

            self.execute_insert_with_retry(
                pool,
                &query,
                message,
                topic_metadata,
                messages_metadata,
                include_metadata,
                include_checksum,
                include_origin_timestamp,
                timestamp,
                origin_timestamp,
                payload_format,
                &payload_bytes,
                &json_value,
                &text_value,
            )
            .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_insert_with_retry(
        &self,
        pool: &Pool<Postgres>,
        query: &str,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        include_metadata: bool,
        include_checksum: bool,
        include_origin_timestamp: bool,
        timestamp: DateTime<Utc>,
        origin_timestamp: DateTime<Utc>,
        payload_format: PayloadFormat,
        payload_bytes: &[u8],
        json_value: &Option<serde_json::Value>,
        text_value: &Option<String>,
    ) -> Result<(), Error> {
        let max_retries = self.get_max_retries();
        let retry_delay = self.get_retry_delay();
        let mut attempts = 0u32;

        loop {
            let mut query_obj = sqlx::query(query).bind(message.id.to_string());

            if include_metadata {
                query_obj = query_obj
                    .bind(message.offset as i64)
                    .bind(timestamp)
                    .bind(&topic_metadata.stream)
                    .bind(&topic_metadata.topic)
                    .bind(messages_metadata.partition_id as i32);
            }

            if include_checksum {
                query_obj = query_obj.bind(message.checksum as i64);
            }

            if include_origin_timestamp {
                query_obj = query_obj.bind(origin_timestamp);
            }

            query_obj = match payload_format {
                PayloadFormat::Bytea => query_obj.bind(payload_bytes.to_vec()),
                PayloadFormat::Json => query_obj.bind(json_value.clone()),
                PayloadFormat::Text => query_obj.bind(text_value.clone()),
            };

            match query_obj.execute(pool).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    handle_retry_error(e, attempts, max_retries)?;
                    tokio::time::sleep(retry_delay * attempts).await;
                }
            }
        }
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

    fn get_retry_delay(&self) -> Duration {
        let delay_str = self
            .config
            .retry_delay
            .as_deref()
            .unwrap_or(DEFAULT_RETRY_DELAY);
        HumanDuration::from_str(delay_str)
            .unwrap_or_else(|e| panic!("Invalid retry_delay '{delay_str}': {e}"))
            .into()
    }

    fn parse_timestamp(&self, micros: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(
            (micros / 1_000_000) as i64,
            ((micros % 1_000_000) * 1_000) as u32,
        )
        .unwrap_or_else(Utc::now)
    }

    fn parse_json_payload(
        &self,
        payload_bytes: &[u8],
        format: PayloadFormat,
    ) -> Result<Option<serde_json::Value>, Error> {
        match format {
            PayloadFormat::Json => {
                let value = serde_json::from_slice(payload_bytes).map_err(|e| {
                    error!("Failed to parse payload as JSON: {e}");
                    Error::InvalidRecord
                })?;
                Ok(Some(value))
            }
            _ => Ok(None),
        }
    }

    fn parse_text_payload(
        &self,
        payload_bytes: &[u8],
        format: PayloadFormat,
    ) -> Result<Option<String>, Error> {
        match format {
            PayloadFormat::Text => {
                let value = String::from_utf8(payload_bytes.to_vec()).map_err(|e| {
                    error!("Failed to parse payload as UTF-8 text: {e}");
                    Error::InvalidRecord
                })?;
                Ok(Some(value))
            }
            _ => Ok(None),
        }
    }

    fn build_insert_query(
        &self,
        table_name: &str,
        include_metadata: bool,
        include_checksum: bool,
        include_origin_timestamp: bool,
    ) -> (String, u32) {
        let mut query = format!("INSERT INTO {table_name} (id");
        let mut values = "($1::numeric".to_string();
        let mut param_count = 1;

        if include_metadata {
            query.push_str(
                ", iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id",
            );
            for i in 2..=6 {
                values.push_str(&format!(", ${i}"));
            }
            param_count = 6;
        }

        if include_checksum {
            param_count += 1;
            query.push_str(", iggy_checksum");
            values.push_str(&format!(", ${param_count}"));
        }

        if include_origin_timestamp {
            param_count += 1;
            query.push_str(", iggy_origin_timestamp");
            values.push_str(&format!(", ${param_count}"));
        }

        param_count += 1;
        query.push_str(", payload");
        values.push_str(&format!(", ${param_count}"));

        query.push_str(&format!(") VALUES {values})"));

        (query, param_count)
    }
}

fn handle_retry_error(e: sqlx::Error, attempts: u32, max_retries: u32) -> Result<bool, Error> {
    if attempts >= max_retries || !is_transient_error(&e) {
        error!("Database operation failed after {attempts} attempts: {e}");
        return Err(Error::InvalidRecord);
    }
    warn!("Transient database error (attempt {attempts}/{max_retries}): {e}. Retrying...");
    Ok(true)
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
        let (query, param_count) = sink.build_insert_query("messages", true, true, true);

        assert!(query.contains("INSERT INTO messages"));
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
        let (query, param_count) = sink.build_insert_query("messages", false, false, false);

        assert!(query.contains("INSERT INTO messages"));
        assert!(!query.contains("iggy_offset"));
        assert!(!query.contains("iggy_checksum"));
        assert!(!query.contains("iggy_origin_timestamp"));
        assert!(query.contains("payload"));
        assert_eq!(param_count, 2);
    }

    #[test]
    fn given_only_checksum_enabled_should_include_checksum() {
        let sink = PostgresSink::new(1, test_config());
        let (query, param_count) = sink.build_insert_query("messages", false, true, false);

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
        assert_eq!(sink.get_retry_delay(), Duration::from_secs(1));
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config();
        config.retry_delay = Some("500ms".to_string());
        let sink = PostgresSink::new(1, config);
        assert_eq!(sink.get_retry_delay(), Duration::from_millis(500));
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
}
