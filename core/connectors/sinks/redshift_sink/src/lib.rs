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

mod config;
mod s3;

use async_trait::async_trait;
use config::RedshiftSinkConfig;
use iggy_connector_sdk::{
    sink_connector, ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
};
use s3::S3Uploader;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

sink_connector!(RedshiftSink);

#[derive(Debug)]
pub struct RedshiftSink {
    id: u32,
    config: RedshiftSinkConfig,
    pool: Option<Pool<Postgres>>,
    s3_uploader: Option<S3Uploader>,
    state: Mutex<SinkState>,
}

#[derive(Debug, Default)]
struct SinkState {
    messages_processed: u64,
    batches_loaded: u64,
    load_errors: u64,
}

impl RedshiftSink {
    pub fn new(id: u32, config: RedshiftSinkConfig) -> Self {
        RedshiftSink {
            id,
            config,
            pool: None,
            s3_uploader: None,
            state: Mutex::new(SinkState::default()),
        }
    }

    async fn connect_redshift(&mut self) -> Result<(), Error> {
        let max_connections = self.config.max_connections.unwrap_or(5);
        let redacted = self
            .config
            .connection_string
            .chars()
            .take(20)
            .collect::<String>();

        info!(
            "Connecting to Redshift with max {} connections, connection: {}...",
            max_connections, redacted
        );

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect(&self.config.connection_string)
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to Redshift: {e}")))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| Error::InitError(format!("Redshift connectivity test failed: {e}")))?;

        self.pool = Some(pool);
        info!("Connected to Redshift cluster");
        Ok(())
    }

    fn init_s3_uploader(&mut self) -> Result<(), Error> {
        let uploader = S3Uploader::new(
            &self.config.s3_bucket,
            self.config.s3_prefix.as_deref().unwrap_or(""),
            &self.config.s3_region,
            self.config.aws_access_key_id.as_deref(),
            self.config.aws_secret_access_key.as_deref(),
            self.config.s3_endpoint.as_deref(),
        )?;
        self.s3_uploader = Some(uploader);
        info!(
            "Initialized S3 uploader for bucket: {}, region: {}{}",
            self.config.s3_bucket,
            self.config.s3_region,
            self.config
                .s3_endpoint
                .as_ref()
                .map_or(String::new(), |e| format!(", endpoint: {}", e))
        );
        Ok(())
    }

    async fn ensure_table_exists(&self) -> Result<(), Error> {
        if !self.config.auto_create_table.unwrap_or(false) {
            return Ok(());
        }

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;

        let table_name = &self.config.target_table;
        let include_metadata = self.config.include_metadata.unwrap_or(false);

        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                id VARCHAR(40) PRIMARY KEY,
                payload VARCHAR(MAX)"
        );

        if include_metadata {
            sql.push_str(
                ",
                iggy_offset BIGINT,
                iggy_timestamp TIMESTAMP,
                iggy_stream VARCHAR(256),
                iggy_topic VARCHAR(256),
                iggy_partition_id INTEGER",
            );
        }

        sql.push_str(
            ",
                created_at TIMESTAMP DEFAULT GETDATE()
            )",
        );

        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to create table '{table_name}': {e}")))?;

        info!("Ensured table '{}' exists in Redshift", table_name);
        Ok(())
    }

    async fn process_batch(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let s3_uploader = self
            .s3_uploader
            .as_ref()
            .ok_or_else(|| Error::InitError("S3 uploader not initialized".to_string()))?;

        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;

        // Convert messages to CSV
        let csv_data = self.messages_to_csv(topic_metadata, messages_metadata, messages)?;

        // Upload to S3
        let s3_key = s3_uploader.upload_csv(&csv_data).await?;
        let s3_path = format!("s3://{}/{}", self.config.s3_bucket, s3_key);

        info!(
            "Uploaded {} messages ({} bytes) to {}",
            messages.len(),
            csv_data.len(),
            s3_path
        );

        // Execute COPY command
        let copy_result = self.execute_copy(pool, &s3_path).await;

        // Cleanup S3 file if configured - always attempt cleanup regardless of COPY result
        if self.config.delete_staged_files.unwrap_or(true) {
            if let Err(e) = s3_uploader.delete_file(&s3_key).await {
                warn!("Failed to delete staged file {}: {}", s3_key, e);
            }
        }

        // Return COPY result after cleanup
        copy_result?;

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;
        state.batches_loaded += 1;

        info!(
            "Redshift sink ID: {} loaded {} messages to table '{}' (total: {}, batches: {})",
            self.id,
            messages.len(),
            self.config.target_table,
            state.messages_processed,
            state.batches_loaded
        );

        Ok(())
    }

    fn messages_to_csv(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<Vec<u8>, Error> {
        let delimiter = self.config.csv_delimiter.unwrap_or(',');
        let quote = self.config.csv_quote.unwrap_or('"');
        let include_metadata = self.config.include_metadata.unwrap_or(false);

        let mut csv_output = Vec::new();
        // Pre-allocate the escaped quote string for performance
        let escaped_quote = format!("{quote}{quote}");

        for message in messages {
            let payload_str = match &message.payload {
                Payload::Json(value) => simd_json::to_string(value).unwrap_or_else(|e| {
                    warn!(
                        "Failed to serialize JSON payload for message {}: {}",
                        message.id, e
                    );
                    String::new()
                }),
                Payload::Text(text) => text.clone(),
                Payload::Raw(bytes) => String::from_utf8_lossy(bytes).to_string(),
                _ => {
                    let bytes = message.payload.clone().try_into_vec().map_err(|e| {
                        error!("Failed to convert payload: {}", e);
                        Error::InvalidRecord
                    })?;
                    String::from_utf8_lossy(&bytes).to_string()
                }
            };

            // Escape quotes in payload
            let escaped_payload = payload_str.replace(quote, &escaped_quote);

            let mut row = format!(
                "{}{delim}{quote}{payload}{quote}",
                message.id,
                delim = delimiter,
                payload = escaped_payload
            );

            if include_metadata {
                // `message.timestamp` is in microseconds. Preserve microsecond precision
                // by converting to seconds and nanoseconds for `from_timestamp`.
                let timestamp_micros = message.timestamp;
                let timestamp_secs = (timestamp_micros / 1_000_000) as i64;
                let timestamp_nanos = ((timestamp_micros % 1_000_000) as u32) * 1_000;
                let timestamp = chrono::DateTime::from_timestamp(timestamp_secs, timestamp_nanos)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                    .unwrap_or_default();

                row.push_str(&format!(
                    "{delim}{offset}{delim}{ts}{delim}{quote}{stream}{quote}{delim}{quote}{topic}{quote}{delim}{partition}",
                    delim = delimiter,
                    offset = message.offset,
                    ts = timestamp,
                    stream = topic_metadata.stream,
                    topic = topic_metadata.topic,
                    partition = messages_metadata.partition_id
                ));
            }

            row.push('\n');
            csv_output.extend_from_slice(row.as_bytes());
        }

        Ok(csv_output)
    }

    async fn execute_copy(&self, pool: &Pool<Postgres>, s3_path: &str) -> Result<(), Error> {
        let table = &self.config.target_table;
        let delimiter = self.config.csv_delimiter.unwrap_or(',');
        let quote = self.config.csv_quote.unwrap_or('"');
        let max_errors = self.config.max_errors.unwrap_or(0);
        let include_metadata = self.config.include_metadata.unwrap_or(false);

        let columns = if include_metadata {
            "(id, payload, iggy_offset, iggy_timestamp, iggy_stream, iggy_topic, iggy_partition_id)"
        } else {
            "(id, payload)"
        };

        let credentials = if let Some(iam_role) = &self.config.iam_role {
            format!("IAM_ROLE '{}'", iam_role)
        } else if let (Some(key_id), Some(secret)) = (
            &self.config.aws_access_key_id,
            &self.config.aws_secret_access_key,
        ) {
            format!("ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}'", key_id, secret)
        } else {
            return Err(Error::InitError(
                "Either IAM role or AWS credentials must be provided".to_string(),
            ));
        };

        let compression = self
            .config
            .compression
            .as_deref()
            .map(|c| format!("{} ", c.to_uppercase()))
            .unwrap_or_default();

        let copy_sql = format!(
            "COPY {table} {columns}
             FROM '{s3_path}'
             {credentials}
             {compression}FORMAT AS CSV
             DELIMITER '{delimiter}'
             QUOTE '{quote}'
             MAXERROR {max_errors}
             REGION '{region}'",
            region = self.config.s3_region
        );

        let max_retries = self.config.max_retries.unwrap_or(3);
        let retry_delay = self.config.retry_delay_ms.unwrap_or(1000);

        for attempt in 0..=max_retries {
            match sqlx::query(&copy_sql).execute(pool).await {
                Ok(_) => return Ok(()),
                Err(e) if attempt < max_retries => {
                    let delay = retry_delay * 2u64.pow(attempt);
                    warn!(
                        "COPY command failed (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt + 1,
                        max_retries + 1,
                        e,
                        delay
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(e) => {
                    error!(
                        "COPY command failed after {} attempts: {}",
                        max_retries + 1,
                        e
                    );
                    let mut state = self.state.lock().await;
                    state.load_errors += 1;
                    return Err(Error::Storage(format!("COPY command failed: {e}")));
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for RedshiftSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Redshift sink connector ID: {}. Target: {}, S3 bucket: {}",
            self.id, self.config.target_table, self.config.s3_bucket
        );

        self.config.validate()?;
        self.init_s3_uploader()?;
        self.connect_redshift().await?;
        self.ensure_table_exists().await?;

        info!(
            "Redshift sink connector ID: {} initialized successfully",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let batch_size = self.config.batch_size.unwrap_or(10000) as usize;

        for chunk in messages.chunks(batch_size) {
            if let Err(e) = self
                .process_batch(topic_metadata, &messages_metadata, chunk)
                .await
            {
                error!(
                    "Failed to process batch for table '{}': {}",
                    self.config.target_table, e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Closing Redshift sink connector ID: {}. Stats: {} messages processed, {} batches loaded, {} errors",
            self.id, state.messages_processed, state.batches_loaded, state.load_errors
        );

        if let Some(pool) = self.pool.take() {
            pool.close().await;
        }

        info!("Redshift sink connector ID: {} closed", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection_string: "postgres://user:pass@localhost:5439/dev".to_string(),
            target_table: "test_table".to_string(),
            iam_role: Some("arn:aws:iam::123456789:role/RedshiftS3Access".to_string()),
            s3_bucket: "test-bucket".to_string(),
            s3_prefix: Some("staging/".to_string()),
            s3_region: "us-east-1".to_string(),
            s3_endpoint: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            batch_size: Some(1000),
            flush_interval_ms: None,
            csv_delimiter: Some(','),
            csv_quote: Some('"'),
            ignore_header: None,
            max_errors: Some(10),
            compression: None,
            delete_staged_files: Some(true),
            max_connections: Some(5),
            max_retries: Some(3),
            retry_delay_ms: Some(1000),
            include_metadata: Some(false),
            auto_create_table: Some(false),
        }
    }

    #[test]
    fn test_config_validation_valid() {
        let config = test_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_missing_credentials() {
        let mut config = test_config();
        config.iam_role = None;
        config.aws_access_key_id = None;
        config.aws_secret_access_key = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_partial_credentials() {
        let mut config = test_config();
        config.iam_role = None;
        config.aws_access_key_id = Some("AKIAIOSFODNN7EXAMPLE".to_string());
        config.aws_secret_access_key = None;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sink_creation() {
        let config = test_config();
        let sink = RedshiftSink::new(1, config);
        assert_eq!(sink.id, 1);
        assert!(sink.pool.is_none());
        assert!(sink.s3_uploader.is_none());
    }
}
