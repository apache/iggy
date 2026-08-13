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

mod config;

use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Array, RecordBatch, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use s3::{Bucket, Region, creds::Credentials};
use secrecy::ExposeSecret;
use sqlx::{AssertSqlSafe, Pool, Postgres, Row, postgres::PgPoolOptions};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{PayloadFormat, RedshiftSinkConfig};

sink_connector!(RedshiftSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_CONNECTIONS: u32 = 5;
const DEFAULT_ARCHIVE_PREFIX: &str = "archive/messages";

#[derive(Debug)]
pub struct RedshiftSink {
    pub id: u32,
    config: RedshiftSinkConfig,
    pool: Option<Pool<Postgres>>,
    state: Mutex<State>,
    verbose: bool,
    bucket: Option<Box<Bucket>>,
}

#[async_trait]
impl Sink for RedshiftSink {
    async fn open(&mut self) -> Result<(), Error> {
        tracing::info!(
            sink_id = self.id,
            table = %self.config.target_table, "opening Redshift sink connector"
        );

        self.connect().await?;
        // Ensuring tables exist
        self.ensure_tables_exist().await?;
        // Checking for schema drift
        self.ensure_schema_match().await?;
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        tracing::debug!(
            sink_id = self.id,
            count = messages.len(),
            "consuming messages"
        );
        self.process_messages(topic_metadata, &messages_metadata, &messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        tracing::info!(sink_id = self.id, "closing Redshift sink connector");

        if let Some(pool) = self.pool.take() {
            pool.close().await;

            tracing::debug!(sink_id = self.id, "database pool closed");
        }

        let state = self.state.lock().await;

        tracing::info!(
            sink_id = self.id,
            messages_processed = state.messages_processed,
            batches_loaded = state.batches_loaded,
            insertion_errors = state.insertion_errors,
            "Redshift sink connector closed",
        );

        Ok(())
    }
}

impl RedshiftSink {
    pub fn new(id: u32, config: RedshiftSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);

        Self {
            id,
            config,
            pool: None,
            state: Mutex::new(State::default()),
            verbose,
            bucket: None,
        }
    }

    async fn connect(&mut self) -> Result<(), Error> {
        let max_connections = self
            .config
            .max_connections
            .unwrap_or(DEFAULT_MAX_CONNECTIONS);

        let redacted = redact_connection_string(self.config.connection_string.expose_secret());

        tracing::info!(max_connections, dsn = %redacted, "connecting to Redshift");

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.config.connection_string.expose_secret())
            .await
            .map_err(|e| Error::InitError(format!("Failed to connect to Redshift: {e}")))?;

        sqlx::query("SELECT 1").execute(&pool).await.map_err(|e| {
            tracing::error!("Tracing failed: {:#?}", e);
            Error::InitError(format!("Warehouse connectivity test failed: {e}"))
        })?;

        self.pool = Some(pool);
        tracing::debug!("Redshift connection pool established");

        let region = self.build_region()?;

        let credentials = Credentials::new(
            self.config
                .aws_access_key_id
                .as_ref()
                .map(|v| v.expose_secret()),
            self.config
                .aws_secret_access_key
                .as_ref()
                .map(|v| v.expose_secret()),
            None,
            None,
            None,
        )
        .map_err(|e| {
            tracing::error!("Failed to create S3 credentials: {e}");
            Error::InvalidConfig
        })?;

        let mut bucket = Bucket::new(&self.config.s3_bucket, region, credentials).map_err(|e| {
            tracing::error!("Failed to create S3 bucket client: {e}");
            Error::InvalidConfig
        })?;

        if self.config.s3_endpoint.is_some() {
            bucket = bucket.with_path_style();
        }

        self.bucket = Some(bucket);

        tracing::info!("Redshift sink connector ready");

        Ok(())
    }

    fn build_region(&self) -> Result<Region, Error> {
        if let Some(endpoint) = &self.config.s3_endpoint {
            tracing::debug!(endpoint = %endpoint, "using custom S3 endpoint");
            Ok(Region::Custom {
                region: self.config.aws_region.clone(),
                endpoint: endpoint.clone(),
            })
        } else {
            Region::from_str(&self.config.aws_region).map_err(|_| Error::InvalidConfig)
        }
    }

    async fn ensure_tables_exist(&self) -> Result<(), Error> {
        let pool = self.get_pool()?;

        let target_table = quote_identifier(&self.config.target_table)?;
        let staging_table = quote_identifier(&format!("staging_{}", self.config.target_table))?;

        let payload_type = self.payload_format().sql_type();

        let target_query = self.build_create_table_sql(&target_table)?;

        let staging_query = self.build_create_table_sql(&staging_table)?;

        tracing::debug!("ensuring staging and target tables exist");

        sqlx::query(AssertSqlSafe(staging_query))
            .execute(pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e);
                Error::InitError(format!("Failed to create table '{staging_table}': {e}"))
            })?;

        tracing::debug!("Staging table created");

        sqlx::query(AssertSqlSafe(target_query))
            .execute(pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e);
                Error::InitError(format!("Failed to create table '{target_table}': {e}"))
            })?;

        tracing::info!(
            staging_table = staging_table,
            target_table = target_table,
            payload_type,
            "staging and target tables ready"
        );

        Ok(())
    }

    // This method ensures that the target table schema matches the expected schema.
    // it also verifies there is a created_at column
    async fn ensure_schema_match(&self) -> Result<(), Error> {
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let target_table = quote_identifier(&self.config.target_table)?;
        let staging_table = quote_identifier(&format!("staging_{}", self.config.target_table))?;
        let payload_type = self.payload_format().sql_type();
        let pool = self.get_pool()?;

        let mut expected_cols: BTreeMap<&str, &str> = BTreeMap::new();
        expected_cols.insert("id", "VARCHAR");
        if include_metadata {
            expected_cols.insert("iggy_offset", "VARCHAR");
            expected_cols.insert("iggy_timestamp", "VARCHAR");
            expected_cols.insert("iggy_stream", "TEXT");
            expected_cols.insert("iggy_topic", "TEXT");
            expected_cols.insert("iggy_partition_id", "BIGINT");
        }
        if include_checksum {
            expected_cols.insert("iggy_checksum", "VARCHAR");
        }
        if include_origin_timestamp {
            expected_cols.insert("iggy_origin_timestamp", "VARCHAR");
        }
        expected_cols.insert("payload", payload_type);
        expected_cols.insert("created_at", "VARCHAR");

        let target_cols = Self::load_columns(pool, &target_table).await?;
        let staging_cols = Self::load_columns(pool, &staging_table).await?;

        let mut mismatches = Self::diff_schema(&target_table, &target_cols, &expected_cols);
        mismatches.extend(Self::diff_schema(
            &staging_table,
            &staging_cols,
            &expected_cols,
        ));

        tracing::info!("Mismatches: {:?}", mismatches);

        if !mismatches.is_empty() {
            return Err(Error::InitError(format!(
                "Schema mismatch detected:\n{}",
                mismatches.join("\n")
            )));
        }

        Ok(())
    }

    fn diff_schema(
        table_name: &str,
        actual_cols: &BTreeMap<String, String>,
        expected_cols: &BTreeMap<&str, &str>,
    ) -> Vec<String> {
        let mut errors = Vec::new();

        for (col_name, expected_type) in expected_cols {
            match actual_cols.get(*col_name) {
                None => errors.push(format!(
                    "{table_name}: missing column '{col_name}' (expected {expected_type})"
                )),
                Some(actual_type) if !Self::type_matches(actual_type, expected_type) => errors.push(format!(
                    "{table_name}: column '{col_name}' type mismatch — expected {expected_type}, found {actual_type}"
                )),
                _ => {}
            }
        }
        errors
    }

    fn type_matches(actual: &str, expected: &str) -> bool {
        Self::normalize_type(actual) == Self::normalize_type(expected)
    }

    async fn load_columns(
        pool: &sqlx::PgPool,
        table: &str,
    ) -> Result<BTreeMap<String, String>, Error> {
        let query = format!(
            "SELECT \"column\", type FROM pg_table_def WHERE tablename = '{}'",
            table.replace('"', "")
        );

        let rows = sqlx::query(AssertSqlSafe(query))
            .fetch_all(pool)
            .await
            .map_err(|e| Error::InitError(format!("Failed to read schema for '{table}': {e}")))?;

        if rows.is_empty() {
            return Err(Error::InitError(format!(
                "Table '{table}' was not found or has no visible columns"
            )));
        }

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("column")
                        .map_err(|e| Error::InitError(e.to_string()))?,
                    Self::normalize_type(
                        &row.try_get::<String, _>("type")
                            .map_err(|e| Error::InitError(e.to_string()))?,
                    )
                    .to_string(),
                ))
            })
            .collect()
    }

    fn normalize_type(t: &str) -> &'static str {
        let base = t.split('(').next().unwrap_or(t).trim();

        match base.to_ascii_uppercase().as_str() {
            "INTEGER" | "INT" | "INT4" => "INTEGER",
            // e.g. "bigint"
            "INT8" | "BIGINT" => "BIGINT",
            // e.g "character varying(40)", "character varying(20)", "character varying(256)"
            "VARCHAR" | "CHARACTER VARYING" => "VARCHAR",
            // Having bytea because of the Postgres Test
            // e.g. "binary varying(64000)"
            "BYTEA" | "VARBYTE" | "VARBINARY" | "BINARY VARYING" => "VARBYTE",
            "TEXT" => "TEXT",
            // e.g. "timestamp with time zone"
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "TIMESTAMPTZ",
            _ => "UNKNOWN",
        }
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let batch_size = self.config.batch_size.unwrap_or(100) as usize;

        for batch in messages.chunks(batch_size) {
            match self
                .insert_batch(batch, topic_metadata, messages_metadata)
                .await
            {
                Ok(path) => {
                    // Messages were received and ingested to Redshift
                    if let Some(s3_path) = path {
                        // Truncate the staging table
                        if let Err(e) = self.staging_cleanup().await {
                            tracing::warn!(error = %e, "failed to cleanup staging table");
                        }

                        // Handle archiving
                        if let Err(e) = self.archive_parquet(&s3_path).await {
                            tracing::warn!(error = %e, "failed to archive parquet file: {}", s3_path);
                        }

                        self.state.lock().await.batches_loaded += 1
                    } else {
                        tracing::info!("Zero messages found for processing");
                    }
                }
                Err(e) => {
                    self.state.lock().await.insertion_errors += batch.len() as u64;
                    tracing::error!(error = %e, batch_size = batch.len(), "failed to insert batch");
                    return Err(e);
                }
            }
        }

        let mut state = self.state.lock().await;
        state.messages_processed += messages.len() as u64;

        if self.verbose {
            tracing::info!(
                sink_id = self.id,
                total_processed = state.messages_processed,
                batch_received = messages.len(),
                table = %self.config.target_table,
                batches_loaded = state.batches_loaded,
                "processed message batch"
            );
        } else {
            tracing::debug!(
                sink_id = self.id,
                total_processed = state.messages_processed,
                table = %self.config.target_table,
                "processed message batch"
            );
        }

        Ok(())
    }

    // This function builds a parquet from messages and metadata
    // It uploads the parquet to S3 and returns the path
    // It then copies the parquet to Redshift target table via
    // a staging table by means of a MERGE statement
    // This function treats parquet-generation, uploading to s3,
    // copying to staging and merging to target as atomic
    // process of focus for this sink connector
    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<Option<String>, Error> {
        if messages.is_empty() {
            return Ok(None);
        }

        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_format = self.payload_format();

        let record_batch = create_record_batch(
            topic_metadata,
            messages_metadata,
            messages,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            payload_format,
        )?;

        let content = encode_parquet(&record_batch)?;

        tracing::debug!(
            bytes = content.len(),
            rows = record_batch.num_rows(),
            "encoded parquet batch"
        );

        let s3_path = self.upload_parquet(&content).await?;

        let schema = record_batch.schema();
        let cols = schema
            .fields
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();

        // Copy the parquet file to Redshift staging
        // Cleanup
        tracing::info!("copying parquet to Redshift staging");
        if let Err(e) = self.copy_parquet(&s3_path, &cols).await {
            let key = s3_path
                .strip_prefix(&format!("s3://{}/", self.config.s3_bucket))
                .ok_or(Error::InvalidConfigValue("Missing Cleanup S3 path".into()))?;

            self.delete_object(key).await?;

            Err(e)?
        }

        tracing::info!("Redshift stging COPY completed");

        // Do a merge into Redshift target table
        self.insert_into_target(&cols).await?;

        tracing::info!("Redshift target table merge completed");

        tracing::info!(count = messages.len(), path = %s3_path, "batch inserted into Redshift");

        Ok(Some(s3_path))
    }

    async fn copy_parquet(&self, s3_path: &str, cols: &[&str]) -> Result<(), Error> {
        let max_retries = self.get_max_retries();
        let retry_delay = self.get_retry_delay();
        let staging_table = quote_identifier(&format!("staging_{}", self.config.target_table))?;

        let sql = self.build_copy_sql(&staging_table, s3_path, &cols.join(", "))?;
        let pool = self.get_pool()?;

        tracing::debug!(table = %self.config.target_table, s3_path, "issuing Redshift COPY");

        retry_with_backoff(
            "Redshift COPY",
            max_retries,
            retry_delay,
            is_transient_error,
            || async {
                sqlx::query(AssertSqlSafe(sql.as_str()))
                    .execute(pool)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

        tracing::debug!(staging_table = staging_table, "Redshift COPY completed");

        Ok(())
    }

    async fn insert_into_target(&self, cols: &[&str]) -> Result<(), Error> {
        let max_retries = self.get_max_retries();
        let retry_delay = self.get_retry_delay();
        let target_table = quote_identifier(&self.config.target_table)?;
        let staging_table = quote_identifier(&format!("staging_{}", self.config.target_table))?;
        let sql = self.build_insert_sql(cols, &staging_table, &target_table);

        let pool = self.get_pool()?;

        tracing::debug!(table = %self.config.target_table, "issuing Redshift MERGE");

        retry_with_backoff(
            "Redshift INSERT",
            max_retries,
            retry_delay,
            is_transient_error,
            || async {
                sqlx::query(AssertSqlSafe(sql.as_str()))
                    .execute(pool)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

        tracing::debug!(staging_table = %staging_table, target_table = %target_table, "Redshift INSERT completed");

        Ok(())
    }

    async fn staging_cleanup(&self) -> Result<(), Error> {
        let max_retries = self.get_max_retries();
        let retry_delay = self.get_retry_delay();
        let staging_table = quote_identifier(&format!("staging_{}", self.config.target_table))?;
        let sql = self.build_truncate_sql(&staging_table);
        let pool = self.get_pool()?;

        tracing::debug!(table = %self.config.target_table, "issuing Redshift TRUNCATE");

        retry_with_backoff(
            "Redshift TRUNCATE",
            max_retries,
            retry_delay,
            is_transient_error,
            || async {
                sqlx::query(AssertSqlSafe(sql.as_str()))
                    .execute(pool)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

        tracing::debug!(table = %self.config.target_table, "Redshift TRUNCATE completed");

        Ok(())
    }

    fn build_create_table_sql(&self, table_name: &str) -> Result<String, Error> {
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let include_checksum = self.config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        let payload_type = self.payload_format().sql_type();

        let mut query = format!("CREATE TABLE IF NOT EXISTS {table_name} (id VARCHAR(40)");

        if include_metadata {
            query.push_str(", iggy_offset VARCHAR(20), iggy_timestamp VARCHAR(20), iggy_stream TEXT, iggy_topic TEXT, iggy_partition_id BIGINT");
        }

        if include_checksum {
            query.push_str(", iggy_checksum VARCHAR");
        }

        if include_origin_timestamp {
            query.push_str(", iggy_origin_timestamp VARCHAR(20)");
        }

        query.push_str(&format!(", payload {payload_type}"));
        query.push_str(", created_at VARCHAR);");

        Ok(query)
    }

    fn build_copy_sql(
        &self,
        staging_table: &str,
        s3_path: &str,
        cols: &str,
    ) -> Result<String, Error> {
        // Redshift allows this from the docs
        // https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html
        let iam_role = quote_identifier(&self.config.aws_iam_role)?.replace('"', "");

        let region = quote_identifier(&self.config.aws_region)?.replace('"', "");

        Ok(format!(
            "COPY {} ({}) FROM '{}' CREDENTIALS 'aws_iam_role={}' FORMAT AS PARQUET REGION '{}';",
            staging_table, cols, s3_path, iam_role, region
        ))
    }

    fn build_insert_sql(&self, cols: &[&str], staging: &str, target: &str) -> String {
        let t_cols = cols.join(", ");

        let s_cols = cols
            .iter()
            .map(|v| format!("s.{v}"))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "
            INSERT INTO {} ({})
                SELECT {}
            FROM (SELECT sm.*, ROW_NUMBER() OVER (PARTITION BY sm.id ORDER BY sm.created_at) AS rn FROM {} sm) s
                WHERE s.rn = 1
                AND NOT EXISTS (SELECT 1 FROM {} t WHERE t.id = s.id);",
            target, t_cols, s_cols, staging, target
        )
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        format!("TRUNCATE {};", table)
    }

    async fn upload_parquet(&self, content: &[u8]) -> Result<String, Error> {
        let file_id = Uuid::now_v7();
        let key = build_s3_key(&self.config.s3_prefix, &format!("{file_id}.parquet"));
        let bucket = self.get_bucket()?;

        tracing::debug!(key = %key, bytes = content.len(), "uploading parquet to S3");

        let response = bucket.put_object(&key, content).await.map_err(|e| {
            tracing::error!("Failed to upload to S3 key '{key}': {e}");
            Error::Storage(format!("S3 upload failed: {e}"))
        })?;

        ensure_s3_status(response.status_code(), 200, "S3 upload")?;

        let path = format!("s3://{}{}", bucket.name(), key);
        tracing::info!(path = %path, bytes = content.len(), "uploaded parquet to S3");

        Ok(path)
    }

    async fn archive_parquet(&self, key: &str) -> Result<(), Error> {
        let old_key = key
            .strip_prefix(&format!("s3://{}/", self.config.s3_bucket))
            .unwrap_or(key);

        if !self.get_archive() {
            self.delete_object(old_key).await?;
            tracing::info!(key = old_key, "deleted parquet file (archiving disabled)");
            return Ok(());
        }

        let bucket = self.get_bucket()?;
        let prefix = self.config.s3_prefix.trim_matches('/');
        let archive_prefix = DEFAULT_ARCHIVE_PREFIX.trim_matches('/');

        let suffix = if prefix.is_empty() {
            old_key
        } else {
            old_key
                .strip_prefix(prefix)
                .map(|s| s.trim_start_matches('/'))
                .unwrap_or(old_key)
        };

        let archived_key = if archive_prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", archive_prefix, suffix)
        };

        tracing::debug!(from = old_key, to = %archived_key, "archiving parquet file");

        let status_code = bucket
            .copy_object_internal(old_key, &archived_key)
            .await
            .map_err(|e| {
                tracing::error!(key = old_key, error = %e, "failed to copy object for archiving");
                Error::Storage(format!("S3 archiving failed: {e}"))
            })?;

        ensure_s3_status(status_code, 200, "S3 archive copy")?;

        self.delete_object(old_key).await?;
        tracing::info!(archived_to = %archived_key, "archived parquet file");

        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<(), Error> {
        let bucket = self.get_bucket()?;

        let response = bucket.delete_object(key).await.map_err(|e| {
            tracing::error!(key, error = %e, "failed to delete S3 object");
            Error::Storage(format!("S3 deleting failed: {e}"))
        })?;
        ensure_s3_status(response.status_code(), 204, "S3 object deletion")?;

        tracing::debug!(key, "deleted S3 object");
        Ok(())
    }

    fn get_pool(&self) -> Result<&Pool<Postgres>, Error> {
        self.pool
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }

    fn get_bucket(&self) -> Result<&Bucket, Error> {
        let r = self
            .bucket
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))?;

        Ok(r)
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn get_max_retries(&self) -> u32 {
        self.config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }

    fn get_retry_delay(&self) -> Duration {
        self.config
            .retry_delay
            .as_deref()
            .unwrap_or(DEFAULT_RETRY_DELAY)
            .parse::<HumanDuration>()
            .map(Into::into)
            .unwrap_or_else(|_| Duration::from_secs(1))
    }

    fn get_archive(&self) -> bool {
        self.config.archive.unwrap_or(false)
    }
}

#[derive(Debug, Default)]
struct State {
    messages_processed: u64,
    batches_loaded: u64,
    insertion_errors: u64,
}

/// Generic retry helper with linear backoff, used for transient warehouse errors.
async fn retry_with_backoff<F, Fut, T>(
    operation: &str,
    max_retries: u32,
    base_delay: Duration,
    is_transient: impl Fn(&sqlx::Error) -> bool,
    mut op: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>,
{
    let mut attempts = 0u32;

    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                attempts += 1;
                let transient = is_transient(&e);

                if !transient || attempts >= max_retries {
                    tracing::error!(operation = operation, attempts = attempts, error = %e, "operation failed permanently");
                    return Err(Error::CannotStoreData(format!(
                        "{operation} failed after {attempts} attempts: {e}"
                    )));
                }

                tracing::warn!(operation, attempts, max_retries, error = %e, "transient error, retrying");
                tokio::time::sleep(base_delay * attempts).await;
            }
        }
    }
}

fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();

    let mut content = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut content, batch.schema(), Some(props)).map_err(|e| {
            tracing::error!(error = %e, "failed to create parquet writer");
            Error::WriteFailure(format!("Failed to create parquet writer: {e}"))
        })?;

    writer.write(batch).map_err(|e| {
        tracing::error!(error = %e, "failed to write parquet batch");
        Error::WriteFailure(format!("Failed to write parquet: {e}"))
    })?;

    writer.close().map_err(|e| {
        tracing::error!(error = %e, "failed to close parquet writer");
        Error::WriteFailure(format!("Failed to close writer: {e}"))
    })?;

    Ok(content)
}

fn build_s3_key(prefix: &str, filename: &str) -> String {
    if prefix.is_empty() {
        format!("/{filename}")
    } else {
        format!("/{}/{filename}", prefix.trim_end_matches('/'))
    }
}

fn ensure_s3_status<T: PartialEq + std::fmt::Display>(
    status: T,
    expected: T,
    context: &str,
) -> Result<(), Error> {
    if status != expected {
        tracing::error!(context, %status, "unexpected S3 response status");
        return Err(Error::Storage(format!(
            "{context} failed with status {status}"
        )));
    }
    Ok(())
}

fn create_record_batch(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    messages: &[ConsumedMessage],
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    payload_format: PayloadFormat,
) -> Result<RecordBatch, Error> {
    let mut fields = vec![Field::new("id", DataType::Utf8, false)];
    let mut columns: Vec<ArrayRef> = vec![id_column(messages)];

    if include_metadata {
        let (mut metadata_fields, mut metadata_columns) =
            metadata_columns(topic_metadata, messages_metadata, messages);
        fields.append(&mut metadata_fields);
        columns.append(&mut metadata_columns);
    }

    if include_checksum {
        fields.push(Field::new("iggy_checksum", DataType::Utf8, false));
        columns.push(checksum_column(messages));
    }

    if include_origin_timestamp {
        fields.push(Field::new("iggy_origin_timestamp", DataType::Utf8, false));
        columns.push(origin_timestamp_column(messages));
    }

    fields.push(Field::new("payload", payload_format.arrow_type(), false));
    columns.push(payload_column(messages, payload_format)?);

    fields.push(Field::new("created_at", DataType::Utf8, false));
    columns.push(created_at_column(messages.len())?);

    let schema = Arc::new(Schema::new(fields));
    let batch =
        RecordBatch::try_new(schema, columns).map_err(|e| Error::CannotStoreData(e.to_string()))?;

    tracing::debug!(
        rows = batch.num_rows(),
        columns = batch.num_columns(),
        "built record batch"
    );

    Ok(batch)
}

fn id_column(messages: &[ConsumedMessage]) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        messages.iter().map(|v| v.id.to_string()),
    ))
}

fn metadata_columns(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    messages: &[ConsumedMessage],
) -> (Vec<Field>, Vec<ArrayRef>) {
    let fields = vec![
        Field::new("iggy_offset", DataType::Utf8, false),
        Field::new("iggy_timestamp", DataType::Utf8, false),
        Field::new("iggy_stream", DataType::Utf8, false),
        Field::new("iggy_topic", DataType::Utf8, false),
        Field::new("iggy_partition_id", DataType::Int64, false),
    ];

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            messages.iter().map(|v| v.offset.to_string()),
        )),
        Arc::new(StringArray::from_iter_values(
            messages.iter().map(|v| v.timestamp.to_string()),
        )),
        Arc::new(StringArray::from_iter_values(
            (0..messages.len()).map(|_| topic_metadata.stream.clone()),
        )),
        Arc::new(StringArray::from_iter_values(
            (0..messages.len()).map(|_| topic_metadata.topic.clone()),
        )),
        Arc::new(Int64Array::from_iter_values(
            (0..messages.len()).map(|_| messages_metadata.partition_id as i64),
        )),
    ];

    (fields, columns)
}

fn checksum_column(messages: &[ConsumedMessage]) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        messages.iter().map(|v| v.checksum.to_string()),
    ))
}

fn origin_timestamp_column(messages: &[ConsumedMessage]) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        messages.iter().map(|v| v.origin_timestamp.to_string()),
    ))
}

fn payload_column(messages: &[ConsumedMessage], format: PayloadFormat) -> Result<ArrayRef, Error> {
    match format {
        PayloadFormat::Varbyte => {
            let mut builder = BinaryBuilder::with_capacity(messages.len(), 0);

            for m in messages {
                builder.append_value(m.payload.try_to_bytes()?);
            }

            Ok(Arc::new(builder.finish()))
        }
        PayloadFormat::Text => {
            let mut builder = StringBuilder::with_capacity(messages.len(), 0);

            for m in messages {
                let bytes = m.payload.try_to_bytes()?;
                let s = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidTextPayload)?;

                builder.append_value(s);
            }

            Ok(Arc::new(builder.finish()))
        }
    }
}

fn created_at_column(size: usize) -> Result<ArrayRef, Error> {
    let now = chrono::Utc::now().to_rfc3339();

    let mut builder = StringBuilder::with_capacity(size, 0);

    for _ in 0..size {
        builder.append_value(now.as_str());
    }

    Ok(Arc::new(builder.finish()))
}

fn redact_connection_string(conn_str: &str) -> String {
    // Guard against very short strings
    const PREVIEW_LEN: usize = 3;

    if let Some(scheme_end) = conn_str.find("://") {
        let scheme = &conn_str[..scheme_end + 3];
        let rest = &conn_str[scheme_end + 3..];

        let bound_end = rest.find([':', '@', '?', '/']).unwrap_or(rest.len());

        // Stop preview at the first sensitive boundary
        let safe_end = rest
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(rest.len()))
            .take_while(|&i| i <= bound_end)
            .nth(PREVIEW_LEN)
            .unwrap_or(bound_end);

        let preview = &rest[..safe_end];
        return format!("{scheme}{preview}***");
    }

    let preview: String = conn_str.chars().take(3).collect();
    format!("{preview}***")
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use iggy_connector_sdk::{Payload, Schema};
    use secrecy::SecretString;

    use super::*;

    fn test_config(
        include_checksum: bool,
        include_origin_timestamp: bool,
        include_metadata: bool,
    ) -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection_string: SecretString::from("postgresql://localhost/db"),
            target_table: "messages".to_string(),
            batch_size: Some(100),
            max_connections: None,
            include_metadata: Some(include_metadata),
            include_checksum: Some(include_checksum),
            include_origin_timestamp: Some(include_origin_timestamp),
            payload_format: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
            aws_access_key_id: Some(SecretString::from("admin")),
            aws_secret_access_key: Some(SecretString::from("password")),
            aws_iam_role: "arn:aws:iam::123456789012:role/Iggy".into(),
            s3_bucket: "iggymessages".into(),
            s3_prefix: "iggy/messages".into(),
            s3_endpoint: None,
            aws_region: "us-east-1".into(),
            archive: None,
        }
    }

    fn test_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

    fn test_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 7,
            current_offset: 0,
            schema: Schema::Json,
        }
    }

    fn test_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 9,
            checksum: 123,
            timestamp: 1_767_225_600_000_000,
            origin_timestamp: 1_700_000_000_000_001,
            headers: None,
            payload,
        }
    }

    fn json_payload(value: serde_json::Value) -> Payload {
        let mut bytes = serde_json::to_vec(&value).expect("Failed to serialize JSON");
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("Failed to parse JSON"))
    }

    #[test]
    fn given_empty_connection_string_should_error() {
        let mut config = test_config(false, false, false);
        config.connection_string = SecretString::default();

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_empty_target_table_should_error() {
        let mut config = test_config(false, false, false);
        config.target_table = String::new();

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_empty_s3_bucket_should_error() {
        let mut config = test_config(false, false, false);
        config.s3_bucket = String::new();

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_empty_aws_region_should_error() {
        let mut config = test_config(false, false, false);
        config.aws_region = String::new();

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_empty_aws_access_key_id_should_error() {
        let mut config = test_config(false, false, false);
        config.aws_access_key_id = Some(SecretString::default());

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_empty_aws_secret_access_key_should_error() {
        let mut config = test_config(false, false, false);
        config.aws_secret_access_key = Some(SecretString::default());

        assert!(config.validate().is_err());
    }

    #[test]
    fn given_json_format_should_return_text() {
        assert_eq!(
            PayloadFormat::from_config(Some("json")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("JSON")),
            PayloadFormat::Text
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
            PayloadFormat::Varbyte
        );
        assert_eq!(
            PayloadFormat::from_config(Some("unknown")),
            PayloadFormat::Varbyte
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Varbyte);
    }

    #[test]
    fn given_payload_format_should_return_correct_sql_type() {
        assert_eq!(PayloadFormat::Varbyte.sql_type(), "VARBYTE");
        assert_eq!(PayloadFormat::Text.sql_type(), "VARCHAR");
    }

    #[test]
    fn given_payload_format_should_return_correct_arrow_type() {
        assert_eq!(
            PayloadFormat::Varbyte.arrow_type(),
            arrow::datatypes::DataType::Binary
        );
        assert_eq!(
            PayloadFormat::Text.arrow_type(),
            arrow::datatypes::DataType::Utf8
        );
    }

    #[test]
    fn given_all_options_enabled_should_build_full_create_query() {
        let sink = RedshiftSink::new(1, test_config(true, true, true));

        let target_table =
            quote_identifier(&sink.config.target_table).expect("Failed to quote table identifier");

        let query = sink
            .build_create_table_sql(&target_table)
            .expect("Failed to build create query");

        assert!(query.contains("CREATE TABLE IF NOT EXISTS \"messages\""));
        assert!(query.contains("iggy_offset"));
        assert!(query.contains("iggy_timestamp"));
        assert!(query.contains("iggy_stream"));
        assert!(query.contains("iggy_topic"));
        assert!(query.contains("iggy_partition_id"));
        assert!(query.contains("iggy_checksum"));
        assert!(query.contains("iggy_origin_timestamp"));
        assert!(query.contains("payload"));
        assert!(query.contains("created_at"));
    }

    #[test]
    fn given_all_options_enabled_should_build_full_parquet() {
        let payload = json_payload(serde_json::json!({"name": "Bebeto", "active": true}));
        let message = test_message(payload);
        let record_batch = create_record_batch(
            &test_topic_metadata(),
            &test_messages_metadata(),
            &[message],
            true,
            true,
            true,
            PayloadFormat::Varbyte,
        )
        .expect("Failed to create record batch");

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 10);

        let columns = record_batch.schema();
        let columns: HashSet<&str> = columns.fields().iter().map(|f| f.name().as_ref()).collect();

        let expected_columns: HashSet<&str> = HashSet::from([
            "id",
            "iggy_offset",
            "iggy_timestamp",
            "iggy_stream",
            "iggy_topic",
            "iggy_partition_id",
            "iggy_checksum",
            "iggy_origin_timestamp",
            "payload",
            "created_at",
        ]);

        assert_eq!(columns.difference(&expected_columns).count(), 0);
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_create_query() {
        let sink = RedshiftSink::new(1, test_config(false, false, false));

        let target_table =
            quote_identifier(&sink.config.target_table).expect("Failed to quote table identifier");
        let query = sink
            .build_create_table_sql(&target_table)
            .expect("Failed to build create query");

        assert!(query.contains("CREATE TABLE IF NOT EXISTS \"messages\""));
        assert!(!query.contains("iggy_offset"));
        assert!(!query.contains("iggy_timestamp"));
        assert!(!query.contains("iggy_stream"));
        assert!(!query.contains("iggy_topic"));
        assert!(!query.contains("iggy_partition_id"));
        assert!(!query.contains("iggy_checksum"));
        assert!(!query.contains("iggy_origin_timestamp"));
        assert!(query.contains("payload"));
        assert!(query.contains("created_at"));
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_parquet() {
        let payload = json_payload(serde_json::json!({"name": "Bebeto", "active": true}));
        let message = test_message(payload);
        let record_batch = create_record_batch(
            &test_topic_metadata(),
            &test_messages_metadata(),
            &[message],
            false,
            false,
            false,
            PayloadFormat::Varbyte,
        )
        .expect("Failed to create record batch");

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 3);

        let columns = record_batch.schema();
        let columns: HashSet<&str> = columns.fields().iter().map(|f| f.name().as_ref()).collect();

        let expected_columns: HashSet<&str> = HashSet::from(["id", "payload", "created_at"]);

        assert_eq!(columns.difference(&expected_columns).count(), 0);
    }

    #[test]
    fn given_microseconds_should_parse_timestamp_correctly() {
        let record_batch = create_record_batch(
            &test_topic_metadata(),
            &test_messages_metadata(),
            &[test_message(json_payload(serde_json::json!({})))],
            true,
            false,
            false,
            PayloadFormat::Varbyte,
        )
        .expect("Failed to create record batch");

        let timestamp_col = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to Timestamp Microsecond array");

        let timestamp = timestamp_col.value(0);

        assert_eq!(timestamp, "1767225600000000");
    }

    #[test]
    fn given_default_config_should_use_default_archive() {
        let sink = RedshiftSink::new(1, test_config(false, false, false));
        assert!(!sink.get_archive());
    }

    #[test]
    fn given_archive_enabled_should_use_archive() {
        let mut sink = RedshiftSink::new(1, test_config(false, false, false));
        sink.config.archive = Some(true);

        assert!(sink.get_archive());
    }

    #[test]
    fn given_default_config_should_use_default_retries() {
        let sink = RedshiftSink::new(1, test_config(false, false, false));
        assert_eq!(sink.get_max_retries(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn given_custom_retries_should_use_custom_value() {
        let mut config = test_config(false, false, false);
        config.max_retries = Some(5);
        let sink = RedshiftSink::new(1, config);
        assert_eq!(sink.get_max_retries(), 5);
    }

    #[test]
    fn given_default_config_should_use_default_retry_delay() {
        let sink = RedshiftSink::new(1, test_config(false, false, false));
        assert_eq!(sink.get_retry_delay(), Duration::from_secs(1));
    }

    #[test]
    fn given_custom_retry_delay_should_parse_humantime() {
        let mut config = test_config(false, false, false);
        config.retry_delay = Some("500ms".to_string());
        let sink = RedshiftSink::new(1, config);
        assert_eq!(sink.get_retry_delay(), Duration::from_millis(500));
    }

    #[test]
    fn given_verbose_logging_enabled_should_set_verbose_flag() {
        let mut config = test_config(false, false, false);
        config.verbose_logging = Some(true);
        let sink = RedshiftSink::new(1, config);
        assert!(sink.verbose);
    }

    #[test]
    fn given_verbose_logging_disabled_should_not_set_verbose_flag() {
        let sink = RedshiftSink::new(1, test_config(false, false, false));
        assert!(!sink.verbose);
    }

    #[test]
    fn given_connection_string_with_credentials_should_redact() {
        let conn = "postgres://redshift:redshift@localhost:5432/db";
        let redacted = redact_connection_string(conn);
        assert_eq!(redacted, "postgres://red***");
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
}
