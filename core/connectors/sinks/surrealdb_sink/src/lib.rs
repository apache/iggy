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
use base64::engine::general_purpose;
use bytes::Bytes;
use iggy_connector_sdk::convert::owned_value_to_serde_json;
use iggy_connector_sdk::retry::{exponential_backoff, jitter, parse_duration};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Body, Client as HttpClient, RequestBuilder, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::fmt;
use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

sink_connector!(SurrealDbSink);

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_QUERY_TIMEOUT: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "100ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const ENCODING_BASE64: &str = "base64";
const ENCODING_JSON: &str = "json";
const ENCODING_TEXT: &str = "text";

type SurrealDbClient = HttpClient;

#[derive(Debug)]
pub struct SurrealDbSink {
    id: u32,
    client: Mutex<Option<SurrealDbClient>>,
    reconnecting: AtomicBool,
    base_url: String,
    endpoint: String,
    namespace: String,
    database: String,
    table: String,
    username: Option<String>,
    password: Option<SecretString>,
    auth_scope_config: Option<String>,
    auth_scope: AuthScope,
    payload_format_config: Option<String>,
    payload_format: PayloadFormat,
    use_tls: bool,
    batch_size: usize,
    query_timeout: Duration,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    include_metadata: bool,
    include_headers: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    auto_define_table: bool,
    define_indexes: bool,
    verbose: bool,
    messages_processed: AtomicU64,
    insertion_errors: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurrealDbSinkConfig {
    pub endpoint: String,
    pub namespace: String,
    pub database: String,
    pub table: String,
    pub username: Option<String>,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub password: Option<SecretString>,
    pub auth_scope: Option<String>,
    pub use_tls: Option<bool>,
    pub auto_define_table: Option<bool>,
    pub define_indexes: Option<bool>,
    pub batch_size: Option<u32>,
    pub payload_format: Option<String>,
    pub include_metadata: Option<bool>,
    pub include_headers: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub query_timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthScope {
    Root,
    Namespace,
    Database,
    None,
}

impl AuthScope {
    fn parse_config(value: Option<&str>) -> Result<Self, Error> {
        match value {
            Some(value) if value.eq_ignore_ascii_case("namespace") => Ok(AuthScope::Namespace),
            Some(value) if value.eq_ignore_ascii_case("database") => Ok(AuthScope::Database),
            Some(value) if value.eq_ignore_ascii_case("none") => Ok(AuthScope::None),
            Some(value) if value.eq_ignore_ascii_case("root") => Ok(AuthScope::Root),
            Some(value) => Err(Error::InvalidConfigValue(format!(
                "SurrealDB auth_scope must be one of root, namespace, database, or none: {value}"
            ))),
            None => Ok(AuthScope::Root),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadFormat {
    Auto,
    Json,
    Text,
    Base64,
}

impl PayloadFormat {
    fn parse_config(value: Option<&str>) -> Result<Self, Error> {
        match value {
            Some(value) if value.eq_ignore_ascii_case("json") => Ok(PayloadFormat::Json),
            Some(value) if value.eq_ignore_ascii_case("text") => Ok(PayloadFormat::Text),
            Some(value) if value.eq_ignore_ascii_case("base64") => Ok(PayloadFormat::Base64),
            Some(value) if value.eq_ignore_ascii_case("binary") => Ok(PayloadFormat::Base64),
            Some(value) if value.eq_ignore_ascii_case("auto") => Ok(PayloadFormat::Auto),
            Some(value) => Err(Error::InvalidConfigValue(format!(
                "SurrealDB payload_format must be one of auto, json, text, base64, or binary: {value}"
            ))),
            None => Ok(PayloadFormat::Auto),
        }
    }
}

#[derive(Debug)]
struct PayloadDocument {
    value: Value,
    encoding: &'static str,
}

#[derive(Debug)]
struct BatchInsertOutcome {
    inserted_count: u64,
    error_count: u64,
    error: Option<Error>,
}

#[derive(Debug, Deserialize)]
struct SurrealSqlStatement {
    status: String,
    detail: Option<String>,
    result: Option<Value>,
}

#[derive(Debug)]
enum SurrealDbRequestError {
    Request(reqwest::Error),
    HttpStatus { status: StatusCode, body: String },
    Query(String),
    Decode(String),
}

impl fmt::Display for SurrealDbRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SurrealDbRequestError::Request(error) => write!(formatter, "{error}"),
            SurrealDbRequestError::HttpStatus { status, body } => {
                write!(formatter, "HTTP status {status}: {body}")
            }
            SurrealDbRequestError::Query(message) | SurrealDbRequestError::Decode(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl SurrealDbSink {
    pub fn new(id: u32, config: SurrealDbSinkConfig) -> Self {
        let endpoint = config.endpoint.clone();
        let namespace = config.namespace.clone();
        let database = config.database.clone();
        let table = config.table.clone();
        let username = config.username.clone();
        let password = config.password.clone();
        let auth_scope_config = config.auth_scope.clone();
        let payload_format_config = config.payload_format.clone();
        let use_tls = config.use_tls.unwrap_or(false);
        let base_url = build_base_url(&endpoint, use_tls);
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE as u32)
            .max(1) as usize;
        let query_timeout = parse_duration(config.query_timeout.as_deref(), DEFAULT_QUERY_TIMEOUT);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let max_retries = match config.max_retries {
            Some(0) => {
                warn!("SurrealDB sink ID: {id} max_retries must be at least 1. Using 1 attempt.");
                1
            }
            Some(max_retries) => max_retries,
            None => DEFAULT_MAX_RETRIES,
        };
        if max_retry_delay < retry_delay {
            warn!(
                "SurrealDB sink ID: {id} max_retry_delay is smaller than retry_delay. Using retry_delay as max_retry_delay."
            );
            max_retry_delay = retry_delay;
        }
        let include_metadata = config.include_metadata.unwrap_or(true);
        let include_headers = config.include_headers.unwrap_or(true);
        let include_checksum = config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = config.include_origin_timestamp.unwrap_or(true);
        let auto_define_table = config.auto_define_table.unwrap_or(false);
        let define_indexes = config.define_indexes.unwrap_or(false);
        let verbose = config.verbose_logging.unwrap_or(false);

        SurrealDbSink {
            id,
            client: Mutex::new(None),
            reconnecting: AtomicBool::new(false),
            base_url,
            endpoint,
            namespace,
            database,
            table,
            username,
            password,
            auth_scope_config,
            auth_scope: AuthScope::Root,
            payload_format_config,
            payload_format: PayloadFormat::Auto,
            use_tls,
            batch_size,
            query_timeout,
            max_retries,
            retry_delay,
            max_retry_delay,
            include_metadata,
            include_headers,
            include_checksum,
            include_origin_timestamp,
            auto_define_table,
            define_indexes,
            verbose,
            messages_processed: AtomicU64::new(0),
            insertion_errors: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Sink for SurrealDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.auth_scope = AuthScope::parse_config(self.auth_scope_config.as_deref())?;
        self.payload_format = PayloadFormat::parse_config(self.payload_format_config.as_deref())?;
        validate_endpoint_config(&self.endpoint, self.use_tls)?;
        validate_identifier("namespace", &self.namespace)?;
        validate_identifier("database", &self.database)?;
        validate_identifier("table", &self.table)?;

        if self.auto_define_table && self.auth_scope != AuthScope::Root {
            return Err(Error::InvalidConfigValue(
                "SurrealDB auto_define_table requires auth_scope=root because namespace/database DDL is executed"
                    .to_string(),
            ));
        }

        if self.define_indexes && !self.include_metadata {
            return Err(Error::InvalidConfigValue(
                "SurrealDB define_indexes requires include_metadata=true because indexes use metadata fields"
                    .to_string(),
            ));
        }

        if self.define_indexes && !self.auto_define_table {
            warn!(
                "SurrealDB sink ID: {} define_indexes=true requires auto_define_table=true; index DDL will not run.",
                self.id
            );
        }

        info!(
            "Opening SurrealDB sink connector with ID: {}. Endpoint: {}, namespace: {}, database: {}, table: {}",
            self.id, self.base_url, self.namespace, self.database, self.table
        );

        let client = self.connect_and_select().await?;
        *self.client.lock().await = Some(client);
        info!(
            "Opened SurrealDB sink connector ID: {} for table: {}",
            self.id, self.table
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.process_messages(topic_metadata, &messages_metadata, messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing SurrealDB sink connector with ID: {}", self.id);
        self.client.get_mut().take();

        let messages_processed = self.messages_processed.load(Ordering::Relaxed);
        let insertion_errors = self.insertion_errors.load(Ordering::Relaxed);
        info!(
            "SurrealDB sink ID: {} processed {} messages with {} errors",
            self.id, messages_processed, insertion_errors
        );
        Ok(())
    }
}

impl SurrealDbSink {
    async fn connect_and_select(&self) -> Result<SurrealDbClient, Error> {
        let client = self.connect()?;
        self.signin_if_configured(&client).await?;
        self.health_check(&client).await?;

        if self.auto_define_table {
            self.ensure_namespace_database(&client).await?;
            self.ensure_table(&client).await?;
        }

        Ok(client)
    }

    fn connect(&self) -> Result<SurrealDbClient, Error> {
        HttpClient::builder()
            .timeout(self.query_timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create SurrealDB HTTP client: {e}")))
    }

    async fn get_client(&self) -> Result<SurrealDbClient, Error> {
        self.client
            .lock()
            .await
            .clone()
            .ok_or_else(|| Error::InitError("SurrealDB sink is not connected".to_string()))
    }

    async fn reconnect(&self) -> Result<bool, Error> {
        if self
            .reconnecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            debug!(
                "Skipping SurrealDB reconnect for connector ID: {} because another reconnect is in progress",
                self.id
            );
            tokio::time::sleep(self.retry_delay).await;
            return Ok(false);
        }

        warn!("Reconnecting SurrealDB sink connector ID: {}", self.id);
        let result = async {
            let client = self.connect_and_select().await?;
            *self.client.lock().await = Some(client);
            Ok(())
        }
        .await;
        self.reconnecting.store(false, Ordering::Release);
        result.map(|()| true)
    }

    async fn signin_if_configured(&self, client: &SurrealDbClient) -> Result<(), Error> {
        if self.auth_scope == AuthScope::None {
            return Ok(());
        }

        let username = self.username.as_ref().ok_or_else(|| {
            Error::InitError(
                "SurrealDB username is required when auth_scope is not none".to_string(),
            )
        })?;
        let password = self.password.as_ref().ok_or_else(|| {
            Error::InitError(
                "SurrealDB password is required when auth_scope is not none".to_string(),
            )
        })?;
        let mut payload = Map::new();
        payload.insert("user".to_string(), Value::String(username.clone()));
        payload.insert(
            "pass".to_string(),
            Value::String(password.expose_secret().to_string()),
        );

        if matches!(self.auth_scope, AuthScope::Namespace | AuthScope::Database) {
            payload.insert("ns".to_string(), Value::String(self.namespace.clone()));
        }
        if matches!(self.auth_scope, AuthScope::Database) {
            payload.insert("db".to_string(), Value::String(self.database.clone()));
        }

        let response = client
            .post(format!("{}/signin", self.base_url))
            .json(&Value::Object(payload))
            .send()
            .await
            .map_err(|e| Error::InitError(format!("Failed to authenticate with SurrealDB: {e}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("failed to read response body: {e}"));
        Err(Error::InitError(format!(
            "Failed to authenticate with SurrealDB: HTTP status {status}: {body}"
        )))
    }

    async fn ensure_table(&self, client: &SurrealDbClient) -> Result<(), Error> {
        let table = &self.table;
        let mut query = format!("DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS;");

        if self.define_indexes {
            let offset_index = format!("{table}_iggy_offset_idx");
            validate_identifier("index", &offset_index)?;
            query.push_str(&format!(
                " DEFINE INDEX IF NOT EXISTS {offset_index} ON TABLE {table} FIELDS iggy_stream, iggy_topic, iggy_partition_id, iggy_offset;"
            ));
        }

        self.execute_sql(client, query)
            .await
            .map_err(|e| Error::InitError(format!("Failed to define SurrealDB table: {e}")))?;

        Ok(())
    }

    async fn ensure_namespace_database(&self, client: &SurrealDbClient) -> Result<(), Error> {
        let query = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {}; USE NS {}; DEFINE DATABASE IF NOT EXISTS {};",
            self.namespace, self.namespace, self.database
        );

        self.execute_sql_without_scope(client, query)
            .await
            .map_err(|e| {
                Error::InitError(format!(
                    "Failed to define SurrealDB namespace/database: {e}"
                ))
            })?;

        Ok(())
    }

    async fn health_check(&self, client: &SurrealDbClient) -> Result<(), Error> {
        let response = client
            .get(format!("{}/health", self.base_url))
            .send()
            .await
            .map_err(|e| Error::InitError(format!("SurrealDB health check failed: {e}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("failed to read response body: {e}"));
        Err(Error::InitError(format!(
            "SurrealDB health check failed: HTTP status {status}: {body}"
        )))
    }

    async fn execute_sql(
        &self,
        client: &SurrealDbClient,
        query: impl Into<Body>,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        self.execute_sql_request(client, query, Some((&self.namespace, &self.database)))
            .await
    }

    async fn execute_sql_without_scope(
        &self,
        client: &SurrealDbClient,
        query: impl Into<Body>,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        self.execute_sql_request(client, query, None).await
    }

    async fn execute_sql_request(
        &self,
        client: &SurrealDbClient,
        query: impl Into<Body>,
        scope: Option<(&str, &str)>,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        let mut request = self
            .apply_auth(client.post(format!("{}/sql", self.base_url)))
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain")
            .body(query);

        if let Some((namespace, database)) = scope {
            request = request
                .header("Surreal-NS", namespace)
                .header("Surreal-DB", database);
        }

        let response = request
            .send()
            .await
            .map_err(SurrealDbRequestError::Request)?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(SurrealDbRequestError::Request)?;

        if !status.is_success() {
            return Err(SurrealDbRequestError::HttpStatus { status, body });
        }

        let statements: Vec<SurrealSqlStatement> = serde_json::from_str(&body).map_err(|e| {
            SurrealDbRequestError::Decode(format!(
                "Failed to decode SurrealDB SQL response: {e}; response: {body}"
            ))
        })?;

        if let Some(statement) = statements
            .iter()
            .find(|statement| !statement.status.eq_ignore_ascii_case("OK"))
        {
            return Err(SurrealDbRequestError::Query(
                statement
                    .detail
                    .clone()
                    .or_else(|| statement.result.as_ref().map(value_to_error_message))
                    .unwrap_or_else(|| format!("SurrealDB query status: {}", statement.status)),
            ));
        }

        Ok(statements)
    }

    fn apply_auth(&self, request: RequestBuilder) -> RequestBuilder {
        if self.auth_scope == AuthScope::None {
            return request;
        }

        let Some(username) = self.username.as_ref() else {
            return request;
        };
        let Some(password) = self.password.as_ref() else {
            return request;
        };

        request.basic_auth(username, Some(password.expose_secret()))
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let mut successful_inserts = 0u64;
        let mut last_error = None;
        let record_id_prefix = RecordIdPrefix::new(topic_metadata);
        let mut batch = Vec::with_capacity(self.batch_size);

        for message in messages {
            batch.push(message);
            if batch.len() == self.batch_size {
                let batch_len = batch.len();
                let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(self.batch_size));
                let outcome = self
                    .insert_batch(
                        full_batch,
                        &record_id_prefix,
                        topic_metadata,
                        messages_metadata,
                    )
                    .await;
                successful_inserts += outcome.inserted_count;

                if let Some(batch_error) = outcome.error {
                    self.insertion_errors
                        .fetch_add(outcome.error_count, Ordering::Relaxed);
                    error!(
                        "Failed to insert SurrealDB batch of {batch_len} messages for connector ID: {}, table: {}, error: {batch_error}",
                        self.id, self.table
                    );
                    last_error = Some(batch_error);
                }
            }
        }

        if !batch.is_empty() {
            let batch_len = batch.len();
            let outcome = self
                .insert_batch(batch, &record_id_prefix, topic_metadata, messages_metadata)
                .await;
            successful_inserts += outcome.inserted_count;

            if let Some(batch_error) = outcome.error {
                self.insertion_errors
                    .fetch_add(outcome.error_count, Ordering::Relaxed);
                error!(
                    "Failed to insert SurrealDB batch of {batch_len} messages for connector ID: {}, table: {}, error: {batch_error}",
                    self.id, self.table
                );
                last_error = Some(batch_error);
            }
        }

        self.messages_processed
            .fetch_add(successful_inserts, Ordering::Relaxed);

        if self.verbose {
            info!(
                "SurrealDB sink ID: {} wrote {successful_inserts} messages to table '{}'",
                self.id, self.table
            );
        } else {
            debug!(
                "SurrealDB sink ID: {} wrote {successful_inserts} messages to table '{}'",
                self.id, self.table
            );
        }

        if let Some(error) = last_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    async fn insert_batch(
        &self,
        messages: Vec<ConsumedMessage>,
        record_id_prefix: &RecordIdPrefix,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> BatchInsertOutcome {
        if messages.is_empty() {
            return BatchInsertOutcome {
                inserted_count: 0,
                error_count: 0,
                error: None,
            };
        }

        let mut records = Vec::with_capacity(messages.len());
        let mut record_error_count = 0u64;
        let mut last_record_error = None;
        for message in messages {
            match self.build_record(record_id_prefix, topic_metadata, messages_metadata, message) {
                Ok(record) => records.push(record),
                Err(error) => {
                    record_error_count += 1;
                    last_record_error = Some(error);
                }
            }
        }

        if records.is_empty() {
            return BatchInsertOutcome {
                inserted_count: 0,
                error_count: record_error_count,
                error: last_record_error,
            };
        }

        let mut outcome = self.insert_records_with_retry(records).await;
        outcome.error_count += record_error_count;
        let db_error = outcome.error.take();
        outcome.error = db_error.or(last_record_error);

        outcome
    }

    async fn insert_records_with_retry(&self, records: Vec<Value>) -> BatchInsertOutcome {
        let mut attempts = 0u32;
        let query = match build_insert_query(&self.table, &records) {
            Ok(query) => query,
            Err(error) => {
                return BatchInsertOutcome {
                    inserted_count: 0,
                    error_count: records.len() as u64,
                    error: Some(error),
                };
            }
        };
        let record_count = records.len() as u64;

        loop {
            let client = match self.get_client().await {
                Ok(client) => client,
                Err(error) => {
                    return BatchInsertOutcome {
                        inserted_count: 0,
                        error_count: record_count,
                        error: Some(error),
                    };
                }
            };
            let result = self.execute_sql(&client, query.clone()).await;

            match result {
                Ok(_) => {
                    return BatchInsertOutcome {
                        inserted_count: record_count,
                        error_count: 0,
                        error: None,
                    };
                }
                Err(error) => {
                    let transient = is_transient_error(&error);
                    attempts += 1;

                    if !transient || attempts >= self.max_retries {
                        return BatchInsertOutcome {
                            inserted_count: 0,
                            error_count: record_count,
                            error: Some(Error::CannotStoreData(format!(
                                "SurrealDB batch insert failed after {attempts} attempts: {error}"
                            ))),
                        };
                    }

                    if transient && is_connection_error(&error) {
                        match self.reconnect().await {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(reconnect_error) => {
                                return BatchInsertOutcome {
                                    inserted_count: 0,
                                    error_count: record_count,
                                    error: Some(Error::Connection(format!(
                                        "Failed to reconnect to SurrealDB after transient write error: {reconnect_error}"
                                    ))),
                                };
                            }
                        }
                    }

                    let delay = jitter(exponential_backoff(
                        self.retry_delay,
                        attempts.saturating_sub(1),
                        self.max_retry_delay,
                    ));
                    warn!(
                        "Transient SurrealDB write error for connector ID: {} (attempt {attempts}/{}): {error}. Retrying in {:?}.",
                        self.id, self.max_retries, delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    fn build_record(
        &self,
        record_id_prefix: &RecordIdPrefix,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: ConsumedMessage,
    ) -> Result<Value, Error> {
        let mut record = Map::new();
        record.insert(
            "id".to_string(),
            Value::String(build_record_id(
                record_id_prefix,
                messages_metadata,
                message.id,
                message.offset,
            )),
        );
        record.insert(
            "iggy_message_id".to_string(),
            Value::String(message.id.to_string()),
        );

        if self.include_metadata {
            record.insert(
                "iggy_stream".to_string(),
                Value::String(topic_metadata.stream.clone()),
            );
            record.insert(
                "iggy_topic".to_string(),
                Value::String(topic_metadata.topic.clone()),
            );
            record.insert(
                "iggy_partition_id".to_string(),
                Value::String(messages_metadata.partition_id.to_string()),
            );
            record.insert(
                "iggy_offset".to_string(),
                Value::String(message.offset.to_string()),
            );
            record.insert(
                "iggy_timestamp".to_string(),
                Value::String(message.timestamp.to_string()),
            );
            record.insert(
                "iggy_schema".to_string(),
                Value::String(messages_metadata.schema.to_string()),
            );
        }

        if self.include_checksum {
            record.insert(
                "iggy_checksum".to_string(),
                Value::String(message.checksum.to_string()),
            );
        }

        if self.include_origin_timestamp {
            record.insert(
                "iggy_origin_timestamp".to_string(),
                Value::String(message.origin_timestamp.to_string()),
            );
        }

        if self.include_headers
            && let Some(headers) = &message.headers
            && !headers.is_empty()
        {
            record.insert("iggy_headers".to_string(), encode_headers(headers)?);
        }

        let payload = self.build_payload_document(message.payload)?;
        record.insert("payload".to_string(), payload.value);
        record.insert(
            "payload_encoding".to_string(),
            Value::String(payload.encoding.to_string()),
        );

        Ok(Value::Object(record))
    }

    fn build_payload_document(&self, payload: Payload) -> Result<PayloadDocument, Error> {
        match self.payload_format {
            PayloadFormat::Auto => build_auto_payload_document(payload),
            PayloadFormat::Json => build_json_payload_document(payload),
            PayloadFormat::Text => build_text_payload_document(payload),
            PayloadFormat::Base64 => build_base64_payload_document(payload),
        }
    }
}

fn build_insert_query(table: &str, records: &[Value]) -> Result<Bytes, Error> {
    let mut query = Vec::with_capacity(table.len() + records.len() * 128 + 32);
    query.extend_from_slice(b"INSERT IGNORE INTO ");
    query.extend_from_slice(table.as_bytes());
    query.push(b' ');
    serde_json::to_writer(&mut query, records)
        .map_err(|e| Error::InvalidRecordValue(format!("Invalid SurrealDB records: {e}")))?;
    query.extend_from_slice(b" RETURN NONE;");
    Ok(Bytes::from(query))
}

fn build_auto_payload_document(payload: Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Json(value) => Ok(PayloadDocument {
            value: owned_value_to_serde_json(&value),
            encoding: ENCODING_JSON,
        }),
        Payload::Text(text) | Payload::Proto(text) => Ok(PayloadDocument {
            value: Value::String(text),
            encoding: ENCODING_TEXT,
        }),
        Payload::Raw(_) | Payload::FlatBuffer(_) | Payload::Avro(_) | Payload::Bson(_) => {
            build_base64_payload_document(payload)
        }
    }
}

fn build_json_payload_document(payload: Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Json(value) => Ok(PayloadDocument {
            value: owned_value_to_serde_json(&value),
            encoding: ENCODING_JSON,
        }),
        _ => {
            let bytes = payload.try_into_vec()?;
            let value = serde_json::from_slice(&bytes)
                .map_err(|e| Error::InvalidRecordValue(format!("Invalid JSON payload: {e}")))?;
            Ok(PayloadDocument {
                value,
                encoding: ENCODING_JSON,
            })
        }
    }
}

fn build_text_payload_document(payload: Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Text(text) | Payload::Proto(text) => Ok(PayloadDocument {
            value: Value::String(text),
            encoding: ENCODING_TEXT,
        }),
        _ => {
            let bytes = payload.try_into_vec()?;
            let text = String::from_utf8(bytes)
                .map_err(|e| Error::InvalidRecordValue(format!("Invalid UTF-8 payload: {e}")))?;
            Ok(PayloadDocument {
                value: Value::String(text),
                encoding: ENCODING_TEXT,
            })
        }
    }
}

fn build_base64_payload_document(payload: Payload) -> Result<PayloadDocument, Error> {
    let bytes = payload.try_into_vec()?;
    Ok(PayloadDocument {
        value: Value::String(general_purpose::STANDARD.encode(bytes)),
        encoding: ENCODING_BASE64,
    })
}

fn encode_headers(
    headers: &std::collections::BTreeMap<iggy_common::HeaderKey, iggy_common::HeaderValue>,
) -> Result<Value, Error> {
    let mut encoded = Map::new();

    for (key, value) in headers {
        let value = if let Ok(raw) = value.as_raw() {
            json!({
                "data": general_purpose::STANDARD.encode(raw),
                "iggy_header_encoding": ENCODING_BASE64
            })
        } else {
            Value::String(value.to_string_value())
        };
        encoded.insert(key.to_string_value(), value);
    }

    Ok(Value::Object(encoded))
}

#[derive(Debug)]
struct RecordIdPrefix {
    stream: String,
    topic: String,
}

impl RecordIdPrefix {
    fn new(topic_metadata: &TopicMetadata) -> Self {
        let mut stream = String::with_capacity(topic_metadata.stream.len() * 2);
        push_hex_component(&mut stream, topic_metadata.stream.as_bytes());
        let mut topic = String::with_capacity(topic_metadata.topic.len() * 2);
        push_hex_component(&mut topic, topic_metadata.topic.as_bytes());

        Self { stream, topic }
    }
}

fn build_record_id(
    record_id_prefix: &RecordIdPrefix,
    messages_metadata: &MessagesMetadata,
    message_id: u128,
    offset: u64,
) -> String {
    let mut id =
        String::with_capacity(record_id_prefix.stream.len() + record_id_prefix.topic.len() + 72);
    id.push('s');
    id.push_str(&record_id_prefix.stream);
    id.push_str("_t");
    id.push_str(&record_id_prefix.topic);
    id.push_str("_p");
    id.push_str(&messages_metadata.partition_id.to_string());
    id.push_str("_o");
    id.push_str(&offset.to_string());
    id.push_str("_m");
    let _ = write!(&mut id, "{message_id:032x}");
    id
}

fn push_hex_component(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

fn validate_identifier(field: &str, value: &str) -> Result<(), Error> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(Error::InvalidConfigValue(format!(
            "SurrealDB {field} cannot be empty"
        )));
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(Error::InvalidConfigValue(format!(
            "SurrealDB {field} must start with an ASCII letter or underscore"
        )));
    }

    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(Error::InvalidConfigValue(format!(
            "SurrealDB {field} must contain only ASCII letters, digits, and underscores"
        )));
    }

    Ok(())
}

fn validate_endpoint_config(endpoint: &str, use_tls: bool) -> Result<(), Error> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(Error::InvalidConfigValue(
            "SurrealDB endpoint cannot be empty".to_string(),
        ));
    }

    let has_scheme = endpoint.starts_with("http://") || endpoint.starts_with("https://");
    if use_tls && endpoint.starts_with("http://") {
        warn!("SurrealDB use_tls=true is ignored because endpoint has explicit http:// scheme.");
    }

    let parsed_endpoint = if has_scheme {
        endpoint.to_string()
    } else {
        let scheme = if use_tls { "https" } else { "http" };
        format!("{scheme}://{endpoint}")
    };
    let url = Url::parse(&parsed_endpoint).map_err(|e| {
        Error::InvalidConfigValue(format!("Invalid SurrealDB endpoint '{endpoint}': {e}"))
    })?;

    if url.host_str().is_none() {
        return Err(Error::InvalidConfigValue(format!(
            "Invalid SurrealDB endpoint '{endpoint}': host is required"
        )));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidConfigValue(
            "SurrealDB endpoint must not include embedded credentials; use username/password config fields instead"
                .to_string(),
        ));
    }

    if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
        return Err(Error::InvalidConfigValue(
            "SurrealDB endpoint must not include a path, query, or fragment".to_string(),
        ));
    }

    Ok(())
}

fn build_base_url(endpoint: &str, use_tls: bool) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let has_scheme = endpoint.starts_with("http://") || endpoint.starts_with("https://");
    let endpoint = if has_scheme {
        endpoint.to_string()
    } else {
        let scheme = if use_tls { "https" } else { "http" };
        format!("{scheme}://{endpoint}")
    };

    if let Ok(mut url) = Url::parse(&endpoint) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_path("");
        url.set_query(None);
        url.set_fragment(None);
        return url.as_str().trim_end_matches('/').to_string();
    }

    endpoint
}

fn value_to_error_message(value: &Value) -> String {
    value
        .as_str()
        .map(ToString::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn is_transient_error(error: &SurrealDbRequestError) -> bool {
    is_transaction_conflict(error)
        || is_connection_error(error)
        || is_timeout_or_service_error(error)
}

fn is_transaction_conflict(error: &SurrealDbRequestError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("transaction conflict") || message.contains("transaction can be retried")
}

fn is_connection_error(error: &SurrealDbRequestError) -> bool {
    let SurrealDbRequestError::Request(error) = error else {
        return false;
    };

    let message = error.to_string().to_ascii_lowercase();
    error.is_connect()
        || error.is_timeout()
        || message.contains("connection")
        || message.contains("network")
        || message.contains("broken pipe")
        || message.contains("reset by peer")
}

fn is_timeout_or_service_error(error: &SurrealDbRequestError) -> bool {
    if let SurrealDbRequestError::Request(error) = error
        && error.is_timeout()
    {
        return true;
    }
    if let SurrealDbRequestError::HttpStatus { status, .. } = error
        && matches!(
            *status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        )
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{HeaderKey, HeaderValue};
    use iggy_connector_sdk::Schema;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex as TokioMutex;

    fn test_config() -> SurrealDbSinkConfig {
        SurrealDbSinkConfig {
            endpoint: "127.0.0.1:8000".to_string(),
            namespace: "iggy".to_string(),
            database: "connectors".to_string(),
            table: "iggy_messages".to_string(),
            username: Some("root".to_string()),
            password: Some(SecretString::from("root")),
            auth_scope: None,
            use_tls: None,
            auto_define_table: None,
            define_indexes: None,
            batch_size: None,
            payload_format: None,
            include_metadata: None,
            include_headers: None,
            include_checksum: None,
            include_origin_timestamp: None,
            query_timeout: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            verbose_logging: None,
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
            timestamp: 1_700_000_000_000_000,
            origin_timestamp: 1_700_000_000_000_001,
            headers: None,
            payload,
        }
    }

    fn json_payload(value: serde_json::Value) -> Payload {
        let mut bytes = serde_json::to_vec(&value).expect("Failed to serialize JSON");
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("Failed to parse JSON"))
    }

    async fn start_surrealdb_test_server() -> (String, Arc<AtomicUsize>, Arc<TokioMutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let endpoint = listener
            .local_addr()
            .expect("test server should expose local address")
            .to_string();
        let sql_requests = Arc::new(AtomicUsize::new(0));
        let sql_body = Arc::new(TokioMutex::new(String::new()));
        let server_sql_requests = Arc::clone(&sql_requests);
        let server_sql_body = Arc::clone(&sql_body);

        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("request should connect");
                let sql_requests = Arc::clone(&server_sql_requests);
                let sql_body = Arc::clone(&server_sql_body);

                tokio::spawn(async move {
                    let (request_line, body) = read_http_request(&mut stream).await;
                    let response_body = if request_line.starts_with("POST /sql ") {
                        sql_requests.fetch_add(1, Ordering::Relaxed);
                        *sql_body.lock().await = body;
                        r#"[{"status":"OK","result":[]}]"#
                    } else {
                        r#"{"status":"OK"}"#
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );
                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("response should be written");
                });
            }
        });

        (endpoint, sql_requests, sql_body)
    }

    async fn read_http_request(stream: &mut TcpStream) -> (String, String) {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 1024];
        let mut headers_end = None;

        while headers_end.is_none() {
            let read = stream.read(&mut chunk).await.expect("request should read");
            assert_ne!(read, 0, "request should include headers");
            buffer.extend_from_slice(&chunk[..read]);
            headers_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
        }

        let headers_end = headers_end.expect("headers should terminate");
        let body_start = headers_end + 4;
        let headers = String::from_utf8_lossy(&buffer[..headers_end]).to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("valid content length"))
                })
            })
            .unwrap_or(0);

        while buffer.len() < body_start + content_length {
            let read = stream.read(&mut chunk).await.expect("body should read");
            assert_ne!(read, 0, "request should include declared body");
            buffer.extend_from_slice(&chunk[..read]);
        }

        let request_line = headers
            .lines()
            .next()
            .expect("request line should exist")
            .to_string();
        let body =
            String::from_utf8_lossy(&buffer[body_start..body_start + content_length]).to_string();

        (request_line, body)
    }

    #[test]
    fn given_default_config_should_apply_expected_runtime_values() {
        let sink = SurrealDbSink::new(1, test_config());

        assert_eq!(sink.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(sink.auth_scope, AuthScope::Root);
        assert_eq!(sink.payload_format, PayloadFormat::Auto);
        assert_eq!(sink.query_timeout, Duration::from_secs(30));
        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(sink.retry_delay, Duration::from_millis(100));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(5));
        assert!(sink.include_metadata);
        assert!(sink.include_headers);
        assert!(sink.include_checksum);
        assert!(sink.include_origin_timestamp);
        assert!(!sink.auto_define_table);
        assert!(!sink.define_indexes);
    }

    #[test]
    fn given_config_overrides_should_apply_expected_values() {
        let mut config = test_config();
        config.auth_scope = Some("database".to_string());
        config.payload_format = Some("base64".to_string());
        config.batch_size = Some(10);
        config.query_timeout = Some("5s".to_string());
        config.max_retries = Some(5);
        config.retry_delay = Some("250ms".to_string());
        config.max_retry_delay = Some("2s".to_string());
        config.include_metadata = Some(false);
        config.include_headers = Some(false);
        config.include_checksum = Some(false);
        config.include_origin_timestamp = Some(false);
        config.auto_define_table = Some(true);
        config.define_indexes = Some(true);
        config.verbose_logging = Some(true);

        let sink = SurrealDbSink::new(1, config);

        assert_eq!(sink.auth_scope_config.as_deref(), Some("database"));
        assert_eq!(sink.payload_format_config.as_deref(), Some("base64"));
        assert_eq!(sink.auth_scope, AuthScope::Root);
        assert_eq!(sink.payload_format, PayloadFormat::Auto);
        assert_eq!(sink.batch_size, 10);
        assert_eq!(sink.query_timeout, Duration::from_secs(5));
        assert_eq!(sink.max_retries, 5);
        assert_eq!(sink.retry_delay, Duration::from_millis(250));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(2));
        assert!(!sink.include_metadata);
        assert!(!sink.include_headers);
        assert!(!sink.include_checksum);
        assert!(!sink.include_origin_timestamp);
        assert!(sink.auto_define_table);
        assert!(sink.define_indexes);
        assert!(sink.verbose);
    }

    #[test]
    fn given_zero_max_retries_should_use_minimum_one_attempt() {
        let mut config = test_config();
        config.max_retries = Some(0);

        let mut sink = SurrealDbSink::new(1, config);
        sink.payload_format = PayloadFormat::Json;

        assert_eq!(sink.max_retries, 1);
    }

    #[test]
    fn given_reversed_retry_delays_should_clamp_max_retry_delay() {
        let mut config = test_config();
        config.retry_delay = Some("5s".to_string());
        config.max_retry_delay = Some("100ms".to_string());

        let mut sink = SurrealDbSink::new(1, config);
        sink.payload_format = PayloadFormat::Json;

        assert_eq!(sink.retry_delay, Duration::from_secs(5));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(5));
    }

    #[test]
    fn given_payload_format_inputs_should_map_expected_variant() {
        let cases = [
            (None, PayloadFormat::Auto),
            (Some("auto"), PayloadFormat::Auto),
            (Some("json"), PayloadFormat::Json),
            (Some("text"), PayloadFormat::Text),
            (Some("base64"), PayloadFormat::Base64),
            (Some("binary"), PayloadFormat::Base64),
        ];

        for (input, expected) in cases {
            assert_eq!(PayloadFormat::parse_config(input).unwrap(), expected);
        }

        assert!(matches!(
            PayloadFormat::parse_config(Some("unknown")),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_auth_scope_inputs_should_map_expected_variant() {
        let cases = [
            (None, AuthScope::Root),
            (Some("root"), AuthScope::Root),
            (Some("namespace"), AuthScope::Namespace),
            (Some("database"), AuthScope::Database),
            (Some("none"), AuthScope::None),
        ];

        for (input, expected) in cases {
            assert_eq!(AuthScope::parse_config(input).unwrap(), expected);
        }

        assert!(matches!(
            AuthScope::parse_config(Some("unknown")),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_unknown_auth_scope_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.auth_scope = Some("roo".to_string());
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_unknown_payload_format_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.payload_format = Some("jsn".to_string());
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_auto_define_table_with_scoped_auth_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.auth_scope = Some("database".to_string());
        config.auto_define_table = Some(true);
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_define_indexes_without_metadata_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.define_indexes = Some(true);
        config.include_metadata = Some(false);
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_endpoint_path_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.endpoint = "http://127.0.0.1:8000/extra/path".to_string();
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_invalid_namespace_when_opening_should_fail_validation() {
        let mut config = test_config();
        config.namespace = "bad-namespace".to_string();
        let mut sink = SurrealDbSink::new(1, config);

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink.open().await;

                assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
            });
    }

    #[test]
    fn given_identifier_values_should_validate_expected_shapes() {
        assert!(validate_identifier("table", "iggy_messages").is_ok());
        assert!(validate_identifier("table", "_messages9").is_ok());
        assert!(validate_identifier("table", "").is_err());
        assert!(validate_identifier("table", "9messages").is_err());
        assert!(validate_identifier("table", "messages-name").is_err());
        assert!(validate_identifier("table", "messages]").is_err());
        assert!(validate_identifier("table", "messages\"").is_err());
        assert!(validate_identifier("table", "messages; DROP TABLE x").is_err());
    }

    #[test]
    fn given_topic_metadata_should_build_deterministic_record_id() {
        let topic_metadata = test_topic_metadata();
        let record_id_prefix = RecordIdPrefix::new(&topic_metadata);
        let id = build_record_id(&record_id_prefix, &test_messages_metadata(), 42, 9);

        assert_eq!(
            id,
            "s746573745f73747265616d_t746573745f746f706963_p7_o9_m0000000000000000000000000000002a"
        );
    }

    #[test]
    fn given_table_name_should_build_bulk_insert_query() {
        let records = [json!({
            "id": "record_1",
            "payload": {"message": "hello"}
        })];

        assert_eq!(
            String::from_utf8(
                build_insert_query("iggy_messages", &records)
                    .expect("query should build")
                    .to_vec()
            )
            .expect("query should be valid UTF-8"),
            r#"INSERT IGNORE INTO iggy_messages [{"id":"record_1","payload":{"message":"hello"}}] RETURN NONE;"#
        );
    }

    #[test]
    fn given_adversarial_record_values_should_build_escaped_insert_query() {
        let records = [json!({
            "id": "record_\"[]",
            "payload": {
                "text": "quote \" bracket ] brace } semi ; newline \n"
            }
        })];
        let query = String::from_utf8(
            build_insert_query("iggy_messages", &records)
                .expect("query should build")
                .to_vec(),
        )
        .expect("query should be valid UTF-8");
        let json_start = "INSERT IGNORE INTO iggy_messages ";
        let json_end = " RETURN NONE;";
        assert!(query.starts_with(json_start));
        assert!(query.ends_with(json_end));

        let encoded_records = &query[json_start.len()..query.len() - json_end.len()];
        let decoded_records: Vec<Value> =
            serde_json::from_str(encoded_records).expect("records should stay valid JSON");
        assert_eq!(decoded_records, records.to_vec());
    }

    #[test]
    fn given_auto_payload_json_should_store_queryable_json() {
        let payload = json_payload(json!({"name": "Alice", "active": true}));
        let document = build_auto_payload_document(payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_JSON);
        assert_eq!(document.value, json!({"name": "Alice", "active": true}));
    }

    #[test]
    fn given_auto_payload_text_should_store_text() {
        let payload = Payload::Text("hello".to_string());
        let document = build_auto_payload_document(payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_TEXT);
        assert_eq!(document.value, Value::String("hello".to_string()));
    }

    #[test]
    fn given_auto_payload_raw_should_store_base64() {
        let payload = Payload::Raw(vec![0, 1, 2, 255]);
        let document = build_auto_payload_document(payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_BASE64);
        assert_eq!(document.value, Value::String("AAEC/w==".to_string()));
    }

    #[test]
    fn given_json_payload_format_should_parse_raw_json() {
        let payload = Payload::Raw(br#"{"count":3}"#.to_vec());
        let document = build_json_payload_document(payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_JSON);
        assert_eq!(document.value, json!({"count": 3}));
    }

    #[test]
    fn given_json_payload_format_when_invalid_should_fail() {
        let payload = Payload::Raw(b"not-json".to_vec());
        let result = build_json_payload_document(payload);

        assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    }

    #[test]
    fn given_text_payload_format_when_invalid_utf8_should_fail() {
        let payload = Payload::Raw(vec![0xff, 0xfe]);
        let result = build_text_payload_document(payload);

        assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    }

    #[test]
    fn given_headers_should_encode_raw_as_base64_and_values_as_strings() {
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("trace-id").expect("valid key"),
            HeaderValue::from_str("abc").expect("valid value"),
        );
        headers.insert(
            HeaderKey::try_from("binary").expect("valid key"),
            HeaderValue::try_from(vec![1_u8, 2, 3]).expect("valid raw"),
        );

        let encoded = encode_headers(&headers).expect("Failed to encode headers");

        assert_eq!(
            encoded,
            json!({
                "binary": {
                    "data": "AQID",
                    "iggy_header_encoding": "base64"
                },
                "trace-id": "abc"
            })
        );
    }

    #[test]
    fn given_message_should_build_full_record() {
        let mut message = test_message(json_payload(json!({"event": "created"})));
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("source").expect("valid key"),
            HeaderValue::from_str("unit-test").expect("valid value"),
        );
        message.headers = Some(headers);

        let sink = SurrealDbSink::new(1, test_config());
        let topic_metadata = test_topic_metadata();
        let record_id_prefix = RecordIdPrefix::new(&topic_metadata);
        let record = sink
            .build_record(
                &record_id_prefix,
                &topic_metadata,
                &test_messages_metadata(),
                message,
            )
            .expect("Failed to build record");
        let object = record.as_object().expect("record should be object");

        assert_eq!(
            object.get("id"),
            Some(&Value::String(
                "s746573745f73747265616d_t746573745f746f706963_p7_o9_m0000000000000000000000000000002a"
                    .to_string()
            ))
        );
        assert_eq!(object.get("iggy_message_id"), Some(&json!("42")));
        assert_eq!(object.get("iggy_stream"), Some(&json!("test_stream")));
        assert_eq!(object.get("iggy_topic"), Some(&json!("test_topic")));
        assert_eq!(object.get("iggy_partition_id"), Some(&json!("7")));
        assert_eq!(object.get("iggy_offset"), Some(&json!("9")));
        assert_eq!(
            object.get("iggy_timestamp"),
            Some(&json!("1700000000000000"))
        );
        assert_eq!(object.get("iggy_checksum"), Some(&json!("123")));
        assert_eq!(
            object.get("iggy_origin_timestamp"),
            Some(&json!("1700000000000001"))
        );
        assert_eq!(object.get("payload"), Some(&json!({"event": "created"})));
        assert_eq!(object.get("payload_encoding"), Some(&json!("json")));
        assert!(object.contains_key("iggy_headers"));
    }

    #[test]
    fn given_large_u64_metadata_should_build_record_with_lossless_strings() {
        let sink = SurrealDbSink::new(1, test_config());
        let mut message = test_message(Payload::Text("large-metadata".to_string()));
        message.offset = u64::MAX;
        message.timestamp = u64::MAX;
        message.checksum = u64::MAX;
        message.origin_timestamp = u64::MAX;
        let topic_metadata = test_topic_metadata();
        let record_id_prefix = RecordIdPrefix::new(&topic_metadata);

        let record = sink
            .build_record(
                &record_id_prefix,
                &topic_metadata,
                &test_messages_metadata(),
                message,
            )
            .expect("Failed to build record");
        let object = record.as_object().expect("record should be object");

        assert_eq!(
            object.get("iggy_offset"),
            Some(&json!("18446744073709551615"))
        );
        assert_eq!(
            object.get("iggy_timestamp"),
            Some(&json!("18446744073709551615"))
        );
        assert_eq!(
            object.get("iggy_checksum"),
            Some(&json!("18446744073709551615"))
        );
        assert_eq!(
            object.get("iggy_origin_timestamp"),
            Some(&json!("18446744073709551615"))
        );
    }

    #[test]
    fn given_invalid_batch_when_processing_messages_should_record_error_and_return_error() {
        let mut config = test_config();
        config.payload_format = Some("json".to_string());
        let mut sink = SurrealDbSink::new(1, config);
        sink.payload_format = PayloadFormat::Json;
        let message = test_message(Payload::Raw(b"not-json".to_vec()));

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink
                    .process_messages(
                        &test_topic_metadata(),
                        &test_messages_metadata(),
                        vec![message],
                    )
                    .await;

                assert!(
                    matches!(result, Err(Error::InvalidRecordValue(_))),
                    "batch failures should be observable by direct plugin callers"
                );
            });

        assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 0);
        assert_eq!(sink.insertion_errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn given_invalid_chunks_when_processing_messages_should_process_all_chunks() {
        let mut config = test_config();
        config.payload_format = Some("json".to_string());
        config.batch_size = Some(1);
        let mut sink = SurrealDbSink::new(1, config);
        sink.payload_format = PayloadFormat::Json;
        let messages = vec![
            test_message(Payload::Raw(b"not-json".to_vec())),
            test_message(Payload::Raw(b"also-not-json".to_vec())),
        ];

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink
                    .process_messages(&test_topic_metadata(), &test_messages_metadata(), messages)
                    .await;

                assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
            });

        assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 0);
        assert_eq!(sink.insertion_errors.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn given_malformed_record_in_batch_should_still_attempt_valid_records() {
        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let (endpoint, sql_requests, sql_body) = start_surrealdb_test_server().await;
                let mut config = test_config();
                config.endpoint = endpoint;
                config.auth_scope = Some("none".to_string());
                config.payload_format = Some("json".to_string());
                let mut sink = SurrealDbSink::new(1, config);
                sink.open().await.expect("sink should open");

                let messages = vec![
                    test_message(Payload::Raw(b"not-json".to_vec())),
                    test_message(Payload::Raw(br#"{"valid":true}"#.to_vec())),
                ];
                let result = sink
                    .process_messages(&test_topic_metadata(), &test_messages_metadata(), messages)
                    .await;

                assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
                assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 1);
                assert_eq!(sink.insertion_errors.load(Ordering::Relaxed), 1);
                assert_eq!(sql_requests.load(Ordering::Relaxed), 1);

                let body = sql_body.lock().await;
                assert!(body.contains(r#""valid":true"#));
                assert!(!body.contains("not-json"));
            });
    }

    #[test]
    fn given_endpoint_should_build_http_base_url() {
        assert_eq!(
            build_base_url("127.0.0.1:8000", false),
            "http://127.0.0.1:8000"
        );
        assert_eq!(
            build_base_url("127.0.0.1:8000", true),
            "https://127.0.0.1:8000"
        );
        assert_eq!(
            build_base_url("http://127.0.0.1:8000/", true),
            "http://127.0.0.1:8000"
        );
    }

    #[test]
    fn given_endpoint_credentials_should_build_sanitized_base_url() {
        assert_eq!(
            build_base_url("http://user:pass@127.0.0.1:8000/", false),
            "http://127.0.0.1:8000"
        );
    }

    #[test]
    fn given_endpoint_shapes_should_validate_expected_values() {
        assert!(validate_endpoint_config("127.0.0.1:8000", false).is_ok());
        assert!(validate_endpoint_config("http://127.0.0.1:8000", true).is_ok());
        assert!(validate_endpoint_config("http://user:pass@127.0.0.1:8000", false).is_err());
        assert!(validate_endpoint_config("http://127.0.0.1:8000/path", false).is_err());
        assert!(validate_endpoint_config("http://127.0.0.1:8000?x=1", false).is_err());
    }

    #[test]
    fn given_http_status_service_error_should_be_transient() {
        let error = SurrealDbRequestError::HttpStatus {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: "retry later".to_string(),
        };

        assert!(is_transient_error(&error));
        assert!(is_timeout_or_service_error(&error));
    }

    #[test]
    fn given_transaction_conflict_error_should_be_transient() {
        let error = SurrealDbRequestError::Query("Transaction conflict".to_string());

        assert!(is_transient_error(&error));
        assert!(is_transaction_conflict(&error));
    }

    #[test]
    fn given_timeout_text_in_query_error_should_not_be_transient() {
        let error = SurrealDbRequestError::Query("Query timed out".to_string());

        assert!(!is_transient_error(&error));
        assert!(!is_timeout_or_service_error(&error));
    }

    #[test]
    fn given_transient_text_in_bad_request_body_should_not_be_transient() {
        let error = SurrealDbRequestError::HttpStatus {
            status: StatusCode::BAD_REQUEST,
            body: "service unavailable timeout".to_string(),
        };

        assert!(!is_transient_error(&error));
        assert!(!is_timeout_or_service_error(&error));
    }

    #[test]
    fn given_connection_text_in_query_error_should_not_be_connection_error() {
        let error = SurrealDbRequestError::Query("connection pool size exceeded".to_string());

        assert!(!is_connection_error(&error));
    }

    #[test]
    fn given_non_transient_query_error_should_not_be_transient() {
        let error = SurrealDbRequestError::Query("syntax error".to_string());

        assert!(!is_transient_error(&error));
    }

    #[test]
    fn given_metadata_disabled_should_build_minimal_record() {
        let mut config = test_config();
        config.include_metadata = Some(false);
        config.include_headers = Some(false);
        config.include_checksum = Some(false);
        config.include_origin_timestamp = Some(false);
        let sink = SurrealDbSink::new(1, config);
        let message = test_message(Payload::Text("minimal".to_string()));
        let topic_metadata = test_topic_metadata();
        let record_id_prefix = RecordIdPrefix::new(&topic_metadata);

        let record = sink
            .build_record(
                &record_id_prefix,
                &topic_metadata,
                &test_messages_metadata(),
                message,
            )
            .expect("Failed to build record");
        let object = record.as_object().expect("record should be object");

        assert!(object.contains_key("id"));
        assert!(object.contains_key("iggy_message_id"));
        assert!(object.contains_key("payload"));
        assert!(!object.contains_key("iggy_stream"));
        assert!(!object.contains_key("iggy_checksum"));
        assert!(!object.contains_key("iggy_origin_timestamp"));
        assert!(!object.contains_key("iggy_headers"));
    }
}
