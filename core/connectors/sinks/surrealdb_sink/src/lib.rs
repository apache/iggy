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
use iggy_connector_sdk::convert::owned_value_to_serde_json;
use iggy_connector_sdk::retry::{exponential_backoff, jitter, parse_duration};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Client as HttpClient, RequestBuilder, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::fmt;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
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
    base_url: String,
    config: SurrealDbSinkConfig,
    table: String,
    auth_scope: AuthScope,
    payload_format: PayloadFormat,
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
    fn from_config(value: Option<&str>) -> Self {
        match value {
            Some(value) if value.eq_ignore_ascii_case("namespace") => AuthScope::Namespace,
            Some(value) if value.eq_ignore_ascii_case("database") => AuthScope::Database,
            Some(value) if value.eq_ignore_ascii_case("none") => AuthScope::None,
            Some(value) if value.eq_ignore_ascii_case("root") => AuthScope::Root,
            Some(value) => {
                warn!("Unknown SurrealDB auth scope '{value}', defaulting to root");
                AuthScope::Root
            }
            None => AuthScope::Root,
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
    fn from_config(value: Option<&str>) -> Self {
        match value {
            Some(value) if value.eq_ignore_ascii_case("json") => PayloadFormat::Json,
            Some(value) if value.eq_ignore_ascii_case("text") => PayloadFormat::Text,
            Some(value) if value.eq_ignore_ascii_case("base64") => PayloadFormat::Base64,
            Some(value) if value.eq_ignore_ascii_case("binary") => PayloadFormat::Base64,
            Some(value) if value.eq_ignore_ascii_case("auto") => PayloadFormat::Auto,
            Some(value) => {
                warn!("Unknown SurrealDB payload format '{value}', defaulting to auto");
                PayloadFormat::Auto
            }
            None => PayloadFormat::Auto,
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
        let table = config.table.clone();
        let base_url = build_base_url(&config.endpoint, config.use_tls.unwrap_or(false));
        let auth_scope = AuthScope::from_config(config.auth_scope.as_deref());
        let payload_format = PayloadFormat::from_config(config.payload_format.as_deref());
        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE as u32)
            .max(1) as usize;
        let query_timeout = parse_duration(config.query_timeout.as_deref(), DEFAULT_QUERY_TIMEOUT);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);

        SurrealDbSink {
            id,
            client: Mutex::new(None),
            base_url,
            config,
            table,
            auth_scope,
            payload_format,
            batch_size,
            query_timeout,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_delay,
            max_retry_delay,
            include_metadata: true,
            include_headers: true,
            include_checksum: true,
            include_origin_timestamp: true,
            auto_define_table: false,
            define_indexes: false,
            verbose: false,
            messages_processed: AtomicU64::new(0),
            insertion_errors: AtomicU64::new(0),
        }
        .with_config_defaults()
    }

    fn with_config_defaults(mut self) -> Self {
        self.max_retries = match self.config.max_retries {
            Some(0) => {
                warn!(
                    "SurrealDB sink ID: {} max_retries must be at least 1. Using 1 attempt.",
                    self.id
                );
                1
            }
            Some(max_retries) => max_retries,
            None => DEFAULT_MAX_RETRIES,
        };
        self.include_metadata = self.config.include_metadata.unwrap_or(true);
        self.include_headers = self.config.include_headers.unwrap_or(true);
        self.include_checksum = self.config.include_checksum.unwrap_or(true);
        self.include_origin_timestamp = self.config.include_origin_timestamp.unwrap_or(true);
        self.auto_define_table = self.config.auto_define_table.unwrap_or(false);
        self.define_indexes = self.config.define_indexes.unwrap_or(false);
        self.verbose = self.config.verbose_logging.unwrap_or(false);

        if self.max_retry_delay < self.retry_delay {
            warn!(
                "SurrealDB sink ID: {} max_retry_delay is smaller than retry_delay. Using retry_delay as max_retry_delay.",
                self.id
            );
            self.max_retry_delay = self.retry_delay;
        }

        self
    }
}

#[async_trait]
impl Sink for SurrealDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        validate_identifier("table", &self.table)?;

        info!(
            "Opening SurrealDB sink connector with ID: {}. Endpoint: {}, namespace: {}, database: {}, table: {}",
            self.id,
            redact_endpoint(&self.config.endpoint),
            self.config.namespace,
            self.config.database,
            self.table
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
        self.process_messages(topic_metadata, &messages_metadata, &messages)
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
        let client = self.connect().await?;
        self.signin_if_configured(&client).await?;
        self.health_check(&client).await?;

        if self.auto_define_table {
            self.ensure_namespace_database(&client).await?;
            self.ensure_table(&client).await?;
        }

        Ok(client)
    }

    async fn connect(&self) -> Result<SurrealDbClient, Error> {
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

    async fn reconnect(&self) -> Result<(), Error> {
        warn!("Reconnecting SurrealDB sink connector ID: {}", self.id);
        let client = self.connect_and_select().await?;
        *self.client.lock().await = Some(client);
        Ok(())
    }

    async fn signin_if_configured(&self, client: &SurrealDbClient) -> Result<(), Error> {
        if self.auth_scope == AuthScope::None {
            return Ok(());
        }

        let username = self.config.username.as_ref().ok_or_else(|| {
            Error::InitError(
                "SurrealDB username is required when auth_scope is not none".to_string(),
            )
        })?;
        let password = self.config.password.as_ref().ok_or_else(|| {
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
            payload.insert(
                "ns".to_string(),
                Value::String(self.config.namespace.clone()),
            );
        }
        if matches!(self.auth_scope, AuthScope::Database) {
            payload.insert(
                "db".to_string(),
                Value::String(self.config.database.clone()),
            );
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

        self.execute_sql(client, &query)
            .await
            .map_err(|e| Error::InitError(format!("Failed to define SurrealDB table: {e}")))?;

        Ok(())
    }

    async fn ensure_namespace_database(&self, client: &SurrealDbClient) -> Result<(), Error> {
        validate_identifier("namespace", &self.config.namespace)?;
        validate_identifier("database", &self.config.database)?;

        let query = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {}; USE NS {}; DEFINE DATABASE IF NOT EXISTS {};",
            self.config.namespace, self.config.namespace, self.config.database
        );

        self.execute_sql_without_scope(client, &query)
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
        query: &str,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        self.execute_sql_request(
            client,
            query,
            Some((&self.config.namespace, &self.config.database)),
        )
        .await
    }

    async fn execute_sql_without_scope(
        &self,
        client: &SurrealDbClient,
        query: &str,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        self.execute_sql_request(client, query, None).await
    }

    async fn execute_sql_request(
        &self,
        client: &SurrealDbClient,
        query: &str,
        scope: Option<(&str, &str)>,
    ) -> Result<Vec<SurrealSqlStatement>, SurrealDbRequestError> {
        let mut request = self
            .apply_auth(client.post(format!("{}/sql", self.base_url)))
            .header("Accept", "application/json")
            .header("Content-Type", "text/plain")
            .body(query.to_string());

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

        let Some(username) = self.config.username.as_ref() else {
            return request;
        };
        let Some(password) = self.config.password.as_ref() else {
            return request;
        };

        request.basic_auth(username, Some(password.expose_secret()))
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let mut successful_inserts = 0u64;
        let mut last_error = None;
        let record_id_prefix = RecordIdPrefix::new(topic_metadata);

        for batch in messages.chunks(self.batch_size) {
            let outcome = self
                .insert_batch(batch, &record_id_prefix, topic_metadata, messages_metadata)
                .await;
            successful_inserts += outcome.inserted_count;

            if let Some(error) = outcome.error {
                self.insertion_errors
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                error!(
                    "Failed to insert SurrealDB batch for connector ID: {}, table: {}, error: {error}",
                    self.id, self.table
                );
                last_error = Some(error);
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
            return Err(error);
        }

        Ok(())
    }

    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        record_id_prefix: &RecordIdPrefix,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> BatchInsertOutcome {
        if messages.is_empty() {
            return BatchInsertOutcome {
                inserted_count: 0,
                error: None,
            };
        }

        let mut records = Vec::with_capacity(messages.len());
        for message in messages {
            match self.build_record(record_id_prefix, topic_metadata, messages_metadata, message) {
                Ok(record) => records.push(record),
                Err(error) => {
                    return BatchInsertOutcome {
                        inserted_count: 0,
                        error: Some(error),
                    };
                }
            }
        }

        self.insert_records_with_retry(records).await
    }

    async fn insert_records_with_retry(&self, records: Vec<Value>) -> BatchInsertOutcome {
        let mut attempts = 0u32;
        let query = match build_insert_query(&self.table, &records) {
            Ok(query) => query,
            Err(error) => {
                return BatchInsertOutcome {
                    inserted_count: 0,
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
                        error: Some(error),
                    };
                }
            };
            let result = self.execute_sql(&client, &query).await;

            match result {
                Ok(_) => {
                    return BatchInsertOutcome {
                        inserted_count: record_count,
                        error: None,
                    };
                }
                Err(error) => {
                    attempts += 1;
                    let transient = is_transient_error(&error);
                    if !transient || attempts >= self.max_retries {
                        return BatchInsertOutcome {
                            inserted_count: 0,
                            error: Some(Error::CannotStoreData(format!(
                                "SurrealDB batch insert failed after {attempts} attempts: {error}"
                            ))),
                        };
                    }
                    if is_connection_error(&error)
                        && let Err(reconnect_error) = self.reconnect().await
                    {
                        return BatchInsertOutcome {
                            inserted_count: 0,
                            error: Some(Error::Connection(format!(
                                "Failed to reconnect to SurrealDB after transient write error: {reconnect_error}"
                            ))),
                        };
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
        message: &ConsumedMessage,
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
                Value::Number(messages_metadata.partition_id.into()),
            );
            record.insert(
                "iggy_offset".to_string(),
                Value::Number(message.offset.into()),
            );
            record.insert(
                "iggy_timestamp".to_string(),
                Value::Number(message.timestamp.into()),
            );
            record.insert(
                "iggy_schema".to_string(),
                Value::String(messages_metadata.schema.to_string()),
            );
        }

        if self.include_checksum {
            record.insert(
                "iggy_checksum".to_string(),
                Value::Number(message.checksum.into()),
            );
        }

        if self.include_origin_timestamp {
            record.insert(
                "iggy_origin_timestamp".to_string(),
                Value::Number(message.origin_timestamp.into()),
            );
        }

        if self.include_headers
            && let Some(headers) = &message.headers
            && !headers.is_empty()
        {
            record.insert("iggy_headers".to_string(), encode_headers(headers)?);
        }

        let payload = self.build_payload_document(&message.payload)?;
        record.insert("payload".to_string(), payload.value);
        record.insert(
            "payload_encoding".to_string(),
            Value::String(payload.encoding.to_string()),
        );

        Ok(Value::Object(record))
    }

    fn build_payload_document(&self, payload: &Payload) -> Result<PayloadDocument, Error> {
        match self.payload_format {
            PayloadFormat::Auto => build_auto_payload_document(payload),
            PayloadFormat::Json => build_json_payload_document(payload),
            PayloadFormat::Text => build_text_payload_document(payload),
            PayloadFormat::Base64 => build_base64_payload_document(payload),
        }
    }
}

fn build_insert_query(table: &str, records: &[Value]) -> Result<String, Error> {
    let records = serde_json::to_string(records)
        .map_err(|e| Error::InvalidRecordValue(format!("Invalid SurrealDB records: {e}")))?;
    Ok(format!("INSERT IGNORE INTO {table} {records} RETURN NONE;"))
}

fn build_auto_payload_document(payload: &Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Json(value) => Ok(PayloadDocument {
            value: owned_value_to_serde_json(value),
            encoding: ENCODING_JSON,
        }),
        Payload::Text(text) | Payload::Proto(text) => Ok(PayloadDocument {
            value: Value::String(text.clone()),
            encoding: ENCODING_TEXT,
        }),
        Payload::Raw(_) | Payload::FlatBuffer(_) | Payload::Avro(_) => {
            build_base64_payload_document(payload)
        }
    }
}

fn build_json_payload_document(payload: &Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Json(value) => Ok(PayloadDocument {
            value: owned_value_to_serde_json(value),
            encoding: ENCODING_JSON,
        }),
        _ => {
            let bytes = payload.try_to_bytes()?;
            let value = serde_json::from_slice(&bytes)
                .map_err(|e| Error::InvalidRecordValue(format!("Invalid JSON payload: {e}")))?;
            Ok(PayloadDocument {
                value,
                encoding: ENCODING_JSON,
            })
        }
    }
}

fn build_text_payload_document(payload: &Payload) -> Result<PayloadDocument, Error> {
    match payload {
        Payload::Text(text) | Payload::Proto(text) => Ok(PayloadDocument {
            value: Value::String(text.clone()),
            encoding: ENCODING_TEXT,
        }),
        _ => {
            let bytes = payload.try_to_bytes()?;
            let text = String::from_utf8(bytes)
                .map_err(|e| Error::InvalidRecordValue(format!("Invalid UTF-8 payload: {e}")))?;
            Ok(PayloadDocument {
                value: Value::String(text),
                encoding: ENCODING_TEXT,
            })
        }
    }
}

fn build_base64_payload_document(payload: &Payload) -> Result<PayloadDocument, Error> {
    let bytes = payload.try_to_bytes()?;
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
        String::with_capacity(record_id_prefix.stream.len() + record_id_prefix.topic.len() + 70);
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

fn build_base_url(endpoint: &str, use_tls: bool) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return endpoint.to_string();
    }

    let scheme = if use_tls { "https" } else { "http" };
    format!("{scheme}://{endpoint}")
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
    if let SurrealDbRequestError::Request(error) = error
        && (error.is_connect() || error.is_timeout())
    {
        return true;
    }

    let message = error.to_string().to_ascii_lowercase();
    message.contains("connection")
        || message.contains("network")
        || message.contains("websocket")
        || message.contains("channel")
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

    let message = error.to_string().to_ascii_lowercase();
    message.contains("timeout")
        || message.contains("timed out")
        || message.contains("temporarily unavailable")
        || message.contains("service unavailable")
}

fn redact_endpoint(endpoint: &str) -> String {
    endpoint.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{HeaderKey, HeaderValue};
    use iggy_connector_sdk::Schema;
    use std::collections::BTreeMap;
    use std::str::FromStr;

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

        assert_eq!(sink.auth_scope, AuthScope::Database);
        assert_eq!(sink.payload_format, PayloadFormat::Base64);
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

        let sink = SurrealDbSink::new(1, config);

        assert_eq!(sink.max_retries, 1);
    }

    #[test]
    fn given_reversed_retry_delays_should_clamp_max_retry_delay() {
        let mut config = test_config();
        config.retry_delay = Some("5s".to_string());
        config.max_retry_delay = Some("100ms".to_string());

        let sink = SurrealDbSink::new(1, config);

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
            (Some("unknown"), PayloadFormat::Auto),
        ];

        for (input, expected) in cases {
            assert_eq!(PayloadFormat::from_config(input), expected);
        }
    }

    #[test]
    fn given_auth_scope_inputs_should_map_expected_variant() {
        let cases = [
            (None, AuthScope::Root),
            (Some("root"), AuthScope::Root),
            (Some("namespace"), AuthScope::Namespace),
            (Some("database"), AuthScope::Database),
            (Some("none"), AuthScope::None),
            (Some("unknown"), AuthScope::Root),
        ];

        for (input, expected) in cases {
            assert_eq!(AuthScope::from_config(input), expected);
        }
    }

    #[test]
    fn given_identifier_values_should_validate_expected_shapes() {
        assert!(validate_identifier("table", "iggy_messages").is_ok());
        assert!(validate_identifier("table", "_messages9").is_ok());
        assert!(validate_identifier("table", "").is_err());
        assert!(validate_identifier("table", "9messages").is_err());
        assert!(validate_identifier("table", "messages-name").is_err());
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
            build_insert_query("iggy_messages", &records).expect("query should build"),
            r#"INSERT IGNORE INTO iggy_messages [{"id":"record_1","payload":{"message":"hello"}}] RETURN NONE;"#
        );
    }

    #[test]
    fn given_auto_payload_json_should_store_queryable_json() {
        let payload = json_payload(json!({"name": "Alice", "active": true}));
        let document = build_auto_payload_document(&payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_JSON);
        assert_eq!(document.value, json!({"name": "Alice", "active": true}));
    }

    #[test]
    fn given_auto_payload_text_should_store_text() {
        let payload = Payload::Text("hello".to_string());
        let document = build_auto_payload_document(&payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_TEXT);
        assert_eq!(document.value, Value::String("hello".to_string()));
    }

    #[test]
    fn given_auto_payload_raw_should_store_base64() {
        let payload = Payload::Raw(vec![0, 1, 2, 255]);
        let document = build_auto_payload_document(&payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_BASE64);
        assert_eq!(document.value, Value::String("AAEC/w==".to_string()));
    }

    #[test]
    fn given_json_payload_format_should_parse_raw_json() {
        let payload = Payload::Raw(br#"{"count":3}"#.to_vec());
        let document = build_json_payload_document(&payload).expect("Failed to build payload");

        assert_eq!(document.encoding, ENCODING_JSON);
        assert_eq!(document.value, json!({"count": 3}));
    }

    #[test]
    fn given_json_payload_format_when_invalid_should_fail() {
        let payload = Payload::Raw(b"not-json".to_vec());
        let result = build_json_payload_document(&payload);

        assert!(matches!(result, Err(Error::InvalidRecordValue(_))));
    }

    #[test]
    fn given_text_payload_format_when_invalid_utf8_should_fail() {
        let payload = Payload::Raw(vec![0xff, 0xfe]);
        let result = build_text_payload_document(&payload);

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
                &message,
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
        assert_eq!(object.get("iggy_partition_id"), Some(&json!(7)));
        assert_eq!(object.get("iggy_offset"), Some(&json!(9)));
        assert_eq!(object.get("iggy_checksum"), Some(&json!(123)));
        assert_eq!(object.get("payload"), Some(&json!({"event": "created"})));
        assert_eq!(object.get("payload_encoding"), Some(&json!("json")));
        assert!(object.contains_key("iggy_headers"));
    }

    #[test]
    fn given_invalid_batch_when_processing_messages_should_record_error_and_return_error() {
        let mut config = test_config();
        config.payload_format = Some("json".to_string());
        let sink = SurrealDbSink::new(1, config);
        let message = test_message(Payload::Raw(b"not-json".to_vec()));

        tokio::runtime::Runtime::new()
            .expect("runtime should start")
            .block_on(async {
                let result = sink
                    .process_messages(
                        &test_topic_metadata(),
                        &test_messages_metadata(),
                        &[message],
                    )
                    .await;

                assert!(
                    matches!(result, Err(Error::InvalidRecordValue(_))),
                    "batch failures should be returned to the runtime"
                );
            });

        assert_eq!(sink.messages_processed.load(Ordering::Relaxed), 0);
        assert_eq!(sink.insertion_errors.load(Ordering::Relaxed), 1);
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
    fn given_timeout_error_should_be_transient() {
        let error = SurrealDbRequestError::Query("Query timed out".to_string());

        assert!(is_transient_error(&error));
        assert!(is_timeout_or_service_error(&error));
    }

    #[test]
    fn given_non_transient_query_error_should_not_be_transient() {
        let error = SurrealDbRequestError::Query("syntax error".to_string());

        assert!(!is_transient_error(&error));
    }

    #[test]
    fn given_minimal_endpoint_should_log_unchanged_host_port() {
        assert_eq!(redact_endpoint("127.0.0.1:8000"), "127.0.0.1:8000");
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
                &message,
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
