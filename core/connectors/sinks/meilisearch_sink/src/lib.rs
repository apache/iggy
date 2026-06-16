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
use base64::{Engine as _, engine::general_purpose};
use iggy_common::IggyTimestamp;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json,
    retry::{exponential_backoff, jitter, parse_duration},
    sink_connector,
};
use meilisearch_sdk::{
    client::Client,
    errors::{
        Error as MeilisearchSdkError, ErrorCode as MeilisearchErrorCode,
        ErrorType as MeilisearchErrorType,
    },
    indexes::Index,
    task_info::TaskInfo,
    tasks::Task,
};
use reqwest::Url;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{cmp, future::Future, time::Duration};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep},
};
use tracing::{debug, info, warn};

sink_connector!(MeilisearchSink);

const DEFAULT_PRIMARY_KEY: &str = "iggy_id";
const DEFAULT_CREATE_INDEX_IF_NOT_EXISTS: bool = true;
const DEFAULT_INCLUDE_METADATA: bool = true;
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_WAIT_FOR_TASKS: bool = true;
const DEFAULT_TASK_TIMEOUT: &str = "30s";
const DEFAULT_TASK_POLL_INTERVAL: &str = "100ms";
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
const ENCODING_BASE64: &str = "base64";

#[derive(Debug)]
struct State {
    invocations_count: usize,
    documents_enqueued: usize,
    documents_indexed: usize,
    errors_count: usize,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MeilisearchDocumentAction {
    #[default]
    Replace,
    Update,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchSinkConfig {
    pub url: String,
    pub index: String,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub api_key: Option<SecretString>,
    pub primary_key: Option<String>,
    pub document_action: Option<MeilisearchDocumentAction>,
    pub create_index_if_not_exists: Option<bool>,
    pub include_metadata: Option<bool>,
    pub batch_size: Option<usize>,
    pub timeout: Option<String>,
    pub wait_for_tasks: Option<bool>,
    pub task_timeout: Option<String>,
    pub task_poll_interval: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_open_retries: Option<u32>,
}

#[derive(Debug)]
pub struct MeilisearchSink {
    id: u32,
    config: ResolvedMeilisearchSinkConfig,
    client: Option<Client>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct ResolvedMeilisearchSinkConfig {
    url: String,
    index: String,
    api_key: Option<SecretString>,
    primary_key: String,
    document_action: MeilisearchDocumentAction,
    create_index_if_not_exists: bool,
    include_metadata: bool,
    batch_size: usize,
    timeout: Duration,
    wait_for_tasks: bool,
    task_timeout: Duration,
    task_poll_interval: Duration,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_open_retries: u32,
}

impl From<MeilisearchSinkConfig> for ResolvedMeilisearchSinkConfig {
    fn from(config: MeilisearchSinkConfig) -> Self {
        let primary_key = config
            .primary_key
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_PRIMARY_KEY.to_string());
        let document_action = config.document_action.unwrap_or_default();
        let create_index_if_not_exists = config
            .create_index_if_not_exists
            .unwrap_or(DEFAULT_CREATE_INDEX_IF_NOT_EXISTS);
        let include_metadata = config.include_metadata.unwrap_or(DEFAULT_INCLUDE_METADATA);
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let wait_for_tasks = config.wait_for_tasks.unwrap_or(DEFAULT_WAIT_FOR_TASKS);
        let task_timeout = parse_duration(config.task_timeout.as_deref(), DEFAULT_TASK_TIMEOUT);
        let task_poll_interval = parse_duration(
            config.task_poll_interval.as_deref(),
            DEFAULT_TASK_POLL_INTERVAL,
        );
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES).max(1);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let max_open_retries = config
            .max_open_retries
            .unwrap_or(DEFAULT_MAX_OPEN_RETRIES)
            .max(1);

        Self {
            url: config.url,
            index: config.index,
            api_key: config.api_key,
            primary_key,
            document_action,
            create_index_if_not_exists,
            include_metadata,
            batch_size,
            timeout,
            wait_for_tasks,
            task_timeout,
            task_poll_interval,
            max_retries,
            retry_delay,
            max_retry_delay,
            max_open_retries,
        }
    }
}

impl MeilisearchSink {
    pub fn new(id: u32, config: MeilisearchSinkConfig) -> Self {
        Self {
            id,
            config: config.into(),
            client: None,
            state: Mutex::new(State {
                invocations_count: 0,
                documents_enqueued: 0,
                documents_indexed: 0,
                errors_count: 0,
            }),
        }
    }

    fn create_client(&self) -> Result<Client, Error> {
        let url = normalize_host(&self.config.url)?;
        let api_key = self.config.api_key.as_ref().map(|key| key.expose_secret());
        Client::new(url, api_key).map_err(|error| {
            Error::Connection(format!("Failed to create Meilisearch client: {error}"))
        })
    }

    async fn check_connectivity(&self, client: &Client) -> Result<(), Error> {
        let health = self
            .retry_sdk_open_operation("health check", || client.health())
            .await?;
        if health.status == "available" {
            return Ok(());
        }

        Err(Error::Connection(format!(
            "Meilisearch health check returned status '{}'",
            health.status
        )))
    }

    async fn ensure_index_exists(&self, client: &Client) -> Result<(), Error> {
        match self.get_index_if_exists(client).await? {
            Some(_) => {
                info!("Meilisearch index '{}' already exists", self.config.index);
                Ok(())
            }
            None if self.config.create_index_if_not_exists => self.create_index(client).await,
            None => Err(Error::InitError(format!(
                "Meilisearch index '{}' does not exist and create_index_if_not_exists=false",
                self.config.index
            ))),
        }
    }

    async fn get_index_if_exists(&self, client: &Client) -> Result<Option<Index>, Error> {
        self.retry_sdk_open_operation("get index", || async {
            match client.get_index(&self.config.index).await {
                Ok(index) => Ok(Some(index)),
                Err(error) if is_index_not_found(&error) => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await
    }

    async fn create_index(&self, client: &Client) -> Result<(), Error> {
        info!(
            "Creating Meilisearch index '{}' with primary key '{}'",
            self.config.index, self.config.primary_key
        );

        let task = self
            .retry_sdk_open_operation("create index", || {
                client.create_index(&self.config.index, Some(&self.config.primary_key))
            })
            .await?;
        self.wait_for_index_creation_task(client, task).await?;

        info!("Created Meilisearch index '{}'", self.config.index);
        Ok(())
    }

    fn prepare_document(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: ConsumedMessage,
    ) -> Result<Option<Value>, Error> {
        let generated_id = generated_document_id(topic_metadata, messages_metadata, &message);
        let ConsumedMessage {
            id,
            offset,
            checksum,
            timestamp,
            origin_timestamp,
            headers,
            payload,
        } = message;

        let mut document = match payload {
            Payload::Json(value) => {
                Self::document_from_json_value(owned_value_to_serde_json(&value))
            }
            Payload::Raw(bytes) => {
                let mut bytes_copy = bytes.clone();
                match simd_json::from_slice::<simd_json::OwnedValue>(&mut bytes_copy) {
                    Ok(value) => Self::document_from_json_value(owned_value_to_serde_json(&value)),
                    Err(_) => Map::from_iter([
                        (
                            "data".to_string(),
                            Value::String(general_purpose::STANDARD.encode(&bytes)),
                        ),
                        ("data_type".to_string(), Value::String("raw".to_string())),
                        (
                            "data_encoding".to_string(),
                            Value::String(ENCODING_BASE64.to_string()),
                        ),
                    ]),
                }
            }
            Payload::Text(text) => Map::from_iter([
                ("text".to_string(), Value::String(text)),
                ("data_type".to_string(), Value::String("text".to_string())),
            ]),
            _ => {
                return Err(Error::InvalidRecordValue(format!(
                    "Unsupported payload format for Meilisearch sink: {}",
                    messages_metadata.schema
                )));
            }
        };

        document
            .entry(self.config.primary_key.clone())
            .or_insert_with(|| Value::String(generated_id.clone()));

        if self.config.include_metadata {
            document
                .entry(DEFAULT_PRIMARY_KEY.to_string())
                .or_insert_with(|| Value::String(generated_id));
            insert_metadata_field(
                &mut document,
                "iggy_message_id",
                Value::String(id.to_string()),
            );
            insert_metadata_field(&mut document, "iggy_offset", Value::from(offset));
            insert_metadata_field(
                &mut document,
                "iggy_stream",
                Value::from(topic_metadata.stream.as_str()),
            );
            insert_metadata_field(
                &mut document,
                "iggy_topic",
                Value::from(topic_metadata.topic.as_str()),
            );
            insert_metadata_field(
                &mut document,
                "iggy_partition",
                Value::from(messages_metadata.partition_id),
            );
            insert_metadata_field(&mut document, "iggy_checksum", Value::from(checksum));
            insert_metadata_field(&mut document, "iggy_timestamp", Value::from(timestamp));
            insert_metadata_field(
                &mut document,
                "iggy_origin_timestamp",
                Value::from(origin_timestamp),
            );
            insert_metadata_field(
                &mut document,
                "iggy_ingested_at",
                Value::from(IggyTimestamp::now().as_millis() as i64),
            );
            if let Some(headers) = &headers
                && let Ok(headers_value) = serde_json::to_value(headers)
            {
                insert_metadata_field(&mut document, "iggy_headers", headers_value);
            }
        }

        Ok(Some(Value::Object(document)))
    }

    fn document_from_json_value(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(object) => object,
            other => {
                let mut object = Map::new();
                object.insert("value".to_string(), other);
                object
            }
        }
    }

    async fn index_documents(
        &self,
        client: &Client,
        documents: Vec<Value>,
    ) -> Result<usize, PartialIndexError> {
        let mut accepted = 0usize;
        for chunk in documents.chunks(self.config.batch_size) {
            match self.index_document_chunk(client, chunk).await {
                Ok(indexed) => accepted += indexed,
                Err(partial_error) => {
                    return Err(PartialIndexError {
                        accepted: accepted + partial_error.accepted,
                        error: partial_error.error,
                    });
                }
            }
        }
        Ok(accepted)
    }

    async fn index_document_chunk(
        &self,
        client: &Client,
        documents: &[Value],
    ) -> Result<usize, PartialIndexError> {
        if documents.is_empty() {
            return Ok(0);
        }

        let index = client.index(&self.config.index);
        let task = match self.config.document_action {
            MeilisearchDocumentAction::Replace => {
                self.retry_sdk_operation("add or replace documents", || {
                    index.add_or_replace(documents, Some(&self.config.primary_key))
                })
                .await
            }
            MeilisearchDocumentAction::Update => {
                self.retry_sdk_operation("add or update documents", || {
                    index.add_or_update(documents, Some(&self.config.primary_key))
                })
                .await
            }
        }
        .map_err(|error| PartialIndexError { accepted: 0, error })?;
        self.wait_for_task(client, task)
            .await
            .map_err(|error| PartialIndexError {
                accepted: documents.len(),
                error,
            })?;
        Ok(documents.len())
    }

    async fn wait_for_task(&self, client: &Client, task: TaskInfo) -> Result<(), Error> {
        if !self.config.wait_for_tasks {
            return Ok(());
        }

        self.wait_for_task_completion(client, task).await
    }

    async fn wait_for_index_creation_task(
        &self,
        client: &Client,
        task: TaskInfo,
    ) -> Result<(), Error> {
        let task = self
            .wait_for_task_status(client, task, self.config.max_open_retries)
            .await?;

        if task.is_success() {
            return Ok(());
        }

        if task.is_failure() {
            let failure = task.unwrap_failure();
            if failure.error_code == MeilisearchErrorCode::IndexAlreadyExists {
                return Ok(());
            }
            return Err(Error::PermanentHttpError(format!(
                "Meilisearch task failed: {}",
                failure
            )));
        }

        Err(Error::HttpRequestFailed(
            "Meilisearch task did not reach a terminal state".to_string(),
        ))
    }

    async fn wait_for_task_completion(&self, client: &Client, task: TaskInfo) -> Result<(), Error> {
        let task = self
            .wait_for_task_status(client, task, self.config.max_retries)
            .await?;

        if task.is_success() {
            return Ok(());
        }

        if task.is_failure() {
            let failure = task.unwrap_failure();
            return Err(Error::PermanentHttpError(format!(
                "Meilisearch task failed: {}",
                failure
            )));
        }

        Err(Error::HttpRequestFailed(
            "Meilisearch task did not reach a terminal state".to_string(),
        ))
    }

    async fn wait_for_task_status(
        &self,
        client: &Client,
        task: TaskInfo,
        max_attempts: u32,
    ) -> Result<Task, Error> {
        let task_uid = task.get_task_uid();
        let started = Instant::now();

        loop {
            if started.elapsed() >= self.config.task_timeout {
                break;
            }
            let status = self
                .retry_sdk_operation_with_attempts("get task status", max_attempts, || {
                    client.get_task(task.clone())
                })
                .await?;

            if status.is_success() || status.is_failure() {
                return Ok(status);
            }

            let remaining = self.config.task_timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            sleep(cmp::min(self.config.task_poll_interval, remaining)).await;
        }

        Err(Error::HttpRequestFailed(format!(
            "Meilisearch task {task_uid} timed out after {:?}",
            self.config.task_timeout
        )))
    }

    async fn retry_sdk_operation<T, Fut, Op>(
        &self,
        operation: &str,
        operation_fn: Op,
    ) -> Result<T, Error>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MeilisearchSdkError>>,
    {
        self.retry_sdk_operation_with_attempts(operation, self.config.max_retries, operation_fn)
            .await
    }

    async fn retry_sdk_open_operation<T, Fut, Op>(
        &self,
        operation: &str,
        operation_fn: Op,
    ) -> Result<T, Error>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MeilisearchSdkError>>,
    {
        self.retry_sdk_operation_with_attempts(
            operation,
            self.config.max_open_retries,
            operation_fn,
        )
        .await
    }

    async fn retry_sdk_operation_with_attempts<T, Fut, Op>(
        &self,
        operation: &str,
        max_attempts: u32,
        mut operation_fn: Op,
    ) -> Result<T, Error>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MeilisearchSdkError>>,
    {
        let mut attempt = 0u32;

        loop {
            let result = tokio::time::timeout(self.config.timeout, operation_fn()).await;
            match result {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => {
                    attempt += 1;
                    let should_retry = attempt < max_attempts && is_transient_sdk_error(&error);
                    if !should_retry {
                        return Err(map_sdk_error(error));
                    }
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        attempt,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch {operation} failed (attempt {attempt}/{max_attempts}): {error}. Retrying in {delay:?}..."
                    );
                    sleep(delay).await;
                }
                Err(_) => {
                    attempt += 1;
                    if attempt >= max_attempts {
                        return Err(Error::HttpRequestFailed(format!(
                            "Meilisearch {operation} timed out after {:?}",
                            self.config.timeout
                        )));
                    }
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        attempt,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch {operation} timed out after {:?} (attempt {attempt}/{max_attempts}). Retrying in {delay:?}...",
                        self.config.timeout
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    async fn record_errors(&self, errors: usize) {
        let mut state = self.state.lock().await;
        state.errors_count += errors;
    }
}

#[async_trait]
impl Sink for MeilisearchSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Meilisearch sink connector with ID: {} for URL: {}, index: {}",
            self.id,
            sanitize_url_for_log(&self.config.url),
            self.config.index
        );

        let client = self.create_client()?;
        self.check_connectivity(&client).await?;
        self.ensure_index_exists(&client).await?;

        self.client = Some(client);
        info!(
            "Successfully opened Meilisearch sink connector with ID: {}",
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
        let mut state = self.state.lock().await;
        state.invocations_count += 1;
        let invocation = state.invocations_count;
        drop(state);

        info!(
            "Meilisearch sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
            messages_metadata.partition_id,
            messages_metadata.current_offset,
            invocation
        );

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Meilisearch client not initialized".to_string()))?;

        let messages_count = messages.len();
        let mut documents = Vec::with_capacity(messages.len());
        let mut invalid_records = 0usize;
        for message in messages {
            match self.prepare_document(topic_metadata, &messages_metadata, message) {
                Ok(Some(document)) => documents.push(document),
                Ok(None) => {}
                Err(Error::InvalidRecordValue(reason)) => {
                    invalid_records += 1;
                    warn!(
                        "Dropping invalid Meilisearch sink record for connector ID: {}, reason: {}",
                        self.id, reason
                    );
                }
                Err(error) => return Err(error),
            }
        }
        if invalid_records > 0 {
            self.record_errors(invalid_records).await;
        }

        if documents.is_empty() {
            return Ok(());
        }

        match self.index_documents(client, documents).await {
            Ok(accepted) => {
                let mut state = self.state.lock().await;
                state.documents_enqueued += accepted;
                if self.config.wait_for_tasks {
                    state.documents_indexed += accepted;
                }
                info!(
                    "Accepted {} of {} messages into Meilisearch index '{}'",
                    accepted, messages_count, self.config.index
                );
                Ok(())
            }
            Err(partial_error) => {
                let mut state = self.state.lock().await;
                state.documents_enqueued += partial_error.accepted;
                if self.config.wait_for_tasks {
                    state.documents_indexed += partial_error.accepted;
                }
                state.errors_count += 1;
                drop(state);
                Err(partial_error.error)
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Meilisearch sink connector with ID: {} is closing. Stats: {} invocations, {} documents enqueued, {} documents indexed, {} errors",
            self.id,
            state.invocations_count,
            state.documents_enqueued,
            state.documents_indexed,
            state.errors_count
        );
        drop(state);

        self.client = None;
        info!("Meilisearch sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

fn generated_document_id(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message: &ConsumedMessage,
) -> String {
    let components = json!([
        topic_metadata.stream.as_str(),
        topic_metadata.topic.as_str(),
        messages_metadata.partition_id,
        message.offset,
        message.id
    ]);
    let encoded = serde_json::to_vec(&components)
        .map(|bytes| general_purpose::URL_SAFE_NO_PAD.encode(bytes))
        .expect("generated ID components are always JSON-serializable");
    format!("iggy_{encoded}")
}

fn insert_metadata_field(object: &mut Map<String, Value>, field: &str, value: Value) {
    if object.contains_key(field) {
        debug!(
            "Document already contains Meilisearch metadata field '{field}', preserving original value"
        );
    } else {
        object.insert(field.to_string(), value);
    }
}

fn sanitize_url_for_log(raw: &str) -> String {
    let normalized = normalize_host(raw).unwrap_or_else(|_| raw.trim().to_string());
    let Ok(mut url) = Url::parse(&normalized) else {
        return "<invalid-url>".to_string();
    };

    if !url.username().is_empty() {
        let _ = url.set_username("");
    }
    if url.password().is_some() {
        let _ = url.set_password(None);
    }
    url.to_string()
}

fn normalize_host(raw: &str) -> Result<String, Error> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Connection(
            "Invalid Meilisearch URL: host cannot be empty".to_string(),
        ));
    }

    let with_scheme = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    let url = Url::parse(&with_scheme)
        .map_err(|error| Error::Connection(format!("Invalid Meilisearch URL: {error}")))?;
    let mut host = url.to_string();
    while host.ends_with('/') {
        host.pop();
    }
    Ok(host)
}

#[derive(Debug)]
struct PartialIndexError {
    accepted: usize,
    error: Error,
}

fn is_index_not_found(error: &MeilisearchSdkError) -> bool {
    matches!(
        error,
        MeilisearchSdkError::Meilisearch(meilisearch_error)
            if meilisearch_error.error_code == MeilisearchErrorCode::IndexNotFound
    )
}

fn is_transient_sdk_error(error: &MeilisearchSdkError) -> bool {
    match error {
        MeilisearchSdkError::Meilisearch(meilisearch_error) => {
            meilisearch_error.error_type == MeilisearchErrorType::Internal
        }
        MeilisearchSdkError::MeilisearchCommunication(communication_error) => {
            communication_error.status_code == 429 || communication_error.status_code >= 500
        }
        MeilisearchSdkError::HttpError(_) | MeilisearchSdkError::Timeout => true,
        _ => false,
    }
}

fn map_sdk_error(error: MeilisearchSdkError) -> Error {
    match error {
        MeilisearchSdkError::Meilisearch(meilisearch_error) => {
            if meilisearch_error.error_type == MeilisearchErrorType::Internal {
                Error::HttpRequestFailed(meilisearch_error.to_string())
            } else {
                Error::PermanentHttpError(meilisearch_error.to_string())
            }
        }
        MeilisearchSdkError::MeilisearchCommunication(communication_error) => {
            if communication_error.status_code == 429 || communication_error.status_code >= 500 {
                Error::HttpRequestFailed(communication_error.to_string())
            } else {
                Error::PermanentHttpError(communication_error.to_string())
            }
        }
        MeilisearchSdkError::ParseError(error) => {
            Error::Serialization(format!("Invalid Meilisearch response: {error}"))
        }
        MeilisearchSdkError::Timeout => {
            Error::HttpRequestFailed("Meilisearch task timed out".to_string())
        }
        MeilisearchSdkError::HttpError(error) => Error::HttpRequestFailed(error.to_string()),
        other => Error::HttpRequestFailed(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;

    fn topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "orders.stream".to_string(),
            topic: "created/topic".to_string(),
        }
    }

    fn messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 7,
            current_offset: 10,
            schema: Schema::Json,
        }
    }

    fn message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 11,
            checksum: 12,
            timestamp: 13,
            origin_timestamp: 14,
            headers: None,
            payload,
        }
    }

    fn sink_with_config(config: MeilisearchSinkConfig) -> MeilisearchSink {
        MeilisearchSink::new(1, config)
    }

    fn base_config() -> MeilisearchSinkConfig {
        MeilisearchSinkConfig {
            url: "http://localhost:7700".to_string(),
            index: "messages".to_string(),
            api_key: None,
            primary_key: None,
            document_action: None,
            create_index_if_not_exists: None,
            include_metadata: None,
            batch_size: None,
            timeout: None,
            wait_for_tasks: None,
            task_timeout: None,
            task_poll_interval: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            max_open_retries: None,
        }
    }

    #[test]
    fn generated_ids_use_meilisearch_safe_characters() {
        let id = generated_document_id(
            &topic_metadata(),
            &messages_metadata(),
            &message(Payload::Text("x".to_string())),
        );

        assert!(id.starts_with("iggy_"));
        assert!(
            id.chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        );
    }

    #[test]
    fn generated_ids_do_not_collapse_sanitized_names() {
        let first_topic = TopicMetadata {
            stream: "orders.stream".to_string(),
            topic: "created/topic".to_string(),
        };
        let second_topic = TopicMetadata {
            stream: "orders/stream".to_string(),
            topic: "created.topic".to_string(),
        };

        let first = generated_document_id(
            &first_topic,
            &messages_metadata(),
            &message(Payload::Text("x".to_string())),
        );
        let second = generated_document_id(
            &second_topic,
            &messages_metadata(),
            &message(Payload::Text("x".to_string())),
        );

        assert_ne!(first, second);
    }

    #[test]
    fn injects_default_primary_key_and_metadata() {
        let sink = sink_with_config(base_config());
        let payload = Payload::Json(simd_json::json!({
            "name": "Alice"
        }));
        let message = message(payload);
        let expected_id = generated_document_id(&topic_metadata(), &messages_metadata(), &message);

        let document = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["name"], "Alice");
        assert_eq!(document["iggy_id"], expected_id);
        assert_eq!(document["iggy_offset"], 11);
        assert_eq!(document["iggy_stream"], "orders.stream");
        assert_eq!(document["iggy_topic"], "created/topic");
    }

    #[test]
    fn preserves_existing_configured_primary_key() {
        let mut config = base_config();
        config.primary_key = Some("id".to_string());
        let sink = sink_with_config(config);
        let payload = Payload::Json(simd_json::json!({
            "id": "existing",
            "name": "Alice"
        }));
        let message = message(payload);
        let expected_id = generated_document_id(&topic_metadata(), &messages_metadata(), &message);

        let document = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["id"], "existing");
        assert_eq!(document["iggy_id"], expected_id);
    }

    #[test]
    fn preserves_existing_metadata_fields() {
        let sink = sink_with_config(base_config());
        let payload = Payload::Json(simd_json::json!({
            "name": "Alice",
            "iggy_offset": 999,
            "iggy_stream": "user-stream"
        }));

        let document = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message(payload))
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["iggy_offset"], 999);
        assert_eq!(document["iggy_stream"], "user-stream");
    }

    #[test]
    fn omits_metadata_when_include_metadata_is_false() {
        let mut config = base_config();
        config.include_metadata = Some(false);
        let sink = sink_with_config(config);
        let payload = Payload::Json(simd_json::json!({
            "name": "Alice"
        }));

        let document = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message(payload))
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["name"], "Alice");
        assert!(document["iggy_id"].as_str().is_some());
        assert!(document.get("iggy_offset").is_none());
        assert!(document.get("iggy_stream").is_none());
    }

    #[test]
    fn wraps_non_object_json_payloads() {
        let sink = sink_with_config(base_config());
        let message = message(Payload::Json(simd_json::json!(["a", "b"])));
        let expected_id = generated_document_id(&topic_metadata(), &messages_metadata(), &message);

        let document = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["value"], json!(["a", "b"]));
        assert_eq!(document["iggy_id"], expected_id);
    }

    #[test]
    fn raw_payloads_are_base64_encoded_when_not_json() {
        let sink = sink_with_config(base_config());

        let document = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Raw(vec![0, 1, 2, 3])),
            )
            .expect("prepare document")
            .expect("document");

        assert_eq!(document["data"], "AAECAw==");
        assert_eq!(document["data_encoding"], ENCODING_BASE64);
    }

    #[test]
    fn sanitize_url_should_redact_credentials_without_scheme() {
        let url = sanitize_url_for_log("user:pass@localhost:7700/indexes");
        assert_eq!(url, "http://localhost:7700/indexes");
    }

    #[test]
    fn unsupported_payloads_return_error() {
        let sink = sink_with_config(base_config());
        let error = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Avro(vec![1, 2, 3])),
            )
            .expect_err("unsupported payload should fail");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
    }
}
