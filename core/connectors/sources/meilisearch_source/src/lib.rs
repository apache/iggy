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
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    retry::{exponential_backoff, jitter, parse_duration},
    source_connector,
};
use meilisearch_sdk::{
    client::Client,
    errors::{
        Error as MeilisearchSdkError, ErrorCode as MeilisearchErrorCode,
        ErrorType as MeilisearchErrorType,
    },
    settings::FilterableAttribute,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{future::Future, time::Duration};
use tokio::{sync::Mutex, time::sleep};
use tracing::{info, warn};
use url::Url;

source_connector!(MeilisearchSource);

const CONNECTOR_NAME: &str = "Meilisearch source";
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_POLLING_INTERVAL: &str = "5s";
const DEFAULT_INCLUDE_METADATA: bool = false;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
const PRIMARY_KEY_SORT_DIRECTION: &str = "asc";

#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchSourceConfig {
    pub url: String,
    pub index: String,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub api_key: Option<SecretString>,
    pub query: Option<String>,
    pub filter: Option<Value>,
    pub batch_size: Option<usize>,
    pub polling_interval: Option<String>,
    pub include_metadata: Option<bool>,
    pub timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_open_retries: Option<u32>,
}

#[derive(Debug)]
pub struct MeilisearchSource {
    id: u32,
    config: ResolvedMeilisearchSourceConfig,
    client: Option<Client>,
    primary_key: Option<String>,
    primary_key_sort: Option<String>,
    filter_expression: Option<String>,
    state: Mutex<State>,
}

#[derive(Debug)]
struct ResolvedMeilisearchSourceConfig {
    url: String,
    index: String,
    api_key: Option<SecretString>,
    query: String,
    filter: Option<Value>,
    batch_size: usize,
    polling_interval: Duration,
    include_metadata: bool,
    timeout: Duration,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_open_retries: u32,
}

impl From<MeilisearchSourceConfig> for ResolvedMeilisearchSourceConfig {
    fn from(config: MeilisearchSourceConfig) -> Self {
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let polling_interval =
            parse_duration(config.polling_interval.as_deref(), DEFAULT_POLLING_INTERVAL);
        let include_metadata = config.include_metadata.unwrap_or(DEFAULT_INCLUDE_METADATA);
        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let mut retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        if retry_delay > max_retry_delay {
            warn!(
                "Meilisearch source retry_delay ({:?}) exceeds max_retry_delay ({:?}). Swapping values.",
                retry_delay, max_retry_delay
            );
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
        }
        let max_open_retries = config.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES);

        Self {
            url: config.url,
            index: config.index,
            api_key: config.api_key,
            query: config.query.unwrap_or_default(),
            filter: config.filter,
            batch_size,
            polling_interval,
            include_metadata,
            timeout,
            max_retries,
            retry_delay,
            max_retry_delay,
            max_open_retries,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct State {
    last_primary_key: Option<Value>,
    documents_produced: usize,
    poll_count: usize,
}

#[derive(Debug)]
struct ValidDocument {
    document: Value,
    message_id: Option<u128>,
}

impl MeilisearchSource {
    pub fn new(id: u32, config: MeilisearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let restored_state = state
            .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|state| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Last primary key: {:?}, documents produced: {}, poll count: {}",
                    state.last_primary_key, state.documents_produced, state.poll_count
                );
            });

        Self {
            id,
            config: config.into(),
            client: None,
            primary_key: None,
            primary_key_sort: None,
            filter_expression: None,
            state: Mutex::new(restored_state.unwrap_or(State {
                last_primary_key: None,
                documents_produced: 0,
                poll_count: 0,
            })),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn create_client(&self) -> Result<Client, Error> {
        let host = normalize_host(&self.config.url)?;
        let api_key = self
            .config
            .api_key
            .as_ref()
            .map(|key| key.expose_secret().to_string());

        Client::new(host, api_key).map_err(|error| {
            Error::Connection(format!("Failed to create Meilisearch client: {error}"))
        })
    }

    async fn check_connectivity(&self, client: &Client) -> Result<(), Error> {
        let mut retries = 0u32;

        loop {
            let result = tokio::time::timeout(self.config.timeout, client.health()).await;
            match result {
                Ok(Ok(health)) if health.status == "available" => return Ok(()),
                Ok(Ok(health)) => {
                    if retries >= self.config.max_open_retries {
                        return Err(Error::Connection(format!(
                            "Meilisearch health check returned status '{}'",
                            health.status
                        )));
                    }
                    retries += 1;
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        retries,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch health check returned status '{}' (retry {retries}/{}). Retrying in {delay:?}...",
                        health.status, self.config.max_open_retries
                    );
                    sleep(delay).await;
                }
                Ok(Err(error)) => {
                    if retries >= self.config.max_open_retries || !is_transient_sdk_error(&error) {
                        return Err(map_sdk_error(error));
                    }
                    retries += 1;
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        retries,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch health check failed (retry {retries}/{}): {error}. Retrying in {delay:?}...",
                        self.config.max_open_retries
                    );
                    sleep(delay).await;
                }
                Err(_) => {
                    if retries >= self.config.max_open_retries {
                        return Err(Error::HttpRequestFailed(format!(
                            "Meilisearch health check timed out after {:?}",
                            self.config.timeout
                        )));
                    }
                    retries += 1;
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        retries,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch health check timed out after {:?} (retry {retries}/{}). Retrying in {delay:?}...",
                        self.config.timeout, self.config.max_open_retries
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    async fn get_primary_key(&self, client: &Client) -> Result<String, Error> {
        let primary_key = self
            .retry_sdk_open_operation("get primary key", || async {
                let mut index = client.get_index(&self.config.index).await?;
                index
                    .get_primary_key()
                    .await
                    .map(|primary_key| primary_key.map(str::to_string))
            })
            .await?;

        primary_key.ok_or_else(|| {
            Error::InvalidConfigValue(format!(
                "Meilisearch index '{}' must define a primary key for stable source polling",
                self.config.index
            ))
        })
    }

    async fn check_primary_key_cursor_settings(
        &self,
        client: &Client,
        primary_key: &str,
    ) -> Result<(), Error> {
        let settings = self
            .retry_sdk_open_operation("get index settings", || {
                let index = client.index(&self.config.index);
                async move { index.get_settings().await }
            })
            .await?;

        let sortable_attributes = settings.sortable_attributes.unwrap_or_default();
        if primary_key_is_sortable(&sortable_attributes, primary_key) {
            let filterable_attributes = settings.filterable_attributes.unwrap_or_default();
            if primary_key_is_filterable(&filterable_attributes, primary_key) {
                return Ok(());
            }

            return Err(Error::InvalidConfigValue(format!(
                "Meilisearch index '{}' must configure primary key '{}' in filterableAttributes for cursor pagination",
                self.config.index, primary_key
            )));
        }

        Err(Error::InvalidConfigValue(format!(
            "Meilisearch index '{}' must configure primary key '{}' in sortableAttributes for stable source polling",
            self.config.index, primary_key
        )))
    }

    async fn validate_restored_cursor(&self, primary_key: &str) {
        let mut state = self.state.lock().await;
        let Some(last_primary_key) = state.last_primary_key.as_ref() else {
            return;
        };

        if let Err(error) = primary_key_filter_literal(last_primary_key) {
            warn!(
                "Discarding restored Meilisearch cursor for source connector with ID: {}. Invalid primary key '{}': {error}",
                self.id, primary_key
            );
            state.last_primary_key = None;
        }
    }

    async fn search_documents(&self, client: &Client) -> Result<Vec<ProducedMessage>, Error> {
        let last_primary_key = {
            let state = self.state.lock().await;
            state.last_primary_key.clone()
        };
        let primary_key = self.primary_key.as_deref().ok_or_else(|| {
            Error::Connection("Meilisearch primary key is not initialized".to_string())
        })?;
        let sort = self.primary_key_sort.as_deref().ok_or_else(|| {
            Error::Connection("Meilisearch primary key sort is not initialized".to_string())
        })?;
        let cursor_filter = cursor_filter_expression(primary_key, last_primary_key.as_ref())?;
        let combined_filter =
            combine_filter_expressions(self.filter_expression.as_deref(), cursor_filter);
        let sort_refs = [sort];
        let index = client.index(&self.config.index);
        let mut query = index.search();
        query
            .with_query(&self.config.query)
            .with_limit(self.config.batch_size)
            .with_sort(&sort_refs);

        if let Some(filter) = &combined_filter {
            query.with_filter(filter);
        }

        let results = self
            .retry_sdk_operation("search documents", || {
                let query = query.clone();
                async move { query.execute::<Value>().await }
            })
            .await?;
        let documents: Vec<Value> = results.hits.into_iter().map(|hit| hit.result).collect();
        let (documents, last_document_primary_key) =
            valid_documents_and_last_primary_key(documents, primary_key, self.id)?;
        let messages = self.documents_to_messages(documents)?;

        let mut state = self.state.lock().await;
        if let Some(primary_key) = last_document_primary_key {
            state.last_primary_key = Some(primary_key);
        }
        state.documents_produced += messages.len();
        state.poll_count += 1;

        Ok(messages)
    }

    fn documents_to_messages(
        &self,
        documents: Vec<ValidDocument>,
    ) -> Result<Vec<ProducedMessage>, Error> {
        documents
            .into_iter()
            .map(|valid_document| {
                let document = valid_document.document;
                let payload = if self.config.include_metadata {
                    json!({
                        "document": document,
                        "meilisearch": {
                            "index": self.config.index,
                            "primary_key": self.primary_key.as_deref(),
                        }
                    })
                } else {
                    document
                };

                serde_json::to_vec(&payload)
                    .map(|payload| ProducedMessage {
                        id: valid_document.message_id,
                        checksum: None,
                        timestamp: None,
                        origin_timestamp: None,
                        headers: None,
                        payload,
                    })
                    .map_err(|error| {
                        Error::Serialization(format!(
                            "Failed to serialize Meilisearch document: {error}"
                        ))
                    })
            })
            .collect()
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
        max_retries: u32,
        mut operation_fn: Op,
    ) -> Result<T, Error>
    where
        Op: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MeilisearchSdkError>>,
    {
        let mut retries = 0u32;

        loop {
            let result = tokio::time::timeout(self.config.timeout, operation_fn()).await;
            match result {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => {
                    if retries >= max_retries || !is_transient_sdk_error(&error) {
                        return Err(map_sdk_error(error));
                    }
                    retries += 1;
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        retries,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch {operation} failed (retry {retries}/{max_retries}): {error}. Retrying in {delay:?}..."
                    );
                    sleep(delay).await;
                }
                Err(_) => {
                    if retries >= max_retries {
                        return Err(Error::HttpRequestFailed(format!(
                            "Meilisearch {operation} timed out after {:?}",
                            self.config.timeout
                        )));
                    }
                    retries += 1;
                    let delay = jitter(exponential_backoff(
                        self.config.retry_delay,
                        retries,
                        self.config.max_retry_delay,
                    ));
                    warn!(
                        "Meilisearch {operation} timed out after {:?} (retry {retries}/{max_retries}). Retrying in {delay:?}...",
                        self.config.timeout
                    );
                    sleep(delay).await;
                }
            }
        }
    }
}

#[async_trait]
impl Source for MeilisearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        let sanitized_url = sanitize_url_for_logging(&self.config.url);
        info!(
            "Opening Meilisearch source connector with ID: {} for URL: {}, index: {}",
            self.id, sanitized_url, self.config.index
        );

        let filter_expression = filter_expression(self.config.filter.as_ref())?;
        let client = self.create_client()?;
        self.check_connectivity(&client).await?;
        let primary_key = self.get_primary_key(&client).await?;
        self.check_primary_key_cursor_settings(&client, &primary_key)
            .await?;
        self.validate_restored_cursor(&primary_key).await;
        info!(
            "Meilisearch source connector with ID: {} requires integer primary key values for cursor pagination. Index: {}, primary key: {}",
            self.id, self.config.index, primary_key
        );
        self.primary_key_sort = Some(format!("{primary_key}:{PRIMARY_KEY_SORT_DIRECTION}"));
        self.filter_expression = filter_expression;
        self.primary_key = Some(primary_key);
        self.client = Some(client);

        info!(
            "Successfully opened Meilisearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.config.polling_interval).await;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Meilisearch client not initialized".to_string()))?;
        let messages = self.search_documents(client).await?;
        let persisted_state = {
            let state = self.state.lock().await;
            self.serialize_state(&state)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        {
            let state = self.state.lock().await;
            info!(
                "Meilisearch source connector with ID: {} is closing. Stats: {} documents produced, {} polls executed",
                self.id, state.documents_produced, state.poll_count
            );
        }

        self.client = None;
        info!(
            "Meilisearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

fn normalize_host(host: &str) -> Result<String, Error> {
    let trimmed = host.trim();
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

    let mut url = Url::parse(&with_scheme)
        .map_err(|error| Error::Connection(format!("Invalid Meilisearch URL: {error}")))?;
    if url.host_str().is_none() {
        return Err(Error::Connection(
            "Invalid Meilisearch URL: host cannot be empty".to_string(),
        ));
    }
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::Connection(format!(
            "Invalid Meilisearch URL scheme '{}': expected http or https",
            url.scheme()
        )));
    }
    if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
        return Err(Error::Connection(
            "Invalid Meilisearch URL: path, query, and fragment components are not supported"
                .to_string(),
        ));
    }

    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    let mut normalized = url.to_string();
    while normalized.ends_with('/') {
        normalized.pop();
    }
    Ok(normalized)
}

fn sanitize_url_for_logging(host: &str) -> String {
    let Ok(normalized) = normalize_host(host) else {
        return "<invalid-url>".to_string();
    };
    let Some((scheme, rest)) = normalized.split_once("://") else {
        return normalized;
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    if let Some((_, host)) = authority.rsplit_once('@') {
        format!("{scheme}://<redacted>@{host}{path}")
    } else {
        normalized
    }
}

fn filter_array_expression(filters: &[Value], nested: bool) -> Result<Option<String>, Error> {
    let separator = if nested { " OR " } else { " AND " };
    let mut expressions = Vec::with_capacity(filters.len());

    for filter in filters {
        match filter {
            Value::String(filter) if !filter.is_empty() => expressions.push(filter.clone()),
            Value::Array(filters) => {
                if let Some(filter) = filter_array_expression(filters, true)? {
                    expressions.push(format!("({filter})"));
                }
            }
            _ => {
                return Err(Error::InvalidConfigValue(
                    "Meilisearch filter arrays must contain only strings or nested arrays"
                        .to_string(),
                ));
            }
        }
    }

    if expressions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(expressions.join(separator)))
    }
}

fn filter_expression(filter: Option<&Value>) -> Result<Option<String>, Error> {
    let Some(filter) = filter.filter(|value| !value.is_null()) else {
        return Ok(None);
    };

    match filter {
        Value::String(filter) if !filter.is_empty() => Ok(Some(filter.clone())),
        Value::Array(filters) => filter_array_expression(filters, false),
        _ => Err(Error::InvalidConfigValue(
            "Meilisearch filter must be a string or an array of strings/arrays".to_string(),
        )),
    }
}

fn combine_filter_expressions(
    user_filter: Option<&str>,
    cursor_filter: Option<String>,
) -> Option<String> {
    match (user_filter, cursor_filter) {
        (Some(user_filter), Some(cursor_filter)) => {
            Some(format!("({user_filter}) AND ({cursor_filter})"))
        }
        (Some(user_filter), None) => Some(user_filter.to_string()),
        (None, Some(cursor_filter)) => Some(cursor_filter),
        (None, None) => None,
    }
}

fn cursor_filter_expression(
    primary_key: &str,
    last_primary_key: Option<&Value>,
) -> Result<Option<String>, Error> {
    last_primary_key
        .map(|value| {
            primary_key_filter_literal(value).map(|literal| format!("{primary_key} > {literal}"))
        })
        .transpose()
}

fn primary_key_filter_literal(value: &Value) -> Result<String, Error> {
    match value {
        Value::Number(number) if number.is_i64() || number.is_u64() => serde_json::to_string(value)
            .map_err(|error| {
                Error::Serialization(format!(
                    "Failed to serialize Meilisearch primary key: {error}"
                ))
            }),
        Value::Number(_) => Err(Error::InvalidConfigValue(
            "Meilisearch source primary key values must be integers for cursor pagination"
                .to_string(),
        )),
        _ => Err(Error::InvalidConfigValue(
            "Meilisearch source primary key values must be numbers for cursor pagination"
                .to_string(),
        )),
    }
}

fn document_primary_key<'a>(document: &'a Value, primary_key: &str) -> Result<&'a Value, Error> {
    let value = document.get(primary_key).ok_or_else(|| {
        Error::InvalidConfigValue(format!(
            "Meilisearch document is missing primary key '{primary_key}'"
        ))
    })?;
    primary_key_filter_literal(value)?;
    Ok(value)
}

fn primary_key_to_message_id(value: &Value) -> Option<u128> {
    let number = value.as_number()?;
    number.as_u64().map(u128::from).or_else(|| {
        number
            .as_i64()
            .filter(|value| *value >= 0)
            .map(|value| value as u128)
    })
}

fn primary_key_is_sortable(sortable_attributes: &[String], primary_key: &str) -> bool {
    sortable_attributes
        .iter()
        .any(|attribute| attribute == primary_key)
}

fn primary_key_is_filterable(
    filterable_attributes: &[FilterableAttribute],
    primary_key: &str,
) -> bool {
    filterable_attributes
        .iter()
        .any(|attribute| match attribute {
            FilterableAttribute::Attribute(attribute) => attribute == primary_key,
            FilterableAttribute::Settings(settings) => settings
                .attribute_patterns
                .iter()
                .any(|pattern| pattern == "*" || pattern == primary_key),
        })
}

fn valid_documents_and_last_primary_key(
    documents: Vec<Value>,
    primary_key: &str,
    connector_id: u32,
) -> Result<(Vec<ValidDocument>, Option<Value>), Error> {
    let mut valid_documents = Vec::with_capacity(documents.len());
    let documents_count = documents.len();

    for document in documents {
        match document_primary_key(&document, primary_key) {
            Ok(primary_key_value) => {
                valid_documents.push(ValidDocument {
                    message_id: primary_key_to_message_id(primary_key_value),
                    document,
                });
            }
            Err(error) => warn!(
                "Skipping Meilisearch document for source connector with ID: {connector_id}. Invalid primary key '{primary_key}': {error}"
            ),
        }
    }

    if documents_count > 0 && valid_documents.is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "Meilisearch source connector with ID: {connector_id} received {documents_count} documents from index, but none had a valid integer primary key '{primary_key}'"
        )));
    }

    let last_primary_key = valid_documents
        .last()
        .map(|document| document_primary_key(&document.document, primary_key).cloned())
        .transpose()?;

    Ok((valid_documents, last_primary_key))
}

fn map_sdk_error(error: MeilisearchSdkError) -> Error {
    match error {
        MeilisearchSdkError::Meilisearch(meilisearch_error) => {
            if meilisearch_error.error_type == MeilisearchErrorType::Internal {
                Error::HttpRequestFailed(meilisearch_error.to_string())
            } else if meilisearch_error.error_code == MeilisearchErrorCode::IndexNotFound {
                Error::InvalidConfigValue(meilisearch_error.to_string())
            } else {
                Error::PermanentHttpError(meilisearch_error.to_string())
            }
        }
        MeilisearchSdkError::MeilisearchCommunication(communication_error) => {
            if communication_error.status_code == 0
                || communication_error.status_code == 429
                || communication_error.status_code >= 500
            {
                Error::HttpRequestFailed(communication_error.to_string())
            } else {
                Error::PermanentHttpError(communication_error.to_string())
            }
        }
        MeilisearchSdkError::ParseError(error) => {
            Error::Serialization(format!("Invalid Meilisearch response: {error}"))
        }
        MeilisearchSdkError::Timeout => {
            Error::HttpRequestFailed("Meilisearch request timed out".to_string())
        }
        MeilisearchSdkError::HttpError(error) => Error::HttpRequestFailed(error.to_string()),
        other => Error::HttpRequestFailed(other.to_string()),
    }
}

fn is_transient_sdk_error(error: &MeilisearchSdkError) -> bool {
    match error {
        MeilisearchSdkError::Meilisearch(meilisearch_error) => {
            meilisearch_error.error_type == MeilisearchErrorType::Internal
        }
        MeilisearchSdkError::MeilisearchCommunication(communication_error) => {
            communication_error.status_code == 0
                || communication_error.status_code == 429
                || communication_error.status_code >= 500
        }
        MeilisearchSdkError::HttpError(_) | MeilisearchSdkError::Timeout => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MeilisearchSourceConfig {
        MeilisearchSourceConfig {
            url: "localhost:7700".to_string(),
            index: "iggy_messages".to_string(),
            api_key: None,
            query: None,
            filter: None,
            batch_size: Some(10),
            polling_interval: Some("1ms".to_string()),
            include_metadata: None,
            timeout: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            max_open_retries: None,
        }
    }

    #[test]
    fn given_host_without_scheme_should_normalize_url() {
        let url = normalize_host("localhost:7700").unwrap();
        assert_eq!(url, "http://localhost:7700");
    }

    #[test]
    fn given_persisted_state_should_restore_last_primary_key() {
        let state = State {
            last_primary_key: Some(json!(42)),
            documents_produced: 42,
            poll_count: 7,
        };
        let connector_state = ConnectorState::serialize(&state, CONNECTOR_NAME, 1).unwrap();
        let source = MeilisearchSource::new(1, config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.last_primary_key, Some(json!(42)));
            assert_eq!(restored.documents_produced, 42);
            assert_eq!(restored.poll_count, 7);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = MeilisearchSource::new(1, config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.last_primary_key, None);
            assert_eq!(state.documents_produced, 0);
            assert_eq!(state.poll_count, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = MeilisearchSource::new(1, config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.last_primary_key, None);
            assert_eq!(state.documents_produced, 0);
            assert_eq!(state.poll_count, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            last_primary_key: Some(json!(9007199254740993_u64)),
            documents_produced: 123,
            poll_count: 11,
        };

        let connector_state = ConnectorState::serialize(&original, CONNECTOR_NAME, 1)
            .expect("Failed to serialize state");
        let restored = connector_state
            .deserialize::<State>(CONNECTOR_NAME, 1)
            .expect("Failed to deserialize state");

        assert_eq!(original, restored);
    }

    #[test]
    fn filter_expression_should_accept_string_filter() {
        let mut config = config();
        config.filter = Some(json!("status = active"));

        assert_eq!(
            filter_expression(config.filter.as_ref()).unwrap(),
            Some("status = active".to_string())
        );
    }

    #[test]
    fn given_retry_delay_exceeds_max_retry_delay_should_swap_values() {
        let mut config = config();
        config.retry_delay = Some("10s".to_string());
        config.max_retry_delay = Some("1s".to_string());

        let source = MeilisearchSource::new(1, config, None);

        assert_eq!(source.config.retry_delay, Duration::from_secs(1));
        assert_eq!(source.config.max_retry_delay, Duration::from_secs(10));
    }

    #[test]
    fn filter_expression_should_accept_nested_filter_arrays() {
        let mut config = config();
        config.filter = Some(json!([
            ["genres = horror", "genres = thriller"],
            "director = 'Jordan Peele'"
        ]));

        assert_eq!(
            filter_expression(config.filter.as_ref()).unwrap(),
            Some(
                "(genres = horror OR genres = thriller) AND director = 'Jordan Peele'".to_string()
            )
        );
    }

    #[test]
    fn filter_expression_should_reject_object_filter() {
        let error = filter_expression(Some(&json!({"status": "active"})))
            .expect_err("object filters should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn cursor_filter_should_compare_against_numeric_last_primary_key() {
        assert_eq!(
            cursor_filter_expression("id", Some(&json!(42))).unwrap(),
            Some("id > 42".to_string())
        );
    }

    #[test]
    fn cursor_filter_should_preserve_large_integer_last_primary_key() {
        assert_eq!(
            cursor_filter_expression("id", Some(&json!(9007199254740993_u64))).unwrap(),
            Some("id > 9007199254740993".to_string())
        );
    }

    #[test]
    fn cursor_filter_should_reject_float_last_primary_key() {
        let error = cursor_filter_expression("id", Some(&json!(1.5)))
            .expect_err("float primary key should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn cursor_filter_should_reject_string_last_primary_key() {
        let error = cursor_filter_expression("id", Some(&json!("movie-1")))
            .expect_err("string primary key should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn normalize_host_should_trim_before_checking_scheme() {
        let url = normalize_host(" https://localhost:7700/ ").unwrap();
        assert_eq!(url, "https://localhost:7700");
    }

    #[test]
    fn normalize_host_should_reject_path_components() {
        let error = normalize_host("https://localhost:7700/v1")
            .expect_err("path components should be rejected");

        assert!(matches!(error, Error::Connection(_)));
    }

    #[test]
    fn normalize_host_should_reject_query_components() {
        let error = normalize_host("https://localhost:7700?tenant=a")
            .expect_err("query should be rejected");

        assert!(matches!(error, Error::Connection(_)));
    }

    #[test]
    fn sanitize_url_should_redact_credentials() {
        let url = sanitize_url_for_logging("https://user:pass@localhost:7700");
        assert_eq!(url, "https://<redacted>@localhost:7700");
    }

    #[test]
    fn sanitize_url_should_redact_credentials_without_scheme() {
        let url = sanitize_url_for_logging("user:pass@localhost:7700");
        assert_eq!(url, "http://<redacted>@localhost:7700");
    }

    #[test]
    fn primary_key_is_sortable_should_match_exact_attribute() {
        let sortable_attributes = vec!["created_at".to_string(), "id".to_string()];

        assert!(primary_key_is_sortable(&sortable_attributes, "id"));
        assert!(!primary_key_is_sortable(&sortable_attributes, "user_id"));
    }

    #[test]
    fn primary_key_is_filterable_should_match_exact_attribute() {
        let filterable_attributes = vec![
            FilterableAttribute::Attribute("created_at".to_string()),
            FilterableAttribute::Attribute("id".to_string()),
        ];

        assert!(primary_key_is_filterable(&filterable_attributes, "id"));
        assert!(!primary_key_is_filterable(
            &filterable_attributes,
            "user_id"
        ));
    }

    #[test]
    fn primary_key_is_filterable_should_match_wildcard_pattern() {
        let filterable_attributes = vec![FilterableAttribute::Settings(
            meilisearch_sdk::settings::FilterableAttributesSettings {
                attribute_patterns: vec!["*".to_string()],
                features: meilisearch_sdk::settings::FilterFeatures {
                    facet_search: false,
                    filter: meilisearch_sdk::settings::FilterFeatureModes {
                        equality: true,
                        comparison: true,
                    },
                },
            },
        )];

        assert!(primary_key_is_filterable(&filterable_attributes, "id"));
    }

    #[test]
    fn valid_documents_should_skip_missing_middle_primary_key() {
        let documents = vec![
            json!({"id": 1, "name": "first"}),
            json!({"name": "missing"}),
            json!({"id": 3, "name": "third"}),
        ];

        let (valid_documents, last_primary_key) =
            valid_documents_and_last_primary_key(documents, "id", 1).unwrap();

        assert_eq!(valid_documents.len(), 2);
        assert_eq!(valid_documents[0].document["name"], "first");
        assert_eq!(valid_documents[1].document["name"], "third");
        assert_eq!(last_primary_key, Some(json!(3)));
    }

    #[test]
    fn valid_documents_should_skip_non_integer_middle_primary_key() {
        let documents = vec![
            json!({"id": 1, "name": "first"}),
            json!({"id": 1.5, "name": "float"}),
            json!({"id": 3, "name": "third"}),
        ];

        let (valid_documents, last_primary_key) =
            valid_documents_and_last_primary_key(documents, "id", 1).unwrap();

        assert_eq!(valid_documents.len(), 2);
        assert_eq!(valid_documents[0].document["name"], "first");
        assert_eq!(valid_documents[1].document["name"], "third");
        assert_eq!(last_primary_key, Some(json!(3)));
    }

    #[test]
    fn valid_documents_should_reject_all_invalid_primary_keys() {
        let documents = vec![
            json!({"id": 1.5, "name": "float"}),
            json!({"name": "missing"}),
        ];

        let error = valid_documents_and_last_primary_key(documents, "id", 1)
            .expect_err("all invalid primary keys should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn documents_should_be_wrapped_when_metadata_is_enabled() {
        let mut config = config();
        config.include_metadata = Some(true);
        let mut source = MeilisearchSource::new(1, config, None);
        source.primary_key = Some("id".to_string());
        let messages = source
            .documents_to_messages(vec![ValidDocument {
                document: json!({"id": 1, "title": "hello"}),
                message_id: Some(1),
            }])
            .unwrap();

        assert_eq!(messages[0].id, Some(1));
        let payload: Value = serde_json::from_slice(&messages[0].payload).unwrap();
        assert_eq!(
            payload,
            json!({
                "document": {"id": 1, "title": "hello"},
                "meilisearch": {
                    "index": "iggy_messages",
                    "primary_key": "id",
                }
            })
        );
    }

    #[test]
    fn documents_should_use_integer_primary_key_as_message_id() {
        let source = MeilisearchSource::new(1, config(), None);
        let (documents, _) = valid_documents_and_last_primary_key(
            vec![json!({"id": 9007199254740993_u64})],
            "id",
            1,
        )
        .unwrap();

        let messages = source.documents_to_messages(documents).unwrap();

        assert_eq!(messages[0].id, Some(9007199254740993));
    }

    #[test]
    fn restored_non_integer_cursor_should_be_wiped() {
        let state = State {
            last_primary_key: Some(json!("bad")),
            documents_produced: 10,
            poll_count: 2,
        };
        let connector_state = ConnectorState::serialize(&state, CONNECTOR_NAME, 1).unwrap();
        let source = MeilisearchSource::new(1, config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            source.validate_restored_cursor("id").await;
            let restored = source.state.lock().await;
            assert_eq!(restored.last_primary_key, None);
            assert_eq!(restored.documents_produced, 10);
            assert_eq!(restored.poll_count, 2);
        });
    }
}
