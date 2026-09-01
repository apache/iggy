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
use bytes::{BufMut, Bytes, BytesMut};
use iggy_common::{HeaderKey, HeaderValue, IggyTimestamp, calculate_256};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json,
    retry::{exponential_backoff, is_transient_status, jitter, parse_duration},
    sink_connector,
};
use opensearch::{
    BulkParts, OpenSearch,
    auth::Credentials,
    cluster::ClusterHealthParts,
    http::{
        StatusCode,
        transport::{SingleNodeConnectionPool, TransportBuilder},
    },
    indices::{IndicesCreateParts, IndicesExistsParts},
    params::Refresh,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    net::IpAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use url::Url;

sink_connector!(OpenSearchSink);

const DEFAULT_CREATE_INDEX_IF_NOT_EXISTS: bool = true;
const DEFAULT_INCLUDE_METADATA: bool = true;
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_MAX_OPEN_RETRIES: u32 = 5;
const ENCODING_BASE64: &str = "base64";
const ENCODING_UTF8: &str = "utf8";
const GENERATED_ID_PREFIX: &str = "iggy_";
const INDEX_ALREADY_EXISTS_ERROR: &str = "resource_already_exists_exception";

/// OpenSearch rejects `_id` values longer than 512 bytes. Payload-supplied IDs
/// are checked before the batch is built because that rejection fails the whole
/// `_bulk` call with an `action_request_validation_exception` rather than the
/// one item: a single oversized ID would cost every document in its chunk.
const MAX_DOCUMENT_ID_BYTES: usize = 512;

// No `Serialize`: nothing serializes this type, and the only in-tree helper for
// a `SecretString` field writes the credential in plaintext.
#[derive(Debug, Default, Deserialize)]
pub struct OpenSearchSinkConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    pub document_id_field: Option<String>,
    pub create_index_if_not_exists: Option<bool>,
    pub index_mapping: Option<Value>,
    pub include_metadata: Option<bool>,
    pub batch_size: Option<usize>,
    pub timeout: Option<String>,
    pub refresh: Option<Refresh>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub max_open_retries: Option<u32>,
    pub verbose_logging: Option<bool>,
}

pub struct OpenSearchSink {
    id: u32,
    config: ResolvedOpenSearchSinkConfig,
    client: Option<OpenSearch>,
    invocations_count: AtomicU64,
    documents_indexed: AtomicU64,
    errors_count: AtomicU64,
}

// `OpenSearch` derives `Debug` down through its `Transport`, and
// `opensearch::auth::Credentials::Basic` derives `Debug` on its raw
// `(String, String)` without redaction, so a derived `Debug` on this struct
// would print the Basic-auth password in plaintext once `client` is set.
impl std::fmt::Debug for OpenSearchSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenSearchSink")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("client", &self.client.is_some())
            .field("invocations_count", &self.invocations_count)
            .field("documents_indexed", &self.documents_indexed)
            .field("errors_count", &self.errors_count)
            .finish()
    }
}

struct ResolvedOpenSearchSinkConfig {
    url: String,
    index: String,
    username: Option<String>,
    password: Option<SecretString>,
    document_id_field: Option<String>,
    create_index_if_not_exists: bool,
    index_mapping: Option<Value>,
    include_metadata: bool,
    batch_size: usize,
    timeout: Duration,
    refresh: Option<Refresh>,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    max_open_retries: u32,
    verbose_logging: bool,
}

// `derive(Debug)` would print `url` verbatim; `open()` isn't what redacts it.
impl std::fmt::Debug for ResolvedOpenSearchSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedOpenSearchSinkConfig")
            .field("url", &redact_url_credentials(&self.url))
            .field("index", &self.index)
            .field("username", &self.username)
            .field("password", &self.password)
            .field(
                "create_index_if_not_exists",
                &self.create_index_if_not_exists,
            )
            .field("index_mapping", &self.index_mapping)
            .field("include_metadata", &self.include_metadata)
            .field("batch_size", &self.batch_size)
            .field("timeout", &self.timeout)
            .field("refresh", &self.refresh)
            .field("max_retries", &self.max_retries)
            .field("retry_delay", &self.retry_delay)
            .field("max_retry_delay", &self.max_retry_delay)
            .field("max_open_retries", &self.max_open_retries)
            .field("verbose_logging", &self.verbose_logging)
            .finish()
    }
}

impl From<OpenSearchSinkConfig> for ResolvedOpenSearchSinkConfig {
    fn from(config: OpenSearchSinkConfig) -> Self {
        let mut retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        if retry_delay > max_retry_delay {
            warn!(
                "OpenSearch sink retry_delay ({:?}) exceeds max_retry_delay ({:?}). Swapping values.",
                retry_delay, max_retry_delay
            );
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
        }

        Self {
            url: config.url,
            index: config.index.trim().to_string(),
            username: trimmed_non_empty(config.username),
            password: config
                .password
                .filter(|password| !is_blank_secret(password)),
            document_id_field: trimmed_non_empty(config.document_id_field),
            create_index_if_not_exists: config
                .create_index_if_not_exists
                .unwrap_or(DEFAULT_CREATE_INDEX_IF_NOT_EXISTS),
            index_mapping: config.index_mapping,
            include_metadata: config.include_metadata.unwrap_or(DEFAULT_INCLUDE_METADATA),
            batch_size: config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1),
            timeout: parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT),
            refresh: config.refresh,
            max_retries: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_delay,
            max_retry_delay,
            max_open_retries: config.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
            verbose_logging: config.verbose_logging.unwrap_or(false),
        }
    }
}

impl OpenSearchSink {
    pub fn new(id: u32, config: OpenSearchSinkConfig) -> Self {
        Self {
            id,
            config: config.into(),
            client: None,
            invocations_count: AtomicU64::new(0),
            documents_indexed: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
        }
    }

    fn validate_config(&self) -> Result<(), Error> {
        if self.config.index.is_empty() {
            return Err(Error::InvalidConfigValue(
                "OpenSearch index cannot be empty".to_string(),
            ));
        }

        match (&self.config.username, &self.config.password) {
            (Some(_), None) => Err(Error::InvalidConfigValue(
                "OpenSearch username is set without a password".to_string(),
            )),
            (None, Some(_)) => Err(Error::InvalidConfigValue(
                "OpenSearch password is set without a username".to_string(),
            )),
            _ => Ok(()),
        }
    }

    /// Takes the normalized URL rather than normalizing again, so
    /// `normalize_url`'s warnings are emitted once per `open()`.
    fn create_client(&self, normalized_url: &str) -> Result<OpenSearch, Error> {
        warn_if_credentials_use_insecure_http(
            &self.config.url,
            normalized_url,
            self.config.password.is_some(),
        );

        let url = Url::parse(normalized_url)
            .map_err(|error| Error::Connection(format!("Invalid OpenSearch URL: {error}")))?;
        // The transport defaults to no timeout at all, and unlike the per-call
        // `tokio::time::timeout` guards this also covers reading response bodies.
        let mut builder =
            TransportBuilder::new(SingleNodeConnectionPool::new(url)).timeout(self.config.timeout);
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            builder = builder.auth(Credentials::Basic(
                username.to_owned(),
                password.expose_secret().to_owned(),
            ));
        }

        let transport = builder.build().map_err(|error| {
            Error::Connection(format!("Failed to build OpenSearch transport: {error}"))
        })?;
        Ok(OpenSearch::new(transport))
    }

    /// Retries a transiently failing `open()`-time call, so one blip does not
    /// park the connector in `Error` until an operator restarts it.
    async fn retry_on_open<T, F, Fut>(&self, operation: &str, call: F) -> Result<T, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut retries = 0u32;

        loop {
            let failure = match call().await {
                Ok(value) => return Ok(value),
                Err(error) if is_transient_error(&error) => error.to_string(),
                Err(error) => return Err(error),
            };

            if retries >= self.config.max_open_retries {
                return Err(Error::InitError(format!(
                    "OpenSearch {operation} failed after {} retries ({failure})",
                    self.config.max_open_retries
                )));
            }

            retries += 1;
            self.sleep_before_retry(operation, retries, self.config.max_open_retries, &failure)
                .await;
        }
    }

    async fn check_connectivity(&self, client: &OpenSearch) -> Result<(), Error> {
        self.retry_on_open("health check", || self.cluster_health(client))
            .await
    }

    async fn cluster_health(&self, client: &OpenSearch) -> Result<(), Error> {
        let response = tokio::time::timeout(
            self.config.timeout,
            client.cluster().health(ClusterHealthParts::None).send(),
        )
        .await
        .map_err(|_| {
            Error::HttpRequestFailed(format!(
                "OpenSearch health check timed out after {:?}",
                self.config.timeout
            ))
        })?
        .map_err(|error| map_client_error("health check", error))?;

        let status = response.status_code();
        if status.is_success() {
            return Ok(());
        }

        // A 403 only means cluster:monitor/health is missing, not that the cluster is down.
        if status == StatusCode::FORBIDDEN {
            warn!(
                "OpenSearch health check returned 403: the configured user lacks the cluster-scoped cluster:monitor/health privilege. Treating the cluster as reachable; grant that privilege to restore the check."
            );
            return Ok(());
        }

        let body = response.text().await.unwrap_or_default();
        Err(map_status_error("health check", status, &body))
    }

    async fn ensure_index_exists(&self, client: &OpenSearch) -> Result<(), Error> {
        if self
            .retry_on_open("index existence check", || self.index_exists(client))
            .await?
        {
            info!("OpenSearch index '{}' already exists", self.config.index);
            return Ok(());
        }

        if !self.config.create_index_if_not_exists {
            return Err(Error::InitError(format!(
                "OpenSearch index '{}' does not exist and create_index_if_not_exists=false",
                self.config.index
            )));
        }

        self.retry_on_open("index creation", || self.create_index(client))
            .await
    }

    async fn index_exists(&self, client: &OpenSearch) -> Result<bool, Error> {
        let response = client
            .indices()
            .exists(IndicesExistsParts::Index(&[&self.config.index]))
            .send()
            .await
            .map_err(|error| map_client_error("index existence check", error))?;

        let status = response.status_code();
        if status.is_success() {
            return Ok(true);
        }

        // A missing index comes back as a 404 response, not a transport error,
        // so anything else is a genuine failure worth surfacing.
        if status == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        let body = response.text().await.unwrap_or_default();
        Err(map_status_error("index existence check", status, &body))
    }

    async fn create_index(&self, client: &OpenSearch) -> Result<(), Error> {
        info!("Creating OpenSearch index '{}'", self.config.index);

        let indices = client.indices();
        let request = indices.create(IndicesCreateParts::Index(&self.config.index));
        let response = if let Some(mapping) = &self.config.index_mapping {
            request.body(mapping.clone()).send().await
        } else {
            request.send().await
        }
        .map_err(|error| map_client_error("index creation", error))?;

        let status = response.status_code();
        if status.is_success() {
            info!("Created OpenSearch index '{}'", self.config.index);
            return Ok(());
        }

        let body = response.text().await.unwrap_or_default();
        // Another runtime instance winning the create race is not an error.
        if is_index_already_exists_error(&body) {
            info!(
                "OpenSearch index '{}' was created concurrently",
                self.config.index
            );
            return Ok(());
        }

        Err(map_status_error("index creation", status, &body))
    }

    fn prepare_document(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        mut message: ConsumedMessage,
    ) -> Result<PreparedDocument, Error> {
        let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));

        let mut document = match payload {
            Payload::Json(value) => document_from_json(owned_value_to_serde_json(&value)),
            Payload::Raw(bytes) => document_from_raw(bytes),
            Payload::Text(text) => Map::from_iter([
                ("text".to_string(), Value::String(text)),
                ("data_type".to_string(), Value::String("text".to_string())),
            ]),
            _ => {
                return Err(Error::InvalidRecordValue(format!(
                    "Unsupported payload format for OpenSearch sink: {}",
                    messages_metadata.schema
                )));
            }
        };

        let id = match self.document_id_from_field(&document)? {
            Some(id) => id,
            None => generated_document_id(
                topic_metadata,
                messages_metadata,
                message.offset,
                message.id,
            )?,
        };

        if self.config.include_metadata {
            inject_metadata(&mut document, topic_metadata, messages_metadata, &message);
        }

        Ok(PreparedDocument {
            id,
            document: Value::Object(document),
        })
    }

    fn document_id_from_field(
        &self,
        document: &Map<String, Value>,
    ) -> Result<Option<String>, Error> {
        let Some(field) = self.config.document_id_field.as_deref() else {
            return Ok(None);
        };
        let Some(value) = document.get(field) else {
            return Ok(None);
        };

        let id = match value {
            Value::String(text) => text.clone(),
            Value::Number(number) => number.to_string(),
            Value::Bool(flag) => flag.to_string(),
            Value::Null | Value::Array(_) | Value::Object(_) => {
                return Err(Error::InvalidRecordValue(format!(
                    "OpenSearch document_id_field '{field}' must be a string, number, or boolean"
                )));
            }
        };

        if id.is_empty() {
            return Err(Error::InvalidRecordValue(format!(
                "OpenSearch document_id_field '{field}' is empty"
            )));
        }
        if id.len() > MAX_DOCUMENT_ID_BYTES {
            return Err(Error::InvalidRecordValue(format!(
                "OpenSearch document_id_field '{field}' exceeds the {MAX_DOCUMENT_ID_BYTES} byte limit"
            )));
        }

        Ok(Some(id))
    }

    async fn index_documents(
        &self,
        client: &OpenSearch,
        documents: Vec<PreparedDocument>,
    ) -> Result<usize, PartialIndexError> {
        let total = documents.len();
        let mut indexed = 0usize;
        let mut attempted = 0usize;
        // The runtime commits the offset regardless of this error, so returning
        // early would drop the remaining chunks for good.
        let mut last_error: Option<Error> = None;

        for chunk in documents.chunks(self.config.batch_size) {
            attempted += chunk.len();
            match self.index_chunk(client, chunk).await {
                Ok(outcome) => {
                    indexed += outcome.indexed;
                    if let Some(error) = outcome.into_error(&self.config.index) {
                        last_error = Some(error);
                    }
                }
                Err(error) => last_error = Some(error),
            }
            debug!(
                "OpenSearch sink with ID: {} indexed {}/{} documents",
                self.id, attempted, total
            );
        }

        match last_error {
            Some(error) => Err(PartialIndexError {
                indexed,
                failed: total - indexed,
                error,
            }),
            None => Ok(indexed),
        }
    }

    async fn index_chunk(
        &self,
        client: &OpenSearch,
        documents: &[PreparedDocument],
    ) -> Result<BulkOutcome, Error> {
        // Only the documents OpenSearch has not yet accepted. A per-item
        // transient rejection (429 under load, most commonly) shrinks this to
        // just those documents rather than resending the whole chunk.
        let mut pending: Vec<&PreparedDocument> = documents.iter().collect();
        // Rebuilt only when `pending` shrinks; `Bytes` clones are a refcount
        // bump, so a whole-request retry resends without re-serializing.
        let mut body = build_bulk_body(&self.config.index, &pending)?;
        let mut outcome = BulkOutcome::default();
        let mut retries = 0u32;

        loop {
            let request = client.bulk(BulkParts::None).body(vec![body.clone()]);
            let request = match self.config.refresh {
                Some(refresh) => request.refresh(refresh),
                None => request,
            };

            let failure = match tokio::time::timeout(self.config.timeout, request.send()).await {
                Ok(Ok(response)) => {
                    let status = response.status_code();
                    if !status.is_success() {
                        let response_body = response.text().await.unwrap_or_default();
                        let error = map_status_error("bulk request", status, &response_body);
                        if !is_transient_status(status) {
                            // Documents an earlier attempt in this same retry loop
                            // already indexed must still count; only `pending` (not
                            // yet accepted) is failed by this non-transient status.
                            outcome.merge(BulkOutcome {
                                indexed: 0,
                                failed: pending.len(),
                                transient: false,
                                first_failure: Some(error.to_string()),
                            });
                            return Ok(outcome);
                        }
                        error.to_string()
                    } else {
                        // A bulk call answers 200 even when individual documents
                        // fail, so the per-item results decide the outcome.
                        match response.json::<Value>().await {
                            Ok(payload) => match parse_bulk_response(&payload, pending.len()) {
                                Ok(attempt) => {
                                    let retry_set = documents_at(&pending, &attempt.retryable);

                                    if retry_set.is_empty() || retries >= self.config.max_retries {
                                        outcome.merge(attempt.into_outcome());
                                        return Ok(outcome);
                                    }

                                    // Only the permanent half is final; the retryable half
                                    // is settled by a later attempt.
                                    outcome.merge(BulkOutcome {
                                        indexed: attempt.indexed,
                                        failed: attempt.permanent_failed,
                                        transient: false,
                                        first_failure: attempt.first_permanent_failure,
                                    });

                                    let rejected = retry_set.len();
                                    pending = retry_set;
                                    body = build_bulk_body(&self.config.index, &pending)?;
                                    format!(
                                        "{rejected} document(s) rejected with a transient status"
                                    )
                                }
                                // A 200 whose `items` array can't be trusted to cover
                                // every pending document leaves the true per-item
                                // outcome unknown. Retrying is safe because indexing
                                // is idempotent by `_id`, so this is treated as
                                // transient rather than silently counting the chunk
                                // as handled.
                                Err(error) => {
                                    format!("failed to parse bulk response items: {error}")
                                }
                            },
                            // A 200 with an unparsable body leaves the true
                            // per-item outcome unknown. Retrying is safe
                            // because indexing is idempotent by `_id`, so this
                            // is treated as transient rather than hard-failing
                            // a chunk OpenSearch may already have accepted.
                            Err(error) => {
                                format!("failed to parse bulk response body: {error}")
                            }
                        }
                    }
                }
                Ok(Err(error)) => {
                    if !is_transient_client_error(&error) {
                        let error = map_client_error("bulk request", error);
                        outcome.merge(BulkOutcome {
                            indexed: 0,
                            failed: pending.len(),
                            transient: false,
                            first_failure: Some(error.to_string()),
                        });
                        return Ok(outcome);
                    }
                    error.to_string()
                }
                Err(_) => format!("timed out after {:?}", self.config.timeout),
            };

            // Reported through the outcome rather than `Err` so documents
            // accepted by earlier attempts still count as indexed.
            if retries >= self.config.max_retries {
                outcome.merge(BulkOutcome {
                    indexed: 0,
                    failed: pending.len(),
                    transient: true,
                    first_failure: Some(format!(
                        "OpenSearch bulk request failed after {} retries ({failure})",
                        self.config.max_retries
                    )),
                });
                return Ok(outcome);
            }

            retries += 1;
            self.sleep_before_retry("bulk request", retries, self.config.max_retries, &failure)
                .await;
        }
    }

    /// Waits out the backoff for an already-incremented `retries`. The first
    /// retry (`retries == 1`) uses attempt `0` so it sleeps `retry_delay`
    /// itself rather than `retry_delay * 2`. Jitter can push the raw backoff
    /// up to 20% above `max_retry_delay`, so the jittered result is clamped
    /// back down to it.
    async fn sleep_before_retry(
        &self,
        operation: &str,
        retries: u32,
        max_retries: u32,
        failure: &str,
    ) {
        let delay = jitter(exponential_backoff(
            self.config.retry_delay,
            retries - 1,
            self.config.max_retry_delay,
        ))
        .min(self.config.max_retry_delay);
        warn!(
            "OpenSearch {} failed (retry {}/{}): {}. Retrying in {:?}...",
            operation, retries, max_retries, failure, delay
        );
        sleep(delay).await;
    }
}

#[async_trait]
impl Sink for OpenSearchSink {
    async fn open(&mut self) -> Result<(), Error> {
        self.validate_config()?;
        let normalized_url = normalize_url(&self.config.url)?;
        info!(
            "Opening OpenSearch sink connector with ID: {} for URL: {}, index: {}",
            self.id,
            sanitize_url_for_log(&normalized_url),
            self.config.index
        );

        let client = self.create_client(&normalized_url)?;
        self.check_connectivity(&client).await?;
        self.ensure_index_exists(&client).await?;
        self.client = Some(client);

        info!(
            "Successfully opened OpenSearch sink connector with ID: {}",
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
        let invocation = self.invocations_count.fetch_add(1, Ordering::Relaxed) + 1;

        if self.config.verbose_logging {
            info!(
                "OpenSearch sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
                self.id,
                messages.len(),
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset,
                invocation
            );
        } else {
            debug!(
                "OpenSearch sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}, partition: {}, offset: {}, invocation: {}",
                self.id,
                messages.len(),
                messages_metadata.schema,
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset,
                invocation
            );
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("OpenSearch client not initialized".to_string()))?;

        let messages_count = messages.len();
        let mut documents = Vec::with_capacity(messages_count);
        let mut invalid_records = 0usize;
        let mut preparation_errors = 0usize;
        for message in messages {
            match self.prepare_document(topic_metadata, &messages_metadata, message) {
                Ok(document) => documents.push(document),
                Err(Error::InvalidRecordValue(reason)) => {
                    invalid_records += 1;
                    warn!(
                        "Dropping invalid OpenSearch sink record for connector ID: {}, reason: {}",
                        self.id, reason
                    );
                }
                // A single message's preparation failing must not discard the
                // documents already built from earlier messages in this batch.
                Err(error) => {
                    preparation_errors += 1;
                    error!(
                        "Failed to prepare OpenSearch sink document for connector ID: {}, error: {}",
                        self.id, error
                    );
                }
            }
        }
        if invalid_records > 0 || preparation_errors > 0 {
            self.errors_count.fetch_add(
                (invalid_records + preparation_errors) as u64,
                Ordering::Relaxed,
            );
        }

        if documents.is_empty() {
            return Ok(());
        }

        match self.index_documents(client, documents).await {
            Ok(indexed) => {
                self.documents_indexed
                    .fetch_add(indexed as u64, Ordering::Relaxed);
                if self.config.verbose_logging {
                    info!(
                        "Indexed {} of {} messages into OpenSearch index '{}'",
                        indexed, messages_count, self.config.index
                    );
                } else {
                    debug!(
                        "Indexed {} of {} messages into OpenSearch index '{}'",
                        indexed, messages_count, self.config.index
                    );
                }
                Ok(())
            }
            Err(partial) => {
                self.documents_indexed
                    .fetch_add(partial.indexed as u64, Ordering::Relaxed);
                self.errors_count
                    .fetch_add(partial.failed as u64, Ordering::Relaxed);
                error!(
                    "Failed to index OpenSearch sink batch for connector ID: {}, index: {}, indexed: {}, failed: {}, error: {}",
                    self.id, self.config.index, partial.indexed, partial.failed, partial.error
                );
                Err(partial.error)
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!(
            "OpenSearch sink connector with ID: {} is closing. Stats: {} invocations, {} documents indexed, {} errors",
            self.id,
            self.invocations_count.load(Ordering::Relaxed),
            self.documents_indexed.load(Ordering::Relaxed),
            self.errors_count.load(Ordering::Relaxed)
        );

        self.client = None;
        info!("OpenSearch sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedDocument {
    id: String,
    document: Value,
}

#[derive(Debug)]
struct PartialIndexError {
    indexed: usize,
    failed: usize,
    error: Error,
}

/// Aggregated result of every `_bulk` attempt made for one chunk, including
/// the per-item retries.
#[derive(Debug, Default, PartialEq, Eq)]
struct BulkOutcome {
    indexed: usize,
    failed: usize,
    transient: bool,
    first_failure: Option<String>,
}

impl BulkOutcome {
    /// Folds one attempt's totals in. The first failure seen across every
    /// attempt for the chunk is the one reported.
    fn merge(&mut self, other: BulkOutcome) {
        self.indexed += other.indexed;
        self.failed += other.failed;
        self.transient |= other.transient;
        self.first_failure = self.first_failure.take().or(other.first_failure);
    }

    fn into_error(self, index: &str) -> Option<Error> {
        let failure = self.first_failure?;
        let message = format!(
            "OpenSearch bulk indexing into '{index}' failed for {} of {} documents: {failure}",
            self.failed,
            self.failed + self.indexed
        );
        Some(if self.transient {
            Error::HttpRequestFailed(message)
        } else {
            Error::PermanentHttpError(message)
        })
    }
}

/// Per-item breakdown of a single `_bulk` call. OpenSearch answers 200 with a
/// per-item list echoed back in request order, so `retryable` holds positions
/// into the slice that was sent.
#[derive(Debug, Default, PartialEq, Eq)]
struct BulkAttempt {
    indexed: usize,
    permanent_failed: usize,
    first_permanent_failure: Option<String>,
    retryable: Vec<usize>,
    first_retryable_failure: Option<String>,
}

impl BulkAttempt {
    /// Collapses one attempt into the aggregate shape, counting every
    /// still-retryable item as failed. This is what a caller out of retries
    /// reports.
    fn into_outcome(self) -> BulkOutcome {
        BulkOutcome {
            indexed: self.indexed,
            failed: self.permanent_failed + self.retryable.len(),
            transient: !self.retryable.is_empty(),
            first_failure: self
                .first_permanent_failure
                .or(self.first_retryable_failure),
        }
    }
}

/// Serializes the `_bulk` NDJSON payload (alternating `index` action line and
/// document line per document) into one `Bytes` buffer. Takes references so a
/// retry can serialize just the rejected subset without cloning documents.
fn build_bulk_body(index: &str, documents: &[&PreparedDocument]) -> Result<Bytes, Error> {
    let mut buffer = BytesMut::new();
    for document in documents {
        serde_json::to_writer(
            (&mut buffer).writer(),
            &json!({ "index": { "_index": index, "_id": document.id } }),
        )
        .map_err(|error| {
            Error::Serialization(format!(
                "Failed to serialize OpenSearch bulk action: {error}"
            ))
        })?;
        buffer.put_u8(b'\n');
        serde_json::to_writer((&mut buffer).writer(), &document.document).map_err(|error| {
            Error::Serialization(format!(
                "Failed to serialize OpenSearch bulk document: {error}"
            ))
        })?;
        buffer.put_u8(b'\n');
    }
    Ok(buffer.freeze())
}

/// Maps server-echoed item positions back to the documents that were sent.
/// Out-of-range positions are dropped: a response carrying more items than
/// were sent would otherwise panic, and a panic crossing the plugin's
/// `extern "C"` boundary aborts the whole connectors runtime process.
fn documents_at<'a>(
    pending: &[&'a PreparedDocument],
    positions: &[usize],
) -> Vec<&'a PreparedDocument> {
    positions
        .iter()
        .filter_map(|&position| pending.get(position).copied())
        .collect()
}

/// Why a `_bulk` response's `items` array could not be trusted to reflect
/// what OpenSearch actually did with every pending document.
#[derive(Debug)]
enum BulkResponseError {
    MissingItems,
    ItemCountMismatch { expected: usize, actual: usize },
    MalformedItem { position: usize },
}

impl std::fmt::Display for BulkResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingItems => write!(f, "response has no `items` array"),
            Self::ItemCountMismatch { expected, actual } => write!(
                f,
                "response `items` array has {actual} entries, expected {expected}"
            ),
            Self::MalformedItem { position } => {
                write!(f, "item at position {position} has no recognizable result")
            }
        }
    }
}

/// Requires exactly one item per pending document. A missing, short, or
/// malformed `items` array leaves the true per-item outcome unknown, so the
/// whole response is rejected rather than silently under-accounted: a chunk
/// that OpenSearch answered with fewer or unparsable items must not be
/// treated as cleanly handled just because nothing came back marked failed.
fn parse_bulk_response(
    response: &Value,
    expected: usize,
) -> Result<BulkAttempt, BulkResponseError> {
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .ok_or(BulkResponseError::MissingItems)?;

    if items.len() != expected {
        return Err(BulkResponseError::ItemCountMismatch {
            expected,
            actual: items.len(),
        });
    }

    // `errors: false` is only trustworthy if every item has a 2xx result.
    if !response
        .get("errors")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        for (position, item) in items.iter().enumerate() {
            let status = item
                .as_object()
                .and_then(|item| item.values().next())
                .and_then(|result| result.get("status"))
                .and_then(Value::as_u64);
            if !status.is_some_and(|status| (200..300).contains(&status)) {
                return Err(BulkResponseError::MalformedItem { position });
            }
        }
        return Ok(BulkAttempt {
            indexed: items.len(),
            ..BulkAttempt::default()
        });
    }

    let mut attempt = BulkAttempt::default();
    for (position, item) in items.iter().enumerate() {
        let result = item
            .as_object()
            .and_then(|item| item.values().next())
            .ok_or(BulkResponseError::MalformedItem { position })?;

        let status = result.get("status").and_then(Value::as_u64).unwrap_or(0) as u16;
        if result.get("error").is_none() && (200..300).contains(&status) {
            attempt.indexed += 1;
            continue;
        }

        let reason = result
            .get("error")
            .and_then(|error| error.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        let failure = format!("status {status}: {reason}");

        if StatusCode::from_u16(status).is_ok_and(is_transient_status) {
            attempt.retryable.push(position);
            if attempt.first_retryable_failure.is_none() {
                attempt.first_retryable_failure = Some(failure);
            }
            continue;
        }

        attempt.permanent_failed += 1;
        if attempt.first_permanent_failure.is_none() {
            attempt.first_permanent_failure = Some(failure);
        }
    }

    Ok(attempt)
}

fn document_from_json(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        other => Map::from_iter([("value".to_string(), other)]),
    }
}

// `serde_json::from_slice` parses without mutating `bytes` (unlike
// `simd_json`, which parses in place), so the base64 fallback can reuse the
// original buffer instead of cloning the whole payload up front.
fn document_from_raw(bytes: Vec<u8>) -> Map<String, Value> {
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => document_from_json(value),
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

/// Writes the reserved `iggy_*` provenance fields, overwriting any same-named
/// payload fields so provenance always reflects the true message coordinates.
fn inject_metadata(
    document: &mut Map<String, Value>,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message: &ConsumedMessage,
) {
    let fields = [
        ("iggy_message_id", Value::String(message.id.to_string())),
        ("iggy_offset", Value::from(message.offset)),
        ("iggy_stream", Value::from(topic_metadata.stream.as_str())),
        ("iggy_topic", Value::from(topic_metadata.topic.as_str())),
        (
            "iggy_partition",
            Value::from(messages_metadata.partition_id),
        ),
        // A string: checksums exceed the 2^53 range JSON represents exactly.
        ("iggy_checksum", Value::String(message.checksum.to_string())),
        ("iggy_timestamp", Value::from(message.timestamp)),
        (
            "iggy_origin_timestamp",
            Value::from(message.origin_timestamp),
        ),
        (
            "iggy_ingested_at",
            Value::from(IggyTimestamp::now().as_millis() as i64),
        ),
    ];

    for (field, value) in fields {
        upsert_metadata_field(document, field, value);
    }

    if let Some(headers) = &message.headers
        && !headers.is_empty()
    {
        upsert_metadata_field(document, "iggy_headers", headers_to_json(headers));
    }
}

/// `HeaderKey`/`HeaderValue` are structs, not strings, so `serde_json::to_value`
/// on the map directly fails with "key must be a string", so the keys and raw
/// binary values are converted explicitly instead.
fn headers_to_json(headers: &BTreeMap<HeaderKey, HeaderValue>) -> Value {
    let map: Map<String, Value> = headers
        .iter()
        .map(|(key, value)| {
            // A per-kind shape would pin iggy_headers.<key> to text or object on
            // first use, so the other kind then fails to index under that key.
            let (data, encoding) = match value.as_raw() {
                Ok(raw) => (general_purpose::STANDARD.encode(raw), ENCODING_BASE64),
                Err(_) => (value.to_string_value(), ENCODING_UTF8),
            };
            (
                key.to_string_value(),
                json!({ "data": data, "data_encoding": encoding }),
            )
        })
        .collect();
    Value::Object(map)
}

fn upsert_metadata_field(document: &mut Map<String, Value>, field: &str, value: Value) {
    if document.insert(field.to_string(), value).is_some() {
        debug!("Overwriting payload field '{field}' with OpenSearch connector provenance");
    }
}

/// Stream and topic names can each reach `iggy_common::MAX_NAME_LENGTH`
/// (255), so encoding them into the ID verbatim can exceed
/// `MAX_DOCUMENT_ID_BYTES`. Hashing keeps the ID a fixed 69 bytes
/// (`GENERATED_ID_PREFIX` + 64 hex chars) regardless of input length, while
/// staying deterministic: same stream, topic, partition, offset, and message
/// ID always produce the same ID, preserving the upsert-based idempotency
/// this function exists for.
fn generated_document_id(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    offset: u64,
    message_id: u128,
) -> Result<String, Error> {
    let components = json!([
        topic_metadata.stream.as_str(),
        topic_metadata.topic.as_str(),
        messages_metadata.partition_id,
        offset,
        message_id.to_string()
    ]);
    let bytes = serde_json::to_vec(&components).map_err(|error| {
        Error::Serialization(format!(
            "Failed to serialize generated document ID: {error}"
        ))
    })?;
    Ok(format!("{GENERATED_ID_PREFIX}{}", calculate_256(&bytes)))
}

fn is_transient_error(error: &Error) -> bool {
    matches!(error, Error::HttpRequestFailed(_))
}

fn is_transient_client_error(error: &opensearch::Error) -> bool {
    if error.is_timeout() {
        return true;
    }
    match error.status_code() {
        Some(status) => is_transient_status(status),
        None => true,
    }
}

fn map_client_error(operation: &str, error: opensearch::Error) -> Error {
    if is_transient_client_error(&error) {
        Error::HttpRequestFailed(format!("OpenSearch {operation} failed: {error}"))
    } else {
        Error::PermanentHttpError(format!("OpenSearch {operation} failed: {error}"))
    }
}

fn map_status_error(operation: &str, status: StatusCode, body: &str) -> Error {
    let message = format!("OpenSearch {operation} failed with status {status}: {body}");
    if is_transient_status(status) {
        Error::HttpRequestFailed(message)
    } else {
        Error::PermanentHttpError(message)
    }
}

fn is_index_already_exists_error(body: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return false;
    };
    value
        .get("error")
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str)
        == Some(INDEX_ALREADY_EXISTS_ERROR)
}

fn trimmed_non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

// Only checks blankness, never trims: unlike a username, trimming a
// password would silently alter a secret that may legitimately contain
// leading/trailing whitespace.
fn is_blank_secret(value: &SecretString) -> bool {
    value.expose_secret().trim().is_empty()
}

fn normalize_url(raw: &str) -> Result<String, Error> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::Connection(
            "Invalid OpenSearch URL: host cannot be empty".to_string(),
        ));
    }

    // `://` (rather than a plain colon) is the marker of an explicit scheme,
    // since a bare `host:port` also contains a colon. Detected
    // case-insensitively so `HTTPS://host` isn't missed and mistaken for a
    // schemeless host, which would otherwise get `http://` prepended and
    // send the request to a bogus host built from the original string.
    let with_scheme = match trimmed.split_once("://") {
        Some((scheme, _))
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") =>
        {
            trimmed.to_string()
        }
        Some((scheme, _)) => {
            return Err(Error::Connection(format!(
                "Invalid OpenSearch URL: unsupported scheme '{scheme}', expected http or https"
            )));
        }
        None => format!("http://{trimmed}"),
    };
    let mut url = Url::parse(&with_scheme)
        .map_err(|error| Error::Connection(format!("Invalid OpenSearch URL: {error}")))?;
    // Rejected rather than silently honored: reqwest promotes URL userinfo
    // into a real `Authorization: Basic` header on every request, which
    // would bypass both `warn_if_credentials_use_insecure_http` (gated on
    // the dedicated `password` field) and the `SecretString` redaction the
    // `username`/`password` config fields get.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidConfigValue(
            "OpenSearch URL must not embed credentials; use the username/password config fields instead".to_string(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        warn!("Ignoring the query string and fragment on the OpenSearch URL");
    }
    url.set_query(None);
    url.set_fragment(None);

    // Kept, not stripped: the transport joins request paths onto it, so a proxy subpath works.
    if url.path() != "/" {
        warn!(
            "Using '{}' as the OpenSearch base path, so requests are sent to paths like '{}/_bulk'",
            url.path(),
            url.path().trim_end_matches('/')
        );
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

/// Textual, not `Url::parse`-based: a schemeless URL has no `//` authority for
/// `Url` to strip credentials from, so it would leave them untouched.
fn redact_url_credentials(raw: &str) -> String {
    let (scheme, rest) = raw.split_once("://").unwrap_or(("", raw));
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);

    if scheme.is_empty() {
        format!("{authority}{remainder}")
    } else {
        format!("{scheme}://{authority}{remainder}")
    }
}

/// The stripping below is unreachable from `open()`'s only call site: `normalize_url`
/// already rejects embedded credentials before a URL gets here. Kept as defense in
/// depth for any future caller that doesn't route through `normalize_url` first.
fn sanitize_url_for_log(normalized: &str) -> String {
    let Ok(mut url) = Url::parse(normalized) else {
        return "<invalid-url>".to_string();
    };

    if !url.username().is_empty() {
        let _ = url.set_username("");
    }
    if url.password().is_some() {
        let _ = url.set_password(None);
    }
    url.to_string().trim_end_matches('/').to_string()
}

fn warn_if_credentials_use_insecure_http(raw: &str, normalized: &str, has_password: bool) {
    if !has_password {
        return;
    }

    let Ok(url) = Url::parse(normalized) else {
        return;
    };
    if url.scheme() != "http" {
        return;
    }
    let Some(host) = url.host_str() else {
        return;
    };
    if is_loopback_host(host) {
        return;
    }

    let scheme_hint = if raw.trim().starts_with("http://") {
        "explicit http://"
    } else {
        "implicit http://"
    };
    warn!(
        "OpenSearch credentials are configured with {scheme_hint} for non-loopback host '{host}'. They will be sent without TLS; use https:// unless this is intentional."
    );
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;
    use std::sync::atomic::AtomicU32;
    use wiremock::matchers::{body_bytes, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    fn base_config() -> OpenSearchSinkConfig {
        OpenSearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            index: "iggy_messages".to_string(),
            ..Default::default()
        }
    }

    fn sink_with_config(config: OpenSearchSinkConfig) -> OpenSearchSink {
        OpenSearchSink::new(1, config)
    }

    fn expected_generated_id(message: &ConsumedMessage) -> String {
        generated_document_id(
            &topic_metadata(),
            &messages_metadata(),
            message.offset,
            message.id,
        )
        .expect("generated ID components should serialize")
    }

    #[test]
    fn given_empty_config_should_apply_documented_defaults() {
        let sink = sink_with_config(base_config());

        assert!(sink.config.create_index_if_not_exists);
        assert!(sink.config.include_metadata);
        assert_eq!(sink.config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(sink.config.timeout, Duration::from_secs(30));
        assert_eq!(sink.config.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(sink.config.retry_delay, Duration::from_millis(500));
        assert_eq!(sink.config.max_retry_delay, Duration::from_secs(5));
        assert_eq!(sink.config.max_open_retries, DEFAULT_MAX_OPEN_RETRIES);
        assert!(sink.config.refresh.is_none());
        assert!(sink.config.document_id_field.is_none());
    }

    #[test]
    fn given_zero_batch_size_should_clamp_to_one() {
        let mut config = base_config();
        config.batch_size = Some(0);

        assert_eq!(sink_with_config(config).config.batch_size, 1);
    }

    #[test]
    fn given_reversed_retry_delays_should_swap_them() {
        let mut config = base_config();
        config.retry_delay = Some("10s".to_string());
        config.max_retry_delay = Some("1s".to_string());

        let sink = sink_with_config(config);

        assert_eq!(sink.config.retry_delay, Duration::from_secs(1));
        assert_eq!(sink.config.max_retry_delay, Duration::from_secs(10));
    }

    #[test]
    fn given_blank_index_should_fail_validation() {
        let mut config = base_config();
        config.index = "   ".to_string();

        let error = sink_with_config(config)
            .validate_config()
            .expect_err("blank index should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_username_without_password_should_fail_validation() {
        let mut config = base_config();
        config.username = Some("admin".to_string());

        let error = sink_with_config(config)
            .validate_config()
            .expect_err("username without password should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_blank_password_with_username_should_fail_validation() {
        let mut config = base_config();
        config.username = Some("admin".to_string());
        config.password = Some(SecretString::from("   "));

        let error = sink_with_config(config)
            .validate_config()
            .expect_err("a whitespace-only password should be treated as unset");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_password_without_username_should_fail_validation() {
        let mut config = base_config();
        config.password = Some(SecretString::from("secret"));

        let error = sink_with_config(config)
            .validate_config()
            .expect_err("password without username should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_json_payload_should_inject_metadata_and_generated_id() {
        let sink = sink_with_config(base_config());
        let message = message(Payload::Json(simd_json::json!({ "name": "Alice" })));
        let expected_id = expected_generated_id(&message);

        let prepared = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document");

        assert_eq!(prepared.id, expected_id);
        assert_eq!(prepared.document["name"], "Alice");
        assert_eq!(prepared.document["iggy_offset"], 11);
        assert_eq!(prepared.document["iggy_stream"], "orders.stream");
        assert_eq!(prepared.document["iggy_topic"], "created/topic");
        assert_eq!(prepared.document["iggy_partition"], 7);
        assert_eq!(prepared.document["iggy_checksum"], "12");
        assert_eq!(prepared.document["iggy_message_id"], "42");
    }

    #[test]
    fn given_include_metadata_disabled_should_omit_provenance_fields() {
        let mut config = base_config();
        config.include_metadata = Some(false);
        let sink = sink_with_config(config);

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "name": "Alice" }))),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["name"], "Alice");
        assert!(prepared.document.get("iggy_offset").is_none());
        assert!(prepared.document.get("iggy_stream").is_none());
        assert!(prepared.id.starts_with(GENERATED_ID_PREFIX));
    }

    #[test]
    fn given_payload_reusing_metadata_names_should_overwrite_with_provenance() {
        let sink = sink_with_config(base_config());
        let payload = Payload::Json(simd_json::json!({
            "iggy_offset": 999,
            "iggy_stream": "spoofed",
            "iggy_checksum": 999
        }));

        let prepared = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message(payload))
            .expect("prepare document");

        assert_eq!(prepared.document["iggy_offset"], 11);
        assert_eq!(prepared.document["iggy_stream"], "orders.stream");
        assert_eq!(prepared.document["iggy_checksum"], "12");
    }

    #[test]
    fn given_non_object_json_should_wrap_in_value_field() {
        let sink = sink_with_config(base_config());

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!(["a", "b"]))),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["value"], json!(["a", "b"]));
    }

    #[test]
    fn given_raw_json_payload_should_index_parsed_document() {
        let sink = sink_with_config(base_config());
        let bytes = br#"{"name":"from-raw"}"#.to_vec();

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Raw(bytes)),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["name"], "from-raw");
    }

    #[test]
    fn given_raw_non_json_payload_should_base64_encode_original_bytes() {
        let sink = sink_with_config(base_config());

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Raw(vec![0, 1, 2, 3])),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["data"], "AAECAw==");
        assert_eq!(prepared.document["data_encoding"], ENCODING_BASE64);
        assert_eq!(prepared.document["data_type"], "raw");
    }

    #[test]
    fn given_truncated_json_raw_payload_should_preserve_original_bytes() {
        let sink = sink_with_config(base_config());
        let bytes = b"[1,2,3".to_vec();
        let expected = general_purpose::STANDARD.encode(&bytes);

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Raw(bytes)),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["data"], expected);
    }

    #[test]
    fn given_message_with_headers_should_index_iggy_headers_field() {
        let sink = sink_with_config(base_config());
        let mut message = message(Payload::Json(simd_json::json!({ "name": "Alice" })));
        message.headers = Some(BTreeMap::from([
            (
                HeaderKey::try_from("x-correlation-id").unwrap(),
                HeaderValue::try_from("abc-123").unwrap(),
            ),
            (
                HeaderKey::try_from("x-raw").unwrap(),
                HeaderValue::try_from([1u8, 2, 3].as_slice()).unwrap(),
            ),
        ]));

        let prepared = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document");

        let headers = &prepared.document["iggy_headers"];
        assert_eq!(headers["x-correlation-id"]["data"], "abc-123");
        assert_eq!(headers["x-correlation-id"]["data_encoding"], ENCODING_UTF8);
        assert_eq!(
            headers["x-raw"]["data"],
            general_purpose::STANDARD.encode([1u8, 2, 3])
        );
        assert_eq!(headers["x-raw"]["data_encoding"], ENCODING_BASE64);
    }

    // OpenSearch pins iggy_headers.<key> to whatever type the first document
    // uses, so a string header and a raw header must serialize to the same
    // shape or the second kind fails with a mapper_parsing_exception.
    #[test]
    fn given_string_and_raw_header_values_should_use_one_document_shape() {
        let headers = headers_to_json(&BTreeMap::from([
            (
                HeaderKey::try_from("x-text").unwrap(),
                HeaderValue::try_from("plain").unwrap(),
            ),
            (
                HeaderKey::try_from("x-number").unwrap(),
                HeaderValue::from(7u32),
            ),
            (
                HeaderKey::try_from("x-raw").unwrap(),
                HeaderValue::try_from([1u8, 2, 3].as_slice()).unwrap(),
            ),
        ]));

        let entries = headers.as_object().expect("headers should be an object");
        assert_eq!(entries.len(), 3);
        for (key, value) in entries {
            let entry = value
                .as_object()
                .unwrap_or_else(|| panic!("header '{key}' should be an object"));
            assert_eq!(
                entry.keys().map(String::as_str).collect::<Vec<_>>(),
                ["data", "data_encoding"],
                "header '{key}' has a divergent shape"
            );
            assert!(
                entry["data"].is_string(),
                "header '{key}' data is not a string"
            );
        }
        assert_eq!(entries["x-text"]["data_encoding"], ENCODING_UTF8);
        assert_eq!(entries["x-number"]["data_encoding"], ENCODING_UTF8);
        assert_eq!(entries["x-raw"]["data_encoding"], ENCODING_BASE64);
    }

    #[test]
    fn given_message_without_headers_should_omit_iggy_headers_field() {
        let sink = sink_with_config(base_config());

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "name": "Alice" }))),
            )
            .expect("prepare document");

        assert!(prepared.document.get("iggy_headers").is_none());
    }

    #[test]
    fn given_text_payload_should_index_text_field() {
        let sink = sink_with_config(base_config());

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Text("hello".to_string())),
            )
            .expect("prepare document");

        assert_eq!(prepared.document["text"], "hello");
        assert_eq!(prepared.document["data_type"], "text");
    }

    #[test]
    fn given_unsupported_payload_should_return_invalid_record() {
        let sink = sink_with_config(base_config());

        let error = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Avro(vec![1, 2, 3])),
            )
            .expect_err("unsupported payload should be rejected");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
    }

    #[test]
    fn given_document_id_field_should_use_payload_value_as_id() {
        let mut config = base_config();
        config.document_id_field = Some("order_id".to_string());
        let sink = sink_with_config(config);

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "order_id": "A-1" }))),
            )
            .expect("prepare document");

        assert_eq!(prepared.id, "A-1");
    }

    #[test]
    fn given_numeric_document_id_field_should_stringify_it() {
        let mut config = base_config();
        config.document_id_field = Some("order_id".to_string());
        let sink = sink_with_config(config);

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "order_id": 17 }))),
            )
            .expect("prepare document");

        assert_eq!(prepared.id, "17");
    }

    #[test]
    fn given_boolean_document_id_field_should_stringify_it() {
        let mut config = base_config();
        config.document_id_field = Some("archived".to_string());
        let sink = sink_with_config(config);

        let prepared = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "archived": true }))),
            )
            .expect("prepare document");

        assert_eq!(prepared.id, "true");
    }

    #[test]
    fn given_missing_document_id_field_should_fall_back_to_generated_id() {
        let mut config = base_config();
        config.document_id_field = Some("order_id".to_string());
        let sink = sink_with_config(config);
        let message = message(Payload::Json(simd_json::json!({ "name": "Alice" })));
        let expected_id = expected_generated_id(&message);

        let prepared = sink
            .prepare_document(&topic_metadata(), &messages_metadata(), message)
            .expect("prepare document");

        assert_eq!(prepared.id, expected_id);
    }

    #[test]
    fn given_oversized_document_id_field_should_return_invalid_record() {
        let mut config = base_config();
        config.document_id_field = Some("order_id".to_string());
        let sink = sink_with_config(config);
        let oversized = "x".repeat(MAX_DOCUMENT_ID_BYTES + 1);

        let error = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "order_id": oversized }))),
            )
            .expect_err("oversized document ID should be rejected");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
    }

    #[test]
    fn given_object_document_id_field_should_return_invalid_record() {
        let mut config = base_config();
        config.document_id_field = Some("order_id".to_string());
        let sink = sink_with_config(config);

        let error = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(
                    simd_json::json!({ "order_id": { "nested": true } }),
                )),
            )
            .expect_err("non-scalar document ID should be rejected");

        assert!(matches!(error, Error::InvalidRecordValue(_)));
    }

    // Bulk `index` upserts on a repeated `_id`, so a stable ID across replays
    // of the same offset is what makes redelivery idempotent.
    #[test]
    fn given_replayed_message_at_same_offset_should_prepare_same_document_id() {
        let sink = sink_with_config(base_config());

        let first = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "name": "Alice" }))),
            )
            .expect("prepare document");
        let second = sink
            .prepare_document(
                &topic_metadata(),
                &messages_metadata(),
                message(Payload::Json(simd_json::json!({ "name": "Alice" }))),
            )
            .expect("prepare document");

        assert_eq!(first.id, second.id);
        assert!(first.id.starts_with(GENERATED_ID_PREFIX));
    }

    #[test]
    fn given_same_message_should_generate_stable_id() {
        let first = expected_generated_id(&message(Payload::Text("x".to_string())));
        let second = expected_generated_id(&message(Payload::Text("different".to_string())));

        assert_eq!(first, second);
        assert!(first.starts_with(GENERATED_ID_PREFIX));
        assert!(
            first
                .chars()
                .all(|character| character.is_ascii_alphanumeric()
                    || character == '-'
                    || character == '_')
        );
        assert!(first.len() <= MAX_DOCUMENT_ID_BYTES);
    }

    #[test]
    fn given_separator_shuffled_names_should_not_collide() {
        let first = TopicMetadata {
            stream: "orders.stream".to_string(),
            topic: "created/topic".to_string(),
        };
        let second = TopicMetadata {
            stream: "orders/stream".to_string(),
            topic: "created.topic".to_string(),
        };

        let first_id = generated_document_id(&first, &messages_metadata(), 11, 42).unwrap();
        let second_id = generated_document_id(&second, &messages_metadata(), 11, 42).unwrap();

        assert_ne!(first_id, second_id);
    }

    #[test]
    fn given_max_u128_message_id_should_generate_id() {
        let id = generated_document_id(&topic_metadata(), &messages_metadata(), 11, u128::MAX)
            .expect("u128::MAX should serialize");

        assert!(id.starts_with(GENERATED_ID_PREFIX));
    }

    #[test]
    fn given_max_length_stream_and_topic_names_should_generate_id_within_byte_limit() {
        // iggy_common::MAX_NAME_LENGTH is 255; encoding two such names
        // verbatim (as the pre-hash implementation did) would exceed
        // MAX_DOCUMENT_ID_BYTES once base64-expanded. Hashing keeps the ID a
        // constant length regardless of input length.
        let topic_metadata = TopicMetadata {
            stream: "s".repeat(255),
            topic: "t".repeat(255),
        };

        let id = generated_document_id(&topic_metadata, &messages_metadata(), u64::MAX, u128::MAX)
            .expect("max-length names should serialize");

        assert!(id.len() <= MAX_DOCUMENT_ID_BYTES);
        assert!(id.starts_with(GENERATED_ID_PREFIX));
    }

    // The bulk-response fixtures below are verbatim captures from OpenSearch
    // 3.8.0; the shapes are measured, not assumed.

    #[test]
    fn given_clean_bulk_response_should_count_every_document_as_indexed() {
        let response = json!({
            "took": 4,
            "errors": false,
            "items": [
                { "index": { "_index": "iggy_probe", "_id": "iggy_abc", "_version": 1, "result": "created", "status": 201 } },
                { "index": { "_index": "iggy_probe", "_id": "iggy_def", "_version": 1, "result": "created", "status": 201 } }
            ]
        });

        let attempt = parse_bulk_response(&response, 2).expect("well-formed response");

        assert_eq!(
            attempt,
            BulkAttempt {
                indexed: 2,
                permanent_failed: 0,
                first_permanent_failure: None,
                retryable: Vec::new(),
                first_retryable_failure: None,
            }
        );
        assert!(attempt.into_outcome().into_error("iggy_probe").is_none());
    }

    // Unlike the `errors: false` fast path (see the test above), this forces the
    // per-item scan by mixing a replay with a fresh insert under `errors: true`,
    // proving that scan also ignores `result` and counts both as indexed.
    #[test]
    fn given_replayed_bulk_response_should_count_updates_as_indexed() {
        let response = json!({
            "took": 3,
            "errors": true,
            "items": [
                { "index": { "_id": "iggy_abc", "_version": 2, "result": "updated", "status": 200 } },
                { "index": { "_id": "iggy_def", "_version": 1, "result": "created", "status": 201 } }
            ]
        });

        assert_eq!(
            parse_bulk_response(&response, 2)
                .expect("well-formed response")
                .indexed,
            2
        );
    }

    #[test]
    fn given_mixed_bulk_response_should_account_per_item() {
        let response = json!({
            "took": 6,
            "errors": true,
            "items": [
                { "index": {
                    "_id": "iggy_bad",
                    "status": 400,
                    "error": {
                        "type": "mapper_parsing_exception",
                        "reason": "failed to parse field [count] of type [integer] in document with id 'iggy_bad'. Preview of field's value: 'not-an-integer'"
                    }
                } },
                { "index": { "_id": "iggy_ok", "_version": 1, "result": "created", "status": 201 } }
            ]
        });

        let attempt = parse_bulk_response(&response, 2).expect("well-formed response");

        assert_eq!(attempt.indexed, 1);
        assert_eq!(attempt.permanent_failed, 1);
        assert!(attempt.retryable.is_empty());
        assert!(
            attempt
                .first_permanent_failure
                .as_deref()
                .is_some_and(|failure| failure.contains("mapper_parsing_exception")
                    || failure.contains("failed to parse field"))
        );
    }

    #[test]
    fn given_mapper_parsing_failure_should_map_to_permanent_error() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "status": 400, "error": { "type": "mapper_parsing_exception", "reason": "bad field" } } }
            ]
        });

        let error = parse_bulk_response(&response, 1)
            .expect("well-formed response")
            .into_outcome()
            .into_error("iggy_probe")
            .expect("failed items should produce an error");

        assert!(matches!(error, Error::PermanentHttpError(_)));
    }

    #[test]
    fn given_rejected_execution_should_map_to_transient_error() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "status": 429, "error": { "type": "es_rejected_execution_exception", "reason": "queue full" } } }
            ]
        });

        let error = parse_bulk_response(&response, 1)
            .expect("well-formed response")
            .into_outcome()
            .into_error("iggy_probe")
            .expect("failed items should produce an error");

        assert!(matches!(error, Error::HttpRequestFailed(_)));
    }

    #[test]
    fn given_any_transient_item_failure_should_prefer_retryable_error() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "status": 400, "error": { "type": "mapper_parsing_exception", "reason": "bad field" } } },
                { "index": { "status": 503, "error": { "type": "unavailable_shards_exception", "reason": "primary shard unavailable" } } }
            ]
        });

        let outcome = parse_bulk_response(&response, 2)
            .expect("well-formed response")
            .into_outcome();

        assert_eq!(outcome.failed, 2);
        assert!(outcome.transient);
        assert!(matches!(
            outcome.into_error("iggy_probe"),
            Some(Error::HttpRequestFailed(_))
        ));
    }

    #[test]
    fn given_merged_outcomes_should_accumulate_totals_and_keep_first_failure() {
        let mut outcome = BulkOutcome {
            indexed: 2,
            failed: 1,
            transient: false,
            first_failure: Some("first".to_string()),
        };

        outcome.merge(BulkOutcome {
            indexed: 3,
            failed: 4,
            transient: true,
            first_failure: Some("second".to_string()),
        });

        assert_eq!(
            outcome,
            BulkOutcome {
                indexed: 5,
                failed: 5,
                transient: true,
                first_failure: Some("first".to_string()),
            }
        );
    }

    #[test]
    fn given_merge_into_clean_outcome_should_adopt_the_incoming_failure() {
        let mut outcome = BulkOutcome::default();

        outcome.merge(BulkOutcome {
            indexed: 1,
            failed: 1,
            transient: false,
            first_failure: Some("only".to_string()),
        });
        outcome.merge(BulkOutcome::default());

        assert_eq!(outcome.first_failure.as_deref(), Some("only"));
        assert_eq!(outcome.indexed, 1);
        assert_eq!(outcome.failed, 1);
    }

    #[test]
    fn given_bulk_response_without_items_should_be_rejected_as_unparsable() {
        assert!(matches!(
            parse_bulk_response(&json!({}), 1),
            Err(BulkResponseError::MissingItems)
        ));
    }

    #[test]
    fn given_bulk_response_with_short_items_array_should_be_rejected_as_unparsable() {
        let response = json!({
            "errors": false,
            "items": [
                { "index": { "_id": "a", "status": 201, "result": "created" } }
            ]
        });

        assert!(matches!(
            parse_bulk_response(&response, 2),
            Err(BulkResponseError::ItemCountMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn given_bulk_response_with_malformed_item_should_be_rejected_as_unparsable() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "_id": "a", "status": 201, "result": "created" } },
                {}
            ]
        });

        assert!(matches!(
            parse_bulk_response(&response, 2),
            Err(BulkResponseError::MalformedItem { position: 1 })
        ));
    }

    #[test]
    fn given_clean_flag_bulk_response_with_malformed_item_should_be_rejected_as_unparsable() {
        let response = json!({
            "errors": false,
            "items": [{}]
        });

        assert!(matches!(
            parse_bulk_response(&response, 1),
            Err(BulkResponseError::MalformedItem { position: 0 })
        ));
    }

    #[test]
    fn given_transient_item_failures_should_report_their_positions_for_retry() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "_id": "a", "_version": 1, "result": "created", "status": 201 } },
                { "index": { "_id": "b", "status": 429, "error": { "type": "es_rejected_execution_exception", "reason": "queue full" } } },
                { "index": { "_id": "c", "status": 400, "error": { "type": "mapper_parsing_exception", "reason": "bad field" } } },
                { "index": { "_id": "d", "status": 503, "error": { "type": "unavailable_shards_exception", "reason": "primary shard unavailable" } } }
            ]
        });

        let attempt = parse_bulk_response(&response, 4).expect("well-formed response");

        assert_eq!(attempt.indexed, 1);
        assert_eq!(attempt.permanent_failed, 1);
        assert_eq!(attempt.retryable, vec![1, 3]);
    }

    #[test]
    fn given_only_permanent_item_failures_should_have_nothing_to_retry() {
        let response = json!({
            "errors": true,
            "items": [
                { "index": { "status": 400, "error": { "type": "mapper_parsing_exception", "reason": "bad field" } } }
            ]
        });

        assert!(
            parse_bulk_response(&response, 1)
                .expect("well-formed response")
                .retryable
                .is_empty()
        );
    }

    fn prepared(id: &str) -> PreparedDocument {
        PreparedDocument {
            id: id.to_string(),
            document: json!({ "id": id }),
        }
    }

    #[test]
    fn given_retryable_positions_should_select_matching_documents() {
        let documents = [prepared("a"), prepared("b"), prepared("c")];
        let pending: Vec<&PreparedDocument> = documents.iter().collect();

        let selected = documents_at(&pending, &[0, 2]);

        assert_eq!(
            selected
                .iter()
                .map(|document| document.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
    }

    // A response echoing more items than were sent must not panic: a panic
    // crossing the plugin's FFI boundary aborts every connector in the runtime.
    #[test]
    fn given_out_of_range_positions_should_drop_them_instead_of_panicking() {
        let documents = [prepared("a")];
        let pending: Vec<&PreparedDocument> = documents.iter().collect();

        let selected = documents_at(&pending, &[0, 7]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "a");
        assert!(documents_at(&pending, &[3, 4]).is_empty());
    }

    fn fast_retry_config(max_open_retries: u32) -> OpenSearchSinkConfig {
        let mut config = base_config();
        config.max_open_retries = Some(max_open_retries);
        config.retry_delay = Some("1ms".to_string());
        config.max_retry_delay = Some("2ms".to_string());
        config
    }

    #[tokio::test]
    async fn given_transient_open_failure_should_retry_until_success() {
        let sink = sink_with_config(fast_retry_config(5));
        let attempts = AtomicU32::new(0);

        let result = sink
            .retry_on_open("index existence check", || async {
                if attempts.fetch_add(1, Ordering::Relaxed) < 2 {
                    return Err(Error::HttpRequestFailed("shard not ready".to_string()));
                }
                Ok(true)
            })
            .await;

        assert!(result.expect("third attempt should succeed"));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn given_permanent_open_failure_should_not_retry() {
        let sink = sink_with_config(fast_retry_config(5));
        let attempts = AtomicU32::new(0);

        let error = sink
            .retry_on_open("index creation", || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err::<(), Error>(Error::PermanentHttpError("invalid mapping".to_string()))
            })
            .await
            .expect_err("a permanent error should surface immediately");

        assert!(matches!(error, Error::PermanentHttpError(_)));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn given_exhausted_open_retries_should_report_init_error() {
        let sink = sink_with_config(fast_retry_config(2));
        let attempts = AtomicU32::new(0);

        let error = sink
            .retry_on_open("health check", || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err::<(), Error>(Error::HttpRequestFailed("connection refused".to_string()))
            })
            .await
            .expect_err("exhausted retries should fail");

        assert!(matches!(error, Error::InitError(_)));
        // The initial attempt plus max_open_retries retries.
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    // The invocation counter is bumped before the client is looked up, so a
    // batch that never reaches OpenSearch still counts as an invocation.
    #[tokio::test]
    async fn given_unopened_sink_when_consuming_should_count_invocation_and_fail() {
        let sink = sink_with_config(base_config());

        let error = sink
            .consume(&topic_metadata(), messages_metadata(), Vec::new())
            .await
            .expect_err("consuming without an open client should fail");

        assert!(matches!(error, Error::Connection(_)));
        assert_eq!(sink.invocations_count.load(Ordering::Relaxed), 1);
        assert_eq!(sink.documents_indexed.load(Ordering::Relaxed), 0);
        assert_eq!(sink.errors_count.load(Ordering::Relaxed), 0);
    }

    async fn mock_health_sink(server: &MockServer, base_path: &str) -> OpenSearchSink {
        let mut config = fast_retry_config(1);
        config.url = format!("{}{base_path}", server.uri());
        sink_with_config(config)
    }

    fn mock_client(sink: &OpenSearchSink) -> OpenSearch {
        let normalized = normalize_url(&sink.config.url).expect("normalize");
        sink.create_client(&normalized).expect("build client")
    }

    async fn mock_bulk_sink(server: &MockServer, max_retries: u32) -> OpenSearchSink {
        let mut config = fast_retry_config(1);
        config.url = server.uri();
        config.max_retries = Some(max_retries);
        sink_with_config(config)
    }

    // An index-scoped ingest user is not normally granted the cluster-scoped
    // cluster:monitor/health privilege, and a 403 still proves the cluster
    // answered and the credentials authenticated.
    #[tokio::test]
    async fn given_forbidden_health_check_should_treat_cluster_as_reachable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_cluster/health"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "error": {
                    "type": "security_exception",
                    "reason": "no permissions for [cluster:monitor/health]"
                },
                "status": 403
            })))
            .expect(1)
            .mount(&server)
            .await;
        let sink = mock_health_sink(&server, "").await;
        let client = mock_client(&sink);

        sink.check_connectivity(&client)
            .await
            .expect("a denied health check should not fail open()");
    }

    #[tokio::test]
    async fn given_unauthorized_health_check_should_fail_open() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_cluster/health"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let sink = mock_health_sink(&server, "").await;
        let client = mock_client(&sink);

        let error = sink
            .check_connectivity(&client)
            .await
            .expect_err("rejected credentials should still fail open()");

        assert!(matches!(error, Error::PermanentHttpError(_)));
    }

    // Proves the preserved base path reaches the wire, not just normalize_url:
    // an unmatched request would land on the mock's 404 and fail this.
    #[tokio::test]
    async fn given_base_path_url_should_send_requests_under_that_path() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/opensearch/_cluster/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "green" })))
            .expect(1)
            .mount(&server)
            .await;
        let sink = mock_health_sink(&server, "/opensearch").await;
        let client = mock_client(&sink);

        sink.check_connectivity(&client)
            .await
            .expect("health check should resolve under the base path");
    }

    #[tokio::test]
    async fn given_transient_item_failure_should_retry_only_rejected_documents() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 2).await;
        let client = mock_client(&sink);
        let documents = [prepared("a"), prepared("b")];
        let full_body = build_bulk_body(&sink.config.index, &[&documents[0], &documents[1]])
            .expect("build initial body");
        let retry_body =
            build_bulk_body(&sink.config.index, &[&documents[1]]).expect("build retry body");

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(full_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": true,
                "items": [
                    { "index": { "_id": "a", "status": 201, "result": "created" } },
                    { "index": { "_id": "b", "status": 429, "error": { "type": "es_rejected_execution_exception", "reason": "queue full" } } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(retry_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "b", "status": 200, "result": "updated" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("bulk retry should eventually succeed");

        assert_eq!(outcome.indexed, 2);
        assert_eq!(outcome.failed, 0);
    }

    #[tokio::test]
    async fn given_transient_bulk_status_should_resend_full_chunk() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 2).await;
        let client = mock_client(&sink);
        let documents = [prepared("a"), prepared("b")];
        let full_body = build_bulk_body(&sink.config.index, &[&documents[0], &documents[1]])
            .expect("build body");

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(full_body.clone()))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(full_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "a", "status": 201, "result": "created" } },
                    { "index": { "_id": "b", "status": 201, "result": "created" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("resending the identical body should eventually succeed");

        assert_eq!(outcome.indexed, 2);
        assert_eq!(outcome.failed, 0);
    }

    #[tokio::test]
    async fn given_bulk_retries_exhausted_should_return_ok_with_partial_outcome() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 1).await;
        let client = mock_client(&sink);
        let documents = [prepared("a")];

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2) // the initial attempt plus one retry (max_retries = 1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("exhausted retries should report Ok with a transient failure, not Err");

        assert_eq!(outcome.indexed, 0);
        assert_eq!(outcome.failed, 1);
        assert!(outcome.transient);
    }

    #[tokio::test]
    async fn given_permanent_bulk_status_should_fail_without_retry() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 3).await;
        let client = mock_client(&sink);
        let documents = [prepared("a")];

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": { "type": "illegal_argument_exception", "reason": "malformed request" }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("a permanent bulk status is reported through the outcome, not Err");

        assert_eq!(outcome.indexed, 0);
        assert_eq!(outcome.failed, 1);
        assert!(!outcome.transient);
        let error = outcome
            .into_error(&sink.config.index)
            .expect("a failed outcome must produce an error");
        assert!(matches!(error, Error::PermanentHttpError(_)));
    }

    // Regression test: attempt 1 partially succeeds (document `a` indexed,
    // `b` rejected transiently) and only the retry of `b` hits a permanent
    // whole-request status. `a`'s success must survive into the final
    // outcome instead of being discarded by the early `Err` return.
    #[tokio::test]
    async fn given_permanent_bulk_status_after_partial_retry_should_keep_earlier_indexed_count() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 2).await;
        let client = mock_client(&sink);
        let documents = [prepared("a"), prepared("b")];
        let full_body = build_bulk_body(&sink.config.index, &[&documents[0], &documents[1]])
            .expect("build initial body");
        let retry_body =
            build_bulk_body(&sink.config.index, &[&documents[1]]).expect("build retry body");

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(full_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": true,
                "items": [
                    { "index": { "_id": "a", "status": 201, "result": "created" } },
                    { "index": { "_id": "b", "status": 429, "error": { "type": "es_rejected_execution_exception", "reason": "queue full" } } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(retry_body))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("a permanent failure on retry is reported through the outcome, not Err");

        assert_eq!(
            outcome.indexed, 1,
            "document `a` from the first attempt must still count as indexed"
        );
        assert_eq!(outcome.failed, 1);
        assert!(!outcome.transient);
    }

    #[tokio::test]
    async fn given_unparsable_bulk_response_body_should_be_retried_as_transient() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 1).await;
        let client = mock_client(&sink);
        let documents = [prepared("a")];

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("not json", "application/json"))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "a", "status": 201, "result": "created" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("an unparsable 200 body should be retried, not hard-failed");

        assert_eq!(outcome.indexed, 1);
        assert_eq!(outcome.failed, 0);
    }

    // A missing `items` array must never be read as zero failures: a chunk
    // OpenSearch didn't fully account for must not be treated as cleanly
    // handled, it must be retried instead.
    #[tokio::test]
    async fn given_bulk_response_missing_items_should_be_retried_as_transient() {
        let server = MockServer::start().await;
        let sink = mock_bulk_sink(&server, 1).await;
        let client = mock_client(&sink);
        let documents = [prepared("a")];

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "a", "status": 201, "result": "created" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let outcome = sink
            .index_chunk(&client, &documents)
            .await
            .expect("a response missing `items` should be retried, not silently dropped");

        assert_eq!(outcome.indexed, 1);
        assert_eq!(outcome.failed, 0);
    }

    #[tokio::test]
    async fn given_batch_larger_than_batch_size_should_split_into_multiple_bulk_calls() {
        let server = MockServer::start().await;
        let mut config = fast_retry_config(1);
        config.url = server.uri();
        config.batch_size = Some(2);
        let sink = sink_with_config(config);
        let client = mock_client(&sink);
        let documents: Vec<PreparedDocument> = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(prepared)
            .collect();

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(|request: &wiremock::Request| {
                let sent_documents = std::str::from_utf8(&request.body)
                    .expect("bulk body should be valid utf8")
                    .lines()
                    .filter(|line| !line.is_empty())
                    .count()
                    / 2;
                let items = vec![
                    json!({ "index": { "status": 201, "result": "created" } });
                    sent_documents
                ];
                ResponseTemplate::new(200).set_body_json(json!({ "errors": false, "items": items }))
            })
            .expect(3) // chunks of [2, 2, 1] for 5 documents at batch_size = 2
            .mount(&server)
            .await;

        let indexed = sink
            .index_documents(&client, documents)
            .await
            .expect("every chunk should be attempted and counted");

        assert_eq!(indexed, 5);
    }

    // Regression test for the specific claim the real-infra
    // `given_missing_index_and_mapping_conflict_should_isolate_failures_from_healthy_sibling`
    // integration test documents but cannot itself verify: a chunk failing
    // must not stop `index_documents` from attempting the chunks queued
    // behind it. Real OpenSearch's own per-item bulk semantics mean the
    // final document count there is identical whether chunking happened or
    // the whole batch went out in one `_bulk` call, so only a mocked
    // per-chunk response (asserted here via wiremock's per-mock `.expect(1)`,
    // which panics on drop if a mock is never hit) can prove every chunk was
    // actually sent.
    #[tokio::test]
    async fn given_permanently_failing_chunk_should_not_abandon_later_chunks() {
        let server = MockServer::start().await;
        let mut config = fast_retry_config(1);
        config.url = server.uri();
        config.batch_size = Some(2);
        let sink = sink_with_config(config);
        let client = mock_client(&sink);
        let documents: Vec<PreparedDocument> = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(prepared)
            .collect();
        let chunk1_body = build_bulk_body(&sink.config.index, &[&documents[0], &documents[1]])
            .expect("build chunk 1 body");
        let chunk2_body = build_bulk_body(&sink.config.index, &[&documents[2], &documents[3]])
            .expect("build chunk 2 body");
        let chunk3_body =
            build_bulk_body(&sink.config.index, &[&documents[4]]).expect("build chunk 3 body");

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(chunk1_body))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": { "type": "illegal_argument_exception", "reason": "malformed request" }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(chunk2_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "c", "status": 201, "result": "created" } },
                    { "index": { "_id": "d", "status": 201, "result": "created" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_bytes(chunk3_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "errors": false,
                "items": [
                    { "index": { "_id": "e", "status": 201, "result": "created" } }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let error = sink
            .index_documents(&client, documents)
            .await
            .expect_err("a permanently failing first chunk should still surface an error");

        assert_eq!(
            error.indexed, 3,
            "documents from the two chunks after the failing one must still be indexed"
        );
        assert_eq!(error.failed, 2);
    }

    #[tokio::test]
    async fn given_concurrent_index_creation_should_not_fail_open() {
        let server = MockServer::start().await;
        let sink = mock_health_sink(&server, "").await;
        let client = mock_client(&sink);

        Mock::given(method("PUT"))
            .and(path(format!("/{}", sink.config.index)))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": {
                    "type": "resource_already_exists_exception",
                    "reason": "index [iggy_messages/abc] already exists"
                },
                "status": 400
            })))
            .expect(1)
            .mount(&server)
            .await;

        sink.create_index(&client)
            .await
            .expect("another instance winning the create race should not fail open()");
    }

    #[test]
    fn given_error_variants_should_classify_open_time_retryability() {
        assert!(is_transient_error(&Error::HttpRequestFailed(
            "503".to_string()
        )));
        assert!(!is_transient_error(&Error::PermanentHttpError(
            "400".to_string()
        )));
        assert!(!is_transient_error(&Error::InitError(
            "missing".to_string()
        )));
    }

    #[test]
    fn given_credentials_in_url_should_redact_them_from_logs() {
        assert_eq!(
            sanitize_url_for_log("https://admin:hunter2@opensearch.example.com:9200/path"),
            "https://opensearch.example.com:9200/path"
        );
    }

    #[test]
    fn given_url_without_scheme_should_default_to_http() {
        assert_eq!(
            normalize_url("localhost:9200").expect("normalize"),
            "http://localhost:9200"
        );
    }

    #[test]
    fn given_uppercase_scheme_should_be_recognized_not_double_prefixed() {
        assert_eq!(
            normalize_url("HTTPS://localhost:9200").expect("normalize"),
            "https://localhost:9200"
        );
    }

    #[test]
    fn given_unsupported_scheme_should_fail_normalization() {
        let error =
            normalize_url("ftp://localhost:9200").expect_err("ftp is not a supported scheme");

        assert!(matches!(error, Error::Connection(_)));
        assert!(format!("{error}").contains("ftp"));
    }

    #[test]
    fn given_url_with_query_and_fragment_should_strip_them_and_keep_the_base_path() {
        assert_eq!(
            normalize_url("https://localhost:9200/opensearch?foo=bar#section").expect("normalize"),
            "https://localhost:9200/opensearch"
        );
    }

    // A reverse proxy exposing OpenSearch under a subpath is a supported
    // topology: the transport joins every request path onto this base.
    #[test]
    fn given_url_with_base_path_should_preserve_it() {
        assert_eq!(
            normalize_url("https://proxy.example.com/opensearch/").expect("normalize"),
            "https://proxy.example.com/opensearch"
        );
    }

    #[test]
    fn given_root_url_should_normalize_to_bare_origin() {
        assert_eq!(
            normalize_url("https://localhost:9200/").expect("normalize"),
            "https://localhost:9200"
        );
    }

    #[test]
    fn given_blank_url_should_fail_normalization() {
        assert!(matches!(normalize_url("   "), Err(Error::Connection(_))));
    }

    #[test]
    fn given_malformed_url_should_fail_normalization() {
        let error = normalize_url("http://[::1")
            .expect_err("an unterminated IPv6 literal should not parse");

        assert!(matches!(error, Error::Connection(_)));
    }

    #[test]
    fn given_credentials_embedded_in_url_should_fail_normalization() {
        let error = normalize_url("https://admin:hunter2@opensearch.example.com:9200")
            .expect_err("embedded credentials should be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_username_only_embedded_in_url_should_fail_normalization() {
        let error = normalize_url("https://admin@opensearch.example.com:9200")
            .expect_err("embedded username without a password should still be rejected");

        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_loopback_hosts_should_be_detected() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("opensearch.prod"));
    }

    #[test]
    fn given_refresh_values_should_match_opensearch_wire_format() {
        assert_eq!(serde_json::to_string(&Refresh::True).unwrap(), "\"true\"");
        assert_eq!(serde_json::to_string(&Refresh::False).unwrap(), "\"false\"");
        assert_eq!(
            serde_json::to_string(&Refresh::WaitFor).unwrap(),
            "\"wait_for\""
        );
    }

    #[test]
    fn given_debug_formatted_config_should_redact_password() {
        let mut config = base_config();
        config.username = Some("admin".to_string());
        config.password = Some(SecretString::from("hunter2"));

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("admin"), "{rendered}");
    }

    #[test]
    fn given_debug_formatted_sink_should_redact_password() {
        let mut config = base_config();
        config.username = Some("admin".to_string());
        config.password = Some(SecretString::from("hunter2"));

        let rendered = format!("{:?}", sink_with_config(config));

        assert!(!rendered.contains("hunter2"), "{rendered}");
    }

    // Regression test: formatting an unopened sink never touches `client`
    // (it's `None`), so it can't catch a leak coming from inside the
    // `OpenSearch` client itself. Install a real authenticated client, as
    // `open()` would, before formatting.
    #[test]
    fn given_debug_formatted_sink_with_open_client_should_redact_password() {
        let mut config = base_config();
        config.username = Some("admin".to_string());
        config.password = Some(SecretString::from("hunter2"));
        let mut sink = sink_with_config(config);
        sink.client = Some(mock_client(&sink));

        let rendered = format!("{sink:?}");

        assert!(!rendered.contains("hunter2"), "{rendered}");
    }

    // Regression test: Debug can run before open() ever validates the URL.
    #[test]
    fn given_debug_formatted_sink_with_url_credentials_should_redact_them() {
        let mut config = base_config();
        config.url = "https://admin:hunter2@opensearch.example.com:9200".to_string();

        let rendered = format!("{:?}", sink_with_config(config));

        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("opensearch.example.com"), "{rendered}");
    }

    #[test]
    fn given_url_without_credentials_should_be_unchanged() {
        assert_eq!(
            redact_url_credentials("https://opensearch.example.com:9200/path"),
            "https://opensearch.example.com:9200/path"
        );
    }

    #[test]
    fn given_url_with_credentials_should_redact_them() {
        assert_eq!(
            redact_url_credentials("https://admin:hunter2@opensearch.example.com:9200/path"),
            "https://opensearch.example.com:9200/path"
        );
    }

    #[test]
    fn given_schemeless_url_with_credentials_should_redact_them() {
        assert_eq!(
            redact_url_credentials("admin:hunter2@opensearch.example.com:9200"),
            "opensearch.example.com:9200"
        );
    }

    #[test]
    fn given_toml_config_should_deserialize_password_into_secret() {
        let config: OpenSearchSinkConfig = toml::from_str(
            r#"
            url = "http://localhost:9200"
            index = "iggy_messages"
            username = "admin"
            password = "hunter2"
            refresh = "wait_for"
            "#,
        )
        .expect("config should deserialize");

        assert_eq!(
            config
                .password
                .as_ref()
                .map(|password| password.expose_secret().to_string()),
            Some("hunter2".to_string())
        );
        assert_eq!(config.refresh, Some(Refresh::WaitFor));
    }
}
