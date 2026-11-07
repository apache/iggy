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
use iggy_common::{DateTime, HeaderKey, HeaderValue, Utc};
use iggy_connector_sdk::retry::{CircuitBreaker, RetryPolicy, parse_duration, retry_async};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use opensearch::{
    OpenSearch, SearchParts,
    auth::Credentials,
    http::{StatusCode, Url, transport::TransportBuilder},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

mod retry;
use crate::retry::{
    DEFAULT_CB_COOL_DOWN, DEFAULT_CB_THRESHOLD, DEFAULT_MAX_OPEN_RETRIES, DEFAULT_MAX_RETRIES,
    DEFAULT_OPEN_RETRY_MAX_DELAY, DEFAULT_RETRY_DELAY, DEFAULT_RETRY_MAX_DELAY, RequestFailure,
};

source_connector!(OpenSearchSource);

const CONNECTOR_NAME: &str = "OpenSearch source";
const DEFAULT_POLLING_INTERVAL: &str = "10s";
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_REQUEST_TIMEOUT: &str = "30s";
const DEFAULT_TIEBREAKER_FIELD: &str = "_id";
// Sort clause length: timestamp, tie-breaker, `_index`.
const SORT_KEY_COUNT: usize = 3;
const TIEBREAKER_SORT_POSITION: usize = 1;
// Bounds the persisted state when many documents share the newest timestamp.
const MAX_WATERMARK_HITS: usize = 10_000;
// OpenSearch rejects `size` above the default `index.max_result_window` with HTTP 400.
const MAX_BATCH_SIZE: usize = 10_000;
const DOCUMENT_ID_HEADER: &str = "opensearch.document_id";
const INDEX_HEADER: &str = "opensearch.index";
const MAX_HEADER_VALUE_LENGTH: usize = 255;
// What OpenSearch reports for a document missing the sort field when it sorts last. The sort
// asks for missing values first, so seeing this means that request option was not honored.
const MISSING_LAST_SORT_VALUE: i64 = i64::MAX;

static DOCUMENT_ID_HEADER_KEY: std::sync::LazyLock<HeaderKey> = std::sync::LazyLock::new(|| {
    HeaderKey::try_from(DOCUMENT_ID_HEADER).expect("document id header key is a valid literal")
});
static INDEX_HEADER_KEY: std::sync::LazyLock<HeaderKey> = std::sync::LazyLock::new(|| {
    HeaderKey::try_from(INDEX_HEADER).expect("index header key is a valid literal")
});

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    #[serde(default)]
    last_poll_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    total_documents_published: usize,
    #[serde(default)]
    poll_count: usize,
    /// Sort tuple of the newest published hit. Its first element is the watermark: the next
    /// scan starts there instead of after the tuple, so a hit that became searchable late is
    /// still found.
    #[serde(default)]
    last_sort: Option<Vec<Value>>,
    #[serde(default)]
    error_count: usize,
    /// Hits published at the watermark, skipped when the next scan reads them again. The
    /// state is MessagePack without field names, so new fields must stay at the end.
    #[serde(default)]
    watermark_hits: Vec<HitKey>,
    /// Identifies the index, timestamp field and query the cursor belongs to. A cursor from
    /// other settings would skip documents, so `open()` refuses to start on a mismatch.
    #[serde(default)]
    config_fingerprint: Option<String>,
    /// Page cursor of the running scan. Not persisted: a restart rescans from the watermark.
    #[serde(skip)]
    scan_cursor: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct HitKey {
    index: String,
    id: String,
}

// `Deserialize` only: nothing re-serializes a plugin config, and omitting `Serialize`
// keeps `password` from ever being written out as plaintext.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenSearchSourceConfig {
    pub url: String,
    pub index: String,
    pub username: Option<String>,
    pub password: Option<SecretString>,
    pub query: Option<Value>,
    pub polling_interval: Option<String>,
    pub batch_size: Option<usize>,
    pub timestamp_field: Option<String>,
    /// Unique `keyword` field with doc values that breaks timestamp ties. Defaults to `_id`,
    /// which OpenSearch sorts through heap-backed fielddata.
    pub tiebreaker_field: Option<String>,
    /// Limit per HTTP request, from connecting to the last byte of the response body.
    pub request_timeout: Option<String>,
    #[serde(default)]
    pub verbose_logging: bool,
    /// Total poll-phase attempt count (not retry count); 1 = no retries, 3 = 2 retries.
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_max_delay: Option<String>,
    /// Total open-phase attempt count (not retry count); 1 = no retries, 5 = 4 retries.
    pub max_open_retries: Option<u32>,
    pub open_retry_max_delay: Option<String>,
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug)]
pub struct OpenSearchSource {
    id: u32,
    config: OpenSearchSourceConfig,
    client: Option<OpenSearch>,
    polling_interval: Duration,
    request_timeout: Duration,
    /// Pre-built query body (query + size + sort). Set once in `open()` after config validation.
    /// `None` before `open()` and after a failed `open()`.
    search_body_base: Option<Value>,
    verbose: bool,
    poll_retry_policy: RetryPolicy,
    open_retry_policy: RetryPolicy,
    circuit_breaker: CircuitBreaker,
    state: Mutex<State>,
    /// Candidate state of the batch in flight; committed on ACK, dropped on NACK.
    pending_state: Mutex<Option<State>>,
    /// `Some(cause)` when runtime state restore was rejected; `None` means restore succeeded.
    state_restore_error: Option<String>,
}

struct SearchOutcome {
    messages: Vec<ProducedMessage>,
    /// `None` when the scan found nothing new to publish or skip.
    last_sort: Option<Vec<Value>>,
    watermark_hits: Vec<HitKey>,
    scan_cursor: Option<Vec<Value>>,
    last_poll_timestamp: Option<DateTime<Utc>>,
}

struct Page {
    messages: Vec<ProducedMessage>,
    sorts: Vec<Vec<Value>>,
    keys: Vec<HitKey>,
    /// Positions in `sorts` and `keys` of hits that were not published before.
    fresh: Vec<usize>,
    last_poll_timestamp: Option<DateTime<Utc>>,
    full: bool,
}

impl OpenSearchSource {
    pub fn new(id: u32, config: OpenSearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval =
            parse_duration(config.polling_interval.as_deref(), DEFAULT_POLLING_INTERVAL);
        let request_timeout =
            parse_duration(config.request_timeout.as_deref(), DEFAULT_REQUEST_TIMEOUT);
        let verbose = config.verbose_logging;
        let (restored_state, state_restore_error) =
            restore_state(id, state, &config_fingerprint(&config));

        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CB_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CB_COOL_DOWN,
        );
        let circuit_breaker = CircuitBreaker::new(cb_threshold, cb_cool_down);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let poll_retry_policy = RetryPolicy {
            max_attempts: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            base_delay: retry_delay,
            max_delay: parse_duration(config.retry_max_delay.as_deref(), DEFAULT_RETRY_MAX_DELAY),
        };
        // The open phase has no base delay of its own: it shares `retry_delay` and only
        // `open_retry_max_delay` caps the growth independently.
        let open_retry_policy = RetryPolicy {
            max_attempts: config.max_open_retries.unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
            base_delay: retry_delay,
            max_delay: parse_duration(
                config.open_retry_max_delay.as_deref(),
                DEFAULT_OPEN_RETRY_MAX_DELAY,
            ),
        };

        OpenSearchSource {
            id,
            config,
            poll_retry_policy,
            open_retry_policy,
            circuit_breaker,
            client: None,
            polling_interval,
            request_timeout,
            search_body_base: None,
            verbose,
            state: Mutex::new(restored_state),
            pending_state: Mutex::new(None),
            state_restore_error,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE)
    }

    fn tiebreaker_field(&self) -> &str {
        self.config
            .tiebreaker_field
            .as_deref()
            .unwrap_or(DEFAULT_TIEBREAKER_FIELD)
    }

    fn timestamp_field(&self) -> Result<&str, Error> {
        self.config
            .timestamp_field
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidConfigValue("timestamp_field is required".to_string()))
    }

    async fn create_client(&self) -> Result<OpenSearch, Error> {
        let url = Url::parse(&self.config.url).map_err(|error| {
            Error::InvalidConfigValue(format!("Invalid OpenSearch URL: {error}"))
        })?;

        let conn_pool = opensearch::http::transport::SingleNodeConnectionPool::new(url);
        // Without a timeout a stalled response never returns, so neither retry nor the circuit
        // breaker would ever see a failure.
        let mut transport_builder = TransportBuilder::new(conn_pool).timeout(self.request_timeout);

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let credentials =
                Credentials::Basic(username.clone(), password.expose_secret().to_string());
            transport_builder = transport_builder.auth(credentials);
        }

        let transport = transport_builder
            .build()
            .map_err(|error| Error::InitError(format!("Failed to build transport: {error}")))?;

        Ok(OpenSearch::new(transport))
    }

    async fn check_index_exists_with_retry(&self, client: &OpenSearch) -> Result<(), Error> {
        let index = &self.config.index;
        retry_async(
            self.open_retry_policy,
            &format!("{CONNECTOR_NAME} ID {} index exists check", self.id),
            RequestFailure::is_transient,
            || async {
                let response = client
                    .indices()
                    .exists(opensearch::indices::IndicesExistsParts::Index(&[index]))
                    .send()
                    .await
                    .map_err(RequestFailure::transport)?;
                RequestFailure::check(response).await.map(drop)
            },
        )
        .await
        .map_err(|failure| match failure.into_error() {
            RequestFailure::Transport(message) => {
                Error::InitError(format!("Failed to check index existence: {message}"))
            }
            RequestFailure::Status { status, .. } if status == StatusCode::NOT_FOUND => {
                Error::InitError(format!(
                    "Index '{index}' does not exist or is not accessible"
                ))
            }
            failure @ RequestFailure::Status { .. } => {
                Error::InitError(format!("Index '{index}' check failed: {failure}"))
            }
        })
    }

    async fn check_tiebreaker_mapping(&self, client: &OpenSearch) -> Result<(), Error> {
        let field = self.tiebreaker_field();
        if field == DEFAULT_TIEBREAKER_FIELD {
            return Ok(());
        }
        let index = &self.config.index;
        let body = retry_async(
            self.open_retry_policy,
            &format!("{CONNECTOR_NAME} ID {} tiebreaker mapping check", self.id),
            RequestFailure::is_transient,
            || async {
                let response = client
                    .indices()
                    .get_field_mapping(
                        opensearch::indices::IndicesGetFieldMappingParts::IndexFields(
                            &[index],
                            &[field],
                        ),
                    )
                    .send()
                    .await
                    .map_err(RequestFailure::transport)?;
                RequestFailure::read_text(response).await
            },
        )
        .await
        .map_err(|failure| {
            Error::InitError(format!(
                "Failed to read the mapping of '{field}': {}",
                failure.into_error()
            ))
        })?;
        let mapping: Value = serde_json::from_str(&body).map_err(|error| {
            Error::InitError(format!("Failed to parse the mapping of '{field}': {error}"))
        })?;
        ensure_keyword_with_doc_values(&mapping, field)
    }

    async fn send_search_with_retry(
        &self,
        client: &OpenSearch,
        search_body: &Value,
    ) -> Result<String, Error> {
        retry_async(
            self.poll_retry_policy,
            &format!("{CONNECTOR_NAME} ID {} search", self.id),
            RequestFailure::is_transient,
            || async {
                let response = client
                    .search(SearchParts::Index(&[&self.config.index]))
                    .allow_partial_search_results(false)
                    .body(search_body)
                    .send()
                    .await
                    .map_err(RequestFailure::transport)?;
                RequestFailure::read_text(response).await
            },
        )
        .await
        .map_err(|failure| failure.into_error().into_search_error())
    }

    async fn search_documents(&self, client: &OpenSearch) -> Result<SearchOutcome, Error> {
        let (last_sort, watermark_hits, scan_cursor) = {
            let state = self.state.lock().await;
            (
                state.last_sort.clone(),
                state.watermark_hits.clone(),
                state.scan_cursor.clone(),
            )
        };
        let watermark = last_sort.as_deref().and_then(<[Value]>::first);
        let published: HashSet<&HitKey> = watermark_hits.iter().collect();
        let mut cursor = scan_cursor.or_else(|| watermark.map(restart_cursor));

        let page = loop {
            let hits = self.fetch_hits(client, cursor.take()).await?;
            let page = self.read_page(hits, watermark, &published)?;
            if !page.fresh.is_empty() || !page.full {
                break page;
            }
            // A full page of hits published before: step past them within this scan.
            cursor = page.sorts.last().cloned();
        };

        let moved = match page.fresh.last() {
            Some(&last) => {
                let sort = page.sorts[last].clone();
                let mut hits = if watermark == sort.first() {
                    watermark_hits.clone()
                } else {
                    Vec::new()
                };
                hits.extend(
                    page.fresh
                        .iter()
                        .filter(|&&position| page.sorts[position].first() == sort.first())
                        .map(|&position| page.keys[position].clone()),
                );
                if hits.len() > MAX_WATERMARK_HITS {
                    return Err(Error::SchemaMismatch(format!(
                        "more than {MAX_WATERMARK_HITS} documents share one value of the sort \
                         field; use a timestamp_field with a finer resolution"
                    )));
                }
                Some((sort, hits))
            }
            None => None,
        };
        let (last_sort, watermark_hits) = moved.unzip();

        Ok(SearchOutcome {
            scan_cursor: if page.full {
                page.sorts.last().cloned()
            } else {
                None
            },
            messages: page.messages,
            last_sort,
            watermark_hits: watermark_hits.unwrap_or_default(),
            last_poll_timestamp: page.last_poll_timestamp,
        })
    }

    async fn fetch_hits(
        &self,
        client: &OpenSearch,
        cursor: Option<Vec<Value>>,
    ) -> Result<Vec<Value>, Error> {
        let mut search_body = self
            .search_body_base
            .as_ref()
            .ok_or_else(|| {
                Error::Connection("connector not initialized; call open() first".to_string())
            })?
            .clone();
        if let Some(cursor) = cursor {
            search_body["search_after"] = Value::Array(cursor);
        }

        let body = self.send_search_with_retry(client, &search_body).await?;
        let mut response_body: Value = serde_json::from_str(&body).map_err(|e| {
            Error::PermanentHttpError(format!("Failed to parse search response: {e}"))
        })?;
        ensure_complete_response(&response_body)?;

        Ok(response_body
            .get_mut("hits")
            .and_then(|h| h.get_mut("hits"))
            .and_then(|arr| arr.as_array_mut())
            .map(std::mem::take)
            .unwrap_or_default())
    }

    fn read_page(
        &self,
        hits: Vec<Value>,
        watermark: Option<&Value>,
        published: &HashSet<&HitKey>,
    ) -> Result<Page, Error> {
        let timestamp_field = self.timestamp_field()?;
        let mut page = Page {
            full: hits.len() >= self.batch_size(),
            messages: Vec::with_capacity(hits.len()),
            sorts: Vec::with_capacity(hits.len()),
            keys: Vec::with_capacity(hits.len()),
            fresh: Vec::new(),
            last_poll_timestamp: None,
        };

        for mut hit in hits {
            let Some(sort) = hit
                .get_mut("sort")
                .and_then(Value::as_array_mut)
                .filter(|a| !a.is_empty())
                .map(std::mem::take)
            else {
                return Err(Error::SchemaMismatch(format!(
                    "OpenSearch hit {:?} has no sort tuple; the cursor cannot move past it \
                     without risking skipped documents",
                    hit.get("_id").and_then(Value::as_str)
                )));
            };

            if sort.first().and_then(Value::as_i64) == Some(MISSING_LAST_SORT_VALUE) {
                return Err(Error::SchemaMismatch(format!(
                    "OpenSearch hit {:?} sorted on the missing-value sentinel; a cursor there \
                     would skip every document indexed later",
                    hit.get("_id").and_then(Value::as_str)
                )));
            }

            let key = hit_key(&hit, &sort, &self.config.index, self.tiebreaker_field())?;
            let already_published = watermark == sort.first() && published.contains(&key);
            if !already_published {
                page.fresh.push(page.sorts.len());
                match hit.get_mut("_source").map(Value::take) {
                    Some(source) => {
                        self.push_message(&mut page, &hit, &key, &source, timestamp_field)?;
                    }
                    None => warn!(
                        connector_id = self.id,
                        hit_id = key.id,
                        hit_index = key.index,
                        "Skipping OpenSearch hit without _source; it has no body to publish"
                    ),
                }
            }
            page.sorts.push(sort);
            page.keys.push(key);
        }
        Ok(page)
    }

    fn push_message(
        &self,
        page: &mut Page,
        hit: &Value,
        key: &HitKey,
        source: &Value,
        timestamp_field: &str,
    ) -> Result<(), Error> {
        let document_timestamp =
            field_value(source, timestamp_field).and_then(parse_document_timestamp);
        if let Some(timestamp_utc) = document_timestamp {
            page.last_poll_timestamp = Some(timestamp_utc);
        }

        let payload = serde_json::to_vec(source)
            .map_err(|e| Error::Serialization(format!("Failed to serialize document: {e}")))?;

        // `index` may be a pattern, so the concrete index of each hit keeps equal `_id`s
        // in different indices from sharing a message id.
        page.messages.push(ProducedMessage {
            id: Some(message_id(&key.index, &key.id)),
            headers: document_headers(
                self.id,
                hit.get("_index").and_then(Value::as_str),
                hit.get("_id").and_then(Value::as_str),
            ),
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            payload,
        });
        Ok(())
    }

    async fn finalize_poll(
        &self,
        outcome: SearchOutcome,
    ) -> Result<(Vec<ProducedMessage>, Option<ConnectorState>), Error> {
        let mut state = self.state.lock().await.clone();
        state.total_documents_published += outcome.messages.len();
        state.poll_count += 1;
        let cursor_moved = outcome.last_sort.is_some();
        if outcome.last_sort.is_some() {
            state.last_sort = outcome.last_sort;
            state.watermark_hits = outcome.watermark_hits;
        }
        state.scan_cursor = outcome.scan_cursor;
        if let Some(timestamp) = outcome.last_poll_timestamp {
            state.last_poll_timestamp = Some(timestamp);
        }

        let produced_count = outcome.messages.len();
        let total_documents_published = state.total_documents_published;
        let messages = outcome.messages;
        // A moved cursor waits for the batch result so a Nack can undo it, including a page
        // whose hits were all skipped for lacking `_source`. A poll that saw no hits has no
        // cursor to protect, so its counters commit directly.
        let persisted_state = if cursor_moved {
            let persisted = self.serialize_state(&state).ok_or_else(|| {
                Error::Serialization("failed to serialize source state".to_string())
            })?;
            *self.pending_state.lock().await = Some(state);
            Some(persisted)
        } else {
            *self.state.lock().await = state;
            None
        };

        if self.verbose {
            info!(
                "OpenSearch source connector ID: {} produced {produced_count} messages. \
                 Total published: {total_documents_published}",
                self.id
            );
        } else {
            debug!(
                "OpenSearch source connector ID: {} produced {produced_count} messages. \
                 Total published: {total_documents_published}",
                self.id
            );
        }

        Ok((messages, persisted_state))
    }

    #[cfg(test)]
    fn client_initialized(&self) -> bool {
        self.client.is_some()
    }

    #[cfg(test)]
    async fn test_metrics(&self) -> (usize, usize, usize) {
        let state = self.state.lock().await;
        (
            state.total_documents_published,
            state.poll_count,
            state.error_count,
        )
    }

    #[cfg(test)]
    async fn test_last_sort(&self) -> Option<Vec<Value>> {
        self.state.lock().await.last_sort.clone()
    }

    #[cfg(test)]
    async fn test_last_poll_timestamp(&self) -> Option<DateTime<Utc>> {
        self.state.lock().await.last_poll_timestamp
    }

    async fn handle_poll_error(&self, error: Error) -> Result<ProducedMessages, Error> {
        // Only cluster-side trouble opens the breaker. A permanent error (bad query, missing
        // sort tuple) is a configuration or mapping problem that skipping polls cannot fix,
        // and an open breaker would hide it behind a quiet empty batch.
        if matches!(error, Error::Connection(_) | Error::HttpRequestFailed(_)) {
            self.circuit_breaker.record_failure().await;
        }
        let mut state = self.state.lock().await;
        // error_count accumulates in State and is captured by the next finalize_poll call
        // (success path).
        state.error_count += 1;
        drop(state);
        error!(
            "{CONNECTOR_NAME} connector ID: {} poll failed: {error}",
            self.id
        );
        Err(error)
    }
}

// Sorting on any other field type would fail or load the field into heap, and a field that is
// missing from one matched index gives its documents no tie-break at all.
fn ensure_keyword_with_doc_values(mappings: &Value, field: &str) -> Result<(), Error> {
    let Some(indices) = mappings.as_object() else {
        return Ok(());
    };
    for (index, entry) in indices {
        let Some(mapping) = entry
            .get("mappings")
            .and_then(|mappings| mappings.get(field))
            .and_then(|mapped| mapped.get("mapping"))
            .and_then(Value::as_object)
            .and_then(|by_leaf_name| by_leaf_name.values().next())
        else {
            return Err(Error::InvalidConfigValue(format!(
                "tiebreaker_field '{field}' is not mapped in index '{index}'"
            )));
        };
        if mapping.get("type").and_then(Value::as_str) != Some("keyword") {
            return Err(Error::InvalidConfigValue(format!(
                "tiebreaker_field '{field}' must be of type keyword in index '{index}'"
            )));
        }
        if mapping.get("doc_values").and_then(Value::as_bool) == Some(false) {
            return Err(Error::InvalidConfigValue(format!(
                "tiebreaker_field '{field}' must have doc_values enabled in index '{index}'"
            )));
        }
    }
    Ok(())
}

/// Identifies a hit by the value it is ordered on, so documents that share an `_id` (custom
/// routing does not make `_id` unique across shards) stay distinct once `tiebreaker_field`
/// names a unique field.
fn hit_key(
    hit: &Value,
    sort: &[Value],
    default_index: &str,
    tiebreaker_field: &str,
) -> Result<HitKey, Error> {
    let value = if tiebreaker_field == DEFAULT_TIEBREAKER_FIELD {
        hit.get("_id")
    } else {
        sort.get(TIEBREAKER_SORT_POSITION)
    };
    let id = value.and_then(Value::as_str).ok_or_else(|| {
        Error::SchemaMismatch(format!(
            "OpenSearch hit has no {tiebreaker_field} value; it cannot be told apart from \
             published documents"
        ))
    })?;
    let index = hit
        .get("_index")
        .and_then(Value::as_str)
        .unwrap_or(default_index);
    Ok(HitKey {
        index: index.to_owned(),
        id: id.to_owned(),
    })
}

// Lowest possible tuple at the watermark, so the scan sees every hit with that sort value.
fn restart_cursor(watermark: &Value) -> Vec<Value> {
    std::iter::once(watermark.clone())
        .chain(std::iter::repeat_n(json!(""), SORT_KEY_COUNT - 1))
        .collect()
}

// A partial page can omit hits sorted before the last returned one. The cursor would then move
// past them for good, so such a response must fail the poll before any cursor update.
fn ensure_complete_response(body: &Value) -> Result<(), Error> {
    if body.get("timed_out").and_then(Value::as_bool) == Some(true) {
        return Err(Error::HttpRequestFailed(
            "OpenSearch search timed out; results may be partial".to_string(),
        ));
    }
    let failed_shards = body
        .get("_shards")
        .and_then(|shards| shards.get("failed"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if failed_shards > 0 {
        return Err(Error::HttpRequestFailed(format!(
            "OpenSearch search failed on {failed_shards} shards; results may be partial"
        )));
    }
    Ok(())
}

fn restore_state(
    id: u32,
    state: Option<ConnectorState>,
    fingerprint: &str,
) -> (State, Option<String>) {
    let fresh = || State {
        config_fingerprint: Some(fingerprint.to_owned()),
        ..State::default()
    };
    let Some(connector_state) = state else {
        return (fresh(), None);
    };

    match connector_state.deserialize::<State>(CONNECTOR_NAME, id) {
        Some(restored)
            if restored.last_sort.is_some()
                && restored
                    .config_fingerprint
                    .as_deref()
                    .is_some_and(|saved| saved != fingerprint) =>
        {
            let cause = "persisted state belongs to a different index, timestamp_field or query. \
                         Delete the runtime state file to start from the beginning."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (fresh(), Some(cause))
        }
        Some(restored) => {
            info!(
                "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                 Documents published: {}, poll count: {}",
                restored.total_documents_published, restored.poll_count
            );
            let restored = State {
                config_fingerprint: Some(fingerprint.to_owned()),
                ..restored
            };
            (restored, None)
        }
        None => {
            // Starting fresh would re-publish the whole index, so refuse to open and let the
            // operator repair or delete the state instead of silently resetting the cursor.
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (fresh(), Some(cause))
        }
    }
}

// The query is part of the identity because a narrower or different query changes which
// documents the saved cursor has already passed.
fn config_fingerprint(config: &OpenSearchSourceConfig) -> String {
    let identity = json!([config.index, config.timestamp_field, config.query]);
    blake3::hash(identity.to_string().as_bytes())
        .to_hex()
        .to_string()
}

fn validate_open_config(config: &OpenSearchSourceConfig) -> Result<(), Error> {
    if config.timestamp_field.as_deref().is_none_or(str::is_empty) {
        return Err(Error::InvalidConfigValue(
            "timestamp_field is required for incremental OpenSearch polling".to_string(),
        ));
    }

    if let Some(batch_size) = config.batch_size
        && !(1..=MAX_BATCH_SIZE).contains(&batch_size)
    {
        return Err(Error::InvalidConfigValue(format!(
            "batch_size must be between 1 and {MAX_BATCH_SIZE}, got {batch_size}"
        )));
    }

    if config
        .tiebreaker_field
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(Error::InvalidConfigValue(
            "tiebreaker_field must not be empty".to_string(),
        ));
    }

    if config
        .query
        .as_ref()
        .is_some_and(|query| !query.is_object())
    {
        return Err(Error::InvalidConfigValue(
            "query must be a JSON object".to_string(),
        ));
    }

    Ok(())
}

/// Strips credentials and query from a URL before it reaches a log line, since a URL can
/// carry `user:password@` or an access token.
fn redacted_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.to_string()
        }
        Err(_) => "<invalid url>".to_string(),
    }
}

/// Stable 128-bit message id so a consumer can drop a document replayed after a NACK or a
/// lost state save. Length-prefixed parts keep distinct (index, id) pairs from aliasing.
fn message_id(index: &str, document_id: &str) -> u128 {
    let mut hasher = blake3::Hasher::new();
    for part in [index, document_id] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let mut id = [0u8; 16];
    hasher.finalize_xof().fill(&mut id);
    u128::from_le_bytes(id)
}

/// The hashed message id cannot be mapped back to the document, so the original `_id` and
/// concrete `_index` travel as headers. A value over the header limit is omitted, the message
/// id remains.
fn document_headers(
    connector_id: u32,
    index: Option<&str>,
    document_id: Option<&str>,
) -> Option<BTreeMap<HeaderKey, HeaderValue>> {
    let mut headers = BTreeMap::new();
    for (key, name, value) in [
        (&*INDEX_HEADER_KEY, INDEX_HEADER, index),
        (&*DOCUMENT_ID_HEADER_KEY, DOCUMENT_ID_HEADER, document_id),
    ] {
        let Some(value) = value else { continue };
        if value.len() > MAX_HEADER_VALUE_LENGTH {
            warn!(
                "{CONNECTOR_NAME} connector ID: {connector_id} omitted the {name} header: \
                 value is {} bytes, limit is {MAX_HEADER_VALUE_LENGTH}",
                value.len()
            );
            continue;
        }
        if let Ok(header_value) = HeaderValue::try_from(value) {
            headers.insert(key.clone(), header_value);
        }
    }
    (!headers.is_empty()).then_some(headers)
}

/// OpenSearch addresses nested fields by dotted path, while `_source` may hold the value
/// under that literal key or under nested objects.
fn field_value<'a>(source: &'a Value, path: &str) -> Option<&'a Value> {
    source.get(path).or_else(|| {
        path.split('.')
            .try_fold(source, |value, segment| value.get(segment))
    })
}

fn parse_document_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(text) => DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|timestamp| timestamp.with_timezone(&Utc)),
        Value::Number(number) => {
            let raw = number.as_i64()?;
            // Values above 1e12 are already milliseconds (Unix epoch seconds won't reach
            // 1e12 until year 33658). Values at or below are treated as seconds and
            // multiplied by 1000.
            let millis = if !(-1_000_000_000_000..=1_000_000_000_000).contains(&raw) {
                raw
            } else {
                raw.saturating_mul(1_000)
            };
            DateTime::from_timestamp_millis(millis).map(|timestamp| timestamp.with_timezone(&Utc))
        }
        _ => None,
    }
}

#[async_trait]
impl Source for OpenSearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(ref cause) = self.state_restore_error {
            return Err(Error::InitError(format!("state restore failed: {cause}")));
        }

        validate_open_config(&self.config)?;

        info!(
            "Opening OpenSearch source connector with ID: {} for URL: {}, index: {}",
            self.id,
            redacted_url(&self.config.url),
            self.config.index
        );

        let client = self.create_client().await?;

        self.check_index_exists_with_retry(&client).await?;
        self.check_tiebreaker_mapping(&client).await?;

        let timestamp_field = self.timestamp_field()?;
        let tiebreaker_field = self.tiebreaker_field();
        let batch_size = self.batch_size();
        self.search_body_base = Some(json!({
            "query": self.config.query.clone().unwrap_or_else(|| json!({ "match_all": {} })),
            "size": batch_size,
            "track_total_hits": false,
            "sort": [
                { timestamp_field: { "order": "asc", "missing": "_first" } },
                { tiebreaker_field: { "order": "asc" } },
                { "_index": { "order": "asc" } }
            ]
        }));
        self.client = Some(client);

        info!(
            "Successfully opened OpenSearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} connector ID: {} circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            sleep(self.polling_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        sleep(self.polling_interval).await;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("OpenSearch client not initialized".to_string()))?;

        match self.search_documents(client).await {
            Ok(outcome) => {
                self.circuit_breaker.record_success();
                match self.finalize_poll(outcome).await {
                    Ok((messages, persisted_state)) => Ok(ProducedMessages {
                        schema: Schema::Json,
                        messages,
                        state: persisted_state,
                    }),
                    Err(error) => self.handle_poll_error(error).await,
                }
            }
            Err(error) => self.handle_poll_error(error).await,
        }
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate) = candidate
        {
            *self.state.lock().await = candidate;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "OpenSearch source connector with ID: {} is closing. Stats: {} total documents published, {} polls executed, {} errors",
            self.id, state.total_documents_published, state.poll_count, state.error_count
        );
        drop(state);

        self.client = None;
        info!(
            "OpenSearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

#[cfg(test)]
mod http_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OpenSearchSourceConfig {
        OpenSearchSourceConfig {
            url: "http://localhost:9200".to_string(),
            index: "test_documents".to_string(),
            username: None,
            password: None,
            query: None,
            polling_interval: Some("100ms".to_string()),
            batch_size: Some(10),
            timestamp_field: Some("timestamp".to_string()),
            verbose_logging: false,
            max_retries: None,
            retry_delay: None,
            retry_max_delay: None,
            max_open_retries: None,
            open_retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            request_timeout: None,
            tiebreaker_field: None,
        }
    }

    fn test_state() -> State {
        State {
            last_poll_timestamp: None,
            total_documents_published: 500,
            poll_count: 7,
            last_sort: Some(vec![json!("2024-01-01T00:00:00Z"), json!("doc_42")]),
            error_count: 1,
            watermark_hits: vec![HitKey {
                index: "logs".to_string(),
                id: "doc_42".to_string(),
            }],
            config_fingerprint: None,
            scan_cursor: None,
        }
    }

    fn mapping_of(index: &str, field: &str, mapping: Value) -> Value {
        json!({ index: { "mappings": { field: { "mapping": { field: mapping } } } } })
    }

    #[test]
    fn given_keyword_with_doc_values_when_checking_tiebreaker_should_accept() {
        let body = mapping_of("logs", "event_id", json!({ "type": "keyword" }));
        assert!(ensure_keyword_with_doc_values(&body, "event_id").is_ok());
        assert!(ensure_keyword_with_doc_values(&json!({}), "event_id").is_ok());
    }

    #[test]
    fn given_text_or_disabled_doc_values_when_checking_tiebreaker_should_reject() {
        for mapping in [
            json!({ "type": "text" }),
            json!({ "type": "keyword", "doc_values": false }),
        ] {
            let body = mapping_of("logs", "event_id", mapping);
            assert!(matches!(
                ensure_keyword_with_doc_values(&body, "event_id"),
                Err(Error::InvalidConfigValue(_))
            ));
        }
    }

    #[test]
    fn given_field_missing_in_one_index_when_checking_tiebreaker_should_reject() {
        let body = json!({
            "logs-a": { "mappings": { "event_id": { "mapping": { "event_id": { "type": "keyword" } } } } },
            "logs-b": { "mappings": {} }
        });
        assert!(matches!(
            ensure_keyword_with_doc_values(&body, "event_id"),
            Err(Error::InvalidConfigValue(message)) if message.contains("logs-b")
        ));
    }

    #[test]
    fn given_persisted_runtime_state_when_new_should_restore_counts() {
        let state = test_state();
        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let source = OpenSearchSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.total_documents_published, 500);
            assert_eq!(restored.poll_count, 7);
            assert_eq!(
                restored.last_sort,
                Some(vec![json!("2024-01-01T00:00:00Z"), json!("doc_42")])
            );
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_state_from_other_index_when_new_should_set_restore_error() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let mut state = test_state();
        state.config_fingerprint = Some(config_fingerprint(&test_config()));
        let saved = source
            .serialize_state(&state)
            .expect("state should serialize");
        let mut other = test_config();
        other.index = "other_documents".to_string();

        let moved = OpenSearchSource::new(1, other, Some(saved));

        assert!(moved.state_restore_error.is_some());
    }

    #[test]
    fn given_state_with_changed_query_when_new_should_set_restore_error() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let mut state = test_state();
        state.config_fingerprint = Some(config_fingerprint(&test_config()));
        let saved = source
            .serialize_state(&state)
            .expect("state should serialize");
        let mut changed = test_config();
        changed.query = Some(json!({ "term": { "level": "error" } }));

        let moved = OpenSearchSource::new(1, changed, Some(saved));

        assert!(moved.state_restore_error.is_some());
    }

    #[test]
    fn given_state_without_fingerprint_when_new_should_restore_and_stamp() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let saved = source
            .serialize_state(&test_state())
            .expect("state should serialize");

        let restored = OpenSearchSource::new(1, test_config(), Some(saved));

        assert!(restored.state_restore_error.is_none());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = restored.state.lock().await;
            assert_eq!(
                state.config_fingerprint,
                Some(config_fingerprint(&test_config()))
            );
        });
    }

    #[test]
    fn given_no_runtime_state_when_new_should_start_fresh() {
        let source = OpenSearchSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_published, 0);
            assert_eq!(state.poll_count, 0);
            assert_eq!(state.last_sort, None);
            assert!(source.state_restore_error.is_none());
        });
    }

    #[test]
    fn given_invalid_runtime_state_when_new_should_set_restore_error() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let source = OpenSearchSource::new(1, test_config(), Some(invalid_state));

        assert!(source.state_restore_error.is_some());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = source.state.lock().await;
            assert_eq!(state.total_documents_published, 0);
            assert_eq!(state.poll_count, 0);
        });
    }

    #[test]
    fn given_invalid_state_when_open_should_fail() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let mut source = OpenSearchSource::new(1, test_config(), Some(invalid_state));
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(source.open());
        assert!(
            matches!(result, Err(Error::InitError(_))),
            "open() must fail with InitError on restore failure"
        );
    }

    #[test]
    fn given_missing_timestamp_field_when_validate_should_fail() {
        let mut config = test_config();
        config.timestamp_field = None;
        let error = validate_open_config(&config).expect_err("missing timestamp_field");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_non_object_query_when_validate_should_fail() {
        let mut config = test_config();
        config.query = Some(json!("match_all"));
        let error = validate_open_config(&config).expect_err("non-object query");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_empty_timestamp_field_when_validate_should_fail() {
        let mut config = test_config();
        config.timestamp_field = Some(String::new());
        let error = validate_open_config(&config).expect_err("empty timestamp_field");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_unparsable_timestamp_when_parsed_should_return_none() {
        let value = json!("not-a-timestamp");
        assert!(parse_document_timestamp(&value).is_none());
    }

    #[test]
    fn given_invalid_url_when_open_should_return_invalid_config() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut config = test_config();
            config.url = "not-a-url".to_string();
            let mut source = OpenSearchSource::new(1, config, None);
            let error = source.open().await.expect_err("invalid url");
            assert!(matches!(error, Error::InvalidConfigValue(_)));
        });
    }

    #[test]
    fn given_url_with_credentials_and_query_when_redacted_should_drop_both() {
        let redacted = redacted_url("https://admin:s3cret@search.example.com:9200/?token=abc");
        assert_eq!(redacted, "https://search.example.com:9200/");
        assert!(!redacted.contains("s3cret") && !redacted.contains("abc"));
    }

    #[test]
    fn given_unparsable_url_when_redacted_should_not_echo_input() {
        assert_eq!(redacted_url("not a url s3cret"), "<invalid url>");
    }

    #[test]
    fn given_same_index_and_id_when_hashed_should_return_same_message_id() {
        assert_eq!(message_id("logs", "doc-1"), message_id("logs", "doc-1"));
    }

    #[test]
    fn given_different_id_or_index_when_hashed_should_return_different_message_id() {
        let base = message_id("logs", "doc-1");
        assert_ne!(base, message_id("logs", "doc-2"));
        assert_ne!(base, message_id("metrics", "doc-1"));
        assert_ne!(message_id("ab", "c"), message_id("a", "bc"));
    }

    #[test]
    fn given_index_and_id_when_building_headers_should_carry_both() {
        let headers = document_headers(1, Some("logs-a"), Some("doc-1")).expect("headers");
        let id = headers
            .get(&*DOCUMENT_ID_HEADER_KEY)
            .expect("document id header");
        let index = headers.get(&*INDEX_HEADER_KEY).expect("index header");
        assert_eq!(id.as_str().expect("string header"), "doc-1");
        assert_eq!(index.as_str().expect("string header"), "logs-a");
    }

    #[test]
    fn given_no_index_or_id_when_building_headers_should_return_none() {
        assert!(document_headers(1, None, None).is_none());
    }

    #[test]
    fn given_oversized_document_id_when_building_headers_should_omit_header() {
        let oversized = "x".repeat(MAX_HEADER_VALUE_LENGTH + 1);
        assert!(document_headers(1, None, Some(&oversized)).is_none());

        let headers = document_headers(1, Some("logs-a"), Some(&oversized)).expect("headers");
        assert!(headers.contains_key(&*INDEX_HEADER_KEY));
        assert!(!headers.contains_key(&*DOCUMENT_ID_HEADER_KEY));
    }

    #[test]
    fn given_zero_batch_size_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(0);
        let error = validate_open_config(&config).expect_err("zero batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));
    }

    #[test]
    fn given_batch_size_above_max_when_validate_should_fail() {
        let mut config = test_config();
        config.batch_size = Some(MAX_BATCH_SIZE + 1);
        let error = validate_open_config(&config).expect_err("oversized batch_size");
        assert!(matches!(error, Error::InvalidConfigValue(_)));

        config.batch_size = Some(MAX_BATCH_SIZE);
        validate_open_config(&config).expect("max batch_size is allowed");
    }

    #[test]
    fn given_dotted_path_when_resolved_should_find_nested_or_literal_key() {
        let nested = json!({ "event": { "created": "2024-01-01T00:00:00Z" } });
        let literal = json!({ "event.created": "2024-01-01T00:00:00Z" });
        let expected = json!("2024-01-01T00:00:00Z");

        assert_eq!(field_value(&nested, "event.created"), Some(&expected));
        assert_eq!(field_value(&literal, "event.created"), Some(&expected));
        assert_eq!(field_value(&nested, "event.missing"), None);
        assert_eq!(field_value(&nested, "event"), nested.get("event"));
    }

    #[test]
    fn given_rfc3339_timestamp_when_parsed_should_succeed() {
        let value = json!("2024-01-15T10:30:00Z");
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn given_epoch_millis_timestamp_when_parsed_should_succeed() {
        let value = json!(1_705_312_200_000_i64);
        assert!(parse_document_timestamp(&value).is_some());
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let source = OpenSearchSource::new(1, test_config(), None);
        let state = test_state();

        let connector_state = source
            .serialize_state(&state)
            .expect("state should serialize");
        let restored: State = connector_state
            .deserialize(CONNECTOR_NAME, 1)
            .expect("Failed to deserialize state");

        assert_eq!(restored.total_documents_published, 500);
        assert_eq!(restored.poll_count, state.poll_count);
        assert_eq!(restored.last_sort, state.last_sort);
        assert_eq!(restored.error_count, state.error_count);
    }
}
