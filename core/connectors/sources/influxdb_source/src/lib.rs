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
use base64::{Engine as _, engine::general_purpose};
use csv::StringRecord;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::retry::{
    CircuitBreaker, exponential_backoff, jitter, parse_duration, parse_retry_after,
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use regex::Regex;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

source_connector!(InfluxDbSource);

const CONNECTOR_NAME: &str = "InfluxDB source";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_CURSOR: &str = "1970-01-01T00:00:00Z";
// Maximum attempts for open() connectivity retries
const DEFAULT_MAX_OPEN_RETRIES: u32 = 10;
// Cap for exponential backoff in open() — never wait longer than this
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
// How many consecutive poll failures open the circuit breaker
const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: u32 = 5;
// How long the circuit stays open before allowing a probe attempt
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

/// RFC 3339 / ISO 8601 datetime pattern.
/// Matches the forms InfluxDB stores in `_time`:
///   "2024-01-15T10:30:00Z"
///   "2024-01-15T10:30:00.123456789Z"
///   "2024-01-15T10:30:00+05:30"
/// Intentionally strict: only digits, T, Z, colon, dot, plus, hyphen.
/// Any Flux syntax character (pipe, quote, paren, space, slash) is rejected.
static CURSOR_RE: OnceLock<Regex> = OnceLock::new();

// ---------------------------------------------------------------------------
// Main connector structs
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InfluxDbSource {
    pub id: u32,
    config: InfluxDbSourceConfig,
    client: Option<Client>,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
    poll_interval: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDbSourceConfig {
    pub url: String,
    pub org: String,
    pub token: String,
    pub query: String,
    pub poll_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub cursor_field: Option<String>,
    pub initial_offset: Option<String>,
    pub payload_column: Option<String>,
    pub payload_format: Option<String>,
    pub include_metadata: Option<bool>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub timeout: Option<String>,
    // How many times open() will retry before giving up
    pub max_open_retries: Option<u32>,
    // Upper cap on open() backoff delay
    pub open_retry_max_delay: Option<String>,
    // Circuit breaker configuration
    pub circuit_breaker_threshold: Option<u32>,
    pub circuit_breaker_cool_down: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PayloadFormat {
    #[default]
    Json,
    Text,
    Raw,
}

impl PayloadFormat {
    fn from_config(value: Option<&str>) -> Self {
        match value.map(|v| v.to_ascii_lowercase()).as_deref() {
            Some("text") | Some("utf8") => PayloadFormat::Text,
            Some("raw") | Some("base64") => PayloadFormat::Raw,
            _ => PayloadFormat::Json,
        }
    }

    fn schema(self) -> Schema {
        match self {
            PayloadFormat::Json => Schema::Json,
            PayloadFormat::Text => Schema::Text,
            PayloadFormat::Raw => Schema::Raw,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    last_poll_time: DateTime<Utc>,
    last_timestamp: Option<String>,
    processed_rows: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_scalar(value: &str) -> serde_json::Value {
    if value.is_empty() {
        return serde_json::Value::Null;
    }
    if let Ok(v) = value.parse::<bool>() {
        return serde_json::Value::Bool(v);
    }
    if let Ok(v) = value.parse::<i64>() {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = value.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(v)
    {
        return serde_json::Value::Number(number);
    }
    serde_json::Value::String(value.to_string())
}

fn is_header_record(record: &StringRecord) -> bool {
    record.iter().any(|v| v == "_time") && record.iter().any(|v| v == "_value")
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

// ---------------------------------------------------------------------------
// InfluxDbSource implementation
// ---------------------------------------------------------------------------

impl InfluxDbSource {
    pub fn new(id: u32, config: InfluxDbSourceConfig, state: Option<ConnectorState>) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let poll_interval = parse_duration(config.poll_interval.as_deref(), DEFAULT_POLL_INTERVAL);

        // Build circuit breaker from config
        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CIRCUIT_COOL_DOWN,
        );

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Last timestamp: {:?}, processed rows: {}",
                    s.last_timestamp, s.processed_rows
                );
            });

        InfluxDbSource {
            id,
            config,
            client: None,
            state: Mutex::new(restored_state.unwrap_or(State {
                last_poll_time: Utc::now(),
                last_timestamp: None,
                processed_rows: 0,
            })),
            verbose,
            retry_delay,
            poll_interval,
            circuit_breaker: Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down)),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn payload_format(&self) -> PayloadFormat {
        PayloadFormat::from_config(self.config.payload_format.as_deref())
    }

    fn cursor_field(&self) -> &str {
        self.config.cursor_field.as_deref().unwrap_or("_time")
    }

    fn get_max_retries(&self) -> u32 {
        self.config
            .max_retries
            .unwrap_or(DEFAULT_MAX_RETRIES)
            .max(1)
    }

    fn build_client(&self) -> Result<Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn get_client(&self) -> Result<&Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client is not initialized".to_string()))
    }

    fn build_health_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        Url::parse(&format!("{base}/health"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))
    }

    fn build_query_url(&self) -> Result<Url, Error> {
        let base = self.config.url.trim_end_matches('/');
        let mut url = Url::parse(&format!("{base}/api/v2/query"))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;
        url.query_pairs_mut().append_pair("org", &self.config.org);
        Ok(url)
    }

    async fn check_connectivity(&self) -> Result<(), Error> {
        let client = self.get_client()?;
        let url = self.build_health_url()?;
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("InfluxDB health check failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read response body".to_string());
            return Err(Error::Connection(format!(
                "InfluxDB health check returned status {status}: {body}"
            )));
        }
        Ok(())
    }

    // Retry connectivity check with exponential backoff + jitter
    // instead of failing hard on the first attempt.
    async fn check_connectivity_with_retry(&self) -> Result<(), Error> {
        let max_open_retries = self
            .config
            .max_open_retries
            .unwrap_or(DEFAULT_MAX_OPEN_RETRIES)
            .max(1);

        let max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

        let mut attempt = 0u32;
        loop {
            match self.check_connectivity().await {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            "InfluxDB connectivity established after {attempt} retries \
                             for connector ID: {}",
                            self.id
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_open_retries {
                        error!(
                            "InfluxDB connectivity check failed after {attempt} attempts \
                             for connector ID: {}. Giving up: {e}",
                            self.id
                        );
                        return Err(e);
                    }
                    // Exponential backoff, with jitter
                    let backoff = jitter(exponential_backoff(self.retry_delay, attempt, max_delay));
                    warn!(
                        "InfluxDB health check failed (attempt {attempt}/{max_open_retries}) \
                         for connector ID: {}. Retrying in {backoff:?}: {e}",
                        self.id
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    async fn current_cursor(&self) -> String {
        let state = self.state.lock().await;
        state
            .last_timestamp
            .clone()
            .or_else(|| self.config.initial_offset.clone())
            .unwrap_or_else(|| DEFAULT_CURSOR.to_string())
    }

    fn cursor_re() -> &'static Regex {
        CURSOR_RE.get_or_init(|| {
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
                .expect("hardcoded regex is valid")
        })
    }

    fn validate_cursor(cursor: &str) -> Result<(), Error> {
        if Self::cursor_re().is_match(cursor) {
            // ← call on &Regex, not on OnceLock
            Ok(())
        } else {
            Err(Error::InvalidConfigValue(format!(
                "cursor value {:?} is not a valid RFC 3339 timestamp; \
             refusing substitution to prevent Flux query injection",
                cursor
            )))
        }
    }

    /// Reject cursor fields that would produce incorrect results.
    ///
    /// Cursor advancement compares values as `String`s (lexicographic order).
    /// This is correct for ISO 8601 / RFC 3339 timestamps — the default
    /// `cursor_field` of `"_time"` — because their fixed-width format makes
    /// lexicographic and chronological order identical.
    fn validate_cursor_field(field: &str) -> Result<(), Error> {
        match field {
            "_time" | "time" => Ok(()),
            other => Err(Error::InvalidConfigValue(format!(
                "cursor_field {:?} is not supported — cursor values are compared as strings \
                 (lexicographic order), which is only correct for ISO 8601 timestamp columns. \
                 Use the default \"_time\" column, or omit cursor_field entirely.",
                other
            ))),
        }
    }

    fn query_with_params(&self, cursor: &str) -> Result<String, Error> {
        // Reject anything that is not a well-formed RFC 3339 timestamp.
        // This prevents a crafted or corrupted _time value (e.g. containing
        // Flux syntax like `") |> drop() //`) from being injected into the
        // query string before it is sent to /api/v2/query.
        // Note: InfluxDB OSS v2 does not support the `params` JSON field for
        // parameterized queries (Cloud-only feature), so substitution is
        // unavoidable for OSS — validation is the correct mitigation here.
        Self::validate_cursor(cursor)?;
        let limit = self.config.batch_size.unwrap_or(500).to_string();
        let mut query = self.config.query.clone();
        if query.contains("$cursor") {
            query = query.replace("$cursor", cursor);
        }
        if query.contains("$limit") {
            query = query.replace("$limit", &limit);
        }
        Ok(query)
    }

    async fn run_query_with_retry(&self, query: &str) -> Result<String, Error> {
        let client = self.get_client()?;
        let url = self.build_query_url()?;
        let max_retries = self.get_max_retries();
        let token = self.config.token.clone();

        // Cap for per-query backoff (reuse open_retry_max_delay config)
        let max_delay = parse_duration(
            self.config.open_retry_max_delay.as_deref(),
            DEFAULT_OPEN_RETRY_MAX_DELAY,
        );

        let body = json!({
            "query": query,
            "dialect": {
                "annotations": [],
                "delimiter": ",",
                "header": true,
                "commentPrefix": "#"
            }
        });

        let mut attempts = 0u32;
        loop {
            let response_result = client
                .post(url.clone())
                .header("Authorization", format!("Token {token}"))
                .header("Content-Type", "application/json")
                .header("Accept", "text/csv")
                .json(&body)
                .send()
                .await;

            match response_result {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return response.text().await.map_err(|e| {
                            Error::Storage(format!("Failed to read query response: {e}"))
                        });
                    }

                    // Honour Retry-After on 429 before our own backoff
                    let retry_after = if status == StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers()
                            .get("Retry-After")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after)
                    } else {
                        None
                    };

                    let body_text = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response body".to_string());

                    attempts += 1;
                    if is_transient_status(status) && attempts < max_retries {
                        // Use server-supplied delay when available
                        let delay = retry_after.unwrap_or_else(|| {
                            // Exponential, with jitter
                            jitter(exponential_backoff(self.retry_delay, attempts, max_delay))
                        });
                        warn!(
                            "Transient InfluxDB query error (attempt {attempts}/{max_retries}): \
                             {status}. Retrying in {delay:?}..."
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    return Err(Error::Storage(format!(
                        "InfluxDB query failed with status {status}: {body_text}"
                    )));
                }
                Err(e) => {
                    attempts += 1;
                    if attempts < max_retries {
                        // Exponential, with jitter
                        let delay =
                            jitter(exponential_backoff(self.retry_delay, attempts, max_delay));
                        warn!(
                            "Failed to query InfluxDB (attempt {attempts}/{max_retries}): \
                             {e}. Retrying in {delay:?}..."
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    return Err(Error::Storage(format!(
                        "InfluxDB query failed after {attempts} attempts: {e}"
                    )));
                }
            }
        }
    }

    fn parse_csv_rows(&self, csv_text: &str) -> Result<Vec<HashMap<String, String>>, Error> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_text.as_bytes());

        let mut headers: Option<StringRecord> = None;
        let mut rows = Vec::new();

        for result in reader.records() {
            let record = result
                .map_err(|e| Error::InvalidRecordValue(format!("Invalid CSV record: {e}")))?;

            if record.is_empty() {
                continue;
            }

            if let Some(first) = record.get(0)
                && first.starts_with('#')
            {
                continue;
            }

            if is_header_record(&record) {
                headers = Some(record.clone());
                continue;
            }

            let Some(active_headers) = headers.as_ref() else {
                continue;
            };

            if record == *active_headers {
                continue;
            }

            let mut mapped = HashMap::new();
            for (idx, key) in active_headers.iter().enumerate() {
                if key.is_empty() {
                    continue;
                }
                let value = record.get(idx).unwrap_or("").to_string();
                mapped.insert(key.to_string(), value);
            }

            if !mapped.is_empty() {
                rows.push(mapped);
            }
        }

        Ok(rows)
    }

    fn build_payload(
        &self,
        row: &HashMap<String, String>,
        include_metadata: bool,
    ) -> Result<Vec<u8>, Error> {
        if let Some(payload_column) = self.config.payload_column.as_deref() {
            let raw_value = row.get(payload_column).cloned().ok_or_else(|| {
                Error::InvalidRecordValue(format!("Missing payload column '{payload_column}'"))
            })?;

            return match self.payload_format() {
                PayloadFormat::Json => {
                    let value: serde_json::Value =
                        serde_json::from_str(&raw_value).map_err(|e| {
                            Error::InvalidRecordValue(format!(
                                "Payload column '{payload_column}' is not valid JSON: {e}"
                            ))
                        })?;
                    serde_json::to_vec(&value).map_err(|e| {
                        Error::Serialization(format!("JSON serialization failed: {e}"))
                    })
                }
                PayloadFormat::Text => Ok(raw_value.into_bytes()),
                PayloadFormat::Raw => general_purpose::STANDARD
                    .decode(raw_value.as_bytes())
                    .map_err(|e| {
                        Error::InvalidRecordValue(format!(
                            "Failed to decode payload as base64: {e}"
                        ))
                    }),
            };
        }

        let mut json_row = serde_json::Map::new();
        for (key, value) in row {
            if include_metadata || key == "_value" || key == "_time" || key == "_measurement" {
                json_row.insert(key.clone(), parse_scalar(value));
            }
        }

        let wrapped = json!({
            "measurement": row.get("_measurement").cloned().unwrap_or_default(),
            "field": row.get("_field").cloned().unwrap_or_default(),
            "timestamp": row.get("_time").cloned().unwrap_or_default(),
            "value": row.get("_value").map(|v| parse_scalar(v)).unwrap_or(serde_json::Value::Null),
            "row": json_row,
        });

        serde_json::to_vec(&wrapped)
            .map_err(|e| Error::Serialization(format!("JSON serialization failed: {e}")))
    }

    async fn poll_messages(&self) -> Result<(Vec<ProducedMessage>, Option<String>), Error> {
        let cursor = self.current_cursor().await;
        let query = self.query_with_params(&cursor).map_err(|e| {
            error!(
                "InfluxDB source ID: {} — invalid cursor, skipping poll: {e}",
                self.id
            );
            e
        })?;
        let csv_data = self.run_query_with_retry(&query).await?;

        let rows = self.parse_csv_rows(&csv_data)?;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let cursor_field = self.cursor_field().to_string();

        let mut messages = Vec::with_capacity(rows.len());
        let mut max_cursor: Option<String> = None;

        for row in rows {
            if let Some(cursor_value) = row.get(&cursor_field)
                && max_cursor
                    .as_ref()
                    .is_none_or(|current| cursor_value > current)
            {
                max_cursor = Some(cursor_value.clone());
            }

            let payload = self.build_payload(&row, include_metadata)?;
            // correct unit, single clock read
            // Capture once so timestamp and origin_timestamp are guaranteed identical
            // and we make exactly one syscall regardless of how many fields use it.
            let now_micros = Utc::now().timestamp_micros() as u64;

            messages.push(ProducedMessage {
                id: Some(Uuid::new_v4().as_u128()),
                checksum: None,
                timestamp: Some(now_micros),
                origin_timestamp: Some(now_micros),
                headers: None,
                payload,
            });
        }

        Ok((messages, max_cursor))
    }
}

// ---------------------------------------------------------------------------
// Source trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Source for InfluxDbSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening InfluxDB source connector with ID: {}. Org: {}",
            self.id, self.config.org
        );

        self.client = Some(self.build_client()?);

        // Validate cursor_field before touching the network: string comparison
        // is only safe for timestamp columns. See validate_cursor_field for details.
        Self::validate_cursor_field(self.cursor_field())?;

        // Use retrying connectivity check instead of hard-fail
        self.check_connectivity_with_retry().await?;

        info!(
            "InfluxDB source connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Skip query if circuit breaker is open
        if self.circuit_breaker.is_open().await {
            warn!(
                "InfluxDB source ID: {} — circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        match self.poll_messages().await {
            Ok((messages, max_cursor)) => {
                // Successful poll — reset circuit breaker
                self.circuit_breaker.record_success();

                let mut state = self.state.lock().await;
                state.last_poll_time = Utc::now();
                state.processed_rows += messages.len() as u64;
                if let Some(cursor) = max_cursor {
                    state.last_timestamp = Some(cursor);
                }

                if self.verbose {
                    info!(
                        "InfluxDB source ID: {} produced {} messages. \
                         Total processed: {}. Cursor: {:?}",
                        self.id,
                        messages.len(),
                        state.processed_rows,
                        state.last_timestamp
                    );
                } else {
                    debug!(
                        "InfluxDB source ID: {} produced {} messages. \
                         Total processed: {}. Cursor: {:?}",
                        self.id,
                        messages.len(),
                        state.processed_rows,
                        state.last_timestamp
                    );
                }

                let schema = if self.config.payload_column.is_some() {
                    self.payload_format().schema()
                } else {
                    Schema::Json
                };

                let persisted_state = self.serialize_state(&state);

                Ok(ProducedMessages {
                    schema,
                    messages,
                    state: persisted_state,
                })
            }
            Err(e) => {
                // Failed poll — notify circuit breaker
                self.circuit_breaker.record_failure().await;
                error!(
                    "InfluxDB source ID: {} poll failed: {e}. \
                     Consecutive failures tracked by circuit breaker.",
                    self.id
                );
                tokio::time::sleep(self.poll_interval).await;
                Err(e)
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        let state = self.state.lock().await;
        info!(
            "InfluxDB source connector ID: {} closed. Total rows processed: {}",
            self.id, state.processed_rows
        );
        Ok(())
    }
}
