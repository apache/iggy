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
use iggy_common::serde_secret::serialize_secret;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use regex::Regex;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretString};
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
// Cap for exponential backoff on per-query retries — kept short so a
// transient InfluxDB blip does not stall polling for too long
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
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
    /// `None` until `open()` is called. Wraps `reqwest::Client` with
    /// [`HttpRetryMiddleware`] so retry/back-off/jitter is handled
    /// transparently by the middleware stack instead of a hand-rolled loop.
    client: Option<ClientWithMiddleware>,
    state: Mutex<State>,
    verbose: bool,
    retry_delay: Duration,
    poll_interval: Duration,
    /// Resolved once in `new()` — avoids a `to_ascii_lowercase()` allocation
    /// on every message in the hot path.
    payload_format: PayloadFormat,
    circuit_breaker: Arc<CircuitBreaker>,
    /// Set when a persisted `ConnectorState` was provided to `new()` but could
    /// not be deserialized into `State` (e.g. schema changed after an upgrade).
    /// `open()` refuses to start when this is `true` so operators are not
    /// surprised by a silent cursor reset and full re-delivery.
    state_restore_failed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxDbSourceConfig {
    pub url: String,
    pub org: String,
    #[serde(serialize_with = "serialize_secret")]
    pub token: SecretString,
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
    // Upper cap on open() backoff delay — can be set high (e.g. "60s") for
    // patient startup without affecting per-query retry behaviour
    pub open_retry_max_delay: Option<String>,
    // Upper cap on per-query retry backoff — kept short so a transient blip
    // does not stall polling; independent of open_retry_max_delay
    pub retry_max_delay: Option<String>,
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
            Some("json") => PayloadFormat::Json,
            other => {
                warn!(
                    "Unrecognized payload_format value {:?}, falling back to JSON. \
                     Valid values are: \"json\", \"text\", \"utf8\", \"base64\", \"raw\".",
                    other
                );
                PayloadFormat::Json
            }
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
    /// How many rows at `last_timestamp` have already been delivered downstream.
    ///
    /// When the user's Flux query uses `>= $cursor`, consecutive polls may
    /// return the same rows for the current cursor timestamp.  This counter
    /// lets `poll_messages` skip those already-delivered rows and inflate
    /// `$limit` accordingly, preventing both duplicates and data loss at
    /// batch boundaries where multiple rows share the same timestamp.
    ///
    /// `#[serde(default)]` keeps existing persisted state files forward-compatible:
    /// the field defaults to 0 when the state was saved by an older version.
    #[serde(default)]
    cursor_row_count: u64,
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

/// Recognise an InfluxDB CSV header row.
///
/// A header row must contain a `_time` column. The `_value` column is
/// intentionally **not** required: Flux aggregation queries (`count()`,
/// `mean()`, `group()`) produce result tables with columns like `_count` or
/// `_mean` instead of `_value`. Requiring `_value` would cause those header
/// rows to be missed, silently skipping all subsequent data rows until the
/// next recognised header.
///
/// InfluxDB annotation rows (`#group`, `#datatype`, `#default`) are already
/// filtered out earlier in `parse_csv_rows` by the leading-`#` check, so
/// they will never reach this function.
fn is_header_record(record: &StringRecord) -> bool {
    record.iter().any(|v| v == "_time")
}

/// Compare two RFC 3339 timestamp strings chronologically.
///
/// InfluxDB strips trailing fractional-second zeros, producing timestamps like
/// `"2026-03-18T12:00:00.60952Z"` (= 609520µs).  A naïve `>` string comparison
/// treats this as *greater* than `"2026-03-18T12:00:00.609521Z"` because `'Z'`
/// (ASCII 90) > `'1'` (ASCII 49), even though the former is chronologically
/// *earlier*.  Always parse to `DateTime<Utc>` so the comparison is correct.
fn is_timestamp_after(a: &str, b: &str) -> bool {
    match (a.parse::<DateTime<Utc>>(), b.parse::<DateTime<Utc>>()) {
        (Ok(dt_a), Ok(dt_b)) => dt_a > dt_b,
        _ => a > b,
    }
}

// ---------------------------------------------------------------------------
// InfluxDbSource implementation
// ---------------------------------------------------------------------------

impl InfluxDbSource {
    pub fn new(id: u32, config: InfluxDbSourceConfig, state: Option<ConnectorState>) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let poll_interval = parse_duration(config.poll_interval.as_deref(), DEFAULT_POLL_INTERVAL);
        let payload_format = PayloadFormat::from_config(config.payload_format.as_deref());

        // Build circuit breaker from config
        let cb_threshold = config
            .circuit_breaker_threshold
            .unwrap_or(DEFAULT_CIRCUIT_BREAKER_THRESHOLD);
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down.as_deref(),
            DEFAULT_CIRCUIT_COOL_DOWN,
        );

        // Distinguish "no prior state" (fresh start, expected) from "state
        // existed but could not be deserialized" (schema mismatch after an
        // upgrade, unexpected).  Collapsing both into None via and_then() would
        // silently reset the cursor to the epoch and cause a full re-delivery.
        let (restored_state, state_restore_failed) = match state {
            None => (None, false),
            Some(s) => match s.deserialize::<State>(CONNECTOR_NAME, id) {
                Some(state) => {
                    info!(
                        "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                         Last timestamp: {:?}, processed rows: {}",
                        state.last_timestamp, state.processed_rows
                    );
                    (Some(state), false)
                }
                None => {
                    // ConnectorState::deserialize already logs at warn level;
                    // escalate to error here so the operator sees the connector
                    // ID and understands the cursor will NOT be silently reset.
                    error!(
                        "InfluxDB source ID: {id} — persisted state exists but could not \
                         be deserialized (possible schema change after upgrade). \
                         Refusing to start to prevent silent cursor reset and full \
                         re-delivery. Clear or migrate the connector state to proceed."
                    );
                    (None, true)
                }
            },
        };

        InfluxDbSource {
            id,
            config,
            client: None,
            state: Mutex::new(restored_state.unwrap_or(State {
                last_poll_time: Utc::now(),
                last_timestamp: None,
                processed_rows: 0,
                cursor_row_count: 0,
            })),
            verbose,
            retry_delay,
            poll_interval,
            payload_format,
            circuit_breaker: Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down)),
            state_restore_failed,
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn payload_format(&self) -> PayloadFormat {
        self.payload_format
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

    fn build_raw_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
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

    fn cursor_re() -> &'static Regex {
        CURSOR_RE.get_or_init(|| {
            Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
                .expect("hardcoded regex is valid")
        })
    }

    fn validate_cursor(cursor: &str) -> Result<(), Error> {
        if Self::cursor_re().is_match(cursor) {
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

    fn query_with_params(&self, cursor: &str, already_seen: u64) -> Result<String, Error> {
        // Reject anything that is not a well-formed RFC 3339 timestamp.
        // This prevents a crafted or corrupted _time value (e.g. containing
        // Flux syntax like `") |> drop() //`) from being injected into the
        // query string before it is sent to /api/v2/query.
        // Note: InfluxDB OSS v2 does not support the `params` JSON field for
        // parameterized queries (Cloud-only feature), so substitution is
        // unavoidable for OSS — validation is the correct mitigation here.
        Self::validate_cursor(cursor)?;
        // Inflate the limit so that after skipping `already_seen` rows at the
        // cursor timestamp we still return a full batch of new rows.  This is
        // a no-op when `already_seen == 0` (first poll or `>` queries).
        let batch_size = self.config.batch_size.unwrap_or(500) as u64;
        let limit = batch_size.saturating_add(already_seen).to_string();
        let mut query = self.config.query.clone();
        if query.contains("$cursor") {
            query = query.replace("$cursor", cursor);
        }
        if query.contains("$limit") {
            query = query.replace("$limit", &limit);
        }
        Ok(query)
    }

    /// Execute a Flux query against `/api/v2/query` and return the raw CSV
    /// response body. Retry/back-off is handled transparently by the
    /// `ClientWithMiddleware` stack (see `build_retry_client`).
    async fn run_query(&self, query: &str) -> Result<String, Error> {
        let client = self.get_client()?;
        let url = self.build_query_url()?;
        let token = self.config.token.expose_secret().to_owned();

        let body = json!({
            "query": query,
            "dialect": {
                "annotations": [],
                "delimiter": ",",
                "header": true,
                "commentPrefix": "#"
            }
        });

        let response = client
            .post(url)
            .header("Authorization", format!("Token {token}"))
            .header("Content-Type", "application/json")
            .header("Accept", "text/csv")
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::Storage(format!("InfluxDB query failed: {e}")))?;

        let status = response.status();
        if status.is_success() {
            return response
                .text()
                .await
                .map_err(|e| Error::Storage(format!("Failed to read query response: {e}")));
        }

        let body_text = response
            .text()
            .await
            .unwrap_or_else(|_| "failed to read response body".to_string());

        // Use PermanentHttpError for non-transient 4xx (400 Bad Request, 401
        // Unauthorized, etc.) so poll() can skip the circuit breaker for these
        // — they indicate a config/data issue, not an infrastructure failure.
        if iggy_connector_sdk::retry::is_transient_status(status) {
            Err(Error::Storage(format!(
                "InfluxDB query failed with status {status}: {body_text}"
            )))
        } else {
            Err(Error::PermanentHttpError(format!(
                "InfluxDB query failed with status {status}: {body_text}"
            )))
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

    /// Returns `(messages, max_cursor, rows_at_max_cursor, skipped)`.
    ///
    /// `rows_at_max_cursor` is the count of delivered messages whose cursor
    /// field value equals `max_cursor`.  The caller stores this in
    /// [`State::cursor_row_count`] so the next poll can skip those rows when
    /// the query uses `>= $cursor`.
    ///
    /// `skipped` is the number of rows that were elided because they fell
    /// within the already-seen window.  When the caller observes zero
    /// delivered messages but `skipped > 0`, it means every row the query
    /// returned was at the current cursor timestamp and had already been
    /// delivered.  In that case `skipped` equals the true row count at that
    /// timestamp, so the caller can correct any over-inflated
    /// `cursor_row_count` rather than getting permanently stuck.
    async fn poll_messages(
        &self,
    ) -> Result<(Vec<ProducedMessage>, Option<String>, u64, u64), Error> {
        // Read cursor and already_seen atomically from the same lock acquisition
        // so the two values are always consistent with each other.
        let (cursor, already_seen) = {
            let state = self.state.lock().await;
            let c = state
                .last_timestamp
                .clone()
                .or_else(|| self.config.initial_offset.clone())
                .unwrap_or_else(|| DEFAULT_CURSOR.to_string());
            (c, state.cursor_row_count)
        };

        let query = self.query_with_params(&cursor, already_seen).map_err(|e| {
            error!(
                "InfluxDB source ID: {} — invalid cursor, skipping poll: {e}",
                self.id
            );
            e
        })?;
        let csv_data = self.run_query(&query).await?;

        let rows = self.parse_csv_rows(&csv_data)?;
        let include_metadata = self.config.include_metadata.unwrap_or(true);
        let cursor_field = self.cursor_field().to_string();

        let mut messages = Vec::with_capacity(rows.len());
        let mut max_cursor: Option<String> = None;
        let mut rows_at_max_cursor = 0u64;
        let mut skipped = 0u64;

        for row in rows {
            // Skip rows at the current cursor that were already delivered in a
            // previous batch.  This deduplicate rows when the query uses
            // `>= $cursor` and a batch boundary landed inside a group of rows
            // sharing the same timestamp.
            if let Some(cv) = row.get(&cursor_field)
                && cv == &cursor
                && skipped < already_seen
            {
                skipped += 1;
                continue;
            }

            // Track the new max cursor and how many delivered rows share it.
            if let Some(cv) = row.get(&cursor_field) {
                match &max_cursor {
                    None => {
                        max_cursor = Some(cv.clone());
                        rows_at_max_cursor = 1;
                    }
                    Some(current) => {
                        if is_timestamp_after(cv, current) {
                            max_cursor = Some(cv.clone());
                            rows_at_max_cursor = 1;
                        } else if cv == current {
                            rows_at_max_cursor += 1;
                        }
                    }
                }
            }

            let payload = self.build_payload(&row, include_metadata)?;
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

        Ok((messages, max_cursor, rows_at_max_cursor, skipped))
    }
}

// ---------------------------------------------------------------------------
// Source trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Source for InfluxDbSource {
    async fn open(&mut self) -> Result<(), Error> {
        if self.state_restore_failed {
            return Err(Error::InvalidState);
        }

        info!(
            "Opening InfluxDB source connector with ID: {}. Org: {}",
            self.id, self.config.org
        );

        // Build the raw client first and use it for the startup connectivity
        // check. The connectivity retry loop uses separate delay bounds
        // (open_retry_max_delay) from the per-query middleware retries, so
        // we keep them independent.
        let raw_client = self.build_raw_client()?;

        // Validate cursor_field before touching the network: string comparison
        // is only safe for timestamp columns. See validate_cursor_field for details.
        Self::validate_cursor_field(self.cursor_field())?;

        let health_url = self.build_health_url()?;
        check_connectivity_with_retry(
            &raw_client,
            health_url,
            "InfluxDB source",
            self.id,
            &ConnectivityConfig {
                max_open_retries: self
                    .config
                    .max_open_retries
                    .unwrap_or(DEFAULT_MAX_OPEN_RETRIES),
                open_retry_max_delay: parse_duration(
                    self.config.open_retry_max_delay.as_deref(),
                    DEFAULT_OPEN_RETRY_MAX_DELAY,
                ),
                retry_delay: self.retry_delay,
            },
        )
        .await?;

        // Wrap in the retry middleware for all subsequent query operations.
        // The middleware handles transient 429 / 5xx retries with
        // exponential back-off, jitter, and Retry-After header support.
        let max_retries = self.get_max_retries();
        let query_retry_max_delay = parse_duration(
            self.config.retry_max_delay.as_deref(),
            DEFAULT_RETRY_MAX_DELAY,
        );
        self.client = Some(build_retry_client(
            raw_client,
            max_retries,
            self.retry_delay,
            query_retry_max_delay,
            "InfluxDB",
        ));

        info!(
            "InfluxDB source connector with ID: {} opened successfully",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        // Skip query if circuit breaker is open; sleep so the runtime does not
        // spin-call poll() in a hot loop while the circuit is held open.
        if self.circuit_breaker.is_open().await {
            warn!(
                "InfluxDB source ID: {} — circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            tokio::time::sleep(self.poll_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        match self.poll_messages().await {
            Ok((messages, max_cursor, rows_at_max_cursor, skipped)) => {
                // Successful poll — reset circuit breaker
                self.circuit_breaker.record_success();

                let mut state = self.state.lock().await;
                state.last_poll_time = Utc::now();
                state.processed_rows += messages.len() as u64;
                match max_cursor {
                    Some(ref new_cursor)
                        if state.last_timestamp.as_deref() != Some(new_cursor.as_str()) =>
                    {
                        // Cursor advanced to a new timestamp — reset the row counter.
                        state.last_timestamp = max_cursor.clone();
                        state.cursor_row_count = rows_at_max_cursor;
                    }
                    Some(_) => {
                        // Cursor stayed at the same timestamp — accumulate so the
                        // next poll skips all already-delivered rows at this timestamp.
                        state.cursor_row_count =
                            state.cursor_row_count.saturating_add(rows_at_max_cursor);
                    }
                    None => {
                        // No rows delivered.  If we skipped some rows it means
                        // every row in the result was at the current cursor
                        // timestamp and had already been seen.  `skipped` is
                        // therefore the true row count at that timestamp for
                        // this query result, so we correct cursor_row_count to
                        // that value.  This prevents a permanently-inflated
                        // counter (e.g. after rows are deleted or compacted in
                        // InfluxDB) from causing the skip logic to over-skip on
                        // every subsequent poll and stall the connector.
                        if skipped > 0 {
                            state.cursor_row_count = skipped;
                        }
                    }
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
                // Only count transient/connectivity failures toward the
                // circuit breaker. PermanentHttpError (400, 401, etc.) are
                // config/data issues that retrying will not fix.
                if !matches!(e, Error::PermanentHttpError(_)) {
                    self.circuit_breaker.record_failure().await;
                }
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_config() -> InfluxDbSourceConfig {
        InfluxDbSourceConfig {
            url: "http://localhost:8086".to_string(),
            org: "test_org".to_string(),
            token: SecretString::from("test_token"),
            query: r#"from(bucket:"b") |> range(start: $cursor) |> limit(n: $limit)"#.to_string(),
            poll_interval: Some("1s".to_string()),
            batch_size: Some(100),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: Some(true),
            verbose_logging: None,
            max_retries: Some(3),
            retry_delay: Some("100ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(3),
            open_retry_max_delay: Some("5s".to_string()),
            retry_max_delay: Some("1s".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
        }
    }

    fn make_source() -> InfluxDbSource {
        InfluxDbSource::new(1, make_config(), None)
    }

    // ── validate_cursor ──────────────────────────────────────────────────

    #[test]
    fn validate_cursor_accepts_valid_rfc3339() {
        assert!(InfluxDbSource::validate_cursor("2024-01-15T10:30:00Z").is_ok());
        assert!(InfluxDbSource::validate_cursor("2024-01-15T10:30:00.123456789Z").is_ok());
        assert!(InfluxDbSource::validate_cursor("2024-01-15T10:30:00+05:30").is_ok());
        assert!(InfluxDbSource::validate_cursor("1970-01-01T00:00:00Z").is_ok());
    }

    #[test]
    fn validate_cursor_rejects_flux_injection_characters() {
        // pipe, quote, parenthesis, space, slash are Flux syntax characters
        assert!(InfluxDbSource::validate_cursor(r#"") |> drop() //"#).is_err());
        assert!(InfluxDbSource::validate_cursor("2024-01-15 10:30:00Z").is_err());
        assert!(InfluxDbSource::validate_cursor("2024/01/15T10:30:00Z").is_err());
        assert!(InfluxDbSource::validate_cursor("not-a-timestamp").is_err());
    }

    #[test]
    fn validate_cursor_rejects_empty_string() {
        assert!(InfluxDbSource::validate_cursor("").is_err());
    }

    #[test]
    fn validate_cursor_rejects_date_only() {
        // Missing time component
        assert!(InfluxDbSource::validate_cursor("2024-01-15").is_err());
    }

    // ── validate_cursor_field ────────────────────────────────────────────

    #[test]
    fn validate_cursor_field_accepts_time_columns() {
        assert!(InfluxDbSource::validate_cursor_field("_time").is_ok());
        assert!(InfluxDbSource::validate_cursor_field("time").is_ok());
    }

    #[test]
    fn validate_cursor_field_rejects_non_timestamp_columns() {
        assert!(InfluxDbSource::validate_cursor_field("_value").is_err());
        assert!(InfluxDbSource::validate_cursor_field("sensor_id").is_err());
        assert!(InfluxDbSource::validate_cursor_field("temperature").is_err());
        assert!(InfluxDbSource::validate_cursor_field("").is_err());
    }

    // ── parse_scalar ─────────────────────────────────────────────────────

    #[test]
    fn parse_scalar_empty_is_null() {
        assert_eq!(parse_scalar(""), serde_json::Value::Null);
    }

    #[test]
    fn parse_scalar_booleans() {
        assert_eq!(parse_scalar("true"), serde_json::Value::Bool(true));
        assert_eq!(parse_scalar("false"), serde_json::Value::Bool(false));
    }

    #[test]
    fn parse_scalar_integers() {
        assert_eq!(parse_scalar("42"), serde_json::Value::Number(42.into()));
        assert_eq!(
            parse_scalar("-7"),
            serde_json::Value::Number((-7i64).into())
        );
    }

    #[test]
    fn parse_scalar_floats() {
        match parse_scalar("1.5") {
            serde_json::Value::Number(n) => {
                let v = n.as_f64().unwrap();
                assert!((v - 1.5).abs() < 1e-10);
            }
            other => panic!("expected Number, got {other:?}"),
        }
    }

    #[test]
    fn parse_scalar_strings() {
        assert_eq!(
            parse_scalar("hello"),
            serde_json::Value::String("hello".to_string())
        );
        // "True" is not a bool (case-sensitive)
        assert_eq!(
            parse_scalar("True"),
            serde_json::Value::String("True".to_string())
        );
    }

    // ── is_timestamp_after ───────────────────────────────────────────────

    #[test]
    fn is_timestamp_after_compares_chronologically_not_lexicographically() {
        // "2026-03-18T12:00:00.60952Z" = 609520µs (chronologically earlier)
        // "2026-03-18T12:00:00.609521Z" = 609521µs (chronologically later)
        // A naive string compare would say the first is > second (Z > 1).
        let earlier = "2026-03-18T12:00:00.60952Z";
        let later = "2026-03-18T12:00:00.609521Z";
        assert!(
            is_timestamp_after(later, earlier),
            "later timestamp should be after earlier"
        );
        assert!(
            !is_timestamp_after(earlier, later),
            "earlier should not be after later"
        );
    }

    #[test]
    fn is_timestamp_after_equal_timestamps() {
        let ts = "2024-01-15T10:30:00Z";
        assert!(!is_timestamp_after(ts, ts));
    }

    // ── parse_csv_rows ───────────────────────────────────────────────────

    #[test]
    fn parse_csv_rows_empty_string_returns_empty() {
        let source = make_source();
        let rows = source.parse_csv_rows("").unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_csv_rows_skips_annotation_rows() {
        let source = make_source();
        // Annotation rows must have the same field count as data rows for the CSV
        // reader to accept them.  InfluxDB always emits uniformly-wide rows.
        let csv = "#group,false\n#datatype,string\n_time,_value\n2024-01-01T00:00:00Z,42\n";
        let rows = source.parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_value").map(String::as_str), Some("42"));
    }

    #[test]
    fn parse_csv_rows_skips_blank_lines() {
        let source = make_source();
        // Two data records separated by a blank line (multi-table CSV format)
        let csv = "_time,_value\n2024-01-01T00:00:00Z,1\n\n_time,_value\n2024-01-01T00:00:01Z,2\n";
        let rows = source.parse_csv_rows(csv).unwrap();
        // Both data rows should be parsed (second header line is skipped)
        assert_eq!(rows.len(), 2, "expected 2 data rows, got {}", rows.len());
    }

    #[test]
    fn parse_csv_rows_skips_repeated_header_rows() {
        let source = make_source();
        // Same header appears twice (InfluxDB multi-table result format)
        let csv = "_time,_value\n2024-01-01T00:00:00Z,10\n_time,_value\n2024-01-01T00:00:01Z,20\n";
        let rows = source.parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parse_csv_rows_handles_empty_value_columns() {
        let source = make_source();
        // Data row with an empty field value (column present but blank).
        // The CSV reader requires uniform field counts, so we keep all 3 columns.
        let csv = "_time,_value,_measurement\n2024-01-01T00:00:00Z,42,\n";
        let rows = source.parse_csv_rows(csv).unwrap();
        assert_eq!(rows.len(), 1);
        // _measurement is present but empty
        assert_eq!(
            rows[0].get("_measurement").map(String::as_str),
            Some(""),
            "empty column value should be stored as empty string"
        );
    }

    // ── build_payload ────────────────────────────────────────────────────

    #[test]
    fn build_payload_missing_column_returns_error() {
        let mut config = make_config();
        config.payload_column = Some("data".to_string());
        let source = InfluxDbSource::new(1, config, None);

        let row: HashMap<String, String> =
            [("_time".to_string(), "2024-01-01T00:00:00Z".to_string())]
                .into_iter()
                .collect();

        let result = source.build_payload(&row, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("data") || err.contains("Missing"),
            "error should mention missing column: {err}"
        );
    }

    #[test]
    fn build_payload_invalid_base64_returns_error() {
        let mut config = make_config();
        config.payload_column = Some("data".to_string());
        config.payload_format = Some("raw".to_string()); // raw = base64 decode
        let source = InfluxDbSource::new(1, config, None);

        let row: HashMap<String, String> =
            [("data".to_string(), "not-valid-base64!!!".to_string())]
                .into_iter()
                .collect();

        let result = source.build_payload(&row, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("base64") || err.contains("decode"),
            "error should mention base64: {err}"
        );
    }

    #[test]
    fn build_payload_invalid_json_returns_error() {
        let mut config = make_config();
        config.payload_column = Some("data".to_string());
        config.payload_format = Some("json".to_string());
        let source = InfluxDbSource::new(1, config, None);

        let row: HashMap<String, String> = [("data".to_string(), "{{not valid json}}".to_string())]
            .into_iter()
            .collect();

        let result = source.build_payload(&row, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("JSON") || err.contains("json"),
            "error should mention JSON: {err}"
        );
    }

    #[test]
    fn build_payload_valid_base64_decodes_correctly() {
        let mut config = make_config();
        config.payload_column = Some("data".to_string());
        config.payload_format = Some("raw".to_string());
        let source = InfluxDbSource::new(1, config, None);

        // base64("hello") = "aGVsbG8="
        let row: HashMap<String, String> = [("data".to_string(), "aGVsbG8=".to_string())]
            .into_iter()
            .collect();

        let result = source.build_payload(&row, true).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn build_payload_text_column_returns_bytes() {
        let mut config = make_config();
        config.payload_column = Some("data".to_string());
        config.payload_format = Some("text".to_string());
        let source = InfluxDbSource::new(1, config, None);

        let row: HashMap<String, String> = [("data".to_string(), "hello world".to_string())]
            .into_iter()
            .collect();

        let result = source.build_payload(&row, true).unwrap();
        assert_eq!(result, b"hello world");
    }

    #[test]
    fn build_payload_whole_row_wraps_measurement_and_value() {
        let source = make_source(); // no payload_column
        let row: HashMap<String, String> = [
            ("_measurement".to_string(), "temperature".to_string()),
            ("_field".to_string(), "v".to_string()),
            ("_time".to_string(), "2024-01-01T00:00:00Z".to_string()),
            ("_value".to_string(), "21.5".to_string()),
        ]
        .into_iter()
        .collect();

        let bytes = source.build_payload(&row, true).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["measurement"], "temperature");
        assert_eq!(parsed["timestamp"], "2024-01-01T00:00:00Z");
        // _value "21.5" → parsed as f64
        assert!(parsed["value"].is_number());
    }

    #[test]
    fn build_payload_include_metadata_false_filters_fields() {
        let source = make_source();
        let row: HashMap<String, String> = [
            ("_measurement".to_string(), "temp".to_string()),
            ("_field".to_string(), "v".to_string()),
            ("_time".to_string(), "2024-01-01T00:00:00Z".to_string()),
            ("_value".to_string(), "42".to_string()),
            ("host".to_string(), "server1".to_string()), // extra annotation column
        ]
        .into_iter()
        .collect();

        let bytes = source.build_payload(&row, false).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        // With include_metadata=false, only _value/_time/_measurement go into row
        let row_obj = parsed["row"].as_object().unwrap();
        // "host" is an annotation column — should be excluded
        assert!(
            !row_obj.contains_key("host"),
            "annotation columns should be excluded when include_metadata=false"
        );
        // Core columns should still be present
        assert!(row_obj.contains_key("_value") || row_obj.contains_key("_time"));
    }

    // ── circuit breaker integration ──────────────────────────────────────

    #[tokio::test]
    async fn poll_returns_empty_when_circuit_is_open() {
        let mut config = make_config();
        config.circuit_breaker_threshold = Some(1);
        config.circuit_breaker_cool_down = Some("60s".to_string());
        // Use a short poll_interval so the circuit-open sleep does not stall
        // the test suite for a full second.
        config.poll_interval = Some("1ms".to_string());
        let source = InfluxDbSource::new(1, config, None);

        // Force the circuit open
        source.circuit_breaker.record_failure().await;
        assert!(source.circuit_breaker.is_open().await);

        let result = source.poll().await;
        assert!(result.is_ok(), "poll should return Ok when circuit is open");
        let produced = result.unwrap();
        assert!(
            produced.messages.is_empty(),
            "no messages should be produced when circuit is open"
        );
    }

    // ── query_with_params — limit inflation ──────────────────────────────

    #[test]
    fn query_with_params_inflates_limit_by_already_seen() {
        let mut config = make_config();
        config.batch_size = Some(10);
        config.query =
            "from(bucket:\"b\") |> range(start: $cursor) |> limit(n: $limit)".to_string();
        let source = InfluxDbSource::new(1, config, None);

        // With already_seen=5, limit should be 10+5=15
        let q = source.query_with_params("2024-01-01T00:00:00Z", 5).unwrap();
        assert!(q.contains("limit(n: 15)"), "inflated limit not found: {q}");
    }

    #[test]
    fn query_with_params_no_inflation_when_already_seen_is_zero() {
        let mut config = make_config();
        config.batch_size = Some(100);
        config.query =
            "from(bucket:\"b\") |> range(start: $cursor) |> limit(n: $limit)".to_string();
        let source = InfluxDbSource::new(1, config, None);

        let q = source.query_with_params("2024-01-01T00:00:00Z", 0).unwrap();
        assert!(
            q.contains("limit(n: 100)"),
            "limit should be batch_size: {q}"
        );
    }

    // ── close() ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn close_drops_client() {
        let mut source = make_source();
        let result = source.close().await;
        assert!(result.is_ok());
        assert!(source.client.is_none(), "client should be None after close");
    }

    // ── cursor_row_count correction (fix: inflated counter reset) ────────

    #[tokio::test]
    async fn cursor_row_count_corrected_when_inflated_above_actual_row_count_at_cursor() {
        use std::io::{Read, Write};
        use std::net::TcpListener as StdTcpListener;

        // The server returns 3 rows, all at the cursor timestamp.
        // The source starts with cursor_row_count = 5 (inflated – e.g. rows
        // deleted from InfluxDB after delivery).  All 3 returned rows will be
        // skipped (skipped=3 < already_seen=5).  The None branch must correct
        // cursor_row_count to 3 instead of leaving it at the inflated 5.
        let t = "2024-01-01T00:00:00Z";
        let csv = format!("_time,_value\n{t},1\n{t},2\n{t},3\n");
        let http_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            csv.len(),
            csv
        );

        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(http_response.as_bytes());
            }
        });

        let initial_state = State {
            last_poll_time: Utc::now(),
            last_timestamp: Some(t.to_string()),
            processed_rows: 0,
            cursor_row_count: 5, // inflated: actual rows at T are only 3
        };
        let persisted = ConnectorState::serialize(&initial_state, CONNECTOR_NAME, 1).unwrap();

        let mut config = make_config();
        config.url = format!("http://127.0.0.1:{port}");
        config.batch_size = Some(10);
        let mut source = InfluxDbSource::new(1, config, Some(persisted));

        // Inject a real HTTP client directly, bypassing open()'s health check.
        let raw = source.build_raw_client().unwrap();
        source.client = Some(build_retry_client(
            raw,
            0,
            Duration::from_millis(0),
            Duration::from_millis(0),
            "InfluxDB",
        ));

        assert_eq!(
            source.state.lock().await.cursor_row_count,
            5,
            "pre-condition: cursor_row_count starts at inflated value"
        );

        // poll() → server returns 3 rows at T, all skipped (already_seen=5 > 3)
        // → (messages=[], max_cursor=None, rows_at_max_cursor=0, skipped=3)
        // → None branch corrects cursor_row_count to skipped (3).
        let result = source.poll().await;
        assert!(result.is_ok(), "poll should succeed: {:?}", result);
        assert!(
            result.unwrap().messages.is_empty(),
            "all rows were already seen – no messages expected"
        );

        assert_eq!(
            source.state.lock().await.cursor_row_count,
            3,
            "cursor_row_count must be corrected to actual row count (3), not left at inflated (5)"
        );
    }

    // ── state restore failure (fix: deserialization failure fails open) ──

    #[tokio::test]
    async fn open_returns_invalid_state_when_persisted_state_cannot_be_deserialized() {
        // Garbage bytes will cause ConnectorState::deserialize to fail.
        // new() must set state_restore_failed=true, and open() must return
        // Err(InvalidState) before attempting any network calls, so the
        // operator sees a hard failure instead of a silent cursor reset.
        let garbage = ConnectorState(vec![0xFF, 0xFE, 0xFD, 0xAA, 0xBB]);
        let mut source = InfluxDbSource::new(1, make_config(), Some(garbage));

        let result = source.open().await;
        assert!(
            matches!(result, Err(Error::InvalidState)),
            "open() must return Err(InvalidState) on state deserialization failure, got: {:?}",
            result
        );
    }

    #[test]
    fn fresh_start_with_no_prior_state_does_not_set_restore_failed() {
        // When no prior ConnectorState is supplied (first boot), state_restore_failed
        // must be false so that open() is not blocked on a normal first run.
        let source = InfluxDbSource::new(1, make_config(), None);
        assert!(
            !source.state_restore_failed,
            "state_restore_failed must be false when no prior state exists"
        );
    }

    // ── payload_format ───────────────────────────────────────────────────

    #[test]
    fn payload_format_aliases() {
        assert_eq!(
            PayloadFormat::from_config(Some("utf8")),
            PayloadFormat::Text
        );
        assert_eq!(
            PayloadFormat::from_config(Some("base64")),
            PayloadFormat::Raw
        );
        assert_eq!(PayloadFormat::from_config(None), PayloadFormat::Json);
    }

    #[test]
    fn payload_format_schema_mapping() {
        assert_eq!(PayloadFormat::Json.schema(), Schema::Json);
        assert_eq!(PayloadFormat::Text.schema(), Schema::Text);
        assert_eq!(PayloadFormat::Raw.schema(), Schema::Raw);
    }
}
