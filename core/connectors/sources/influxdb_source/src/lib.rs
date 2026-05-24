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

mod common;
mod row;
mod v2;
mod v3;

use crate::common::{
    is_timestamp_after, query_has_cursor_placeholder, query_has_offset_placeholder,
    strip_block_comments,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use common::{
    InfluxDbSourceConfig, PayloadFormat, PersistedState, V2State, V3State, validate_cursor,
    validate_cursor_field,
};
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Schema, Source, source_connector,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use secrecy::{ExposeSecret, SecretBox};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

source_connector!(InfluxDbSource);

const CONNECTOR_NAME: &str = "InfluxDB source";
const MAX_STATE_SERIALIZE_FAILURES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

// ── Connector state ───────────────────────────────────────────────────────────

#[derive(Debug)]
enum VersionState {
    V2(Mutex<V2State>),
    V3(Mutex<V3State>),
}

// ── Connector struct ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct InfluxDbSource {
    id: u32,
    config: InfluxDbSourceConfig,
    client: Option<ClientWithMiddleware>,
    version_state: VersionState,
    payload_format: PayloadFormat,
    poll_interval: Duration,
    retry_delay: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
    auth_header: Option<SecretBox<String>>,
    /// `Some(cause)` when state restore was rejected; `None` means restore succeeded.
    state_restore_error: Option<String>,
    /// Consecutive `ConnectorState::serialize` failures since last success.
    state_serialize_failures: AtomicU32,
}

impl InfluxDbSource {
    pub fn new(id: u32, config: InfluxDbSourceConfig, state: Option<ConnectorState>) -> Self {
        let retry_delay = parse_duration(config.retry_delay(), DEFAULT_RETRY_DELAY);
        let poll_interval = parse_duration(config.poll_interval(), DEFAULT_POLL_INTERVAL);
        let payload_format = PayloadFormat::from_config(config.payload_format());

        let cb_threshold = config.circuit_breaker_threshold();
        let cb_cool_down = parse_duration(
            config.circuit_breaker_cool_down(),
            DEFAULT_CIRCUIT_COOL_DOWN,
        );
        let circuit_breaker = Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down));

        let (version_state, state_restore_error) = match &config {
            InfluxDbSourceConfig::V2(_) => {
                let (s, err) = restore_v2_state(id, state);
                (VersionState::V2(Mutex::new(s)), err)
            }
            InfluxDbSourceConfig::V3(_) => {
                let (s, err) = restore_v3_state(id, state);
                (VersionState::V3(Mutex::new(s)), err)
            }
        };

        InfluxDbSource {
            id,
            config,
            client: None,
            version_state,
            payload_format,
            poll_interval,
            retry_delay,
            circuit_breaker,
            auth_header: None,
            state_restore_error,
            state_serialize_failures: AtomicU32::new(0),
        }
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client not initialized".to_string()))
    }
}

// ── State restore helpers ─────────────────────────────────────────────────────

fn restore_v2_state(id: u32, state: Option<ConnectorState>) -> (V2State, Option<String>) {
    let Some(cs) = state else {
        return (V2State::default(), None);
    };
    // Clone bytes before consuming cs — needed for the legacy fallback path below.
    let raw_bytes = cs.0.clone();
    match cs.deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V2(s)) => {
            if let Some(ref ts) = s.last_timestamp
                && let Err(e) = validate_cursor(ts)
            {
                let cause = format!(
                    "persisted V2 cursor {ts:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V2State::default(), Some(cause));
            }

            info!(
                "{CONNECTOR_NAME} ID {id}: restored V2 state — \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, None)
        }
        Some(PersistedState::V3(_)) => {
            let cause = "persisted state is V3 but connector is configured as V2. \
                 Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed.";

            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V2State::default(), Some(cause.to_string()))
        }
        None => {
            // PersistedState deserialization failed. Try the legacy format: direct V2State
            // bytes without the PersistedState wrapper, written by an older connector version.
            if let Some(s) = ConnectorState(raw_bytes).deserialize::<V2State>(CONNECTOR_NAME, id) {
                if let Some(ref ts) = s.last_timestamp
                    && let Err(e) = validate_cursor(ts)
                {
                    let cause = format!(
                        "legacy V2 cursor {ts:?} failed validation: {e}. \
                             Clear or migrate the connector state to recover."
                    );
                    error!("{CONNECTOR_NAME} ID {id}: {cause}");
                    return (V2State::default(), Some(cause));
                }
                info!(
                    "{CONNECTOR_NAME} ID {id}: restored V2 state from legacy format — \
                     last_timestamp={:?}, processed_rows={}",
                    s.last_timestamp, s.processed_rows
                );
                return (s, None);
            }
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V2State::default(), Some(cause))
        }
    }
}

fn restore_v3_state(id: u32, state: Option<ConnectorState>) -> (V3State, Option<String>) {
    let Some(cs) = state else {
        return (V3State::default(), None);
    };
    match cs.deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V3(s)) => {
            if let Some(ref ts) = s.last_timestamp
                && let Err(e) = validate_cursor(ts)
            {
                let cause = format!(
                    "persisted V3 cursor {ts:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V3State::default(), Some(cause));
            }
            if let Some(ref sc) = s.stuck_cursor
                && let Err(e) = validate_cursor(sc)
            {
                let cause = format!(
                    "persisted V3 stuck_cursor {sc:?} failed validation: {e}. \
                         Clear or migrate the connector state to recover."
                );
                error!("{CONNECTOR_NAME} ID {id}: {cause}");
                return (V3State::default(), Some(cause));
            }

            info!(
                "{CONNECTOR_NAME} ID {id}: restored V3 state — \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, None)
        }
        Some(PersistedState::V2(_)) => {
            let cause = "persisted state is V2 but connector is configured as V3. \
                 Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed.";
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V3State::default(), Some(cause.to_string()))
        }
        None => {
            let cause = "persisted state exists but could not be deserialized. \
                         Refusing to start to prevent silent cursor reset."
                .to_string();
            error!("{CONNECTOR_NAME} ID {id}: {cause}");
            (V3State::default(), Some(cause))
        }
    }
}

// ── Source trait ──────────────────────────────────────────────────────────────

#[async_trait]
impl Source for InfluxDbSource {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(ref cause) = self.state_restore_error {
            return Err(Error::InitError(format!("state restore failed: {cause}")));
        }

        let ver = self.config.version_label();
        info!(
            "Opening {CONNECTOR_NAME} with ID: {} (version={ver})",
            self.id
        );

        validate_cursor_field(self.config.cursor_field(), self.config.version_label())?;
        if let Some(offset) = self.config.initial_offset() {
            validate_cursor(offset)?;
        }

        // #10 — non-empty required fields
        match &self.config {
            InfluxDbSourceConfig::V2(c) if c.org.trim().is_empty() => {
                return Err(Error::InvalidConfigValue(
                    "V2 source config requires a non-empty 'org'".into(),
                ));
            }
            InfluxDbSourceConfig::V3(c) if c.db.trim().is_empty() => {
                return Err(Error::InvalidConfigValue(
                    "V3 source config requires a non-empty 'db'".into(),
                ));
            }
            _ => {}
        }

        // #11 — $cursor placeholder must be present; without it apply_query_params
        // never substitutes the cursor and the same rows are re-delivered every poll.
        let query = match &self.config {
            InfluxDbSourceConfig::V2(c) => c.query.as_str(),
            InfluxDbSourceConfig::V3(c) => c.query.as_str(),
        };
        if !query_has_cursor_placeholder(query) {
            return Err(Error::InvalidConfigValue(
                "query must contain the '$cursor' placeholder — without it the connector \
                 cannot advance and will re-deliver the same rows on every poll."
                    .into(),
            ));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && let Some(cap) = cfg.stuck_batch_cap_factor
            && cap > v3::MAX_STUCK_CAP_FACTOR
        {
            return Err(Error::InvalidConfigValue(format!(
                "stuck_batch_cap_factor {cap} exceeds maximum of {}; \
                 reduce it to avoid querying up to {cap}×batch_size rows per poll.",
                v3::MAX_STUCK_CAP_FACTOR
            )));
        }

        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && cfg.stuck_batch_cap_factor == Some(1)
        {
            return Err(Error::InvalidConfigValue(
                "stuck_batch_cap_factor 1 is invalid — cap equals base_batch so \
                 the connector trips the circuit breaker on the first stuck poll \
                 with no inflation at all; use 0 to disable stuck detection or >= 2."
                    .into(),
            ));
        }

        // V3 stuck-batch inflation writes last_timestamp_row_offset to state and
        // passes it as `$offset` on the next poll so already-seen rows at the same
        // timestamp are skipped. If the query template lacks `$offset`, apply_query_params
        // silently no-ops and the same head rows are re-fetched and re-emitted on
        // every poll — duplicate delivery with no error.
        if let InfluxDbSourceConfig::V3(cfg) = &self.config {
            let cap = cfg
                .stuck_batch_cap_factor
                .unwrap_or(v3::DEFAULT_STUCK_CAP_FACTOR);
            if cap > 0 && !query_has_offset_placeholder(&cfg.query) {
                return Err(Error::InvalidConfigValue(
                    "V3 source query must contain the '$offset' placeholder when \
                     stuck_batch_cap_factor > 0 (the default). Add 'OFFSET $offset' \
                     to your query to prevent duplicate delivery during stuck-batch \
                     inflation. For simple queries add it at the top level: \
                     \"WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset\". \
                     For queries with subqueries or CTEs, $offset must appear on the \
                     innermost SELECT that filters by time."
                        .into(),
                ));
            }
            // DataFusion does not guarantee stable row ordering for tied timestamps
            // unless ORDER BY is present. OFFSET-based dedup relies on consistent ordering:
            // without it, different rows may be skipped on each poll, causing silent data
            // loss or re-delivery.
            if cap > 0 && !query_has_order_by(&cfg.query) {
                return Err(Error::InvalidConfigValue(
                    "V3 source query must contain an ORDER BY clause when \
                     stuck_batch_cap_factor > 0 (the default). DataFusion does not \
                     guarantee stable row ordering for tied timestamps without ORDER BY; \
                     OFFSET-based dedup relies on consistent ordering. Add \
                     'ORDER BY <cursor_field>' to your query, for example: \
                     \"SELECT * FROM <measurement> WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset\"."
                        .into(),
                ));
            }
        }

        // Skip-N dedup for V2 requires rows to arrive sorted by time. If the Flux
        // query uses `>=` semantics (inclusive cursor) without an explicit sort,
        // InfluxDB may return rows in storage order, causing skip-N to silently
        // skip the wrong rows and produce incorrect output. Hard-error so operators
        // don't discover this only after data loss. Queries using strict `>` do not
        // need skip-N and are not affected.
        if let InfluxDbSourceConfig::V2(cfg) = &self.config
            && cfg.query.contains(">=")
            && !query_has_sort_call(&cfg.query)
        {
            return Err(Error::InvalidConfigValue(format!(
                "{CONNECTOR_NAME} ID: {}: V2 query uses '>=' (inclusive cursor) but does \
                 not contain `|> sort(columns: [\"_time\"])`. Skip-N dedup is \
                 order-dependent; without sorting, InfluxDB may return rows in storage \
                 order and the wrong rows will be silently skipped. \
                 Add `|> sort(columns: [\"_time\"])` before `|> limit(...)` in your query.",
                self.id
            )));
        }

        // #12 — V3 uses strict `> $cursor` semantics. If the query uses
        // `>= '$cursor'`, rows at the cursor timestamp are re-fetched and
        // re-delivered on the next poll (there is no skip-N dedup for V3).
        if let InfluxDbSourceConfig::V3(cfg) = &self.config
            && (cfg.query.contains(">= '$cursor'") || cfg.query.contains(">='$cursor'"))
        {
            return Err(Error::InvalidConfigValue(
                "V3 source query uses '>= $cursor' (inclusive). V3 uses strict \
                 '> $cursor' semantics — an inclusive cursor causes rows at the \
                 cursor timestamp to be re-delivered on the next poll. \
                 Change '>=' to '>' in your query."
                    .into(),
            ));
        }

        let timeout = parse_duration(self.config.timeout(), DEFAULT_TIMEOUT);
        let raw_client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))?;

        let health_url = Url::parse(&format!("{}/health", self.config.base_url()))
            .map_err(|e| Error::InvalidConfigValue(format!("Invalid InfluxDB URL: {e}")))?;

        check_connectivity_with_retry(
            &raw_client,
            health_url,
            CONNECTOR_NAME,
            self.id,
            &ConnectivityConfig {
                max_open_retries: self.config.max_open_retries(),
                open_retry_max_delay: parse_duration(
                    self.config.open_retry_max_delay(),
                    DEFAULT_OPEN_RETRY_MAX_DELAY,
                ),
                retry_delay: self.retry_delay,
            },
        )
        .await?;

        let query_retry_max_delay =
            parse_duration(self.config.retry_max_delay(), DEFAULT_RETRY_MAX_DELAY);
        self.client = Some(build_retry_client(
            raw_client,
            self.config.max_retries(),
            self.retry_delay,
            query_retry_max_delay,
            "InfluxDB",
        ));

        let token = self.config.token_secret().expose_secret();
        self.auth_header = Some(SecretBox::new(Box::new(match &self.config {
            InfluxDbSourceConfig::V2(_) => format!("Token {token}"),
            InfluxDbSourceConfig::V3(_) => format!("Bearer {token}"),
        })));

        info!(
            "{CONNECTOR_NAME} ID: {} opened successfully (version={ver})",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        if self.circuit_breaker.is_open().await {
            warn!(
                "{CONNECTOR_NAME} ID: {} — circuit breaker is OPEN. Skipping poll.",
                self.id
            );
            // TODO: sleep for the CB's remaining cool-down duration rather than
            // poll_interval to avoid wakeup churn. Requires CircuitBreaker to
            // expose a remaining_cool_down() method (SDK change).
            tokio::time::sleep(self.poll_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        let client = self.get_client()?;
        let auth = self
            .auth_header
            .as_ref()
            .map(|s| s.expose_secret().as_str())
            .ok_or_else(|| {
                Error::Connection("auth_header not initialised — was open() called?".to_string())
            })?;
        match &self.version_state {
            VersionState::V2(state_mu) => {
                let InfluxDbSourceConfig::V2(cfg) = &self.config else {
                    return Err(Error::InvalidState);
                };

                let state_snap = state_mu.lock().await.clone();
                match v2::poll(
                    client,
                    cfg,
                    auth,
                    &state_snap,
                    self.payload_format,
                    self.config.include_metadata(),
                )
                .await
                {
                    Ok(result) => {
                        self.circuit_breaker.record_success();
                        let messages = result.messages;
                        let schema = result.schema;
                        let mut state = state_mu.lock().await;
                        state.processed_rows += messages.len() as u64;
                        apply_v2_cursor_advance(
                            &mut state,
                            result.max_cursor,
                            result.rows_at_max_cursor,
                            result.skipped,
                        );

                        if self.config.verbose_logging() {
                            info!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V2). \
                                 Total: {}. Cursor: {:?}",
                                self.id,
                                messages.len(),
                                state.processed_rows,
                                state.last_timestamp
                            );
                        } else {
                            debug!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V2). \
                                 Total: {}. Cursor: {:?}",
                                self.id,
                                messages.len(),
                                state.processed_rows,
                                state.last_timestamp
                            );
                        }

                        let persisted = ConnectorState::serialize(
                            &PersistedState::V2(state.clone()),
                            CONNECTOR_NAME,
                            self.id,
                        );
                        match &persisted {
                            Some(_) => {
                                self.state_serialize_failures.store(0, Ordering::Relaxed);
                            }
                            None => {
                                let failures = self
                                    .state_serialize_failures
                                    .fetch_add(1, Ordering::Relaxed)
                                    + 1;
                                if failures >= MAX_STATE_SERIALIZE_FAILURES {
                                    // Trip the CB to surface the persistence fault, but
                                    // still deliver messages to maintain at-least-once.
                                    // state:None means disk cursor lags; a process restart
                                    // will re-deliver from the last successfully persisted cursor.
                                    self.circuit_breaker.record_failure().await;
                                    return Ok(ProducedMessages {
                                        schema,
                                        messages,
                                        state: None,
                                    });
                                }
                                warn!(
                                    "{CONNECTOR_NAME} ID: {} — state serialization failed \
                                     ({failures}/{MAX_STATE_SERIALIZE_FAILURES}); \
                                     cursor will not be persisted. Messages were emitted; \
                                     restart may cause re-delivery.",
                                    self.id
                                );
                            }
                        }

                        Ok(ProducedMessages {
                            schema,
                            messages,
                            state: persisted,
                        })
                    }
                    Err(e) => self.handle_poll_error(e).await,
                }
            }

            VersionState::V3(state_mu) => {
                let InfluxDbSourceConfig::V3(cfg) = &self.config else {
                    return Err(Error::InvalidState);
                };

                let state_snap = state_mu.lock().await.clone();
                match v3::poll(
                    client,
                    cfg,
                    auth,
                    &state_snap,
                    self.payload_format,
                    self.config.include_metadata(),
                )
                .await
                {
                    Ok(result) => {
                        if result.trip_circuit_breaker {
                            self.circuit_breaker.record_failure().await;
                        } else {
                            self.circuit_breaker.record_success();
                        }

                        let new = result.new_state;
                        let messages = result.messages;
                        let schema = result.schema;
                        let msg_count = messages.len();
                        let mut state = state_mu.lock().await;
                        *state = new;

                        if self.config.verbose_logging() {
                            info!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V3). \
                                 Total: {}. Cursor: {:?}",
                                self.id, msg_count, state.processed_rows, state.last_timestamp
                            );
                        } else {
                            debug!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V3). \
                                 Total: {}. Cursor: {:?}",
                                self.id, msg_count, state.processed_rows, state.last_timestamp
                            );
                        }

                        let persisted = ConnectorState::serialize(
                            &PersistedState::V3(state.clone()),
                            CONNECTOR_NAME,
                            self.id,
                        );
                        match &persisted {
                            Some(_) => {
                                self.state_serialize_failures.store(0, Ordering::Relaxed);
                            }
                            None => {
                                let failures = self
                                    .state_serialize_failures
                                    .fetch_add(1, Ordering::Relaxed)
                                    + 1;
                                if failures >= MAX_STATE_SERIALIZE_FAILURES {
                                    self.circuit_breaker.record_failure().await;
                                    return Ok(ProducedMessages {
                                        schema,
                                        messages,
                                        state: None,
                                    });
                                }
                                warn!(
                                    "{CONNECTOR_NAME} ID: {} — state serialization failed \
                                     ({failures}/{MAX_STATE_SERIALIZE_FAILURES}); \
                                     cursor will not be persisted. Messages were emitted; \
                                     restart may cause re-delivery.",
                                    self.id
                                );
                            }
                        }

                        Ok(ProducedMessages {
                            schema,
                            messages,
                            state: persisted,
                        })
                    }
                    Err(e) => self.handle_poll_error(e).await,
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.client = None;
        let processed = match &self.version_state {
            VersionState::V2(mu) => mu.lock().await.processed_rows,
            VersionState::V3(mu) => mu.lock().await.processed_rows,
        };
        info!(
            "{CONNECTOR_NAME} ID: {} closed. Total rows processed: {processed}",
            self.id
        );
        Ok(())
    }
}

impl InfluxDbSource {
    async fn handle_poll_error(&self, e: Error) -> Result<ProducedMessages, Error> {
        if !matches!(e, Error::PermanentHttpError(_)) {
            self.circuit_breaker.record_failure().await;
        }
        error!("{CONNECTOR_NAME} ID: {} poll failed: {e}", self.id);
        tokio::time::sleep(self.poll_interval).await;
        Err(e)
    }
}

// ── Query structure heuristics ────────────────────────────────────────────────

/// Return `true` if `query` contains an `ORDER BY` clause (case-insensitive).
///
/// DataFusion (used by InfluxDB 3 Core) does not guarantee stable row ordering
/// for tied timestamps unless `ORDER BY` is present. V3 OFFSET-based dedup relies
/// on consistent ordering - without `ORDER BY`, different rows may be skipped on
/// each poll, causing silent data loss or re-delivery.
///
/// `/* */` block comments are stripped first; `--` line comments are stripped
/// per-line. `ORDER BY` appearing only inside a comment does NOT satisfy the check.
fn query_has_order_by(query: &str) -> bool {
    let q = strip_block_comments(query);
    q.lines().any(|line| {
        let code = match line.find("--") {
            Some(pos) => &line[..pos],
            None => line,
        };
        code.to_ascii_uppercase().contains("ORDER BY")
    })
}

/// Return `true` if `query` contains a `sort(` call that is not part of a longer
/// identifier (e.g. `mysort(` is excluded; `|> sort(` and bare `sort(` are included).
/// Best-effort check: warns if `sort(` does not appear outside
/// line comments. Not a full parser; documents its limitations.
fn query_has_sort_call(query: &str) -> bool {
    query.lines().any(|line| {
        // Strip line comments before checking for sort(
        let code = match line.find("//") {
            Some(pos) => &line[..pos],
            None => line,
        };
        // Find all occurrences of "sort(" and verify none are preceded by a
        // word character (letter, digit, underscore) — that would mean the
        // "sort" is part of a longer identifier like "mysort" or "do_sort".
        let mut search = code;
        while let Some(pos) = search.find("sort(") {
            let preceded_by_word_char = search[..pos]
                .chars()
                .last()
                .is_some_and(|c| c.is_alphanumeric() || c == '_');
            if !preceded_by_word_char {
                return true;
            }
            search = &search[pos + 5..]; // skip past "sort("
        }
        false
    })
}

// ── V2 cursor advance logic ───────────────────────────────────────────────────

/// Update V2 polling state after a successful poll.
///
/// V2 uses `>= $cursor` semantics, so the first batch after a cursor advance
/// will include rows already delivered at the previous max timestamp. The
/// `cursor_row_count` tracks how many such rows to skip on the next poll.
///
/// - New cursor → store it with the count of rows that landed at that timestamp.
/// - Same cursor → accumulate: more rows at this timestamp were delivered.
/// - No new cursor (all skipped) → correct `cursor_row_count` to `skipped`
///   so the skip counter reflects reality rather than a stale inflated value.
fn apply_v2_cursor_advance(
    state: &mut V2State,
    max_cursor: Option<String>,
    rows_at_max_cursor: u64,
    skipped: u64,
) {
    if let Some(ref new_cursor) = max_cursor {
        let should_advance = match state.last_timestamp.as_deref() {
            None => true,
            Some(old) => match old.parse::<DateTime<Utc>>() {
                Ok(dt) => is_timestamp_after(new_cursor, dt),
                Err(e) => {
                    error!(
                        "V2 source: persisted cursor {old:?} failed RFC 3339 parse ({e}); \
                         cannot advance cursor — connector state may be corrupt. \
                         Clear or migrate the connector state to recover."
                    );
                    false
                }
            },
        };
        if should_advance {
            state.last_timestamp = Some(new_cursor.clone());
            state.cursor_row_count = rows_at_max_cursor;
        } else {
            // Cursor stayed at same timestamp — accumulate new rows for the offset tiebreaker.
            state.cursor_row_count += rows_at_max_cursor;
        }
    } else if skipped > 0 {
        // max_cursor is None (all rows were at or before the current cursor and were
        // skipped). Reset the counter to `skipped` to correct an over-inflated offset.
        state.cursor_row_count = skipped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{V2SourceConfig, V3SourceConfig};
    use secrecy::SecretString;

    fn make_v2_config() -> InfluxDbSourceConfig {
        InfluxDbSourceConfig::V2(V2SourceConfig {
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
        })
    }

    fn make_v3_config() -> InfluxDbSourceConfig {
        InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:8181".to_string(),
            db: "test_db".to_string(),
            token: SecretString::from("test_token"),
            query: "SELECT time, val FROM tbl WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                .to_string(),
            poll_interval: Some("1s".to_string()),
            batch_size: Some(100),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(3),
            retry_delay: Some("100ms".to_string()),
            timeout: Some("5s".to_string()),
            max_open_retries: Some(3),
            open_retry_max_delay: Some("5s".to_string()),
            retry_max_delay: Some("1s".to_string()),
            circuit_breaker_threshold: Some(5),
            circuit_breaker_cool_down: Some("30s".to_string()),
            stuck_batch_cap_factor: Some(10),
        })
    }

    #[test]
    fn v2_source_new_creates_v2_state() {
        let source = InfluxDbSource::new(1, make_v2_config(), None);
        assert!(matches!(source.version_state, VersionState::V2(_)));
        assert!(source.state_restore_error.is_none());
    }

    #[test]
    fn v3_source_new_creates_v3_state() {
        let source = InfluxDbSource::new(1, make_v3_config(), None);
        assert!(matches!(source.version_state, VersionState::V3(_)));
        assert!(source.state_restore_error.is_none());
    }

    #[tokio::test]
    async fn state_restore_fails_on_version_mismatch() {
        // Persist a V2 state, then try to open a V3 connector
        let v2_state = PersistedState::V2(V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            processed_rows: 42,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&v2_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "V3 connector must refuse V2 persisted state"
        );
    }

    #[tokio::test]
    async fn open_returns_init_error_when_restore_failed() {
        // open() uses Error::InitError (which carries a String) so the cause
        // is visible to callers without requiring log access.
        let garbage = ConnectorState(vec![0xFF, 0xFE, 0xFD]);
        let mut source = InfluxDbSource::new(1, make_v2_config(), Some(garbage));
        assert!(source.state_restore_error.is_some());
        let result = source.open().await;
        assert!(
            matches!(result, Err(Error::InitError(_))),
            "open() must fail with InitError on restore failure"
        );
        // The error message must include the cause so operators can diagnose the problem.
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("persisted state exists but could not be deserialized"),
            "error message must include the cause: {msg}"
        );
    }

    #[test]
    fn restore_v2_state_with_corrupt_cursor_marks_restore_error() {
        // A state that deserializes successfully but contains a corrupt cursor
        // must be rejected (state_restore_error set) so open() fails fast.
        let bad_state = PersistedState::V2(V2State {
            last_timestamp: Some("not-a-timestamp".to_string()),
            processed_rows: 5,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt cursor in persisted V2 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad cursor value: {msg}"
        );
    }

    #[test]
    fn restore_v3_state_with_corrupt_cursor_marks_restore_error() {
        let bad_state = PersistedState::V3(V3State {
            last_timestamp: Some("not-a-timestamp".to_string()),
            processed_rows: 5,
            effective_batch_size: 500,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt cursor in persisted V3 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad cursor value: {msg}"
        );
    }

    #[test]
    fn restore_v2_state_with_valid_cursor_succeeds() {
        // A well-formed V2 state with a valid cursor must restore without error.
        let good_state = PersistedState::V2(V2State {
            last_timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            processed_rows: 100,
            cursor_row_count: 2,
        });
        let persisted = ConnectorState::serialize(&good_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "valid cursor must not produce restore error"
        );
    }

    #[test]
    fn restore_v2_state_with_none_cursor_succeeds() {
        // last_timestamp = None (fresh start) must also restore without error.
        let state = PersistedState::V2(V2State {
            last_timestamp: None,
            processed_rows: 0,
            cursor_row_count: 0,
        });
        let persisted = ConnectorState::serialize(&state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(source.state_restore_error.is_none());
    }

    #[tokio::test]
    async fn open_rejects_invalid_initial_offset() {
        // Validates initial_offset before attempting any network connection.
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            initial_offset: Some("not-a-timestamp".to_string()),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue for bad initial_offset, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_timezone_free_initial_offset() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            initial_offset: Some("2024-01-15T10:30:00".to_string()),
            org: "o".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "initial_offset without timezone must be rejected"
        );
    }

    // ── #10: org / db non-empty validation ────────────────────────────────────

    #[tokio::test]
    async fn open_v2_rejects_empty_org() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "".to_string(),
            token: SecretString::from("t"),
            query: "q WHERE time > '$cursor'".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "empty org must be rejected"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_empty_db() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "empty db must be rejected"
        );
    }

    // ── #11: $cursor placeholder ──────────────────────────────────────────────

    #[tokio::test]
    async fn open_rejects_query_missing_cursor_placeholder() {
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "myorg".to_string(),
            token: SecretString::from("t"),
            query: "from(bucket:\"b\") |> range(start: -1h) |> limit(n: $limit)".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$cursor")),
            "query without $cursor must be rejected: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_query_with_cursor_as_prefix_of_longer_identifier() {
        // "$cursor_field" contains "$cursor" as a substring but is NOT a bare
        // $cursor placeholder; apply_query_params will not substitute it, so
        // open() must reject it the same as a missing placeholder.
        let config = InfluxDbSourceConfig::V2(V2SourceConfig {
            url: "http://localhost:18086".to_string(),
            org: "myorg".to_string(),
            token: SecretString::from("t"),
            query: "from(bucket:\"b\") |> range(start: $cursor_field) |> limit(n: $limit)"
                .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$cursor")),
            "$cursor_field must not satisfy the $cursor placeholder check: {err:?}"
        );
    }

    // ── #12: V3 >= guard ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_v3_rejects_inclusive_cursor_operator() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time >= '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains(">=")),
            "V3 query with >= '$cursor' must be rejected: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_allows_other_gte_in_query() {
        // `>=` that does not apply to $cursor (e.g. a value filter) must be allowed.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time > '$cursor' AND val >= 0 ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        // Must fail on connectivity, NOT on the >= or ORDER BY checks.
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains(">=")),
            "query with >= on non-cursor field must not be rejected for >=: {err:?}"
        );
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "query with ORDER BY must not be rejected for ORDER BY: {err:?}"
        );
    }

    #[tokio::test]
    async fn poll_returns_empty_when_circuit_is_open() {
        let config = match make_v2_config() {
            InfluxDbSourceConfig::V2(mut c) => {
                c.circuit_breaker_threshold = Some(1);
                c.circuit_breaker_cool_down = Some("60s".to_string());
                c.poll_interval = Some("1ms".to_string());
                InfluxDbSourceConfig::V2(c)
            }
            other => other,
        };
        let source = InfluxDbSource::new(1, config, None);
        source.circuit_breaker.record_failure().await;
        assert!(source.circuit_breaker.is_open().await);

        let result = source.poll().await;
        assert!(result.is_ok());
        assert!(result.unwrap().messages.is_empty());
    }

    #[tokio::test]
    async fn close_clears_client() {
        let mut source = InfluxDbSource::new(1, make_v2_config(), None);
        let result = source.close().await;
        assert!(result.is_ok());
        assert!(source.client.is_none());
    }

    #[test]
    fn apply_v2_cursor_advance_moves_cursor() {
        let mut state = V2State::default();
        apply_v2_cursor_advance(&mut state, Some("2024-01-01T00:00:01Z".to_string()), 3, 0);
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:01Z")
        );
        assert_eq!(state.cursor_row_count, 3);
    }

    #[test]
    fn apply_v2_cursor_advance_accumulates_same_cursor() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 3,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, Some("2024-01-01T00:00:00Z".to_string()), 2, 0);
        assert_eq!(state.cursor_row_count, 5);
    }

    #[test]
    fn apply_v2_cursor_advance_corrects_inflated_counter() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 10,
            processed_rows: 0,
        };
        // None + skipped=3 → correction
        apply_v2_cursor_advance(&mut state, None, 0, 3);
        assert_eq!(state.cursor_row_count, 3);
    }

    #[tokio::test]
    async fn open_v3_rejects_query_without_offset_when_stuck_cap_active() {
        // Default stuck_batch_cap_factor is 10 (> 0), so any V3 query without
        // '$offset' must be rejected at open() to prevent duplicate delivery.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            // deliberately missing $offset
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None, // uses default (10 > 0)
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue when $offset missing in V3 query, got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_accepts_query_with_offset_placeholder() {
        // A query with $offset and ORDER BY (and a URL that fails health check) must NOT be
        // rejected for the offset or ORDER BY reason — it must proceed to the connectivity check.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query:
                "SELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit OFFSET $offset"
                    .to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None,
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        // Must NOT be InvalidConfigValue for offset or ORDER BY reasons; connectivity fails instead.
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$offset")),
            "open() must not reject a query that contains $offset; got {err:?}"
        );
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "open() must not reject a query that contains ORDER BY; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_with_zero_stuck_cap_skips_offset_check() {
        // stuck_batch_cap_factor = 0 disables the stuck-cap feature; $offset is
        // not required because no inflation will ever happen.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(0), // explicitly disabled
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("$offset")),
            "open() must not check $offset when stuck_batch_cap_factor=0; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_query_without_order_by_when_stuck_cap_active() {
        // $offset present but no ORDER BY — DataFusion ordering is unstable for tied
        // timestamps, so OFFSET-based dedup may skip different rows each poll.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: None, // default 10 > 0
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "V3 query without ORDER BY must be rejected when stuck detection active: {err:?}"
        );
    }

    #[tokio::test]
    async fn open_v3_with_zero_stuck_cap_skips_order_by_check() {
        // stuck_batch_cap_factor=0 disables stuck detection; ORDER BY is not required.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(0),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            !matches!(err, Error::InvalidConfigValue(ref msg) if msg.contains("ORDER BY")),
            "ORDER BY check must be skipped when stuck_batch_cap_factor=0; got {err:?}"
        );
    }

    #[tokio::test]
    async fn open_rejects_stuck_batch_cap_factor_above_max() {
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "db".to_string(),
            token: SecretString::from("t"),
            query: "SELECT 1".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(v3::MAX_STUCK_CAP_FACTOR + 1),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "expected InvalidConfigValue for oversized stuck_batch_cap_factor, got {err:?}"
        );
    }

    #[test]
    fn config_accessors_v2() {
        let cfg = make_v2_config();
        assert_eq!(cfg.version_label(), "v2");
        assert_eq!(cfg.cursor_field(), "_time");
        assert_eq!(cfg.batch_size(), 100);
    }

    #[test]
    fn config_accessors_v3() {
        let cfg = make_v3_config();
        assert_eq!(cfg.version_label(), "v3");
        assert_eq!(cfg.cursor_field(), "time");
        assert_eq!(cfg.batch_size(), 100);
    }

    // ── query_has_sort_call heuristic ─────────────────────────────────────────

    #[test]
    fn sort_call_detected_in_flux_pipeline() {
        assert!(query_has_sort_call(
            r#"from(bucket:"b") |> range(start: -1h) |> sort(columns: ["_time"])"#
        ));
    }

    #[test]
    fn sort_call_detected_without_pipe() {
        assert!(query_has_sort_call("sort(columns: [\"_time\"])"));
    }

    #[test]
    fn sort_call_not_detected_when_absent() {
        assert!(!query_has_sort_call(
            r#"from(bucket:"b") |> range(start: $cursor) |> limit(n: $limit)"#
        ));
    }

    #[test]
    fn sort_call_not_false_positive_on_identifier_prefix() {
        // `mysort(` must NOT be treated as a sort call — it is a different function name.
        assert!(!query_has_sort_call("mysort(columns: [\"_time\"])"));
        assert!(!query_has_sort_call("do_sort(x)"));
    }

    #[test]
    fn sort_call_detected_at_start_of_string() {
        assert!(query_has_sort_call(
            "sort(columns: [\"_time\"]) |> limit(n: 10)"
        ));
    }

    #[test]
    fn sort_call_not_detected_with_space_before_paren() {
        // `sort (` with a space is not valid Flux syntax; the heuristic searches
        // for the literal token `sort(` and does not match this form. The warning
        // is therefore not emitted, which is acceptable: a query written this way
        // would fail at the InfluxDB level for a different reason.
        assert!(!query_has_sort_call("sort (columns: [\"_time\"])"));
    }

    #[test]
    fn sort_call_in_line_comment_is_ignored() {
        // sort( appearing only in a // comment must NOT trigger the heuristic.
        assert!(!query_has_sort_call(
            "from(bucket:\"b\") // sort(columns:[\"_time\"]) not real"
        ));
        // But sort( before the comment on the same line is still detected.
        assert!(query_has_sort_call(
            "sort(columns:[\"_time\"]) // also present"
        ));
    }

    // ── query_has_order_by heuristic ──────────────────────────────────────────

    #[test]
    fn order_by_detected_in_sql_query() {
        assert!(query_has_order_by(
            "SELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit"
        ));
        assert!(query_has_order_by("SELECT 1 order by id"));
        assert!(query_has_order_by("SELECT 1 Order By x DESC"));
    }

    #[test]
    fn order_by_not_detected_when_absent() {
        assert!(!query_has_order_by(
            "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
    }

    #[test]
    fn order_by_in_sql_comment_is_ignored() {
        // "ORDER BY" only in a -- comment must NOT trigger the check — the query
        // executes without ORDER BY and DataFusion ordering is unstable.
        assert!(!query_has_order_by(
            "-- ORDER BY legacy behavior\nSELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
        // ORDER BY before the comment on the same line must still be detected.
        assert!(query_has_order_by(
            "SELECT * FROM t ORDER BY time -- sort by time\nLIMIT $limit"
        ));
    }

    #[test]
    fn order_by_in_block_comment_is_ignored() {
        // "ORDER BY" only inside /* */ must NOT satisfy the check.
        assert!(!query_has_order_by(
            "/* ORDER BY example */\nSELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset"
        ));
        // Multi-line block comment spanning ORDER BY must also be ignored.
        assert!(!query_has_order_by(
            "/* this is\n   ORDER BY a comment */\nSELECT * FROM t LIMIT $limit"
        ));
        // Real ORDER BY with an unrelated block comment must still be detected.
        assert!(query_has_order_by(
            "/* header */\nSELECT * FROM t WHERE time > '$cursor' ORDER BY time LIMIT $limit"
        ));
    }

    #[test]
    fn apply_v2_cursor_advance_no_new_cursor_no_skipped_is_noop() {
        let mut state = V2State {
            last_timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            cursor_row_count: 7,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, None, 0, 0);
        // Neither max_cursor nor skipped — state must not change.
        assert_eq!(state.cursor_row_count, 7);
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
    }

    // ── restore_v2_state / restore_v3_state paths ─────────────────────────────

    #[test]
    fn restore_v2_state_with_v2_persisted_restores_successfully() {
        // V2 config + V2 persisted state → state is restored with state_restore_failed=false.
        let v2_state = PersistedState::V2(V2State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 99,
            cursor_row_count: 3,
        });
        let persisted = ConnectorState::serialize(&v2_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "V2 state on V2 connector must succeed"
        );
        if let VersionState::V2(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(
                state.last_timestamp.as_deref(),
                Some("2024-06-01T00:00:00Z")
            );
            assert_eq!(state.processed_rows, 99);
        } else {
            panic!("expected V2 version state");
        }
    }

    #[test]
    fn restore_v2_state_with_v3_persisted_marks_restore_failed() {
        // V2 config + V3 persisted state → mismatch must be rejected.
        let v3_state = PersistedState::V3(V3State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 10,
            effective_batch_size: 500,
            last_timestamp_row_offset: 0,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&v3_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "V3 state on V2 connector must set state_restore_error"
        );
    }

    #[test]
    fn restore_v3_state_with_v3_persisted_restores_successfully() {
        // V3 config + V3 persisted state → state is restored with state_restore_failed=false.
        let v3_state = PersistedState::V3(V3State {
            last_timestamp: Some("2024-07-15T12:00:00Z".to_string()),
            processed_rows: 500,
            effective_batch_size: 1000,
            last_timestamp_row_offset: 5,
            stuck_cursor: None,
        });
        let persisted = ConnectorState::serialize(&v3_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_none(),
            "V3 state on V3 connector must succeed"
        );
        if let VersionState::V3(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(
                state.last_timestamp.as_deref(),
                Some("2024-07-15T12:00:00Z")
            );
            assert_eq!(state.processed_rows, 500);
            assert_eq!(state.effective_batch_size, 1000);
        } else {
            panic!("expected V3 version state");
        }
    }

    // ── get_client() uncalled open() ─────────────────────────────────────────

    #[tokio::test]
    async fn poll_returns_connection_error_when_client_not_initialized() {
        // Calling poll() without open() means client is None; get_client() must
        // return a Connection error rather than panicking.
        let source = InfluxDbSource::new(1, make_v2_config(), None);
        let result = source.poll().await;
        assert!(
            matches!(result, Err(Error::Connection(_))),
            "expected Connection error when client not initialized, got {result:?}"
        );
    }

    // ── Warning 3: legacy V2 state without version tag ───────────────────────

    #[test]
    fn restore_v2_state_with_legacy_format_succeeds() {
        // State persisted before PersistedState was introduced contains only
        // V2State fields with no version tag. Must restore without error.
        let old_v2 = V2State {
            last_timestamp: Some("2024-06-01T00:00:00Z".to_string()),
            processed_rows: 42,
            cursor_row_count: 3,
        };
        // Serialize V2State directly (not wrapped in PersistedState) to simulate old format.
        let cs = ConnectorState::serialize(&old_v2, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v2_config(), Some(cs));
        assert!(
            source.state_restore_error.is_none(),
            "legacy V2 state without version tag must restore without error"
        );
        if let VersionState::V2(mu) = &source.version_state {
            let state = mu.blocking_lock();
            assert_eq!(state.processed_rows, 42);
            assert_eq!(state.cursor_row_count, 3);
        } else {
            panic!("expected V2 version state");
        }
    }

    #[test]
    fn restore_v3_state_with_invalid_stuck_cursor_marks_restore_error() {
        // A state with a valid last_timestamp but corrupt stuck_cursor must be rejected —
        // an invalid stuck_cursor could advance the cursor to a garbage value on the
        // next empty poll, causing data loss or query errors.
        let bad_state = PersistedState::V3(V3State {
            last_timestamp: Some("2025-01-01T00:00:00Z".to_string()),
            processed_rows: 5,
            effective_batch_size: 500,
            last_timestamp_row_offset: 3,
            stuck_cursor: Some("not-a-timestamp".to_string()),
        });
        let persisted = ConnectorState::serialize(&bad_state, CONNECTOR_NAME, 1).unwrap();
        let source = InfluxDbSource::new(1, make_v3_config(), Some(persisted));
        assert!(
            source.state_restore_error.is_some(),
            "corrupt stuck_cursor in persisted V3 state must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("not-a-timestamp"),
            "error message must quote the bad stuck_cursor value: {msg}"
        );
    }

    // ── restore_v3_state — None deserialization path ─────────────────────────

    #[test]
    fn restore_v3_state_with_garbage_data_marks_restore_error() {
        // Garbage bytes that cannot be deserialized must set state_restore_error
        // on a V3 connector (mirrors the existing V2 test for coverage of the
        // None branch in restore_v3_state at source/lib.rs:202-206).
        let garbage = ConnectorState(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let source = InfluxDbSource::new(1, make_v3_config(), Some(garbage));
        assert!(
            source.state_restore_error.is_some(),
            "garbage state for V3 connector must set state_restore_error"
        );
        let msg = source.state_restore_error.unwrap();
        assert!(
            msg.contains("could not be deserialized"),
            "error message must explain the cause: {msg}"
        );
    }

    // ── apply_v2_cursor_advance — corrupt cursor parse failure ───────────────

    #[test]
    fn apply_v2_cursor_advance_corrupt_cursor_does_not_advance() {
        // When the persisted cursor is not valid RFC 3339, the parse fails and
        // last_timestamp must NOT advance to the new max_cursor.
        // The else-branch still accumulates cursor_row_count (normal behaviour
        // for "same-cursor" polls); only the timestamp guard is affected.
        let mut state = V2State {
            last_timestamp: Some("this-is-not-a-timestamp".to_string()),
            cursor_row_count: 3,
            processed_rows: 0,
        };
        apply_v2_cursor_advance(&mut state, Some("2024-06-01T00:00:00Z".to_string()), 1, 0);
        // last_timestamp must remain the corrupt string — should_advance=false
        // because the old cursor failed to parse, so we do NOT replace it.
        assert_eq!(
            state.last_timestamp.as_deref(),
            Some("this-is-not-a-timestamp"),
            "corrupt cursor must not be replaced by new max_cursor"
        );
        // cursor_row_count is accumulated in the else branch (not reset), which
        // is correct: the connector treats the parse failure as "same cursor".
        assert_eq!(
            state.cursor_row_count, 4,
            "cursor_row_count must accumulate (+rows_at_max_cursor) in else branch"
        );
    }

    #[tokio::test]
    async fn open_v3_rejects_stuck_cap_factor_of_one() {
        // cap_factor=1 means cap = base_batch × 1 = base_batch.
        // next_stuck_batch_size(base, base, 1) returns None immediately so the CB
        // trips on the very first stuck poll with zero inflation — an infinite CB loop.
        let config = InfluxDbSourceConfig::V3(V3SourceConfig {
            url: "http://localhost:18181".to_string(),
            db: "mydb".to_string(),
            token: SecretString::from("t"),
            query: "SELECT * FROM t WHERE time > '$cursor' LIMIT $limit OFFSET $offset".to_string(),
            poll_interval: None,
            batch_size: None,
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
            include_metadata: None,
            verbose_logging: None,
            max_retries: Some(1),
            retry_delay: None,
            timeout: Some("1s".to_string()),
            max_open_retries: Some(1),
            open_retry_max_delay: Some("1ms".to_string()),
            retry_max_delay: None,
            circuit_breaker_threshold: None,
            circuit_breaker_cool_down: None,
            stuck_batch_cap_factor: Some(1),
        });
        let mut source = InfluxDbSource::new(1, config, None);
        let err = source.open().await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidConfigValue(_)),
            "stuck_batch_cap_factor=1 must be rejected — use 0 to disable or >= 2; got {err:?}"
        );
    }
}
