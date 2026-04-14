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
mod v2;
mod v3;

use async_trait::async_trait;
use common::{
    InfluxDbSourceConfig, PayloadFormat, PersistedState, V2State, V3State, validate_cursor_field,
};
use iggy_connector_sdk::retry::{
    CircuitBreaker, ConnectivityConfig, build_retry_client, check_connectivity_with_retry,
    parse_duration,
};
use iggy_connector_sdk::{ConnectorState, Error, ProducedMessages, Schema, Source, source_connector};
use reqwest_middleware::ClientWithMiddleware;
use secrecy::ExposeSecret;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

source_connector!(InfluxDbSource);

const CONNECTOR_NAME: &str = "InfluxDB source";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_POLL_INTERVAL: &str = "5s";
const DEFAULT_TIMEOUT: &str = "10s";
const DEFAULT_OPEN_RETRY_MAX_DELAY: &str = "60s";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
const DEFAULT_CIRCUIT_COOL_DOWN: &str = "30s";

// ---------------------------------------------------------------------------
// Connector state enum — version-specific polling state
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum VersionState {
    V2(Mutex<V2State>),
    V3(Mutex<V3State>),
}

// ---------------------------------------------------------------------------
// Main connector struct
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct InfluxDbSource {
    pub id: u32,
    config: InfluxDbSourceConfig,
    client: Option<ClientWithMiddleware>,
    version_state: VersionState,
    payload_format: PayloadFormat,
    poll_interval: Duration,
    retry_delay: Duration,
    circuit_breaker: Arc<CircuitBreaker>,
    auth_header: Option<String>,
    state_restore_failed: bool,
}

impl InfluxDbSource {
    pub fn new(id: u32, config: InfluxDbSourceConfig, state: Option<ConnectorState>) -> Self {
        let retry_delay = parse_duration(config.retry_delay(), DEFAULT_RETRY_DELAY);
        let poll_interval = parse_duration(config.poll_interval(), DEFAULT_POLL_INTERVAL);
        let payload_format = PayloadFormat::from_config(config.payload_format());

        let cb_threshold = config.circuit_breaker_threshold();
        let cb_cool_down =
            parse_duration(config.circuit_breaker_cool_down(), DEFAULT_CIRCUIT_COOL_DOWN);
        let circuit_breaker = Arc::new(CircuitBreaker::new(cb_threshold, cb_cool_down));

        let (version_state, state_restore_failed) = match &config {
            InfluxDbSourceConfig::V2(_) => {
                let (s, failed) = restore_v2_state(id, state);
                (VersionState::V2(Mutex::new(s)), failed)
            }
            InfluxDbSourceConfig::V3(_) => {
                let (s, failed) = restore_v3_state(id, state);
                (VersionState::V3(Mutex::new(s)), failed)
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
            state_restore_failed,
        }
    }

    fn get_client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::Connection("InfluxDB client not initialized".to_string()))
    }
}

// ---------------------------------------------------------------------------
// State restore helpers
// ---------------------------------------------------------------------------

fn restore_v2_state(id: u32, state: Option<ConnectorState>) -> (V2State, bool) {
    let Some(cs) = state else {
        return (V2State::default(), false);
    };
    match cs.deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V2(s)) => {
            info!(
                "{CONNECTOR_NAME} ID {id}: restored V2 state — \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, false)
        }
        Some(PersistedState::V3(_)) => {
            error!(
                "{CONNECTOR_NAME} ID {id}: persisted state is V3 but connector is configured \
                 as V2. Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed."
            );
            (V2State::default(), true)
        }
        None => {
            error!(
                "{CONNECTOR_NAME} ID {id}: persisted state exists but could not be deserialized. \
                 Refusing to start to prevent silent cursor reset."
            );
            (V2State::default(), true)
        }
    }
}

fn restore_v3_state(id: u32, state: Option<ConnectorState>) -> (V3State, bool) {
    let Some(cs) = state else {
        return (V3State::default(), false);
    };
    match cs.deserialize::<PersistedState>(CONNECTOR_NAME, id) {
        Some(PersistedState::V3(s)) => {
            info!(
                "{CONNECTOR_NAME} ID {id}: restored V3 state — \
                 last_timestamp={:?}, processed_rows={}",
                s.last_timestamp, s.processed_rows
            );
            (s, false)
        }
        Some(PersistedState::V2(_)) => {
            error!(
                "{CONNECTOR_NAME} ID {id}: persisted state is V2 but connector is configured \
                 as V3. Refusing to start to prevent cursor reset. \
                 Clear or migrate the connector state to proceed."
            );
            (V3State::default(), true)
        }
        None => {
            error!(
                "{CONNECTOR_NAME} ID {id}: persisted state exists but could not be deserialized. \
                 Refusing to start to prevent silent cursor reset."
            );
            (V3State::default(), true)
        }
    }
}

// ---------------------------------------------------------------------------
// Source trait
// ---------------------------------------------------------------------------

#[async_trait]
impl Source for InfluxDbSource {
    async fn open(&mut self) -> Result<(), Error> {
        if self.state_restore_failed {
            return Err(Error::InvalidState);
        }

        let ver = self.config.version_label();
        info!(
            "Opening {CONNECTOR_NAME} with ID: {} (version={ver})",
            self.id
        );

        validate_cursor_field(self.config.cursor_field())?;

        let timeout = parse_duration(self.config.timeout(), DEFAULT_TIMEOUT);
        let raw_client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to create HTTP client: {e}")))?;

        let health_url = match &self.config {
            InfluxDbSourceConfig::V2(c) => v2::health_url(c.url.trim_end_matches('/'))?,
            InfluxDbSourceConfig::V3(c) => v3::health_url(c.url.trim_end_matches('/'))?,
        };

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

        self.auth_header = Some(match &self.config {
            InfluxDbSourceConfig::V2(c) => v2::auth_header(c.token.expose_secret()),
            InfluxDbSourceConfig::V3(c) => v3::auth_header(c.token.expose_secret()),
        });

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
            tokio::time::sleep(self.poll_interval).await;
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: None,
            });
        }

        let client = self.get_client()?;
        let auth = self.auth_header.as_deref().ok_or_else(|| {
            Error::Connection("auth_header not initialised — was open() called?".to_string())
        })?;

        match &self.version_state {
            VersionState::V2(state_mu) => {
                let InfluxDbSourceConfig::V2(cfg) = &self.config else {
                    unreachable!("V2 state with non-V2 config")
                };

                let state_snap = state_mu.lock().await.clone_for_poll();
                match v2::poll(client, cfg, auth, &state_snap, self.payload_format).await {
                    Ok(result) => {
                        self.circuit_breaker.record_success();
                        let mut state = state_mu.lock().await;
                        state.processed_rows += result.messages.len() as u64;
                        apply_v2_cursor_advance(
                            &mut state,
                            result.max_cursor,
                            result.rows_at_max_cursor,
                            result.skipped,
                        );

                        if cfg.verbose_logging.unwrap_or(false) {
                            info!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V2). \
                                 Total: {}. Cursor: {:?}",
                                self.id,
                                result.messages.len(),
                                state.processed_rows,
                                state.last_timestamp
                            );
                        } else {
                            debug!(
                                "{CONNECTOR_NAME} ID: {} produced {} messages (V2). \
                                 Total: {}. Cursor: {:?}",
                                self.id,
                                result.messages.len(),
                                state.processed_rows,
                                state.last_timestamp
                            );
                        }

                        let persisted = ConnectorState::serialize(
                            &PersistedState::V2(V2State {
                                last_timestamp: state.last_timestamp.clone(),
                                processed_rows: state.processed_rows,
                                cursor_row_count: state.cursor_row_count,
                            }),
                            CONNECTOR_NAME,
                            self.id,
                        );

                        Ok(ProducedMessages {
                            schema: result.schema,
                            messages: result.messages,
                            state: persisted,
                        })
                    }
                    Err(e) => self.handle_poll_error(e).await,
                }
            }

            VersionState::V3(state_mu) => {
                let InfluxDbSourceConfig::V3(cfg) = &self.config else {
                    unreachable!("V3 state with non-V3 config")
                };

                let state_snap = state_mu.lock().await.clone_for_poll();
                match v3::poll(client, cfg, auth, &state_snap, self.payload_format).await {
                    Ok(result) => {
                        if result.trip_circuit_breaker {
                            self.circuit_breaker.record_failure().await;
                        } else {
                            self.circuit_breaker.record_success();
                        }

                        let new = result.new_state;
                        let msg_count = result.messages.len();
                        let mut state = state_mu.lock().await;
                        *state = new;

                        if cfg.verbose_logging.unwrap_or(false) {
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
                            &PersistedState::V3(V3State {
                                last_timestamp: state.last_timestamp.clone(),
                                processed_rows: state.processed_rows,
                                effective_batch_size: state.effective_batch_size,
                            }),
                            CONNECTOR_NAME,
                            self.id,
                        );

                        Ok(ProducedMessages {
                            schema: result.schema,
                            messages: result.messages,
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
        error!(
            "{CONNECTOR_NAME} ID: {} poll failed: {e}",
            self.id
        );
        tokio::time::sleep(self.poll_interval).await;
        Err(e)
    }
}

// ---------------------------------------------------------------------------
// V2 cursor advance logic
// ---------------------------------------------------------------------------

fn apply_v2_cursor_advance(
    state: &mut V2State,
    max_cursor: Option<String>,
    rows_at_max_cursor: u64,
    skipped: u64,
) {
    match max_cursor {
        Some(ref new_cursor) if state.last_timestamp.as_deref() != Some(new_cursor.as_str()) => {
            state.last_timestamp = max_cursor.clone();
            state.cursor_row_count = rows_at_max_cursor;
        }
        Some(_) => {
            state.cursor_row_count = state.cursor_row_count.saturating_add(rows_at_max_cursor);
        }
        None => {
            if skipped > 0 {
                state.cursor_row_count = skipped;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Clone helpers for poll snapshots (avoids holding the lock across await)
// ---------------------------------------------------------------------------

trait CloneForPoll: Sized {
    fn clone_for_poll(&self) -> Self;
}

impl CloneForPoll for V2State {
    fn clone_for_poll(&self) -> Self {
        V2State {
            last_timestamp: self.last_timestamp.clone(),
            processed_rows: self.processed_rows,
            cursor_row_count: self.cursor_row_count,
        }
    }
}

impl CloneForPoll for V3State {
    fn clone_for_poll(&self) -> Self {
        V3State {
            last_timestamp: self.last_timestamp.clone(),
            processed_rows: self.processed_rows,
            effective_batch_size: self.effective_batch_size,
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

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
            query: r#"from(bucket:"b") |> range(start: $cursor) |> limit(n: $limit)"#
                .to_string(),
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
            query:
                "SELECT time, val FROM tbl WHERE time > '$cursor' ORDER BY time LIMIT $limit"
                    .to_string(),
            poll_interval: Some("1s".to_string()),
            batch_size: Some(100),
            cursor_field: None,
            initial_offset: None,
            payload_column: None,
            payload_format: None,
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
        assert!(!source.state_restore_failed);
    }

    #[test]
    fn v3_source_new_creates_v3_state() {
        let source = InfluxDbSource::new(1, make_v3_config(), None);
        assert!(matches!(source.version_state, VersionState::V3(_)));
        assert!(!source.state_restore_failed);
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
            source.state_restore_failed,
            "V3 connector must refuse V2 persisted state"
        );
    }

    #[tokio::test]
    async fn open_returns_invalid_state_when_restore_failed() {
        let garbage = ConnectorState(vec![0xFF, 0xFE, 0xFD]);
        let mut source = InfluxDbSource::new(1, make_v2_config(), Some(garbage));
        assert!(source.state_restore_failed);
        let result = source.open().await;
        assert!(
            matches!(result, Err(Error::InvalidState)),
            "open() must fail fast on restore failure"
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
        apply_v2_cursor_advance(
            &mut state,
            Some("2024-01-01T00:00:01Z".to_string()),
            3,
            0,
        );
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
        apply_v2_cursor_advance(
            &mut state,
            Some("2024-01-01T00:00:00Z".to_string()),
            2,
            0,
        );
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
}
