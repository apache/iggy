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

//! Apache Airflow trigger sink: consume Iggy messages and create DAG runs via REST.

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::Bytes;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata,
    convert::owned_value_to_serde_json, sink_connector,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    RetryTransientMiddleware, Retryable, RetryableStrategy, policies::ExponentialBackoff,
};
use reqwest_tracing::TracingMiddleware;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(AirflowSink);

const CONNECTOR_NAME: &str = "Airflow sink";
const DEFAULT_TIMEOUT: &str = "30s";
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_BACKOFF_MULTIPLIER: u32 = 2;
const DEFAULT_MAX_CONNECTIONS: usize = 10;
const DEFAULT_API_PREFIX: &str = "/api/v1";
const DEFAULT_HEALTH_PATH: &str = "/api/v1/version";
const DEFAULT_TCP_KEEPALIVE_SECS: u64 = 30;
const DEFAULT_POOL_IDLE_TIMEOUT_SECS: u64 = 90;
const MAX_CONSECUTIVE_FAILURES: u32 = 3;
const MAX_RESPONSE_LOG_BYTES: usize = 500;

/// Authentication mode for the Airflow REST API.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    #[default]
    None,
    Basic,
    Bearer,
}

/// How message payload maps into the DAG-run `conf` body.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfMode {
    /// Entire JSON (or text) payload becomes `conf`.
    #[default]
    Payload,
}

/// Plugin config from `[plugin_config]`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AirflowSinkConfig {
    /// Airflow webserver base URL (required), e.g. `http://localhost:8080`.
    pub base_url: String,
    /// Default DAG id to trigger (required).
    pub dag_id: String,
    /// REST path prefix (default: `/api/v1`).
    pub api_prefix: Option<String>,
    /// Auth mode: `none` | `basic` | `bearer` (default: `none`).
    pub auth: Option<AuthMode>,
    /// Basic-auth username (required when `auth = basic`).
    pub username: Option<String>,
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub password: Option<SecretString>,
    #[serde(
        default,
        serialize_with = "iggy_common::serde_secret::serialize_optional_secret"
    )]
    pub token: Option<SecretString>,
    /// Message header that overrides `dag_id` when present.
    pub dag_id_header: Option<String>,
    /// How payload becomes `conf` (default: `payload`).
    pub conf_mode: Option<ConfMode>,
    /// Nest Iggy metadata under `conf.iggy` (default: false).
    pub include_iggy_metadata_in_conf: Option<bool>,
    /// Run connectivity check in `open()` (default: true).
    pub health_check_enabled: Option<bool>,
    /// Path relative to `base_url` for the health check (default: `/api/v1/version`).
    pub health_path: Option<String>,
    pub timeout: Option<String>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub retry_backoff_multiplier: Option<u32>,
    pub max_retry_delay: Option<String>,
    pub tls_danger_accept_invalid_certs: Option<bool>,
    pub max_connections: Option<usize>,
    pub verbose_logging: Option<bool>,
}

/// Trigger sink: one Airflow DAG run per consumed message.
#[derive(Debug)]
pub struct AirflowSink {
    id: u32,
    base_url: String,
    log_url: String,
    dag_id: String,
    api_prefix: String,
    auth: AuthMode,
    username: Option<String>,
    password: Option<SecretString>,
    token: Option<SecretString>,
    dag_id_header: Option<String>,
    conf_mode: ConfMode,
    include_iggy_metadata_in_conf: bool,
    health_check_enabled: bool,
    health_path: String,
    timeout: Duration,
    max_retries: u32,
    retry_delay: Duration,
    retry_backoff_multiplier: u32,
    max_retry_delay: Duration,
    tls_danger_accept_invalid_certs: bool,
    max_connections: usize,
    verbose: bool,
    request_headers: Option<HeaderMap>,
    client: Option<ClientWithMiddleware>,
    trigger_attempts: AtomicU64,
    messages_triggered: AtomicU64,
    errors_count: AtomicU64,
}

impl AirflowSink {
    pub fn new(id: u32, config: AirflowSinkConfig) -> Self {
        let base_url = config.base_url.trim_end_matches('/').to_string();
        let log_url = sanitize_url_for_log(&base_url);
        let dag_id = config.dag_id;
        let api_prefix =
            normalize_path_prefix(config.api_prefix.as_deref().unwrap_or(DEFAULT_API_PREFIX));
        let auth = config.auth.unwrap_or_default();
        let conf_mode = config.conf_mode.unwrap_or_default();
        let include_iggy_metadata_in_conf = config.include_iggy_metadata_in_conf.unwrap_or(false);
        let health_check_enabled = config.health_check_enabled.unwrap_or(true);
        let health_path =
            normalize_path(config.health_path.as_deref().unwrap_or(DEFAULT_HEALTH_PATH));
        let timeout = parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let mut retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let retry_backoff_multiplier = config
            .retry_backoff_multiplier
            .unwrap_or(DEFAULT_BACKOFF_MULTIPLIER)
            .max(1);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        let tls_danger_accept_invalid_certs =
            config.tls_danger_accept_invalid_certs.unwrap_or(false);
        let max_connections = config.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
        let verbose = config.verbose_logging.unwrap_or(false);

        if retry_delay > max_retry_delay {
            warn!(
                "{CONNECTOR_NAME} ID: {id} — retry_delay ({retry_delay:?}) exceeds \
                 max_retry_delay ({max_retry_delay:?}). Swapping values."
            );
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
        }

        if tls_danger_accept_invalid_certs {
            warn!(
                "{CONNECTOR_NAME} ID: {id} — tls_danger_accept_invalid_certs is enabled. \
                 TLS certificate validation is DISABLED."
            );
        }

        Self {
            id,
            base_url,
            log_url,
            dag_id,
            api_prefix,
            auth,
            username: config.username,
            password: config.password,
            token: config.token,
            dag_id_header: config.dag_id_header.filter(|h| !h.is_empty()),
            conf_mode,
            include_iggy_metadata_in_conf,
            health_check_enabled,
            health_path,
            timeout,
            max_retries,
            retry_delay,
            retry_backoff_multiplier,
            max_retry_delay,
            tls_danger_accept_invalid_certs,
            max_connections,
            verbose,
            request_headers: None,
            client: None,
            trigger_attempts: AtomicU64::new(0),
            messages_triggered: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
        }
    }

    fn build_client(&self) -> Result<ClientWithMiddleware, Error> {
        let raw_client = reqwest::Client::builder()
            .timeout(self.timeout)
            .pool_max_idle_per_host(self.max_connections)
            .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
            .tcp_keepalive(Duration::from_secs(DEFAULT_TCP_KEEPALIVE_SECS))
            .danger_accept_invalid_certs(self.tls_danger_accept_invalid_certs)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build HTTP client: {e}")))?;

        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(self.retry_delay, self.max_retry_delay)
            .base(self.retry_backoff_multiplier)
            .build_with_max_retries(self.max_retries);

        let retry_middleware = RetryTransientMiddleware::new_with_policy_and_strategy(
            retry_policy,
            AirflowRetryStrategy,
        );

        Ok(ClientBuilder::new(raw_client)
            .with(TracingMiddleware::default())
            .with(retry_middleware)
            .build())
    }

    fn client(&self) -> Result<&ClientWithMiddleware, Error> {
        self.client.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "{CONNECTOR_NAME} client not initialized — was open() called?"
            ))
        })
    }

    fn build_auth_headers(&self) -> Result<HeaderMap, Error> {
        let mut headers = HeaderMap::new();
        match self.auth {
            AuthMode::None => {}
            AuthMode::Basic => {
                let username = self.username.as_deref().ok_or_else(|| {
                    Error::InvalidConfigValue("username is required when auth = basic".to_string())
                })?;
                let password = self.password.as_ref().ok_or_else(|| {
                    Error::InvalidConfigValue("password is required when auth = basic".to_string())
                })?;
                let credentials = format!("{username}:{}", password.expose_secret());
                let encoded = general_purpose::STANDARD.encode(credentials.as_bytes());
                let mut value =
                    HeaderValue::from_str(&format!("Basic {encoded}")).map_err(|e| {
                        Error::InitError(format!("Invalid basic auth header value: {e}"))
                    })?;
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
            }
            AuthMode::Bearer => {
                let token = self.token.as_ref().ok_or_else(|| {
                    Error::InvalidConfigValue("token is required when auth = bearer".to_string())
                })?;
                let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                    .map_err(|e| {
                        Error::InitError(format!("Invalid bearer auth header value: {e}"))
                    })?;
                value.set_sensitive(true);
                headers.insert(AUTHORIZATION, value);
            }
        }
        Ok(headers)
    }

    fn dag_runs_url(&self, dag_id: &str) -> String {
        format!(
            "{}{}/dags/{}/dagRuns",
            self.base_url,
            self.api_prefix,
            encode_path_segment(dag_id)
        )
    }

    fn health_url(&self) -> String {
        format!("{}{}", self.base_url, self.health_path)
    }

    fn resolve_dag_id(&self, message: &ConsumedMessage) -> Result<String, Error> {
        if let Some(header_name) = self.dag_id_header.as_deref()
            && let Some(headers) = message.headers.as_ref()
        {
            for (key, value) in headers {
                if key.to_string_value().eq_ignore_ascii_case(header_name) {
                    let override_id = value.to_string_value();
                    if override_id.is_empty() {
                        return Err(Error::InvalidRecordValue(format!(
                            "header '{header_name}' is present but empty"
                        )));
                    }
                    return Ok(override_id);
                }
            }
        }
        if self.dag_id.is_empty() {
            return Err(Error::InvalidConfigValue(
                "dag_id is required when no per-message header override is present".to_string(),
            ));
        }
        Ok(self.dag_id.clone())
    }

    fn build_conf(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        payload: Payload,
    ) -> Result<serde_json::Value, Error> {
        let mut conf = match self.conf_mode {
            ConfMode::Payload => match payload {
                Payload::Json(value) => owned_value_to_serde_json(&value),
                Payload::Text(text) => serde_json::json!({ "payload": text }),
                Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                    // Binary payloads are base64 so Airflow conf stays valid JSON.
                    serde_json::json!({
                        "payload": general_purpose::STANDARD.encode(&bytes),
                        "iggy_payload_encoding": "base64",
                    })
                }
                Payload::Proto(proto) => serde_json::json!({
                    "payload": general_purpose::STANDARD.encode(proto.as_bytes()),
                    "iggy_payload_encoding": "base64",
                }),
            },
        };

        if self.include_iggy_metadata_in_conf {
            let conf_obj = conf.as_object_mut().ok_or_else(|| {
                Error::InvalidRecordValue(
                    "conf must be a JSON object to nest iggy metadata".to_string(),
                )
            })?;
            conf_obj.insert(
                "iggy".to_string(),
                serde_json::json!({
                    "stream": topic_metadata.stream,
                    "topic": topic_metadata.topic,
                    "partition_id": messages_metadata.partition_id,
                    "offset": message.offset,
                    "id": format_u128_as_hex(message.id),
                    "timestamp": message.timestamp,
                }),
            );
        }

        Ok(conf)
    }

    async fn trigger_one(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        mut message: ConsumedMessage,
    ) -> Result<(), Error> {
        let dag_id = self.resolve_dag_id(&message)?;
        let payload = std::mem::replace(&mut message.payload, Payload::Raw(vec![]));
        let conf = self.build_conf(&message, topic_metadata, messages_metadata, payload)?;
        let dag_run_id =
            build_dag_run_id(messages_metadata.partition_id, message.offset, message.id);

        let body = serde_json::json!({
            "dag_run_id": dag_run_id,
            "conf": conf,
        });
        let body_bytes = serde_json::to_vec(&body)
            .map_err(|e| Error::Serialization(format!("dag run body: {e}")))?;

        let url = self.dag_runs_url(&dag_id);
        let client = self.client()?;
        let headers = self.request_headers.as_ref().ok_or_else(|| {
            Error::InitError("request headers not initialized — was open() called?".to_string())
        })?;

        if self.verbose {
            debug!(
                "{CONNECTOR_NAME} ID: {} — triggering DAG '{}' at {} (dag_run_id={}, offset={})",
                self.id, dag_id, self.log_url, dag_run_id, message.offset
            );
        }

        self.trigger_attempts.fetch_add(1, Ordering::Relaxed);

        let response = client
            .post(&url)
            .headers(headers.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(Bytes::from(body_bytes))
            .send()
            .await
            .map_err(|e| {
                self.errors_count.fetch_add(1, Ordering::Relaxed);
                error!(
                    "{CONNECTOR_NAME} ID: {} — trigger request failed after retries: {e:#}",
                    self.id
                );
                Error::HttpRequestFailed(format!("Airflow trigger {url}: {e}"))
            })?;

        let status = response.status();
        let status_code = status.as_u16();

        // 2xx created / accepted; 409 means this dag_run_id already exists (idempotent replay).
        if status.is_success() || status_code == 409 {
            if self.verbose {
                debug!(
                    "{CONNECTOR_NAME} ID: {} — DAG '{}' trigger ok (status {})",
                    self.id, dag_id, status_code
                );
            }
            self.messages_triggered.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let response_body = match response.text().await {
            Ok(body) => body,
            Err(e) => format!("<body read error: {e}>"),
        };
        let truncated = truncate_response(&response_body, MAX_RESPONSE_LOG_BYTES);

        self.errors_count.fetch_add(1, Ordering::Relaxed);

        if is_permanent_status(status_code) {
            error!(
                "{CONNECTOR_NAME} ID: {} — permanent trigger failure for DAG '{}' \
                 (status {}). Response: {truncated}",
                self.id, dag_id, status_code
            );
            return Err(Error::PermanentHttpError(format!(
                "Airflow trigger status {status_code}: {truncated}"
            )));
        }

        error!(
            "{CONNECTOR_NAME} ID: {} — trigger failure for DAG '{}' (status {}). Response: {truncated}",
            self.id, dag_id, status_code
        );
        Err(Error::HttpRequestFailed(format!(
            "Airflow trigger status {status_code}: {truncated}"
        )))
    }
}

#[async_trait]
impl Sink for AirflowSink {
    async fn open(&mut self) -> Result<(), Error> {
        if self.base_url.is_empty() {
            return Err(Error::InitError(
                "base_url is required in [plugin_config]".to_string(),
            ));
        }
        match reqwest::Url::parse(&self.base_url) {
            Ok(parsed) => {
                let scheme = parsed.scheme();
                if scheme != "http" && scheme != "https" {
                    return Err(Error::InitError(format!(
                        "base_url scheme '{scheme}' is not allowed — only http/https \
                         (url: '{}')",
                        self.log_url
                    )));
                }
            }
            Err(e) => {
                return Err(Error::InitError(format!(
                    "base_url '{}' is not a valid URL: {e}",
                    self.log_url
                )));
            }
        }

        if self.dag_id.is_empty() && self.dag_id_header.is_none() {
            return Err(Error::InitError(
                "dag_id is required unless dag_id_header is set for per-message overrides"
                    .to_string(),
            ));
        }

        // Validate auth credentials early.
        self.request_headers = Some(self.build_auth_headers()?);
        self.client = Some(self.build_client()?);

        if self.health_check_enabled {
            let client = self.client.as_ref().expect("client just built");
            let headers = self
                .request_headers
                .as_ref()
                .expect("request_headers just built");
            let health_url = self.health_url();
            let response = client
                .get(&health_url)
                .headers(headers.clone())
                .send()
                .await
                .map_err(|e| {
                    Error::Connection(format!("Health check failed for '{}': {e}", self.log_url))
                })?;
            let status = response.status();
            if !status.is_success() {
                return Err(Error::Connection(format!(
                    "Health check returned status {} for '{}{}'",
                    status.as_u16(),
                    self.log_url,
                    self.health_path
                )));
            }
            info!(
                "{CONNECTOR_NAME} ID: {} — health check passed (status {})",
                self.id,
                status.as_u16()
            );
        }

        info!(
            "Opened {CONNECTOR_NAME} connector ID: {} for URL: {} (dag_id: {}, auth: {:?}, \
             max_retries: {})",
            self.id, self.log_url, self.dag_id, self.auth, self.max_retries
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        if self.verbose {
            info!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages from stream: {}, topic: {}, \
                 partition: {}, offset: {}",
                self.id,
                messages.len(),
                topic_metadata.stream,
                topic_metadata.topic,
                messages_metadata.partition_id,
                messages_metadata.current_offset
            );
        } else {
            debug!(
                "{CONNECTOR_NAME} ID: {} consuming {} messages",
                self.id,
                messages.len()
            );
        }

        let total = messages.len();
        let mut triggered = 0u64;
        let mut failures = 0u64;
        let mut consecutive_failures = 0u32;
        let mut last_error: Option<Error> = None;

        for message in messages {
            let offset = message.offset;
            match self
                .trigger_one(topic_metadata, &messages_metadata, message)
                .await
            {
                Ok(()) => {
                    triggered += 1;
                    consecutive_failures = 0;
                }
                Err(Error::PermanentHttpError(message)) => {
                    // Drop permanently bad records; do not abort the batch.
                    error!(
                        "{CONNECTOR_NAME} ID: {} dropping message at offset {} (permanent): {message}",
                        self.id, offset
                    );
                    failures += 1;
                    consecutive_failures = 0;
                }
                Err(error) => {
                    error!(
                        "{CONNECTOR_NAME} ID: {} failed to trigger at offset {}: {error}",
                        self.id, offset
                    );
                    failures += 1;
                    consecutive_failures += 1;
                    last_error = Some(error);
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        let processed = triggered + failures;
                        let skipped = (total as u64).saturating_sub(processed);
                        error!(
                            "{CONNECTOR_NAME} ID: {} aborting batch after {} consecutive failures \
                             ({} remaining messages skipped)",
                            self.id, consecutive_failures, skipped
                        );
                        self.errors_count.fetch_add(skipped, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        match last_error {
            Some(error) => {
                error!(
                    "{CONNECTOR_NAME} ID: {} partial delivery: {}/{} triggered, {} failures",
                    self.id, triggered, total, failures
                );
                Err(error)
            }
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        let attempts = self.trigger_attempts.load(Ordering::Relaxed);
        let triggered = self.messages_triggered.load(Ordering::Relaxed);
        let errors = self.errors_count.load(Ordering::Relaxed);
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, trigger_attempts: {}, \
             messages_triggered: {}, errors: {}",
            self.id, attempts, triggered, errors
        );
        self.request_headers = None;
        self.client = None;
        Ok(())
    }
}

/// Retry 429/5xx and network errors. Treat 409 as success (no retry).
struct AirflowRetryStrategy;

impl RetryableStrategy for AirflowRetryStrategy {
    fn handle(&self, res: &reqwest_middleware::Result<reqwest::Response>) -> Option<Retryable> {
        match res {
            Ok(response) => {
                let status = response.status().as_u16();
                if (200..300).contains(&status) || status == 409 {
                    return None;
                }
                match status {
                    429 | 500 | 502 | 503 | 504 => Some(Retryable::Transient),
                    _ => Some(Retryable::Fatal),
                }
            }
            Err(_) => Some(Retryable::Transient),
        }
    }
}

fn is_permanent_status(status: u16) -> bool {
    matches!(status, 400 | 401 | 403 | 404 | 405 | 422)
}

/// Deterministic DAG run id for idempotent redelivery.
fn build_dag_run_id(partition_id: u32, offset: u64, message_id: u128) -> String {
    format!(
        "iggy-{partition_id}-{offset}-{}",
        format_u128_as_hex(message_id)
    )
}

fn format_u128_as_hex(id: u128) -> String {
    format!("{id:032x}")
}

fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!("Invalid duration '{raw}': {e}, using default '{default}'");
            *HumanDuration::from_str(default).expect("default duration must be valid")
        })
}

fn normalize_path_prefix(path: &str) -> String {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return DEFAULT_API_PREFIX.to_string();
    }
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return DEFAULT_HEALTH_PATH.to_string();
    }
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Minimal path-segment encoding for DAG ids (slashes and spaces).
fn encode_path_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
    }
    out
}

fn truncate_response(body: &str, max_len: usize) -> &str {
    if body.len() <= max_len {
        body
    } else {
        let end = body.floor_char_boundary(max_len);
        &body[..end]
    }
}

fn sanitize_url_for_log(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) if parsed.username().is_empty() && parsed.password().is_none() => {
            url.to_string()
        }
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        Err(_) => {
            if let Some(scheme_end) = url.find("://") {
                let after_scheme = &url[scheme_end + 3..];
                if let Some(at_pos) = after_scheme.find('@') {
                    let slash_pos = after_scheme.find('/').unwrap_or(after_scheme.len());
                    if at_pos < slash_pos {
                        return format!(
                            "{}{}",
                            &url[..scheme_end + 3],
                            &after_scheme[at_pos + 1..]
                        );
                    }
                }
            }
            url.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;
    use std::collections::BTreeMap;

    fn test_config() -> AirflowSinkConfig {
        AirflowSinkConfig {
            base_url: "http://localhost:8080".to_string(),
            dag_id: "example_dag".to_string(),
            api_prefix: None,
            auth: Some(AuthMode::None),
            username: None,
            password: None,
            token: None,
            dag_id_header: None,
            conf_mode: None,
            include_iggy_metadata_in_conf: None,
            health_check_enabled: Some(false),
            health_path: None,
            timeout: None,
            max_retries: None,
            retry_delay: None,
            retry_backoff_multiplier: None,
            max_retry_delay: None,
            tls_danger_accept_invalid_certs: None,
            max_connections: None,
            verbose_logging: None,
        }
    }

    fn sample_message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset,
            timestamp: 1_700_000_000_000_000,
            origin_timestamp: 1_700_000_000_000_000,
            checksum: 0,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_all_none_optional_fields_when_new_should_apply_defaults() {
        let sink = AirflowSink::new(1, test_config());
        assert_eq!(sink.api_prefix, "/api/v1");
        assert_eq!(sink.auth, AuthMode::None);
        assert_eq!(sink.conf_mode, ConfMode::Payload);
        assert!(!sink.include_iggy_metadata_in_conf);
        assert!(!sink.health_check_enabled);
        assert_eq!(sink.health_path, DEFAULT_HEALTH_PATH);
        assert_eq!(sink.timeout, Duration::from_secs(30));
        assert_eq!(sink.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(sink.retry_delay, Duration::from_secs(1));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(30));
        assert_eq!(sink.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert!(!sink.verbose);
        assert_eq!(sink.base_url, "http://localhost:8080");
    }

    #[test]
    fn given_trailing_slash_base_url_when_new_should_strip_it() {
        let mut config = test_config();
        config.base_url = "http://localhost:8080/".to_string();
        let sink = AirflowSink::new(1, config);
        assert_eq!(sink.base_url, "http://localhost:8080");
    }

    #[test]
    fn given_partition_offset_id_when_build_dag_run_id_should_be_deterministic() {
        let first = build_dag_run_id(0, 7, 42);
        let second = build_dag_run_id(0, 7, 42);
        assert_eq!(first, second);
        assert_eq!(first, format!("iggy-0-7-{}", format_u128_as_hex(42)));
    }

    #[test]
    fn given_special_dag_id_when_encode_path_segment_should_percent_encode() {
        assert_eq!(encode_path_segment("my dag"), "my%20dag");
        assert_eq!(encode_path_segment("a/b"), "a%2Fb");
        assert_eq!(encode_path_segment("plain_dag-1.0"), "plain_dag-1.0");
    }

    #[test]
    fn given_status_codes_when_classify_should_mark_permanent() {
        assert!(is_permanent_status(400));
        assert!(is_permanent_status(401));
        assert!(is_permanent_status(403));
        assert!(is_permanent_status(404));
        assert!(is_permanent_status(422));
        assert!(!is_permanent_status(409));
        assert!(!is_permanent_status(500));
        assert!(!is_permanent_status(429));
    }

    fn json_payload(raw: &str) -> Payload {
        let mut bytes = raw.as_bytes().to_vec();
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("valid JSON"))
    }

    #[test]
    fn given_json_payload_when_build_conf_should_use_object() {
        let sink = AirflowSink::new(1, test_config());
        let message = sample_message(3, json_payload(r#"{"order_id":1}"#));
        let topic = TopicMetadata {
            stream: "orders".to_string(),
            topic: "created".to_string(),
        };
        let meta = MessagesMetadata {
            partition_id: 0,
            current_offset: 3,
            schema: Schema::Json,
        };
        let conf = sink
            .build_conf(&message, &topic, &meta, json_payload(r#"{"order_id":1}"#))
            .expect("conf");
        assert_eq!(conf["order_id"], 1);
        assert!(conf.get("iggy").is_none());
    }

    #[test]
    fn given_include_iggy_metadata_when_build_conf_should_nest_iggy_object() {
        let mut config = test_config();
        config.include_iggy_metadata_in_conf = Some(true);
        let sink = AirflowSink::new(1, config);
        let message = sample_message(3, json_payload(r#"{"order_id":1}"#));
        let topic = TopicMetadata {
            stream: "orders".to_string(),
            topic: "created".to_string(),
        };
        let meta = MessagesMetadata {
            partition_id: 2,
            current_offset: 3,
            schema: Schema::Json,
        };
        let conf = sink
            .build_conf(&message, &topic, &meta, json_payload(r#"{"order_id":1}"#))
            .expect("conf");
        assert_eq!(conf["order_id"], 1);
        assert_eq!(conf["iggy"]["stream"], "orders");
        assert_eq!(conf["iggy"]["topic"], "created");
        assert_eq!(conf["iggy"]["partition_id"], 2);
        assert_eq!(conf["iggy"]["offset"], 3);
    }

    #[test]
    fn given_dag_id_header_when_resolve_should_override_config() {
        use iggy_common::{HeaderKey, HeaderValue};

        let mut config = test_config();
        config.dag_id_header = Some("airflow_dag_id".to_string());
        let sink = AirflowSink::new(1, config);

        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::try_from("airflow_dag_id").unwrap(),
            HeaderValue::try_from("override_dag").unwrap(),
        );
        let mut message = sample_message(0, Payload::Text("x".into()));
        message.headers = Some(headers);

        let resolved = sink.resolve_dag_id(&message).expect("dag id");
        assert_eq!(resolved, "override_dag");
    }

    #[test]
    fn given_config_toml_when_deserialize_should_parse_plugin_fields() {
        let raw = r#"
base_url = "http://airflow:8080"
dag_id = "demo"
auth = "basic"
username = "admin"
password = "secret"
"#;
        let config: AirflowSinkConfig = toml::from_str(raw).expect("toml");
        assert_eq!(config.base_url, "http://airflow:8080");
        assert_eq!(config.dag_id, "demo");
        assert_eq!(config.auth, Some(AuthMode::Basic));
        assert_eq!(config.username.as_deref(), Some("admin"));
        assert_eq!(
            config
                .password
                .as_ref()
                .map(|p| p.expose_secret().to_string()),
            Some("secret".to_string())
        );
    }

    #[test]
    fn given_url_with_userinfo_when_sanitize_should_strip_credentials() {
        let sanitized = sanitize_url_for_log("http://user:pass@localhost:8080/path");
        assert!(!sanitized.contains("user"));
        assert!(!sanitized.contains("pass"));
        assert!(sanitized.contains("localhost:8080"));
    }

    #[test]
    fn given_dag_id_when_dag_runs_url_should_include_api_prefix() {
        let sink = AirflowSink::new(1, test_config());
        assert_eq!(
            sink.dag_runs_url("example_dag"),
            "http://localhost:8080/api/v1/dags/example_dag/dagRuns"
        );
    }
}
