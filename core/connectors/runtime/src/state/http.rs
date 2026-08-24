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

use crate::configs::runtime::{HttpStateConfig, HttpStateMethod, RetryConfig};
use crate::error::RuntimeError;
use crate::state::{StateProvider, StateStorage, StateStorageFactory};
use bytes::Bytes;
use iggy_connector_sdk::{ConnectorState, Error};
use reqwest::header::{
    CONTENT_TYPE, ETAG, HeaderMap, HeaderName, HeaderValue, IF_MATCH, IF_NONE_MATCH, RETRY_AFTER,
};
use reqwest::{Method, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, RequestBuilder};
use reqwest_tracing::{SpanBackendWithUrl, TracingMiddleware};
use secrecy::ExposeSecret;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const IDEMPOTENCY_KEY_HEADER: HeaderName = HeaderName::from_static("idempotency-key");
const OCTET_STREAM: &str = "application/octet-stream";
const ERROR_BODY_SNIPPET_CHARS: usize = 256;

/// Builds [`HttpStateProvider`]s that store source state at
/// `{url}/source_{connector_key}` on a generic HTTP state server. The shared
/// client (connection pool, static headers, timeout) is built eagerly so a
/// bad URL, TLS, or header configuration fails at boot.
pub struct HttpStateFactory {
    client: ClientWithMiddleware,
    base_url: Url,
    base_url_label: String,
    load_method: Method,
    save_method: Method,
    retry: RetryPolicy,
}

impl HttpStateFactory {
    pub fn new(config: &HttpStateConfig) -> Result<Self, RuntimeError> {
        if config.url.trim().is_empty() {
            return Err(RuntimeError::InvalidConfiguration(
                "state.http.url is required when state.storage = \"http\"".to_string(),
            ));
        }
        let base_url = Url::parse(&config.url).map_err(|parse_error| {
            RuntimeError::InvalidConfiguration(format!("Invalid state.http.url: {parse_error}"))
        })?;
        if base_url.cannot_be_a_base() || !matches!(base_url.scheme(), "http" | "https") {
            return Err(RuntimeError::InvalidConfiguration(
                "state.http.url must be an http(s) URL".to_string(),
            ));
        }
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(RuntimeError::InvalidConfiguration(
                "state.http.url must not contain user credentials; use request_headers instead"
                    .to_string(),
            ));
        }
        if base_url.fragment().is_some() {
            return Err(RuntimeError::InvalidConfiguration(
                "state.http.url must not contain a fragment".to_string(),
            ));
        }
        let base_url_label = url_label(&base_url);
        let load_method = load_method(config.load_method)?;
        let save_method = save_method(config.save_method)?;

        let mut headers = HeaderMap::new();
        for (name, value) in &config.request_headers {
            let header_name = HeaderName::from_str(name).map_err(|header_error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Invalid state.http header name '{name}': {header_error}"
                ))
            })?;
            if is_runtime_managed_header(&header_name) {
                return Err(RuntimeError::InvalidConfiguration(format!(
                    "state.http.request_headers cannot override runtime-managed header '{name}'"
                )));
            }
            // The parse error never echoes the value, and marking it
            // sensitive keeps it out of any Debug output downstream.
            let mut header_value =
                HeaderValue::from_str(value.expose_secret()).map_err(|header_error| {
                    RuntimeError::InvalidConfiguration(format!(
                        "Invalid state.http header value for '{name}': {header_error}"
                    ))
                })?;
            header_value.set_sensitive(true);
            headers.insert(header_name, header_value);
        }

        // Redirects are disabled: a redirected conditional PUT could drop the
        // body or method, so any 3xx is surfaced as a protocol violation.
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(config.timeout.get_duration())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|client_error| {
                RuntimeError::InvalidConfiguration(format!(
                    "Failed to build the state HTTP client: {client_error}"
                ))
            })?;
        let client = ClientBuilder::new(client)
            .with(TracingMiddleware::<SpanBackendWithUrl>::new())
            .build();

        Ok(Self {
            client,
            base_url,
            base_url_label,
            load_method,
            save_method,
            retry: RetryPolicy::from(&config.retry),
        })
    }
}

impl StateStorageFactory for HttpStateFactory {
    fn storage_for(&self, connector_key: &str) -> Result<StateStorage, RuntimeError> {
        let mut resource_url = self.base_url.clone();
        {
            let mut segments = resource_url.path_segments_mut().map_err(|_| {
                RuntimeError::InvalidConfiguration(format!(
                    "State URL cannot host per-connector resources: {}",
                    self.base_url_label
                ))
            })?;
            // push() percent-encodes the segment, so any connector key is safe
            // in the path. pop_if_empty() tolerates a trailing slash in the
            // configured base URL.
            segments.pop_if_empty();
            segments.push(&format!("source_{connector_key}"));
        }
        Ok(StateStorage::Http(Box::new(HttpStateProvider::new(
            self.client.clone(),
            url_label(&resource_url),
            resource_url,
            self.load_method.clone(),
            self.save_method.clone(),
            self.retry.clone(),
        ))))
    }
}

impl fmt::Debug for HttpStateFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpStateFactory")
            .field("base_url", &self.base_url_label)
            .field("load_method", &self.load_method)
            .field("save_method", &self.save_method)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

/// Stores one source connector's state on an HTTP state server with
/// optimistic concurrency: every read remembers the returned `ETag`, every
/// write is conditional (`If-Match`, or `If-None-Match: *` before the first
/// write), and every write carries an `Idempotency-Key` that is stable across
/// the retries of one logical save.
///
/// Errors are classified per the state protocol contract: `5xx`, timeouts and
/// connect failures are [`Error::TransientState`] after bounded retries;
/// version conflicts, lost authorization and protocol violations are
/// [`Error::PermanentState`]. Any permanent save error latches the provider,
/// so later saves fail fast with [`Error::StateLatched`] without touching the
/// network.
pub struct HttpStateProvider {
    client: ClientWithMiddleware,
    resource_label: String,
    resource_url: Url,
    load_method: Method,
    save_method: Method,
    retry: RetryPolicy,
    tracker: Mutex<StateTracker>,
    has_pending: AtomicBool,
    latched: AtomicBool,
}

impl HttpStateProvider {
    pub(crate) fn new(
        client: ClientWithMiddleware,
        resource_label: String,
        resource_url: Url,
        load_method: Method,
        save_method: Method,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            client,
            resource_label,
            resource_url,
            load_method,
            save_method,
            retry,
            tracker: Mutex::new(StateTracker::default()),
            has_pending: AtomicBool::new(false),
            latched: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_latched(&self) -> bool {
        self.latched.load(Ordering::Acquire)
    }

    pub(crate) async fn resolve_pending(&self) -> Result<(), Error> {
        if self.is_latched() {
            return Err(Error::StateLatched);
        }
        if !self.has_pending.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut tracker = self.tracker.lock().await;
        if self.is_latched() {
            return Err(Error::StateLatched);
        }
        if tracker.pending.is_none() {
            self.has_pending.store(false, Ordering::Release);
            return Ok(());
        }
        self.commit_pending(&mut tracker).await
    }

    async fn load_with_version(
        &self,
        version: &mut TrackedVersion,
    ) -> Result<Option<ConnectorState>, Error> {
        match self.execute_load_with_retry().await? {
            LoadResponse::Found { etag, bytes } => {
                debug!(
                    "Loaded state from {} ({} bytes)",
                    self.resource_label,
                    bytes.len()
                );
                *version = TrackedVersion::Etag(etag);
                // A zero-length body is valid state, unlike the file backend
                // where an empty file means "no state yet".
                Ok(Some(ConnectorState(bytes.to_vec())))
            }
            LoadResponse::NotFound => {
                info!("No state stored at {}, starting fresh", self.resource_label);
                *version = TrackedVersion::Absent;
                Ok(None)
            }
            LoadResponse::Failure(response) => {
                let status = response.status();
                Err(Error::PermanentState(
                    describe_failure("load", status, &self.resource_label, response).await,
                ))
            }
        }
    }

    async fn execute_load_with_retry(&self) -> Result<LoadResponse, Error> {
        let max_attempts = if self.retry.enabled {
            self.retry.max_attempts
        } else {
            0
        };
        let mut attempt = 0u32;
        loop {
            let failure = match self
                .client
                .request(self.load_method.clone(), self.resource_url.clone())
                .send()
                .await
            {
                Ok(response) if is_transient_status(response.status()) => {
                    TransientFailure::Status(response)
                }
                Ok(response) if response.status() == StatusCode::OK => {
                    let etag = required_etag(&response, "load", &self.resource_label)?;
                    match response.bytes().await {
                        Ok(bytes) => return Ok(LoadResponse::Found { etag, bytes }),
                        Err(read_error) => TransientFailure::Read(read_error.to_string()),
                    }
                }
                Ok(response) if response.status() == StatusCode::NOT_FOUND => {
                    return Ok(LoadResponse::NotFound);
                }
                Ok(response) => return Ok(LoadResponse::Failure(response)),
                Err(send_error) => TransientFailure::Send(send_error),
            };
            if attempt >= max_attempts {
                return Err(Error::TransientState(failure.describe(
                    "load",
                    &self.resource_label,
                    attempt + 1,
                )));
            }
            let delay = self.retry.delay_for(attempt, failure.retry_after());
            warn!(
                "load against {} hit a transient failure, retrying in {delay:?} (attempt {} of {})",
                self.resource_label,
                attempt + 1,
                max_attempts + 1
            );
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn execute_with_retry<F>(
        &self,
        method: Method,
        operation: &str,
        build_request: F,
    ) -> Result<reqwest::Response, Error>
    where
        F: Fn(RequestBuilder) -> RequestBuilder,
    {
        let max_attempts = if self.retry.enabled {
            self.retry.max_attempts
        } else {
            0
        };
        let mut attempt = 0u32;
        loop {
            let request = build_request(
                self.client
                    .request(method.clone(), self.resource_url.clone()),
            );
            let failure = match request.send().await {
                Ok(response) if !is_transient_status(response.status()) => return Ok(response),
                Ok(response) => TransientFailure::Status(response),
                Err(send_error) => TransientFailure::Send(send_error),
            };
            if attempt >= max_attempts {
                return Err(Error::TransientState(failure.describe(
                    operation,
                    &self.resource_label,
                    attempt + 1,
                )));
            }
            let delay = self.retry.delay_for(attempt, failure.retry_after());
            warn!(
                "{operation} against {} hit a transient failure, retrying in {delay:?} (attempt {} of {})",
                self.resource_label,
                attempt + 1,
                max_attempts + 1
            );
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn commit_pending(&self, tracker: &mut StateTracker) -> Result<(), Error> {
        let pending = tracker.pending.clone().ok_or_else(|| {
            Error::PermanentState(format!(
                "No pending state write exists for {}",
                self.resource_label
            ))
        })?;
        let response = self
            .execute_with_retry(self.save_method.clone(), "save", |request| {
                request
                    .header(
                        pending.condition_name.clone(),
                        pending.condition_value.clone(),
                    )
                    .header(
                        IDEMPOTENCY_KEY_HEADER.clone(),
                        pending.idempotency_key.as_str(),
                    )
                    .header(CONTENT_TYPE, OCTET_STREAM)
                    .body(pending.body.clone())
            })
            .await?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => {
                match required_etag(&response, "save", &self.resource_label) {
                    Ok(etag) => {
                        debug!(
                            "Saved state to {} ({} bytes)",
                            self.resource_label,
                            pending.body.len()
                        );
                        tracker.version = TrackedVersion::Etag(etag);
                        tracker.pending = None;
                        self.has_pending.store(false, Ordering::Release);
                        Ok(())
                    }
                    Err(etag_error) => {
                        self.latch();
                        Err(etag_error)
                    }
                }
            }
            status => {
                let mut message =
                    describe_failure("save", status, &self.resource_label, response).await;
                if matches!(
                    status,
                    StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT
                ) {
                    message.push_str(
                        "; the stored state changed under this writer or its write authority was revoked",
                    );
                }
                self.latch();
                Err(Error::PermanentState(message))
            }
        }
    }

    fn latch(&self) {
        self.latched.store(true, Ordering::Release);
        error!(
            "State provider for {} latched after a permanent save error; further saves fail fast until restart",
            self.resource_label
        );
    }
}

impl StateProvider for HttpStateProvider {
    async fn load(&self) -> Result<Option<ConnectorState>, Error> {
        let mut tracker = self.tracker.lock().await;
        let result = self.load_with_version(&mut tracker.version).await;
        if result.is_ok() {
            tracker.pending = None;
            self.has_pending.store(false, Ordering::Release);
        }
        result
    }

    async fn save(&self, state: ConnectorState) -> Result<(), Error> {
        if self.is_latched() {
            return Err(Error::StateLatched);
        }
        let mut tracker = self.tracker.lock().await;
        // Re-check under the lock: a concurrent save may have latched while
        // this one was waiting.
        if self.is_latched() {
            return Err(Error::StateLatched);
        }
        if matches!(tracker.version, TrackedVersion::Unknown) {
            // Never issue an unconditional write. The runtime always loads at
            // init, so this safety net should not trigger in practice.
            warn!(
                "Saving state to {} before any load; loading first to resolve the stored version",
                self.resource_label
            );
            self.load_with_version(&mut tracker.version).await?;
        }
        let body = Bytes::from(state.0);
        if let Some(pending) = tracker.pending.as_ref() {
            if pending.body == body {
                return self.commit_pending(&mut tracker).await;
            }
            self.commit_pending(&mut tracker).await?;
        }
        tracker.pending = Some(PendingSave::new(
            &tracker.version,
            body,
            &self.resource_label,
        )?);
        self.has_pending.store(true, Ordering::Release);
        self.commit_pending(&mut tracker).await
    }
}

impl fmt::Debug for HttpStateProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpStateProvider")
            .field("resource_url", &self.resource_label)
            .field("load_method", &self.load_method)
            .field("save_method", &self.save_method)
            .field("retry", &self.retry)
            .field("has_pending", &self.has_pending.load(Ordering::Relaxed))
            .field("latched", &self.latched.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct StateTracker {
    version: TrackedVersion,
    pending: Option<PendingSave>,
}

#[derive(Debug, Clone)]
struct PendingSave {
    condition_name: HeaderName,
    condition_value: HeaderValue,
    idempotency_key: String,
    body: Bytes,
}

impl PendingSave {
    fn new(version: &TrackedVersion, body: Bytes, url: &str) -> Result<Self, Error> {
        let (condition_name, condition_value) = match version {
            TrackedVersion::Etag(etag) => (
                IF_MATCH,
                HeaderValue::from_str(etag).map_err(|_| {
                    Error::PermanentState(format!(
                        "Tracked ETag for {url} is not a valid header value"
                    ))
                })?,
            ),
            TrackedVersion::Absent => (IF_NONE_MATCH, HeaderValue::from_static("*")),
            TrackedVersion::Unknown => {
                return Err(Error::PermanentState(format!(
                    "State version for {url} is unresolved after load"
                )));
            }
        };
        Ok(Self {
            condition_name,
            condition_value,
            idempotency_key: Uuid::new_v4().to_string(),
            body,
        })
    }
}

enum LoadResponse {
    Found { etag: String, bytes: Bytes },
    NotFound,
    Failure(reqwest::Response),
}

#[derive(Debug, Default)]
enum TrackedVersion {
    #[default]
    Unknown,
    Absent,
    Etag(String),
}

#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    enabled: bool,
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    backoff_multiplier: u32,
}

impl RetryPolicy {
    fn delay_for(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        // Retry-After is honored but capped at max_backoff so a hostile or
        // misconfigured server cannot stall the forwarding loop indefinitely.
        if let Some(retry_after) = retry_after {
            return retry_after.min(self.max_backoff);
        }
        let factor = self
            .backoff_multiplier
            .checked_pow(attempt)
            .unwrap_or(u32::MAX);
        self.initial_backoff
            .saturating_mul(factor)
            .min(self.max_backoff)
    }
}

impl From<&RetryConfig> for RetryPolicy {
    fn from(config: &RetryConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_attempts: config.max_attempts,
            initial_backoff: config.initial_backoff.get_duration(),
            max_backoff: config.max_backoff.get_duration(),
            backoff_multiplier: config.backoff_multiplier,
        }
    }
}

enum TransientFailure {
    Status(reqwest::Response),
    Send(reqwest_middleware::Error),
    Read(String),
}

impl TransientFailure {
    fn retry_after(&self) -> Option<Duration> {
        match self {
            TransientFailure::Status(response) => response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(Duration::from_secs),
            TransientFailure::Send(_) | TransientFailure::Read(_) => None,
        }
    }

    fn describe(&self, operation: &str, url: &str, attempts: u32) -> String {
        match self {
            TransientFailure::Status(response) => format!(
                "{operation} against {url} still failing with HTTP {} after {attempts} attempts",
                response.status()
            ),
            TransientFailure::Send(send_error) => {
                format!(
                    "{operation} against {url} still failing after {attempts} attempts: {send_error}"
                )
            }
            TransientFailure::Read(read_error) => format!(
                "{operation} against {url} still failing while reading the response body after {attempts} attempts: {read_error}"
            ),
        }
    }
}

fn url_label(url: &Url) -> String {
    let mut label = url.clone();
    if label.query().is_some() {
        label.set_query(Some("redacted"));
    }
    label.to_string()
}

fn load_method(method: HttpStateMethod) -> Result<Method, RuntimeError> {
    match method {
        HttpStateMethod::Get => Ok(Method::GET),
        HttpStateMethod::Post => Ok(Method::POST),
        other => Err(RuntimeError::InvalidConfiguration(format!(
            "state.http.load_method must be GET or POST, got {other}"
        ))),
    }
}

fn save_method(method: HttpStateMethod) -> Result<Method, RuntimeError> {
    match method {
        HttpStateMethod::Put => Ok(Method::PUT),
        HttpStateMethod::Post => Ok(Method::POST),
        HttpStateMethod::Patch => Ok(Method::PATCH),
        other => Err(RuntimeError::InvalidConfiguration(format!(
            "state.http.save_method must be PUT, POST, or PATCH, got {other}"
        ))),
    }
}

fn is_transient_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_EARLY
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn required_etag(
    response: &reqwest::Response,
    operation: &str,
    url: &str,
) -> Result<String, Error> {
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            Error::PermanentState(format!(
                "{operation} against {url} succeeded with HTTP {} but returned no usable ETag; \
                 the state server violates the protocol contract",
                response.status()
            ))
        })?;
    if !is_strong_etag(etag) {
        return Err(Error::PermanentState(format!(
            "{operation} against {url} succeeded with HTTP {} but returned invalid strong ETag '{etag}'; the state server violates the protocol contract",
            response.status()
        )));
    }
    Ok(etag.to_owned())
}

fn is_strong_etag(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2
        && bytes.first() == Some(&b'"')
        && bytes.last() == Some(&b'"')
        && bytes[1..bytes.len() - 1]
            .iter()
            .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte))
}

fn is_runtime_managed_header(name: &HeaderName) -> bool {
    name == IF_MATCH
        || name == IF_NONE_MATCH
        || name == IDEMPOTENCY_KEY_HEADER
        || name == CONTENT_TYPE
}

async fn describe_failure(
    operation: &str,
    status: StatusCode,
    url: &str,
    response: reqwest::Response,
) -> String {
    let detail = match response.text().await {
        Ok(body) if !body.is_empty() => {
            let snippet: String = body.chars().take(ERROR_BODY_SNIPPET_CHARS).collect();
            format!(" - {snippet}")
        }
        _ => String::new(),
    };
    format!("{operation} against {url} returned HTTP {status}{detail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::IggyDuration;
    use secrecy::SecretString;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Instant;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const RESOURCE_PATH: &str = "/source_test";

    fn test_config(url: &str) -> HttpStateConfig {
        HttpStateConfig {
            url: url.to_string(),
            load_method: HttpStateMethod::Get,
            save_method: HttpStateMethod::Put,
            timeout: IggyDuration::new(Duration::from_secs(5)),
            request_headers: HashMap::new(),
            retry: RetryConfig {
                enabled: true,
                max_attempts: 2,
                initial_backoff: IggyDuration::new(Duration::from_millis(1)),
                max_backoff: IggyDuration::new(Duration::from_millis(5)),
                backoff_multiplier: 2,
            },
        }
    }

    fn storage_for(config: &HttpStateConfig) -> StateStorage {
        HttpStateFactory::new(config)
            .expect("test factory should build")
            .storage_for("test")
            .expect("test storage should build")
    }

    fn storage(server: &MockServer) -> StateStorage {
        storage_for(&test_config(&server.uri()))
    }

    fn ok_with_etag(etag: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).insert_header(ETAG, etag)
    }

    async fn requests_of(server: &MockServer, http_method: Method) -> Vec<Request> {
        server
            .received_requests()
            .await
            .expect("request recording is enabled")
            .into_iter()
            .filter(|request| request.method == http_method)
            .collect()
    }

    #[tokio::test]
    async fn given_stored_state_when_loaded_should_return_bytes_and_track_etag() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\"").set_body_bytes(vec![1, 2, 3]))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .and(header(IF_MATCH.as_str(), "\"v1\""))
            .respond_with(ok_with_etag("\"v2\""))
            .mount(&server)
            .await;

        let storage = storage(&server);
        let loaded = storage.load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![1, 2, 3]);
        storage
            .save(ConnectorState(vec![4, 5]))
            .await
            .expect("save with the tracked ETag should hit the If-Match mock");
    }

    #[tokio::test]
    async fn given_empty_body_when_loaded_should_return_empty_state() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\""))
            .mount(&server)
            .await;

        let loaded = storage(&server).load().await.unwrap();
        assert_eq!(
            loaded.expect("zero-length body is valid state").0,
            Vec::<u8>::new()
        );
    }

    #[tokio::test]
    async fn given_missing_state_when_loaded_should_return_none_then_create_on_save() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .and(header(IF_NONE_MATCH.as_str(), "*"))
            .and(header(CONTENT_TYPE.as_str(), OCTET_STREAM))
            .respond_with(ResponseTemplate::new(201).insert_header(ETAG, "\"v1\""))
            .mount(&server)
            .await;

        let storage = storage(&server);
        assert!(storage.load().await.unwrap().is_none());
        storage
            .save(ConnectorState(vec![7]))
            .await
            .expect("first save should create via If-None-Match: *");
        let puts = requests_of(&server, Method::PUT).await;
        assert_eq!(puts.len(), 1);
        assert!(puts[0].headers.get(&IDEMPOTENCY_KEY_HEADER).is_some());
    }

    #[tokio::test]
    async fn given_success_without_etag_when_loaded_should_classify_permanent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1]))
            .mount(&server)
            .await;

        let result = storage(&server).load().await;
        assert!(
            matches!(result, Err(Error::PermanentState(_))),
            "missing ETag on load must be a protocol violation, got {result:?}"
        );
    }

    #[tokio::test]
    async fn given_non_strong_etag_when_loaded_should_classify_permanent() {
        for etag in ["W/\"v1\"", "*"] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(RESOURCE_PATH))
                .respond_with(ResponseTemplate::new(200).insert_header(ETAG, etag))
                .mount(&server)
                .await;

            let result = storage(&server).load().await;
            assert!(
                matches!(result, Err(Error::PermanentState(_))),
                "ETag {etag} must not be accepted for optimistic concurrency, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn given_terminal_statuses_when_loaded_should_classify_permanent() {
        for status in [401u16, 403, 400] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(RESOURCE_PATH))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;

            let result = storage(&server).load().await;
            assert!(
                matches!(result, Err(Error::PermanentState(_))),
                "HTTP {status} on load must be permanent, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn given_server_error_then_success_when_loaded_should_retry() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\"").set_body_bytes(vec![9]))
            .mount(&server)
            .await;

        let loaded = storage(&server).load().await.unwrap().unwrap();
        assert_eq!(loaded.0, vec![9]);
    }

    #[tokio::test]
    async fn given_persistent_server_errors_when_loaded_should_exhaust_as_transient() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(503))
            .expect(3) // initial try + max_attempts retries
            .mount(&server)
            .await;

        let result = storage(&server).load().await;
        assert!(
            matches!(result, Err(Error::TransientState(_))),
            "exhausted retries must be transient, got {result:?}"
        );
    }

    #[tokio::test]
    async fn given_retry_after_when_retrying_should_wait_at_least_that_long() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "1"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\""))
            .mount(&server)
            .await;

        let mut config = test_config(&server.uri());
        config.retry.max_backoff = IggyDuration::new(Duration::from_secs(2));
        let started = Instant::now();
        storage_for(&config).load().await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "Retry-After: 1 should delay the retry, elapsed {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn given_unresponsive_server_when_loaded_should_classify_transient() {
        // Bound but never accepted: the request times out instead of racing
        // other tests for a recycled port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut config = test_config(&format!("http://127.0.0.1:{port}"));
        config.timeout = IggyDuration::new(Duration::from_millis(200));
        config.retry.enabled = false;

        let result = storage_for(&config).load().await;
        assert!(
            matches!(result, Err(Error::TransientState(_))),
            "a timed-out request must be transient, got {result:?}"
        );
    }

    #[tokio::test]
    async fn given_version_conflict_when_saved_should_latch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(412))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        let result = storage.save(ConnectorState(vec![1])).await;
        assert!(matches!(result, Err(Error::PermanentState(_))));

        let requests_before = requests_of(&server, Method::PUT).await.len();
        let latched = storage.save(ConnectorState(vec![2])).await;
        assert!(
            matches!(latched, Err(Error::StateLatched)),
            "expected StateLatched, got {latched:?}"
        );
        assert_eq!(
            requests_of(&server, Method::PUT).await.len(),
            requests_before,
            "a latched save must not touch the network"
        );
    }

    #[tokio::test]
    async fn given_forbidden_save_should_latch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        assert!(matches!(
            storage.save(ConnectorState(vec![1])).await,
            Err(Error::PermanentState(_))
        ));
        assert!(matches!(
            storage.save(ConnectorState(vec![2])).await,
            Err(Error::StateLatched)
        ));
    }

    #[tokio::test]
    async fn given_save_success_without_etag_should_latch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        assert!(matches!(
            storage.save(ConnectorState(vec![1])).await,
            Err(Error::PermanentState(_))
        ));
        assert!(matches!(
            storage.save(ConnectorState(vec![2])).await,
            Err(Error::StateLatched)
        ));
    }

    #[tokio::test]
    async fn given_transient_save_failures_when_retried_should_reuse_idempotency_key_and_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\""))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        storage.save(ConnectorState(vec![1, 2, 3])).await.unwrap();

        let puts = requests_of(&server, Method::PUT).await;
        assert_eq!(puts.len(), 3, "one initial try plus two retries");
        let first_key = puts[0]
            .headers
            .get(&IDEMPOTENCY_KEY_HEADER)
            .expect("idempotency key must be present")
            .clone();
        for put in &puts {
            assert_eq!(
                put.headers.get(&IDEMPOTENCY_KEY_HEADER),
                Some(&first_key),
                "every retry of one logical save must reuse the same key"
            );
            assert_eq!(put.body, vec![1u8, 2, 3], "retries must be byte-identical");
            assert_eq!(
                put.headers.get(IF_NONE_MATCH).map(|v| v.as_bytes()),
                Some(b"*".as_slice())
            );
        }
    }

    struct CommitThenTimeout {
        committed_key: StdMutex<Option<String>>,
    }

    impl Respond for CommitThenTimeout {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let key = request
                .headers
                .get(&IDEMPOTENCY_KEY_HEADER)
                .and_then(|value| value.to_str().ok())
                .expect("idempotency key must be present")
                .to_string();
            let mut committed_key = self.committed_key.lock().expect("commit lock");
            if committed_key.as_ref() == Some(&key) {
                return ok_with_etag("\"v1\"");
            }
            assert!(
                committed_key.is_none(),
                "a new key must not overwrite the unresolved write"
            );
            *committed_key = Some(key);
            ok_with_etag("\"v1\"").set_delay(Duration::from_millis(200))
        }
    }

    #[tokio::test]
    async fn given_committed_save_with_lost_response_when_resolved_should_reuse_pending_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(CommitThenTimeout {
                committed_key: StdMutex::new(None),
            })
            .mount(&server)
            .await;

        let mut config = test_config(&server.uri());
        config.timeout = IggyDuration::new(Duration::from_millis(50));
        config.retry.enabled = false;
        let storage = storage_for(&config);
        storage.load().await.unwrap();
        assert!(matches!(
            storage.save(ConnectorState(vec![1, 2, 3])).await,
            Err(Error::TransientState(_))
        ));
        storage
            .resolve_pending()
            .await
            .expect("pending resolution should replay the committed outcome");

        let puts = requests_of(&server, Method::PUT).await;
        assert_eq!(puts.len(), 2);
        assert_eq!(
            puts[0].headers.get(&IDEMPOTENCY_KEY_HEADER),
            puts[1].headers.get(&IDEMPOTENCY_KEY_HEADER),
            "the unresolved logical save must retain its idempotency key across calls"
        );
    }

    #[tokio::test]
    async fn given_two_logical_saves_should_mint_fresh_idempotency_keys() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ok_with_etag("\"v1\""))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        storage.save(ConnectorState(vec![1])).await.unwrap();
        storage.save(ConnectorState(vec![2])).await.unwrap();

        let puts = requests_of(&server, Method::PUT).await;
        assert_eq!(puts.len(), 2);
        assert_ne!(
            puts[0].headers.get(&IDEMPOTENCY_KEY_HEADER),
            puts[1].headers.get(&IDEMPOTENCY_KEY_HEADER),
            "a new state value must get a new idempotency key"
        );
    }

    #[tokio::test]
    async fn given_persistent_save_errors_should_be_transient_and_not_latch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let storage = storage(&server);
        storage.load().await.unwrap();
        assert!(matches!(
            storage.save(ConnectorState(vec![1])).await,
            Err(Error::TransientState(_))
        ));
        let puts_after_first = requests_of(&server, Method::PUT).await.len();
        assert!(
            matches!(
                storage.save(ConnectorState(vec![1])).await,
                Err(Error::TransientState(_))
            ),
            "retrying the same unresolved state must not latch"
        );
        assert!(
            requests_of(&server, Method::PUT).await.len() > puts_after_first,
            "the next save must reach the network after a transient failure"
        );
    }

    #[tokio::test]
    async fn given_unloaded_provider_when_saved_should_load_before_writing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .and(header(IF_NONE_MATCH.as_str(), "*"))
            .respond_with(ok_with_etag("\"v1\""))
            .mount(&server)
            .await;

        storage(&server)
            .save(ConnectorState(vec![1]))
            .await
            .expect("save before load should resolve the version first");

        let all = server.received_requests().await.unwrap();
        assert_eq!(
            all[0].method,
            Method::GET,
            "version must be resolved before writing"
        );
        assert_eq!(all[1].method, Method::PUT);
    }

    /// Conditional PUT responder: enforces `If-None-Match: *` before the first
    /// write and `If-Match: "v{n}"` afterwards, answering 412 on any mismatch.
    struct VersionedPut(Arc<AtomicU64>);

    impl Respond for VersionedPut {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let current = self.0.load(Ordering::SeqCst);
            let matches = if current == 0 {
                request
                    .headers
                    .get(IF_NONE_MATCH)
                    .map(|value| value.as_bytes() == b"*")
                    .unwrap_or(false)
            } else {
                request
                    .headers
                    .get(IF_MATCH)
                    .and_then(|value| value.to_str().ok())
                    .map(|value| value == format!("\"v{current}\""))
                    .unwrap_or(false)
            };
            if !matches {
                return ResponseTemplate::new(412);
            }
            let next = current + 1;
            self.0.store(next, Ordering::SeqCst);
            ResponseTemplate::new(200).insert_header(ETAG, format!("\"v{next}\"").as_str())
        }
    }

    #[tokio::test]
    async fn given_concurrent_saves_when_completed_should_serialize_and_chain_etags() {
        let server = MockServer::start().await;
        let version = Arc::new(AtomicU64::new(0));
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(RESOURCE_PATH))
            .respond_with(VersionedPut(version.clone()))
            .mount(&server)
            .await;

        let storage = Arc::new(storage(&server));
        storage.load().await.unwrap();
        let mut handles = Vec::new();
        for value in 0u8..8 {
            let storage = storage.clone();
            handles.push(tokio::spawn(async move {
                storage.save(ConnectorState(vec![value])).await
            }));
        }
        for handle in handles {
            handle
                .await
                .unwrap()
                .expect("serialized saves must all commit against the fresh ETag");
        }
        assert_eq!(version.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn given_configured_headers_when_requesting_should_attach_them() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(RESOURCE_PATH))
            .and(header("authorization", "Bearer secret-token"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let mut config = test_config(&server.uri());
        config.request_headers.insert(
            "authorization".to_string(),
            SecretString::from("Bearer secret-token"),
        );
        assert!(
            storage_for(&config).load().await.unwrap().is_none(),
            "the mock only matches when the configured header is attached"
        );
    }

    #[tokio::test]
    async fn given_post_load_method_when_loaded_should_use_post() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(RESOURCE_PATH))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let mut config = test_config(&server.uri());
        config.load_method = HttpStateMethod::Post;
        assert!(storage_for(&config).load().await.unwrap().is_none());
        assert_eq!(requests_of(&server, Method::POST).await.len(), 1);
    }

    #[tokio::test]
    async fn given_configured_save_method_when_saved_should_use_it() {
        for (configured, expected) in [
            (HttpStateMethod::Put, Method::PUT),
            (HttpStateMethod::Post, Method::POST),
            (HttpStateMethod::Patch, Method::PATCH),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(RESOURCE_PATH))
                .respond_with(ResponseTemplate::new(404))
                .mount(&server)
                .await;
            Mock::given(method(expected.clone()))
                .and(path(RESOURCE_PATH))
                .and(header(IF_NONE_MATCH.as_str(), "*"))
                .respond_with(ok_with_etag("\"v1\""))
                .mount(&server)
                .await;

            let mut config = test_config(&server.uri());
            config.save_method = configured;
            let storage = storage_for(&config);
            storage.load().await.unwrap();
            storage.save(ConnectorState(vec![1])).await.unwrap();
            assert_eq!(requests_of(&server, expected).await.len(), 1);
        }
    }

    #[tokio::test]
    async fn given_query_string_in_base_url_when_requesting_should_preserve_it() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/state/source_test"))
            .and(query_param("tenant", "acme"))
            .and(query_param("region", "eu"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let config = test_config(&format!("{}/state?tenant=acme&region=eu", server.uri()));
        assert!(storage_for(&config).load().await.unwrap().is_none());
    }

    #[test]
    fn given_secret_headers_when_formatted_should_not_leak_values() {
        let mut config = test_config("http://localhost:1/state?token=query-secret");
        config.request_headers.insert(
            "authorization".to_string(),
            SecretString::from("Bearer secret-token"),
        );
        let debug_output = format!("{config:?}");
        let display_output = format!("{config}");
        assert!(!debug_output.contains("secret-token"), "{debug_output}");
        assert!(!display_output.contains("secret-token"), "{display_output}");
        assert!(!debug_output.contains("query-secret"), "{debug_output}");
        assert!(!display_output.contains("query-secret"), "{display_output}");
        assert!(
            display_output.contains("authorization"),
            "header names stay visible for operators: {display_output}"
        );

        let factory = HttpStateFactory::new(&config).expect("factory should build");
        let factory_debug = format!("{factory:?}");
        let storage_debug = format!("{:?}", factory.storage_for("test").unwrap());
        assert!(!factory_debug.contains("query-secret"), "{factory_debug}");
        assert!(!storage_debug.contains("query-secret"), "{storage_debug}");
    }

    #[test]
    fn given_empty_url_when_factory_built_should_fail() {
        let result = HttpStateFactory::new(&test_config(" "));
        assert!(
            matches!(result, Err(RuntimeError::InvalidConfiguration(ref message)) if message.contains("state.http.url")),
            "expected InvalidConfiguration about state.http.url"
        );
    }

    #[test]
    fn given_non_http_scheme_when_factory_built_should_fail() {
        assert!(HttpStateFactory::new(&test_config("ftp://example.com/state")).is_err());
        assert!(HttpStateFactory::new(&test_config("not a url")).is_err());
    }

    #[test]
    fn given_userinfo_or_fragment_in_url_when_factory_built_should_fail() {
        assert!(
            HttpStateFactory::new(&test_config("https://user:secret@example.com/state")).is_err()
        );
        assert!(HttpStateFactory::new(&test_config("https://example.com/state#fragment")).is_err());
    }

    #[test]
    fn given_invalid_header_name_when_factory_built_should_fail() {
        let mut config = test_config("http://localhost:1/state");
        config
            .request_headers
            .insert("bad header".to_string(), SecretString::from("value"));
        assert!(HttpStateFactory::new(&config).is_err());
    }

    #[test]
    fn given_runtime_managed_header_when_factory_built_should_fail() {
        for name in [
            IF_MATCH.as_str(),
            IF_NONE_MATCH.as_str(),
            IDEMPOTENCY_KEY_HEADER.as_str(),
            CONTENT_TYPE.as_str(),
        ] {
            let mut config = test_config("http://localhost:1/state");
            config
                .request_headers
                .insert(name.to_string(), SecretString::from("value"));
            assert!(
                HttpStateFactory::new(&config).is_err(),
                "{name} is owned by the state protocol"
            );
        }
    }

    #[test]
    fn given_unsupported_method_for_operation_when_factory_built_should_fail() {
        let mut config = test_config("http://localhost:1/state");
        config.load_method = HttpStateMethod::Put;
        assert!(HttpStateFactory::new(&config).is_err());

        config.load_method = HttpStateMethod::Get;
        config.save_method = HttpStateMethod::Get;
        assert!(HttpStateFactory::new(&config).is_err());
    }

    #[test]
    fn given_connector_key_when_storage_built_should_percent_encode_the_segment() {
        let factory = HttpStateFactory::new(&test_config("http://localhost:1/state")).unwrap();
        let StateStorage::Http(provider) = factory.storage_for("a b/c").unwrap() else {
            panic!("http factory must build http storage");
        };
        assert_eq!(provider.resource_url.path(), "/state/source_a%20b%2Fc");
    }

    #[test]
    fn given_trailing_slash_base_url_when_storage_built_should_not_double_slash() {
        let factory = HttpStateFactory::new(&test_config("http://localhost:1/state/")).unwrap();
        let StateStorage::Http(provider) = factory.storage_for("test").unwrap() else {
            panic!("http factory must build http storage");
        };
        assert_eq!(provider.resource_url.path(), "/state/source_test");
    }

    #[test]
    fn given_backoff_policy_when_delays_computed_should_grow_and_cap() {
        let policy = RetryPolicy {
            enabled: true,
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(350),
            backoff_multiplier: 2,
        };
        assert_eq!(policy.delay_for(0, None), Duration::from_millis(100));
        assert_eq!(policy.delay_for(1, None), Duration::from_millis(200));
        assert_eq!(policy.delay_for(2, None), Duration::from_millis(350));
        assert_eq!(policy.delay_for(30, None), Duration::from_millis(350));
    }

    #[test]
    fn given_retry_after_when_delay_computed_should_honor_but_cap_it() {
        let policy = RetryPolicy {
            enabled: true,
            max_attempts: 5,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_secs(2),
            backoff_multiplier: 2,
        };
        assert_eq!(
            policy.delay_for(0, Some(Duration::from_secs(1))),
            Duration::from_secs(1)
        );
        assert_eq!(
            policy.delay_for(0, Some(Duration::from_secs(3600))),
            Duration::from_secs(2),
            "a hostile Retry-After must not stall the loop past max_backoff"
        );
    }
}
