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

use crate::diagnostic_events::diagnostic_event_channel;
use crate::driver::{DeferredLease, MAX_DEFERRED_REQUESTS, fetch_error_is_uncertain};
use crate::http::http_transport::HttpTransport;
use crate::prelude::{Client, HttpClientConfig, IggyError, NonZeroIggyDuration};
use crate::vsr::LIFECYCLE_RETRY_MAX_INTERVAL;
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::{
    ConnectionString, ConnectionStringUtils, ConsumerGroupClientState, DeferredPollOptions,
    DiagnosticEvent, HttpConnectionStringOptions, HttpMethod, IdentityInfo, PollMessages,
    PolledMessages, PollingKind, TransportProtocol, validate_api_url,
};
use reqwest::{Method, Response, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, RequestBuilder};
use reqwest_retry::{
    DefaultRetryableStrategy, RetryTransientMiddleware, Retryable, RetryableStrategy,
    policies::ExponentialBackoff,
};
use reqwest_tracing::{SpanBackendWithUrl, TracingMiddleware};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

/// The first pause before a request refused with [`IggyError::LifecycleBusy`] is sent again.
const LIFECYCLE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
/// No retry of a [`IggyError::LifecycleBusy`] refusal starts past this budget, which matches
/// the time the binary transports wait for one reply.
const LIFECYCLE_RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// The query key of [`iggy_common::PollMessages::auto_commit`].
const AUTO_COMMIT_QUERY_KEY: &str = "auto_commit";

/// The first local operation id. Binary request ids also start at 1, because request 0
/// registers the session, so no transport reports operation 0.
const FIRST_OPERATION_ID: u64 = 1;

const PUBLIC_PATHS: &[&str] = &[
    "/",
    "/ping",
    "/users/login",
    "/users/refresh-token",
    "/personal-access-tokens/login",
];

/// HTTP client for interacting with the Iggy API.
/// It requires a valid API URL.
#[derive(Debug)]
pub struct HttpClient {
    /// The URL of the Iggy API.
    pub api_url: Url,
    pub(crate) heartbeat_interval: NonZeroIggyDuration,
    client: reqwest::Client,
    retry_policy: ExponentialBackoff,
    deferred_leases: tokio::sync::Semaphore,
    access_token: IggyRwLock<String>,
    /// The contexts that fence sends to an explicit partition, read from the topic details.
    pub(super) send_contexts: ConsumerGroupClientState,
    /// The partitions blocked by a poll with `auto_commit` whose outcome is unknown, by the
    /// local operation id that `abandon_operation` takes. A name and a numeric id can spell
    /// the same stream or topic, and HTTP has no id independent of the spelling, so a block
    /// covers its partition id in every stream and topic.
    progress_blocks: Mutex<BTreeMap<u64, u32>>,
    next_operation_id: AtomicU64,
    /// HTTP keeps no connection, so no event is ever published here. The
    /// channel exists only because the `Client` trait requires
    /// `subscribe_events`.
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
}

#[async_trait]
impl Client for HttpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        HttpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        HttpClient::disconnect(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        Ok(())
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }

    async fn abandon_operation(&self, op: u64) -> Result<(), IggyError> {
        self.progress_blocks()
            .remove(&op)
            .map(drop)
            .ok_or(IggyError::OperationNotRetained(op))
    }
}

unsafe impl Send for HttpClient {}
unsafe impl Sync for HttpClient {}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::create(Arc::new(HttpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl HttpTransport for HttpClient {
    /// Get full URL for the provided path.
    fn get_url(&self, path: &str) -> Result<Url, IggyError> {
        self.api_url
            .join(path)
            .map_err(|_| IggyError::CannotParseUrl)
    }

    /// Invoke HTTP GET request to the Iggy API.
    async fn get(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::GET, url, false, |request| request)
            .await
    }

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    async fn get_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::GET, url, false, |request| request.query(query))
            .await
    }

    /// Invoke HTTP POST request to the Iggy API.
    async fn post<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::POST, url, true, |request| request.json(payload))
            .await
    }

    /// Invoke HTTP PUT request to the Iggy API.
    async fn put<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::PUT, url, true, |request| request.json(payload))
            .await
    }

    /// Invoke HTTP DELETE request to the Iggy API.
    async fn delete(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::DELETE, url, true, |request| request)
            .await
    }

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    async fn delete_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::DELETE, url, true, |request| request.query(query))
            .await
    }

    async fn send_http_request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Bytes>,
    ) -> Result<Bytes, IggyError> {
        let method = Method::from_bytes(<&str>::from(method).as_bytes())
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        let url = self.get_url(path)?;
        // A raw poll asks for `auto_commit` in its query, so the query tells whether a GET writes.
        let may_commit = method != Method::GET
            || url.query_pairs().any(|(key, value)| {
                key == AUTO_COMMIT_QUERY_KEY && value.parse::<bool>() == Ok(true)
            });
        let response = self
            .execute(method, url, may_commit, |request| match &body {
                Some(body) => request.body(body.clone()),
                None => request,
            })
            .await?;
        response
            .bytes()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)
    }

    /// Returns true if the client is authenticated.
    async fn is_authenticated(&self) -> bool {
        let token = self.access_token.read().await;
        !token.is_empty()
    }

    /// Set the access token.
    async fn set_access_token(&self, token: Option<String>) {
        let mut current_token = self.access_token.write().await;
        if let Some(token) = token {
            *current_token = token;
        } else {
            *current_token = "".to_string();
        }
    }

    /// Set the access token from the provided identity.
    async fn set_token_from_identity(&self, identity: &IdentityInfo) -> Result<(), IggyError> {
        if identity.access_token.is_none() {
            return Err(IggyError::JwtMissing);
        }

        let access_token = identity.access_token.as_ref().unwrap();
        self.set_access_token(Some(access_token.token.clone()))
            .await;
        Ok(())
    }
}

impl HttpClient {
    /// Sends a deferred poll. Only a refusal that admitted nothing is sent again, within the
    /// request timeout. A progress poll, one with `auto_commit` or from
    /// [`PollingKind::Next`], fails with [`IggyError::PartitionLaneBlocked`] while its partition
    /// is blocked.
    ///
    /// The server stores the offset of a poll with `auto_commit` before it answers, and it can
    /// still store it after the client gives up. So such a poll that may have reached the
    /// server fails with [`IggyError::FetchProgressUncertain`] unless its answer proves the
    /// outcome, and a caller that drops it after the send gets the same block: the partition
    /// stays blocked until `abandon_operation`. A failure after the connect proves the outcome
    /// only as a 503 or a 404, or as another 4xx whose error id [`fetch_error_is_uncertain`]
    /// takes as proof, as on binary.
    pub(super) async fn get_deferred_poll(
        &self,
        path: &str,
        poll: &PollMessages,
        options: DeferredPollOptions,
    ) -> Result<PolledMessages, IggyError> {
        // Bound JSON expansion and error bodies under the same request budget.
        const MAX_JSON_EXPANSION: usize = 16;
        options.validate(poll.count)?;
        let partition_id = poll.partition_id.unwrap_or(DEFAULT_PARTITION_ID);
        let progress = poll.auto_commit || poll.strategy.kind == PollingKind::Next;
        // Before the lease wait, so a client full of waiting polls cannot hide the block
        // behind `TransientNotAccepted`.
        if progress {
            self.fail_if_progress_blocked(partition_id)?;
        }
        let limit = (options.max_bytes as usize).saturating_mul(MAX_JSON_EXPANSION);
        let started = Instant::now();
        let deadline = started + options.request_timeout.get_duration();
        let _permit = DeferredLease::acquire(&self.deferred_leases, deadline).await?;
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let mut retry_interval = LIFECYCLE_RETRY_INTERVAL;
        loop {
            // Another call can block the partition during the lease wait or a retry pause.
            if progress {
                self.fail_if_progress_blocked(partition_id)?;
            }
            let request = {
                let token = self.access_token.read().await;
                let remaining = options.remaining(started.elapsed())?;
                // Deferred auto-commits cannot replay an ambiguous transport failure.
                self.client
                    .get(url.clone())
                    .bearer_auth(token.as_str())
                    .query(poll)
                    .query(&[
                        ("wait_us", remaining.max_wait.as_micros()),
                        ("min_count", u64::from(remaining.min_count)),
                        ("max_bytes", u64::from(remaining.max_bytes)),
                        ("request_timeout_us", remaining.request_timeout.as_micros()),
                    ])
                    .timeout(remaining.request_timeout.get_duration())
            };
            // Armed before the send, because HTTP cannot tell when the request leaves.
            let attempt = self.poll_attempt(partition_id, poll.auto_commit);
            let error = match Self::deferred_reply(request, poll, limit, deadline).await {
                Ok(messages) => {
                    attempt.settle();
                    return Ok(messages);
                }
                Err(PollFailure::Refused(error)) => {
                    attempt.settle();
                    error
                }
                Err(PollFailure::Unknown(error)) => return Err(attempt.unknown(error)),
            };
            if !matches!(
                error,
                IggyError::TransientNotAccepted | IggyError::DeferredPollCapacityExceeded
            ) {
                return Err(error);
            }
            if Instant::now() + retry_interval >= deadline {
                return Err(error);
            }
            sleep(retry_interval).await;
            retry_interval = (retry_interval * 2).min(LIFECYCLE_RETRY_MAX_INTERVAL);
        }
    }

    /// Sends one attempt of a deferred poll and decodes its answer, whose body may take up to
    /// `limit` bytes. A decoded batch records `poll.strategy` as its request start.
    async fn deferred_reply(
        request: reqwest::RequestBuilder,
        poll: &PollMessages,
        limit: usize,
        deadline: Instant,
    ) -> Result<PolledMessages, PollFailure> {
        let sent = request
            .send()
            .await
            .map_err(reqwest_middleware::Error::Reqwest);
        let uncertain = may_have_committed(&sent);
        let mut response = sent.map_err(|_| {
            if uncertain {
                PollFailure::Unknown(IggyError::TransientNotCommitted)
            } else {
                PollFailure::Refused(IggyError::InvalidHttpRequest)
            }
        })?;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(PollFailure::Unknown(IggyError::InvalidSizeBytes));
        }
        let mut body = Vec::new();
        while let Some(chunk) = timeout_at(deadline, response.chunk())
            .await
            .map_err(|_| PollFailure::Unknown(IggyError::TransientNotCommitted))?
            .map_err(|_| PollFailure::Unknown(IggyError::TransientNotCommitted))?
        {
            if chunk.len() > limit.saturating_sub(body.len()) {
                return Err(PollFailure::Unknown(IggyError::InvalidSizeBytes));
            }
            body.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let reason = String::from_utf8_lossy(&body).into_owned();
            // A 503 refuses a poll before the server takes it, and the server sends a 404 only
            // before the read or for a partition that its owner does not have. The server
            // checks a poll again after the read, so another 4xx proves the outcome only with
            // an error that binary also takes as proof.
            let proved = matches!(
                status,
                StatusCode::SERVICE_UNAVAILABLE | StatusCode::NOT_FOUND
            ) || (status.is_client_error()
                && error_id(&reason)
                    .is_some_and(|id| !fetch_error_is_uncertain(&IggyError::from_code(id))));
            let error = Self::response_error(status, reason);
            return Err(if proved {
                PollFailure::Refused(error)
            } else {
                PollFailure::Unknown(error)
            });
        }
        let mut messages: PolledMessages = serde_json::from_slice(&body)
            .map_err(|_| PollFailure::Unknown(IggyError::InvalidJsonResponse))?;
        if messages.count > poll.count || messages.messages.len() != messages.count as usize {
            return Err(PollFailure::Unknown(IggyError::InvalidMessagesCount));
        }
        messages.set_request_start(&poll.strategy);
        Ok(messages)
    }

    /// Fails with [`IggyError::PartitionLaneBlocked`] while a poll with `auto_commit` of
    /// `partition_id` has an unknown outcome. Polls that store the offset or start at it, and
    /// offset stores and deletes, check it.
    pub(super) fn fail_if_progress_blocked(&self, partition_id: u32) -> Result<(), IggyError> {
        let blocking = self
            .progress_blocks()
            .iter()
            .find_map(|(&operation_id, &blocked)| {
                (blocked == partition_id).then_some(operation_id)
            });
        blocking.map_or(Ok(()), |operation_id| {
            Err(IggyError::PartitionLaneBlocked(partition_id, operation_id))
        })
    }

    /// Whether the poll `op` still blocks its partition.
    pub(crate) fn retains_operation(&self, op: u64) -> bool {
        self.progress_blocks().contains_key(&op)
    }

    fn poll_attempt(&self, partition_id: u32, auto_commit: bool) -> PollAttempt<'_> {
        PollAttempt {
            client: self,
            partition_id,
            commit: auto_commit.then(|| self.next_operation_id.fetch_add(1, Ordering::Relaxed)),
        }
    }

    fn progress_blocks(&self) -> MutexGuard<'_, BTreeMap<u64, u32>> {
        self.progress_blocks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Create a new HTTP client for interacting with the Iggy API using the provided API URL.
    pub fn new(api_url: &str) -> Result<Self, IggyError> {
        Self::create(Arc::new(HttpClientConfig {
            api_url: api_url.to_string(),
            ..Default::default()
        }))
    }

    /// Create a new HTTP client for interacting with the Iggy API using the provided configuration.
    pub fn create(config: Arc<HttpClientConfig>) -> Result<Self, IggyError> {
        validate_api_url(&config.api_url)?;
        let api_url = Url::parse(&config.api_url).map_err(|_| IggyError::CannotParseUrl)?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.retries);

        let access_token = config.jwt.clone().unwrap_or_default();

        Ok(Self {
            api_url,
            client: reqwest::Client::new(),
            retry_policy,
            deferred_leases: tokio::sync::Semaphore::new(MAX_DEFERRED_REQUESTS),
            heartbeat_interval: config.heartbeat_interval,
            access_token: IggyRwLock::new(access_token),
            send_contexts: ConsumerGroupClientState::new(),
            progress_blocks: Mutex::default(),
            next_operation_id: AtomicU64::new(FIRST_OPERATION_ID),
            events: diagnostic_event_channel(),
        })
    }

    /// Create a new HttpClient from a connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Http {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(Arc::new(
            ConnectionString::<HttpConnectionStringOptions>::from_str(connection_string)?.into(),
        ))
    }

    /// Present the stored access token to `POST /users/refresh-token`, then
    /// swap it for the reissued one. Returns the new identity so the caller can
    /// schedule the next refresh from `IdentityInfo.access_token.expiry`
    /// (unix seconds). Scheduling is the caller's job: no auto-refresh or
    /// retry-on-401 happens anywhere in the request path.
    ///
    /// Server semantics differ and the caller must account for it:
    /// - Legacy server: one-shot. The presented token is revoked as it is
    ///   consumed, so a concurrent in-flight request still carrying the old
    ///   token may fail with 401.
    /// - the server: stateless. The old token stays valid until its natural
    ///   expiry; refreshing never revokes it.
    pub async fn refresh_access_token(&self) -> Result<IdentityInfo, IggyError> {
        // Release the read guard before `set_token_from_identity` takes the
        // write guard on the same lock, otherwise the reissue self-deadlocks.
        let current_token = {
            let token = self.access_token.read().await;
            if token.is_empty() {
                return Err(IggyError::AccessTokenMissing);
            }
            token.to_owned()
        };

        let response = self
            .post(
                "/users/refresh-token",
                &RefreshToken {
                    token: current_token,
                },
            )
            .await?;
        let identity_info: IdentityInfo = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;

        self.set_token_from_identity(&identity_info).await?;
        Ok(identity_info)
    }

    /// Sends one request. Each [`IggyError::LifecycleBusy`] refusal commits, so the request is
    /// sent again as a new one after a pause that doubles up to [`LIFECYCLE_RETRY_MAX_INTERVAL`],
    /// until a retry would start past [`LIFECYCLE_RETRY_DEADLINE`]. A write whose outcome is
    /// unknown is never sent again and fails with [`IggyError::TransientNotCommitted`]. The
    /// caller marks a write with `may_commit`, because a poll with `auto_commit` is a GET that
    /// writes.
    async fn execute<F>(
        &self,
        method: Method,
        url: Url,
        may_commit: bool,
        decorate: F,
    ) -> Result<Response, IggyError>
    where
        F: Fn(RequestBuilder) -> RequestBuilder + Send + Sync,
    {
        let outcome_unknown = Arc::new(AtomicBool::new(false));
        let client = self.retrying_client(may_commit, &outcome_unknown);
        let deadline = Instant::now() + LIFECYCLE_RETRY_DEADLINE;
        let mut pause = LIFECYCLE_RETRY_INTERVAL;
        loop {
            // The token is copied into the request, so no pause or slow reply holds its lock.
            let request = {
                let token = self.access_token.read().await;
                decorate(
                    client
                        .request(method.clone(), url.clone())
                        .bearer_auth(token.as_str()),
                )
            };
            let response = request.send().await;
            // The middleware stops at such an attempt, so it was the last one.
            if outcome_unknown.load(Ordering::Relaxed) {
                return Err(IggyError::TransientNotCommitted);
            }
            let response = response.map_err(|_| IggyError::InvalidHttpRequest)?;
            match Self::handle_response(response).await {
                Ok(response) => return Ok(response),
                Err(IggyError::LifecycleBusy) if Instant::now() + pause < deadline => {
                    sleep(pause).await;
                    pause = (pause * 2).min(LIFECYCLE_RETRY_MAX_INTERVAL);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// The retry middleware resends transient failures. It stops at an attempt of a write whose
    /// outcome is unknown and reports it to `outcome_unknown`.
    fn retrying_client(
        &self,
        may_commit: bool,
        outcome_unknown: &Arc<AtomicBool>,
    ) -> ClientWithMiddleware {
        ClientBuilder::new(self.client.clone())
            .with(TracingMiddleware::<SpanBackendWithUrl>::new())
            .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                self.retry_policy,
                SafeResends {
                    may_commit,
                    outcome_unknown: Arc::clone(outcome_unknown),
                },
            ))
            .build()
    }

    async fn handle_response(response: Response) -> Result<Response, IggyError> {
        let status = response.status();
        match status.is_success() {
            true => Ok(response),
            false => {
                let reason = response.text().await.unwrap_or("error".to_string());
                Err(Self::response_error(status, reason))
            }
        }
    }

    async fn fail_if_not_authenticated(&self, path: &str) -> Result<(), IggyError> {
        if PUBLIC_PATHS.contains(&path) {
            return Ok(());
        }
        if !self.is_authenticated().await {
            return Err(IggyError::Unauthenticated);
        }
        Ok(())
    }

    async fn connect(&self) -> Result<(), IggyError> {
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        Ok(())
    }

    fn response_error(status: StatusCode, reason: String) -> IggyError {
        match status {
            StatusCode::UNAUTHORIZED => IggyError::Unauthenticated,
            StatusCode::FORBIDDEN => IggyError::Unauthorized,
            StatusCode::NOT_FOUND => IggyError::ResourceNotFound(reason),
            _ => typed_refusal(&reason)
                .unwrap_or_else(|| IggyError::HttpResponseError(status.as_u16(), reason)),
        }
    }
}

#[derive(Debug, Serialize)]
struct RefreshToken {
    token: String,
}

/// Why an attempt of a deferred poll returned no messages.
enum PollFailure {
    /// Nothing was sent, or the answer proves the outcome.
    Refused(IggyError),
    /// The attempt may have reached the server, and no answer proves what it did.
    Unknown(IggyError),
}

/// One attempt of a deferred poll. An attempt with `auto_commit` that its caller drops before
/// it settles blocks its partition, because the server can still store the offset.
struct PollAttempt<'a> {
    client: &'a HttpClient,
    partition_id: u32,
    /// The local operation id of an attempt with `auto_commit`.
    commit: Option<u64>,
}

impl PollAttempt<'_> {
    /// The answer proved the outcome, so the partition stays open.
    fn settle(mut self) {
        self.commit = None;
    }

    /// The error of an attempt with an unknown outcome. With `auto_commit`, it is
    /// [`IggyError::FetchProgressUncertain`], and the partition stays blocked.
    fn unknown(self, error: IggyError) -> IggyError {
        self.commit.map_or(error, IggyError::FetchProgressUncertain)
    }
}

impl Drop for PollAttempt<'_> {
    fn drop(&mut self) {
        if let Some(operation_id) = self.commit {
            self.client
                .progress_blocks()
                .insert(operation_id, self.partition_id);
        }
    }
}

/// Resends what the default strategy calls transient, but never a write that may have committed.
struct SafeResends {
    may_commit: bool,
    outcome_unknown: Arc<AtomicBool>,
}

impl RetryableStrategy for SafeResends {
    fn handle(&self, result: &Result<Response, reqwest_middleware::Error>) -> Option<Retryable> {
        if self.may_commit && may_have_committed(result) {
            self.outcome_unknown.store(true, Ordering::Relaxed);
            return Some(Retryable::Fatal);
        }
        DefaultRetryableStrategy.handle(result)
    }
}

/// HTTP has no request dedup, so a resend is a new request that can apply a write twice. A 503
/// refuses a request before the server takes it, and a request that was never built or never
/// connected was never sent.
fn may_have_committed(result: &Result<Response, reqwest_middleware::Error>) -> bool {
    match result {
        Ok(response) => {
            response.status().is_server_error()
                && response.status() != StatusCode::SERVICE_UNAVAILABLE
        }
        Err(reqwest_middleware::Error::Reqwest(error)) => {
            !error.is_builder() && !error.is_connect()
        }
        Err(reqwest_middleware::Error::Middleware(_)) => false,
    }
}

/// Decodes the refusals the client acts on: a producer never repeats `RequestTooOld`, a send
/// refreshes its context once on `HistoryUnavailable`, `LifecycleBusy` is retried,
/// non-admission and capacity refusals permit a retry without risking duplicate progress, and a
/// poll reads a first message larger than `max_bytes` alone after `InvalidSizeBytes`.
fn typed_refusal(body: &str) -> Option<IggyError> {
    let id = error_id(body)?;
    [
        IggyError::RequestTooOld,
        IggyError::HistoryUnavailable,
        IggyError::LifecycleBusy,
        IggyError::TransientNotAccepted,
        IggyError::DeferredPollCapacityExceeded,
        IggyError::InvalidSizeBytes,
    ]
    .into_iter()
    .find(|error| error.as_code() == id)
}

/// The id of the error that the server rendered into an error body.
fn error_id(body: &str) -> Option<u32> {
    #[derive(Deserialize)]
    struct ErrorId {
        id: u32,
    }
    serde_json::from_str::<ErrorId>(body)
        .ok()
        .map(|error| error.id)
}

/// Unit tests for HttpClient.
/// TODO: Add complete unit tests for HttpClient.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::scripted_server::{Recorded, Reply, ScriptedServer};
    use crate::prelude::{Identifier, StreamClient};
    use std::iter;

    /// The number of `LifecycleBusy` refusals scripted for a request retried until the deadline,
    /// more than the retries that fit before it.
    const REFUSALS_PAST_THE_DEADLINE: usize = 40;

    fn pauses(requests: &[Recorded]) -> Vec<Duration> {
        requests
            .windows(2)
            .map(|pair| pair[1].at - pair[0].at)
            .collect()
    }

    fn stream_id() -> Identifier {
        Identifier::numeric(1).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn given_lifecycle_busy_when_deleting_should_retry_as_new_requests_until_it_clears() {
        let server = ScriptedServer::start(vec![
            Reply::error(400, &IggyError::LifecycleBusy),
            Reply::error(400, &IggyError::LifecycleBusy),
            Reply::empty(200),
        ])
        .await;

        server.client().delete_stream(&stream_id()).await.unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            pauses(&requests),
            [LIFECYCLE_RETRY_INTERVAL, LIFECYCLE_RETRY_INTERVAL * 2]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_lifecycle_busy_until_the_deadline_when_deleting_should_return_it() {
        let server = ScriptedServer::start(
            iter::repeat_with(|| Reply::error(400, &IggyError::LifecycleBusy))
                .take(REFUSALS_PAST_THE_DEADLINE)
                .collect(),
        )
        .await;

        let busy = server.client().delete_stream(&stream_id()).await;

        assert!(matches!(busy, Err(IggyError::LifecycleBusy)), "{busy:?}");
        let requests = server.requests();
        let pauses = pauses(&requests);
        let doubling = iter::successors(Some(LIFECYCLE_RETRY_INTERVAL), |pause| {
            Some((*pause * 2).min(LIFECYCLE_RETRY_MAX_INTERVAL))
        });
        assert_eq!(
            pauses,
            doubling.take(pauses.len()).collect::<Vec<_>>(),
            "every pause doubles up to the cap"
        );
        let last = requests.last().unwrap().at;
        assert!(last < LIFECYCLE_RETRY_DEADLINE, "{last:?}");
        assert!(
            last + LIFECYCLE_RETRY_MAX_INTERVAL >= LIFECYCLE_RETRY_DEADLINE,
            "another retry fit before the deadline: {last:?}"
        );
    }

    /// A resend is a new request, so a write that may have committed is never sent again.
    #[tokio::test(start_paused = true)]
    async fn given_write_when_it_may_have_committed_should_report_not_committed_without_resend() {
        for unknown in [
            Reply::empty(500),
            Reply::empty(502),
            Reply::error(504, &IggyError::TransientNotCommitted),
            Reply::hang_up(),
        ] {
            let server = ScriptedServer::start(vec![unknown, Reply::empty(200)]).await;

            let deleted = server.client().delete_stream(&stream_id()).await;

            assert!(
                matches!(deleted, Err(IggyError::TransientNotCommitted)),
                "{deleted:?}"
            );
            assert_eq!(server.requests().len(), 1);
        }
    }

    /// A 503 refuses a write before the server takes it, so the refusal of its resend is
    /// definitive.
    #[tokio::test(start_paused = true)]
    async fn given_write_refused_before_taken_when_resend_is_refused_should_report_the_refusal() {
        let server = ScriptedServer::start(vec![
            Reply::error(503, &IggyError::TransientNotAccepted),
            Reply::error(403, &IggyError::Unauthorized),
        ])
        .await;

        let refused = server.client().delete_stream(&stream_id()).await;

        assert!(
            matches!(refused, Err(IggyError::Unauthorized)),
            "{refused:?}"
        );
        assert_eq!(server.requests().len(), 2);
    }

    /// A read commits nothing, so a timed out read is sent again.
    #[tokio::test(start_paused = true)]
    async fn given_read_when_it_times_out_should_resend_it() {
        let server = ScriptedServer::start(vec![Reply::empty(504), Reply::stream()]).await;

        server.client().get_stream(&stream_id()).await.unwrap();

        assert_eq!(server.requests().len(), 2);
    }

    /// A read commits nothing, so the refusal of its resend is definitive.
    #[tokio::test(start_paused = true)]
    async fn given_resend_of_a_read_when_refused_should_report_the_refusal() {
        let server = ScriptedServer::start(vec![
            Reply::empty(503),
            Reply::error(403, &IggyError::Unauthorized),
        ])
        .await;

        let refused = server.client().get_stream(&stream_id()).await;

        assert!(
            matches!(refused, Err(IggyError::Unauthorized)),
            "{refused:?}"
        );
    }

    /// A raw poll that asks for `auto_commit` is a write, so only a raw poll without it is sent
    /// again after a timeout.
    #[tokio::test(start_paused = true)]
    async fn given_raw_poll_when_it_times_out_should_resend_it_only_without_auto_commit() {
        for auto_commit in [true, false] {
            let server = ScriptedServer::start(vec![Reply::empty(504), Reply::empty(200)]).await;

            let polled = server
                .client()
                .send_http_request(
                    HttpMethod::Get,
                    &format!("/streams/1/topics/1/messages?auto_commit={auto_commit}"),
                    None,
                )
                .await;

            let requests = server.requests().len();
            if auto_commit {
                assert!(
                    matches!(polled, Err(IggyError::TransientNotCommitted)),
                    "{polled:?}"
                );
                assert_eq!(requests, 1);
            } else {
                polled.unwrap();
                assert_eq!(requests, 2);
            }
        }
    }

    #[test]
    fn typed_refusals_survive_http_decoding() {
        for error in [
            IggyError::RequestTooOld,
            IggyError::HistoryUnavailable,
            IggyError::LifecycleBusy,
            IggyError::TransientNotAccepted,
            IggyError::DeferredPollCapacityExceeded,
            IggyError::InvalidSizeBytes,
        ] {
            let body = format!(r#"{{"id":{},"reason":"unavailable"}}"#, error.as_code());
            assert_eq!(typed_refusal(&body), Some(error));
        }
        for body in ["", "unavailable", r#"{"id":500}"#, r#"{"id":"87"}"#] {
            assert_eq!(typed_refusal(body), None);
        }
    }

    use iggy_common::{Consumer, PollMessages, PollingStrategy};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn deferred_poll_retries_only_nonadmission_with_remaining_wait() {
        let (client, requests) = deferred_response_server(
            [
                IggyError::TransientNotAccepted.as_code(),
                IggyError::DeferredPollCapacityExceeded.as_code(),
                IggyError::TransientNotCommitted.as_code(),
            ]
            .into_iter()
            .map(|id| (503, format!(r#"{{"id":{id}}}"#)))
            .collect(),
        )
        .await;
        let result = client
            .get_deferred_poll(
                "messages/deferred",
                &deferred_request(),
                iggy_common::DeferredPollOptions::default(),
            )
            .await;
        assert!(
            matches!(result, Err(IggyError::HttpResponseError(503, body))
            if serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"].as_u64().unwrap() as u32
                == IggyError::TransientNotCommitted.as_code())
        );
        let waits = requests.await.unwrap();
        assert_eq!(waits.len(), 3);
        assert!(waits[0] <= 1_000_000);
        assert!(
            waits
                .windows(2)
                .all(|pair| pair[1] > 0 && pair[1] < pair[0]),
            "waits: {waits:?}"
        );
    }

    #[tokio::test]
    async fn deferred_poll_exhausted_retry_budget_does_not_send_zero_wait() {
        let (client, requests) = deferred_response_server(vec![(
            503,
            format!(r#"{{"id":{}}}"#, IggyError::TransientNotAccepted.as_code()),
        )])
        .await;
        let result = client
            .get_deferred_poll(
                "messages/deferred",
                &deferred_request(),
                iggy_common::DeferredPollOptions {
                    max_wait: 0.into(),
                    request_timeout: 50_000.into(),
                    ..Default::default()
                },
            )
            .await;
        assert!(matches!(result, Err(IggyError::TransientNotAccepted)));
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), requests)
                .await
                .unwrap()
                .unwrap()
                .len(),
            1
        );
    }

    /// A poll that stores nothing reports the bound itself. With `auto_commit`, a body it cannot
    /// read leaves the outcome unknown.
    #[tokio::test]
    async fn deferred_poll_bounds_success_and_error_bodies() {
        const MAX_BYTES: u32 = iggy_common::MIN_DEFERRED_POLL_MAX_BYTES as u32;
        for status in [200, 503] {
            let (client, requests) =
                deferred_response_server(vec![(status, " ".repeat(16 * MAX_BYTES as usize + 1))])
                    .await;
            let result = client
                .get_deferred_poll(
                    "messages/deferred",
                    &PollMessages {
                        auto_commit: false,
                        ..deferred_request()
                    },
                    iggy_common::DeferredPollOptions {
                        max_bytes: MAX_BYTES,
                        ..Default::default()
                    },
                )
                .await;
            assert!(
                matches!(result, Err(IggyError::InvalidSizeBytes)),
                "status {status}: {result:?}"
            );
            assert_eq!(requests.await.unwrap().len(), 1);
        }
    }

    fn deferred_request() -> PollMessages {
        PollMessages {
            consumer: Consumer::default(),
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: Some(0),
            strategy: PollingStrategy::default(),
            count: 1,
            auto_commit: true,
        }
    }

    async fn deferred_response_server(
        responses: Vec<(u16, String)>,
    ) -> (HttpClient, JoinHandle<Vec<u64>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = HttpClient::create(Arc::new(HttpClientConfig {
            api_url: format!("http://{}", listener.local_addr().unwrap()),
            jwt: Some("test-token".to_owned()),
            ..HttpClientConfig::default()
        }))
        .unwrap();
        let requests = tokio::spawn(async move {
            let mut waits = Vec::new();
            for (status, body) in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                let path = line.split_whitespace().nth(1).unwrap();
                let url = Url::parse(&format!("http://localhost{path}")).unwrap();
                let wait = url
                    .query_pairs()
                    .find(|(name, _)| name == "wait_us")
                    .unwrap()
                    .1
                    .parse::<u64>()
                    .unwrap();
                waits.push(wait);
                loop {
                    line.clear();
                    assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            }
            waits
        });
        (client, requests)
    }

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let http_client = HttpClient::from_connection_string(value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_with_unmatch_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_err());
    }

    #[test]
    fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_ok());

        assert_eq!(
            http_client.as_ref().unwrap().api_url.to_string(),
            format!("{protocol}://{server_address}:{port}/")
        );
        assert_eq!(
            http_client.as_ref().unwrap().heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let retries = "10";
        let heartbeat_interval = "10s";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?retries={retries}&heartbeat_interval={heartbeat_interval}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_ok());

        assert_eq!(
            http_client.as_ref().unwrap().api_url.to_string(),
            format!("{protocol}://{server_address}:{port}/")
        );
        assert_eq!(
            http_client.as_ref().unwrap().heartbeat_interval,
            NonZeroIggyDuration::from_str(heartbeat_interval).unwrap()
        );
    }

    #[test]
    fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_ok());

        assert_eq!(
            http_client.as_ref().unwrap().api_url.to_string(),
            format!("{protocol}://{server_address}:{port}/")
        );
        assert_eq!(
            http_client.as_ref().unwrap().heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_fail_create_with_invalid_api_url_even_without_builder() {
        let config = Arc::new(HttpClientConfig {
            api_url: "http://127.0.0.1:0".to_string(),
            ..Default::default()
        });

        let http_client = HttpClient::create(config);
        assert!(http_client.is_err());
    }
}
