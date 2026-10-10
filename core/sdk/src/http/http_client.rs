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

use crate::http::http_transport::HttpTransport;
use crate::prelude::{Client, HttpClientConfig, IggyError, NonZeroIggyDuration};
use crate::vsr::LIFECYCLE_RETRY_MAX_INTERVAL;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    ConnectionString, ConnectionStringUtils, ConsumerGroupClientState, DiagnosticEvent,
    HttpConnectionStringOptions, HttpMethod, IdentityInfo, TransportProtocol, validate_api_url,
};
use reqwest::{Method, Response, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, RequestBuilder};
use reqwest_retry::{
    DefaultRetryableStrategy, RetryTransientMiddleware, Retryable, RetryableStrategy,
    policies::ExponentialBackoff,
};
use reqwest_tracing::{SpanBackendWithUrl, TracingMiddleware};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{Instant, sleep};

/// The first pause before a request refused with [`IggyError::LifecycleBusy`] is sent again.
const LIFECYCLE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
/// No retry of a [`IggyError::LifecycleBusy`] refusal starts past this budget, which matches
/// the time the binary transports wait for one reply.
const LIFECYCLE_RETRY_DEADLINE: Duration = Duration::from_secs(30);

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
    access_token: IggyRwLock<String>,
    /// The contexts that fence sends to an explicit partition, read from the topic details.
    pub(super) send_contexts: ConsumerGroupClientState,
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
        self.execute(Method::GET, url, |request| request).await
    }

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    async fn get_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::GET, url, |request| request.query(query))
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
        self.execute(Method::POST, url, |request| request.json(payload))
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
        self.execute(Method::PUT, url, |request| request.json(payload))
            .await
    }

    /// Invoke HTTP DELETE request to the Iggy API.
    async fn delete(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::DELETE, url, |request| request).await
    }

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    async fn delete_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        self.execute(Method::DELETE, url, |request| request.query(query))
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
        let response = self
            .execute(method, url, |request| match &body {
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
            heartbeat_interval: config.heartbeat_interval,
            access_token: IggyRwLock::new(access_token),
            send_contexts: ConsumerGroupClientState::new(),
            events: broadcast(1000),
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
    /// until a retry would start past [`LIFECYCLE_RETRY_DEADLINE`].
    async fn execute<F>(&self, method: Method, url: Url, decorate: F) -> Result<Response, IggyError>
    where
        F: Fn(RequestBuilder) -> RequestBuilder + Send + Sync,
    {
        let uncertain_attempt = Arc::new(AtomicBool::new(false));
        let client = self.retrying_client(&uncertain_attempt);
        // Reads commit nothing, so only a write can leave an uncertain outcome behind.
        let may_commit = method != Method::GET;
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
            let response = request
                .send()
                .await
                .map_err(|_| IggyError::InvalidHttpRequest)?;
            let error = match Self::handle_response(response).await {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            let uncertain = may_commit && uncertain_attempt.load(Ordering::Relaxed);
            match settle(error, uncertain) {
                IggyError::LifecycleBusy if Instant::now() + pause < deadline => {
                    sleep(pause).await;
                    pause = (pause * 2).min(LIFECYCLE_RETRY_MAX_INTERVAL);
                }
                error => return Err(error),
            }
        }
    }

    /// The retry middleware resends transient failures. It reports each of them to
    /// `uncertain_attempt` unless the server provably never took the request.
    fn retrying_client(&self, uncertain_attempt: &Arc<AtomicBool>) -> ClientWithMiddleware {
        ClientBuilder::new(self.client.clone())
            .with(TracingMiddleware::<SpanBackendWithUrl>::new())
            .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                self.retry_policy,
                UncertainAttempts(Arc::clone(uncertain_attempt)),
            ))
            .build()
    }

    async fn handle_response(response: Response) -> Result<Response, IggyError> {
        let status = response.status();
        match status.is_success() {
            true => Ok(response),
            false => {
                let reason = response.text().await.unwrap_or("error".to_string());
                match status {
                    StatusCode::UNAUTHORIZED => Err(IggyError::Unauthenticated),
                    StatusCode::FORBIDDEN => Err(IggyError::Unauthorized),
                    StatusCode::NOT_FOUND => Err(IggyError::ResourceNotFound(reason)),
                    _ => Err(typed_refusal(&reason)
                        .unwrap_or_else(|| IggyError::HttpResponseError(status.as_u16(), reason))),
                }
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
}

#[derive(Debug, Serialize)]
struct RefreshToken {
    token: String,
}

/// Marks the request uncertain when the middleware treats an attempt as transient and the
/// attempt may have committed. A 503 can mean either, and only the body tells, so every
/// transient answer counts except an admission refusal (429) and a failed connection.
struct UncertainAttempts(Arc<AtomicBool>);

impl RetryableStrategy for UncertainAttempts {
    fn handle(&self, result: &Result<Response, reqwest_middleware::Error>) -> Option<Retryable> {
        let retryable = DefaultRetryableStrategy.handle(result);
        let never_taken = match result {
            Ok(response) => response.status() == StatusCode::TOO_MANY_REQUESTS,
            Err(reqwest_middleware::Error::Reqwest(error)) => error.is_connect(),
            Err(reqwest_middleware::Error::Middleware(_)) => false,
        };
        if retryable == Some(Retryable::Transient) && !never_taken {
            self.0.store(true, Ordering::Relaxed);
        }
        retryable
    }
}

/// HTTP has no request dedup: a resend is a new request, so its refusal cannot tell whether an
/// earlier attempt committed. Such a refusal reports an unknown outcome, which nothing refreshes
/// or repeats.
fn settle(error: IggyError, uncertain: bool) -> IggyError {
    if uncertain
        && matches!(
            error,
            IggyError::HistoryUnavailable
                | IggyError::LifecycleBusy
                | IggyError::Unauthorized
                | IggyError::Unauthenticated
        )
    {
        IggyError::TransientNotCommitted
    } else {
        error
    }
}

/// Decodes the refusals the client acts on: a producer never repeats `RequestTooOld`, a send
/// refreshes its context once on `HistoryUnavailable`, and `LifecycleBusy` is retried.
fn typed_refusal(body: &str) -> Option<IggyError> {
    #[derive(Deserialize)]
    struct ErrorId {
        id: u32,
    }
    let id = serde_json::from_str::<ErrorId>(body).ok()?.id;
    [
        IggyError::RequestTooOld,
        IggyError::HistoryUnavailable,
        IggyError::LifecycleBusy,
    ]
    .into_iter()
    .find(|error| error.as_code() == id)
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

    /// The first attempt may have committed and the resend is a new request, so the refusal
    /// of the resend reports an unknown outcome that nothing retries.
    #[tokio::test(start_paused = true)]
    async fn given_resend_after_uncertain_attempt_when_refused_should_report_not_committed() {
        for (status, refusal) in [
            (400, IggyError::LifecycleBusy),
            (403, IggyError::Unauthorized),
            (401, IggyError::Unauthenticated),
        ] {
            let server = ScriptedServer::start(vec![
                Reply::empty(503),
                Reply::error(status, &refusal),
                Reply::empty(200),
            ])
            .await;

            let unknown = server.client().delete_stream(&stream_id()).await;

            assert!(
                matches!(unknown, Err(IggyError::TransientNotCommitted)),
                "{refusal}: {unknown:?}"
            );
            assert_eq!(server.requests().len(), 2, "{refusal}");
        }
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

    #[test]
    fn typed_refusals_survive_http_decoding() {
        for error in [
            IggyError::RequestTooOld,
            IggyError::HistoryUnavailable,
            IggyError::LifecycleBusy,
        ] {
            let body = format!(r#"{{"id":{},"reason":"unavailable"}}"#, error.as_code());
            assert_eq!(typed_refusal(&body), Some(error));
        }
        for body in ["", "unavailable", r#"{"id":500}"#, r#"{"id":"87"}"#] {
            assert_eq!(typed_refusal(body), None);
        }
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
