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
use crate::poll_routing::{ROUTING_RETRY_INTERVAL, ROUTING_RETRY_MAX_INTERVAL};
use crate::prelude::{Client, HttpClientConfig, IggyError, NonZeroIggyDuration};
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    ConnectionString, ConnectionStringUtils, DiagnosticEvent, HttpConnectionStringOptions,
    HttpMethod, IdentityInfo, TransportProtocol, validate_api_url,
};
use reqwest::{Method, Response, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use reqwest_tracing::{SpanBackendWithUrl, TracingMiddleware};
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

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
    client: ClientWithMiddleware,
    deferred_leases: tokio::sync::Semaphore,
    access_token: IggyRwLock<String>,
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
        let token = self.access_token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token.deref())
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP GET request to the Iggy API with query parameters.
    async fn get_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .get(url)
            .bearer_auth(token.deref())
            .query(query)
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP POST request to the Iggy API.
    async fn post<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .post(url)
            .bearer_auth(token.deref())
            .json(payload)
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP PUT request to the Iggy API.
    async fn put<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        payload: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .put(url)
            .bearer_auth(token.deref())
            .json(payload)
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API.
    async fn delete(&self, path: &str) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token.deref())
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
    }

    /// Invoke HTTP DELETE request to the Iggy API with query parameters.
    async fn delete_with_query<T: Serialize + Sync + ?Sized>(
        &self,
        path: &str,
        query: &T,
    ) -> Result<Response, IggyError> {
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let token = self.access_token.read().await;
        let response = self
            .client
            .delete(url)
            .bearer_auth(token.deref())
            .query(query)
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        Self::handle_response(response).await
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
        let token = self.access_token.read().await;
        let mut request = self.client.request(method, url).bearer_auth(token.deref());
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|_| IggyError::InvalidHttpRequest)?;
        let response = Self::handle_response(response).await?;
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
    pub(super) async fn get_deferred_poll(
        &self,
        path: &str,
        poll: &iggy_common::PollMessages,
        options: iggy_common::DeferredPollOptions,
    ) -> Result<iggy_common::PolledMessages, IggyError> {
        options.validate(poll.count)?;
        let started = tokio::time::Instant::now();
        let deadline = started + options.request_timeout.get_duration();
        let _permit = tokio::time::timeout_at(deadline, self.deferred_leases.acquire())
            .await
            .map_err(|_| IggyError::TransientNotAccepted)?
            .map_err(|_| IggyError::ClientShutdown)?;
        let url = self.get_url(path)?;
        self.fail_if_not_authenticated(path).await?;
        let mut retry_interval = ROUTING_RETRY_INTERVAL;
        loop {
            let request = {
                let token = self.access_token.read().await;
                let remaining = options.remaining(started.elapsed())?;
                // Deferred auto-commits cannot replay an ambiguous transport failure.
                self.client
                    .as_ref()
                    .get(url.clone())
                    .bearer_auth(token.deref())
                    .query(poll)
                    .query(&[
                        ("wait_us", remaining.max_wait.as_micros()),
                        ("min_count", u64::from(remaining.min_count)),
                        ("max_bytes", u64::from(remaining.max_bytes)),
                        ("request_timeout_us", remaining.request_timeout.as_micros()),
                    ])
                    .timeout(remaining.request_timeout.get_duration())
            };
            let mut response = request
                .send()
                .await
                .map_err(|_| IggyError::InvalidHttpRequest)?;
            let status = response.status();
            // Bound JSON expansion and error bodies under the same request budget.
            const MAX_JSON_EXPANSION: usize = 16;
            let limit = (options.max_bytes as usize).saturating_mul(MAX_JSON_EXPANSION);
            if response
                .content_length()
                .is_some_and(|length| length > limit as u64)
            {
                return Err(IggyError::InvalidSizeBytes);
            }
            let mut body = Vec::new();
            while let Some(chunk) = tokio::time::timeout_at(deadline, response.chunk())
                .await
                .map_err(|_| IggyError::TransientNotCommitted)?
                .map_err(|_| IggyError::InvalidHttpRequest)?
            {
                if chunk.len() > limit.saturating_sub(body.len()) {
                    return Err(IggyError::InvalidSizeBytes);
                }
                body.extend_from_slice(&chunk);
            }
            if status.is_success() {
                let messages: iggy_common::PolledMessages =
                    serde_json::from_slice(&body).map_err(|_| IggyError::InvalidJsonResponse)?;
                if messages.count > poll.count || messages.messages.len() != messages.count as usize
                {
                    return Err(IggyError::InvalidMessagesCount);
                }
                return Ok(messages);
            }
            let error = Self::response_error(status, String::from_utf8_lossy(&body).into_owned());
            if !HttpRejection::is_not_accepted(&error) {
                return Err(error);
            }
            if tokio::time::Instant::now() + retry_interval >= deadline {
                return Err(IggyError::TransientNotAccepted);
            }
            tokio::time::sleep(retry_interval).await;
            retry_interval = (retry_interval * 2).min(ROUTING_RETRY_MAX_INTERVAL);
        }
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
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(TracingMiddleware::<SpanBackendWithUrl>::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let access_token = config.jwt.clone().unwrap_or_default();

        Ok(Self {
            api_url,
            client,
            deferred_leases: tokio::sync::Semaphore::new(
                crate::poll_routing::MAX_DEFERRED_CONNECTIONS,
            ),
            heartbeat_interval: config.heartbeat_interval,
            access_token: IggyRwLock::new(access_token),
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
            _ => IggyError::HttpResponseError(status.as_u16(), reason),
        }
    }
}

#[derive(Debug, Serialize)]
struct RefreshToken {
    token: String,
}

#[derive(Deserialize)]
struct HttpRejection {
    id: u32,
}

impl HttpRejection {
    fn is_not_accepted(error: &IggyError) -> bool {
        matches!(error, IggyError::HttpResponseError(status, body)
            if *status == StatusCode::SERVICE_UNAVAILABLE.as_u16()
                && serde_json::from_str::<Self>(body)
                    .is_ok_and(|error| error.id == IggyError::TransientNotAccepted.as_code()))
    }
}

/// Unit tests for HttpClient.
/// TODO: Add complete unit tests for HttpClient.
#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{Consumer, Identifier, PollMessages, PollingStrategy};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn deferred_poll_retries_only_nonadmission_with_remaining_wait() {
        let (client, requests) = deferred_response_server(
            [
                IggyError::TransientNotAccepted.as_code(),
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
            if serde_json::from_str::<HttpRejection>(&body).unwrap().id
                == IggyError::TransientNotCommitted.as_code())
        );
        let waits = requests.await.unwrap();
        assert_eq!(waits.len(), 2);
        assert!(waits[0] <= 1_000_000);
        assert!(waits[1] > 0 && waits[1] < waits[0], "waits: {waits:?}");
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
                    max_wait: 50_000.into(),
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

    #[tokio::test]
    async fn deferred_poll_bounds_success_and_error_bodies() {
        const MAX_BYTES: u32 = 16;
        for status in [200, 503] {
            let (client, requests) =
                deferred_response_server(vec![(status, " ".repeat(16 * MAX_BYTES as usize + 1))])
                    .await;
            let result = client
                .get_deferred_poll(
                    "messages/deferred",
                    &deferred_request(),
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
