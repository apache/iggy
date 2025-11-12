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

use crate::http::http_transport::HttpTransport;
use crate::prelude::{Client, HttpClientConfig, IggyDuration, IggyError};
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{
    ConnectionString, ConnectionStringUtils, DiagnosticEvent, HttpConnectionStringOptions,
    IdentityInfo, TransportProtocol,
};
use reqwest::{Response, StatusCode, Url};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::Serialize;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

const PUBLIC_PATHS: &[&str] = &[
    "/",
    "/metrics",
    "/ping",
    "/stats",
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
    pub(crate) heartbeat_interval: IggyDuration,
    client: ClientWithMiddleware,
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

    /// Returns true if the client is authenticated.
    async fn is_authenticated(&self) -> bool {
        let token = self.access_token.read().await;
        !token.is_empty()
    }

    /// Refresh the access token using the current access token.
    // TODO(hubcio): method `refresh_access_token` is never used
    async fn _refresh_access_token(&self) -> Result<(), IggyError> {
        let token = self.access_token.read().await;
        if token.is_empty() {
            return Err(IggyError::AccessTokenMissing);
        }

        let command = _RefreshToken {
            token: token.to_owned(),
        };
        let response = self.post("/users/refresh-token", &command).await?;
        let identity_info: IdentityInfo = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        if identity_info.access_token.is_none() {
            return Err(IggyError::JwtMissing);
        }

        self.set_token_from_identity(&identity_info).await?;
        Ok(())
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
        let api_url = Url::parse(&config.api_url);
        if api_url.is_err() {
            return Err(IggyError::CannotParseUrl);
        }
        let api_url = api_url.unwrap();
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(config.retries);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Ok(Self {
            api_url,
            client,
            heartbeat_interval: IggyDuration::from_str("5s").unwrap(),
            access_token: IggyRwLock::new("".to_string()),
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
                    _ => Err(IggyError::HttpResponseError(status.as_u16(), reason)),
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
struct _RefreshToken {
    token: String,
}

/// Unit tests for HttpClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for HttpClient.
#[cfg(test)]
mod tests {
    use super::*;

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
            IggyDuration::from_str("5s").unwrap()
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
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?retries={retries}"
        );
        let http_client = HttpClient::from_connection_string(&value);
        assert!(http_client.is_ok());

        assert_eq!(
            http_client.as_ref().unwrap().api_url.to_string(),
            format!("{protocol}://{server_address}:{port}/")
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
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
