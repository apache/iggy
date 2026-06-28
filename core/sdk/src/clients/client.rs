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

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::client_wrappers::connection_info::ConnectionInfo;
use crate::clients::client_builder::IggyClientBuilder;
use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::EncryptorKind;
use crate::prelude::IggyConsumerBuilder;
use crate::prelude::IggyError;
use crate::prelude::IggyProducerBuilder;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use crate::websocket::websocket_client::WebSocketClient;
use async_broadcast::Receiver;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE, LOGIN_USER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGOUT_USER_CODE,
};
use iggy_common::Consumer;
use iggy_common::locking::{IggyRwLock, IggyRwLockFn};
use iggy_common::{BinaryTransport, Client, HttpMethod, SystemClient};
use iggy_common::{ConnectionStringUtils, DiagnosticEvent, Partitioner, TransportProtocol};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::spawn;
use tokio::time::sleep;
use tracing::log::warn;
use tracing::{debug, error, info};

/// Auth/session codes rejected by the raw binary path. Must go through the
/// typed `login_user` / `logout_user` methods to keep session state correct.
const SESSION_CONTROL_CODES: [u32; 5] = [
    LOGIN_USER_CODE,
    LOGOUT_USER_CODE,
    LOGIN_REGISTER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
    LOGIN_REGISTER_WITH_PAT_CODE,
];

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
///
/// It also provides the additional builders for the standalone consumer, consumer group, and producer.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IggyClient {
    pub(crate) client: IggyRwLock<ClientWrapper>,
    partitioner: Option<Arc<dyn Partitioner>>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(ClientWrapper::Tcp(TcpClient::default()))
    }
}

impl IggyClient {
    /// Creates a new `IggyClientBuilder`.
    pub fn builder() -> IggyClientBuilder {
        IggyClientBuilder::new()
    }

    /// Creates a new `IggyClientBuilder` from the provided connection string.
    pub fn builder_from_connection_string(
        connection_string: &str,
    ) -> Result<IggyClientBuilder, IggyError> {
        IggyClientBuilder::from_connection_string(connection_string)
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport.
    pub fn new(client: ClientWrapper) -> Self {
        let client = IggyRwLock::new(client);
        IggyClient {
            client,
            partitioner: None,
            encryptor: None,
        }
    }

    /// Creates a new `IggyClient` from the provided connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => Ok(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Quic => Ok(IggyClient::new(ClientWrapper::Quic(
                QuicClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Http => Ok(IggyClient::new(ClientWrapper::Http(
                HttpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::WebSocket => Ok(IggyClient::new(ClientWrapper::WebSocket(
                WebSocketClient::from_connection_string(connection_string)?,
            ))),
        }
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport and the optional implementations for the `partitioner` and `encryptor`.
    pub fn create(
        client: ClientWrapper,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = IggyRwLock::new(client);
        IggyClient {
            client,
            partitioner,
            encryptor,
        }
    }

    /// Returns the underlying client implementation for the specific transport.
    pub fn client(&self) -> IggyRwLock<ClientWrapper> {
        self.client.clone()
    }

    /// Returns the builder for the standalone consumer.
    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the consumer group.
    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the producer.
    pub fn producer(&self, stream: &str, topic: &str) -> Result<IggyProducerBuilder, IggyError> {
        Ok(IggyProducerBuilder::new(
            self.client.clone(),
            stream.try_into()?,
            stream.to_owned(),
            topic.try_into()?,
            topic.to_owned(),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the current connection information including the transport protocol and server address.
    /// This is useful for verifying which server the client is connected to, especially after
    /// leader redirection in a clustered environment.
    pub async fn get_connection_info(&self) -> ConnectionInfo {
        self.client.read().await.get_connection_info().await
    }

    /// Send a raw binary command (`code` + serialized `payload`), returning the
    /// raw response. Binary transports only (HTTP yields `FeatureUnavailable`).
    ///
    /// Login and logout codes are rejected with `InvalidCommand`. Use the
    /// `login_user` / `logout_user` methods so SDK session state stays correct.
    ///
    /// Custom codes only work on the classic protocol. Under `vsr` the encoder
    /// is closed-world: an unknown code yields `InvalidCommand`, a replicated
    /// code with no mapping yields `UnknownReplicatedCommand`.
    pub async fn send_binary_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if SESSION_CONTROL_CODES.contains(&code) {
            return Err(IggyError::InvalidCommand);
        }
        match &*self.client.read().await {
            ClientWrapper::Tcp(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Quic(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::WebSocket(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => Err(IggyError::FeatureUnavailable),
        }
    }

    /// Invoke an arbitrary HTTP endpoint and return the raw response body. HTTP
    /// transport only; binary transports yield `FeatureUnavailable`.
    pub async fn send_http_request(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<Bytes>,
    ) -> Result<Bytes, IggyError> {
        match &*self.client.read().await {
            ClientWrapper::Http(client) => client.send_http_request(method, path, body).await,
            ClientWrapper::Tcp(_)
            | ClientWrapper::Quic(_)
            | ClientWrapper::WebSocket(_)
            | ClientWrapper::Iggy(_) => Err(IggyError::FeatureUnavailable),
        }
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&self) -> Result<(), IggyError> {
        let heartbeat_interval;
        {
            let client = self.client.read().await;
            client.connect().await?;
            heartbeat_interval = client.heartbeat_interval().await;
        }

        let client = self.client.clone();
        spawn(async move {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(error) = client.read().await.ping().await {
                    error!("There was an error when sending a heartbeat. {error}");
                    if error == IggyError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                    // Picks up a widened assignment (e.g. partition-count
                    // change) without waiting for an ownership-fence rejection.
                    client
                        .read()
                        .await
                        .refresh_consumer_group_assignments()
                        .await;
                }
                sleep(heartbeat_interval.get_duration()).await
            }
        });
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().await.disconnect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.client.read().await.shutdown().await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.client.read().await.subscribe_events().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = IggyClient::from_connection_string(value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_reject_http_request_on_binary_transport() {
        let client = IggyClient::default();
        let result = client
            .send_http_request(HttpMethod::Get, "/ping", None)
            .await;
        assert!(matches!(result, Err(IggyError::FeatureUnavailable)));
    }

    #[tokio::test]
    async fn should_reject_binary_request_on_http_transport() {
        let client =
            IggyClient::from_connection_string("iggy+http://user:secret@127.0.0.1:1234").unwrap();
        let result = client.send_binary_request(0, Bytes::new()).await;
        assert!(matches!(result, Err(IggyError::FeatureUnavailable)));
    }

    #[tokio::test]
    async fn should_reject_session_control_codes_on_binary_request() {
        let client = IggyClient::default();
        for code in SESSION_CONTROL_CODES {
            let result = client.send_binary_request(code, Bytes::new()).await;
            assert!(
                matches!(result, Err(IggyError::InvalidCommand)),
                "code {code} must be rejected before reaching the transport"
            );
        }
    }
}
