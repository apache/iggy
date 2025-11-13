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

use crate::prelude::Client;
use crate::prelude::TcpClientConfig;
use crate::tcp::tcp_connection_stream::TcpConnectionStream;
use crate::tcp::tcp_connection_stream_kind::ConnectionStreamKind;
use crate::tcp::tcp_tls_connection_stream::TcpTlsConnectionStream;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use iggy_common::{
    AutoLogin, ClientState, Command, ConnectionString, ConnectionStringUtils, Credentials,
    DiagnosticEvent, IggyDuration, IggyError, IggyErrorDiscriminants, IggyTimestamp,
    TcpConnectionStringOptions, TransportProtocol,
};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_rustls::{TlsConnector, TlsStream};
use tracing::{error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// TCP client for interacting with the Iggy API.
/// It requires a valid server address.
#[derive(Debug)]
pub struct TcpClient {
    pub(crate) stream: Arc<Mutex<Option<ConnectionStreamKind>>>,
    pub(crate) config: Arc<TcpClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    connected_at: Mutex<Option<IggyTimestamp>>,
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        TcpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        TcpClient::disconnect(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        TcpClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
#[async_trait]
impl BinaryTransport for TcpClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
        }
    }

    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let result = self.send_raw(code, payload.clone()).await;
        if result.is_ok() {
            return result;
        }

        let error = result.unwrap_err();
        if !matches!(
            error,
            IggyError::Disconnected
                | IggyError::EmptyResponse
                | IggyError::Unauthenticated
                | IggyError::StaleClient
        ) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        self.disconnect().await?;

        {
            let client_address = self.get_client_address_value().await;
            info!(
                "Reconnecting to the server: {} by client: {client_address}...",
                self.config.server_address
            );
        }

        self.connect().await?;
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }
}

impl BinaryClient for TcpClient {}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new TCP client for the provided server address using TLS.
    pub fn new_tls(
        server_address: &str,
        domain: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(TcpClientConfig {
            heartbeat_interval,
            server_address: server_address.to_string(),
            tls_enabled: true,
            tls_domain: domain.to_string(),
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new TCP client from the provided connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Tcp {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(Arc::new(
            ConnectionString::<TcpConnectionStringOptions>::from_str(connection_string)?.into(),
        ))
    }

    /// Create a new TCP client based on the provided configuration.
    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        Ok(Self {
            config,
            client_address: Mutex::new(None),
            stream: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
        })
    }

    async fn handle_response(
        status: u32,
        length: u32,
        stream: &mut ConnectionStreamKind,
    ) -> Result<Bytes, IggyError> {
        if status != 0 {
            // TEMP: See https://github.com/apache/iggy/pull/604 for context.
            if status == IggyErrorDiscriminants::TopicNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::UserAlreadyExists as u32
                || status == IggyErrorDiscriminants::PersonalAccessTokenAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupNameAlreadyExists as u32
            {
                tracing::debug!(
                    "Received a server resource already exists response: {} ({})",
                    status,
                    IggyError::from_code_as_string(status)
                )
            } else {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status),
                );
            }

            return Err(IggyError::from_code(status));
        }

        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        let mut response_buffer = BytesMut::with_capacity(length as usize);
        response_buffer.put_bytes(0, length as usize);
        stream.read(&mut response_buffer).await?;
        Ok(response_buffer.freeze())
    }

    async fn connect(&self) -> Result<(), IggyError> {
        match self.get_state().await {
            ClientState::Shutdown => {
                trace!("Cannot connect. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated => {
                let client_address = self.get_client_address_value().await;
                trace!("Client: {client_address} is already connected.");
                return Ok(());
            }
            ClientState::Connecting => {
                trace!("Client is already connecting.");
                return Ok(());
            }
            _ => {}
        }

        self.set_state(ClientState::Connecting).await;
        if let Some(connected_at) = self.connected_at.lock().await.as_ref() {
            let now = IggyTimestamp::now();
            let elapsed = now.as_micros() - connected_at.as_micros();
            let interval = self.config.reconnection.reestablish_after.as_micros();
            trace!(
                "Elapsed time since last connection: {}",
                IggyDuration::from(elapsed)
            );
            if elapsed < interval {
                let remaining = IggyDuration::from(interval - elapsed);
                info!("Trying to connect to the server in: {remaining}",);
                sleep(remaining.get_duration()).await;
            }
        }

        let tls_enabled = self.config.tls_enabled;
        let mut retry_count = 0;
        let connection_stream: ConnectionStreamKind;
        let remote_address;
        let client_address;
        loop {
            info!(
                "{NAME} client is connecting to server: {}...",
                self.config.server_address
            );

            let connection = TcpStream::connect(&self.config.server_address).await;
            if let Err(err) = &connection {
                error!(
                    "Failed to connect to server: {}. Error: {}",
                    self.config.server_address, err
                );
                if !self.config.reconnection.enabled {
                    warn!("Automatic reconnection is disabled.");
                    return Err(IggyError::CannotEstablishConnection);
                }

                let unlimited_retries = self.config.reconnection.max_retries.is_none();
                let max_retries = self.config.reconnection.max_retries.unwrap_or_default();
                let max_retries_str =
                    if let Some(max_retries) = self.config.reconnection.max_retries {
                        max_retries.to_string()
                    } else {
                        "unlimited".to_string()
                    };

                let interval_str = self.config.reconnection.interval.as_human_time_string();
                if unlimited_retries || retry_count < max_retries {
                    retry_count += 1;
                    info!(
                        "Retrying to connect to server ({retry_count}/{max_retries_str}): {} in: {interval_str}",
                        self.config.server_address,
                    );
                    sleep(self.config.reconnection.interval.get_duration()).await;
                    continue;
                }

                self.set_state(ClientState::Disconnected).await;
                self.publish_event(DiagnosticEvent::Disconnected).await;
                return Err(IggyError::CannotEstablishConnection);
            }

            let stream = connection.map_err(|error| {
                error!("Failed to establish TCP connection to the server: {error}",);
                IggyError::CannotEstablishConnection
            })?;
            client_address = stream.local_addr().map_err(|error| {
                error!("Failed to get the local address of the client: {error}",);
                IggyError::CannotEstablishConnection
            })?;
            remote_address = stream.peer_addr().map_err(|error| {
                error!("Failed to get the remote address of the server: {error}",);
                IggyError::CannotEstablishConnection
            })?;
            self.client_address.lock().await.replace(client_address);

            if let Err(e) = stream.set_nodelay(self.config.nodelay) {
                error!("Failed to set the nodelay option on the client: {e}, continuing...",);
            }

            if !tls_enabled {
                connection_stream =
                    ConnectionStreamKind::Tcp(TcpConnectionStream::new(client_address, stream));
                break;
            }

            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

            let config = if self.config.tls_validate_certificate {
                let mut root_cert_store = rustls::RootCertStore::empty();
                if let Some(certificate_path) = &self.config.tls_ca_file {
                    for cert in
                        CertificateDer::pem_file_iter(certificate_path).map_err(|error| {
                            error!("Failed to read the CA file: {certificate_path}. {error}",);
                            IggyError::InvalidTlsCertificatePath
                        })?
                    {
                        let certificate = cert.map_err(|error| {
                            error!(
                                "Failed to read a certificate from the CA file: {certificate_path}. {error}",
                            );
                            IggyError::InvalidTlsCertificate
                        })?;
                        root_cert_store.add(certificate).map_err(|error| {
                            error!(
                                "Failed to add a certificate to the root certificate store. {error}",
                            );
                            IggyError::InvalidTlsCertificate
                        })?;
                    }
                } else {
                    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                }

                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            } else {
                use crate::tcp::tcp_tls_verifier::NoServerVerification;
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoServerVerification))
                    .with_no_client_auth()
            };
            let connector = TlsConnector::from(Arc::new(config));
            let tls_domain = if self.config.tls_domain.is_empty() {
                // Extract hostname/IP from server_address when tls_domain is not specified
                self.config
                    .server_address
                    .split(':')
                    .next()
                    .unwrap_or(&self.config.server_address)
                    .to_string()
            } else {
                self.config.tls_domain.to_owned()
            };
            let domain = ServerName::try_from(tls_domain).map_err(|error| {
                error!("Failed to create a server name from the domain. {error}",);
                IggyError::InvalidTlsDomain
            })?;
            let stream = connector.connect(domain, stream).await.map_err(|error| {
                error!("Failed to establish a TLS connection to the server: {error}",);
                IggyError::CannotEstablishConnection
            })?;
            connection_stream = ConnectionStreamKind::TcpTls(TcpTlsConnectionStream::new(
                client_address,
                TlsStream::Client(stream),
            ));
            break;
        }

        let now = IggyTimestamp::now();
        info!(
            "{NAME} client: {client_address} has connected to server: {remote_address} at: {now}",
        );
        self.stream.lock().await.replace(connection_stream);
        self.set_state(ClientState::Connected).await;
        self.connected_at.lock().await.replace(now);
        self.publish_event(DiagnosticEvent::Connected).await;
        match &self.config.auto_login {
            AutoLogin::Disabled => {
                info!("Automatic sign-in is disabled.");
                Ok(())
            }
            AutoLogin::Enabled(credentials) => {
                info!("{NAME} client: {client_address} is signing in...");
                self.set_state(ClientState::Authenticating).await;
                match credentials {
                    Credentials::UsernamePassword(username, password) => {
                        self.login_user(username, password).await?;
                        info!(
                            "{NAME} client: {client_address} has signed in with the user credentials, username: {username}",
                        );
                        Ok(())
                    }
                    Credentials::PersonalAccessToken(token) => {
                        self.login_with_personal_access_token(token).await?;
                        info!(
                            "{NAME} client: {client_address} has signed in with a personal access token.",
                        );
                        Ok(())
                    }
                }
            }
        }
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        let client_address = self.get_client_address_value().await;
        info!("{NAME} client: {client_address} is disconnecting from server...");
        self.set_state(ClientState::Disconnected).await;
        self.stream.lock().await.take();
        self.publish_event(DiagnosticEvent::Disconnected).await;
        let now = IggyTimestamp::now();
        info!("{NAME} client: {client_address} has disconnected from server at: {now}.");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Shutdown {
            return Ok(());
        }

        let client_address = self.get_client_address_value().await;
        info!("Shutting down the {NAME} TCP client: {client_address}");
        let stream = self.stream.lock().await.take();
        if let Some(mut stream) = stream {
            stream.shutdown().await?;
        }
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} TCP client: {client_address} has been shutdown.");
        Ok(())
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        match self.get_state().await {
            ClientState::Shutdown => {
                trace!("Cannot send data. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            ClientState::Disconnected => {
                trace!("Cannot send data. Client is not connected.");
                return Err(IggyError::NotConnected);
            }
            ClientState::Connecting => {
                trace!("Cannot send data. Client is still connecting.");
                return Err(IggyError::NotConnected);
            }
            _ => {}
        }

        let stream = self.stream.clone();
        // SAFETY: we run code holding the `stream` lock in a task so we can't be cancelled while holding the lock.
        tokio::spawn(async move {
            let mut stream = stream.lock().await;
            if let Some(stream) = stream.as_mut() {
                let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;
                trace!("Sending a TCP request of size {payload_length} with code: {code}");
                stream.write(&(payload_length as u32).to_le_bytes()).await?;
                stream.write(&code.to_le_bytes()).await?;
                stream.write(&payload).await?;
                stream.flush().await?;
                trace!("Sent a TCP request with code: {code}, waiting for a response...");
                let mut response_buffer = [0u8; RESPONSE_INITIAL_BYTES_LENGTH];
                let read_bytes = stream.read(&mut response_buffer).await.map_err(|error| {
                    error!(
                        "Failed to read response for TCP request with code: {code}: {error}",
                        code = code,
                        error = error
                    );
                    IggyError::Disconnected
                })?;

                if read_bytes != RESPONSE_INITIAL_BYTES_LENGTH {
                    error!("Received an invalid or empty response.");
                    return Err(IggyError::EmptyResponse);
                }

                let status = u32::from_le_bytes(
                    response_buffer[..4]
                        .try_into()
                        .map_err(|_| IggyError::InvalidNumberEncoding)?,
                );
                let length = u32::from_le_bytes(
                    response_buffer[4..]
                        .try_into()
                        .map_err(|_| IggyError::InvalidNumberEncoding)?,
                );
                return TcpClient::handle_response(status, length, stream).await;
            }

            error!("Cannot send data. Client is not connected.");
            Err(IggyError::NotConnected)
        })
        .await
        .map_err(|e| {
            error!("Task execution failed during TCP request: {}", e);
            IggyError::TcpError
        })?
    }

    async fn get_client_address_value(&self) -> String {
        let client_address = self.client_address.lock().await;
        if let Some(client_address) = &*client_address {
            client_address.to_string()
        } else {
            "unknown".to_string()
        }
    }
}

/// Unit tests for TcpClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for TcpClient.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let tcp_client = TcpClient::from_connection_string(value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
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
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_err());
    }

    #[test]
    fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let heartbeat_interval = "10s";
        let reconnection_retries = "10";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?heartbeat_interval={heartbeat_interval}&reconnection_retries={reconnection_retries}"
        );
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            IggyDuration::from_str(heartbeat_interval).unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert_eq!(
            tcp_client_config.reconnection.max_retries.unwrap(),
            reconnection_retries.parse::<u32>().unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[test]
    fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let tcp_client = TcpClient::from_connection_string(&value);
        assert!(tcp_client.is_ok());

        let tcp_client_config = tcp_client.unwrap().config;
        assert_eq!(
            tcp_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            tcp_client_config.auto_login,
            AutoLogin::Enabled(Credentials::PersonalAccessToken(pat.to_string()))
        );

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
