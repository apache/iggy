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

use crate::leader_aware::{LeaderRedirectionState, check_and_redirect_to_leader};
use crate::prelude::AutoLogin;
use iggy_binary_protocol::{
    BinaryClient, BinaryTransport, Client, PersonalAccessTokenClient, UserClient,
};

use crate::prelude::{IggyDuration, IggyError, IggyTimestamp, QuicClientConfig};
use crate::quic::skip_server_verification::SkipServerVerification;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::{
    ClientState, Command, ConnectionString, ConnectionStringUtils, Credentials, DiagnosticEvent,
    IggyErrorDiscriminants, QuicConnectionStringOptions, TransportProtocol,
};
use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, VarInt};
use rustls::crypto::CryptoProvider;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// QUIC client for interacting with the Iggy API.
#[derive(Debug)]
pub struct QuicClient {
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Arc<Mutex<Option<Connection>>>,
    pub(crate) config: Arc<QuicClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    pub(crate) connected_at: Mutex<Option<IggyTimestamp>>,
    leader_redirection_state: Mutex<LeaderRedirectionState>,
    pub(crate) current_server_address: Mutex<String>,
}

unsafe impl Send for QuicClient {}
unsafe impl Sync for QuicClient {}

impl Default for QuicClient {
    fn default() -> Self {
        QuicClient::create(Arc::new(QuicClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for QuicClient {
    async fn connect(&self) -> Result<(), IggyError> {
        QuicClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        QuicClient::disconnect(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        QuicClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for QuicClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a QUIC diagnostic event: {error}");
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
            IggyError::Disconnected | IggyError::EmptyResponse | IggyError::Unauthenticated
        ) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        self.disconnect().await?;
        let server_address = self.current_server_address.lock().await.to_string();
        info!(
            "Reconnecting to the server: {}, by client: {}",
            server_address, self.config.client_address
        );
        self.connect().await?;
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }
}

impl BinaryClient for QuicClient {}

impl QuicClient {
    /// Creates a new QUIC client for the provided client and server addresses.
    pub fn new(
        client_address: &str,
        server_address: &str,
        server_name: &str,
        validate_certificate: bool,
        auto_sign_in: AutoLogin,
    ) -> Result<Self, IggyError> {
        Self::create(Arc::new(QuicClientConfig {
            client_address: client_address.to_string(),
            server_address: server_address.to_string(),
            server_name: server_name.to_string(),
            validate_certificate,
            auto_login: auto_sign_in,
            ..Default::default()
        }))
    }

    /// Create a new QUIC client for the provided configuration.
    pub fn create(config: Arc<QuicClientConfig>) -> Result<Self, IggyError> {
        let server_address = config
            .server_address
            .parse::<SocketAddr>()
            .map_err(|error| {
                error!("Invalid server address: {error}");
                IggyError::InvalidServerAddress
            })?;
        let client_address = if server_address.is_ipv6()
            && config.client_address == QuicClientConfig::default().client_address
        {
            "[::1]:0"
        } else {
            &config.client_address
        }
        .parse::<SocketAddr>()
        .map_err(|error| {
            error!("Invalid client address: {error}");
            IggyError::InvalidClientAddress
        })?;

        let quic_config = configure(&config)?;
        let endpoint = Endpoint::client(client_address);
        if endpoint.is_err() {
            error!("Cannot create client endpoint");
            return Err(IggyError::CannotCreateEndpoint);
        }

        let mut endpoint = endpoint.unwrap();
        endpoint.set_default_client_config(quic_config);

        Ok(Self {
            config,
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address.to_string()),
        })
    }

    /// Creates a new QUIC client from a connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        if ConnectionStringUtils::parse_protocol(connection_string)? != TransportProtocol::Quic {
            return Err(IggyError::InvalidConnectionString);
        }

        Self::create(Arc::new(
            ConnectionString::<QuicConnectionStringOptions>::from_str(connection_string)?.into(),
        ))
    }

    async fn handle_response(
        recv: &mut RecvStream,
        response_buffer_size: usize,
    ) -> Result<Bytes, IggyError> {
        let buffer = recv
            .read_to_end(response_buffer_size)
            .await
            .map_err(|error| {
                error!("Failed to read response data: {error}");
                IggyError::QuicError
            })?;
        if buffer.is_empty() {
            return Err(IggyError::EmptyResponse);
        }

        let status = u32::from_le_bytes(
            buffer[..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        if status != 0 {
            // Log FeatureUnavailable as debug instead of error (e.g., when clustering is disabled)
            if status == IggyErrorDiscriminants::FeatureUnavailable as u32 {
                tracing::debug!(
                    "Feature unavailable on server: {} ({})",
                    status,
                    IggyError::from_code_as_string(status)
                );
            } else {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status)
                );
            }

            return Err(IggyError::from_code(status));
        }

        let length = u32::from_le_bytes(
            buffer[4..RESPONSE_INITIAL_BYTES_LENGTH]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        trace!("Status: OK. Response length: {}", length);
        if length <= 1 {
            return Ok(Bytes::new());
        }

        Ok(Bytes::copy_from_slice(
            &buffer[RESPONSE_INITIAL_BYTES_LENGTH..RESPONSE_INITIAL_BYTES_LENGTH + length as usize],
        ))
    }

    async fn connect(&self) -> Result<(), IggyError> {
        loop {
            match self.get_state().await {
                ClientState::Shutdown => {
                    trace!("Cannot connect. Client is shutdown.");
                    return Err(IggyError::ClientShutdown);
                }
                ClientState::Connected
                | ClientState::Authenticating
                | ClientState::Authenticated => {
                    trace!("Client is already connected.");
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

            let mut retry_count = 0;
            let connection;
            let remote_address;
            loop {
                let server_address_str = self.current_server_address.lock().await.clone();
                let server_address: SocketAddr = server_address_str.parse().map_err(|e| {
                    error!(
                        "Failed to parse server address '{}': {}",
                        server_address_str, e
                    );
                    IggyError::InvalidServerAddress
                })?;
                info!(
                    "{NAME} client is connecting to server: {}...",
                    server_address
                );
                let connection_result = self
                    .endpoint
                    .connect(server_address, &self.config.server_name)
                    .unwrap()
                    .await;

                if connection_result.is_err() {
                    error!("Failed to connect to server: {}", server_address);
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
                            server_address,
                        );
                        sleep(self.config.reconnection.interval.get_duration()).await;
                        continue;
                    }

                    self.set_state(ClientState::Disconnected).await;
                    self.publish_event(DiagnosticEvent::Disconnected).await;
                    return Err(IggyError::CannotEstablishConnection);
                }

                connection = connection_result.map_err(|error| {
                    error!("Failed to establish QUIC connection: {error}");
                    IggyError::CannotEstablishConnection
                })?;
                remote_address = connection.remote_address();
                break;
            }

            let now = IggyTimestamp::now();
            info!("{NAME} client has connected to server: {remote_address} at {now}",);
            self.set_state(ClientState::Connected).await;
            self.connection.lock().await.replace(connection);
            self.connected_at.lock().await.replace(now);
            self.publish_event(DiagnosticEvent::Connected).await;

            // Handle auto-login
            let should_redirect = match &self.config.auto_login {
                AutoLogin::Disabled => {
                    info!("Automatic sign-in is disabled.");
                    false
                }
                AutoLogin::Enabled(credentials) => {
                    info!(
                        "{NAME} client: {} is signing in...",
                        self.config.client_address
                    );
                    self.set_state(ClientState::Authenticating).await;
                    match credentials {
                        Credentials::UsernamePassword(username, password) => {
                            self.login_user(username, password).await?;
                            self.publish_event(DiagnosticEvent::SignedIn).await;
                            info!(
                                "{NAME} client: {} has signed in with the user credentials, username: {username}",
                                self.config.client_address
                            );
                        }
                        Credentials::PersonalAccessToken(token) => {
                            self.login_with_personal_access_token(token).await?;
                            self.publish_event(DiagnosticEvent::SignedIn).await;
                            info!(
                                "{NAME} client: {} has signed in with a personal access token.",
                                self.config.client_address
                            );
                        }
                    }

                    self.handle_leader_redirection().await?
                }
            };

            if should_redirect {
                continue;
            }

            return Ok(());
        }
    }

    /// Checks cluster metadata and handles leader redirection if needed.
    /// Returns true if redirection occurred and reconnection is needed.
    pub(crate) async fn handle_leader_redirection(&self) -> Result<bool, IggyError> {
        let current_address = self.current_server_address.lock().await.clone();
        let leader_address = check_and_redirect_to_leader(self, &current_address).await?;

        if let Some(new_leader_address) = leader_address {
            let mut redirection_state = self.leader_redirection_state.lock().await;
            if !redirection_state.can_redirect() {
                warn!("Maximum leader redirections reached, continuing with current connection");
                return Ok(false);
            }

            info!(
                "Current node is not leader, redirecting to leader at: {}",
                new_leader_address
            );
            redirection_state.increment_redirect(new_leader_address.clone());
            drop(redirection_state);

            // Clear connected_at to avoid reestablish_after delay during redirection
            self.connected_at.lock().await.take();
            self.disconnect().await?;
            *self.current_server_address.lock().await = new_leader_address;

            Ok(true)
        } else {
            self.leader_redirection_state.lock().await.reset();
            Ok(false)
        }
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Shutdown {
            return Ok(());
        }

        info!("Shutting down the {NAME} QUIC client.");
        let connection = self.connection.lock().await.take();
        if let Some(connection) = connection {
            connection.close(0u32.into(), b"");
        }

        self.endpoint.wait_idle().await;
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} QUIC client has been shutdown.");
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        if self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        info!(
            "{NAME} client: {} is disconnecting from server...",
            self.config.client_address
        );
        self.set_state(ClientState::Disconnected).await;
        self.connection.lock().await.take();
        self.endpoint.wait_idle().await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        let now = IggyTimestamp::now();
        info!(
            "{NAME} client: {} has disconnected from server at: {now}.",
            self.config.client_address
        );
        Ok(())
    }

    async fn send_raw(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        match self.get_state().await {
            ClientState::Shutdown => {
                trace!("Cannot send data. Client is shutdown.");
                return Err(IggyError::ClientShutdown);
            }
            ClientState::Disconnected => {
                trace!(
                    "Cannot send data. Client: {} is not connected.",
                    self.config.client_address
                );
                return Err(IggyError::NotConnected);
            }
            ClientState::Connecting => {
                trace!(
                    "Cannot send data. Client: {} is still connecting.",
                    self.config.client_address
                );
                return Err(IggyError::NotConnected);
            }
            _ => {}
        }

        let connection = self.connection.clone();
        let response_buffer_size = self.config.response_buffer_size;
        // SAFETY: we run code holding the `connection` lock in a task so we can't be cancelled while holding the lock.
        tokio::spawn(async move {
            let connection = connection.lock().await;
            if let Some(connection) = connection.as_ref() {
                let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;
                let (mut send, mut recv) = connection.open_bi().await.map_err(|error| {
                    error!("Failed to open a bidirectional stream: {error}");
                    IggyError::QuicError
                })?;
                trace!("Sending a QUIC request with code: {code}");
                send.write_all(&(payload_length as u32).to_le_bytes())
                    .await
                    .map_err(|error| {
                        error!("Failed to write payload length: {error}");
                        IggyError::QuicError
                    })?;
                send.write_all(&code.to_le_bytes()).await.map_err(|error| {
                    error!("Failed to write payload code: {error}");
                    IggyError::QuicError
                })?;
                send.write_all(&payload).await.map_err(|error| {
                    error!("Failed to write payload: {error}");
                    IggyError::QuicError
                })?;
                send.finish().map_err(|error| {
                    error!("Failed to finish sending data: {error}");
                    IggyError::QuicError
                })?;
                trace!("Sent a QUIC request with code: {code}, waiting for a response...");
                return QuicClient::handle_response(&mut recv, response_buffer_size as usize).await;
            }

            error!("Cannot send data. Client is not connected.");
            Err(IggyError::NotConnected)
        })
        .await
        .map_err(|e| {
            error!("Task execution failed during QUIC request: {}", e);
            IggyError::QuicError
        })?
    }
}

fn configure(config: &QuicClientConfig) -> Result<ClientConfig, IggyError> {
    let max_concurrent_bidi_streams = VarInt::try_from(config.max_concurrent_bidi_streams);
    if max_concurrent_bidi_streams.is_err() {
        error!(
            "Invalid 'max_concurrent_bidi_streams': {}",
            config.max_concurrent_bidi_streams
        );
        return Err(IggyError::InvalidConfiguration);
    }

    let receive_window = VarInt::try_from(config.receive_window);
    if receive_window.is_err() {
        error!("Invalid 'receive_window': {}", config.receive_window);
        return Err(IggyError::InvalidConfiguration);
    }

    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(receive_window.unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size as usize);
    transport.max_concurrent_bidi_streams(max_concurrent_bidi_streams.unwrap());
    if config.keep_alive_interval > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval)));
    }
    if config.max_idle_timeout > 0 {
        let max_idle_timeout =
            IdleTimeout::try_from(Duration::from_millis(config.max_idle_timeout));
        if max_idle_timeout.is_err() {
            error!("Invalid 'max_idle_timeout': {}", config.max_idle_timeout);
            return Err(IggyError::InvalidConfiguration);
        }
        transport.max_idle_timeout(Some(max_idle_timeout.unwrap()));
    }

    if CryptoProvider::get_default().is_none()
        && let Err(e) = rustls::crypto::ring::default_provider().install_default()
    {
        warn!(
            "Failed to install rustls crypto provider. Error: {:?}. This may be normal if another thread installed it first.",
            e
        );
    }
    let mut client_config = match config.validate_certificate {
        true => ClientConfig::try_with_platform_verifier().map_err(|error| {
            error!("Failed to create QUIC client configuration: {error}");
            IggyError::InvalidConfiguration
        })?,
        false => {
            match QuinnQuicClientConfig::try_from(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            ) {
                Ok(config) => ClientConfig::new(Arc::new(config)),
                Err(error) => {
                    error!("Failed to create QUIC client configuration: {error}");
                    return Err(IggyError::InvalidConfiguration);
                }
            }
        }
    };
    client_config.transport_config(Arc::new(transport));
    Ok(client_config)
}

/// Unit tests for QuicClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for QuicClient.
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_fail_with_empty_connection_string() {
        let value = "";
        let quic_client = QuicClient::from_connection_string(value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_unmatch_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_fail_with_invalid_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?invalid_option=invalid"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_err());
    }

    #[tokio::test]
    async fn should_succeed_without_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            quic_client_config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(quic_client_config.initial_mtu, 1200);
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn should_succeed_with_options() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let initial_mtu = "3000";
        let reconnection_interval = "5s";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}?initial_mtu={initial_mtu}&reconnection_interval={reconnection_interval}"
        );
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            quic_client_config.auto_login,
            AutoLogin::Enabled(Credentials::UsernamePassword(
                username.to_string(),
                password.to_string()
            ))
        );

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(
            quic_client_config.initial_mtu,
            initial_mtu.parse::<u16>().unwrap()
        );
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            IggyDuration::from_str(reconnection_interval).unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn should_succeed_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let quic_client = QuicClient::from_connection_string(&value);
        assert!(quic_client.is_ok());

        let quic_client_config = quic_client.unwrap().config;
        assert_eq!(
            quic_client_config.server_address,
            format!("{server_address}:{port}")
        );
        assert_eq!(
            quic_client_config.auto_login,
            AutoLogin::Enabled(Credentials::PersonalAccessToken(pat.to_string()))
        );

        assert_eq!(quic_client_config.response_buffer_size, 10_000_000);
        assert_eq!(quic_client_config.max_concurrent_bidi_streams, 10_000);
        assert_eq!(quic_client_config.datagram_send_buffer_size, 100_000);
        assert_eq!(quic_client_config.initial_mtu, 1200);
        assert_eq!(quic_client_config.send_window, 100_000);
        assert_eq!(quic_client_config.receive_window, 100_000);
        assert_eq!(quic_client_config.keep_alive_interval, 5000);
        assert_eq!(quic_client_config.max_idle_timeout, 10_000);
        assert!(!quic_client_config.validate_certificate);
        assert_eq!(
            quic_client_config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            quic_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
