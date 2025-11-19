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
use crate::websocket::websocket_connection_stream::WebSocketConnectionStream;
use crate::websocket::websocket_stream_kind::WebSocketStreamKind;
use crate::websocket::websocket_tls_connection_stream::WebSocketTlsConnectionStream;
use rustls::{ClientConfig, pki_types::pem::PemObject};

use crate::prelude::Client;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use iggy_common::{
    AutoLogin, ClientState, Command, ConnectionString, Credentials, DiagnosticEvent, IggyDuration,
    IggyError, IggyErrorDiscriminants, IggyTimestamp, WebSocketClientConfig,
    WebSocketConnectionStringOptions,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{
    Connector, client_async_with_config, connect_async_tls_with_config,
    tungstenite::client::IntoClientRequest,
};
use tracing::{debug, error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "WebSocket";

#[derive(Debug)]
pub struct WebSocketClient {
    stream: Arc<Mutex<Option<WebSocketStreamKind>>>,
    pub(crate) config: Arc<WebSocketClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    pub(crate) connected_at: Mutex<Option<IggyTimestamp>>,
    leader_redirection_state: Mutex<LeaderRedirectionState>,
    pub(crate) current_server_address: Mutex<String>,
}

impl Default for WebSocketClient {
    fn default() -> Self {
        WebSocketClient::create(Arc::new(WebSocketClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for WebSocketClient {
    async fn connect(&self) -> Result<(), IggyError> {
        WebSocketClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        WebSocketClient::disconnect(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        WebSocketClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for WebSocketClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a {} diagnostic event: {error}", NAME);
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

impl BinaryClient for WebSocketClient {}

impl WebSocketClient {
    /// Create a new WebSocket client with the provided configuration.
    pub fn create(config: Arc<WebSocketClientConfig>) -> Result<Self, IggyError> {
        let (sender, receiver) = broadcast(1000);
        let server_address = config.server_address.clone();
        Ok(WebSocketClient {
            stream: Arc::new(Mutex::new(None)),
            config,
            state: Mutex::new(ClientState::Disconnected),
            client_address: Mutex::new(None),
            events: (sender, receiver),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address),
        })
    }

    /// Create a new WebSocket client from a connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        let parsed_connection_string =
            ConnectionString::<WebSocketConnectionStringOptions>::new(connection_string)?;
        let config = WebSocketClientConfig::from(parsed_connection_string);
        Self::create(Arc::new(config))
    }

    async fn get_client_address_value(&self) -> String {
        let client_address = self.client_address.lock().await;
        match client_address.as_ref() {
            Some(address) => address.to_string(),
            None => "unknown".to_string(),
        }
    }

    async fn connect(&self) -> Result<(), IggyError> {
        loop {
            if self.get_state().await == ClientState::Connected {
                return Ok(());
            }

            let mut retry_count = 0;

            loop {
                let current_address = self.current_server_address.lock().await.clone();
                let protocol = if self.config.tls_enabled { "wss" } else { "ws" };
                info!(
                    "{NAME} client is connecting to server: {}://{}...",
                    protocol, current_address
                );
                self.set_state(ClientState::Connecting).await;

                if retry_count > 0 {
                    let elapsed = self
                        .connected_at
                        .lock()
                        .await
                        .map(|ts| IggyTimestamp::now().as_micros() - ts.as_micros())
                        .unwrap_or(0);

                    let interval = self.config.reconnection.reestablish_after.as_micros();
                    debug!("Elapsed time since last connection: {}μs", elapsed);

                    if elapsed < interval {
                        let remaining =
                            IggyDuration::new(std::time::Duration::from_micros(interval - elapsed));
                        info!("Trying to connect to the server in: {remaining}");
                        sleep(remaining.get_duration()).await;
                    }
                }

                let server_addr = current_address.parse::<SocketAddr>().map_err(|_| {
                    error!("Invalid server address: {}", current_address);
                    IggyError::InvalidConfiguration
                })?;

                let connection_stream = if self.config.tls_enabled {
                    match self.connect_tls(server_addr, &mut retry_count).await {
                        Ok(stream) => stream,
                        Err(IggyError::CannotEstablishConnection) => {
                            return Err(IggyError::CannotEstablishConnection);
                        }
                        Err(_) => continue, // retry
                    }
                } else {
                    match self.connect_plain(server_addr, &mut retry_count).await {
                        Ok(stream) => stream,
                        Err(IggyError::CannotEstablishConnection) => {
                            return Err(IggyError::CannotEstablishConnection);
                        }
                        Err(_) => continue, // retry
                    }
                };

                *self.stream.lock().await = Some(connection_stream);
                *self.client_address.lock().await = Some(server_addr);
                self.set_state(ClientState::Connected).await;
                *self.connected_at.lock().await = Some(IggyTimestamp::now());
                self.publish_event(DiagnosticEvent::Connected).await;

                let now = IggyTimestamp::now();
                info!(
                    "{NAME} client has connected to server: {} at: {now}",
                    server_addr
                );

                break;
            }

            if !self.check_and_maybe_redirect().await? {
                return Ok(());
            }
        }
    }

    async fn connect_plain(
        &self,
        server_addr: SocketAddr,
        retry_count: &mut u32,
    ) -> Result<WebSocketStreamKind, IggyError> {
        let tcp_stream = match TcpStream::connect(&server_addr).await {
            Ok(stream) => stream,
            Err(error) => {
                error!(
                    "Failed to connect to server: {}. Error: {}",
                    self.config.server_address, error
                );
                return self.handle_connection_error(retry_count).await;
            }
        };

        let ws_url = format!("ws://{}", server_addr);
        let request = ws_url.into_client_request().map_err(|e| {
            error!("Failed to create WebSocket request: {}", e);
            IggyError::InvalidConfiguration
        })?;

        let tungstenite_config = self.config.ws_config.to_tungstenite_config();

        let (websocket_stream, response) =
            match client_async_with_config(request, tcp_stream, Some(tungstenite_config)).await {
                Ok(result) => result,
                Err(error) => {
                    error!("WebSocket handshake failed: {}", error);
                    return self.handle_connection_error(retry_count).await;
                }
            };

        debug!(
            "WebSocket connection established. Response status: {}",
            response.status()
        );

        let connection_stream = WebSocketConnectionStream::new(server_addr, websocket_stream);
        Ok(WebSocketStreamKind::Plain(connection_stream))
    }

    async fn connect_tls(
        &self,
        server_addr: SocketAddr,
        retry_count: &mut u32,
    ) -> Result<WebSocketStreamKind, IggyError> {
        let tls_config = self.build_tls_config()?;
        let connector = Connector::Rustls(Arc::new(tls_config));

        let domain = if !self.config.tls_domain.is_empty() {
            self.config.tls_domain.clone()
        } else {
            server_addr.ip().to_string()
        };

        let ws_url = format!("wss://{}:{}", domain, server_addr.port());
        let tungstenite_config = self.config.ws_config.to_tungstenite_config();

        debug!("Initiating WebSocket TLS connection to: {}", ws_url);
        let (websocket_stream, response) = match connect_async_tls_with_config(
            ws_url,
            Some(tungstenite_config),
            false,
            Some(connector),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                error!("WebSocket TLS handshake failed: {}", error);
                return self.handle_connection_error(retry_count).await;
            }
        };

        debug!(
            "WebSocket TLS connection established. Response status: {}",
            response.status()
        );

        let connection_stream = WebSocketTlsConnectionStream::new(server_addr, websocket_stream);
        Ok(WebSocketStreamKind::Tls(connection_stream))
    }

    fn build_tls_config(&self) -> Result<ClientConfig, IggyError> {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        }

        let config = if self.config.tls_validate_certificate {
            let mut root_cert_store = rustls::RootCertStore::empty();

            if let Some(certificate_path) = &self.config.tls_ca_file {
                // load CA certificates from file
                for cert in rustls::pki_types::CertificateDer::pem_file_iter(certificate_path)
                    .map_err(|error| {
                        error!("Failed to read the CA file: {certificate_path}. {error}");
                        IggyError::InvalidTlsCertificatePath
                    })?
                {
                    let certificate = cert.map_err(|error| {
                        error!("Failed to read a certificate from the CA file: {certificate_path}. {error}");
                        IggyError::InvalidTlsCertificate
                    })?;
                    root_cert_store.add(certificate).map_err(|error| {
                        error!(
                            "Failed to add a certificate to the root certificate store. {error}"
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
            // skip certificate validation (development/self-signed certs)
            use crate::tcp::tcp_tls_verifier::NoServerVerification;
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoServerVerification))
                .with_no_client_auth()
        };

        Ok(config)
    }

    async fn handle_connection_error<T>(&self, retry_count: &mut u32) -> Result<T, IggyError> {
        if !self.config.reconnection.enabled {
            warn!("Automatic reconnection is disabled.");
            return Err(IggyError::CannotEstablishConnection);
        }

        let unlimited_retries = self.config.reconnection.max_retries.is_none();
        let max_retries = self.config.reconnection.max_retries.unwrap_or_default();
        let max_retries_str = self
            .config
            .reconnection
            .max_retries
            .map(|r| r.to_string())
            .unwrap_or_else(|| "unlimited".to_string());

        let interval_str = self.config.reconnection.interval.as_human_time_string();

        if unlimited_retries || *retry_count < max_retries {
            *retry_count += 1;
            info!(
                "Retrying to connect to server ({}/{}): {} in: {}",
                retry_count, max_retries_str, self.config.server_address, interval_str
            );
            sleep(self.config.reconnection.interval.get_duration()).await;
            return Err(IggyError::Disconnected); // signal to retry
        }

        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Err(IggyError::CannotEstablishConnection)
    }

    async fn check_and_maybe_redirect(&self) -> Result<bool, IggyError> {
        match &self.config.auto_login {
            AutoLogin::Disabled => Ok(false),
            AutoLogin::Enabled(_) => {
                self.auto_login().await?;
                self.handle_leader_redirection().await
            }
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
                warn!("Maximum leader redirections reached for WebSocket client");
                return Ok(false);
            }

            redirection_state.increment_redirect(new_leader_address.clone());
            drop(redirection_state);

            info!(
                "WebSocket client redirecting to leader at: {}",
                new_leader_address
            );
            self.connected_at.lock().await.take();
            self.disconnect().await?;
            *self.current_server_address.lock().await = new_leader_address;
            Ok(true)
        } else {
            self.leader_redirection_state.lock().await.reset();
            Ok(false)
        }
    }

    async fn auto_login(&self) -> Result<(), IggyError> {
        let client_address = self.get_client_address_value().await;
        match &self.config.auto_login {
            AutoLogin::Disabled => {
                info!("{NAME} client: {client_address} - automatic sign-in is disabled.");
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
        info!("Shutting down the {NAME} client: {client_address}");

        self.set_state(ClientState::Disconnected).await;

        let stream = self.stream.lock().await.take();
        if let Some(mut stream) = stream {
            let _ = stream.shutdown().await;
        }

        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} client: {client_address} has been shutdown.");
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
            ClientState::Connected | ClientState::Authenticating | ClientState::Authenticated => {}
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| {
            trace!("Cannot send data. Client is not connected.");
            IggyError::NotConnected
        })?;

        let payload_length = payload.len() + REQUEST_INITIAL_BYTES_LENGTH;
        let mut request = BytesMut::with_capacity(4 + REQUEST_INITIAL_BYTES_LENGTH + payload.len());
        request.put_u32_le(payload_length as u32);
        request.put_u32_le(code);
        request.put_slice(&payload);

        trace!(
            "Sending {NAME} message with code: {}, payload size: {} bytes",
            code,
            payload.len()
        );

        stream.write(&request).await?;
        stream.flush().await?;

        let mut response_initial_buffer = vec![0u8; RESPONSE_INITIAL_BYTES_LENGTH];
        stream.read(&mut response_initial_buffer).await?;

        let status = u32::from_le_bytes([
            response_initial_buffer[0],
            response_initial_buffer[1],
            response_initial_buffer[2],
            response_initial_buffer[3],
        ]);

        let length = u32::from_le_bytes([
            response_initial_buffer[4],
            response_initial_buffer[5],
            response_initial_buffer[6],
            response_initial_buffer[7],
        ]) as usize;

        trace!(
            "Received {NAME} response status: {}, length: {} bytes",
            status, length
        );

        if status != 0 {
            // TEMP: See https://github.com/apache/iggy/pull/604 for context.
            if status == IggyErrorDiscriminants::TopicNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::StreamNameAlreadyExists as u32
                || status == IggyErrorDiscriminants::UserAlreadyExists as u32
                || status == IggyErrorDiscriminants::PersonalAccessTokenAlreadyExists as u32
                || status == IggyErrorDiscriminants::ConsumerGroupNameAlreadyExists as u32
            {
                debug!(
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

        if length == 0 {
            return Ok(Bytes::new());
        }

        let mut response_buffer = vec![0u8; length];
        stream.read(&mut response_buffer).await?;

        trace!("Received {NAME} response payload, size: {} bytes", length);
        Ok(Bytes::from(response_buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn should_be_created_with_default_config() {
        let client = WebSocketClient::default();
        assert_eq!(client.config.server_address, "127.0.0.1:8092");
        assert_eq!(
            client.config.heartbeat_interval,
            IggyDuration::from_str("5s").unwrap()
        );
        assert!(matches!(client.config.auto_login, AutoLogin::Disabled));
        assert!(client.config.reconnection.enabled);
    }

    #[tokio::test]
    async fn should_be_disconnected_by_default() {
        let client = WebSocketClient::default();
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    #[test]
    fn should_succeed_from_connection_string() {
        let connection_string = "iggy+ws://user:secret@127.0.0.1:8092";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_ok());
    }

    #[test]
    fn should_create_with_custom_config() {
        let config = WebSocketClientConfig {
            server_address: "localhost:9090".to_string(),
            heartbeat_interval: IggyDuration::from_str("10s").unwrap(),
            ..Default::default()
        };

        let client = WebSocketClient::create(Arc::new(config));
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:9090");
        assert_eq!(
            client.config.heartbeat_interval,
            IggyDuration::from_str("10s").unwrap()
        );
    }

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = WebSocketClient::from_connection_string(value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string = "iggy+ws://:secret@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string = "iggy+ws://user:@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string = "iggy+ws://user:secret@:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string = "iggy+ws://user:secret@127.0.0.1:8080?invalid_option=invalid";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }
}
