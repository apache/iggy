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

use crate::websocket::websocket_connection_stream::WebSocketConnectionStream;
use crate::{prelude::Client, websocket::websocket_stream::ConnectionStream};
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use iggy_common::{
    AutoLogin, ClientState, Command, ConnectionString, Credentials, DiagnosticEvent, IggyDuration,
    IggyError, IggyTimestamp, WebSocketClientConfig, WebSocketConnectionStringOptions,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{client_async_with_config, tungstenite::client::IntoClientRequest};
use tracing::{debug, error, info, trace, warn};

const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "WebSocket";

#[derive(Debug)]
pub struct WebSocketClient {
    stream: Arc<Mutex<Option<WebSocketConnectionStream>>>,
    pub(crate) config: Arc<WebSocketClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    connected_at: Mutex<Option<IggyTimestamp>>,
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
        Ok(WebSocketClient {
            stream: Arc::new(Mutex::new(None)),
            config,
            state: Mutex::new(ClientState::Disconnected),
            client_address: Mutex::new(None),
            events: (sender, receiver),
            connected_at: Mutex::new(None),
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
        if self.get_state().await == ClientState::Connected {
            return Ok(());
        }

        let mut retry_count = 0;

        loop {
            info!(
                "{NAME} client is connecting to server: {}...",
                self.config.server_address
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

            let server_addr = self
                .config
                .server_address
                .parse::<SocketAddr>()
                .map_err(|_| {
                    error!("Invalid server address: {}", self.config.server_address);
                    IggyError::InvalidConfiguration
                })?;

            let tcp_stream = match TcpStream::connect(&server_addr).await {
                Ok(stream) => stream,
                Err(error) => {
                    error!(
                        "Failed to connect to server: {}. Error: {}",
                        self.config.server_address, error
                    );

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
            };

            let ws_url = format!("ws://{}", server_addr);

            let request = ws_url.into_client_request().map_err(|e| {
                error!("Failed to create WebSocket request: {}", e);
                IggyError::InvalidConfiguration
            })?;

            let tungstenite_config = self.config.ws_config.to_tungstenite_config();

            let (websocket_stream, response) =
                match client_async_with_config(request, tcp_stream, tungstenite_config).await {
                    Ok(result) => result,
                    Err(error) => {
                        error!("WebSocket handshake failed: {}", error);

                        if !self.config.reconnection.enabled {
                            return Err(IggyError::WebSocketConnectionError);
                        }

                        let unlimited_retries = self.config.reconnection.max_retries.is_none();
                        let max_retries = self.config.reconnection.max_retries.unwrap_or_default();

                        if unlimited_retries || retry_count < max_retries {
                            retry_count += 1;
                            sleep(self.config.reconnection.interval.get_duration()).await;
                            continue;
                        }

                        return Err(IggyError::WebSocketConnectionError);
                    }
                };

            debug!(
                "WebSocket connection established. Response status: {}",
                response.status()
            );

            let connection_stream = WebSocketConnectionStream::new(server_addr, websocket_stream);

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

            self.auto_login().await?;
            return Ok(());
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

        if let Some(mut stream) = self.stream.lock().await.take() {
            let _ = stream.shutdown().await;
        }

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

        self.disconnect().await?;
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
            if length > 0 {
                let mut error_buffer = vec![0u8; length];
                stream.read(&mut error_buffer).await?;
                let _error_message = String::from_utf8(error_buffer).unwrap_or_default();
                // FIXME: add error message
                return Err(IggyError::Error);
            }
            // FIXME: add error message
            return Err(IggyError::Error);
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
        assert_eq!(client.config.server_address, "127.0.0.1:8080");
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
        let connection_string = "iggy+websocket://user:secret@127.0.0.1:8080";
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
        let connection_string = "iggy+websocket://:secret@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string = "iggy+websocket://user:@127.0.0.1:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string = "iggy+websocket://user:secret@:8080";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_options() {
        let connection_string =
            "iggy+websocket://user:secret@127.0.0.1:8080?invalid_option=invalid";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_err());
    }
}
