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

use crate::leader_aware::{LeaderRedirectionState, check_and_redirect_to_leader};
use crate::prelude::AutoLogin;
#[cfg(feature = "vsr")]
use crate::session::ConsensusSession;
#[cfg(feature = "vsr")]
use iggy_common::VsrSessionControl as _;
use iggy_common::{BinaryClient, BinaryTransport, Client, PersonalAccessTokenClient, UserClient};

use crate::prelude::{IggyDuration, IggyError, IggyTimestamp, QuicClientConfig};
use crate::quic::skip_server_verification::SkipServerVerification;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
#[cfg(feature = "vsr")]
use iggy_binary_protocol::codes::{LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE};
use iggy_common::{
    ClientState, ConnectionString, ConnectionStringUtils, Credentials, DiagnosticEvent,
    QuicConnectionStringOptions, TransportProtocol, validate_server_address,
};
use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, VarInt};
use rustls::crypto::CryptoProvider;
use secrecy::ExposeSecret;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
#[cfg(feature = "vsr")]
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

#[cfg(not(feature = "vsr"))]
const REQUEST_INITIAL_BYTES_LENGTH: usize = 4;
#[cfg(not(feature = "vsr"))]
const RESPONSE_INITIAL_BYTES_LENGTH: usize = 8;
const NAME: &str = "Iggy";

/// Bound on how long a single QUIC request waits for its response, mirroring the
/// TCP client's `RESPONSE_READ_TIMEOUT`. A replicated request that the server
/// cannot commit transiently is answered with silence - the server expects the
/// SDK read-timeout to drive a replay. `RecvStream::read_to_end` on an
/// unanswered bidi stream otherwise blocks until the QUIC idle timeout (which
/// can be minutes), parking that request and, because the connection is held
/// under lock per send, every later request on the client.
const RESPONSE_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-attempt response read timeout for the same-connection transient resend
/// loop (see `send_raw`). A replicated submit the server can't yet commit is
/// answered with silence; rather than wait the full `RESPONSE_READ_TIMEOUT`
/// once, abandon the unanswered bidi after this shorter interval and resend the
/// same request on a fresh stream. Bounded overall by `RESPONSE_READ_TIMEOUT`.
#[cfg(feature = "vsr")]
const TRANSIENT_RETRY_INTERVAL: Duration = Duration::from_secs(3);

/// Backoff before replaying a request the server answered with an explicit
/// `TransientNotCommitted` frame (not-caught-up / in-flight / pipeline-full /
/// view-change cancel). Unlike a silent timeout, this reply arrives promptly, so
/// a short pause keeps the replay from spinning while the primary catches up.
/// Bounded overall by `RESPONSE_READ_TIMEOUT`.
#[cfg(feature = "vsr")]
const NOT_READY_RETRY_INTERVAL: Duration = Duration::from_millis(50);

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
    #[cfg(feature = "vsr")]
    // See `core/sdk/src/tcp/tcp_client.rs` for the `tokio::sync::Mutex` ->
    // `std::sync::Mutex` rationale (pure-CPU critical section).
    consensus_session: Arc<StdMutex<ConsensusSession>>,
    #[cfg(feature = "vsr")]
    skip_auto_login_once: Mutex<bool>,
    #[cfg(feature = "vsr")]
    consumer_group_state: Arc<iggy_common::ConsumerGroupClientState>,
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
                | IggyError::NotConnected
                | IggyError::CannotEstablishConnection
                | IggyError::QuicError
        ) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        #[cfg(feature = "vsr")]
        if matches!(self.config.auto_login, AutoLogin::Disabled) && !is_login_register_code(code) {
            // Without auto-login a reconnect cannot re-establish the session, so
            // non-login requests are not recovered here - their transient replay
            // happens on the live connection inside `send_raw`. Login/register
            // is the exception: the server stays deliberately silent on a
            // transient register failure and relies on the client replaying via
            // a reconnect with a fresh session.
            return Err(error);
        }

        self.disconnect().await?;
        #[cfg(feature = "vsr")]
        let skip_auto_login = is_login_register_code(code);
        #[cfg(feature = "vsr")]
        if skip_auto_login {
            *self.skip_auto_login_once.lock().await = true;
            // The replayed login/register must mint a fresh Register: the failed
            // attempt already consumed the one-shot register request id and may
            // have half-bound the session.
            *self
                .consensus_session
                .lock()
                .expect("consensus session mutex poisoned") = ConsensusSession::new();
        }
        let server_address = self.current_server_address.lock().await.to_string();
        info!(
            "Reconnecting to the server: {}, by client: {}",
            server_address, self.config.client_address
        );
        let reconnect = self.connect().await;
        #[cfg(feature = "vsr")]
        if skip_auto_login && reconnect.is_err() {
            *self.skip_auto_login_once.lock().await = false;
        }
        reconnect?;
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }

    #[cfg(feature = "vsr")]
    fn consumer_group_state(&self) -> Arc<iggy_common::ConsumerGroupClientState> {
        Arc::clone(&self.consumer_group_state)
    }
}

#[cfg(feature = "vsr")]
impl iggy_common::VsrSessionSealed for QuicClient {}

#[cfg(feature = "vsr")]
#[async_trait::async_trait]
impl iggy_common::VsrSessionControl for QuicClient {
    async fn bind_vsr_session(&self, session: u64) -> Result<(), IggyError> {
        if session == 0 {
            return Err(IggyError::InvalidSession(session));
        }

        let mut consensus_session = self
            .consensus_session
            .lock()
            .expect("consensus session mutex poisoned");
        if consensus_session.is_bound() {
            return Err(IggyError::AlreadyAuthenticated);
        }

        consensus_session.bind(session);
        Ok(())
    }

    async fn reset_vsr_session(&self) -> Result<(), IggyError> {
        *self
            .consensus_session
            .lock()
            .expect("consensus session mutex poisoned") = ConsensusSession::new();
        Ok(())
    }

    fn sdk_version(&self) -> &'static str {
        crate::SDK_VERSION
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
        validate_server_address(&config.server_address)?;

        let resolved_addr = config
            .server_address
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next());

        let client_address = if resolved_addr.is_some_and(|a| a.is_ipv6())
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

        let server_address = config.server_address.clone();
        Ok(Self {
            config,
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address),
            #[cfg(feature = "vsr")]
            consensus_session: Arc::new(StdMutex::new(ConsensusSession::new())),
            #[cfg(feature = "vsr")]
            skip_auto_login_once: Mutex::new(false),
            #[cfg(feature = "vsr")]
            consumer_group_state: Arc::new(iggy_common::ConsumerGroupClientState::new()),
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
        read_timeout: Duration,
    ) -> Result<Bytes, IggyError> {
        let buffer = tokio::time::timeout(read_timeout, recv.read_to_end(response_buffer_size))
            .await
            .map_err(|_| {
                error!("Timed out after {read_timeout:?} waiting for QUIC response");
                IggyError::Disconnected
            })?
            .map_err(|error| {
                error!("Failed to read response data: {error}");
                IggyError::QuicError
            })?;
        if buffer.is_empty() {
            return Err(IggyError::EmptyResponse);
        }

        #[cfg(feature = "vsr")]
        {
            crate::vsr::decode_response(Bytes::from(buffer))
        }

        #[cfg(not(feature = "vsr"))]
        {
            let status = u32::from_le_bytes(
                buffer[..4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            if status != 0 {
                error!(
                    "Received an invalid response with status: {} ({}).",
                    status,
                    IggyError::from_code_as_string(status)
                );

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
                &buffer[RESPONSE_INITIAL_BYTES_LENGTH
                    ..RESPONSE_INITIAL_BYTES_LENGTH + length as usize],
            ))
        }
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
                let server_address = tokio::net::lookup_host(&server_address_str)
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to resolve server address '{}': {}",
                            server_address_str, e
                        );
                        IggyError::InvalidServerAddress
                    })?
                    .next()
                    .ok_or_else(|| {
                        error!("No addresses resolved for '{}'", server_address_str);
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

            #[cfg(feature = "vsr")]
            let skip_auto_login = {
                let mut guard = self.skip_auto_login_once.lock().await;
                std::mem::take(&mut *guard)
            };

            // Handle auto-login
            let should_redirect = match &self.config.auto_login {
                AutoLogin::Disabled => {
                    info!("Automatic sign-in is disabled.");
                    false
                }
                AutoLogin::Enabled(credentials) => {
                    #[cfg(feature = "vsr")]
                    if skip_auto_login {
                        info!("Skipping automatic sign-in for a retried login/register request.");
                        false
                    } else {
                        info!(
                            "{NAME} client: {} is signing in...",
                            self.config.client_address
                        );
                        self.set_state(ClientState::Authenticating).await;
                        match credentials {
                            Credentials::UsernamePassword(username, password) => {
                                self.login_user(username, password.expose_secret()).await?;
                                self.publish_event(DiagnosticEvent::SignedIn).await;
                                info!(
                                    "{NAME} client: {} has signed in with the user credentials, username: {username}",
                                    self.config.client_address
                                );
                            }
                            Credentials::PersonalAccessToken(token) => {
                                self.login_with_personal_access_token(token.expose_secret())
                                    .await?;
                                self.publish_event(DiagnosticEvent::SignedIn).await;
                                info!(
                                    "{NAME} client: {} has signed in with a personal access token.",
                                    self.config.client_address
                                );
                            }
                        }

                        self.handle_leader_redirection().await?
                    }
                    #[cfg(not(feature = "vsr"))]
                    {
                        info!(
                            "{NAME} client: {} is signing in...",
                            self.config.client_address
                        );
                        self.set_state(ClientState::Authenticating).await;
                        match credentials {
                            Credentials::UsernamePassword(username, password) => {
                                self.login_user(username, password.expose_secret()).await?;
                                self.publish_event(DiagnosticEvent::SignedIn).await;
                                info!(
                                    "{NAME} client: {} has signed in with the user credentials, username: {username}",
                                    self.config.client_address
                                );
                            }
                            Credentials::PersonalAccessToken(token) => {
                                self.login_with_personal_access_token(token.expose_secret())
                                    .await?;
                                self.publish_event(DiagnosticEvent::SignedIn).await;
                                info!(
                                    "{NAME} client: {} has signed in with a personal access token.",
                                    self.config.client_address
                                );
                            }
                        }

                        self.handle_leader_redirection().await?
                    }
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
        let leader_address = check_and_redirect_to_leader(
            self,
            &current_address,
            iggy_common::TransportProtocol::Quic,
        )
        .await?;

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
        #[cfg(feature = "vsr")]
        self.reset_vsr_session().await?;
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
        #[cfg(feature = "vsr")]
        self.reset_vsr_session().await?;
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
        #[cfg(feature = "vsr")]
        let consensus_session = self.consensus_session.clone();
        // SAFETY: we run code holding the `connection` lock in a task so we can't be cancelled while holding the lock.
        tokio::spawn(async move {
            let connection = connection.lock().await;
            let Some(connection) = connection.as_ref() else {
                error!("Cannot send data. Client is not connected.");
                return Err(IggyError::NotConnected);
            };

            #[cfg(feature = "vsr")]
            {
                let (request_header, request_size) = {
                    let mut consensus_session = consensus_session
                        .lock()
                        .expect("consensus session mutex poisoned");
                    crate::vsr::encode_request_header(&mut consensus_session, code, &payload)?
                };
                trace!("Sending a QUIC VSR request of size {request_size} with code: {code}");
                // Same-connection transient resend. A replicated submit the
                // server cannot yet commit is answered with silence - it expects
                // the client to replay. The connection and login session are
                // intact; only this bidi went unanswered. So abandon it after a
                // short read timeout and resend the SAME request header (same
                // request id, so the server dedups/commits it idempotently) on a
                // fresh bidi, bounded overall by `RESPONSE_READ_TIMEOUT`. This
                // needs no reconnect or relogin, so it recovers transients even
                // with auto-login disabled. Login/register is one-shot (a replay
                // must mint a fresh session), so it is left to the reconnect path
                // in `send_raw_with_response` and not resent here.
                let allow_resend = !is_login_register_code(code);
                let header_bytes = bytemuck::bytes_of(&request_header);
                let deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
                loop {
                    let (mut send, mut recv) = connection.open_bi().await.map_err(|error| {
                        error!("Failed to open a bidirectional stream: {error}");
                        IggyError::QuicError
                    })?;
                    send.write_all(header_bytes).await.map_err(|error| {
                        error!("Failed to write VSR request header: {error}");
                        IggyError::QuicError
                    })?;
                    if !payload.is_empty() {
                        send.write_all(&payload).await.map_err(|error| {
                            error!("Failed to write VSR request payload: {error}");
                            IggyError::QuicError
                        })?;
                    }
                    send.finish().map_err(|error| {
                        error!("Failed to finish VSR request stream: {error}");
                        IggyError::QuicError
                    })?;
                    let remaining =
                        deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(IggyError::Disconnected);
                    }
                    let attempt_timeout = remaining.min(TRANSIENT_RETRY_INTERVAL);
                    match QuicClient::handle_response(
                        &mut recv,
                        response_buffer_size as usize,
                        attempt_timeout,
                    )
                    .await
                    {
                        Ok(reply) => return Ok(reply),
                        // `Disconnected` = our read timed out (server never
                        // answered). `EmptyResponse` = the server abandoned the
                        // bidi (finished it with no body). `TransientNotCommitted`
                        // = the server replied with an explicit retry frame
                        // because it could not commit yet (not-caught-up /
                        // in-flight / pipeline-full / view-change cancel). All
                        // three mean "resend the same request on a fresh bidi";
                        // the session stays intact so no reconnect/relogin needed.
                        // Login/register resends on `TransientNotCommitted` too
                        // (nothing committed -> the same Register replays
                        // idempotently, no fresh session needed); for the silent
                        // paths login still falls through to the reconnect+relogin
                        // path in `send_raw_with_response`.
                        Err(
                            error @ (IggyError::Disconnected
                            | IggyError::EmptyResponse
                            | IggyError::TransientNotCommitted),
                        ) if (allow_resend || error == IggyError::TransientNotCommitted)
                            && tokio::time::Instant::now() < deadline =>
                        {
                            // The explicit frame returns promptly (no read
                            // timeout elapsed), so pace the replay; the silent
                            // paths already waited out `attempt_timeout`.
                            if error == IggyError::TransientNotCommitted {
                                let remaining = deadline
                                    .saturating_duration_since(tokio::time::Instant::now());
                                tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                            }
                            warn!(
                                "QUIC request code {code} not committed (transient); resending on a new stream"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
            }

            #[cfg(not(feature = "vsr"))]
            {
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
                QuicClient::handle_response(
                    &mut recv,
                    response_buffer_size as usize,
                    RESPONSE_READ_TIMEOUT,
                )
                .await
            }
        })
        .await
        .map_err(|e| {
            error!("Task execution failed during QUIC request: {}", e);
            IggyError::QuicError
        })?
    }
}

#[cfg(feature = "vsr")]
const fn is_login_register_code(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
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
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

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
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

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
        match &quic_client_config.auto_login {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(t)) => {
                assert_eq!(t.expose_secret(), pat);
            }
            other => panic!("expected PersonalAccessToken auto_login, got {other:?}"),
        }

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
    async fn should_create_with_hostname_address() {
        let config = QuicClientConfig {
            server_address: "localhost:8080".to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config));
        assert!(client.is_ok(), "Expected Ok, got: {:?}", client.err());
    }

    #[tokio::test]
    async fn should_create_with_fqdn_address() {
        let config = QuicClientConfig {
            server_address: "my-server.example.com:8080".to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config));
        assert!(client.is_ok(), "Expected Ok, got: {:?}", client.err());
    }

    #[tokio::test]
    async fn should_store_raw_hostname_in_current_server_address() {
        let hostname = "localhost:8080";
        let config = QuicClientConfig {
            server_address: hostname.to_string(),
            ..Default::default()
        };
        let client = QuicClient::create(Arc::new(config)).unwrap();
        let stored = client.current_server_address.lock().await;
        assert_eq!(*stored, hostname);
    }

    #[tokio::test]
    async fn should_succeed_from_connection_string_with_hostname() {
        let connection_string = "iggy+quic://user:secret@localhost:1234";
        let client = QuicClient::from_connection_string(connection_string);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:1234");
    }

    #[test]
    fn should_fail_create_with_invalid_server_address_even_without_builder() {
        let config = Arc::new(QuicClientConfig {
            server_address: "127.0.0.1".to_string(),
            ..Default::default()
        });

        let client = QuicClient::create(config);
        assert!(client.is_err());
    }
}
