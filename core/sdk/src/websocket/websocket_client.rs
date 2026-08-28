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

use crate::leader_aware::{
    ConnectCoordinator, ConnectOwnerContext, LeaderRedirectionState, RosterWalk,
    check_and_redirect_to_leader, is_unauthenticated_metadata_probe,
};
use crate::session::ConsensusSession;
use crate::vsr::replay_after_session_reset_is_safe;
use crate::websocket::websocket_connection_stream::WebSocketConnectionStream;
use crate::websocket::websocket_stream_kind::WebSocketStreamKind;
use crate::websocket::websocket_tls_connection_stream::WebSocketTlsConnectionStream;
use rustls::{ClientConfig, pki_types::pem::PemObject};

use crate::prelude::Client;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
};
use iggy_common::VsrSessionControl as _;
use iggy_common::{
    AutoLogin, ClientState, ConnectionString, Credentials, DiagnosticEvent, IggyDuration,
    IggyError, IggyTimestamp, NonZeroIggyDuration, WebSocketClientConfig,
    WebSocketConnectionStringOptions,
};
use iggy_common::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use secrecy::ExposeSecret;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_tungstenite::{
    Connector, client_async_tls_with_config, client_async_with_config,
    tungstenite::client::IntoClientRequest,
};
use tracing::{debug, error, info, trace, warn};

const NAME: &str = "WebSocket";
/// Bound on how long a single VSR reply read may block. The connection is
/// lockstep and the read runs in the caller's task while holding the stream
/// lock, so an unanswered read (lost server reply) would wedge every later
/// request on this client forever. On expiry the stream is dropped.
const RESPONSE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Backoff before replaying a request the server answered with an explicit
/// `TransientNotCommitted` frame (not-caught-up / in-flight / pipeline-full /
/// view-change cancel). The reply arrives promptly, so a short pause keeps the
/// replay from spinning while the primary catches up. Bounded by
/// `RESPONSE_READ_TIMEOUT`.
const NOT_READY_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// How long a request replays `TransientNotAccepted` on the SAME connection
/// before it is handed back for a leader recheck or a roster walk. A node
/// that is not the target group's primary refuses forever, so replaying on it
/// for the whole request budget would burn the budget against a verdict.
const TRANSIENT_FAILOVER_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

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
    // See `core/sdk/src/tcp/tcp_client.rs` for the `tokio::sync::Mutex` ->
    // `std::sync::Mutex` rationale (pure-CPU critical section).
    consensus_session: Arc<StdMutex<ConsensusSession>>,
    skip_auto_login_once: Mutex<bool>,
    /// Every endpoint the cluster roster named on the last leader check, kept
    /// as walk candidates for a request the current node keeps refusing to
    /// admit (its replica of the target partition group is not the primary).
    roster_endpoints: Mutex<Vec<String>>,
    /// Serializes leader checks and roster walks after refused requests, so
    /// concurrent callers cannot tear down each other's new connection.
    routing_lock: Mutex<()>,
    connect_coordinator: ConnectCoordinator,
    consumer_group_state: Arc<iggy_common::ConsumerGroupClientState>,
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

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        let roster_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
        let mut result = self.send_raw(code, payload.clone()).await;

        // A persistent not-admitted refusal is a verdict about who leads the
        // TARGET group, which the metadata leader check alone cannot repair:
        // metadata and partition consensus groups elect independently. Recheck
        // the leader once, then walk the roster, one visit per endpoint. Only
        // recoverable with a session to re-establish, hence the auto-login
        // gate, and a login/register replay stays on its own connection.
        if matches!(result, Err(IggyError::TransientNotAccepted))
            && !is_login_register_code(code)
            && code != GET_CLUSTER_METADATA_CODE
            && self.config.reconnection.enabled
            && !matches!(self.config.auto_login, AutoLogin::Disabled)
        {
            let _routing_guard =
                match tokio::time::timeout_at(roster_deadline, self.routing_lock.lock()).await {
                    Ok(guard) => guard,
                    Err(_) => return Err(IggyError::TransientNotAccepted),
                };
            let overall_deadline = roster_deadline;
            // A concurrent refused request may have completed the movement
            // while this request waited for the gate.
            result = match tokio::time::timeout_at(
                overall_deadline,
                self.send_raw(code, payload.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(IggyError::TransientNotAccepted),
            };
            let mut roster_walk: Option<RosterWalk> = None;
            // Once the walk starts it keeps walking: a leader recheck between
            // hops would put the request straight back on the node whose
            // partition replica refused it.
            let mut checked_metadata_leader = false;
            while matches!(result, Err(IggyError::TransientNotAccepted)) {
                let current = self.current_server_address.lock().await.clone();
                let redirected = if checked_metadata_leader {
                    false
                } else {
                    checked_metadata_leader = true;
                    let redirected = match tokio::time::timeout_at(
                        overall_deadline,
                        self.handle_leader_redirection(),
                    )
                    .await
                    {
                        Ok(result) => matches!(result, Ok(true)),
                        Err(_) => return Err(IggyError::TransientNotAccepted),
                    };
                    let roster = self.roster_endpoints.lock().await.clone();
                    roster_walk = Some(RosterWalk::new(&current, &roster));
                    redirected
                };
                let (mut target, mut needs_settle) = if redirected {
                    let target = self.current_server_address.lock().await.clone();
                    if let Some(walk) = roster_walk.as_mut() {
                        walk.record_attempt(&target);
                    }
                    (target, false)
                } else if let Some(next) = roster_walk.as_mut().and_then(RosterWalk::next) {
                    (next, true)
                } else {
                    break;
                };

                loop {
                    if tokio::time::Instant::now() >= overall_deadline {
                        return Err(IggyError::TransientNotAccepted);
                    }
                    let settled = if needs_settle {
                        match tokio::time::timeout_at(
                            overall_deadline,
                            self.settle_on_endpoint(target.clone()),
                        )
                        .await
                        {
                            Ok(Ok(())) => true,
                            Ok(Err(IggyError::CannotEstablishConnection)) => false,
                            Ok(Err(error)) => return Err(error),
                            Err(_) => return Err(IggyError::TransientNotAccepted),
                        }
                    } else {
                        true
                    };
                    if settled {
                        let connect_result = if needs_settle {
                            tokio::time::timeout_at(overall_deadline, self.connect_off_leader())
                                .await
                        } else {
                            tokio::time::timeout_at(overall_deadline, self.connect()).await
                        };
                        match connect_result {
                            Ok(Ok(())) => {
                                let connected = self.current_server_address.lock().await.clone();
                                let first_visit = roster_walk
                                    .as_mut()
                                    .is_some_and(|walk| walk.record_attempt(&connected));
                                if crate::leader_aware::is_same_spelling(&connected, &target)
                                    || first_visit
                                {
                                    break;
                                }
                            }
                            Ok(Err(IggyError::CannotEstablishConnection)) => {}
                            Ok(Err(error)) => return Err(error),
                            Err(_) => return Err(IggyError::TransientNotAccepted),
                        }
                    }

                    let Some(next) = roster_walk.as_mut().and_then(RosterWalk::next) else {
                        return Err(IggyError::TransientNotAccepted);
                    };
                    target = next;
                    needs_settle = true;
                }
                result = match tokio::time::timeout_at(
                    overall_deadline,
                    self.send_raw(code, payload.clone()),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(IggyError::TransientNotAccepted),
                };
            }
        }

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
                | IggyError::TcpError
                | IggyError::ConnectionClosed
                | IggyError::WebSocketSendError
                | IggyError::WebSocketReceiveError
        ) {
            return Err(error);
        }

        if is_unauthenticated_metadata_probe(code, &error) {
            return Err(error);
        }

        if code == GET_CLUSTER_METADATA_CODE {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        if matches!(self.config.auto_login, AutoLogin::Disabled) && !is_login_register_code(code) {
            return Err(error);
        }

        let replay_after_reconnect = replay_after_session_reset_is_safe(code, &error);
        let skip_auto_login = is_login_register_code(code);
        let owner_context = skip_auto_login
            .then(|| self.connect_coordinator.current_owner_context())
            .flatten();
        let nested_connect = owner_context.is_some();
        let _routing_guard = if nested_connect {
            None
        } else {
            Some(self.routing_lock.lock().await)
        };
        if !nested_connect && self.connect_coordinator.is_active() {
            self.connect().await?;
            if !replay_after_reconnect {
                return Err(error);
            }
            return self.send_raw(code, payload).await;
        }
        self.disconnect().await?;

        if skip_auto_login {
            *self.skip_auto_login_once.lock().await = true;
        }

        {
            let client_address = self.get_client_address_value().await;
            info!(
                "Reconnecting to the server: {} by client: {client_address}...",
                self.config.server_address
            );
        }

        let reconnect = if nested_connect {
            self.connect_inner(owner_context.expect("owner context checked above"))
                .await
        } else {
            self.connect().await
        };
        if skip_auto_login && reconnect.is_err() {
            *self.skip_auto_login_once.lock().await = false;
        }
        reconnect?;
        if !replay_after_reconnect {
            warn!(
                "Reconnected, but command: {code} may have committed before its reply was lost; \
                 replaying it under the new session could apply it twice."
            );
            return Err(error);
        }
        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.config.heartbeat_interval
    }

    fn consumer_group_state(&self) -> Arc<iggy_common::ConsumerGroupClientState> {
        Arc::clone(&self.consumer_group_state)
    }
}

impl iggy_common::VsrSessionSealed for WebSocketClient {}

#[async_trait::async_trait]
impl iggy_common::VsrSessionControl for WebSocketClient {
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

impl BinaryClient for WebSocketClient {}

impl WebSocketClient {
    /// Whether an `AutoLogin` is configured on this client, which makes the
    /// session after any connect the configured user's rather than whoever
    /// signed in by hand.
    pub(crate) fn auto_login_configured(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_))
    }
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
            consensus_session: Arc::new(StdMutex::new(ConsensusSession::new())),
            skip_auto_login_once: Mutex::new(false),
            roster_endpoints: Mutex::new(Vec::new()),
            routing_lock: Mutex::new(()),
            connect_coordinator: ConnectCoordinator::new(),
            consumer_group_state: Arc::new(iggy_common::ConsumerGroupClientState::new()),
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
        self.connect_with_settlement(false).await
    }

    pub(crate) async fn connect_off_leader(&self) -> Result<(), IggyError> {
        self.connect_with_settlement(true).await
    }

    async fn connect_with_settlement(&self, settle_off_leader: bool) -> Result<(), IggyError> {
        self.connect_coordinator
            .run(|abandoned, token| async move {
                let context = self.connect_coordinator.owner_context(
                    token,
                    settle_off_leader,
                    settle_off_leader,
                );
                self.connect_coordinator
                    .scope_owner(context, async move {
                        if abandoned {
                            self.clear_abandoned_connect().await?;
                        }
                        self.connect_inner(context).await
                    })
                    .await
            })
            .await
    }

    async fn connect_inner(&self, context: ConnectOwnerContext) -> Result<(), IggyError> {
        let settle_off_leader = context.settle_off_leader();
        let single_attempt = context.single_attempt();
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

                let server_addr = tokio::net::lookup_host(&*current_address)
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to resolve server address '{}': {}",
                            current_address, e
                        );
                        IggyError::InvalidConfiguration
                    })?
                    .next()
                    .ok_or_else(|| {
                        error!("No addresses resolved for '{}'", current_address);
                        IggyError::InvalidConfiguration
                    })?;

                let connection_stream = if self.config.tls_enabled {
                    match self
                        .connect_tls(server_addr, &mut retry_count, single_attempt)
                        .await
                    {
                        Ok(stream) => stream,
                        Err(IggyError::CannotEstablishConnection) => {
                            return Err(IggyError::CannotEstablishConnection);
                        }
                        Err(_) => continue, // retry
                    }
                } else {
                    match self
                        .connect_plain(server_addr, &mut retry_count, single_attempt)
                        .await
                    {
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

            if !self.check_and_maybe_redirect(settle_off_leader).await? {
                return Ok(());
            }
        }
    }

    async fn clear_abandoned_connect(&self) -> Result<(), IggyError> {
        self.stream.lock().await.take();
        self.reset_vsr_session().await?;
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Ok(())
    }

    async fn connect_plain(
        &self,
        server_addr: SocketAddr,
        retry_count: &mut u32,
        single_attempt: bool,
    ) -> Result<WebSocketStreamKind, IggyError> {
        let tcp_stream = match TcpStream::connect(&server_addr).await {
            Ok(stream) => stream,
            Err(error) => {
                error!(
                    "Failed to connect to server: {}. Error: {}",
                    self.config.server_address, error
                );
                return self
                    .handle_connection_error(retry_count, single_attempt)
                    .await;
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
                    return self
                        .handle_connection_error(retry_count, single_attempt)
                        .await;
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
        single_attempt: bool,
    ) -> Result<WebSocketStreamKind, IggyError> {
        let tcp_stream = match TcpStream::connect(server_addr).await {
            Ok(stream) => stream,
            Err(error) => {
                error!("Failed to connect to server: {server_addr}. Error: {error}");
                return self
                    .handle_connection_error(retry_count, single_attempt)
                    .await;
            }
        };
        let tls_config = self.build_tls_config()?;
        let connector = Connector::Rustls(Arc::new(tls_config));

        let domain = if !self.config.tls_domain.is_empty() {
            self.config.tls_domain.clone()
        } else {
            server_addr.ip().to_string()
        };

        let uri_domain = if domain.contains(':') && !domain.starts_with('[') {
            format!("[{domain}]")
        } else {
            domain
        };
        let ws_url = format!("wss://{}:{}", uri_domain, server_addr.port());
        let tungstenite_config = self.config.ws_config.to_tungstenite_config();

        debug!("Initiating WebSocket TLS connection to: {}", ws_url);
        let (websocket_stream, response) = match client_async_tls_with_config(
            ws_url,
            tcp_stream,
            Some(tungstenite_config),
            Some(connector),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                error!("WebSocket TLS handshake failed: {}", error);
                return self
                    .handle_connection_error(retry_count, single_attempt)
                    .await;
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

    async fn handle_connection_error<T>(
        &self,
        retry_count: &mut u32,
        single_attempt: bool,
    ) -> Result<T, IggyError> {
        if single_attempt {
            self.set_state(ClientState::Disconnected).await;
            self.publish_event(DiagnosticEvent::Disconnected).await;
            return Err(IggyError::CannotEstablishConnection);
        }
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

    async fn check_and_maybe_redirect(&self, settle_off_leader: bool) -> Result<bool, IggyError> {
        match &self.config.auto_login {
            // Only `IggyClient` redirects after a manual sign-in, so a raw
            // transport can stay on a backup, and nothing on the send path
            // redirects either: its replicated writes replay on the live
            // connection, then surface the transient failure to the caller.
            AutoLogin::Disabled => Ok(false),
            AutoLogin::Enabled(_) => {
                self.auto_login().await?;
                // A roster walk stays on the endpoint it dialed: the leader
                // settlement below would put the connection straight back on
                // the node whose partition replica keeps refusing the request.
                // One connect only.
                if settle_off_leader {
                    info!("{NAME} client stays on the dialed node for a partition failover.");
                    return Ok(false);
                }
                // The sole leader settlement, and it runs authenticated. Any
                // node completes a login now -- a backup forwards the register
                // to the primary -- so this decides where later ops land, not
                // whether sign-in works.
                self.handle_leader_redirection().await
            }
        }
    }

    /// Checks cluster metadata and handles leader redirection if needed.
    /// Returns true if redirection occurred and reconnection is needed.
    pub(crate) async fn handle_leader_redirection(&self) -> Result<bool, IggyError> {
        let current_address = self.current_server_address.lock().await.clone();
        let leader_check = check_and_redirect_to_leader(
            self,
            &current_address,
            iggy_common::TransportProtocol::WebSocket,
        )
        .await?;
        // Replaced wholesale rather than merged: the roster is the cluster's
        // own answer about where its nodes are. Kept for the roster walk a
        // persistently refused request runs, not for dead-node redial (which
        // remains TCP-only).
        if !leader_check.endpoints.is_empty() {
            *self.roster_endpoints.lock().await = leader_check.endpoints;
        }
        let leader_address = leader_check.redirect;

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

    /// Move the connection to the roster endpoint after the current one, for
    /// a request the current node keeps refusing to admit. See the TCP twin:
    /// metadata and partition consensus groups elect independently, so the
    /// metadata leader can hold a follower replica of the target partition,
    /// and only walking the roster reaches that group's primary.
    async fn settle_on_endpoint(&self, next: String) -> Result<(), IggyError> {
        let current = self.current_server_address.lock().await.clone();

        info!(
            "The request keeps being refused on {current} while the roster names it the \
             metadata leader; trying the next cluster node at {next}."
        );
        self.connected_at.lock().await.take();
        self.disconnect().await?;
        *self.current_server_address.lock().await = next;
        Ok(())
    }

    async fn auto_login(&self) -> Result<(), IggyError> {
        let client_address = self.get_client_address_value().await;
        let skip_auto_login = {
            let mut guard = self.skip_auto_login_once.lock().await;
            std::mem::take(&mut *guard)
        };

        match &self.config.auto_login {
            AutoLogin::Disabled => {
                info!("{NAME} client: {client_address} - automatic sign-in is disabled.");
                Ok(())
            }
            AutoLogin::Enabled(credentials) => {
                if skip_auto_login {
                    info!("Skipping automatic sign-in for a retried login/register request.");
                    return Ok(());
                }
                info!("{NAME} client: {client_address} is signing in...");
                self.set_state(ClientState::Authenticating).await;
                match credentials {
                    Credentials::UsernamePassword(username, password) => {
                        self.login_user(username, password.expose_secret()).await?;
                        info!(
                            "{NAME} client: {client_address} has signed in with the user credentials, username: {username}",
                        );
                        Ok(())
                    }
                    Credentials::PersonalAccessToken(token) => {
                        self.login_with_personal_access_token(token.expose_secret())
                            .await?;
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
        self.reset_vsr_session().await?;

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

        self.reset_vsr_session().await?;
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

        let stream = self.stream.clone();
        let consensus_session = self.consensus_session.clone();
        // The spawned task owns the lockstep exchange to completion. Cancelling
        // the caller after a partial WebSocket frame or response header must
        // not release the stream lock while leaving that connection reusable.
        tokio::spawn(async move {
            let mut stream_guard = stream.lock().await;
            if stream_guard.is_none() {
                trace!("Cannot send data. Client is not connected.");
                return Err(IggyError::NotConnected);
            }

            // Encode the request ONCE: `next_request_id` advances here, so a
            // transient replay must reuse the same id for the server's dedup.
            // The connection is lockstep (one request in flight per client), so a
            // complete reply leaves the stream at a clean frame boundary -- a
            // `TransientNotCommitted` answer (the server could not commit yet)
            // lets us resend the SAME request on the SAME connection with no
            // reconnect and the session intact. Bounded by RESPONSE_READ_TIMEOUT.
            let request = {
                let mut consensus_session = consensus_session
                    .lock()
                    .expect("consensus session mutex poisoned");
                crate::vsr::encode_contiguous_request(&mut consensus_session, code, &payload)?
            };
            trace!(
                "Sending {NAME} VSR request of size {} with code: {code}",
                request.len()
            );
            // One deadline bounds the whole request including transient replays.
            let retry_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
            // `TransientNotAccepted` gets a short same-connection window only:
            // past it the refusal is a verdict about who leads, not load, and
            // the caller runs a leader recheck or roster walk. Login/register
            // keeps the full budget here: the connect flow owns its own
            // leader settlement.
            let not_accepted_deadline = if is_login_register_code(code) {
                retry_deadline
            } else {
                retry_deadline.min(tokio::time::Instant::now() + TRANSIENT_FAILOVER_CHECK_INTERVAL)
            };
            loop {
                let stream = stream_guard.as_mut().ok_or(IggyError::NotConnected)?;
                stream.write(&request).await?;
                stream.flush().await?;

                // One deadline spans both the header and body reads so a reply
                // that delivers a header then stalls cannot wait up to 2x the
                // timeout. On expiry drop the stream so a late reply cannot
                // desync framing for the next request.
                let mut response_header = [0u8; iggy_binary_protocol::HEADER_SIZE];
                let header_read =
                    tokio::time::timeout_at(retry_deadline, stream.read(&mut response_header))
                        .await;
                let Ok(header_read) = header_read else {
                    error!(
                        "Timed out after {RESPONSE_READ_TIMEOUT:?} waiting for {NAME} VSR response header for request with code: {code}"
                    );
                    *stream_guard = None;
                    return Err(IggyError::Disconnected);
                };
                header_read?;

                let response_size = crate::vsr::response_size(&response_header)?;
                let body_size = response_size - iggy_binary_protocol::HEADER_SIZE;
                let body = if body_size > 0 {
                    let mut body = vec![0u8; body_size];
                    let body_read =
                        tokio::time::timeout_at(retry_deadline, stream.read(&mut body)).await;
                    let Ok(body_read) = body_read else {
                        error!(
                            "Timed out after {RESPONSE_READ_TIMEOUT:?} waiting for {NAME} VSR response body for request with code: {code}"
                        );
                        *stream_guard = None;
                        return Err(IggyError::Disconnected);
                    };
                    body_read?;
                    Bytes::from(body)
                } else {
                    Bytes::new()
                };

                match crate::vsr::decode_response_split(&response_header, body) {
                    Err(IggyError::TransientNotAccepted)
                        if tokio::time::Instant::now() >= not_accepted_deadline =>
                    {
                        // Never admitted, so re-issuable anywhere: hand it
                        // back for a leader recheck or a roster walk instead
                        // of replaying into the same refusal for the whole
                        // request budget.
                        return Err(IggyError::TransientNotAccepted);
                    }
                    // The server answered with a complete transient frame. The
                    // lockstep stream is in sync, and replaying the same request
                    // id on this session preserves metadata dedup even when the
                    // original outcome is still resolving.
                    Err(IggyError::TransientNotCommitted | IggyError::TransientNotAccepted)
                        if tokio::time::Instant::now() < retry_deadline =>
                    {
                        let remaining =
                            retry_deadline.saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                    }
                    other => return other,
                }
            }
        })
        .await
        .map_err(|error| {
            error!("Task execution failed during {NAME} request: {error}");
            IggyError::WebSocketSendError
        })?
    }
}

const fn is_login_register_code(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn a_roster_hop_does_not_enter_the_reconnect_ladder() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve TCP address");
        let server_address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address,
            ..WebSocketClientConfig::default()
        }))
        .expect("create WebSocket client");

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.connect_off_leader(),
        )
        .await
        .expect("one WebSocket dial must not enter unlimited reconnect");
        assert!(matches!(result, Err(IggyError::CannotEstablishConnection)));
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    #[test]
    fn should_be_created_with_default_config() {
        let client = WebSocketClient::default();
        assert_eq!(client.config.server_address, "127.0.0.1:8092");
        assert_eq!(
            client.config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
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
            heartbeat_interval: NonZeroIggyDuration::from_str("10s").unwrap(),
            ..Default::default()
        };

        let client = WebSocketClient::create(Arc::new(config));
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:9090");
        assert_eq!(
            client.config.heartbeat_interval,
            NonZeroIggyDuration::from_str("10s").unwrap()
        );
    }

    #[test]
    fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+ws://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = WebSocketClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+ws://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = WebSocketClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
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

    #[test]
    fn should_succeed_from_connection_string_with_hostname() {
        let connection_string = "iggy+ws://user:secret@localhost:8092";
        let client = WebSocketClient::from_connection_string(connection_string);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.config.server_address, "localhost:8092");
    }
}
