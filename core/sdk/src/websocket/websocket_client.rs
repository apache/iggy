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

use crate::leader_aware::read_transport_endpoints;
use crate::leader_aware::{
    ConnectCoordinator, ConnectOwnerContext, LeaderRedirectionState, RosterWalk,
    check_and_redirect_to_leader, is_unauthenticated_metadata_probe,
};
use crate::poll_routing::{PollRouter, PollTransport, ROSTER_READ_TIMEOUT, is_poll_routing_code};
use crate::session::ConsensusSession;
use crate::vsr::retain_replay_header;
use crate::websocket::websocket_connection_stream::WebSocketConnectionStream;
use crate::websocket::websocket_stream_kind::WebSocketStreamKind;
use crate::websocket::websocket_tls_connection_stream::WebSocketTlsConnectionStream;
use iggy_common::TransportProtocol;
use rustls::{ClientConfig, pki_types::pem::PemObject};

use crate::prelude::Client;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
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
use std::sync::atomic::{AtomicBool, Ordering};
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
    poll_router: PollRouter<Self>,
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
    roster_learned: AtomicBool,
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
        self.forget_session_credentials().await;
        WebSocketClient::disconnect_transport(self).await?;
        self.reset_vsr_session().await
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
    async fn send_offset_write_with_response(
        &self,
        code: u32,
        payload: Bytes,
    ) -> Result<Bytes, IggyError> {
        self.poll_router.write_offset(self, code, payload).await
    }

    async fn send_poll_with_response(
        &self,
        request: &iggy_binary_protocol::requests::messages::PollMessagesRequest,
    ) -> Result<Bytes, IggyError> {
        self.poll_router.poll(self, request).await
    }
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        let mut current = self.state.lock().await;
        // Shutdown is final: a later write, such as a disconnect or a lost
        // connection, must not make the client usable again.
        if *current != ClientState::Shutdown {
            *current = state;
        }
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a {} diagnostic event: {error}", NAME);
        }
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        if is_poll_routing_code(code) {
            return self.send_poll_request(code, payload).await;
        }
        let roster_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
        let mut header = None;
        let mut result = self
            .send_raw_retaining_header(code, payload.clone(), &mut header)
            .await;

        // A persistent not-admitted refusal is a verdict about who leads the
        // TARGET group, which the metadata leader check alone cannot repair:
        // metadata and partition consensus groups elect independently. Recheck
        // the leader once, then walk the roster, one visit per endpoint. Only
        // recoverable with a session to re-establish, hence the auto-login
        // gate, and a login/register replay stays on its own connection.
        if matches!(result, Err(IggyError::TransientNotAccepted))
            && !is_login_register_code(code)
            && !matches!(code, GET_CLUSTER_METADATA_CODE | BIND_SESSION_CODE)
            && self.config.reconnection.enabled
            && self.sign_in_credentials().is_some()
        {
            let routing_guard =
                match tokio::time::timeout_at(roster_deadline, self.routing_lock.lock()).await {
                    Ok(guard) => guard,
                    Err(_) => return Err(IggyError::TransientNotAccepted),
                };
            let mut routing_guard = Some(routing_guard);
            let overall_deadline = roster_deadline;
            // A concurrent refused request may have completed the movement
            // while this request waited for the gate.
            result = match tokio::time::timeout_at(
                overall_deadline,
                self.send_raw_retaining_header(code, payload.clone(), &mut header),
            )
            .await
            {
                Ok(result) => result,
                // The frame is on the wire with the reply unread, so the
                // outcome is unknown: it may be admitted, replicated, and
                // committed. `TransientNotAccepted` here would license the
                // walk to re-issue the payload under a fresh session the
                // server's dedup fence cannot match. `TransientNotCommitted`
                // states the truth and also ends the hop chain.
                Err(_) => Err(IggyError::TransientNotCommitted),
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
                } else if roster_walk
                    .as_ref()
                    .is_some_and(RosterWalk::is_single_endpoint)
                {
                    // A single-node roster can still be converging a newly
                    // committed partition. Retry this explicitly unadmitted
                    // request on the current endpoint within the same budget.
                    drop(routing_guard.take());
                    (current, false)
                } else {
                    return Err(IggyError::TransientNotAccepted);
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
                retain_replay_header(
                    &mut header,
                    &self.consensus_session,
                    code,
                    &IggyError::TransientNotAccepted,
                )?;
                result = match tokio::time::timeout_at(
                    overall_deadline,
                    self.send_raw_retaining_header(code, payload.clone(), &mut header),
                )
                .await
                {
                    Ok(result) => result,
                    // On the wire, reply unread: unknown outcome. See the
                    // matching arm above; a fabricated not-admitted would
                    // re-issue a possibly committed write on the next hop.
                    Err(_) => Err(IggyError::TransientNotCommitted),
                };
            }
        }

        if result.is_ok() {
            return result;
        }

        let error = result.unwrap_err();
        let mut retry_outcome = crate::vsr::RetryOutcome::for_reconnect(code, &error);
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

        if matches!(code, GET_CLUSTER_METADATA_CODE | BIND_SESSION_CODE) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        if !is_login_register_code(code) && self.sign_in_credentials().is_none() {
            return Err(error);
        }

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
            self.connect()
                .await
                .map_err(|error| retry_outcome.observe(error))?;
            drop(_routing_guard);
            return self
                .replay_after_reconnect(code, payload, &mut header, &error, roster_deadline)
                .await
                .map_err(|error| retry_outcome.observe(error));
        }
        self.disconnect_transport()
            .await
            .map_err(|error| retry_outcome.observe(error))?;

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
        reconnect.map_err(|error| retry_outcome.observe(error))?;
        drop(_routing_guard);
        self.replay_after_reconnect(code, payload, &mut header, &error, roster_deadline)
            .await
            .map_err(|error| retry_outcome.observe(error))
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
    async fn session_identity(
        &self,
    ) -> Result<iggy_binary_protocol::requests::system::SessionIdentity, IggyError> {
        let session = self
            .consensus_session
            .lock()
            .map_err(|_| IggyError::InvalidConfiguration)?;
        Ok(iggy_binary_protocol::requests::system::SessionIdentity {
            client_id: session.client_id(),
            session: session.session().ok_or(IggyError::Unauthenticated)?,
            metadata_watermark: self
                .poll_router
                .metadata_watermark
                .load(std::sync::atomic::Ordering::Acquire),
        })
    }

    async fn session_bind_secret(
        &self,
    ) -> Result<iggy_binary_protocol::requests::users::login_register::BindSecret, IggyError> {
        Ok(self
            .consensus_session
            .lock()
            .map_err(|_| IggyError::InvalidConfiguration)?
            .bind_secret())
    }

    async fn bind_vsr_session(&self, session: u64) -> Result<(), IggyError> {
        let mut consensus_session = self
            .consensus_session
            .lock()
            .map_err(|_| IggyError::InvalidConfiguration)?;
        let was_bound = consensus_session.is_bound();
        consensus_session.bind(session)?;
        drop(consensus_session);
        if !was_bound {
            self.consumer_group_state.clear_session_scoped();
            self.poll_router.clear_session();
        }
        Ok(())
    }

    async fn reset_vsr_session(&self) -> Result<(), IggyError> {
        *self
            .consensus_session
            .lock()
            .expect("consensus session mutex poisoned") = ConsensusSession::new();
        self.consumer_group_state.clear_session_scoped();
        self.poll_router.clear_session();
        Ok(())
    }

    async fn remember_session_credentials(&self, credentials: Credentials, user_id: u32) {
        self.poll_router.remember_credentials(credentials, user_id);
        if self.auto_login_configured()
            || self.get_state().await != ClientState::Authenticated
            || self.roster_learned.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let read = read_transport_endpoints(self, TransportProtocol::WebSocket);
        let Ok((node_count, endpoints)) = tokio::time::timeout(ROSTER_READ_TIMEOUT, read).await
        else {
            warn!("Reading the cluster roster took longer than {ROSTER_READ_TIMEOUT:?}");
            return;
        };
        if !endpoints.is_empty() {
            self.poll_router
                .roster_size
                .store(node_count, Ordering::Release);
            *self.roster_endpoints.lock().await = endpoints;
        }
    }

    async fn forget_session_credentials(&self) {
        self.poll_router.forget_credentials();
    }

    async fn refresh_session_password(&self, user: &iggy_common::Identifier, new_password: &str) {
        self.poll_router.refresh_password(user, new_password);
    }

    async fn refresh_session_username(&self, user: &iggy_common::Identifier, new_username: &str) {
        self.poll_router.refresh_username(user, new_username);
    }

    fn sdk_version(&self) -> &'static str {
        crate::SDK_VERSION
    }
}

impl BinaryClient for WebSocketClient {}

#[async_trait]
impl PollTransport for WebSocketClient {
    const PROTOCOL: TransportProtocol = TransportProtocol::WebSocket;

    async fn connect_poll_client(&self, endpoint: &str) -> Result<Self, IggyError> {
        let mut config = (*self.config).clone();
        config.server_address = endpoint.to_owned();
        config.auto_login = AutoLogin::Disabled;
        config.reconnection.enabled = false;
        let mut client = Self::create(Arc::new(config))?;
        client.consensus_session = Arc::clone(&self.consensus_session);
        client.poll_router.metadata_watermark = Arc::clone(&self.poll_router.metadata_watermark);
        client.connect_off_leader().await?;
        Ok(client)
    }

    async fn send_poll_request(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
        self.send_raw_request(code, payload, false, &mut None).await
    }
}

impl WebSocketClient {
    fn sign_in_credentials(&self) -> Option<Credentials> {
        self.poll_router
            .remembered_credentials()
            .or_else(|| match &self.config.auto_login {
                AutoLogin::Enabled(credentials) => Some(credentials.clone()),
                AutoLogin::Disabled => None,
            })
    }

    /// Whether an `AutoLogin` is configured on this client, which makes the
    /// session after any connect the configured user's rather than whoever
    /// signed in by hand.
    pub(crate) fn auto_login_configured(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_))
    }
    /// Create a new WebSocket client with the provided configuration.
    ///
    /// Returns [`IggyError::InvalidConfiguration`] if the maximum write buffer
    /// size does not exceed the write buffer size.
    pub fn create(config: Arc<WebSocketClientConfig>) -> Result<Self, IggyError> {
        let ws_config = config.ws_config.to_tungstenite_config();
        if ws_config.max_write_buffer_size <= ws_config.write_buffer_size {
            error!("WebSocket max_write_buffer_size must be greater than write_buffer_size");
            return Err(IggyError::InvalidConfiguration);
        }

        let (sender, receiver) = broadcast(1000);
        let server_address = config.server_address.clone();
        Ok(WebSocketClient {
            poll_router: PollRouter::default(),
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
            roster_learned: AtomicBool::new(false),
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
        let mut resume_walk = None;
        loop {
            match self.get_state().await {
                ClientState::Shutdown => {
                    trace!("Cannot connect. Client is shutdown.");
                    return Err(IggyError::ClientShutdown);
                }
                ClientState::Connected
                | ClientState::Authenticating
                | ClientState::Authenticated => return Ok(()),
                _ => {}
            }

            let mut retry_count = 0;
            let first_address = self.current_server_address.lock().await.clone();
            let roster = self.roster_endpoints.lock().await.clone();
            let mut dial_walk = RosterWalk::new(&first_address, &roster);
            let walk_roster = self.config.reconnection.enabled && !dial_walk.is_single_endpoint();

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

                let dial_once = single_attempt || walk_roster;
                let connection_result = if self.config.tls_enabled {
                    self.connect_tls(server_addr, &mut retry_count, dial_once)
                        .await
                } else {
                    self.connect_plain(server_addr, &mut retry_count, dial_once)
                        .await
                };
                let connection_stream = match connection_result {
                    Ok(stream) => stream,
                    Err(IggyError::CannotEstablishConnection) if !single_attempt && walk_roster => {
                        if let Some(next) = dial_walk.next() {
                            *self.current_server_address.lock().await = next;
                            continue;
                        }
                        match self
                            .handle_connection_error::<()>(&mut retry_count, false)
                            .await
                        {
                            Err(IggyError::Disconnected) => {
                                dial_walk = RosterWalk::new(&first_address, &roster);
                                *self.current_server_address.lock().await = first_address.clone();
                                continue;
                            }
                            Err(error) => return Err(error),
                            Ok(()) => {
                                unreachable!("connection-error handler always returns an error")
                            }
                        }
                    }
                    Err(IggyError::Disconnected) => continue,
                    Err(error) => return Err(error),
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

            match self.check_and_maybe_redirect(settle_off_leader).await {
                Ok(false) => return Ok(()),
                Ok(true) => {}
                Err(error) => {
                    if !single_attempt && crate::vsr::resume_error_is_retryable(&error) {
                        self.disconnect_transport().await?;
                        let current = self.current_server_address.lock().await.clone();
                        let roster = self.roster_endpoints.lock().await.clone();
                        let walk =
                            resume_walk.get_or_insert_with(|| RosterWalk::new(&current, &roster));
                        if let Some(next) = walk.next() {
                            *self.current_server_address.lock().await = next;
                            continue;
                        }
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn clear_abandoned_connect(&self) -> Result<(), IggyError> {
        self.stream.lock().await.take();
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
                    server_addr, error
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
        if !self.auto_login().await? || settle_off_leader {
            return Ok(false);
        }
        self.handle_leader_redirection().await
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
        // Replace the roster with the cluster's current endpoint list.
        if !leader_check.endpoints.is_empty() {
            self.poll_router
                .roster_size
                .store(leader_check.node_count, Ordering::Release);
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
            self.disconnect_transport().await?;
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
        self.disconnect_transport().await?;
        *self.current_server_address.lock().await = next;
        Ok(())
    }

    async fn auto_login(&self) -> Result<bool, IggyError> {
        let skip_auto_login = {
            let mut guard = self.skip_auto_login_once.lock().await;
            std::mem::take(&mut *guard)
        };
        if skip_auto_login {
            return Ok(false);
        }
        let Some(credentials) = self.sign_in_credentials() else {
            return Ok(false);
        };
        if self
            .consensus_session
            .lock()
            .map_err(|_| IggyError::InvalidConfiguration)?
            .is_bound()
        {
            match self.resume_vsr_session().await {
                Ok(()) => return Ok(true),
                Err(IggyError::Unauthenticated | IggyError::TransientNotCommitted) => {
                    self.reset_vsr_session().await?;
                }
                Err(error) => {
                    self.disconnect_transport().await?;
                    return Err(error);
                }
            }
        }
        self.set_state(ClientState::Authenticating).await;
        let result = match credentials {
            Credentials::UsernamePassword(username, password) => self
                .login_user(&username, password.expose_secret())
                .await
                .map(|_| ()),
            Credentials::PersonalAccessToken(token) => self
                .login_with_personal_access_token(token.expose_secret())
                .await
                .map(|_| ()),
        };
        if let Err(error) = result {
            if crate::vsr::resume_error_is_retryable(&error) {
                self.disconnect_transport().await?;
            } else {
                self.set_state(ClientState::Connected).await;
            }
            return Err(error);
        }
        Ok(true)
    }

    async fn disconnect_transport(&self) -> Result<(), IggyError> {
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

        self.reset_vsr_session().await?;
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} client: {client_address} has been shutdown.");
        Ok(())
    }

    async fn send_raw_retaining_header(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut Option<iggy_binary_protocol::RequestHeader>,
    ) -> Result<Bytes, IggyError> {
        self.send_raw_request(code, payload, true, header).await
    }

    async fn replay_after_reconnect(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut Option<iggy_binary_protocol::RequestHeader>,
        original_error: &IggyError,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, IggyError> {
        let current = self.current_server_address.lock().await.clone();
        let roster = self.roster_endpoints.lock().await.clone();
        let mut walk = RosterWalk::new(&current, &roster);
        'replay: loop {
            retain_replay_header(header, &self.consensus_session, code, original_error)?;
            let result = tokio::time::timeout_at(
                deadline,
                self.send_raw_retaining_header(code, payload.clone(), header),
            )
            .await
            .map_err(|_| IggyError::TransientNotCommitted)?;
            if !matches!(result, Err(IggyError::TransientNotAccepted)) {
                return result;
            }
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(IggyError::TransientNotCommitted);
                }
                while let Some(next) = walk.next() {
                    let connected = tokio::time::timeout_at(deadline, async {
                        self.settle_on_endpoint(next).await?;
                        self.connect_off_leader().await
                    })
                    .await
                    .map_err(|_| IggyError::TransientNotCommitted)?;
                    match connected {
                        Ok(()) => continue 'replay,
                        Err(IggyError::CannotEstablishConnection) => {}
                        Err(error) => return Err(error),
                    }
                }
                // A view can settle after its endpoint was already visited.
                // A failed last dial must reconnect before the next replay.
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + NOT_READY_RETRY_INTERVAL),
                )
                .await;
                let current = self.current_server_address.lock().await.clone();
                walk = RosterWalk::new(&current, &roster);
                if self.get_state().await == ClientState::Authenticated {
                    continue 'replay;
                }
            }
        }
    }

    async fn send_raw_request(
        &self,
        code: u32,
        payload: Bytes,
        retry_transient: bool,
        header: &mut Option<iggy_binary_protocol::RequestHeader>,
    ) -> Result<Bytes, IggyError> {
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
        let metadata_watermark = Arc::clone(&self.poll_router.metadata_watermark);
        // The spawned task owns the lockstep exchange to completion. Cancelling
        // the caller after a partial WebSocket frame or response header must
        // not release the stream lock while leaving that connection reusable.
        let preencoded = *header;
        let (used_header, result) = tokio::spawn(async move {
            let mut used_header = preencoded;
            let result = async {
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
                    let mut session = consensus_session
                        .lock()
                        .expect("consensus session mutex poisoned");
                    crate::vsr::encode_contiguous_request(&mut session, code, &payload, &mut used_header)?
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
                let mut frame_complete = false;
                let mut retry_outcome = crate::vsr::RetryOutcome::default();
                let outcome = async {
                    loop {
                        frame_complete = false;
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

                        frame_complete = true;
                        crate::vsr::observe_metadata_reply(&metadata_watermark, &response_header);
                        match crate::vsr::decode_response_split(&response_header, body)
                            .map_err(|error| retry_outcome.observe(error))
                        {
                            Err(error) if !retry_transient => return Err(error),
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
                }
                .await;
                if !frame_complete {
                    stream_guard.take();
                }
                outcome
            }
            .await;
            (used_header, result)
        })
        .await
        .map_err(|error| {
            error!("Task execution failed during {NAME} request: {error}");
            IggyError::WebSocketSendError
        })?;
        *header = used_header;
        result
    }
}

const fn is_login_register_code(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use futures_util::{SinkExt, StreamExt};
    use iggy_binary_protocol::codes::SEND_MESSAGES_CODE;
    use iggy_binary_protocol::requests::system::BindSessionRequest;
    use iggy_binary_protocol::responses::users::LoginRegisterResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, IGGY_PROTOCOL_VERSION, Operation, ReplyHeader, RequestHeader,
        WireDecode, WireEncode, WireName,
    };
    use iggy_common::WebSocketClientReconnectionConfig;
    use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

    async fn read_test_request(
        stream: &mut WebSocketStream<TcpStream>,
    ) -> (RequestHeader, Vec<u8>) {
        let frame = stream.next().await.unwrap().unwrap().into_data();
        let header = bytemuck::checked::try_pod_read_unaligned(&frame[..HEADER_SIZE]).unwrap();
        (header, frame[HEADER_SIZE..].to_vec())
    }

    async fn answer_test_request(
        stream: &mut WebSocketStream<TcpStream>,
        request: &RequestHeader,
        status: u32,
        body: &[u8],
    ) {
        let header = ReplyHeader {
            command: Command::Reply,
            operation: request.operation,
            client: request.client,
            request: request.request,
            size: u32::try_from(HEADER_SIZE + body.len()).unwrap(),
            status,
            ..Default::default()
        };
        let mut frame = bytemuck::bytes_of(&header).to_vec();
        frame.extend_from_slice(body);
        stream.send(Message::Binary(frame.into())).await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_reply_resumes_and_replays_only_the_original_session() {
        const TEST_USER_ID: u32 = 7;
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);
        for (fresh_login, hop_after_resume, dead_roster_tail) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, true, true),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let next_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let dead_address = dead_listener.local_addr().unwrap().to_string();
            drop(dead_listener);
            let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
                server_address: listener.local_addr().unwrap().to_string(),
                reconnection: WebSocketClientReconnectionConfig {
                    reestablish_after: IggyDuration::from_str("0s").unwrap(),
                    max_retries: Some(0),
                    ..Default::default()
                },
                ..Default::default()
            }))
            .unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let (first, connected) = tokio::join!(
                async {
                    accept_async(listener.accept().await.unwrap().0)
                        .await
                        .unwrap()
                },
                Client::connect(&client)
            );
            connected.unwrap();
            client.set_state(ClientState::Authenticated).await;
            client.roster_learned.store(true, Ordering::SeqCst);
            if hop_after_resume {
                *client.roster_endpoints.lock().await = vec![
                    listener.local_addr().unwrap().to_string(),
                    next_listener.local_addr().unwrap().to_string(),
                ];
                if dead_roster_tail {
                    client.roster_endpoints.lock().await.push(dead_address);
                }
            }
            client
                .remember_session_credentials(
                    Credentials::UsernamePassword("iggy".into(), "secret".into()),
                    TEST_USER_ID,
                )
                .await;
            let original = client.session_identity().await.unwrap();
            let secret = client.session_bind_secret().await.unwrap();
            let peer = tokio::spawn(async move {
                let mut first = first;
                let (lost_header, lost_body) = read_test_request(&mut first).await;
                assert_eq!(lost_header.operation, Operation::SendMessages);
                drop(first);
                let mut resumed = accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();
                let (bind_header, bind_body) = read_test_request(&mut resumed).await;
                assert_eq!(
                    u32::from_le_bytes(bind_header.reserved[..4].try_into().unwrap()),
                    BIND_SESSION_CODE
                );
                let binding = BindSessionRequest::decode_from(&bind_body).unwrap();
                assert_eq!(binding.identity, original);
                assert_eq!(binding.bind_secret.expose_secret(), secret.expose_secret());
                let login = LoginRegisterResponse {
                    user_id: TEST_USER_ID,
                    session: if fresh_login { 2 } else { 1 },
                    server_protocol_version: IGGY_PROTOCOL_VERSION,
                    server_version: WireName::new("test").unwrap(),
                }
                .to_bytes();
                answer_test_request(
                    &mut resumed,
                    &bind_header,
                    if fresh_login {
                        IggyError::Unauthenticated.as_code()
                    } else {
                        0
                    },
                    if fresh_login { &[] } else { &login },
                )
                .await;
                if fresh_login {
                    let (register, _) = read_test_request(&mut resumed).await;
                    assert_eq!(register.operation, Operation::Register);
                    assert_ne!(register.client, original.client_id);
                    let mut result = vec![0; size_of::<u32>()];
                    result.extend_from_slice(&login);
                    answer_test_request(&mut resumed, &register, 0, &result).await;
                }
                loop {
                    let (header, body) = read_test_request(&mut resumed).await;
                    if header.operation == Operation::NonReplicated {
                        answer_test_request(
                            &mut resumed,
                            &header,
                            IggyError::FeatureUnavailable.as_code(),
                            &[],
                        )
                        .await;
                        continue;
                    }
                    if fresh_login {
                        assert_ne!(header.client, lost_header.client);
                        assert_eq!(body, b"explicit-new-send");
                    } else {
                        assert_eq!(
                            bytemuck::bytes_of(&header),
                            bytemuck::bytes_of(&lost_header)
                        );
                        assert_eq!(body, lost_body);
                    }
                    if hop_after_resume {
                        answer_test_request(
                            &mut resumed,
                            &header,
                            IggyError::TransientNotAccepted.as_code(),
                            &[],
                        )
                        .await;
                        tokio::time::sleep(TRANSIENT_FAILOVER_CHECK_INTERVAL).await;
                        let (retry_header, retry_body) = read_test_request(&mut resumed).await;
                        assert_eq!(
                            bytemuck::bytes_of(&retry_header),
                            bytemuck::bytes_of(&lost_header)
                        );
                        assert_eq!(retry_body, lost_body);
                        answer_test_request(
                            &mut resumed,
                            &retry_header,
                            IggyError::TransientNotAccepted.as_code(),
                            &[],
                        )
                        .await;
                        resumed = accept_async(next_listener.accept().await.unwrap().0)
                            .await
                            .unwrap();
                        let (next_bind, bind_body) = read_test_request(&mut resumed).await;
                        let binding = BindSessionRequest::decode_from(&bind_body).unwrap();
                        assert_eq!(binding.identity, original);
                        assert_eq!(binding.bind_secret.expose_secret(), secret.expose_secret());
                        answer_test_request(&mut resumed, &next_bind, 0, &login).await;
                        let (next_header, next_body) = read_test_request(&mut resumed).await;
                        assert_eq!(
                            bytemuck::bytes_of(&next_header),
                            bytemuck::bytes_of(&lost_header)
                        );
                        assert_eq!(next_body, lost_body);
                        if dead_roster_tail {
                            answer_test_request(
                                &mut resumed,
                                &next_header,
                                IggyError::TransientNotAccepted.as_code(),
                                &[],
                            )
                            .await;
                            tokio::time::sleep(TRANSIENT_FAILOVER_CHECK_INTERVAL).await;
                            let (retry_header, retry_body) = read_test_request(&mut resumed).await;
                            assert_eq!(
                                bytemuck::bytes_of(&retry_header),
                                bytemuck::bytes_of(&lost_header)
                            );
                            assert_eq!(retry_body, lost_body);
                            answer_test_request(
                                &mut resumed,
                                &retry_header,
                                IggyError::TransientNotAccepted.as_code(),
                                &[],
                            )
                            .await;
                            resumed = accept_async(listener.accept().await.unwrap().0)
                                .await
                                .unwrap();
                            let (bind_header, bind_body) = read_test_request(&mut resumed).await;
                            let binding = BindSessionRequest::decode_from(&bind_body).unwrap();
                            assert_eq!(binding.identity, original);
                            assert_eq!(binding.bind_secret.expose_secret(), secret.expose_secret());
                            answer_test_request(&mut resumed, &bind_header, 0, &login).await;
                            let (replay_header, replay_body) =
                                read_test_request(&mut resumed).await;
                            assert_eq!(
                                bytemuck::bytes_of(&replay_header),
                                bytemuck::bytes_of(&lost_header)
                            );
                            assert_eq!(replay_body, lost_body);
                            answer_test_request(&mut resumed, &replay_header, 0, b"receipt").await;
                            return;
                        }
                        answer_test_request(&mut resumed, &next_header, 0, b"receipt").await;
                        return;
                    }
                    answer_test_request(&mut resumed, &header, 0, b"receipt").await;
                    return;
                }
            });
            let result = tokio::time::timeout(
                TEST_BUDGET,
                client.send_raw_with_response(SEND_MESSAGES_CODE, Bytes::from_static(b"lost-send")),
            )
            .await
            .unwrap();
            if fresh_login {
                assert_eq!(result.unwrap_err(), IggyError::TransientNotCommitted);
                assert_ne!(
                    client.session_identity().await.unwrap().client_id,
                    original.client_id
                );
                assert_eq!(
                    client
                        .send_raw_with_response(
                            SEND_MESSAGES_CODE,
                            Bytes::from_static(b"explicit-new-send")
                        )
                        .await
                        .unwrap(),
                    Bytes::from_static(b"receipt")
                );
            } else {
                assert_eq!(result.unwrap(), Bytes::from_static(b"receipt"));
                assert_eq!(client.session_identity().await.unwrap(), original);
            }
            tokio::time::timeout(TEST_BUDGET, peer)
                .await
                .unwrap()
                .unwrap();
            let _ = Client::shutdown(&client).await;
        }
    }

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
    fn should_reject_invalid_write_buffer_limits() {
        for options in [
            "max_write_buffer_size=1",
            "write_buffer_size=0&max_write_buffer_size=0",
            "write_buffer_size=1024&max_write_buffer_size=1024",
            "write_buffer_size=1024&max_write_buffer_size=1023",
        ] {
            let connection_string = format!("iggy+ws://user:secret@127.0.0.1:8092?{options}");
            let error = WebSocketClient::from_connection_string(&connection_string).err();

            assert_eq!(error, Some(IggyError::InvalidConfiguration), "{options}");
        }
    }

    #[test]
    fn should_accept_write_buffer_limits_above_the_write_buffer() {
        for options in [
            "write_buffer_size=0&max_write_buffer_size=1",
            "write_buffer_size=1024&max_write_buffer_size=1025",
        ] {
            let connection_string = format!("iggy+ws://user:secret@127.0.0.1:8092?{options}");
            let error = WebSocketClient::from_connection_string(&connection_string).err();

            assert_eq!(error, None, "{options}");
        }
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
