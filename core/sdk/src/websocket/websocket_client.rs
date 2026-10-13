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

use crate::driver::{PollRouter, PollTransport, ROSTER_READ_TIMEOUT, is_poll_routing_code};
use crate::leader_aware::read_transport_endpoints;
use crate::leader_aware::{
    LeaderRedirectionState, RosterWalk, check_and_redirect_to_leader,
    is_unauthenticated_metadata_probe,
};
use crate::links::Frame;
use crate::links::connect::{
    ConnectCoordinator, ConnectOwnerContext, accepts_state, is_same_spelling,
};
use crate::links::event_channel;
use crate::links::mux::MuxLink;
use crate::session_owner::SessionOwner;
use crate::vsr::{RetainedRequest, retain_replay_header};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::TransportProtocol;
use rustls::{ClientConfig, pki_types::pem::PemObject};

use crate::prelude::Client;
use async_broadcast::{Receiver, Sender};
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
/// Bound on how long a request waits for its VSR reply. The wait holds the
/// link lock, so an unanswered request (lost server reply) would wedge every
/// later request on this client forever. On expiry the link is dropped.
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
    link: Arc<Mutex<Option<MuxLink>>>,
    pub(crate) config: Arc<WebSocketClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    pub(crate) connected_at: Mutex<Option<IggyTimestamp>>,
    leader_redirection_state: Mutex<LeaderRedirectionState>,
    pub(crate) current_server_address: Mutex<String>,
    /// The session and the sign-in, shared with the poll router and the
    /// poll clients it dials.
    session_owner: Arc<SessionOwner>,
    skip_auto_login_once: Mutex<bool>,
    /// Every endpoint the cluster roster named on the last leader check, kept
    /// as walk candidates for a request the current node keeps refusing to
    /// admit (its replica of the target partition group is not the primary).
    roster_endpoints: Mutex<Vec<String>>,
    roster_learned: AtomicBool,
    /// Set by an explicit `Client::disconnect` and cleared by `Client::connect`.
    /// While it is set, no request reconnects the client on its own. A
    /// connection that drops without a `disconnect()` call leaves it clear, so
    /// that loss still heals.
    disconnected_by_caller: AtomicBool,
    /// Set when `shutdown()` finishes its cleanup. The state turns `Shutdown`
    /// before that cleanup and stays there, so it cannot tell a finished
    /// shutdown from a cancelled one: a cancelled `shutdown()` leaves the
    /// cleanup to the next call.
    shutdown_complete: AtomicBool,
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
        // A sweep that started before the last disconnect ends at its next
        // read of the flag. Joined, it would go on with the session that the
        // disconnect reset, and hand its failure to this connect. Once
        // another connect cleared the flag, its sweep is the one to join.
        self.connect_coordinator
            .wait_until_inactive(|| self.caller_disconnected())
            .await;
        self.disconnected_by_caller.store(false, Ordering::SeqCst);
        WebSocketClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.disconnected_by_caller.store(true, Ordering::SeqCst);
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
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        self.poll_router
            .write_offset(self, code, payload, context)
            .await
    }

    async fn send_poll_with_response(
        &self,
        request: &iggy_binary_protocol::requests::messages::PollMessagesRequest,
        context: Option<PartitionContext>,
    ) -> Result<Bytes, IggyError> {
        self.poll_router.poll(self, request, context).await
    }
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        let mut current = self.state.lock().await;
        if accepts_state(*current, state, self.caller_disconnected()) {
            *current = state;
        }
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.try_broadcast(event) {
            error!("Failed to send a {} diagnostic event: {error}", NAME);
        }
    }

    async fn send_raw_with_context(
        &self,
        code: u32,
        payload: Bytes,
        context: PartitionContext,
    ) -> Result<Bytes, IggyError> {
        if self.caller_disconnected() {
            return Err(match self.get_state().await {
                ClientState::Shutdown => IggyError::ClientShutdown,
                _ => IggyError::NotConnected,
            });
        }
        if is_poll_routing_code(code) {
            return self
                .send_poll_request_with_context(code, payload, context)
                .await;
        }
        let roster_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
        let mut header = RetainedRequest::new(context);
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
            && self.session_owner.sign_in_credentials().is_some()
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
                                if is_same_spelling(&connected, &target) || first_visit {
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
                    &self.session_owner,
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

        if !is_login_register_code(code) && self.session_owner.sign_in_credentials().is_none() {
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
        self.session_owner.identity()
    }

    async fn session_bind_secret(
        &self,
    ) -> Result<iggy_binary_protocol::requests::users::login_register::BindSecret, IggyError> {
        self.session_owner.bind_secret()
    }

    async fn bind_vsr_session(&self, session: u64) -> Result<(), IggyError> {
        self.bind_vsr_session_at(self.session_fence(), session)
            .await
    }

    fn session_fence(&self) -> u64 {
        self.session_owner.fence()
    }

    async fn bind_vsr_session_at(&self, fence: u64, session: u64) -> Result<(), IggyError> {
        let first_bind = self
            .session_owner
            .bind_at(fence, session)
            .map_err(|error| {
                // The reset of an explicit disconnect moved the fence, and the
                // caller needs that reason, not the session the reply named.
                if self.caller_disconnected() {
                    IggyError::NotConnected
                } else {
                    error
                }
            })?;
        if first_bind {
            self.consumer_group_state.clear_session_scoped();
            self.poll_router.clear_session();
        }
        Ok(())
    }

    async fn reset_vsr_session(&self) -> Result<(), IggyError> {
        self.session_owner.reset()?;
        self.consumer_group_state.clear_session_scoped();
        self.poll_router.clear_session();
        Ok(())
    }

    async fn remember_session_credentials(&self, credentials: Credentials, user_id: u32) {
        // `disconnect()` raises the flag before it forgets the sign-in, so a
        // sign-in answered after that cannot bring the credentials back.
        if !self
            .session_owner
            .remember_unless(|| self.caller_disconnected(), credentials, user_id)
            || self.auto_login_configured()
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
        self.session_owner.forget();
    }

    async fn refresh_session_password(&self, user: &iggy_common::Identifier, new_password: &str) {
        self.session_owner.refresh_password(user, new_password);
    }

    async fn refresh_session_username(&self, user: &iggy_common::Identifier, new_username: &str) {
        self.session_owner.refresh_username(user, new_username);
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
        let mut client =
            Self::with_session_owner(Arc::new(config), Arc::clone(&self.session_owner))?;
        // The shared owner holds the sign-in of this client, and a poll client
        // must never sign in on the shared session.
        *client.skip_auto_login_once.get_mut() = true;
        client.connect_off_leader().await?;
        Ok(client)
    }

    async fn send_poll_request_with_context(
        &self,
        code: u32,
        payload: Bytes,
        context: PartitionContext,
    ) -> Result<Bytes, IggyError> {
        self.send_raw_request(code, payload, false, &mut RetainedRequest::new(context))
            .await
    }
}

impl WebSocketClient {
    /// Whether fallback `AutoLogin` credentials are configured. A remembered
    /// successful sign-in takes precedence after reconnecting.
    pub(crate) fn auto_login_configured(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_))
    }
    /// Create a new WebSocket client with the provided configuration.
    ///
    /// Returns [`IggyError::InvalidConfiguration`] if the maximum write buffer
    /// size does not exceed the write buffer size.
    pub fn create(config: Arc<WebSocketClientConfig>) -> Result<Self, IggyError> {
        let session_owner = Arc::new(SessionOwner::new(config.auto_login.clone()));
        Self::with_session_owner(config, session_owner)
    }

    /// A poll client passes the owner of the client that dials it.
    fn with_session_owner(
        config: Arc<WebSocketClientConfig>,
        session_owner: Arc<SessionOwner>,
    ) -> Result<Self, IggyError> {
        let ws_config = config.ws_config.to_tungstenite_config();
        if ws_config.max_write_buffer_size <= ws_config.write_buffer_size {
            error!("WebSocket max_write_buffer_size must be greater than write_buffer_size");
            return Err(IggyError::InvalidConfiguration);
        }

        let server_address = config.server_address.clone();
        Ok(WebSocketClient {
            poll_router: PollRouter::new(Arc::clone(&session_owner)),
            link: Arc::new(Mutex::new(None)),
            config,
            state: Mutex::new(ClientState::Disconnected),
            client_address: Mutex::new(None),
            events: event_channel(),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address),
            session_owner,
            skip_auto_login_once: Mutex::new(false),
            roster_endpoints: Mutex::new(Vec::new()),
            roster_learned: AtomicBool::new(false),
            disconnected_by_caller: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
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

    pub(crate) async fn connect(&self) -> Result<(), IggyError> {
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
                        self.connect_inner(context).await?;
                        // A disconnect after the last check of the sweep, such
                        // as one during its sign-in, ends the sweep here, so
                        // nothing that the sweep set up outlives it.
                        if self.caller_disconnected() {
                            self.disconnect_transport().await?;
                            return Err(IggyError::NotConnected);
                        }
                        Ok(())
                    })
                    .await
            })
            .await
    }

    /// A reconnect sweep checks this before each dial, before it installs the
    /// link and when it ends: once the caller disconnects, nothing in the
    /// sweep may bring the client back.
    fn caller_disconnected(&self) -> bool {
        self.disconnected_by_caller.load(Ordering::SeqCst)
    }

    /// A reconnect sweep checks this before each dial and after it installs
    /// the link: once the client is shut down or the caller disconnects,
    /// nothing in the sweep may bring the client back.
    async fn sweep_interrupted(&self) -> Option<IggyError> {
        if self.get_state().await == ClientState::Shutdown {
            return Some(IggyError::ClientShutdown);
        }
        self.caller_disconnected()
            .then_some(IggyError::NotConnected)
    }

    async fn fail_connect(&self) {
        // The state stays `Shutdown`, and a `Disconnected` event would
        // contradict the `Shutdown` one.
        if self.get_state().await == ClientState::Shutdown {
            return;
        }
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
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
                _ if self.caller_disconnected() => {
                    trace!("Cannot connect. The caller disconnected the client.");
                    return Err(IggyError::NotConnected);
                }
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

                if let Some(error) = self.sweep_interrupted().await {
                    self.fail_connect().await;
                    return Err(error);
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
                let link = match connection_result {
                    Ok(link) => link,
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

                *self.link.lock().await = Some(link);
                // Checked once the link is installed, not before: `shutdown()`
                // sets `Shutdown` before it takes the link, so a shutdown
                // during the dial either takes this link or is seen here.
                if let Some(error) = self.sweep_interrupted().await {
                    self.link.lock().await.take();
                    self.fail_connect().await;
                    return Err(error);
                }
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
                        // After an explicit disconnect, the next connect
                        // starts from the endpoint the client was on.
                        if !self.caller_disconnected()
                            && let Some(next) = walk.next()
                        {
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
        self.link.lock().await.take();
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Ok(())
    }

    async fn connect_plain(
        &self,
        server_addr: SocketAddr,
        retry_count: &mut u32,
        single_attempt: bool,
    ) -> Result<MuxLink, IggyError> {
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

        Ok(MuxLink::websocket(websocket_stream, server_addr))
    }

    async fn connect_tls(
        &self,
        server_addr: SocketAddr,
        retry_count: &mut u32,
        single_attempt: bool,
    ) -> Result<MuxLink, IggyError> {
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

        Ok(MuxLink::websocket(websocket_stream, server_addr))
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
        let Some(credentials) = self.session_owner.sign_in_credentials() else {
            return Ok(false);
        };
        if self.session_owner.session()?.is_bound() {
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
            // An explicit disconnect landed during the sign-in. The
            // non-retryable arm below would keep the link up.
            if self.caller_disconnected() {
                self.disconnect_transport().await?;
                return Err(IggyError::NotConnected);
            }
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
        // Taken before the state is read: a teardown aborted after it set
        // `Disconnected` can leave the link installed.
        let link = self.link.lock().await.take();
        if link.is_none() && self.get_state().await == ClientState::Disconnected {
            return Ok(());
        }

        let client_address = self.get_client_address_value().await;
        info!("{NAME} client: {client_address} is disconnecting from server...");
        self.set_state(ClientState::Disconnected).await;
        drop(link);

        self.publish_event(DiagnosticEvent::Disconnected).await;
        let now = IggyTimestamp::now();
        info!("{NAME} client: {client_address} has disconnected from server at: {now}.");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        if self.shutdown_complete.load(Ordering::SeqCst) {
            return Ok(());
        }

        let client_address = self.get_client_address_value().await;
        info!("Shutting down the {NAME} client: {client_address}");

        // Set before the link is taken: a reconnect sweep that installs a link
        // after this point sees `Shutdown` and drops it.
        self.set_state(ClientState::Shutdown).await;

        let link = self.link.lock().await.take();
        if let Some(link) = link {
            link.shutdown().await;
        }

        self.reset_vsr_session().await?;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        self.shutdown_complete.store(true, Ordering::SeqCst);
        info!("{NAME} client: {client_address} has been shutdown.");
        Ok(())
    }

    async fn send_raw_retaining_header(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut RetainedRequest,
    ) -> Result<Bytes, IggyError> {
        self.send_raw_request(code, payload, true, header).await
    }

    async fn replay_after_reconnect(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut RetainedRequest,
        original_error: &IggyError,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, IggyError> {
        let current = self.current_server_address.lock().await.clone();
        let roster = self.roster_endpoints.lock().await.clone();
        let mut walk = RosterWalk::new(&current, &roster);
        'replay: loop {
            retain_replay_header(header, &self.session_owner, code, original_error)?;
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
        header: &mut RetainedRequest,
    ) -> Result<Bytes, IggyError> {
        // One deadline bounds the whole request including transient replays
        // and lifecycle retries.
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
        let mut retry_outcome = crate::vsr::RetryOutcome::default();
        let mut lifecycle_retry_interval = NOT_READY_RETRY_INTERVAL;
        loop {
            match self
                .send_raw_exchange(
                    code,
                    payload.clone(),
                    retry_transient,
                    header,
                    retry_deadline,
                    not_accepted_deadline,
                    &mut retry_outcome,
                )
                .await
            {
                // The refusal committed under this request id, so the retry is a
                // new request. The pause holds no link lock, and the next
                // exchange takes its id only once it holds the link, above any
                // id another request sent during the pause.
                Err(IggyError::LifecycleBusy)
                    if retry_transient
                        && tokio::time::Instant::now() + lifecycle_retry_interval
                            < retry_deadline =>
                {
                    header.header = None;
                    tokio::time::sleep(lifecycle_retry_interval).await;
                    lifecycle_retry_interval = (lifecycle_retry_interval * 2)
                        .min(crate::vsr::LIFECYCLE_RETRY_MAX_INTERVAL);
                }
                result => return result,
            }
        }
    }

    /// The part of [`Self::send_raw_request`] that holds the link lock.
    /// `retry_outcome` carries over between the exchanges of one request.
    #[allow(clippy::too_many_arguments)]
    async fn send_raw_exchange(
        &self,
        code: u32,
        payload: Bytes,
        retry_transient: bool,
        header: &mut RetainedRequest,
        retry_deadline: tokio::time::Instant,
        not_accepted_deadline: tokio::time::Instant,
        retry_outcome: &mut crate::vsr::RetryOutcome,
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

        let link = self.link.clone();
        let session_owner = Arc::clone(&self.session_owner);
        // A task finishes the exchange when its caller goes away. An unbound
        // request that left before its reply would close the link.
        let preencoded = *header;
        let mut outcome_so_far = std::mem::take(retry_outcome);
        let (used_header, result, outcome_so_far) = tokio::spawn(async move {
            let mut used_header = preencoded;
            let result = async {
                let mut link_guard = link.lock().await;
                let Some(link) = link_guard.as_ref() else {
                    trace!("Cannot send data. Client is not connected.");
                    return Err(IggyError::NotConnected);
                };

                // Encode the request ONCE: `next_request_id` advances here, so a
                // transient replay must reuse the same id for the server's dedup.
                // A `TransientNotCommitted` answer (the server could not commit
                // yet) lets us resend the SAME request on the SAME connection with
                // no reconnect and the session intact. Bounded by
                // RESPONSE_READ_TIMEOUT.
                let request_header = {
                    let mut session = session_owner.session()?;
                    used_header.encode(&mut session, code, &payload)?
                };
                trace!(
                    "Sending {NAME} VSR request of size {} with code: {code}",
                    request_header.size
                );
                let mut link_failed = false;
                let mut replay = false;
                let outcome = loop {
                    let frame = Frame {
                        header: request_header,
                        body: payload.clone(),
                    };
                    let reply = match link.send(frame, retry_deadline, &mut false).await {
                        Ok(reply) => reply,
                        Err(error) => {
                            link_failed = true;
                            break Err(outcome_so_far.observe_link_error(error, replay));
                        }
                    };
                    replay = true;
                    session_owner.observe_metadata_reply(&reply.header);
                    match crate::vsr::decode_response_split(&reply.header, reply.body)
                        .map_err(|error| outcome_so_far.observe(error))
                    {
                        Err(error) if !retry_transient => break Err(error),
                        Err(IggyError::TransientNotAccepted)
                            if tokio::time::Instant::now() >= not_accepted_deadline =>
                        {
                            // Never admitted, so re-issuable anywhere: hand it
                            // back for a leader recheck or a roster walk instead
                            // of replaying into the same refusal for the whole
                            // request budget.
                            break Err(IggyError::TransientNotAccepted);
                        }
                        // The server answered with a transient frame. Replaying
                        // the same request id on this session preserves metadata
                        // dedup even when the original outcome is still
                        // resolving.
                        Err(IggyError::TransientNotCommitted | IggyError::TransientNotAccepted)
                            if tokio::time::Instant::now() < retry_deadline =>
                        {
                            let remaining = retry_deadline
                                .saturating_duration_since(tokio::time::Instant::now());
                            tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                        }
                        other => break other,
                    }
                };
                // A failed send leaves the link closed or its reply unread, so
                // the next request needs a new connection.
                if link_failed {
                    link_guard.take();
                }
                outcome
            }
            .await;
            (used_header, result, outcome_so_far)
        })
        .await
        .map_err(|error| {
            error!("Task execution failed during {NAME} request: {error}");
            IggyError::WebSocketSendError
        })?;
        *header = used_header;
        *retry_outcome = outcome_so_far;
        result
    }
}

const fn is_login_register_code(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::links::EVENT_CHANNEL_CAPACITY;
    use crate::links::test_support::{
        SIGNED_IN_USER_ID, register_reply, sign_in_reply, sign_in_steps_after_a_disconnect,
        spawn_disconnect_behind_the_link, spawn_restart_behind_the_link,
    };
    use std::str::FromStr;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use iggy_binary_protocol::codes::{GET_ME_CODE, JOIN_CONSUMER_GROUP_CODE, SEND_MESSAGES_CODE};
    use iggy_binary_protocol::consensus::{
        EvictionHeader, EvictionReason, REJECTION_SECTION_LEN, write_rejection_section,
    };
    use iggy_binary_protocol::requests::system::BindSessionRequest;
    use iggy_binary_protocol::responses::users::LoginRegisterResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, IGGY_PROTOCOL_VERSION, Operation, ReplyHeader, RequestHeader,
        WireDecode, WireEncode, WireName,
    };
    use iggy_common::{Identifier, WebSocketClientReconnectionConfig};
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
    async fn given_poisoned_session_when_sending_or_resetting_should_return_configuration_error() {
        const NO_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address: listener.local_addr().unwrap().to_string(),
            ..Default::default()
        }))
        .unwrap();
        let (mut peer, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&client)
        );
        connected.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let session_owner = Arc::clone(&client.session_owner);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _session = session_owner.session().unwrap();
                panic!("poison the session for the regression test");
            }))
            .is_err()
        );

        let mut request = crate::vsr::RetainedRequest::default();
        assert_eq!(
            client
                .send_raw_request(
                    SEND_MESSAGES_CODE,
                    Bytes::from_static(b"send"),
                    false,
                    &mut request,
                )
                .await,
            Err(IggyError::InvalidConfiguration)
        );
        assert!(request.header.is_none());
        assert_eq!(
            client.reset_vsr_session().await,
            Err(IggyError::InvalidConfiguration)
        );
        {
            let session = session_owner.session_ignoring_poison();
            assert_eq!(session.session(), Some(1));
            assert_eq!(session.current_request_id(), 1);
        }
        assert!(
            tokio::time::timeout(NO_REQUEST_TIMEOUT, peer.next())
                .await
                .is_err()
        );
        client.disconnect_transport().await.unwrap();
    }

    #[tokio::test]
    async fn a_retained_write_waiting_for_the_link_must_recheck_its_session() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address: listener.local_addr().unwrap().to_string(),
            ..Default::default()
        }))
        .unwrap();
        let (mut peer, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&client)
        );
        connected.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let payload = Bytes::from_static(b"uncertain-send");
        let retained = crate::vsr::encode_request_header(
            &mut client.session_owner.session().unwrap(),
            SEND_MESSAGES_CODE,
            &payload,
        )
        .unwrap()
        .0;
        let link_guard = client.link.lock().await;
        let mut retained = RetainedRequest {
            header: Some(retained),
            ..Default::default()
        };
        let attempt = client.send_raw_request(SEND_MESSAGES_CODE, payload, false, &mut retained);
        tokio::pin!(attempt);
        tokio::select! {
            biased;
            _ = &mut attempt => panic!("the exchange must wait for the link lock"),
            () = tokio::task::yield_now() => {}
        }
        client.reset_vsr_session().await.unwrap();
        client.bind_vsr_session(2).await.unwrap();
        drop(link_guard);
        assert_eq!(
            tokio::time::timeout(TEST_BUDGET, &mut attempt)
                .await
                .unwrap(),
            Err(IggyError::TransientNotCommitted)
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), peer.next())
                .await
                .is_err()
        );
        Client::shutdown(&client).await.unwrap();
    }

    #[tokio::test]
    async fn given_a_request_without_a_reply_when_its_deadline_passes_should_drop_the_link() {
        const READ_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address: listener.local_addr().unwrap().to_string(),
            ..Default::default()
        }))
        .unwrap();
        let (mut peer, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&client)
        );
        connected.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let deadline = tokio::time::Instant::now() + READ_BUDGET;
        let mut header = RetainedRequest::default();
        let mut retry_outcome = crate::vsr::RetryOutcome::default();

        let (result, _) = tokio::join!(
            client.send_raw_exchange(
                GET_ME_CODE,
                Bytes::new(),
                false,
                &mut header,
                deadline,
                deadline,
                &mut retry_outcome,
            ),
            read_test_request(&mut peer)
        );

        assert_eq!(result, Err(IggyError::Disconnected));
        assert!(client.link.lock().await.is_none());
        let end = tokio::time::timeout(TEST_BUDGET, peer.next())
            .await
            .unwrap();
        assert!(!matches!(end, Some(Ok(Message::Binary(_)))), "{end:?}");
    }

    #[tokio::test]
    async fn given_an_uncertain_write_when_the_replay_loses_the_link_should_stay_uncertain() {
        const READ_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        for (evict_on_replay, expected) in [
            (true, IggyError::TransientNotCommitted),
            (false, IggyError::Disconnected),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
                server_address: listener.local_addr().unwrap().to_string(),
                ..Default::default()
            }))
            .unwrap();
            let (mut peer, connected) = tokio::join!(
                async {
                    accept_async(listener.accept().await.unwrap().0)
                        .await
                        .unwrap()
                },
                Client::connect(&client)
            );
            connected.unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let deadline = tokio::time::Instant::now() + READ_BUDGET;
            let mut header = RetainedRequest::default();
            let mut retry_outcome = crate::vsr::RetryOutcome::default();
            let answer = async move {
                let (request, _) = read_test_request(&mut peer).await;
                let uncertain = IggyError::TransientNotCommitted.as_code();
                answer_test_request(&mut peer, &request, uncertain, &[]).await;
                // Without an eviction the connection closes before the replay.
                if evict_on_replay {
                    read_test_request(&mut peer).await;
                    let eviction =
                        EvictionHeader::new(0, 0, 0, request.client, EvictionReason::SessionError);
                    let frame = bytemuck::bytes_of(&eviction).to_vec();
                    peer.send(Message::Binary(frame.into())).await.unwrap();
                }
            };

            let (result, ()) = tokio::join!(
                client.send_raw_exchange(
                    SEND_MESSAGES_CODE,
                    Bytes::new(),
                    true,
                    &mut header,
                    deadline,
                    deadline,
                    &mut retry_outcome,
                ),
                answer
            );

            assert_eq!(result, Err(expected));
        }
    }

    #[tokio::test]
    async fn given_lifecycle_refusal_when_retried_should_not_block_other_requests() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Arc::new(
            WebSocketClient::create(Arc::new(WebSocketClientConfig {
                server_address: listener.local_addr().unwrap().to_string(),
                ..Default::default()
            }))
            .unwrap(),
        );
        let (mut stream, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&*client)
        );
        connected.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let (refused, first_refusal) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut rejection = [0; REJECTION_SECTION_LEN];
            write_rejection_section(&mut rejection, IggyError::LifecycleBusy.as_code());
            let mut refused = Some(refused);
            let mut other = None;
            loop {
                let (request, _) = read_test_request(&mut stream).await;
                if request.operation == Operation::SendMessages {
                    answer_test_request(&mut stream, &request, 0, b"sent").await;
                    other = Some(request);
                } else if let Some(other) = other {
                    // The server accepts only request ids above its watermark.
                    assert!(
                        request.request > other.request,
                        "a retry must take its request id after the requests sent during its pause"
                    );
                    let mut success = 0u32.to_le_bytes().to_vec();
                    success.extend_from_slice(b"joined");
                    answer_test_request(&mut stream, &request, 0, &success).await;
                    return;
                } else {
                    answer_test_request(&mut stream, &request, 0, &rejection).await;
                    if let Some(refused) = refused.take() {
                        refused.send(()).unwrap();
                    }
                }
            }
        });
        let join = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                client
                    .send_raw_request(
                        JOIN_CONSUMER_GROUP_CODE,
                        Bytes::new(),
                        true,
                        &mut RetainedRequest::default(),
                    )
                    .await
            })
        };
        first_refusal.await.unwrap();
        // Start inside the first pause, so a retry that took its request id
        // before the pause would reach the peer below this request's id.
        tokio::time::sleep(NOT_READY_RETRY_INTERVAL / 2).await;

        let sent = tokio::time::timeout(
            TEST_BUDGET,
            client.send_raw_request(
                SEND_MESSAGES_CODE,
                Bytes::from_static(b"send-body"),
                true,
                &mut RetainedRequest::default(),
            ),
        )
        .await
        .expect("a request must not wait out the lifecycle retries of another request");
        assert_eq!(sent.unwrap(), Bytes::from_static(b"sent"));
        tokio::time::timeout(TEST_BUDGET, peer)
            .await
            .unwrap()
            .unwrap();
        let joined = tokio::time::timeout(TEST_BUDGET, join)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(joined.unwrap(), Bytes::from_static(b"joined"));
        Client::shutdown(&*client).await.unwrap();
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
                client.send_raw_with_context(
                    SEND_MESSAGES_CODE,
                    Bytes::from_static(b"lost-send"),
                    PartitionContext::default(),
                ),
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
                        .send_raw_with_context(
                            SEND_MESSAGES_CODE,
                            Bytes::from_static(b"explicit-new-send"),
                            PartitionContext::default()
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

    #[tokio::test]
    async fn a_reconnect_after_an_explicit_disconnect_is_refused() {
        let client = WebSocketClient::default();
        Client::disconnect(&client).await.unwrap();

        // The recovery path of a request that was in flight during the
        // disconnect calls the inherent `connect`, not the trait one.
        let reconnect = tokio::time::timeout(Duration::from_secs(1), client.connect())
            .await
            .expect("a refused reconnect must not dial");

        assert!(
            matches!(reconnect, Err(IggyError::NotConnected)),
            "got {reconnect:?}"
        );
    }

    #[tokio::test]
    async fn given_a_configured_auto_login_when_the_password_changes_and_the_client_disconnects_should_sign_in_with_the_new_password()
     {
        const SESSION_USER_ID: u32 = 7;
        let credentials = Credentials::UsernamePassword("iggy".to_owned(), "old".into());
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            auto_login: AutoLogin::Enabled(credentials.clone()),
            ..Default::default()
        }))
        .unwrap();
        client
            .remember_session_credentials(credentials, SESSION_USER_ID)
            .await;
        client
            .refresh_session_password(&Identifier::numeric(SESSION_USER_ID).unwrap(), "new")
            .await;

        Client::disconnect(&client).await.unwrap();

        match client.session_owner.sign_in_credentials() {
            Some(Credentials::UsernamePassword(username, password)) => {
                assert_eq!(username, "iggy");
                assert_eq!(password.expose_secret(), "new");
            }
            other => panic!("expected the configured user with the new password, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_late_steps_of_a_sign_in_leave_a_disconnected_client_signed_out() {
        let client = WebSocketClient::default();

        let state = sign_in_steps_after_a_disconnect(&client).await;

        assert_eq!(state, ClientState::Disconnected);
        assert!(client.session_owner.sign_in_credentials().is_none());
    }

    /// A client whose connection dropped after a sign-in, so the next sweep
    /// resumes its session.
    async fn client_to_resume(server_address: &str) -> Arc<WebSocketClient> {
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address: server_address.to_string(),
            ..Default::default()
        }))
        .unwrap();
        client.bind_vsr_session(1).await.unwrap();
        client.session_owner.remember(
            Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
            SIGNED_IN_USER_ID,
        );
        Arc::new(client)
    }

    #[tokio::test]
    async fn a_resume_answered_after_a_disconnect_lets_the_next_connect_dial() {
        const TEST_BUDGET: Duration = Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = client_to_resume(&listener.local_addr().unwrap().to_string()).await;
        // The reconnect that a request runs after its connection dropped.
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let mut peer = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (bind, _) = read_test_request(&mut peer).await;
        assert_eq!(
            u32::from_le_bytes(bind.reserved[..4].try_into().unwrap()),
            BIND_SESSION_CODE
        );

        let disconnect =
            spawn_disconnect_behind_the_link(&client, WebSocketClient::caller_disconnected).await;
        answer_test_request(&mut peer, &bind, 0, &sign_in_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();
        let state = client.get_state().await;
        let bound = client.session_owner.epoch().bound;

        // The upgrade completes only when the server answers it.
        let reconnect = tokio::spawn({
            let client = Arc::clone(&client);
            async move { Client::connect(&*client).await }
        });
        let redial = tokio::time::timeout(TEST_BUDGET, listener.accept()).await;
        reconnect.abort();
        assert!(
            redial.is_ok() && matches!(sweep, Err(IggyError::NotConnected)) && !bound,
            "sweep {sweep:?}, state after the disconnect {state:?}, \
             session after the disconnect bound {bound}, \
             connect() after the disconnect dialed {}",
            redial.is_ok()
        );
    }

    #[tokio::test]
    async fn a_login_answered_after_a_disconnect_does_not_sign_the_client_back_in() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Arc::new(
            WebSocketClient::create(Arc::new(WebSocketClientConfig {
                server_address: listener.local_addr().unwrap().to_string(),
                ..Default::default()
            }))
            .unwrap(),
        );
        let (mut peer, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&*client)
        );
        connected.unwrap();
        let login = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.login_user("iggy", "iggy").await.map(|_| ()) }
        });
        let (register, _) = read_test_request(&mut peer).await;
        assert_eq!(register.operation, Operation::Register);

        let disconnect =
            spawn_disconnect_behind_the_link(&client, WebSocketClient::caller_disconnected).await;
        answer_test_request(&mut peer, &register, 0, &register_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let login = login.await.unwrap();

        let state = client.get_state().await;
        let remembered = client.session_owner.sign_in_credentials().is_some();
        let bound = client.session_owner.epoch().bound;
        assert!(
            state == ClientState::Disconnected && !remembered && !bound,
            "login {login:?}, state {state:?}, remembered sign-in {remembered}, \
             session bound {bound}"
        );
    }

    #[tokio::test]
    async fn a_resume_refused_by_a_disconnect_keeps_the_endpoint_the_client_was_on() {
        const OTHER_NODE: &str = "127.0.0.1:9";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let client = client_to_resume(&address).await;
        *client.roster_endpoints.lock().await = vec![address.clone(), OTHER_NODE.to_string()];
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let mut peer = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (bind, _) = read_test_request(&mut peer).await;

        let disconnect =
            spawn_disconnect_behind_the_link(&client, WebSocketClient::caller_disconnected).await;
        answer_test_request(&mut peer, &bind, 0, &sign_in_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();

        let current = client.current_server_address.lock().await.clone();
        assert!(
            current == address,
            "sweep {sweep:?}, the next connect starts from {current}, not {address}"
        );
    }

    #[tokio::test]
    async fn a_connect_right_after_a_disconnect_survives_a_resume_in_flight() {
        const TEST_BUDGET: Duration = Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = client_to_resume(&listener.local_addr().unwrap().to_string()).await;
        // The reconnect that a request of another task runs.
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let mut peer = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let (bind, _) = read_test_request(&mut peer).await;

        let restart =
            spawn_restart_behind_the_link(&client, WebSocketClient::caller_disconnected).await;
        // The connect dials again, and the upgrade completes only when the
        // server answers it.
        let redial = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_async(stream).await.unwrap()
        });
        answer_test_request(&mut peer, &bind, 0, &sign_in_reply(1)).await;
        let connect = tokio::time::timeout(TEST_BUDGET, restart)
            .await
            .expect("the restart must return")
            .unwrap();
        let sweep = sweep.await.unwrap();
        redial.abort();

        let state = client.get_state().await;
        let link = client.link.lock().await.is_some();
        assert!(
            connect.is_ok() && link,
            "connect() right after disconnect() {connect:?}, the sweep {sweep:?}, \
             state {state:?}, link installed {link}"
        );
    }

    #[tokio::test]
    async fn a_bind_at_a_fence_from_before_a_reset_is_refused() {
        let client = WebSocketClient::default();
        let fence = client.session_fence();
        client.reset_vsr_session().await.unwrap();

        let bind = client.bind_vsr_session_at(fence, 1).await;

        let bound = client.session_owner.epoch().bound;
        assert!(
            matches!(bind, Err(IggyError::InvalidSession(_))) && !bound,
            "bind {bind:?}, session bound {bound}"
        );
    }

    #[tokio::test]
    async fn given_a_full_event_channel_when_publishing_should_not_block() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig::default())).unwrap();

        let publish = async {
            for _ in 0..=EVENT_CHANNEL_CAPACITY {
                client.publish_event(DiagnosticEvent::Disconnected).await;
            }
        };

        assert!(
            tokio::time::timeout(TEST_BUDGET, publish).await.is_ok(),
            "a publish waited for room that an unread receiver never frees"
        );
    }

    #[tokio::test]
    async fn a_shutdown_during_a_reconnect_sweep_ends_the_sweep() {
        let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = dead_listener.local_addr().unwrap().to_string();
        drop(dead_listener);
        let client = Arc::new(
            WebSocketClient::create(Arc::new(WebSocketClientConfig {
                server_address: address.clone(),
                reconnection: WebSocketClientReconnectionConfig {
                    enabled: true,
                    max_retries: None,
                    interval: NonZeroIggyDuration::from_str("500ms").unwrap(),
                    reestablish_after: IggyDuration::from_str("0s").unwrap(),
                },
                ..Default::default()
            }))
            .unwrap(),
        );
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { WebSocketClient::connect(&client).await }
        });
        // The first dial is refused at once, so the sweep now waits out its
        // retry interval.
        sleep(Duration::from_millis(100)).await;

        Client::shutdown(&*client).await.unwrap();
        // The server comes back before the next dial.
        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), sweep)
            .await
            .expect("the sweep must end after the shutdown")
            .expect("the sweep task must not panic");

        assert!(
            matches!(result, Err(IggyError::ClientShutdown)),
            "got {result:?}"
        );
        assert_eq!(client.get_state().await, ClientState::Shutdown);
        assert!(
            client.link.lock().await.is_none(),
            "no link may be installed after the shutdown"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "the sweep must not dial after the shutdown"
        );
    }

    #[tokio::test]
    async fn a_cancelled_shutdown_is_finished_by_the_next_call() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = WebSocketClient::create(Arc::new(WebSocketClientConfig {
            server_address: listener.local_addr().unwrap().to_string(),
            ..Default::default()
        }))
        .unwrap();
        let (mut peer, connected) = tokio::join!(
            async {
                accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap()
            },
            Client::connect(&client)
        );
        connected.unwrap();
        let mut events = client.subscribe_events().await;
        {
            let _transport = client.link.lock().await;
            // The shutdown waits for the transport lock, and its caller gives up.
            assert!(
                tokio::time::timeout(Duration::from_millis(100), Client::shutdown(&client))
                    .await
                    .is_err()
            );
        }

        Client::shutdown(&client).await.unwrap();

        assert_eq!(client.get_state().await, ClientState::Shutdown);
        assert!(client.link.lock().await.is_none());
        let next = tokio::time::timeout(Duration::from_secs(1), peer.next())
            .await
            .expect("the peer must see the connection close");
        assert!(
            matches!(next, None | Some(Ok(Message::Close(_))) | Some(Err(_))),
            "got {next:?}"
        );
        let mut shutdowns = 0;
        while let Ok(event) = events.try_recv() {
            if event == DiagnosticEvent::Shutdown {
                shutdowns += 1;
            }
        }
        assert_eq!(shutdowns, 1);
    }
}
