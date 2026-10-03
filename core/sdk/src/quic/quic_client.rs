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
use crate::prelude::AutoLogin;
use crate::session::ConsensusSession;
use crate::vsr::retain_replay_header;
use iggy_common::VsrSessionControl as _;
use iggy_common::{BinaryClient, BinaryTransport, Client, PersonalAccessTokenClient, UserClient};

use crate::prelude::{
    IggyDuration, IggyError, IggyTimestamp, NonZeroIggyDuration, QuicClientConfig,
};
use crate::quic::skip_server_verification::SkipServerVerification;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
};
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
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

const NAME: &str = "Iggy";

/// Bound on how long a single QUIC request waits for its response, mirroring the
/// TCP client's `RESPONSE_READ_TIMEOUT`. A replicated request that the server
/// cannot commit transiently is answered with silence - the server expects the
/// SDK read-timeout to drive a replay. `RecvStream::read_to_end` on an
/// unanswered bidi stream otherwise blocks until the QUIC idle timeout (which
/// can be minutes), parking that request and, because the connection is held
/// under lock per send, every later request on the client.
const RESPONSE_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Backoff before replaying a request the server answered with an explicit
/// `TransientNotCommitted` frame (not-caught-up / in-flight / pipeline-full /
/// view-change cancel). Unlike a silent timeout, this reply arrives promptly, so
/// a short pause keeps the replay from spinning while the primary catches up.
/// Bounded overall by `RESPONSE_READ_TIMEOUT`.
const NOT_READY_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// How long a request replays `TransientNotAccepted` on the SAME connection
/// before it is handed back for a leader recheck or a roster walk. A node
/// that is not the target group's primary refuses forever, so replaying on it
/// for the whole request budget would burn the budget against a verdict.
const TRANSIENT_FAILOVER_CHECK_INTERVAL: Duration = Duration::from_secs(2);

/// QUIC client for interacting with the Iggy API.
#[derive(Debug)]
pub struct QuicClient {
    poll_router: PollRouter<Self>,
    pub(crate) endpoint: Endpoint,
    pub(crate) connection: Arc<Mutex<Option<Connection>>>,
    pub(crate) config: Arc<QuicClientConfig>,
    pub(crate) state: Mutex<ClientState>,
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
    /// concurrent QUIC streams cannot tear down each other's new connection.
    routing_lock: Mutex<()>,
    connect_coordinator: ConnectCoordinator,
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
        self.forget_session_credentials().await;
        QuicClient::disconnect_transport(self).await?;
        self.reset_vsr_session().await
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
            error!("Failed to send a QUIC diagnostic event: {error}");
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
                    &*self
                        .consensus_session
                        .lock()
                        .map_err(|_| IggyError::InvalidConfiguration)?,
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
                | IggyError::QuicError
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
            // Without auto-login a reconnect cannot re-establish the session, so
            // non-login requests are not recovered here - their transient replay
            // happens on the live connection inside `send_raw`. Login/register
            // is the exception: the server stays deliberately silent on a
            // transient register failure and relies on the client replaying via
            // a reconnect with a fresh session.
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
            retain_replay_header(
                &mut header,
                &*self
                    .consensus_session
                    .lock()
                    .map_err(|_| IggyError::InvalidConfiguration)?,
                code,
                &error,
            )?;
            drop(_routing_guard);
            return self
                .send_raw_retaining_header(code, payload, &mut header)
                .await
                .map_err(|error| retry_outcome.observe(error));
        }
        self.disconnect_transport()
            .await
            .map_err(|error| retry_outcome.observe(error))?;
        if skip_auto_login {
            *self.skip_auto_login_once.lock().await = true;
        }
        let server_address = self.current_server_address.lock().await.to_string();
        info!(
            "Reconnecting to the server: {}, by client: {}",
            server_address, self.config.client_address
        );
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
        retain_replay_header(
            &mut header,
            &*self
                .consensus_session
                .lock()
                .map_err(|_| IggyError::InvalidConfiguration)?,
            code,
            &error,
        )?;
        drop(_routing_guard);
        self.send_raw_retaining_header(code, payload, &mut header)
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

impl iggy_common::VsrSessionSealed for QuicClient {}

#[async_trait::async_trait]
impl iggy_common::VsrSessionControl for QuicClient {
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
        let read = read_transport_endpoints(self, TransportProtocol::Quic);
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

impl BinaryClient for QuicClient {}

#[async_trait]
impl PollTransport for QuicClient {
    const PROTOCOL: TransportProtocol = TransportProtocol::Quic;

    async fn connect_poll_client(&self, endpoint: &str) -> Result<Self, IggyError> {
        let mut config = (*self.config).clone();
        config.server_address = endpoint.to_owned();
        config.auto_login = AutoLogin::Disabled;
        config.reconnection.enabled = false;
        let mut client_address = config
            .client_address
            .parse::<SocketAddr>()
            .map_err(|_| IggyError::InvalidClientAddress)?;
        client_address.set_port(0);
        config.client_address = client_address.to_string();
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

impl QuicClient {
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
            poll_router: PollRouter::default(),
            config,
            endpoint,
            connection: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
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
        metadata_watermark: &std::sync::atomic::AtomicU64,
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

        if let Some(header) = buffer
            .get(..iggy_binary_protocol::HEADER_SIZE)
            .and_then(|header| header.try_into().ok())
        {
            crate::vsr::observe_metadata_reply(metadata_watermark, header);
        }
        crate::vsr::decode_response(Bytes::from(buffer))
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
        'connect: loop {
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
            if !single_attempt && let Some(connected_at) = self.connected_at.lock().await.as_ref() {
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
                let connection_result = match self
                    .endpoint
                    .connect(server_address, &self.config.server_name)
                {
                    Ok(connecting) => connecting.await,
                    Err(error) => {
                        error!("Failed to start QUIC connection: {error}");
                        self.set_state(ClientState::Disconnected).await;
                        self.publish_event(DiagnosticEvent::Disconnected).await;
                        return Err(IggyError::CannotEstablishConnection);
                    }
                };

                if connection_result.is_err() {
                    error!("Failed to connect to server: {}", server_address);
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

            let skip_auto_login = {
                let mut guard = self.skip_auto_login_once.lock().await;
                std::mem::take(&mut *guard)
            };

            if skip_auto_login {
                return Ok(());
            }
            let Some(credentials) = self.sign_in_credentials() else {
                return Ok(());
            };
            let bound = self
                .consensus_session
                .lock()
                .map_err(|_| IggyError::InvalidConfiguration)?
                .is_bound();
            let resumed = if bound {
                match self.resume_vsr_session().await {
                    Ok(()) => true,
                    Err(IggyError::Unauthenticated | IggyError::TransientNotCommitted) => {
                        self.reset_vsr_session().await?;
                        false
                    }
                    Err(error) => {
                        self.disconnect_transport().await?;
                        if !single_attempt && crate::vsr::resume_error_is_retryable(&error) {
                            let current = self.current_server_address.lock().await.clone();
                            let roster = self.roster_endpoints.lock().await.clone();
                            let walk = resume_walk
                                .get_or_insert_with(|| RosterWalk::new(&current, &roster));
                            if let Some(next) = walk.next() {
                                *self.current_server_address.lock().await = next;
                                continue 'connect;
                            }
                        }
                        return Err(error);
                    }
                }
            } else {
                false
            };
            if !resumed {
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
                self.publish_event(DiagnosticEvent::SignedIn).await;
            }
            let should_redirect = if settle_off_leader {
                false
            } else {
                self.handle_leader_redirection().await?
            };

            if should_redirect {
                continue;
            }

            return Ok(());
        }
    }

    async fn clear_abandoned_connect(&self) -> Result<(), IggyError> {
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close(0u32.into(), b"");
        }
        self.endpoint.wait_idle().await;
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Ok(())
    }

    /// Checks cluster metadata and handles leader redirection if needed.
    /// Returns true if redirection occurred and reconnection is needed.
    pub(crate) async fn handle_leader_redirection(&self) -> Result<bool, IggyError> {
        let current_address = self.current_server_address.lock().await.clone();
        let leader_check = check_and_redirect_to_leader(
            self,
            &current_address,
            iggy_common::TransportProtocol::Quic,
        )
        .await?;
        // Replaced wholesale rather than merged: the roster is the cluster's
        // own answer about where its nodes are. Kept for the roster walk a
        // persistently refused request runs, not for dead-node redial (which
        // remains TCP-only).
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
        self.reset_vsr_session().await?;
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} QUIC client has been shutdown.");
        Ok(())
    }

    async fn disconnect_transport(&self) -> Result<(), IggyError> {
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

    async fn send_raw_retaining_header(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut Option<iggy_binary_protocol::RequestHeader>,
    ) -> Result<Bytes, IggyError> {
        self.send_raw_request(code, payload, true, header).await
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
        let consensus_session = self.consensus_session.clone();
        let metadata_watermark = Arc::clone(&self.poll_router.metadata_watermark);
        // SAFETY: we run code holding the `connection` lock in a task so we can't be cancelled while holding the lock.
        let preencoded = *header;
        let (used_header, result) = tokio::spawn(async move {
            let mut used_header = preencoded;
            let result = async {
                let connection = connection.lock().await;
                let Some(connection) = connection.as_ref() else {
                    error!("Cannot send data. Client is not connected.");
                    return Err(IggyError::NotConnected);
                };

                let request_header = match preencoded {
                    Some(header) => header,
                    None => {
                        let mut session = consensus_session
                        .lock()
                        .expect("consensus session mutex poisoned");
                        crate::vsr::encode_request_header(&mut session, code, &payload)?.0
                    }
                };
                used_header = Some(request_header);
                trace!(
                    "Sending a QUIC VSR request of size {} with code: {code}",
                    request_header.size
                );
                // Replays retain the exact identity so committed receipts resolve
                // uncertain attempts. Silence alone does not authorize a replay.
                let header_bytes = bytemuck::bytes_of(&request_header);
                let deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
                // `TransientNotAccepted` gets a short same-connection window
                // only: past it the refusal is a verdict about who leads, not
                // load, and the caller runs a leader recheck or roster walk.
                // Login/register keeps the full budget on this connection: the
                // connect flow owns its leader settlement.
                let not_accepted_deadline = if is_login_register_code(code) {
                    deadline
                } else {
                    deadline.min(tokio::time::Instant::now() + TRANSIENT_FAILOVER_CHECK_INTERVAL)
                };
                let mut retry_outcome = crate::vsr::RetryOutcome::default();
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
                    match QuicClient::handle_response(
                        &mut recv,
                        response_buffer_size as usize,
                        remaining,
                        &metadata_watermark,
                    )
                    .await
                    .map_err(|error| retry_outcome.observe(error))
                    {
                        Ok(reply) => return Ok(reply),
                        Err(error) if !retry_transient => return Err(error),
                        // `TransientNotCommitted` = the server replied with an
                        // explicit retry frame with an outcome that may still
                        // be resolving (not-caught-up / in-flight /
                        // pipeline-full / view-change cancel). Replaying the
                        // same request id on the same session is safe because
                        // metadata dedup returns the committed reply if needed.
                        // Anything else, including a silent read timeout, is
                        // terminal here and handled by the caller.
                        Err(IggyError::TransientNotAccepted)
                            if tokio::time::Instant::now() >= not_accepted_deadline =>
                        {
                            // Never admitted, so re-issuable anywhere: hand it
                            // back for a leader recheck or a roster walk
                            // instead of replaying into the same refusal for
                            // the whole request budget.
                            return Err(IggyError::TransientNotAccepted);
                        }
                        Err(IggyError::TransientNotCommitted | IggyError::TransientNotAccepted)
                            if tokio::time::Instant::now() < deadline =>
                        {
                            // The explicit frame returns promptly (no read
                            // timeout elapsed), so pace the replay.
                            let remaining =
                                deadline.saturating_duration_since(tokio::time::Instant::now());
                            tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                            warn!(
                                "QUIC request code {code} not committed (transient); resending on a new stream"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            .await;
            (used_header, result)
        })
        .await
        .map_err(|e| {
            error!("Task execution failed during QUIC request: {}", e);
            IggyError::QuicError
        })?;
        *header = used_header;
        result
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    use iggy_binary_protocol::codes::SEND_MESSAGES_CODE;
    use iggy_binary_protocol::requests::system::BindSessionRequest;
    use iggy_binary_protocol::responses::users::LoginRegisterResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, IGGY_PROTOCOL_VERSION, Operation, ReplyHeader, RequestHeader,
        WireDecode, WireEncode, WireName,
    };
    use iggy_common::QuicClientReconnectionConfig;

    async fn read_test_request(
        connection: &quinn::Connection,
    ) -> (quinn::SendStream, RequestHeader, Vec<u8>) {
        let (send, mut recv) = connection.accept_bi().await.unwrap();
        let mut bytes = [0; HEADER_SIZE];
        recv.read_exact(&mut bytes).await.unwrap();
        let header: RequestHeader = bytemuck::checked::try_pod_read_unaligned(&bytes).unwrap();
        let mut body = vec![0; header.size as usize - HEADER_SIZE];
        recv.read_exact(&mut body).await.unwrap();
        (send, header, body)
    }

    async fn answer_test_request(
        send: &mut quinn::SendStream,
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
        send.write_all(bytemuck::bytes_of(&header)).await.unwrap();
        send.write_all(body).await.unwrap();
        send.finish().unwrap();
        send.stopped().await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_reply_resumes_and_replays_only_the_original_session() {
        const TEST_USER_ID: u32 = 7;
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        for fresh_login in [false, true] {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let certified =
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let config = quinn::ServerConfig::with_single_cert(
                vec![certified.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der())
                    .into(),
            )
            .unwrap();
            let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
            let client = QuicClient::create(Arc::new(QuicClientConfig {
                server_address: endpoint.local_addr().unwrap().to_string(),
                reconnection: QuicClientReconnectionConfig {
                    reestablish_after: IggyDuration::from_str("0s").unwrap(),
                    max_retries: Some(0),
                    ..Default::default()
                },
                ..Default::default()
            }))
            .unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let (first, connected) = tokio::join!(
                async { endpoint.accept().await.unwrap().await.unwrap() },
                Client::connect(&client)
            );
            connected.unwrap();
            client.set_state(ClientState::Authenticated).await;
            client.roster_learned.store(true, Ordering::SeqCst);
            client
                .remember_session_credentials(
                    Credentials::UsernamePassword("iggy".into(), "secret".into()),
                    TEST_USER_ID,
                )
                .await;
            let original = client.session_identity().await.unwrap();
            let secret = client.session_bind_secret().await.unwrap();
            let peer = tokio::spawn(async move {
                let first = first;
                let (lost_reply, lost_header, lost_body) = read_test_request(&first).await;
                assert_eq!(lost_header.operation, Operation::SendMessages);
                drop(lost_reply);
                first.close(quinn::VarInt::from_u32(0), b"reply lost");
                let resumed = endpoint.accept().await.unwrap().await.unwrap();
                let (mut bind_reply, bind_header, bind_body) = read_test_request(&resumed).await;
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
                    &mut bind_reply,
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
                    let (mut register_reply, register, _) = read_test_request(&resumed).await;
                    assert_eq!(register.operation, Operation::Register);
                    assert_ne!(register.client, original.client_id);
                    let mut result = vec![0; size_of::<u32>()];
                    result.extend_from_slice(&login);
                    answer_test_request(&mut register_reply, &register, 0, &result).await;
                }
                loop {
                    let (mut reply, header, body) = read_test_request(&resumed).await;
                    if header.operation == Operation::NonReplicated {
                        answer_test_request(
                            &mut reply,
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
                    answer_test_request(&mut reply, &header, 0, b"receipt").await;
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
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve UDP address");
        let server_address = socket.local_addr().unwrap().to_string();
        drop(socket);
        let client = QuicClient::create(Arc::new(QuicClientConfig {
            server_address,
            max_idle_timeout: 100,
            ..QuicClientConfig::default()
        }))
        .expect("create QUIC client");

        let result = tokio::time::timeout(Duration::from_secs(10), client.connect_off_leader())
            .await
            .expect("one QUIC dial must not enter unlimited reconnect");
        assert!(matches!(result, Err(IggyError::CannotEstablishConnection)));
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    #[tokio::test]
    async fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+quic://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = QuicClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[tokio::test]
    async fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+quic://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = QuicClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

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
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
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
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str(reconnection_interval).unwrap()
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
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(quic_client_config.reconnection.enabled);
        assert!(quic_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            quic_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
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
