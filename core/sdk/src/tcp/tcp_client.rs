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
use crate::leader_aware::{
    LeaderRedirectionState, RosterWalk, check_and_redirect_to_leader,
    is_unauthenticated_metadata_probe, read_transport_endpoints,
};
use crate::links::Frame;
use crate::links::connect::{
    ConnectCoordinator, ConnectOwnerContext, accepts_state, is_same_spelling,
};
use crate::links::event_channel;
use crate::links::mux::MuxLink;
use crate::prelude::Client;
use crate::prelude::TcpClientConfig;
use crate::session_owner::SessionOwner;
use crate::vsr::{RetainedRequest, retain_replay_header};
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
#[cfg(test)]
use iggy_common::TcpClientReconnectionConfig;
use iggy_common::VsrSessionControl as _;
use iggy_common::{
    AutoLogin, ClientState, ConnectionString, ConnectionStringUtils, Credentials, DiagnosticEvent,
    Identifier, IggyDuration, IggyError, IggyTimestamp, NonZeroIggyDuration,
    TcpConnectionStringOptions, TransportProtocol,
};
use iggy_common::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use secrecy::ExposeSecret;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_rustls::TlsConnector;
use tracing::{error, info, trace, warn};

const NAME: &str = "Iggy";
/// Upper bound for awaiting a reply on the VSR connection. Far beyond any
/// healthy round-trip; only trips when the server loses the reply entirely
/// (e.g. stalled replication quorum), which would otherwise hold the link
/// lock forever and wedge the client.
const RESPONSE_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Backoff before replaying a request the server answered with an explicit
/// `TransientNotCommitted` frame (not-caught-up / in-flight / pipeline-full /
/// view-change cancel). The reply arrives promptly, so a short pause keeps the
/// replay from spinning while the primary catches up. Bounded by
/// `RESPONSE_READ_TIMEOUT`.
const NOT_READY_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// How long a request replays `TransientNotCommitted` on the SAME connection
/// before the client re-checks cluster leadership. A node that stopped being
/// primary (view change while the connection stayed up) answers transient
/// forever, so replaying alone never recovers; periodically consult the
/// roster and fail over to the leader. Bounded by `RESPONSE_READ_TIMEOUT`
/// overall.
const TRANSIENT_FAILOVER_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Bound on one dial while the client has other endpoints to try. A host
/// that drops the SYN -- powered off, or partitioned away -- takes the OS
/// connect timeout to fail, which is minutes, and every other endpoint waits
/// behind it. A client that knows a single endpoint has nothing to starve, so
/// its dial stays unbounded.
const FAILOVER_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// TCP client for interacting with the Iggy API.
/// It requires a valid server address.
#[derive(Debug)]
pub struct TcpClient {
    poll_router: PollRouter<Self>,
    pub(crate) link: Arc<Mutex<Option<MuxLink>>>,
    pub(crate) config: Arc<TcpClientConfig>,
    pub(crate) state: Mutex<ClientState>,
    client_address: Mutex<Option<SocketAddr>>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    pub(crate) connected_at: Mutex<Option<IggyTimestamp>>,
    leader_redirection_state: Mutex<LeaderRedirectionState>,
    pub(crate) current_server_address: Mutex<String>,
    /// Every endpoint the cluster roster named, refreshed on each leader
    /// check. A node dies together with its address, and the roster is
    /// unreachable exactly when it is needed, so the client has to have
    /// remembered it while the connection was still healthy.
    roster_endpoints: Mutex<Vec<String>>,
    /// Set once a sign-in on this client has gone looking for the roster, so
    /// that read happens once (see [`TcpClient::learn_roster_once`]).
    roster_learned: AtomicBool,
    /// Set by an explicit `Client::disconnect` and cleared by `Client::connect`.
    /// While it is set, no request reconnects the client on its own. A
    /// connection that drops without a `disconnect()` call leaves it clear, so
    /// that loss still heals.
    disconnected_by_caller: AtomicBool,
    /// The session and the sign-in, shared with the poll router and the
    /// poll clients it dials.
    session_owner: Arc<SessionOwner>,
    skip_auto_login_once: Mutex<bool>,
    /// Serializes connection movement after a refused request. The refusal
    /// releases the link before the leader check and reconnect, where another
    /// request could otherwise run a competing walk.
    routing_lock: Mutex<()>,
    connect_coordinator: ConnectCoordinator,
    consumer_group_state: Arc<iggy_common::ConsumerGroupClientState>,
}

/// A connection that completed every step of coming up, TLS included.
struct EstablishedConnection {
    link: MuxLink,
    client_address: SocketAddr,
    remote_address: SocketAddr,
}

/// A sign-in that did not complete, and whether the connection it ran on went
/// with it. A connection that is gone leaves the endpoints the sweep has not
/// reached yet worth dialing; one that stands means only the session is
/// missing, which no other endpoint would answer differently.
struct SignInFailure {
    error: IggyError,
    connection_lost: bool,
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        // A sweep that started before the last disconnect ends at its next
        // read of the flag. Joined, it would go on with the session that the
        // disconnect reset, and hand its failure to this connect. Once
        // another connect cleared the flag, its sweep is the one to join.
        self.connect_coordinator
            .wait_until_inactive(|| self.caller_disconnected())
            .await;
        self.disconnected_by_caller.store(false, Ordering::SeqCst);
        TcpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.disconnected_by_caller.store(true, Ordering::SeqCst);
        // An explicit disconnect is caller intent, like a logout: the session
        // it ends must not be resurrected by the next reconnect, so the
        // remembered sign-in goes with it. Involuntary drops (a dead socket,
        // a failover) go through `disconnect_transport` and keep it.
        self.forget_session_credentials().await;
        TcpClient::disconnect_transport(self).await?;
        self.reset_vsr_session().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        TcpClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
impl BinaryTransport for TcpClient {
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
            error!("Failed to send a TCP diagnostic event: {error}");
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
        let mut header = RetainedRequest::new(context);
        let result = self
            .send_raw_retaining_header(
                code,
                payload.clone(),
                &mut header,
                &IggyError::TransientNotAccepted,
            )
            .await;
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
            // With no credentials -- neither configured nor remembered from a
            // sign-in -- a reconnect cannot re-establish the session, so
            // non-login requests fail fast. Login/register itself is the
            // exception: the server stays deliberately silent on transient
            // register failures (the server `surface_login_failure`) and
            // relies on the client timing out and replaying the request.
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
            retain_replay_header(&mut header, &self.session_owner, code, &error)?;
            drop(_routing_guard);
            return self
                .send_raw_retaining_header(code, payload, &mut header, &error)
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
            let server_address = self.current_server_address.lock().await.clone();
            info!(
                "Reconnecting to the server: {} by client: {client_address}...",
                server_address
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

        retain_replay_header(&mut header, &self.session_owner, code, &error)?;

        drop(_routing_guard);
        self.send_raw_retaining_header(code, payload, &mut header, &error)
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

impl iggy_common::VsrSessionSealed for TcpClient {}

#[async_trait::async_trait]
impl iggy_common::VsrSessionControl for TcpClient {
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
        if self
            .session_owner
            .remember_unless(|| self.caller_disconnected(), credentials, user_id)
        {
            self.learn_roster_once().await;
        }
    }

    async fn forget_session_credentials(&self) {
        self.session_owner.forget();
    }

    async fn refresh_session_password(&self, user: &Identifier, new_password: &str) {
        self.session_owner.refresh_password(user, new_password);
    }

    async fn refresh_session_username(&self, user: &Identifier, new_username: &str) {
        self.session_owner.refresh_username(user, new_username);
    }

    fn sdk_version(&self) -> &'static str {
        crate::SDK_VERSION
    }
}

impl BinaryClient for TcpClient {}

#[async_trait]
impl PollTransport for TcpClient {
    const PROTOCOL: TransportProtocol = TransportProtocol::Tcp;

    async fn connect_poll_client(&self, endpoint: &str) -> Result<Self, IggyError> {
        let mut config = (*self.config).clone();
        config.server_address = endpoint.to_owned();
        config.auto_login = AutoLogin::Disabled;
        config.reconnection.enabled = false;
        let mut client =
            Self::with_session_owner(Arc::new(config), Arc::clone(&self.session_owner));
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
        let now = tokio::time::Instant::now();
        let (_, result) = self
            .send_raw_vsr_attempt(
                code,
                payload,
                RetainedRequest::new(context),
                now,
                now + RESPONSE_READ_TIMEOUT,
                false,
            )
            .await;
        if matches!(result, Err(IggyError::Disconnected | IggyError::TcpError)) {
            self.set_state(ClientState::Disconnected).await;
        }
        result
    }
}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: NonZeroIggyDuration,
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
        heartbeat_interval: NonZeroIggyDuration,
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
        let session_owner = Arc::new(SessionOwner::new(config.auto_login.clone()));
        Ok(Self::with_session_owner(config, session_owner))
    }

    /// A poll client passes the owner of the client that dials it.
    fn with_session_owner(config: Arc<TcpClientConfig>, session_owner: Arc<SessionOwner>) -> Self {
        let server_address = config.server_address.clone();
        Self {
            poll_router: PollRouter::new(Arc::clone(&session_owner)),
            config,
            client_address: Mutex::new(None),
            link: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: event_channel(),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address),
            roster_endpoints: Mutex::new(Vec::new()),
            roster_learned: AtomicBool::new(false),
            disconnected_by_caller: AtomicBool::new(false),
            session_owner,
            skip_auto_login_once: Mutex::new(false),
            routing_lock: Mutex::new(()),
            connect_coordinator: ConnectCoordinator::new(),
            consumer_group_state: Arc::new(iggy_common::ConsumerGroupClientState::new()),
        }
    }

    pub(crate) async fn connect(&self) -> Result<(), IggyError> {
        self.connect_with_settlement(false).await
    }

    async fn connect_off_leader(&self) -> Result<(), IggyError> {
        self.connect_with_settlement(true).await
    }

    async fn connect_with_settlement(&self, settle_off_leader: bool) -> Result<(), IggyError> {
        self.connect_coordinator
            .run(|abandoned, token| async move {
                let context =
                    self.connect_coordinator
                        .owner_context(token, settle_off_leader, false);
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
                            // After a teardown, such as the one of the
                            // disconnect, a second one would only publish
                            // `Disconnected` again.
                            let torn_down = self.link.lock().await.is_none()
                                && self.get_state().await == ClientState::Disconnected;
                            if !torn_down {
                                self.clear_abandoned_connect().await?;
                            }
                            return Err(IggyError::NotConnected);
                        }
                        Ok(())
                    })
                    .await
            })
            .await
    }

    async fn connect_inner(&self, context: ConnectOwnerContext) -> Result<(), IggyError> {
        let settle_off_leader = context.settle_off_leader();
        loop {
            // Read and claimed under one lock acquisition. Apart, two callers
            // both find `Disconnected` and both sweep: the loser's
            // `reset_vsr_session` re-mints the client id under the identity the
            // winner is binding, and its `replace` below drops the live
            // authenticated link.
            {
                let mut state = self.state.lock().await;
                match *state {
                    ClientState::Shutdown => {
                        trace!("Cannot connect. Client is shutdown.");
                        return Err(IggyError::ClientShutdown);
                    }
                    ClientState::Connected
                    | ClientState::Authenticating
                    | ClientState::Authenticated => {
                        let client_address = self.get_client_address_value().await;
                        trace!("Client: {client_address} is already connected.");
                        return Ok(());
                    }
                    ClientState::Connecting => {
                        trace!("Client is already connecting.");
                        return Ok(());
                    }
                    _ if self.caller_disconnected() => {
                        trace!("Cannot connect. The caller disconnected the client.");
                        return Err(IggyError::NotConnected);
                    }
                    _ => *state = ClientState::Connecting,
                }
            }

            let mut candidates = self.dial_candidates().await;
            // `reestablish_after` paces reconnects to the endpoint this client
            // was last on, and to that one only: the other endpoints owe it no
            // cooldown, and pausing before dialing them would push the failover
            // past the window the caller is willing to wait. So when there is
            // somewhere else to go, the paced endpoint goes last -- by which
            // time its window has usually elapsed anyway -- instead of the wait
            // being skipped outright.
            let paced_endpoint = self.current_server_address.lock().await.clone();
            if candidates.len() > 1 && self.reestablish_wait().await.is_some() {
                candidates.rotate_left(1);
            }

            let skip_auto_login = {
                let mut guard = self.skip_auto_login_once.lock().await;
                std::mem::take(&mut *guard)
            };
            let mut retry_count = 0;
            let mut candidate = 0;
            // A fault no retry can fix, remembered rather than returned at
            // once: it belongs to the endpoint that raised it (an unreadable CA
            // file, a domain that will not parse), and the endpoints behind
            // that one may be perfectly usable.
            let mut config_fault: Option<IggyError> = None;
            // A sign-in that failed together with the connection it ran on.
            // The sweep carries on -- a node that answers the dial and then
            // goes quiet must not own the client, and it is also the endpoint
            // the next connect would lead with -- and this is the reason the
            // caller gets if nothing behind it works out either.
            let mut sign_in_failure: Option<IggyError> = None;
            let should_redirect = loop {
                let server_address = candidates[candidate].clone();
                if server_address == paced_endpoint
                    && let Some(remaining) = self.reestablish_wait().await
                {
                    info!("Trying to connect to the server: {server_address} in: {remaining}");
                    sleep(remaining.get_duration()).await;
                }

                if self.caller_disconnected() {
                    self.fail_connect().await;
                    return Err(IggyError::NotConnected);
                }

                info!("{NAME} client is connecting to server: {server_address}...");
                match self.establish_bounded(&server_address, &candidates).await {
                    // Dropped before it is installed: the caller disconnected
                    // while it was being dialed.
                    Ok(_) if self.caller_disconnected() => {
                        self.fail_connect().await;
                        return Err(IggyError::NotConnected);
                    }
                    Ok(connection) => {
                        let dialed = server_address.clone();
                        // The endpoint that answered is where this client now
                        // lives: the leader check compares against it, and the
                        // next reconnect starts from it. Recorded only once the
                        // stream is usable, so a node that accepts TCP but
                        // fails the TLS handshake does not become sticky and
                        // shadow the endpoints behind it.
                        *self.current_server_address.lock().await = server_address;
                        let client_address = connection.client_address;
                        self.client_address.lock().await.replace(client_address);
                        let now = IggyTimestamp::now();
                        info!(
                            "{NAME} client: {client_address} has connected to server: {} at: {now}",
                            connection.remote_address,
                        );
                        self.link.lock().await.replace(connection.link);
                        self.set_state(ClientState::Connected).await;
                        self.connected_at.lock().await.replace(now);
                        self.publish_event(DiagnosticEvent::Connected).await;

                        match self
                            .establish_session(client_address, skip_auto_login, settle_off_leader)
                            .await
                        {
                            Ok(should_redirect) => break should_redirect,
                            Err(failure) if failure.connection_lost => {
                                // The sign-in or the disconnect already took
                                // the connection down and published
                                // `Disconnected`, which `fail_connect` would
                                // publish again.
                                if self.caller_disconnected() {
                                    return Err(IggyError::NotConnected);
                                }
                                warn!(
                                    "The sign-in on the server: {dialed} did not complete: {}",
                                    failure.error,
                                );
                                sign_in_failure = Some(failure.error);
                                // The sweep owns the state again: the sign-in
                                // took the connection down with it, and left
                                // `Disconnected` another caller would start a
                                // second sweep alongside this one.
                                self.set_state(ClientState::Connecting).await;
                            }
                            // The connection stands and only the session is
                            // missing: rejected credentials say the same thing
                            // on every node, and no endpoint behind this one
                            // would answer differently.
                            Err(failure) => return Err(failure.error),
                        }
                    }
                    Err(IggyError::CannotEstablishConnection) => {}
                    Err(error) => config_fault = Some(error),
                }

                // Every other endpoint gets its turn before the retry
                // interval: the node just lost may be gone for good, and
                // pausing on it helps nothing.
                candidate += 1;
                if candidate < candidates.len() {
                    continue;
                }
                candidate = 0;

                if self.caller_disconnected() {
                    self.fail_connect().await;
                    return Err(IggyError::NotConnected);
                }

                // An unreadable CA file, a domain that will not parse: no
                // endpoint answered and at least one said why in a way that a
                // retry cannot change, so the caller gets that reason instead
                // of a retry loop that buries it (`max_retries = None` would
                // otherwise redial it every interval forever).
                if let Some(error) = config_fault {
                    self.fail_connect().await;
                    return Err(error);
                }

                // The sweep is what reconnection settings apply to, not a
                // single dial: with reconnection off there are no retries, but
                // the failover endpoints were configured to be tried and they
                // get their one turn first.
                if !self.config.reconnection.enabled {
                    warn!("Automatic reconnection is disabled.");
                    self.fail_connect().await;
                    return Err(sign_in_failure.unwrap_or(IggyError::CannotEstablishConnection));
                }

                let unlimited_retries = self.config.reconnection.max_retries.is_none();
                let max_retries = self.config.reconnection.max_retries.unwrap_or_default();
                let max_retries_str =
                    if let Some(max_retries) = self.config.reconnection.max_retries {
                        max_retries.to_string()
                    } else {
                        "unlimited".to_string()
                    };

                if unlimited_retries || retry_count < max_retries {
                    retry_count += 1;
                    let interval_str = self.config.reconnection.interval.as_human_time_string();
                    info!(
                        "Retrying to connect ({retry_count}/{max_retries_str}), \
                         {} endpoint(s) in: {interval_str}",
                        candidates.len(),
                    );
                    sleep(self.config.reconnection.interval.get_duration()).await;
                    continue;
                }

                self.fail_connect().await;
                return Err(sign_in_failure.unwrap_or(IggyError::CannotEstablishConnection));
            };

            if should_redirect {
                continue;
            }

            return Ok(());
        }
    }

    async fn clear_abandoned_connect(&self) -> Result<(), IggyError> {
        self.link.lock().await.take();
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
        Ok(())
    }

    /// Re-establish the session on a connection that just came up and settle it
    /// on the leader. Reports whether the leader check asks for a redirect.
    async fn establish_session(
        &self,
        client_address: SocketAddr,
        skip_auto_login: bool,
        settle_off_leader: bool,
    ) -> Result<bool, SignInFailure> {
        let Some(credentials) = self.session_owner.sign_in_credentials() else {
            info!("No credentials to sign in with.");
            // A bound session without remembered credentials stays bound,
            // but this connection cannot authenticate a resume or fresh login.
            return Ok(false);
        };

        if skip_auto_login {
            info!("Skipping automatic sign-in for a retried login/register request.");
            return Ok(false);
        }

        if self
            .session_owner
            .session()
            .map_err(|error| SignInFailure {
                error,
                connection_lost: false,
            })?
            .is_bound()
        {
            match self.resume_vsr_session().await {
                Ok(()) => {
                    return if settle_off_leader {
                        Ok(false)
                    } else {
                        self.handle_leader_redirection()
                            .await
                            .map_err(|error| SignInFailure {
                                error,
                                connection_lost: false,
                            })
                    };
                }
                Err(IggyError::Unauthenticated | IggyError::TransientNotCommitted) => {
                    self.reset_vsr_session()
                        .await
                        .map_err(|error| SignInFailure {
                            error,
                            connection_lost: false,
                        })?;
                }
                Err(error) => {
                    let connection_lost = crate::vsr::resume_error_is_retryable(&error);
                    let _ = self.disconnect_transport().await;
                    return Err(SignInFailure {
                        error,
                        connection_lost,
                    });
                }
            }
        }
        info!("{NAME} client: {client_address} is signing in...");
        self.set_state(ClientState::Authenticating).await;
        let signed_in = match &credentials {
            Credentials::UsernamePassword(username, password) => self
                .login_user(username, password.expose_secret())
                .await
                .map(|_| format!("the user credentials, username: {username}")),
            Credentials::PersonalAccessToken(token) => self
                .login_with_personal_access_token(token.expose_secret())
                .await
                .map(|_| "a personal access token".to_owned()),
        };
        match signed_in {
            Ok(how) => info!("{NAME} client: {client_address} has signed in with {how}."),
            Err(error) => return Err(self.fail_sign_in(error).await),
        }

        // A failover walking the roster past the metadata leader stays where
        // it dialed: the leader settlement below would put the connection
        // straight back on the node whose partition replica keeps refusing
        // the request. One connect only; the next ordinary connect settles
        // normally.
        if settle_off_leader {
            info!(
                "{NAME} client: {client_address} stays on the dialed node for a partition failover."
            );
            return Ok(false);
        }

        // The sole leader settlement, and it runs authenticated. Any node
        // completes a login now -- a backup forwards the register to the
        // primary -- so this decides where later ops land, not whether sign-in
        // works.
        self.handle_leader_redirection()
            .await
            .map_err(|error| SignInFailure {
                error,
                connection_lost: false,
            })
    }

    /// Put the client back into a state that describes what a failed sign-in
    /// left behind, and report whether the connection survived it.
    async fn fail_sign_in(&self, error: IggyError) -> SignInFailure {
        // A sign-in can fail because the socket died under it. Whatever is left
        // of that connection cannot carry a request, so it goes rather than
        // being kept behind a `Connected` that makes the next `connect()` a
        // no-op and leaves every gated operation failing until someone calls
        // `disconnect()` by hand.
        let connection_lost = matches!(
            error,
            IggyError::Disconnected
                | IggyError::EmptyResponse
                | IggyError::NotConnected
                | IggyError::CannotEstablishConnection
                | IggyError::TcpError
                | IggyError::StaleClient
        );
        if connection_lost {
            if let Err(teardown_error) = self.disconnect_transport().await {
                warn!("Failed to drop the connection of a failed sign-in: {teardown_error}");
            }
        } else if self.get_state().await == ClientState::Authenticating {
            // With the transport up and only the session missing, the state has
            // to say so: left at `Authenticating` every gated operation fails
            // client-side with `Disconnected`, `connect()` returns ok without
            // dialing, and nothing short of an explicit `login_user` recovers.
            self.set_state(ClientState::Connected).await;
        }

        // A rejected credential does not become valid on the next reconnect,
        // and replaying it costs an argon2 on the server every time. Configured
        // credentials stay as configured -- they are the caller's to fix -- so
        // only the remembered sign-in is dropped.
        if matches!(
            error,
            IggyError::InvalidCredentials
                | IggyError::InvalidUsername
                | IggyError::InvalidPassword
                | IggyError::Unauthenticated
        ) {
            self.forget_session_credentials().await;
        }

        SignInFailure {
            error,
            connection_lost,
        }
    }

    /// Checks cluster metadata and handles leader redirection if needed.
    /// Returns true if redirection occurred and reconnection is needed.
    pub(crate) async fn handle_leader_redirection(&self) -> Result<bool, IggyError> {
        let current_address = self.current_server_address.lock().await.clone();
        let leader_check = check_and_redirect_to_leader(
            self,
            &current_address,
            iggy_common::TransportProtocol::Tcp,
        )
        .await?;

        // Replaced wholesale rather than merged: the roster is the cluster's
        // own answer about where its nodes are, so a node it dropped should
        // stop being dialed. The configured seeds are kept separately and
        // outlive it.
        if !leader_check.endpoints.is_empty() {
            self.poll_router
                .roster_size
                .store(leader_check.node_count, Ordering::Release);
            *self.roster_endpoints.lock().await = leader_check.endpoints;
        }

        if let Some(new_leader_address) = leader_check.redirect {
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
    /// a request the current node keeps refusing to admit.
    ///
    /// The metadata leader check cannot repair that refusal: metadata and
    /// partition consensus groups elect independently, so the metadata leader
    /// can hold a follower replica of the partition the request targets.
    /// `TransientNotAccepted` marks the request as never admitted and safe to
    /// re-issue anywhere, so walking the roster is correct, and the caller's
    /// request budget bounds the walk. Reports whether there was another
    /// endpoint to move to.
    async fn settle_on_endpoint(&self, next: String) -> Result<(), IggyError> {
        let current = self.current_server_address.lock().await.clone();

        info!(
            "The request keeps being refused on {current} while the roster names it the \
             metadata leader; trying the next cluster node at {next}."
        );
        // No reestablish pacing: the node being left is healthy, the one being
        // dialed owes no cooldown, and the request is already burning budget.
        self.connected_at.lock().await.take();
        self.disconnect_transport().await?;
        *self.current_server_address.lock().await = next;
        Ok(())
    }

    /// Whether fallback `AutoLogin` credentials are configured. A remembered
    /// successful sign-in takes precedence after reconnecting.
    pub(crate) fn auto_login_configured(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_))
    }

    /// Read the cluster roster once, on the first sign-in that succeeds on a
    /// client whose caller signs it in by hand.
    ///
    /// `connect()` follows the sign-in it runs itself with a leader check, and
    /// that check is what refreshes the roster. A client with no configured
    /// `AutoLogin` is signed in by its caller instead, and only `IggyClient`
    /// follows that with a leader check, so a raw transport would know exactly
    /// one endpoint -- the one it was configured with -- and redial the node
    /// that died for as long as it lived.
    ///
    /// Once per client, which is also what keeps the read from nesting: it goes
    /// through the reconnect path, whose sign-in calls straight back into here.
    /// Bounded for the same reason: the read is a convenience for a failover
    /// that may never happen, so it must not hold up the sign-in that triggered
    /// it -- unbounded retries would do exactly that.
    async fn learn_roster_once(&self) {
        // Only a live session can read the roster, and only the caller's own
        // sign-in leaves one behind here: a connect that signs in follows it
        // with a leader check of its own.
        if self.auto_login_configured()
            || self.get_state().await != ClientState::Authenticated
            || self.roster_learned.swap(true, Ordering::SeqCst)
        {
            return;
        }

        let read = read_transport_endpoints(self, TransportProtocol::Tcp);
        let Ok((node_count, endpoints)) = tokio::time::timeout(ROSTER_READ_TIMEOUT, read).await
        else {
            warn!("Reading the cluster roster took longer than {ROSTER_READ_TIMEOUT:?}");
            return;
        };
        if endpoints.is_empty() {
            return;
        }

        info!(
            "{NAME} client learned {} endpoint(s) to fail over to.",
            endpoints.len()
        );
        self.poll_router
            .roster_size
            .store(node_count, Ordering::Release);
        *self.roster_endpoints.lock().await = endpoints;
    }

    /// Endpoints to dial for one connect, likeliest first: where the client
    /// currently is, the address it was configured with, then the roster it
    /// learned while connected.
    ///
    /// Configured before learned, as in the other SDKs: that is the endpoint
    /// the caller vouched for, while a roster read from a cluster that has since
    /// changed shape may name nodes that are gone.
    async fn dial_candidates(&self) -> Vec<String> {
        let mut candidates = vec![self.current_server_address.lock().await.clone()];
        let roster = self.roster_endpoints.lock().await.clone();
        let configured = std::iter::once(&self.config.server_address);
        for endpoint in configured.chain(roster.iter()) {
            // Spellings only, no name resolution: one duplicate endpoint costs
            // a dial that fails on its own, while a resolver that does not
            // answer would stall the failover before it dialed anything.
            if !candidates
                .iter()
                .any(|candidate| is_same_spelling(candidate, endpoint))
            {
                candidates.push(endpoint.clone());
            }
        }
        candidates
    }

    /// Bring one endpoint all the way up: TCP connect, socket options, and the
    /// TLS handshake when it is configured. Nothing about the connection is
    /// recorded until this succeeds, so a half-usable endpoint leaves no trace
    /// for the next connect to lead with.
    ///
    /// `CannotEstablishConnection` means this endpoint failed and the next one
    /// is worth trying; any other error is a configuration fault that no
    /// endpoint can satisfy.
    async fn establish(&self, server_address: &str) -> Result<EstablishedConnection, IggyError> {
        let stream = TcpStream::connect(server_address).await.map_err(|error| {
            error!("Failed to connect to server: {server_address}. Error: {error}");
            IggyError::CannotEstablishConnection
        })?;
        let client_address = stream.local_addr().map_err(|error| {
            error!("Failed to get the local address of the client: {error}");
            IggyError::CannotEstablishConnection
        })?;
        let remote_address = stream.peer_addr().map_err(|error| {
            error!("Failed to get the remote address of the server: {error}");
            IggyError::CannotEstablishConnection
        })?;

        if let Err(error) = stream.set_nodelay(self.config.nodelay) {
            error!("Failed to set the nodelay option on the client: {error}, continuing...");
        }

        if !self.config.tls_enabled {
            let (reader, writer) = stream.into_split();
            return Ok(EstablishedConnection {
                link: MuxLink::tcp(reader, writer, remote_address),
                client_address,
                remote_address,
            });
        }

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let config = if self.config.tls_validate_certificate {
            let mut root_cert_store = rustls::RootCertStore::empty();
            if let Some(certificate_path) = &self.config.tls_ca_file {
                for cert in CertificateDer::pem_file_iter(certificate_path).map_err(|error| {
                    error!("Failed to read the CA file: {certificate_path}. {error}");
                    IggyError::InvalidTlsCertificatePath
                })? {
                    let certificate = cert.map_err(|error| {
                        error!(
                            "Failed to read a certificate from the CA file: {certificate_path}. {error}",
                        );
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
            use crate::tcp::tcp_tls_verifier::NoServerVerification;
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoServerVerification))
                .with_no_client_auth()
        };

        let connector = TlsConnector::from(Arc::new(config));
        let tls_domain = if self.config.tls_domain.is_empty() {
            tls_server_name(server_address)
        } else {
            self.config.tls_domain.to_owned()
        };
        let domain = ServerName::try_from(tls_domain).map_err(|error| {
            error!("Failed to create a server name from the domain. {error}");
            IggyError::InvalidTlsDomain
        })?;
        let stream = connector.connect(domain, stream).await.map_err(|error| {
            // The verdict describes the peer, not this client: a certificate
            // that names another host, a peer that answers a ClientHello with
            // something else. The endpoints behind it may be fine, and with
            // one roster entry per node the SNI is a bare address that no
            // certificate has to cover, so this ends the dial rather than the
            // connect.
            error!("Failed to establish a TLS connection to the server: {error}");
            IggyError::CannotEstablishConnection
        })?;

        let (reader, writer) = tokio::io::split(stream);
        Ok(EstablishedConnection {
            link: MuxLink::tcp(reader, writer, remote_address),
            client_address,
            remote_address,
        })
    }

    /// Give up on connecting. The state has to go back to `Disconnected`:
    /// left at `Connecting`, the next `connect()` returns ok at the top
    /// without ever dialing.
    async fn fail_connect(&self) {
        self.set_state(ClientState::Disconnected).await;
        self.publish_event(DiagnosticEvent::Disconnected).await;
    }

    /// A reconnect sweep checks this before each dial, before each retry
    /// pause, before it signs in and when it ends: once the caller
    /// disconnects, nothing in the sweep may bring the client back.
    fn caller_disconnected(&self) -> bool {
        self.disconnected_by_caller.load(Ordering::SeqCst)
    }

    /// [`Self::establish`], bounded while other endpoints are queued behind
    /// this one (see `FAILOVER_DIAL_TIMEOUT`). The bound covers the handshake
    /// as well as the connect: a peer that accepts TCP and then never answers
    /// the ClientHello is exactly the kind of failure the survivors are there
    /// for, and neither step has a deadline of its own.
    async fn establish_bounded(
        &self,
        server_address: &str,
        candidates: &[String],
    ) -> Result<EstablishedConnection, IggyError> {
        if candidates.len() < 2 {
            return self.establish(server_address).await;
        }

        match tokio::time::timeout(FAILOVER_DIAL_TIMEOUT, self.establish(server_address)).await {
            Ok(connection) => connection,
            Err(_elapsed) => {
                error!(
                    "Connecting to server: {server_address} took longer than \
                     {FAILOVER_DIAL_TIMEOUT:?}"
                );
                Err(IggyError::CannotEstablishConnection)
            }
        }
    }

    /// What is left of the `reestablish_after` window since the last
    /// successful connection, if any.
    async fn reestablish_wait(&self) -> Option<IggyDuration> {
        let connected_at = self
            .connected_at
            .lock()
            .await
            .as_ref()
            .map(IggyTimestamp::as_micros)?;
        let elapsed = IggyTimestamp::now().as_micros() - connected_at;
        let interval = self.config.reconnection.reestablish_after.as_micros();
        trace!(
            "Elapsed time since last connection: {}",
            IggyDuration::from(elapsed)
        );
        (elapsed < interval).then(|| IggyDuration::from(interval - elapsed))
    }

    /// Tear down the connection without touching the remembered sign-in.
    ///
    /// The reconnect and redirect paths use this: their disconnect is not
    /// caller intent, and forgetting the credentials here would strand the
    /// failover unauthenticated. The public [`Client::disconnect`] wraps this
    /// and forgets them first.
    async fn disconnect_transport(&self) -> Result<(), IggyError> {
        // Preserve the link a concurrent reconnect is installing.
        // A `Connecting` that no sweep owns, such as one left by an aborted
        // heartbeat, is torn down like any other state.
        if self.get_state().await == ClientState::Connecting && self.connect_coordinator.is_active()
        {
            trace!("Not disconnecting; a connect is already in flight.");
            return Ok(());
        }
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
        if self.get_state().await == ClientState::Shutdown {
            return Ok(());
        }

        let client_address = self.get_client_address_value().await;
        info!("Shutting down the {NAME} TCP client: {client_address}");
        let link = self.link.lock().await.take();
        if let Some(link) = link {
            link.shutdown().await;
        }
        self.reset_vsr_session().await?;
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} TCP client: {client_address} has been shutdown.");
        Ok(())
    }

    async fn send_raw_retaining_header(
        &self,
        code: u32,
        payload: Bytes,
        header: &mut RetainedRequest,
        previous_error: &IggyError,
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
            _ => {}
        }

        // One overall deadline bounds the request across transient replays
        // AND leader failovers, matching the previous single-connection
        // budget. Login/register replays stay on this connection for the
        // whole budget: the connect flow owns leader redirection for the
        // sign-in handshake, and reconnecting from underneath it would
        // recurse.
        let overall_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
        // Set once this request starts walking the roster past the metadata
        // leader, so later rounds keep walking instead of being redirected
        // back onto the node whose partition replica keeps refusing them.
        let mut roster_walk: Option<RosterWalk> = None;
        let mut checked_metadata_leader = false;
        let mut routing_guard = None;
        loop {
            let transient_deadline = if is_login_register_code(code) {
                overall_deadline
            } else {
                overall_deadline
                    .min(tokio::time::Instant::now() + TRANSIENT_FAILOVER_CHECK_INTERVAL)
            };
            retain_replay_header(header, &self.session_owner, code, previous_error)?;
            let (encoded, result) = self
                .send_raw_vsr_attempt(
                    code,
                    payload.clone(),
                    *header,
                    transient_deadline,
                    overall_deadline,
                    true,
                )
                .await;
            header.header = encoded.or(header.header);
            match result {
                Err(IggyError::TransientNotAccepted)
                    if tokio::time::Instant::now() < overall_deadline
                        && !is_login_register_code(code) =>
                {
                    if matches!(code, GET_CLUSTER_METADATA_CODE | BIND_SESSION_CODE) {
                        return Err(IggyError::TransientNotAccepted);
                    }

                    if roster_walk
                        .as_ref()
                        .is_some_and(RosterWalk::is_single_endpoint)
                    {
                        // Discovery already proved there is no routing choice.
                        // Retry without reacquiring the lock so reconnects and
                        // unrelated requests do not wait out this deadline.
                        drop(routing_guard.take());
                        continue;
                    }

                    if routing_guard.is_none() {
                        routing_guard = Some(
                            tokio::time::timeout_at(overall_deadline, self.routing_lock.lock())
                                .await
                                .map_err(|_| IggyError::TransientNotAccepted)?,
                        );
                        // A concurrent refused request may have moved the
                        // shared client while this request waited.
                        continue;
                    }

                    // This replica did not admit this attempt. An earlier
                    // connection may still have committed it, so retain its
                    // original outcome when checking the session after a
                    // redirect. Only a proven unadmitted request may acquire
                    // a fresh identity; a resumed session keeps the header.
                    let current = self.current_server_address.lock().await.clone();
                    let mut redirected = false;
                    if !checked_metadata_leader {
                        checked_metadata_leader = true;
                        redirected = matches!(self.handle_leader_redirection().await, Ok(true));
                        let roster = self.roster_endpoints.lock().await.clone();
                        roster_walk = Some(RosterWalk::new(&current, &roster));
                    }
                    let (mut target, mut needs_settle) = if redirected {
                        let target = self.current_server_address.lock().await.clone();
                        if let Some(walk) = roster_walk.as_mut() {
                            walk.record_attempt(&target);
                        }
                        (target, false)
                    } else if let Some(next) = roster_walk.as_mut().and_then(RosterWalk::next) {
                        // The roster says this node IS the metadata leader
                        // (or answered nothing usable), yet it keeps refusing
                        // to admit the request: its replica of the target
                        // partition group is not that group's primary, and no
                        // metadata redirect can fix that. Walk the roster
                        // instead of replaying into the same refusal until
                        // the whole request budget burns. Once walking, keep
                        // walking: rechecking the metadata leader between
                        // hops would bounce the request between two nodes and
                        // never reach the rest of the roster.
                        (next, true)
                    } else if roster_walk
                        .as_ref()
                        .is_some_and(RosterWalk::is_single_endpoint)
                    {
                        // Partition materialisation can outlast the short retry
                        // window on a single node. No routing change is needed.
                        // Subsequent refusals take the lock-free local retry
                        // branch above rather than reacquiring this guard.
                        drop(routing_guard.take());
                        continue;
                    } else {
                        return Err(IggyError::TransientNotAccepted);
                    };

                    loop {
                        if needs_settle {
                            self.settle_on_endpoint(target.clone()).await?;
                        }
                        let connect = if needs_settle {
                            self.connect_off_leader().await
                        } else {
                            self.connect().await
                        };
                        match connect {
                            Ok(()) => {
                                let connected = self.current_server_address.lock().await.clone();
                                let first_visit = roster_walk
                                    .as_mut()
                                    .is_some_and(|walk| walk.record_attempt(&connected));
                                if is_same_spelling(&connected, &target) || first_visit {
                                    break;
                                }
                            }
                            Err(IggyError::CannotEstablishConnection) => {}
                            Err(error) => return Err(error),
                        }

                        let Some(next) = roster_walk.as_mut().and_then(RosterWalk::next) else {
                            return Err(IggyError::TransientNotAccepted);
                        };
                        target = next;
                        needs_settle = true;
                    }
                }
                Err(IggyError::Disconnected) => {
                    // No reply came in time, or the connection broke. Drop
                    // the connection and let callers reconnect.
                    self.link.lock().await.take();
                    self.set_state(ClientState::Disconnected).await;
                    return Err(IggyError::Disconnected);
                }
                other => return other,
            }
        }
    }

    /// One send attempt on the current connection: encode the header (or reuse
    /// `preencoded` so a same-session replay keeps its request id for the
    /// server's dedup), write the frame, and replay uncertain results until
    /// `read_deadline`. Refusals use the shorter `transient_deadline` only
    /// when every attempt is known unadmitted. Reads use `read_deadline`, the
    /// full request budget -- so a short transient window cannot tear down a
    /// connection that is merely slow to reply. Returns the header used so the
    /// caller can replay the same id on a later attempt.
    async fn send_raw_vsr_attempt(
        &self,
        code: u32,
        payload: Bytes,
        mut preencoded: RetainedRequest,
        transient_deadline: tokio::time::Instant,
        read_deadline: tokio::time::Instant,
        retry_transient: bool,
    ) -> (
        Option<iggy_binary_protocol::consensus::RequestHeader>,
        Result<Bytes, IggyError>,
    ) {
        let mut retry_outcome = crate::vsr::RetryOutcome::default();
        let mut lifecycle_retry_interval = NOT_READY_RETRY_INTERVAL;
        loop {
            let (request_header, result) = self
                .send_raw_vsr_exchange(
                    code,
                    payload.clone(),
                    preencoded,
                    transient_deadline,
                    read_deadline,
                    retry_transient,
                    &mut retry_outcome,
                )
                .await;
            match result {
                // The refusal committed under this request id, so the retry is a
                // new request. The pause holds no link lock, and the next
                // exchange takes its id only once it holds the link, above any
                // id another request sent during the pause.
                Err(IggyError::LifecycleBusy)
                    if retry_transient
                        && tokio::time::Instant::now() + lifecycle_retry_interval
                            < read_deadline =>
                {
                    preencoded.header = None;
                    tokio::time::sleep(lifecycle_retry_interval).await;
                    lifecycle_retry_interval = (lifecycle_retry_interval * 2)
                        .min(crate::vsr::LIFECYCLE_RETRY_MAX_INTERVAL);
                }
                result => return (request_header, result),
            }
        }
    }

    /// The part of [`Self::send_raw_vsr_attempt`] that holds the link lock.
    /// `retry_outcome` carries over between the exchanges of one attempt.
    #[allow(clippy::too_many_arguments)]
    async fn send_raw_vsr_exchange(
        &self,
        code: u32,
        payload: Bytes,
        mut preencoded: RetainedRequest,
        transient_deadline: tokio::time::Instant,
        read_deadline: tokio::time::Instant,
        retry_transient: bool,
        retry_outcome: &mut crate::vsr::RetryOutcome,
    ) -> (
        Option<iggy_binary_protocol::consensus::RequestHeader>,
        Result<Bytes, IggyError>,
    ) {
        let link = self.link.clone();
        let session_owner = Arc::clone(&self.session_owner);
        let mut outcome_so_far = std::mem::take(retry_outcome);
        // A task finishes the exchange when its caller goes away. An unbound
        // request that left before its reply would close the link.
        let joined = tokio::spawn(async move {
            let mut link_guard = link.lock().await;
            let Some(link) = link_guard.as_ref() else {
                error!("Cannot send data. Client is not connected.");
                return (None, Err(IggyError::NotConnected), outcome_so_far);
            };
            // Encode the request header ONCE per session: `next_request_id`
            // advances here, so a transient replay must reuse the same id for
            // the server's dedup. A `TransientNotCommitted` answer (the server
            // could not commit yet: not-caught-up / in-flight / pipeline-full /
            // view-change cancel) lets us resend the SAME request on the SAME
            // connection with no reconnect and the session intact.
            let request_header = {
                let Ok(mut session) = session_owner.session() else {
                    return (
                        preencoded.header,
                        Err(IggyError::InvalidConfiguration),
                        outcome_so_far,
                    );
                };
                match preencoded.encode(&mut session, code, &payload) {
                    Ok(header) => header,
                    Err(error) => return (preencoded.header, Err(error), outcome_so_far),
                }
            };
            let mut link_failed = false;
            let mut replay = false;
            let outcome = loop {
                let frame = Frame {
                    header: request_header,
                    body: payload.clone(),
                };
                trace!("Sent a TCP request with code: {code}, waiting for a response...");
                // The deadline guards against server-side reply loss (e.g. a
                // stalled replication quorum that never commits the op), which
                // would hold the link lock forever and wedge every later
                // request on this client.
                let reply = match link.send(frame, read_deadline, &mut false).await {
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
                    // `TransientNotCommitted`: the op's outcome is unknown
                    // (e.g. a view change canceled it in flight) -- ONLY a
                    // same-session replay of the same request id is safe
                    // (the client-table serves it from cache if it did
                    // commit). Replay on this connection for the whole
                    // request budget; never hand it to the failover path,
                    // which re-issues under a fresh session and could
                    // double-apply a committed write.
                    Err(IggyError::TransientNotCommitted)
                        if code != iggy_binary_protocol::codes::POLL_MESSAGES_ON_PRIMARY_CODE
                            && tokio::time::Instant::now() < read_deadline =>
                    {
                        let remaining =
                            read_deadline.saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                    }
                    // `TransientNotAccepted`: the server never admitted the
                    // request, so it is re-issuable anywhere. Replay here
                    // briefly, then hand it back to the caller for a
                    // leader recheck / failover.
                    Err(IggyError::TransientNotAccepted)
                        if tokio::time::Instant::now() < transient_deadline =>
                    {
                        let remaining = transient_deadline
                            .saturating_duration_since(tokio::time::Instant::now());
                        tokio::time::sleep(NOT_READY_RETRY_INTERVAL.min(remaining)).await;
                    }
                    other => break other,
                }
            };
            // A failed send leaves the link closed or its reply unread, so the
            // next request needs a new connection.
            if link_failed {
                link_guard.take();
            }
            (Some(request_header), outcome, outcome_so_far)
        })
        .await;
        match joined {
            Ok((request_header, outcome, outcome_so_far)) => {
                *retry_outcome = outcome_so_far;
                (request_header, outcome)
            }
            Err(e) => {
                error!("Task execution failed during TCP request: {}", e);
                (None, Err(IggyError::TcpError))
            }
        }
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

const fn is_login_register_code(code: u32) -> bool {
    matches!(code, LOGIN_REGISTER_CODE | LOGIN_REGISTER_WITH_PAT_CODE)
}

fn tls_server_name(server_address: &str) -> String {
    if let Ok(address) = SocketAddr::from_str(server_address) {
        return address.ip().to_string();
    }
    server_address
        .rsplit_once(':')
        .map_or(server_address, |(host, _port)| host)
        .to_owned()
}

/// Unit tests for TcpClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for TcpClient.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic_events::DIAGNOSTIC_EVENT_CAPACITY;
    use crate::links::EVENT_CHANNEL_CAPACITY;
    use crate::links::test_support::{
        SIGNED_IN_USER_ID, register_reply, sign_in_reply, sign_in_steps_after_a_disconnect,
        spawn_disconnect_behind_the_link, spawn_restart_behind_the_link,
    };
    use crate::vsr::replay_after_session_reset_is_safe;
    use iggy_binary_protocol::codes::{
        GET_ME_CODE, JOIN_CONSUMER_GROUP_CODE, LOGOUT_USER_CODE, PING_CODE, SEND_MESSAGES_CODE,
    };
    use iggy_binary_protocol::consensus::{
        EvictionHeader, EvictionReason, REJECTION_SECTION_LEN, write_rejection_section,
    };
    use iggy_binary_protocol::requests::system::BindSessionRequest;
    use iggy_binary_protocol::responses::users::LoginRegisterResponse;
    use iggy_binary_protocol::{
        Command, HEADER_SIZE, IGGY_PROTOCOL_VERSION, Operation, ReplyHeader, RequestHeader,
        WireDecode, WireEncode, WireName,
    };
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const SESSION_USER_ID: u32 = 7;

    async fn read_test_request(stream: &mut TcpStream) -> (RequestHeader, Vec<u8>) {
        let mut bytes = [0; HEADER_SIZE];
        stream.read_exact(&mut bytes).await.unwrap();
        let header: RequestHeader = bytemuck::checked::try_pod_read_unaligned(&bytes).unwrap();
        let mut body = vec![0; header.size as usize - HEADER_SIZE];
        stream.read_exact(&mut body).await.unwrap();
        (header, body)
    }

    async fn answer_test_request(
        stream: &mut TcpStream,
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
        stream.write_all(bytemuck::bytes_of(&header)).await.unwrap();
        stream.write_all(body).await.unwrap();
    }

    #[tokio::test]
    async fn a_mismatched_resume_reply_should_preserve_the_original_session() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let (listener, address) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: address,
            ..TcpClientConfig::default()
        }))
        .unwrap();
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let original = client.session_identity().await.unwrap();
        let state = client.get_state().await;
        let mut events = client.subscribe_events().await;
        let peer = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let (header, body) = read_test_request(&mut connection).await;
            assert_eq!(
                BindSessionRequest::decode_from(&body).unwrap().identity,
                original
            );
            let response = LoginRegisterResponse {
                user_id: SESSION_USER_ID,
                session: original.session + 1,
                server_protocol_version: IGGY_PROTOCOL_VERSION,
                server_version: WireName::new("test").unwrap(),
            }
            .to_bytes();
            answer_test_request(&mut connection, &header, 0, &response).await;
        });
        let result = tokio::time::timeout(TEST_BUDGET, client.resume_vsr_session())
            .await
            .unwrap();
        assert_eq!(
            result,
            Err(IggyError::SessionMismatch(
                original.session,
                original.session + 1
            ))
        );
        assert_eq!(client.session_identity().await.unwrap(), original);
        assert_eq!(client.get_state().await, state);
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(event, DiagnosticEvent::SignedIn));
        }
        tokio::time::timeout(TEST_BUDGET, peer)
            .await
            .unwrap()
            .unwrap();
        Client::shutdown(&client).await.unwrap();
    }

    #[tokio::test]
    async fn a_lost_reply_resumes_and_replays_only_the_original_session() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        for refusal in [
            None,
            Some(IggyError::Unauthenticated),
            Some(IggyError::TransientNotCommitted),
            Some(IggyError::TransientNotAccepted),
        ] {
            let (listener, address) = live_endpoint().await;
            let client = TcpClient::create(Arc::new(TcpClientConfig {
                server_address: address,
                reconnection: TcpClientReconnectionConfig {
                    reestablish_after: IggyDuration::from_str("0s").unwrap(),
                    max_retries: Some(0),
                    ..TcpClientReconnectionConfig::default()
                },
                ..TcpClientConfig::default()
            }))
            .unwrap();
            Client::connect(&client).await.unwrap();
            client.bind_vsr_session(1).await.unwrap();
            client.set_state(ClientState::Authenticated).await;
            client.roster_learned.store(true, Ordering::SeqCst);
            client
                .remember_session_credentials(
                    Credentials::UsernamePassword("iggy".into(), "secret".into()),
                    SESSION_USER_ID,
                )
                .await;
            let original = client.session_identity().await.unwrap();
            let secret = client.session_bind_secret().await.unwrap();
            let mut events = client.subscribe_events().await;
            let server_refusal = refusal.clone();
            let peer = tokio::spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                let (lost_header, lost_body) = read_test_request(&mut first).await;
                assert_eq!(lost_header.operation, Operation::SendMessages);
                drop(first);
                let (mut resumed, _) = listener.accept().await.unwrap();
                let (bind_header, bind_body) = read_test_request(&mut resumed).await;
                assert_eq!(
                    u32::from_le_bytes(bind_header.reserved[..4].try_into().unwrap()),
                    BIND_SESSION_CODE
                );
                let binding = BindSessionRequest::decode_from(&bind_body).unwrap();
                assert_eq!(binding.identity, original);
                assert_eq!(binding.bind_secret.expose_secret(), secret.expose_secret());
                let epoch = if server_refusal.is_some() { 2 } else { 1 };
                let login = LoginRegisterResponse {
                    user_id: SESSION_USER_ID,
                    session: epoch,
                    server_protocol_version: IGGY_PROTOCOL_VERSION,
                    server_version: WireName::new("test").unwrap(),
                }
                .to_bytes();
                answer_test_request(
                    &mut resumed,
                    &bind_header,
                    server_refusal.as_ref().map_or(0, IggyError::as_code),
                    if server_refusal.is_some() {
                        &[]
                    } else {
                        &login
                    },
                )
                .await;
                if server_refusal == Some(IggyError::TransientNotAccepted) {
                    let mut byte = [0];
                    assert_eq!(
                        resumed.read(&mut byte).await.unwrap(),
                        0,
                        "a lagging replica must not start a fresh registration"
                    );
                    return;
                }
                if server_refusal.is_some() {
                    let (register, _) = read_test_request(&mut resumed).await;
                    assert_eq!(register.operation, Operation::Register);
                    assert_ne!(register.client, original.client_id);
                    let mut framed = vec![0; std::mem::size_of::<u32>()];
                    framed.extend_from_slice(&login);
                    answer_test_request(&mut resumed, &register, 0, &framed).await;
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
                    assert_eq!(header.operation, Operation::SendMessages);
                    if server_refusal.is_none() {
                        assert_eq!(
                            bytemuck::bytes_of(&header),
                            bytemuck::bytes_of(&lost_header)
                        );
                        assert_eq!(body, lost_body);
                    } else {
                        assert_ne!(header.client, lost_header.client);
                        assert_eq!(body, b"explicit-new-send");
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
            .expect("resume must finish without waiting for the ordinary write retry budget");
            match refusal {
                None => assert_eq!(result.unwrap(), Bytes::from_static(b"receipt")),
                Some(IggyError::TransientNotAccepted) => {
                    assert_eq!(result.unwrap_err(), IggyError::TransientNotCommitted);
                    assert_eq!(client.session_identity().await.unwrap(), original);
                    assert_eq!(client.get_state().await, ClientState::Disconnected);
                }
                Some(_) => {
                    assert_eq!(
                        result.unwrap_err(),
                        IggyError::TransientNotCommitted,
                        "fresh login must not replay the ambiguous old send"
                    );
                    assert_eq!(client.get_state().await, ClientState::Authenticated);
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
                }
            }
            if refusal != Some(IggyError::TransientNotAccepted) {
                let mut signed_in = false;
                while let Ok(event) = events.try_recv() {
                    signed_in |= matches!(event, DiagnosticEvent::SignedIn);
                }
                assert!(
                    signed_in,
                    "successful reconnect must reopen producer and consumer gates"
                );
            }
            tokio::time::timeout(TEST_BUDGET, peer)
                .await
                .unwrap()
                .unwrap();
            Client::shutdown(&client).await.unwrap();
        }
    }

    #[tokio::test]
    async fn given_poisoned_session_when_sending_or_resetting_should_return_configuration_error() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let (listener, address) = live_endpoint().await;
        let client = client_with(&address);
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let session_owner = Arc::clone(&client.session_owner);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _session = session_owner.session().unwrap();
                panic!("poison the session for the regression test");
            }))
            .is_err()
        );

        let deadline = tokio::time::Instant::now() + TEST_BUDGET;
        let (header, result) = client
            .send_raw_vsr_attempt(
                SEND_MESSAGES_CODE,
                Bytes::from_static(b"send"),
                crate::vsr::RetainedRequest::default(),
                deadline,
                deadline,
                false,
            )
            .await;
        assert!(header.is_none());
        assert_eq!(result, Err(IggyError::InvalidConfiguration));
        assert_eq!(
            client.reset_vsr_session().await,
            Err(IggyError::InvalidConfiguration)
        );
        {
            let session = session_owner.session_ignoring_poison();
            assert_eq!(session.session(), Some(1));
            assert_eq!(session.current_request_id(), 1);
        }
        let mut byte = [0];
        assert_eq!(
            peer.try_read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        client.disconnect_transport().await.unwrap();
    }

    #[tokio::test]
    async fn a_retained_write_waiting_for_the_link_must_recheck_its_session() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let (listener, address) = live_endpoint().await;
        let client = client_with(&address);
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let payload = Bytes::from_static(b"uncertain-send");
        let retained = crate::vsr::encode_request_header(
            &mut client.session_owner.session().unwrap(),
            SEND_MESSAGES_CODE,
            &payload,
        )
        .unwrap()
        .0;
        let (peer, _) = listener.accept().await.unwrap();
        let link_guard = client.link.lock().await;
        let deadline = tokio::time::Instant::now() + TEST_BUDGET;
        let attempt = client.send_raw_vsr_attempt(
            SEND_MESSAGES_CODE,
            payload,
            RetainedRequest {
                header: Some(retained),
                context: PartitionContext::default(),
            },
            deadline,
            deadline,
            false,
        );
        tokio::pin!(attempt);
        tokio::select! {
            biased;
            _ = &mut attempt => panic!("the exchange must wait for the link lock"),
            () = tokio::task::yield_now() => {}
        }
        client.reset_vsr_session().await.unwrap();
        client.bind_vsr_session(2).await.unwrap();
        drop(link_guard);
        let (_, result) = tokio::time::timeout(TEST_BUDGET, &mut attempt)
            .await
            .unwrap();
        assert_eq!(result.unwrap_err(), IggyError::TransientNotCommitted);
        let mut byte = [0];
        assert_eq!(
            peer.try_read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        Client::shutdown(&client).await.unwrap();
    }

    #[tokio::test]
    async fn a_later_session_reset_does_not_replay_an_uncertain_write() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let (listener, address) = live_endpoint().await;
        let client = Arc::new(client_with(&address));
        Client::connect(&*client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let payload = Bytes::from_static(b"uncertain-send");
        let mut retained = RetainedRequest {
            header: Some(
                crate::vsr::encode_request_header(
                    &mut client.session_owner.session().unwrap(),
                    SEND_MESSAGES_CODE,
                    &payload,
                )
                .unwrap()
                .0,
            ),
            context: PartitionContext::default(),
        };
        let original = retained.header.unwrap();
        let peer_client = Arc::clone(&client);
        let (finished, mut finish) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (first, _) = read_test_request(&mut stream).await;
            assert_eq!(first.client, original.client);
            peer_client.reset_vsr_session().await.unwrap();
            peer_client.bind_vsr_session(2).await.unwrap();
            answer_test_request(
                &mut stream,
                &first,
                IggyError::TransientNotAccepted.as_code(),
                &[],
            )
            .await;
            loop {
                let (header, body) = tokio::select! {
                    _ = &mut finish => return false,
                    request = read_test_request(&mut stream) => request,
                };
                assert_eq!(body, b"uncertain-send");
                if header.client != original.client {
                    answer_test_request(&mut stream, &header, 0, b"duplicate-append").await;
                    return true;
                }
                answer_test_request(
                    &mut stream,
                    &header,
                    IggyError::TransientNotAccepted.as_code(),
                    &[],
                )
                .await;
            }
        });
        let result = tokio::time::timeout(
            TEST_BUDGET,
            client.send_raw_retaining_header(
                SEND_MESSAGES_CODE,
                payload,
                &mut retained,
                &IggyError::TransientNotCommitted,
            ),
        )
        .await
        .expect("a session reset must stop replay before the request deadline");
        let _ = finished.send(());
        let replayed_under_new_identity = peer.await.unwrap();
        Client::shutdown(&*client).await.unwrap();
        assert!(
            !replayed_under_new_identity,
            "an ambiguous send was reissued under a new identity"
        );
        assert_eq!(result.unwrap_err(), IggyError::TransientNotCommitted);
        assert_eq!(retained.header.unwrap().client, original.client);
    }

    #[tokio::test]
    async fn given_committed_lifecycle_refusal_when_retried_should_use_a_fresh_request_id() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let (listener, address) = live_endpoint().await;
        let client = client_with(&address);
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let context = PartitionContext {
            incarnation: 17,
            owner_generation: 3,
            metadata_op: 4,
        };
        let payload = Bytes::from_static(b"join-body");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (first, body) = read_test_request(&mut stream).await;
            assert_eq!(first.operation, Operation::JoinConsumerGroup);
            assert_eq!(body, b"join-body");
            let mut rejection = [0; REJECTION_SECTION_LEN];
            write_rejection_section(&mut rejection, IggyError::LifecycleBusy.as_code());
            answer_test_request(&mut stream, &first, 0, &rejection).await;
            let (second, retry_body) = read_test_request(&mut stream).await;
            assert_ne!(second.request, first.request);
            assert_eq!(second.client, first.client);
            assert_eq!(second.session, first.session);
            assert_eq!(second.partition_incarnation, first.partition_incarnation);
            assert_eq!(second.owner_generation, first.owner_generation);
            assert_eq!(second.minimum_metadata_op, first.minimum_metadata_op);
            assert_eq!(first.partition_incarnation, context.incarnation);
            assert_eq!(first.owner_generation, context.owner_generation);
            assert_eq!(first.minimum_metadata_op, context.metadata_op);
            assert_eq!(retry_body, body);
            let mut success = 0u32.to_le_bytes().to_vec();
            success.extend_from_slice(b"joined");
            answer_test_request(&mut stream, &second, 0, &success).await;
        });
        let deadline = tokio::time::Instant::now() + TEST_BUDGET;
        let (_, result) = tokio::time::timeout(
            TEST_BUDGET,
            client.send_raw_vsr_attempt(
                JOIN_CONSUMER_GROUP_CODE,
                payload,
                RetainedRequest {
                    header: None,
                    context,
                },
                deadline,
                deadline,
                true,
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap(), Bytes::from_static(b"joined"));
        peer.await.unwrap();
        Client::shutdown(&client).await.unwrap();
    }

    #[tokio::test]
    async fn given_lasting_lifecycle_refusal_when_retried_should_back_off() {
        const RETRY_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        // A pause doubling from 50 ms fits five attempts into the budget, a fixed one twenty.
        const MAX_ATTEMPTS: usize = 5;
        let (listener, address) = live_endpoint().await;
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut rejection = [0; REJECTION_SECTION_LEN];
            write_rejection_section(&mut rejection, IggyError::LifecycleBusy.as_code());
            let mut attempts = 0;
            let mut header = [0; HEADER_SIZE];
            while stream.read_exact(&mut header).await.is_ok() {
                attempts += 1;
                let request: RequestHeader =
                    bytemuck::checked::try_pod_read_unaligned(&header).unwrap();
                answer_test_request(&mut stream, &request, 0, &rejection).await;
            }
            attempts
        });
        let client = client_with(&address);
        Client::connect(&client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let deadline = tokio::time::Instant::now() + RETRY_BUDGET;

        let (_, result) = client
            .send_raw_vsr_attempt(
                JOIN_CONSUMER_GROUP_CODE,
                Bytes::new(),
                RetainedRequest::default(),
                deadline,
                deadline,
                true,
            )
            .await;

        Client::shutdown(&client).await.unwrap();
        assert_eq!(result.unwrap_err(), IggyError::LifecycleBusy);
        let attempts = peer.await.unwrap();
        assert!(
            attempts <= MAX_ATTEMPTS,
            "{attempts} retries each committed another lifecycle refusal"
        );
    }

    #[tokio::test]
    async fn given_lifecycle_refusal_when_retried_should_not_block_other_requests() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let (listener, address) = live_endpoint().await;
        let client = Arc::new(client_with(&address));
        Client::connect(&*client).await.unwrap();
        client.bind_vsr_session(1).await.unwrap();
        let (refused, first_refusal) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
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
                let deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
                client
                    .send_raw_vsr_attempt(
                        JOIN_CONSUMER_GROUP_CODE,
                        Bytes::new(),
                        RetainedRequest::default(),
                        deadline,
                        deadline,
                        true,
                    )
                    .await
                    .1
            })
        };
        first_refusal.await.unwrap();
        // Start inside the first pause, so a retry that took its request id
        // before the pause would reach the peer below this request's id.
        tokio::time::sleep(NOT_READY_RETRY_INTERVAL / 2).await;

        let deadline = tokio::time::Instant::now() + TEST_BUDGET;
        let (_, sent) = tokio::time::timeout(
            TEST_BUDGET,
            client.send_raw_vsr_attempt(
                SEND_MESSAGES_CODE,
                Bytes::from_static(b"send-body"),
                RetainedRequest::default(),
                deadline,
                deadline,
                true,
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
    async fn a_later_refusal_does_not_resolve_an_earlier_uncertain_write() {
        const READ_BUDGET: std::time::Duration = std::time::Duration::from_millis(150);
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        for later_refusal in [
            IggyError::TransientNotAccepted,
            IggyError::Unauthorized,
            IggyError::Unauthenticated,
            IggyError::StaleClient,
        ] {
            let (listener, address) = live_endpoint().await;
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut requests = Vec::new();
                let mut header = [0; HEADER_SIZE];
                while stream.read_exact(&mut header).await.is_ok() {
                    requests.push(header);
                    let request: RequestHeader =
                        bytemuck::checked::try_pod_read_unaligned(&header).unwrap();
                    let response = ReplyHeader {
                        command: Command::Reply,
                        operation: Operation::SendMessages,
                        client: request.client,
                        request: request.request,
                        size: u32::try_from(HEADER_SIZE).unwrap(),
                        status: if requests.len() == 1 {
                            IggyError::TransientNotCommitted.as_code()
                        } else {
                            later_refusal.as_code()
                        },
                        ..Default::default()
                    };
                    if stream
                        .write_all(bytemuck::bytes_of(&response))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                requests
            });
            let client = client_with(&address);
            Client::connect(&client).await.unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let now = tokio::time::Instant::now();
            let (_, result) = tokio::time::timeout(
                TEST_BUDGET,
                client.send_raw_vsr_attempt(
                    SEND_MESSAGES_CODE,
                    Bytes::new(),
                    RetainedRequest::default(),
                    now,
                    now + READ_BUDGET,
                    true,
                ),
            )
            .await
            .expect("replaying an uncertain write must remain bounded");
            Client::shutdown(&client).await.unwrap();
            let requests = peer.await.unwrap();
            assert!(
                requests.len() >= 2,
                "the peer must send both transient statuses"
            );
            assert!(requests.iter().all(|request| request == &requests[0]));
            assert!(
                matches!(
                    result,
                    Err(IggyError::TransientNotCommitted | IggyError::Disconnected)
                ),
                "a later refusal must not authorize a fresh write: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn given_an_uncertain_write_when_the_replay_loses_the_link_should_stay_uncertain() {
        const READ_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        for (evict_on_replay, expected) in [
            (true, IggyError::TransientNotCommitted),
            (false, IggyError::Disconnected),
        ] {
            let (listener, address) = live_endpoint().await;
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request, _) = read_test_request(&mut stream).await;
                let uncertain = IggyError::TransientNotCommitted.as_code();
                answer_test_request(&mut stream, &request, uncertain, &[]).await;
                // Without an eviction the connection closes before the replay.
                if evict_on_replay {
                    read_test_request(&mut stream).await;
                    let eviction =
                        EvictionHeader::new(0, 0, 0, request.client, EvictionReason::SessionError);
                    stream
                        .write_all(bytemuck::bytes_of(&eviction))
                        .await
                        .unwrap();
                }
            });
            let client = client_with(&address);
            Client::connect(&client).await.unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let now = tokio::time::Instant::now();
            let (_, result) = client
                .send_raw_vsr_attempt(
                    SEND_MESSAGES_CODE,
                    Bytes::new(),
                    RetainedRequest::default(),
                    now,
                    now + READ_BUDGET,
                    true,
                )
                .await;
            peer.await.unwrap();
            assert_eq!(result.unwrap_err(), expected);
        }
    }

    #[tokio::test]
    async fn a_canceled_exchange_discards_its_incomplete_reply_before_the_next_request() {
        const READ_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);
        let (listener, address) = live_endpoint().await;
        let client = client_with(&address);
        Client::connect(&client).await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let deadline = tokio::time::Instant::now() + READ_BUDGET;
        let mut exchange = Box::pin(client.send_raw_vsr_attempt(
            GET_ME_CODE,
            Bytes::new(),
            RetainedRequest::default(),
            tokio::time::Instant::now(),
            deadline,
            false,
        ));
        let mut request = [0; HEADER_SIZE];
        tokio::select! {
            result = &mut exchange => panic!("exchange ended before its reply: {result:?}"),
            result = peer.read_exact(&mut request) => { result.unwrap(); }
        }
        let reply = ReplyHeader {
            command: Command::Reply,
            operation: Operation::NonReplicated,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            ..Default::default()
        };
        peer.write_all(&bytemuck::bytes_of(&reply)[..HEADER_SIZE / 2])
            .await
            .unwrap();
        // Dropping the caller leaves the transport task holding the link lock.
        drop(exchange);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.send_poll_request(GET_ME_CODE, Bytes::new()),
        )
        .await
        .expect("the detached exchange must release its link lock");
        assert!(matches!(result, Err(IggyError::NotConnected)));
        assert!(client.link.lock().await.is_none());
        let mut next_request = [0; 1];
        assert_eq!(peer.read(&mut next_request).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_routed_consumer_operation_with_an_unknown_outcome_is_not_replayed() {
        for code in [
            iggy_binary_protocol::codes::POLL_MESSAGES_ON_PRIMARY_CODE,
            iggy_binary_protocol::codes::STORE_CONSUMER_OFFSET_CODE,
            iggy_binary_protocol::codes::DELETE_CONSUMER_OFFSET_CODE,
        ] {
            let (listener, address) = live_endpoint().await;
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut requests = 0;
                let mut header = [0; HEADER_SIZE];
                while stream.read_exact(&mut header).await.is_ok() {
                    requests += 1;
                    let request: RequestHeader =
                        bytemuck::checked::try_pod_read_unaligned(&header).unwrap();
                    let response = ReplyHeader {
                        command: Command::Reply,
                        operation: Operation::NonReplicated,
                        client: request.client,
                        request: request.request,
                        size: u32::try_from(HEADER_SIZE).unwrap(),
                        status: IggyError::TransientNotCommitted.as_code(),
                        ..Default::default()
                    };
                    if stream
                        .write_all(bytemuck::bytes_of(&response))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                requests
            });
            let client = client_with(&address);
            Client::connect(&client).await.unwrap();
            client.bind_vsr_session(1).await.unwrap();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                client.send_poll_request(code, Bytes::new()),
            )
            .await
            .expect("an unknown outcome must return without entering a retry window");
            assert!(
                matches!(result, Err(IggyError::TransientNotCommitted)),
                "code {code}: {result:?}"
            );
            Client::shutdown(&client).await.unwrap();
            assert_eq!(
                peer.await.unwrap(),
                1,
                "an unknown outcome must not become a later non-admission"
            );
        }
    }

    #[test]
    fn tls_server_names_support_dns_ipv4_and_ipv6_endpoints() {
        assert_eq!(tls_server_name("iggy-1:8090"), "iggy-1");
        assert_eq!(tls_server_name("127.0.0.1:8090"), "127.0.0.1");
        assert_eq!(tls_server_name("[fd00::1]:8090"), "fd00::1");
    }

    fn client_with(server_address: &str) -> TcpClient {
        TcpClient::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            ..TcpClientConfig::default()
        }))
        .expect("create the client")
    }

    /// A client whose roster names `endpoints`, as a leader check leaves it.
    async fn client_with_roster(server_address: &str, endpoints: Vec<String>) -> TcpClient {
        let client = client_with(server_address);
        *client.roster_endpoints.lock().await = endpoints;
        client
    }

    /// A listener nothing ever accepts from: the kernel completes the TCP
    /// handshake out of its backlog, which is all a dial needs to succeed.
    async fn live_endpoint() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a listener");
        let address = listener.local_addr().expect("listener address").to_string();
        (listener, address)
    }

    /// An address with nothing behind it: the dial is refused at once.
    async fn dead_endpoint() -> String {
        let (listener, address) = live_endpoint().await;
        drop(listener);
        address
    }

    /// A peer that accepts TCP and hangs up without a byte: enough for the
    /// dial, never enough for a TLS handshake or a sign-in. The counter says
    /// how many dials reached it.
    async fn counted_endpoint_that_hangs_up() -> (String, Arc<AtomicUsize>) {
        let (listener, address) = live_endpoint().await;
        let dials = Arc::new(AtomicUsize::new(0));
        let accepted = dials.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        (address, dials)
    }

    async fn endpoint_that_hangs_up() -> String {
        counted_endpoint_that_hangs_up().await.0
    }

    #[tokio::test]
    async fn concurrent_connect_waits_for_the_owners_result() {
        let (_listener, silent) = live_endpoint().await;
        let client = Arc::new(
            TcpClient::create(Arc::new(TcpClientConfig {
                server_address: silent,
                tls_enabled: true,
                tls_validate_certificate: false,
                reconnection: TcpClientReconnectionConfig {
                    enabled: false,
                    ..TcpClientReconnectionConfig::default()
                },
                ..TcpClientConfig::default()
            }))
            .expect("create the client"),
        );
        let owner = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { TcpClient::connect(&client).await })
        };
        while client.get_state().await != ClientState::Connecting {
            tokio::task::yield_now().await;
        }
        let mut waiter = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { TcpClient::connect(&client).await })
        };

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter)
                .await
                .is_err(),
            "a concurrent caller reported success while the owner was still connecting"
        );
        owner.abort();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(IggyError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn an_unrelated_public_login_cannot_take_over_a_connect_owner() {
        let (_listener, silent) = live_endpoint().await;
        let client = Arc::new(
            TcpClient::create(Arc::new(TcpClientConfig {
                server_address: silent,
                tls_enabled: true,
                tls_validate_certificate: false,
                ..TcpClientConfig::default()
            }))
            .expect("create the client"),
        );
        let owner = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { TcpClient::connect(&client).await })
        };
        while client.get_state().await != ClientState::Connecting {
            tokio::task::yield_now().await;
        }
        let mut login = {
            let client = Arc::clone(&client);
            tokio::spawn(async move { client.login_user("iggy", "iggy").await })
        };

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut login)
                .await
                .is_err(),
            "an unrelated login bypassed the connect owner instead of waiting"
        );
        assert_eq!(client.get_state().await, ClientState::Connecting);
        owner.abort();
        assert!(login.await.unwrap().is_err());
    }

    // With reconnection off there are no retries, but the endpoints the roster
    // named are still there to be tried and each gets its one turn.
    #[tokio::test]
    async fn a_client_with_reconnection_disabled_still_sweeps_the_endpoints_it_knows() {
        let (_listener, survivor) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![survivor.clone()];

        TcpClient::connect(&client).await.expect("connect");
        assert_eq!(*client.current_server_address.lock().await, survivor);
    }

    // A connect that gives up has to leave the state at `Disconnected`: left
    // at `Connecting`, the next `connect()` returns ok at the top without ever
    // dialing, and the client is wedged for good.
    #[tokio::test]
    async fn a_connect_that_exhausts_every_endpoint_leaves_the_client_disconnected() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![dead_endpoint().await];

        assert!(matches!(
            TcpClient::connect(&client).await,
            Err(IggyError::CannotEstablishConnection)
        ));
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert!(
            matches!(
                TcpClient::connect(&client).await,
                Err(IggyError::CannotEstablishConnection)
            ),
            "a second connect has to dial again rather than report success"
        );
    }

    // Each failed connect publishes `Disconnected`, and nothing reads the
    // receiver the client keeps. A server that stays down must not fill the
    // event channel and wedge the next connect, nor every call queued behind it.
    #[tokio::test]
    async fn failed_connects_past_the_event_capacity_do_not_wedge_the_client() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

        for attempt in 1..=DIAGNOSTIC_EVENT_CAPACITY + 1 {
            let connect = tokio::time::timeout(Duration::from_secs(5), TcpClient::connect(&client))
                .await
                .unwrap_or_else(|_| panic!("connect {attempt} hung"));

            assert!(
                matches!(connect, Err(IggyError::CannotEstablishConnection)),
                "connect {attempt}: {connect:?}"
            );
        }
    }

    // An endpoint that accepts TCP but fails the TLS handshake is not where
    // this client lives: recording it would make the next connect lead with
    // it and shadow every endpoint behind it.
    #[tokio::test]
    async fn an_endpoint_that_fails_the_tls_handshake_does_not_become_the_current_one() {
        let configured = dead_endpoint().await;
        // Plain TCP behind a TLS client: the dial succeeds, the handshake
        // cannot.
        let plaintext = endpoint_that_hangs_up().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: configured.clone(),
            tls_enabled: true,
            tls_validate_certificate: false,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![plaintext];

        assert!(matches!(
            TcpClient::connect(&client).await,
            Err(IggyError::CannotEstablishConnection)
        ));
        assert_eq!(*client.current_server_address.lock().await, configured);
    }

    // A peer that accepts TCP and then never answers is what the other
    // endpoints are there for; without a bound on the handshake the sweep
    // waits on it forever.
    #[tokio::test]
    async fn an_endpoint_that_never_answers_the_handshake_does_not_hold_up_the_sweep() {
        let (_listener, silent) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: silent,
            tls_enabled: true,
            tls_validate_certificate: false,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![dead_endpoint().await];

        let sweep = tokio::time::timeout(
            FAILOVER_DIAL_TIMEOUT * 3,
            std::pin::pin!(TcpClient::connect(&client)),
        )
        .await
        .expect("the sweep has to end on its own");
        assert!(matches!(sweep, Err(IggyError::CannotEstablishConnection)));
    }

    /// A peer that accepts TCP and then answers a ClientHello with something
    /// else, so the handshake fails on the peer's own answer. The counter says
    /// how many dials reached it.
    async fn counted_endpoint_that_speaks_no_tls() -> (String, Arc<AtomicUsize>) {
        let (listener, address) = live_endpoint().await;
        let dials = Arc::new(AtomicUsize::new(0));
        let accepted = dials.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let _ = stream.write_all(b"this is not a TLS record\n").await;
                    // Held open, so the failure is the handshake's verdict
                    // rather than a closed socket.
                    let mut sink = [0u8; 64];
                    while stream.read(&mut sink).await.is_ok_and(|read| read > 0) {}
                });
            }
        });
        (address, dials)
    }

    // A handshake verdict describes the peer -- a certificate that names
    // another host, an answer that is not TLS at all -- and not this client's
    // configuration, so it ends the dial rather than the connect: the endpoints
    // behind it are untried, and a redial can find a repaired node.
    #[tokio::test]
    async fn a_handshake_the_peer_failed_is_dialed_again() {
        let (plaintext, dials) = counted_endpoint_that_speaks_no_tls().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: plaintext,
            tls_enabled: true,
            tls_validate_certificate: false,
            reconnection: TcpClientReconnectionConfig {
                // One retry, so the pass runs twice and the connect still ends
                // on its own.
                max_retries: Some(1),
                interval: NonZeroIggyDuration::from_str("100ms").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

        let connect = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            std::pin::pin!(TcpClient::connect(&client)),
        )
        .await
        .expect("the connect has to end on its own");
        assert!(matches!(connect, Err(IggyError::CannotEstablishConnection)));
        assert_eq!(
            dials.load(Ordering::SeqCst),
            2,
            "a handshake the peer failed ended the connect instead of the dial"
        );
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    // A CA file that cannot be read is this client's own configuration, and it
    // says the same thing on every attempt: reported as a lost connection it
    // would be redialed every interval forever under `max_retries = None`,
    // which is how a wrong CA path looks like a flaky network.
    #[tokio::test]
    async fn a_ca_file_that_cannot_be_read_ends_the_connect() {
        let (_listener, endpoint) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: endpoint,
            tls_enabled: true,
            tls_validate_certificate: true,
            tls_ca_file: Some("no-such-ca-file.pem".to_string()),
            reconnection: TcpClientReconnectionConfig {
                // Unlimited retries, so a transient classification never
                // returns and this test times out instead of failing.
                max_retries: None,
                interval: NonZeroIggyDuration::from_str("100ms").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

        let connect = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            std::pin::pin!(TcpClient::connect(&client)),
        )
        .await
        .expect("the connect has to end on its own");
        assert!(matches!(connect, Err(IggyError::InvalidTlsCertificatePath)));
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    // A node that answers the dial and then cannot carry the sign-in has to
    // hand the sweep on. Ending it there leaves that node the one the client is
    // recorded on, so every later connect leads with it and the endpoints
    // behind it are never reached.
    #[tokio::test]
    async fn a_sign_in_that_failed_hands_the_sweep_on_to_the_next_endpoint() {
        let (dialed_first, _) = counted_endpoint_that_hangs_up().await;
        let (survivor, survivor_dials) = counted_endpoint_that_hangs_up().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dialed_first,
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "iggy".to_string(),
                "iggy".into(),
            )),
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![survivor];

        assert!(TcpClient::connect(&client).await.is_err());
        assert_eq!(
            survivor_dials.load(Ordering::SeqCst),
            1,
            "the endpoint behind the one whose sign-in failed was never dialed"
        );
        assert_eq!(client.get_state().await, ClientState::Disconnected);
    }

    // A sign-in that lost its socket leaves nothing to send on, so the
    // transport goes with it: kept behind a `Connected`, the next `connect()`
    // would return ok without dialing and every gated operation would fail
    // until someone disconnected by hand.
    #[tokio::test]
    async fn a_sign_in_that_lost_its_socket_leaves_the_client_disconnected() {
        let (listener, address) = live_endpoint().await;
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                // Accept, then hang up: the dial succeeds and the sign-in dies
                // on the socket.
                drop(stream);
            }
        });

        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: address,
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "iggy".to_string(),
                "iggy".into(),
            )),
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

        assert!(TcpClient::connect(&client).await.is_err());
        assert_eq!(client.get_state().await, ClientState::Disconnected);
        assert!(
            client.link.lock().await.is_none(),
            "a dead connection must not be kept for the next request to find"
        );
    }

    // The reconnect registers a new client identity, so the server's dedup
    // fence no longer covers the original request.
    #[test]
    fn only_requests_that_cannot_double_apply_are_replayed() {
        // Never written, or refused before execution.
        assert!(replay_after_session_reset_is_safe(
            SEND_MESSAGES_CODE,
            &IggyError::NotConnected
        ));
        assert!(replay_after_session_reset_is_safe(
            SEND_MESSAGES_CODE,
            &IggyError::CannotEstablishConnection
        ));
        // Written, and its outcome unknown: a replicated write must not be
        // re-sent under a session the fence cannot match it against. An
        // eviction is consumed in place of the reply, so it says nothing about
        // whether the write committed.
        assert!(!replay_after_session_reset_is_safe(
            SEND_MESSAGES_CODE,
            &IggyError::StaleClient
        ));
        assert!(!replay_after_session_reset_is_safe(
            SEND_MESSAGES_CODE,
            &IggyError::Disconnected
        ));
        assert!(!replay_after_session_reset_is_safe(
            SEND_MESSAGES_CODE,
            &IggyError::EmptyResponse
        ));
        // A read never enters the log, and a logout ends a session the
        // reconnect already replaced -- `logout_before_relogin` depends on it.
        assert!(replay_after_session_reset_is_safe(
            GET_ME_CODE,
            &IggyError::Disconnected
        ));
        assert!(replay_after_session_reset_is_safe(
            LOGOUT_USER_CODE,
            &IggyError::Disconnected
        ));
        // The register replay is the protocol: the server stays silent on a
        // transient failure and waits for the resend.
        assert!(replay_after_session_reset_is_safe(
            LOGIN_REGISTER_CODE,
            &IggyError::Disconnected
        ));
    }

    // `reestablish_after` paces reconnects to the endpoint that was lost. With
    // somewhere else to go, that pause must not hold up the failover.
    #[tokio::test]
    async fn a_pending_reestablish_pause_does_not_delay_dialing_another_endpoint() {
        let (_listener, survivor) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            reconnection: TcpClientReconnectionConfig {
                reestablish_after: IggyDuration::from_str("10s").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![survivor.clone()];
        client
            .connected_at
            .lock()
            .await
            .replace(IggyTimestamp::now());

        let started = std::time::Instant::now();
        TcpClient::connect(&client).await.expect("connect");
        assert_eq!(*client.current_server_address.lock().await, survivor);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "the failover waited out a pause it owed only the lost endpoint: {:?}",
            started.elapsed()
        );
    }

    // The other half of the same promise: `with_reestablish_after` is a
    // cooldown on redialing the endpoint that was lost, and a known roster
    // does not cancel it.
    #[tokio::test]
    async fn the_reestablish_pause_still_applies_to_the_endpoint_that_was_lost() {
        let (_listener, current) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: current.clone(),
            reconnection: TcpClientReconnectionConfig {
                reestablish_after: IggyDuration::from_str("1s").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        *client.roster_endpoints.lock().await = vec![dead_endpoint().await];
        client
            .connected_at
            .lock()
            .await
            .replace(IggyTimestamp::now());

        let started = std::time::Instant::now();
        TcpClient::connect(&client).await.expect("connect");
        assert_eq!(*client.current_server_address.lock().await, current);
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(700),
            "the cooldown on the lost endpoint was skipped: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn dial_candidates_lead_with_the_current_endpoint_and_name_each_other_one_once() {
        let client = client_with_roster(
            "127.0.0.1:8090",
            vec![
                "127.0.0.1:8090".to_string(),
                "localhost:8090".to_string(),
                "127.0.0.1:8091".to_string(),
            ],
        )
        .await;

        // The current endpoint leads and the roster follows, the same order as
        // the other SDKs. An endpoint the roster names again earns no second
        // dial, whether it is spelled the same way or not.
        assert_eq!(
            client.dial_candidates().await,
            vec!["127.0.0.1:8090".to_string(), "127.0.0.1:8091".to_string()]
        );
    }

    #[tokio::test]
    async fn a_client_that_learned_no_roster_dials_only_its_configured_endpoint() {
        let client = client_with("127.0.0.1:8090");

        assert_eq!(
            client.dial_candidates().await,
            vec!["127.0.0.1:8090".to_string()]
        );
    }

    // The C++/Rust e2e contract: `login -> disconnect -> op` must fail until
    // the caller signs in again. Only involuntary drops keep the sign-in.
    #[tokio::test]
    async fn an_explicit_disconnect_forgets_the_remembered_sign_in() {
        let client = client_with("127.0.0.1:8090");
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
                SESSION_USER_ID,
            )
            .await;

        Client::disconnect(&client).await.expect("disconnect");
        assert!(
            client.session_owner.sign_in_credentials().is_none(),
            "an explicit disconnect ends the session for good, like a logout"
        );
    }

    #[tokio::test]
    async fn a_transport_drop_keeps_the_remembered_sign_in() {
        let client = client_with("127.0.0.1:8090");
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
                SESSION_USER_ID,
            )
            .await;

        client
            .disconnect_transport()
            .await
            .expect("transport teardown");
        assert!(
            client.session_owner.sign_in_credentials().is_some(),
            "an involuntary drop is what the failover exists for; the sign-in survives it"
        );
    }

    #[tokio::test]
    async fn given_an_aux_poll_client_when_it_connects_should_send_no_sign_in() {
        const TEST_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);
        let (listener, address) = live_endpoint().await;
        let client = client_with(&address);
        client.bind_vsr_session(1).await.unwrap();
        client.session_owner.remember(
            Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
            SESSION_USER_ID,
        );

        let poll_client = tokio::time::timeout(TEST_BUDGET, client.connect_poll_client(&address))
            .await
            .expect("a poll client must not wait for a sign-in reply")
            .unwrap();

        let (peer, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        assert_eq!(
            peer.try_read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "a poll client sent a sign-in on the shared session"
        );
        poll_client.disconnect_transport().await.unwrap();
    }

    #[test]
    fn should_fail_with_a_zero_heartbeat_interval() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?heartbeat_interval=none";

        let error = TcpClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

    #[test]
    fn should_succeed_with_a_zero_reestablish_after() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?reestablish_after=0";

        let client = TcpClient::from_connection_string(value).unwrap();

        assert!(client.config.reconnection.reestablish_after.is_zero());
    }

    #[test]
    fn should_fail_with_a_zero_reconnection_interval() {
        let value = "iggy+tcp://user:secret@127.0.0.1:1234?reconnection_interval=0";

        let error = TcpClient::from_connection_string(value).err();

        assert!(matches!(error, Some(IggyError::InvalidConnectionString)));
    }

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
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
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
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(u, p)) => {
                assert_eq!(u, &username.to_string());
                assert_eq!(p.expose_secret(), password);
            }
            other => panic!("expected UsernamePassword auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str(heartbeat_interval).unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert_eq!(
            tcp_client_config.reconnection.max_retries.unwrap(),
            reconnection_retries.parse::<u32>().unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
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
        match &tcp_client_config.auto_login {
            AutoLogin::Enabled(Credentials::PersonalAccessToken(t)) => {
                assert_eq!(t.expose_secret(), pat);
            }
            other => panic!("expected PersonalAccessToken auto_login, got {other:?}"),
        }

        assert!(!tcp_client_config.tls_enabled);
        assert!(tcp_client_config.tls_domain.is_empty());
        assert!(tcp_client_config.tls_ca_file.is_none());
        assert_eq!(
            tcp_client_config.heartbeat_interval,
            NonZeroIggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            NonZeroIggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }

    #[tokio::test]
    async fn a_reconnect_after_an_explicit_disconnect_is_refused() {
        let client = TcpClient::default();
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

    fn auto_login_client(server_address: String) -> TcpClient {
        TcpClient::create(Arc::new(TcpClientConfig {
            server_address,
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "iggy".to_string(),
                "iggy".into(),
            )),
            reconnection: TcpClientReconnectionConfig {
                enabled: true,
                max_retries: None,
                interval: NonZeroIggyDuration::from_str("50ms").expect("duration"),
                // The sweep checks the disconnect between pauses, so a zero
                // pause keeps this test about the check, not the pacing.
                reestablish_after: IggyDuration::new(Duration::ZERO),
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client")
    }

    #[tokio::test]
    async fn a_disconnect_during_a_reconnect_sweep_ends_the_sweep() {
        // Every sign-in fails on the socket, so with unlimited retries the
        // sweep only ends on a disconnect.
        let (address, accepts) = counted_endpoint_that_hangs_up().await;
        let client = Arc::new(auto_login_client(address));
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { TcpClient::connect(&client).await }
        });
        while accepts.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }

        Client::disconnect(&*client).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), sweep)
            .await
            .expect("the sweep must end after the disconnect")
            .expect("the sweep task must not panic");
        let accepts_after_end = accepts.load(Ordering::SeqCst);
        sleep(Duration::from_millis(300)).await;

        assert!(
            matches!(result, Err(IggyError::NotConnected)),
            "got {result:?}"
        );
        assert_eq!(
            accepts.load(Ordering::SeqCst),
            accepts_after_end,
            "nothing may redial after the sweep ends"
        );
    }

    #[tokio::test]
    async fn a_request_after_an_explicit_disconnect_fails_without_a_dial() {
        let (listener, address) = live_endpoint().await;
        let client = auto_login_client(address);
        Client::disconnect(&client).await.unwrap();

        let ping = tokio::time::timeout(
            Duration::from_secs(1),
            client.send_raw_with_response(PING_CODE, Bytes::new()),
        )
        .await
        .expect("a refused request must not wait");

        assert!(matches!(ping, Err(IggyError::NotConnected)), "got {ping:?}");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "the request must not dial"
        );
    }

    #[tokio::test]
    async fn a_disconnect_after_a_shutdown_keeps_reporting_the_shutdown() {
        let client = auto_login_client("127.0.0.1:8090".to_string());
        Client::shutdown(&client).await.unwrap();
        Client::disconnect(&client).await.unwrap();

        let ping = client.send_raw_with_response(PING_CODE, Bytes::new()).await;

        assert!(
            matches!(ping, Err(IggyError::ClientShutdown)),
            "got {ping:?}"
        );
    }

    /// Signs in with `credentials` and disconnects while the Register is in
    /// flight. The reply that comes after must sign nothing back in.
    async fn sign_in_answered_after_a_disconnect(credentials: Credentials) {
        let (listener, address) = live_endpoint().await;
        let client = Arc::new(client_with(&address));
        Client::connect(&*client).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let sign_in = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                match credentials {
                    Credentials::UsernamePassword(username, password) => client
                        .login_user(&username, password.expose_secret())
                        .await
                        .map(|_| ()),
                    Credentials::PersonalAccessToken(token) => client
                        .login_with_personal_access_token(token.expose_secret())
                        .await
                        .map(|_| ()),
                }
            }
        });
        let (register, _) = read_test_request(&mut server).await;
        assert_eq!(register.operation, Operation::Register);

        let disconnect =
            spawn_disconnect_behind_the_link(&client, TcpClient::caller_disconnected).await;
        answer_test_request(&mut server, &register, 0, &register_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sign_in = sign_in.await.unwrap();

        let state = client.get_state().await;
        let remembered = client.session_owner.sign_in_credentials().is_some();
        let bound = client.session_owner.epoch().bound;
        assert!(
            state == ClientState::Disconnected && !remembered && !bound,
            "sign-in {sign_in:?}, state {state:?}, remembered sign-in {remembered}, \
             session bound {bound}"
        );
    }

    #[tokio::test]
    async fn a_login_answered_after_a_disconnect_does_not_sign_the_client_back_in() {
        sign_in_answered_after_a_disconnect(Credentials::UsernamePassword(
            "iggy".to_string(),
            "iggy".into(),
        ))
        .await;
    }

    #[tokio::test]
    async fn a_token_login_answered_after_a_disconnect_does_not_sign_the_client_back_in() {
        sign_in_answered_after_a_disconnect(Credentials::PersonalAccessToken("token".into())).await;
    }

    #[tokio::test]
    async fn the_late_steps_of_a_sign_in_leave_a_disconnected_client_signed_out() {
        let client = TcpClient::default();

        let state = sign_in_steps_after_a_disconnect(&client).await;

        assert_eq!(state, ClientState::Disconnected);
        assert!(client.session_owner.sign_in_credentials().is_none());
    }

    /// A client whose connection dropped after a sign-in: the next sweep
    /// resumes its session, and dials the endpoint it left at once.
    async fn client_to_resume(server_address: &str) -> Arc<TcpClient> {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            reconnection: TcpClientReconnectionConfig {
                reestablish_after: IggyDuration::new(Duration::ZERO),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
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
        let (listener, address) = live_endpoint().await;
        let client = client_to_resume(&address).await;
        // The reconnect that a request runs after its connection dropped.
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let (bind, _) = read_test_request(&mut server).await;
        assert_eq!(
            u32::from_le_bytes(bind.reserved[..4].try_into().unwrap()),
            BIND_SESSION_CODE
        );

        let disconnect =
            spawn_disconnect_behind_the_link(&client, TcpClient::caller_disconnected).await;
        answer_test_request(&mut server, &bind, 0, &sign_in_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();
        let state = client.get_state().await;
        let bound = client.session_owner.epoch().bound;

        Client::connect(&*client).await.unwrap();
        let redial = tokio::time::timeout(TEST_BUDGET, listener.accept()).await;
        assert!(
            redial.is_ok() && matches!(sweep, Err(IggyError::NotConnected)) && !bound,
            "sweep {sweep:?}, state after the disconnect {state:?}, \
             session after the disconnect bound {bound}, \
             connect() after the disconnect dialed {}",
            redial.is_ok()
        );
    }

    #[tokio::test]
    async fn a_connect_right_after_a_disconnect_survives_a_resume_in_flight() {
        let (listener, address) = live_endpoint().await;
        let client = client_to_resume(&address).await;
        // The reconnect that a request of another task runs.
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let (bind, _) = read_test_request(&mut server).await;

        let restart = spawn_restart_behind_the_link(&client, TcpClient::caller_disconnected).await;
        answer_test_request(&mut server, &bind, 0, &sign_in_reply(1)).await;
        let connect = restart.await.unwrap();
        let sweep = sweep.await.unwrap();

        let state = client.get_state().await;
        let link = client.link.lock().await.is_some();
        assert!(
            connect.is_ok() && link,
            "connect() right after disconnect() {connect:?}, the sweep {sweep:?}, \
             state {state:?}, link installed {link}"
        );
    }

    #[tokio::test]
    async fn a_sweep_that_a_disconnect_ends_publishes_one_disconnected_event() {
        let (listener, address) = live_endpoint().await;
        let client = client_to_resume(&address).await;
        let mut events = client.subscribe_events().await;
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let (bind, _) = read_test_request(&mut server).await;
        answer_test_request(&mut server, &bind, 0, &sign_in_reply(1)).await;
        // The leader check that follows the resume.
        let (metadata, _) = read_test_request(&mut server).await;
        assert_eq!(
            u32::from_le_bytes(metadata.reserved[..4].try_into().unwrap()),
            GET_CLUSTER_METADATA_CODE
        );

        let disconnect =
            spawn_disconnect_behind_the_link(&client, TcpClient::caller_disconnected).await;
        // A refused leader check is inconclusive, so the sweep goes on to its end.
        answer_test_request(&mut server, &metadata, IggyError::Error.as_code(), &[]).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();

        let (disconnected, seen) = disconnected_events(&mut events);
        assert_eq!(disconnected, 1, "sweep {sweep:?}, events {seen:?}");
    }

    #[tokio::test]
    async fn a_resume_refused_by_a_disconnect_publishes_one_disconnected_event() {
        let (listener, address) = live_endpoint().await;
        let client = client_to_resume(&address).await;
        let mut events = client.subscribe_events().await;
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let (bind, _) = read_test_request(&mut server).await;

        let disconnect =
            spawn_disconnect_behind_the_link(&client, TcpClient::caller_disconnected).await;
        answer_test_request(&mut server, &bind, 0, &sign_in_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();

        let (disconnected, seen) = disconnected_events(&mut events);
        assert_eq!(disconnected, 1, "sweep {sweep:?}, events {seen:?}");
    }

    #[tokio::test]
    async fn a_sweep_login_refused_by_a_disconnect_publishes_one_disconnected_event() {
        let (listener, address) = live_endpoint().await;
        let client = Arc::new(auto_login_client(address));
        let mut events = client.subscribe_events().await;
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let (register, _) = read_test_request(&mut server).await;
        assert_eq!(register.operation, Operation::Register);

        let disconnect =
            spawn_disconnect_behind_the_link(&client, TcpClient::caller_disconnected).await;
        answer_test_request(&mut server, &register, 0, &register_reply(1)).await;
        disconnect.await.unwrap().unwrap();
        let sweep = sweep.await.unwrap();

        let (disconnected, seen) = disconnected_events(&mut events);
        assert_eq!(disconnected, 1, "sweep {sweep:?}, events {seen:?}");
    }

    /// The events published so far, and how many of them are `Disconnected`.
    fn disconnected_events(
        events: &mut Receiver<DiagnosticEvent>,
    ) -> (usize, Vec<DiagnosticEvent>) {
        let mut seen = Vec::new();
        while let Ok(event) = events.try_recv() {
            seen.push(event);
        }
        let disconnected = seen
            .iter()
            .filter(|event| matches!(event, DiagnosticEvent::Disconnected))
            .count();
        (disconnected, seen)
    }

    #[tokio::test]
    async fn two_connects_after_a_disconnect_return_from_one_sweep() {
        const TEST_BUDGET: Duration = Duration::from_secs(5);
        const RETRY_INTERVAL: Duration = Duration::from_millis(300);
        let client = Arc::new(
            TcpClient::create(Arc::new(TcpClientConfig {
                server_address: dead_endpoint().await,
                reconnection: TcpClientReconnectionConfig {
                    max_retries: Some(1),
                    interval: NonZeroIggyDuration::new(RETRY_INTERVAL).unwrap(),
                    reestablish_after: IggyDuration::new(Duration::ZERO),
                    ..TcpClientReconnectionConfig::default()
                },
                ..TcpClientConfig::default()
            }))
            .unwrap(),
        );
        // The reconnect that a request runs, now in its retry pause.
        let sweep = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.connect().await }
        });
        sleep(RETRY_INTERVAL / 3).await;
        assert!(
            client.connect_coordinator.is_active(),
            "the sweep must sit in its pause"
        );
        Client::disconnect(&*client).await.unwrap();

        let started = std::time::Instant::now();
        let connects: Vec<_> = (0..2)
            .map(|_| {
                let client = Arc::clone(&client);
                tokio::spawn(async move { (Client::connect(&*client).await, started.elapsed()) })
            })
            .collect();
        let mut results = Vec::new();
        for connect in connects {
            results.push(
                tokio::time::timeout(TEST_BUDGET, connect)
                    .await
                    .expect("the connect must return")
                    .unwrap(),
            );
        }
        let sweep = sweep.await.unwrap();

        let slowest = results.iter().map(|(_, took)| *took).max().unwrap();
        let fastest = results.iter().map(|(_, took)| *took).min().unwrap();
        assert!(
            slowest - fastest < RETRY_INTERVAL / 2,
            "the second connect ran a sweep of its own: sweep {sweep:?}, results {results:?}"
        );
    }

    #[tokio::test]
    async fn a_bind_at_a_fence_from_before_a_reset_is_refused() {
        let client = TcpClient::default();
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
        let client = TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap();

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
}
