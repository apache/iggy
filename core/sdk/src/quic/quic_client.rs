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
use crate::prelude::AutoLogin;
use crate::session::ConsensusSession;
use crate::vsr::replay_after_session_reset_is_safe;
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
    GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
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
                } else {
                    // A single-node roster can still be converging a newly
                    // committed partition. Retry this explicitly unadmitted
                    // request on the current endpoint within the same budget.
                    (current, false)
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

        if code == GET_CLUSTER_METADATA_CODE {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        if matches!(self.config.auto_login, AutoLogin::Disabled) && !is_login_register_code(code) {
            // Without auto-login a reconnect cannot re-establish the session, so
            // non-login requests are not recovered here - their transient replay
            // happens on the live connection inside `send_raw`. Login/register
            // is the exception: the server stays deliberately silent on a
            // transient register failure and relies on the client replaying via
            // a reconnect with a fresh session.
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

impl iggy_common::VsrSessionSealed for QuicClient {}

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
        drop(consensus_session);
        // Every fresh client identity passes through here, including one that
        // replaces a session the transport never reset: a connection lost
        // mid-request can leave the old session in place until this sign-in
        // re-mints it.
        self.consumer_group_state.clear_session_scoped();
        Ok(())
    }

    async fn reset_vsr_session(&self) -> Result<(), IggyError> {
        *self
            .consensus_session
            .lock()
            .expect("consensus session mutex poisoned") = ConsensusSession::new();
        self.consumer_group_state.clear_session_scoped();
        Ok(())
    }

    fn sdk_version(&self) -> &'static str {
        crate::SDK_VERSION
    }
}

impl BinaryClient for QuicClient {}

impl QuicClient {
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

            // Handle auto-login
            let should_redirect = match &self.config.auto_login {
                AutoLogin::Disabled => {
                    info!("Automatic sign-in is disabled.");
                    // Only `IggyClient` redirects after a manual sign-in, so
                    // a raw transport can stay on a backup, and nothing on
                    // the send path redirects either: its replicated writes
                    // replay on the live connection, then surface the
                    // transient failure to the caller.
                    false
                }
                AutoLogin::Enabled(credentials) => {
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

                        // A roster walk stays on the endpoint it dialed: the
                        // leader settlement below would put the connection
                        // straight back on the node whose partition replica
                        // keeps refusing the request. One connect only.
                        if settle_off_leader {
                            info!(
                                "{NAME} client stays on the dialed node for a partition failover."
                            );
                            false
                        } else {
                            // The sole leader settlement, and it runs
                            // authenticated. Any node completes a login now --
                            // a backup forwards the register to the primary --
                            // so this decides where later ops land, not
                            // whether sign-in works.
                            self.handle_leader_redirection().await?
                        }
                    }
                }
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
        self.reset_vsr_session().await?;
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
        let consensus_session = self.consensus_session.clone();
        // SAFETY: we run code holding the `connection` lock in a task so we can't be cancelled while holding the lock.
        tokio::spawn(async move {
            let connection = connection.lock().await;
            let Some(connection) = connection.as_ref() else {
                error!("Cannot send data. Client is not connected.");
                return Err(IggyError::NotConnected);
            };

            let (request_header, request_size) = {
                    let mut consensus_session = consensus_session
                        .lock()
                        .expect("consensus session mutex poisoned");
                    crate::vsr::encode_request_header(&mut consensus_session, code, &payload)?
                };
                trace!("Sending a QUIC VSR request of size {request_size} with code: {code}");
                // Same-connection transient resend, gated on the EXPLICIT
                // `TransientNotCommitted` frame only. The server answers every
                // transient submit with that frame (it is pre-commit by
                // construction, so replaying the SAME request header on a fresh
                // bidi cannot double-commit), and it no longer abandons a bidi
                // whose op is still committing. Silence therefore is NOT a
                // retry signal: the partition plane has no dedup or reply
                // cache, so resending a silently-unanswered request whose
                // first attempt was buffered and later commits would commit it
                // twice (duplicate `SendMessages`, or a succeeded delete coming
                // back as terminal `ConsumerOffsetNotFound`). A silent deadline
                // expiry surfaces `Disconnected` and takes the
                // reconnect path in `send_raw_with_response`, same as TCP.
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
                    )
                    .await
                    {
                        Ok(reply) => return Ok(reply),
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
        })
        .await
        .map_err(|e| {
            error!("Task execution failed during QUIC request: {}", e);
            IggyError::QuicError
        })?
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

/// Unit tests for QuicClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for QuicClient.
#[cfg(test)]
mod tests {
    use super::*;

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
