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
    LeaderRedirectionState, check_and_redirect_to_leader, is_same_address,
    is_unauthenticated_metadata_probe,
};
use crate::prelude::Client;
use crate::prelude::TcpClientConfig;
use crate::session::ConsensusSession;
use crate::tcp::tcp_connection_stream::TcpConnectionStream;
use crate::tcp::tcp_connection_stream_kind::ConnectionStreamKind;
use crate::tcp::tcp_tls_connection_stream::TcpTlsConnectionStream;
use crate::vsr::operation_for_code;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::codes::{LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE};
use iggy_binary_protocol::consensus::Operation;
#[cfg(test)]
use iggy_common::TcpClientReconnectionConfig;
use iggy_common::VsrSessionControl as _;
use iggy_common::{
    AutoLogin, ClientState, ConnectionString, ConnectionStringUtils, Credentials, DiagnosticEvent,
    IdKind, Identifier, IggyDuration, IggyError, IggyTimestamp, TcpConnectionStringOptions,
    TransportProtocol,
};
use iggy_common::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use secrecy::{ExposeSecret, SecretString};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
#[cfg(test)]
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio_rustls::{TlsConnector, TlsStream};
use tracing::{error, info, trace, warn};

const NAME: &str = "Iggy";
/// Upper bound for awaiting a reply on the lockstep VSR connection. Far
/// beyond any healthy round-trip; only trips when the server loses the
/// reply entirely (e.g. stalled replication quorum), which would otherwise
/// hold the stream lock forever and wedge the client.
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
    pub(crate) stream: Arc<Mutex<Option<ConnectionStreamKind>>>,
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
    /// Credentials a sign-in on this client succeeded with, so a reconnect --
    /// onto this node or, after a failover, another one -- can re-establish
    /// the session instead of surfacing `Unauthenticated`. Cleared on logout.
    session_credentials: Mutex<Option<RememberedSignIn>>,
    // `std::sync::Mutex` (not `tokio::sync::Mutex`): the critical section
    // is `encode_request_header`, which is pure CPU and never awaits. The
    // tokio variant would pay a waker alloc + internal semaphore on
    // contention with zero correctness benefit.
    consensus_session: Arc<StdMutex<ConsensusSession>>,
    skip_auto_login_once: Mutex<bool>,
    consumer_group_state: Arc<iggy_common::ConsumerGroupClientState>,
}

/// The sign-in a manual login on this client succeeded with, and who it signed
/// in as. The user matters because a password change has to swap the new
/// password in here, and `change_password` may well target somebody else.
#[derive(Debug)]
struct RememberedSignIn {
    credentials: Credentials,
    user_id: u32,
}

/// A connection that completed every step of coming up, TLS included.
struct EstablishedConnection {
    stream: ConnectionStreamKind,
    client_address: SocketAddr,
    remote_address: SocketAddr,
}

impl Default for TcpClient {
    fn default() -> Self {
        TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap()
    }
}

#[async_trait]
impl Client for TcpClient {
    async fn connect(&self) -> Result<(), IggyError> {
        TcpClient::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        // An explicit disconnect is caller intent, like a logout: the session
        // it ends must not be resurrected by the next reconnect, so the
        // remembered sign-in goes with it. Involuntary drops (a dead socket,
        // a failover) go through `disconnect_transport` and keep it.
        self.forget_session_credentials().await;
        TcpClient::disconnect_transport(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        TcpClient::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

#[async_trait]
#[async_trait]
impl BinaryTransport for TcpClient {
    async fn get_state(&self) -> ClientState {
        *self.state.lock().await
    }

    async fn set_state(&self, state: ClientState) {
        *self.state.lock().await = state;
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a TCP diagnostic event: {error}");
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
                | IggyError::TcpError
        ) {
            return Err(error);
        }

        if is_unauthenticated_metadata_probe(code, &error) {
            return Err(error);
        }

        if !self.config.reconnection.enabled {
            return Err(IggyError::Disconnected);
        }

        if !is_login_register_code(code) && self.sign_in_credentials().await.is_none() {
            // With no credentials -- neither configured nor remembered from a
            // sign-in -- a reconnect cannot re-establish the session, so
            // non-login requests fail fast. Login/register itself is the
            // exception: the server stays deliberately silent on transient
            // register failures (the server `surface_login_failure`) and
            // relies on the client timing out and replaying the request.
            return Err(error);
        }

        // Reconnecting heals the transport, but replaying the request over the
        // new connection is a second attempt under a new session:
        // `reset_vsr_session` drops the client id the server's dedup fence is
        // keyed on, so a replicated write that committed before its reply was
        // lost would apply a second time. Replay only what provably never
        // reached the log -- the errors raised before the request was written,
        // and the operations that never enter it.
        //
        // Login and register are the exception: the server stays deliberately
        // silent on a transient register failure and relies on the client
        // replaying, so that replay is the protocol rather than a retry.
        let replay_after_reconnect = is_login_register_code(code)
            || matches!(
                error,
                IggyError::NotConnected
                    | IggyError::CannotEstablishConnection
                    | IggyError::Unauthenticated
                    | IggyError::StaleClient
            )
            || matches!(operation_for_code(code), Operation::NonReplicated);

        self.disconnect_transport().await?;

        let skip_auto_login = is_login_register_code(code);
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

        let reconnect = self.connect().await;
        if skip_auto_login && reconnect.is_err() {
            *self.skip_auto_login_once.lock().await = false;
        }
        reconnect?;

        if !replay_after_reconnect {
            warn!(
                "Reconnected, but command: {code} is replicated and its outcome is unknown: \
                 replaying it under the new session could apply it twice, so the original \
                 error is returned instead."
            );
            return Err(error);
        }

        self.send_raw(code, payload).await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        self.config.heartbeat_interval
    }

    fn consumer_group_state(&self) -> Arc<iggy_common::ConsumerGroupClientState> {
        Arc::clone(&self.consumer_group_state)
    }
}

impl iggy_common::VsrSessionSealed for TcpClient {}

#[async_trait::async_trait]
impl iggy_common::VsrSessionControl for TcpClient {
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

    async fn remember_session_credentials(&self, credentials: Credentials, user_id: u32) {
        self.session_credentials
            .lock()
            .await
            .replace(RememberedSignIn {
                credentials,
                user_id,
            });
    }

    async fn forget_session_credentials(&self) {
        self.session_credentials.lock().await.take();
    }

    async fn refresh_session_password(&self, user: &Identifier, new_password: &str) {
        let mut remembered = self.session_credentials.lock().await;
        let Some(sign_in) = remembered.as_mut() else {
            return;
        };
        // A personal access token is not derived from the password.
        let Credentials::UsernamePassword(username, password) = &mut sign_in.credentials else {
            return;
        };

        let targets_session_user = match user.kind {
            IdKind::Numeric => user.get_u32_value().is_ok_and(|id| id == sign_in.user_id),
            IdKind::String => user
                .get_cow_str_value()
                .is_ok_and(|name| name.as_ref() == username),
        };
        if targets_session_user {
            *password = SecretString::from(new_password.to_owned());
        }
    }

    fn sdk_version(&self) -> &'static str {
        crate::SDK_VERSION
    }
}

impl BinaryClient for TcpClient {}

impl TcpClient {
    /// Create a new TCP client for the provided server address.
    pub fn new(
        server_address: &str,
        auto_sign_in: AutoLogin,
        heartbeat_interval: IggyDuration,
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
        heartbeat_interval: IggyDuration,
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
        let server_address = config.server_address.clone();
        Ok(Self {
            config,
            client_address: Mutex::new(None),
            stream: Arc::new(Mutex::new(None)),
            state: Mutex::new(ClientState::Disconnected),
            events: broadcast(1000),
            connected_at: Mutex::new(None),
            leader_redirection_state: Mutex::new(LeaderRedirectionState::new()),
            current_server_address: Mutex::new(server_address),
            roster_endpoints: Mutex::new(Vec::new()),
            session_credentials: Mutex::new(None),
            consensus_session: Arc::new(StdMutex::new(ConsensusSession::new())),
            skip_auto_login_once: Mutex::new(false),
            consumer_group_state: Arc::new(iggy_common::ConsumerGroupClientState::new()),
        })
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
                    let client_address = self.get_client_address_value().await;
                    trace!("Client: {client_address} is already connected.");
                    return Ok(());
                }
                ClientState::Connecting => {
                    trace!("Client is already connecting.");
                    return Ok(());
                }
                _ => {}
            }

            self.set_state(ClientState::Connecting).await;
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

            let mut retry_count = 0;
            let connection_stream: ConnectionStreamKind;
            let remote_address;
            let client_address;
            let mut candidate = 0;
            loop {
                let server_address = candidates[candidate].clone();
                if server_address == paced_endpoint
                    && let Some(remaining) = self.reestablish_wait().await
                {
                    info!("Trying to connect to the server: {server_address} in: {remaining}");
                    sleep(remaining.get_duration()).await;
                }

                info!("{NAME} client is connecting to server: {server_address}...");
                match self.establish_bounded(&server_address, &candidates).await {
                    Ok(connection) => {
                        // The endpoint that answered is where this client now
                        // lives: the leader check compares against it, and the
                        // next reconnect starts from it. Recorded only once the
                        // stream is usable, so a node that accepts TCP but
                        // fails the TLS handshake does not become sticky and
                        // shadow the endpoints behind it.
                        *self.current_server_address.lock().await = server_address;
                        client_address = connection.client_address;
                        remote_address = connection.remote_address;
                        self.client_address.lock().await.replace(client_address);
                        connection_stream = connection.stream;
                        break;
                    }
                    Err(IggyError::CannotEstablishConnection) => {}
                    Err(error) => {
                        // An unreadable CA file or an unusable TLS domain is a
                        // configuration fault: no other endpoint fixes it, and
                        // retrying forever under `max_retries = None` would
                        // only bury it.
                        self.fail_connect().await;
                        return Err(error);
                    }
                }

                // Every other endpoint gets its turn before the retry
                // interval: the node just lost may be gone for good, and
                // pausing on it helps nothing.
                candidate += 1;
                if candidate < candidates.len() {
                    continue;
                }
                candidate = 0;

                // The sweep is what reconnection settings apply to, not a
                // single dial: with reconnection off there are no retries, but
                // the failover endpoints were configured to be tried and they
                // get their one turn first.
                if !self.config.reconnection.enabled {
                    warn!("Automatic reconnection is disabled.");
                    self.fail_connect().await;
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
                return Err(IggyError::CannotEstablishConnection);
            }

            let now = IggyTimestamp::now();
            info!(
                "{NAME} client: {client_address} has connected to server: {remote_address} at: {now}",
            );
            self.stream.lock().await.replace(connection_stream);
            self.set_state(ClientState::Connected).await;
            self.connected_at.lock().await.replace(now);
            self.publish_event(DiagnosticEvent::Connected).await;
            let skip_auto_login = {
                let mut guard = self.skip_auto_login_once.lock().await;
                std::mem::take(&mut *guard)
            };

            // Handle auto-login
            let should_redirect = match self.sign_in_credentials().await {
                None => {
                    info!("No credentials to sign in with.");
                    // Only `IggyClient` redirects after a manual sign-in, so
                    // a raw transport can stay on a backup: its first
                    // replicated write gets `TransientNotAccepted`, the
                    // redirect drops the session, and the retry fails
                    // `Unauthenticated` until the caller signs in again.
                    false
                }
                Some(credentials) => {
                    if skip_auto_login {
                        info!("Skipping automatic sign-in for a retried login/register request.");
                        false
                    } else {
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
                            Ok(how) => {
                                info!("{NAME} client: {client_address} has signed in with {how}.")
                            }
                            Err(error) => {
                                // The transport is up and only the session is
                                // not, so the state has to say so: left at
                                // `Authenticating` every gated operation fails
                                // client-side with `Disconnected`, `connect()`
                                // returns ok without dialing, and nothing short
                                // of an explicit `login_user` recovers.
                                self.set_state(ClientState::Connected).await;
                                // A rejected credential does not become valid on
                                // the next reconnect, and replaying it costs an
                                // argon2 on the server every time. Configured
                                // credentials stay as configured -- they are the
                                // caller's to fix -- so only the remembered
                                // sign-in is dropped.
                                if matches!(
                                    error,
                                    IggyError::InvalidCredentials
                                        | IggyError::InvalidUsername
                                        | IggyError::InvalidPassword
                                        | IggyError::Unauthenticated
                                ) {
                                    self.forget_session_credentials().await;
                                }
                                return Err(error);
                            }
                        }

                        // The sole leader settlement, and it runs
                        // authenticated. Any node completes a login now -- a
                        // backup forwards the register to the primary -- so
                        // this decides where later ops land, not whether
                        // sign-in works.
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

    /// Whether an `AutoLogin` is configured on this client, which makes the
    /// session after any connect the configured user's rather than whoever
    /// signed in by hand.
    pub(crate) fn auto_login_configured(&self) -> bool {
        matches!(self.config.auto_login, AutoLogin::Enabled(_))
    }

    /// Credentials to sign in with after connecting: the configured ones, or
    /// else the ones a manual sign-in on this client succeeded with. A manual
    /// sign-in is otherwise less reconnectable than a configured one, which
    /// is a surprising difference between two ways of doing the same thing.
    async fn sign_in_credentials(&self) -> Option<Credentials> {
        match &self.config.auto_login {
            AutoLogin::Enabled(credentials) => Some(credentials.clone()),
            AutoLogin::Disabled => self
                .session_credentials
                .lock()
                .await
                .as_ref()
                .map(|remembered| remembered.credentials.clone()),
        }
    }

    /// Endpoints to dial for one connect, likeliest first: where the client
    /// currently is, then the roster it learned while connected, then the
    /// configured seeds.
    async fn dial_candidates(&self) -> Vec<String> {
        let mut candidates = vec![self.current_server_address.lock().await.clone()];
        let roster = self.roster_endpoints.lock().await.clone();
        for endpoint in roster.iter().chain(self.config.failover_addresses.iter()) {
            if !candidates
                .iter()
                .any(|candidate| is_same_address(candidate, endpoint))
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
            return Ok(EstablishedConnection {
                stream: ConnectionStreamKind::Tcp(TcpConnectionStream::new(client_address, stream)),
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
            // Extract hostname/IP from server_address when tls_domain is not specified
            server_address
                .split(':')
                .next()
                .unwrap_or(server_address)
                .to_string()
        } else {
            self.config.tls_domain.to_owned()
        };
        let domain = ServerName::try_from(tls_domain).map_err(|error| {
            error!("Failed to create a server name from the domain. {error}");
            IggyError::InvalidTlsDomain
        })?;
        let stream = connector.connect(domain, stream).await.map_err(|error| {
            error!("Failed to establish a TLS connection to the server: {error}");
            IggyError::CannotEstablishConnection
        })?;

        Ok(EstablishedConnection {
            stream: ConnectionStreamKind::TcpTls(TcpTlsConnectionStream::new(
                client_address,
                TlsStream::Client(stream),
            )),
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
        info!("Shutting down the {NAME} TCP client: {client_address}");
        let stream = self.stream.lock().await.take();
        if let Some(mut stream) = stream {
            stream.shutdown().await?;
        }
        self.reset_vsr_session().await?;
        self.set_state(ClientState::Shutdown).await;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        info!("{NAME} TCP client: {client_address} has been shutdown.");
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
            _ => {}
        }

        // One overall deadline bounds the request across transient replays
        // AND leader failovers, matching the previous single-connection
        // budget. Login/register replays stay on this connection for the
        // whole budget: the connect flow owns leader redirection for the
        // sign-in handshake, and reconnecting from underneath it would
        // recurse.
        let overall_deadline = tokio::time::Instant::now() + RESPONSE_READ_TIMEOUT;
        let mut preencoded = None;
        loop {
            let transient_deadline = if is_login_register_code(code) {
                overall_deadline
            } else {
                overall_deadline
                    .min(tokio::time::Instant::now() + TRANSIENT_FAILOVER_CHECK_INTERVAL)
            };
            let (header, result) = self
                .send_raw_vsr_attempt(
                    code,
                    payload.clone(),
                    preencoded,
                    transient_deadline,
                    overall_deadline,
                )
                .await;
            match result {
                Err(IggyError::TransientNotAccepted)
                    if tokio::time::Instant::now() < overall_deadline
                        && !is_login_register_code(code) =>
                {
                    // The server explicitly did NOT admit the request, so
                    // re-issuing it -- same id on this session, or a fresh
                    // id under a new session after a failover -- cannot
                    // double-apply. Keep the encoded id for same-session
                    // replays; a redirect re-registers, so the id is
                    // re-encoded under the new session.
                    // (`TransientNotCommitted` never reaches this branch:
                    // its outcome is unknown, so the attempt loop replays
                    // it same-session for the whole budget and then the
                    // error propagates to the caller.)
                    preencoded = header;
                    if let Ok(true) = self.handle_leader_redirection().await {
                        self.connect().await?;
                        preencoded = None;
                    }
                }
                Err(IggyError::Disconnected) => {
                    // Reply stream state is unknown (timed out or torn
                    // mid-frame); a late reply would desync framing for the
                    // next request, so drop the connection and let callers
                    // reconnect.
                    self.stream.lock().await.take();
                    self.set_state(ClientState::Disconnected).await;
                    return Err(IggyError::Disconnected);
                }
                other => return other,
            }
        }
    }

    /// One send attempt on the current connection: encode the header (or reuse
    /// `preencoded` so a same-session replay keeps its request id for the
    /// server's dedup), write the frame, and replay on `TransientNotCommitted`
    /// until `transient_deadline`. Reads are bounded by `read_deadline` -- the
    /// full request budget -- so a short transient window cannot tear down a
    /// connection that is merely slow to reply. Returns the header used so the
    /// caller can replay the same id on a later attempt.
    async fn send_raw_vsr_attempt(
        &self,
        code: u32,
        payload: Bytes,
        preencoded: Option<iggy_binary_protocol::consensus::RequestHeader>,
        transient_deadline: tokio::time::Instant,
        read_deadline: tokio::time::Instant,
    ) -> (
        Option<iggy_binary_protocol::consensus::RequestHeader>,
        Result<Bytes, IggyError>,
    ) {
        let stream = self.stream.clone();
        let consensus_session = self.consensus_session.clone();
        // SAFETY: we run code holding the `stream` lock in a task so we can't be cancelled while holding the lock.
        let joined = tokio::spawn(async move {
            let mut stream = stream.lock().await;
            let Some(stream) = stream.as_mut() else {
                error!("Cannot send data. Client is not connected.");
                return (None, Err(IggyError::NotConnected));
            };
            // Encode the request header ONCE per session: `next_request_id`
            // advances here, so a transient replay must reuse the same id for
            // the server's dedup. The connection is lockstep (one request in
            // flight per client), so a complete reply leaves the stream at a
            // clean frame boundary -- a `TransientNotCommitted` answer (the
            // server could not commit yet: not-caught-up / in-flight /
            // pipeline-full / view-change cancel) lets us resend the SAME
            // request on the SAME connection with no reconnect and the session
            // intact.
            let request_header = match preencoded {
                Some(header) => header,
                None => {
                    let encoded = {
                        let mut consensus_session = consensus_session
                            .lock()
                            .expect("consensus session mutex poisoned");
                        crate::vsr::encode_request_header(&mut consensus_session, code, &payload)
                    };
                    match encoded {
                        Ok((header, request_size)) => {
                            trace!(
                                "Sending a TCP VSR request of size {request_size} with code: {code}"
                            );
                            header
                        }
                        Err(error) => return (None, Err(error)),
                    }
                }
            };
            let header_bytes = bytemuck::bytes_of(&request_header);
            let outcome = async {
                loop {
                    stream.write(header_bytes).await?;
                    if !payload.is_empty() {
                        stream.write(&payload).await?;
                    }
                    stream.flush().await?;
                    trace!("Sent a TCP request with code: {code}, waiting for a response...");

                    let mut response_header = [0u8; iggy_binary_protocol::HEADER_SIZE];
                    // `stream.read` delegates to `read_exact`; on success it
                    // always returns the requested length, so no short-read
                    // guard is needed here.
                    //
                    // Deadline guards against server-side reply loss (e.g. a
                    // stalled replication quorum that never commits the op):
                    // the connection is lockstep, so an unanswered read would
                    // hold the stream lock forever and wedge every later
                    // request on this client. On expiry drop the stream --
                    // a late reply would desync framing for the next request.
                    //
                    // One deadline spans BOTH the header and body reads: a
                    // reply that delivers a header then stalls must not get a
                    // fresh full timeout for the body.
                    let header_read =
                        tokio::time::timeout_at(read_deadline, stream.read(&mut response_header))
                            .await;
                    let Ok(header_read) = header_read else {
                        error!(
                            "Timed out after {RESPONSE_READ_TIMEOUT:?} waiting for VSR response header for TCP request with code: {code}",
                        );
                        return Err(IggyError::Disconnected);
                    };
                    header_read.map_err(|error| {
                        error!(
                            "Failed to read VSR response header for TCP request with code: {code}: {error}",
                        );
                        IggyError::Disconnected
                    })?;

                    let response_size = crate::vsr::response_size(&response_header)?;
                    let body_size = response_size - iggy_binary_protocol::HEADER_SIZE;
                    let body = if body_size > 0 {
                        let mut body = BytesMut::with_capacity(body_size);
                        let body_read = tokio::time::timeout_at(
                            read_deadline,
                            stream.read_buf(&mut body, body_size),
                        )
                        .await;
                        let Ok(body_read) = body_read else {
                            error!(
                                "Timed out after {RESPONSE_READ_TIMEOUT:?} waiting for VSR response body for TCP request with code: {code}",
                            );
                            return Err(IggyError::Disconnected);
                        };
                        body_read.map_err(|error| {
                            error!(
                                "Failed to read VSR response body for TCP request with code: {code}: {error}",
                            );
                            IggyError::Disconnected
                        })?;
                        body.freeze()
                    } else {
                        Bytes::new()
                    };

                    match crate::vsr::decode_response_split(&response_header, body) {
                        // `TransientNotCommitted`: the op's outcome is unknown
                        // (e.g. a view change canceled it in flight) -- ONLY a
                        // same-session replay of the same request id is safe
                        // (the client-table serves it from cache if it did
                        // commit). Replay on this connection for the whole
                        // request budget; never hand it to the failover path,
                        // which re-issues under a fresh session and could
                        // double-apply a committed write.
                        Err(IggyError::TransientNotCommitted)
                            if tokio::time::Instant::now() < read_deadline =>
                        {
                            let remaining = read_deadline
                                .saturating_duration_since(tokio::time::Instant::now());
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
                        other => return other,
                    }
                }
            }
            .await;
            (Some(request_header), outcome)
        })
        .await;
        match joined {
            Ok(result) => result,
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

/// Unit tests for TcpClient.
/// Currently only tests for "from_connection_string()" are implemented.
/// TODO: Add complete unit tests for TcpClient.
#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_USER_ID: u32 = 7;

    fn client_with(server_address: &str, failover_addresses: Vec<String>) -> TcpClient {
        TcpClient::create(Arc::new(TcpClientConfig {
            server_address: server_address.to_string(),
            failover_addresses,
            ..TcpClientConfig::default()
        }))
        .expect("create the client")
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
    /// dial, never enough for a TLS handshake.
    async fn endpoint_that_hangs_up() -> String {
        let (listener, address) = live_endpoint().await;
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });
        address
    }

    // With reconnection off there are no retries, but the failover endpoints
    // were configured to be tried and each still gets its one turn.
    #[tokio::test]
    async fn a_client_with_reconnection_disabled_still_sweeps_its_failover_endpoints() {
        let (_listener, survivor) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            failover_addresses: vec![survivor.clone()],
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

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
            failover_addresses: vec![dead_endpoint().await],
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

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
            failover_addresses: vec![plaintext],
            tls_enabled: true,
            tls_validate_certificate: false,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

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
            failover_addresses: vec![dead_endpoint().await],
            tls_enabled: true,
            tls_validate_certificate: false,
            reconnection: TcpClientReconnectionConfig {
                enabled: false,
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");

        let sweep = tokio::time::timeout(
            FAILOVER_DIAL_TIMEOUT * 3,
            std::pin::pin!(TcpClient::connect(&client)),
        )
        .await
        .expect("the sweep has to end on its own");
        assert!(matches!(sweep, Err(IggyError::CannotEstablishConnection)));
    }

    // `reestablish_after` paces reconnects to the endpoint that was lost. With
    // somewhere else to go, that pause must not hold up the failover.
    #[tokio::test]
    async fn a_pending_reestablish_pause_does_not_delay_dialing_another_endpoint() {
        let (_listener, survivor) = live_endpoint().await;
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            server_address: dead_endpoint().await,
            failover_addresses: vec![survivor.clone()],
            reconnection: TcpClientReconnectionConfig {
                reestablish_after: IggyDuration::from_str("10s").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
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
            failover_addresses: vec![dead_endpoint().await],
            reconnection: TcpClientReconnectionConfig {
                reestablish_after: IggyDuration::from_str("1s").expect("duration"),
                ..TcpClientReconnectionConfig::default()
            },
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
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
        let client = client_with(
            "127.0.0.1:8090",
            vec!["127.0.0.1:8092".to_string(), "localhost:8090".to_string()],
        );
        *client.roster_endpoints.lock().await = vec![
            "127.0.0.1:8090".to_string(),
            "127.0.0.1:8091".to_string(),
            "127.0.0.1:8092".to_string(),
        ];

        // The current endpoint leads, the roster follows, and neither the
        // roster's copy of the current endpoint nor a seed that only spells
        // the same endpoint differently earns a second dial.
        assert_eq!(
            client.dial_candidates().await,
            vec![
                "127.0.0.1:8090".to_string(),
                "127.0.0.1:8091".to_string(),
                "127.0.0.1:8092".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_client_that_learned_no_roster_still_dials_its_configured_seeds() {
        let client = client_with("127.0.0.1:8090", vec!["127.0.0.1:8091".to_string()]);

        assert_eq!(
            client.dial_candidates().await,
            vec!["127.0.0.1:8090".to_string(), "127.0.0.1:8091".to_string()]
        );
    }

    // The C++/Rust e2e contract: `login -> disconnect -> op` must fail until
    // the caller signs in again. Only involuntary drops keep the sign-in.
    #[tokio::test]
    async fn an_explicit_disconnect_forgets_the_remembered_sign_in() {
        let client = client_with("127.0.0.1:8090", Vec::new());
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
                SESSION_USER_ID,
            )
            .await;

        Client::disconnect(&client).await.expect("disconnect");
        assert!(
            client.sign_in_credentials().await.is_none(),
            "an explicit disconnect ends the session for good, like a logout"
        );
    }

    #[tokio::test]
    async fn a_transport_drop_keeps_the_remembered_sign_in() {
        let client = client_with("127.0.0.1:8090", Vec::new());
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
            client.sign_in_credentials().await.is_some(),
            "an involuntary drop is what the failover exists for; the sign-in survives it"
        );
    }

    #[tokio::test]
    async fn a_sign_in_makes_a_client_without_auto_login_reconnectable() {
        let client = client_with("127.0.0.1:8090", Vec::new());
        assert!(client.sign_in_credentials().await.is_none());

        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "iggy".into()),
                SESSION_USER_ID,
            )
            .await;
        assert!(client.sign_in_credentials().await.is_some());

        // An explicit logout leaves no session to restore, and a reconnect
        // must not resurrect one.
        client.forget_session_credentials().await;
        assert!(client.sign_in_credentials().await.is_none());
    }

    // A password change for the signed-in user has to reach the remembered
    // sign-in, or the next reconnect replays the old password and fails an
    // unrelated request with `InvalidCredentials`.
    #[tokio::test]
    async fn a_password_change_for_the_signed_in_user_updates_the_remembered_sign_in() {
        let client = client_with("127.0.0.1:8090", Vec::new());
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "old".into()),
                SESSION_USER_ID,
            )
            .await;

        for user in [
            Identifier::numeric(SESSION_USER_ID).expect("numeric identifier"),
            Identifier::named("iggy").expect("named identifier"),
        ] {
            client.refresh_session_password(&user, "new").await;
            match client.sign_in_credentials().await {
                Some(Credentials::UsernamePassword(_, password)) => {
                    assert_eq!(password.expose_secret(), "new", "for user: {user}");
                }
                other => panic!("expected the remembered user credentials, got {other:?}"),
            }
            // Put it back so the second identifier form starts from the same
            // place as the first.
            client
                .remember_session_credentials(
                    Credentials::UsernamePassword("iggy".to_string(), "old".into()),
                    SESSION_USER_ID,
                )
                .await;
        }
    }

    // `change_password` can target anyone the caller may manage, and those
    // changes say nothing about the credentials this client reconnects with.
    #[tokio::test]
    async fn a_password_change_for_another_user_leaves_the_remembered_sign_in_alone() {
        let client = client_with("127.0.0.1:8090", Vec::new());
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "old".into()),
                SESSION_USER_ID,
            )
            .await;

        for user in [
            Identifier::numeric(SESSION_USER_ID + 1).expect("numeric identifier"),
            Identifier::named("someone-else").expect("named identifier"),
        ] {
            client.refresh_session_password(&user, "new").await;
            match client.sign_in_credentials().await {
                Some(Credentials::UsernamePassword(_, password)) => {
                    assert_eq!(password.expose_secret(), "old", "for user: {user}");
                }
                other => panic!("expected the remembered user credentials, got {other:?}"),
            }
        }
    }

    // A personal access token is not derived from any password.
    #[tokio::test]
    async fn a_password_change_leaves_a_remembered_personal_access_token_alone() {
        let client = client_with("127.0.0.1:8090", Vec::new());
        client
            .remember_session_credentials(
                Credentials::PersonalAccessToken("token".into()),
                SESSION_USER_ID,
            )
            .await;

        client
            .refresh_session_password(
                &Identifier::numeric(SESSION_USER_ID).expect("numeric identifier"),
                "new",
            )
            .await;
        match client.sign_in_credentials().await {
            Some(Credentials::PersonalAccessToken(token)) => {
                assert_eq!(token.expose_secret(), "token");
            }
            other => panic!("expected the remembered token, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn configured_credentials_outrank_the_ones_a_sign_in_remembered() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "configured".to_string(),
                "iggy".into(),
            )),
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("signed-in".to_string(), "iggy".into()),
                SESSION_USER_ID,
            )
            .await;

        match client.sign_in_credentials().await {
            Some(Credentials::UsernamePassword(username, _)) => assert_eq!(username, "configured"),
            other => panic!("expected the configured credentials, got {other:?}"),
        }
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
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
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
            IggyDuration::from_str(heartbeat_interval).unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert_eq!(
            tcp_client_config.reconnection.max_retries.unwrap(),
            reconnection_retries.parse::<u32>().unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
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
            IggyDuration::from_str("5s").unwrap()
        );

        assert!(tcp_client_config.reconnection.enabled);
        assert!(tcp_client_config.reconnection.max_retries.is_none());
        assert_eq!(
            tcp_client_config.reconnection.interval,
            IggyDuration::from_str("1s").unwrap()
        );
        assert_eq!(
            tcp_client_config.reconnection.reestablish_after,
            IggyDuration::from_str("5s").unwrap()
        );
    }
}
