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
    check_and_redirect_to_leader, is_same_spelling, is_unauthenticated_metadata_probe,
    read_transport_endpoints,
};
use crate::prelude::Client;
use crate::prelude::TcpClientConfig;
use crate::session::ConsensusSession;
use crate::tcp::tcp_connection_stream::TcpConnectionStream;
use crate::tcp::tcp_connection_stream_kind::ConnectionStreamKind;
use crate::tcp::tcp_tls_connection_stream::TcpTlsConnectionStream;
use crate::vsr::replay_after_session_reset_is_safe;
use async_broadcast::{Receiver, Sender, broadcast};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::codes::{
    GET_CLUSTER_METADATA_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
};
#[cfg(test)]
use iggy_common::TcpClientReconnectionConfig;
use iggy_common::VsrSessionControl as _;
use iggy_common::{
    AutoLogin, ClientState, ConnectionString, ConnectionStringUtils, Credentials, DiagnosticEvent,
    IdKind, Identifier, IggyDuration, IggyError, IggyTimestamp, NonZeroIggyDuration,
    TcpConnectionStringOptions, TransportProtocol,
};
use iggy_common::{BinaryClient, BinaryTransport, PersonalAccessTokenClient, UserClient};
use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use secrecy::{ExposeSecret, SecretString};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Bound on the roster read that follows a sign-in the caller ran itself. The
/// read is a convenience for a failover that may never happen, so a cluster
/// that answers it slowly must not hold up the sign-in.
const ROSTER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
    /// Set once a sign-in on this client has gone looking for the roster, so
    /// that read happens once (see [`TcpClient::learn_roster_once`]).
    roster_learned: AtomicBool,
    /// Credentials a sign-in on this client succeeded with, so a reconnect --
    /// onto this node or, after a failover, another one -- can re-establish
    /// the session instead of surfacing `Unauthenticated`. Cleared on logout.
    session_credentials: Mutex<Option<RememberedSignIn>>,
    /// The password a committed change gave the user a configured `AutoLogin`
    /// signs in as. The configured credentials cannot be rewritten, and the
    /// password they carry is dead once the change commits, so every later
    /// sign-in reads this instead.
    configured_password: Mutex<Option<SecretString>>,
    // `std::sync::Mutex` (not `tokio::sync::Mutex`): the critical section
    // is `encode_request_header`, which is pure CPU and never awaits. The
    // tokio variant would pay a waker alloc + internal semaphore on
    // contention with zero correctness benefit.
    consensus_session: Arc<StdMutex<ConsensusSession>>,
    skip_auto_login_once: Mutex<bool>,
    /// Serializes connection movement after a refused request. The stream is
    /// lockstep, but the refusal releases it before the leader check and
    /// reconnect, where another request could otherwise run a competing walk.
    routing_lock: Mutex<()>,
    connect_coordinator: ConnectCoordinator,
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

        if code == GET_CLUSTER_METADATA_CODE {
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
            drop(_routing_guard);
            return self.send_raw(code, payload).await;
        }
        self.disconnect_transport().await?;

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
        reconnect?;

        if !replay_after_reconnect {
            warn!(
                "Reconnected, but command: {code} is replicated and its outcome is unknown: \
                 replaying it under the new session could apply it twice, so the original \
                 error is returned instead."
            );
            return Err(error);
        }

        drop(_routing_guard);
        self.send_raw(code, payload).await
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
        self.learn_roster_once().await;
    }

    async fn forget_session_credentials(&self) {
        self.session_credentials.lock().await.take();
    }

    async fn refresh_session_password(&self, user: &Identifier, new_password: &str) {
        // Two independent copies of a password can go stale here, and a change
        // may target either user: the sign-in this client remembers, and the
        // one a configured `AutoLogin` carries. A caller signed in as somebody
        // else -- by hand, or with a token -- can still be the one who changed
        // the configured user's password.
        let remembered_username = {
            let mut remembered = self.session_credentials.lock().await;
            remembered.as_mut().and_then(|sign_in| {
                // A personal access token is not derived from the password.
                let Credentials::UsernamePassword(username, password) = &mut sign_in.credentials
                else {
                    return None;
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
                Some((username.clone(), targets_session_user))
            })
        };

        let AutoLogin::Enabled(Credentials::UsernamePassword(configured, _)) =
            &self.config.auto_login
        else {
            return;
        };

        // The configured credentials cannot be rewritten -- the config is
        // shared and immutable -- and the password they carry will never work
        // again, so the new one is kept beside them. Kept outside the
        // remembered sign-in on purpose: that record is replaced wholesale by
        // every later login, so a marker on it would survive exactly one
        // reconnect and the one after that would replay the dead password.
        //
        // A numeric identifier can only be recognised as the configured user
        // through the id the signed-in user's own login reported, so a change
        // made from another user's session has to name the user for the
        // configured copy to be refreshed. Naming it is what an administrator
        // doing this from elsewhere does anyway.
        let targets_configured_user = match user.kind {
            IdKind::String => user
                .get_cow_str_value()
                .is_ok_and(|name| name.as_ref() == configured),
            IdKind::Numeric => remembered_username.is_some_and(|(username, is_session_user)| {
                is_session_user && &username == configured
            }),
        };
        if targets_configured_user {
            self.configured_password
                .lock()
                .await
                .replace(SecretString::from(new_password.to_owned()));
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
            roster_learned: AtomicBool::new(false),
            configured_password: Mutex::new(None),
            session_credentials: Mutex::new(None),
            consensus_session: Arc::new(StdMutex::new(ConsensusSession::new())),
            skip_auto_login_once: Mutex::new(false),
            routing_lock: Mutex::new(()),
            connect_coordinator: ConnectCoordinator::new(),
            consumer_group_state: Arc::new(iggy_common::ConsumerGroupClientState::new()),
        })
    }

    async fn connect(&self) -> Result<(), IggyError> {
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
                        self.connect_inner(context).await
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
            // authenticated stream.
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

                info!("{NAME} client is connecting to server: {server_address}...");
                match self.establish_bounded(&server_address, &candidates).await {
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
                        self.stream.lock().await.replace(connection.stream);
                        self.set_state(ClientState::Connected).await;
                        self.connected_at.lock().await.replace(now);
                        self.publish_event(DiagnosticEvent::Connected).await;

                        match self
                            .establish_session(client_address, skip_auto_login, settle_off_leader)
                            .await
                        {
                            Ok(should_redirect) => break should_redirect,
                            Err(failure) if failure.connection_lost => {
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
        self.stream.lock().await.take();
        self.reset_vsr_session().await?;
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
        let Some(credentials) = self.sign_in_credentials().await else {
            info!("No credentials to sign in with.");
            // Only `IggyClient` redirects after a manual sign-in, so a raw
            // transport can stay on a backup: its first replicated write gets
            // `TransientNotAccepted`, the redirect drops the session, and the
            // retry fails `Unauthenticated` until the caller signs in again.
            return Ok(false);
        };

        if skip_auto_login {
            info!("Skipping automatic sign-in for a retried login/register request.");
            return Ok(false);
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
    ///
    /// A password change this client committed for the configured user is
    /// applied on top: the configured password will never work again, and every
    /// later reconnect would otherwise fail `InvalidCredentials`.
    async fn sign_in_credentials(&self) -> Option<Credentials> {
        // The sign-in that last succeeded, whoever ran it: a client is whoever
        // it last signed in as, so a reconnect restores the session the caller
        // last asked for rather than one it had moved off. A configured
        // `AutoLogin` signs in through this very path, so for a client that
        // never signed in by hand the remembered credentials *are* the
        // configured ones.
        if let Some(remembered) = self.session_credentials.lock().await.as_ref() {
            return Some(remembered.credentials.clone());
        }

        match &self.config.auto_login {
            // Before the first sign-in, or after a logout dropped what was
            // remembered. A password change this client committed for the
            // configured user is applied on top: the configured password will
            // never work again, and the config cannot be rewritten.
            AutoLogin::Enabled(Credentials::UsernamePassword(username, configured_password)) => {
                let password = self
                    .configured_password
                    .lock()
                    .await
                    .clone()
                    .unwrap_or_else(|| configured_password.clone());
                Some(Credentials::UsernamePassword(username.clone(), password))
            }
            AutoLogin::Enabled(credentials) => Some(credentials.clone()),
            AutoLogin::Disabled => None,
        }
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
        let Ok(endpoints) = tokio::time::timeout(ROSTER_READ_TIMEOUT, read).await else {
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
        match self.get_state().await {
            ClientState::Disconnected => return Ok(()),
            // A connect is already sweeping, and every caller here is tearing
            // the connection down in order to reconnect -- which is what that
            // sweep is doing. Tearing it down under the sweep would re-mint the
            // client id the sign-in in flight is binding and take the stream it
            // just installed.
            ClientState::Connecting => {
                trace!("Not disconnecting; a connect is already in flight.");
                return Ok(());
            }
            _ => {}
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
            let (_header, result) = self
                .send_raw_vsr_attempt(
                    code,
                    payload.clone(),
                    None,
                    transient_deadline,
                    overall_deadline,
                )
                .await;
            match result {
                Err(IggyError::TransientNotAccepted)
                    if tokio::time::Instant::now() < overall_deadline
                        && !is_login_register_code(code) =>
                {
                    if code == GET_CLUSTER_METADATA_CODE {
                        return Err(IggyError::TransientNotAccepted);
                    }

                    if routing_guard.is_none() {
                        routing_guard = Some(self.routing_lock.lock().await);
                        // A concurrent refused request may have moved the
                        // shared client while this request waited.
                        continue;
                    }

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
    use iggy_binary_protocol::codes::{GET_ME_CODE, LOGOUT_USER_CODE, SEND_MESSAGES_CODE};
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const SESSION_USER_ID: u32 = 7;

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
            client.stream.lock().await.is_none(),
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
            client.sign_in_credentials().await.is_none(),
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
            client.sign_in_credentials().await.is_some(),
            "an involuntary drop is what the failover exists for; the sign-in survives it"
        );
    }

    #[tokio::test]
    async fn a_sign_in_makes_a_client_without_auto_login_reconnectable() {
        let client = client_with("127.0.0.1:8090");
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
        let client = client_with("127.0.0.1:8090");
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
        let client = client_with("127.0.0.1:8090");
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
        let client = client_with("127.0.0.1:8090");
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

    // The configured credentials cannot be rewritten, so a committed password
    // change for the configured user has to reach the next reconnect through
    // the remembered copy, or every later drop replays the password this very
    // client replaced.
    #[tokio::test]
    async fn a_password_change_reaches_a_configured_auto_login() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "iggy".to_string(),
                "old".into(),
            )),
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("iggy".to_string(), "old".into()),
                SESSION_USER_ID,
            )
            .await;

        client
            .refresh_session_password(
                &Identifier::numeric(SESSION_USER_ID).expect("numeric identifier"),
                "new",
            )
            .await;

        // Every later reconnect, not just the next one: each of them signs in
        // and remembers that sign-in afresh, and the configured password is
        // dead for good once the change commits.
        for reconnect in 0..3 {
            match client.sign_in_credentials().await {
                Some(Credentials::UsernamePassword(username, password)) => {
                    assert_eq!(username, "iggy");
                    assert_eq!(
                        password.expose_secret(),
                        "new",
                        "the configured password came back on reconnect {reconnect}"
                    );
                    // What the reconnect's own login does with what it signed
                    // in with.
                    client
                        .remember_session_credentials(
                            Credentials::UsernamePassword(username, password),
                            SESSION_USER_ID,
                        )
                        .await;
                }
                other => {
                    panic!("expected the configured user with the new password, got {other:?}")
                }
            }
        }
    }

    // A change for somebody else says nothing about the configured user's
    // password, and a sign-in as another user does not get to replace it.
    #[tokio::test]
    async fn a_password_change_for_another_user_leaves_a_configured_auto_login_alone() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "configured".to_string(),
                "old".into(),
            )),
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        client
            .remember_session_credentials(
                Credentials::UsernamePassword("signed-in".to_string(), "old".into()),
                SESSION_USER_ID,
            )
            .await;

        client
            .refresh_session_password(
                &Identifier::numeric(SESSION_USER_ID).expect("numeric identifier"),
                "new",
            )
            .await;

        // A sign-out drops what was remembered, so what the configured
        // credentials carry is what the next connect signs in with -- and this
        // change was somebody else's.
        client.forget_session_credentials().await;
        match client.sign_in_credentials().await {
            Some(Credentials::UsernamePassword(username, password)) => {
                assert_eq!(username, "configured");
                assert_eq!(password.expose_secret(), "old");
            }
            other => panic!("expected the configured credentials, got {other:?}"),
        }
    }

    // A change made from a session signed in as somebody else still kills the
    // configured password, so the next connect must not replay it. Named rather
    // than numbered, since only the signed-in user's own id is known here.
    #[tokio::test]
    async fn a_password_change_naming_the_configured_user_reaches_it_from_another_session() {
        let client = TcpClient::create(Arc::new(TcpClientConfig {
            auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
                "configured".to_string(),
                "old".into(),
            )),
            ..TcpClientConfig::default()
        }))
        .expect("create the client");
        client
            .remember_session_credentials(
                Credentials::PersonalAccessToken("token".into()),
                SESSION_USER_ID,
            )
            .await;

        client
            .refresh_session_password(
                &Identifier::named("configured").expect("named identifier"),
                "new",
            )
            .await;

        client.forget_session_credentials().await;
        match client.sign_in_credentials().await {
            Some(Credentials::UsernamePassword(username, password)) => {
                assert_eq!(username, "configured");
                assert_eq!(password.expose_secret(), "new");
            }
            other => panic!("expected the configured user with the new password, got {other:?}"),
        }
    }

    // A client is whoever it last signed in as: the connection re-authenticates
    // from the login it captured, and a redial that replayed somebody else
    // would make the outcome depend on which of the two got there first.
    #[tokio::test]
    async fn the_last_sign_in_outranks_the_configured_credentials() {
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
            Some(Credentials::UsernamePassword(username, _)) => assert_eq!(username, "signed-in"),
            other => panic!("expected the sign-in that last succeeded, got {other:?}"),
        }

        // A sign-out leaves no session to restore, and the configured
        // credentials are what every connect of this client signs in as.
        client.forget_session_credentials().await;
        match client.sign_in_credentials().await {
            Some(Credentials::UsernamePassword(username, _)) => assert_eq!(username, "configured"),
            other => panic!("expected the configured credentials, got {other:?}"),
        }
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
}
