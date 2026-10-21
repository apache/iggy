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

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::Decodable;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, SemaphorePermit, broadcast};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

use crate::auth::{
    AuthError, AuthenticatedPrincipal, FailedLoginThrottle, PrincipalResolver, SaslAuthenticator,
};
use crate::bridge::IggyBridge;
use crate::cluster_id::ClusterId;
use crate::error::{KafkaProtocolError, Result};
use crate::group::{GroupCoordinator, GroupCoordinatorConfig};
use crate::listener::{ListenerConfig, SecurityProtocol};
use crate::protocol::api::{
    API_KEY_DESCRIBE_ACLS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, BrokerAdvertise,
    ConnectionState, DEFAULT_KAFKA_PORT, ERROR_ILLEGAL_SASL_STATE, ERROR_INVALID_REQUEST,
    ERROR_NONE, ERROR_SASL_AUTHENTICATION_FAILED, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNSUPPORTED_SASL_MECHANISM, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS, decode_acl_filter, decode_sasl_auth_bytes,
    decode_sasl_mechanism, describe_acls_outcome, encode_error_for_key, handle_connection_request,
    respond_describe_acls_error, sasl_authenticate_outcome, sasl_handshake_outcome,
};
use crate::protocol::auto_topic_creation::{self, AutoTopicCreationConfig};
use crate::protocol::handlers::api_versions::Advertisement;
use crate::protocol::header::{request_header_version, response_header_version};
use crate::protocol::sasl::{
    SASL_AUTHENTICATE_MAX_VERSION, SASL_HANDSHAKE_VERSION, SaslAction, SaslState, parse_plain,
};
use crate::tls::{IdentityError, TlsConfig, TlsState};

const READ_CHUNK: usize = 65536;
const GATEWAY_LOG_TARGET: &str = env!("CARGO_CRATE_NAME");

/// Builds the log filter, forcing the Iggy SDK quiet unless the operator asked otherwise.
///
/// This is a credential-disclosure control, not noise reduction. The SDK logs the username it
/// signed in with at INFO on every successful login, and this gateway drives that path once per
/// authentication with a *Kafka client's* username, so at INFO every principal that connects ends
/// up in the gateway's log. Because the line fires only on success, what leaks is precisely the
/// set of valid accounts.
///
/// Appending the directive rather than only supplying a default is the point. Reading `RUST_LOG`
/// and using it verbatim silently drops this the moment anyone sets it, including on the run
/// commands this repository's own documentation gives. An explicit `iggy=` directive still wins,
/// so raising it deliberately for debugging remains possible.
///
/// `EnvFilter` matches targets by prefix, so `iggy=warn` also catches this crate. The gateway gets
/// its own directive at the operator's default level, since the longer target wins, and without it
/// every authentication decision this gateway logs would be filtered out.
fn sdk_quieted_filter(rust_log: Option<&str>) -> String {
    let base = rust_log.unwrap_or("info");
    let base = if base.trim().is_empty() { "info" } else { base };
    let directives: Vec<&str> = base.split(',').map(str::trim).collect();
    if directives
        .iter()
        .any(|directive| directive.starts_with("iggy="))
    {
        return base.to_string();
    }
    let names_gateway = directives
        .iter()
        .any(|directive| directive.split(['=', '[']).next() == Some(GATEWAY_LOG_TARGET));
    if names_gateway {
        return format!("{base},iggy=warn");
    }
    let default_level = directives
        .iter()
        .rev()
        .copied()
        .find(|directive| directive.parse::<LevelFilter>().is_ok())
        .unwrap_or("info");
    format!("{base},iggy=warn,{GATEWAY_LOG_TARGET}={default_level}")
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// The listeners, one per security protocol (`IGGY_KAFKA_LISTENERS`, or the single-listener
    /// `IGGY_KAFKA_BIND_ADDR` form). Which of them run SASL and which run TLS is what the
    /// protocol says; `tls` holds the material the TLS ones share.
    pub listeners: Vec<ListenerConfig>,
    /// Certificate, key and client-authentication policy for every `SSL` and `SASL_SSL` listener.
    /// Required exactly when one of those exists.
    pub tls: Option<TlsConfig>,
    pub max_frame_size: usize,
    /// Maximum concurrent connections each listener accepts before rejecting new ones. Every
    /// listener has its own allowance, so clients of one cannot use up another's.
    pub max_connections: usize,
    /// Bound on how long an accepted connection may sit idle before sending the next
    /// frame's length prefix. Kafka brokers default `connections.max.idle.ms` to 10 minutes;
    /// match that so well-behaved idle clients aren't dropped.
    pub idle_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    /// Cap on how long graceful shutdown waits for in-flight connections to finish. Without
    /// this, a connection idling inside `idle_timeout` (10 minutes by default) would otherwise
    /// hold shutdown open past typical orchestrator grace periods (e.g. Kubernetes' default
    /// 30s `terminationGracePeriodSeconds`).
    pub shutdown_drain_timeout: Duration,
    /// Consumer-group timeouts and capacity caps. No environment variable maps onto these yet;
    /// Kafka's own defaults apply.
    pub group: GroupCoordinatorConfig,
    /// This gateway's number among the gateways fronting one Iggy cluster
    /// (`IGGY_KAFKA_INSTANCE_ID`). It is the high half of every producer id handed out by
    /// `InitProducerId`, which Kafka requires to be cluster-unique; two gateways left on the
    /// same number hand out the same ids.
    pub instance_id: u16,
    /// How long an unauthenticated connection may sit between frames, and how long a TLS
    /// handshake or a certificate identity lookup may take.
    ///
    /// Separate from `idle_timeout` because that one is sized for a well-behaved idle client (10
    /// minutes, matching a real broker) and applies to connections that have proven who they are.
    /// A connection that has proven nothing still holds a `max_connections` permit, so it gets a
    /// budget measured in seconds instead.
    pub pre_auth_timeout: Duration,
    /// Credential verifications the gateway runs at once, across every connection.
    ///
    /// Each one costs an Argon2id verify on an Iggy shard thread that has no blocking pool, so
    /// unauthenticated traffic can otherwise pile that work onto the server with nothing but
    /// shape-valid tokens. `max_connections` alone is not a bound on that: it caps sockets, not
    /// the work each one can ask Iggy to do. Verifications beyond this queue rather than fail,
    /// since a rejected login is indistinguishable from a wrong password to the client.
    ///
    /// This bounds the gateway's side only. A verification that times out frees its slot, but the
    /// login it started keeps hashing inside Iggy, so a slow server can briefly carry more than
    /// this many.
    ///
    /// Four by default, which stays under the shard count of any node with 16 or fewer physical
    /// cores. A default above that is no bound at all on the deployments most likely to run one
    /// gateway in front of one node; a larger node raises it deliberately.
    ///
    /// Client certificate user lookups get the same number of slots again, separate from these.
    pub max_concurrent_authentications: usize,
    /// Whether a `Metadata` lookup of a missing topic creates it, and with how many partitions
    /// (`IGGY_KAFKA_AUTO_CREATE_TOPICS_*`). Off by default.
    pub auto_topic_creation: AutoTopicCreationConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            listeners: vec![ListenerConfig::new(
                SecurityProtocol::Plaintext,
                format!("127.0.0.1:{DEFAULT_KAFKA_PORT}"),
            )],
            tls: None,
            max_frame_size: 8 * 1024 * 1024,
            max_connections: 1024,
            idle_timeout: Duration::from_mins(10),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
            shutdown_drain_timeout: Duration::from_secs(25),
            group: GroupCoordinatorConfig::default(),
            instance_id: 0,
            pre_auth_timeout: Duration::from_secs(15),
            max_concurrent_authentications: 4,
            auto_topic_creation: AutoTopicCreationConfig::default(),
        }
    }
}

impl GatewayConfig {
    /// Whether any listener runs the SASL exchange.
    ///
    /// Switching a listener to SASL is a breaking change for every client already talking to it:
    /// the two SASL keys only appear in that listener's `ApiVersions` advertisement while it is
    /// on, and unauthenticated clients stop being served there.
    ///
    /// On a listener without SASL the keys are still answered, with `ILLEGAL_SASL_STATE` and an
    /// empty mechanism list, because a well-formed response exists for them unlike a genuinely
    /// unknown key. They are kept out of the advertisement so nothing is invited into an exchange
    /// that cannot finish.
    #[must_use]
    pub fn sasl_enabled(&self) -> bool {
        self.listeners
            .iter()
            .any(|listener| listener.protocol.uses_sasl())
    }

    /// Whether any listener speaks TLS.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.listeners
            .iter()
            .any(|listener| listener.protocol.uses_tls())
    }

    /// Whether some listener turns client certificates into Iggy identities: an `SSL` listener
    /// with client authentication on.
    #[must_use]
    pub fn maps_certificate_identities(&self) -> bool {
        self.tls
            .as_ref()
            .is_some_and(|tls| tls.client_auth.is_enabled())
            && self
                .listeners
                .iter()
                .any(|listener| listener.protocol.authenticates_by_certificate())
    }
}

/// Binds the gateway's TCP listener with a deep accept backlog.
///
/// `tokio::net::TcpListener::bind` delegates to mio, which hardcodes `listen(fd, 128)`
/// (`mio-*/src/net/tcp/listener.rs`) on every non-Horizon/Haiku target, independent of how high
/// `max_connections` is configured. A burst larger than 128 simultaneous connects - a
/// consumer-group rebalance, a deployment rollout, a load-balancer failover - overflows the
/// kernel's accept queue; with the Linux default `net.ipv4.tcp_abort_on_overflow=0` the SYNs are
/// silently dropped and clients retry on exponential SYN backoff (1s, 3s, 7s), which reads as a
/// network fault rather than a server limit. Bind through `socket2` instead and request
/// `SOMAXCONN` (4096 on Linux >= 5.4), matching `core/message_bus/src/socket_opts.rs`'s
/// precedent for the identical problem.
///
/// Deliberately synchronous and callable from both `main` and the test harness
/// (`tests/common/server.rs`) so the tuned bind path is what integration tests actually exercise,
/// not a bare `TcpListener::bind`.
///
/// # Errors
///
/// Returns an error if `addr` does not parse as `host:port`, or if bind/listen/conversion to a
/// Tokio listener fails.
pub fn bind_listener(addr: &str) -> Result<TcpListener> {
    let socket_addr: SocketAddr = addr.parse().map_err(|e| {
        KafkaProtocolError::InvalidConfig(format!("invalid bind address '{addr}': {e}"))
    })?;
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(socket_addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.bind(&socket_addr.into())?;
    socket.listen(libc::SOMAXCONN)?;
    socket.set_nonblocking(true)?;
    let std_listener: std::net::TcpListener = socket.into();
    Ok(TcpListener::from_std(std_listener)?)
}

/// A listener bound by the caller, paired with the configuration it was bound from.
pub struct BoundListener {
    pub config: ListenerConfig,
    pub socket: TcpListener,
}

impl BoundListener {
    #[must_use]
    pub const fn new(config: ListenerConfig, socket: TcpListener) -> Self {
        Self { config, socket }
    }

    /// The address the socket is actually bound to.
    ///
    /// # Errors
    ///
    /// Returns the socket's error when the address cannot be read.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

/// Binds every listener in `config`, through [`bind_listener`].
///
/// # Errors
///
/// Returns the first bind failure, naming the listener's protocol and address.
pub fn bind_listeners(config: &GatewayConfig) -> Result<Vec<BoundListener>> {
    config
        .listeners
        .iter()
        .map(|listener| {
            let socket = bind_listener(&listener.bind_addr).map_err(|error| {
                KafkaProtocolError::InvalidConfig(format!(
                    "failed to bind the {} listener on {}: {error}",
                    listener.protocol, listener.bind_addr
                ))
            })?;
            Ok(BoundListener::new(listener.clone(), socket))
        })
        .collect()
}

pub struct KafkaGateway {
    config: Arc<GatewayConfig>,
    authenticator: Option<Arc<dyn SaslAuthenticator>>,
    bridge: Option<Arc<IggyBridge>>,
    principal_resolver: Option<Arc<dyn PrincipalResolver>>,
    cluster_id: Option<ClusterId>,
}

/// Owned by [`KafkaGateway::run`] and shared by every connection it serves.
struct SharedAuth {
    authenticator: Option<Arc<dyn SaslAuthenticator>>,
    /// Looks up the Iggy user a client certificate names. The bridge, unless a test swaps one in.
    resolver: Option<Arc<dyn PrincipalResolver>>,
    slots: Semaphore,
    /// Certificate user lookups, kept apart from `slots` so a burst of either kind cannot starve
    /// the other of its budget.
    lookup_slots: Semaphore,
    failed_logins: FailedLoginThrottle,
}

impl SharedAuth {
    fn new(
        authenticator: Option<Arc<dyn SaslAuthenticator>>,
        resolver: Option<Arc<dyn PrincipalResolver>>,
        max_concurrent: usize,
    ) -> Self {
        Self {
            authenticator,
            resolver,
            slots: Semaphore::new(max_concurrent),
            lookup_slots: Semaphore::new(max_concurrent),
            failed_logins: FailedLoginThrottle::default(),
        }
    }
}

/// One listener as its accept loop and its connections see it.
struct Listener {
    protocol: SecurityProtocol,
    broker: BrokerAdvertise,
    /// Whether this listener names a principal, by SASL or by a verified client certificate.
    /// Decides whether `DescribeAcls` is advertised and answered.
    acls_enabled: bool,
    /// Whether a connection here that names no principal is served as
    /// [`AuthenticatedPrincipal::anonymous`]. True on every listener without SASL once any
    /// listener of the gateway names principals, so the permission gates hold there too.
    anonymous_without_principal: bool,
}

impl Listener {
    fn connection_state(&self) -> ConnectionState {
        ConnectionState::for_listener(
            self.broker.clone(),
            self.protocol.uses_sasl(),
            self.acls_enabled,
        )
    }

    const fn advertisement(&self) -> Advertisement {
        Advertisement {
            sasl: self.protocol.uses_sasl(),
            acls: self.acls_enabled,
        }
    }
}

impl KafkaGateway {
    #[must_use]
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config: Arc::new(config),
            authenticator: None,
            bridge: None,
            principal_resolver: None,
            cluster_id: None,
        }
    }

    /// Serve requests against `bridge` instead of the stub answers.
    ///
    /// `None` is the default and keeps every handler on its stub. The bridge also resolves the
    /// Iggy user a client certificate names, unless [`Self::with_principal_resolver`] supplies
    /// something else.
    #[must_use]
    pub fn with_bridge(mut self, bridge: Option<Arc<IggyBridge>>) -> Self {
        self.bridge = bridge;
        self
    }

    /// Reports `cluster_id`, already loaded from Iggy, instead of loading it in [`Self::run`].
    ///
    /// `main` loads it before it binds the listeners, so a client is never queued behind the
    /// calls to Iggy. Without a bridge or this, the id is random and lasts as long as the
    /// process.
    #[must_use]
    pub fn with_cluster_id(mut self, cluster_id: ClusterId) -> Self {
        self.cluster_id = Some(cluster_id);
        self
    }

    /// Supplies the verifier that SASL credentials are checked against.
    ///
    /// Required when a listener runs SASL: a gateway that demands authentication with nothing to
    /// authenticate against would refuse every client, so [`Self::run`] rejects that combination
    /// at startup rather than at the first login attempt.
    #[must_use]
    pub fn with_authenticator(mut self, authenticator: Arc<dyn SaslAuthenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// Supplies the lookup that turns a client certificate's mapped name into an Iggy user.
    ///
    /// The bridge does this by default. A test drives the certificate path without an Iggy
    /// server by supplying its own.
    #[must_use]
    pub fn with_principal_resolver(mut self, resolver: Arc<dyn PrincipalResolver>) -> Self {
        self.principal_resolver = Some(resolver);
        self
    }

    /// Accept Kafka wire connections on every listener until `shutdown` fires, then drain
    /// in-flight tasks.
    ///
    /// The listeners must already be bound by the caller. This lets tests and `main` bind the
    /// ports before spawning the task, eliminating the TOCTOU race of bind-drop-rebind.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid config, unusable TLS material, or a non-transient `accept()`
    /// error on any listener. With a bridge and no [`Self::with_cluster_id`], also returns the
    /// bridge's error when it cannot load the cluster id from Iggy.
    pub async fn run(
        self,
        listeners: Vec<BoundListener>,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<()> {
        self.check_sasl_wiring(&listeners)?;
        let tls = self.check_tls_wiring(&listeners)?;
        if listeners.is_empty() {
            return Err(KafkaProtocolError::InvalidConfig(
                "no listener is configured".into(),
            ));
        }
        let resolver = self.principal_resolver.clone().or_else(|| {
            self.bridge
                .clone()
                .map(|bridge| bridge as Arc<dyn PrincipalResolver>)
        });
        let acls_by_certificate = tls
            .as_ref()
            .is_some_and(|tls| tls.config().client_auth.is_enabled());
        let names_principals = |protocol: SecurityProtocol| {
            protocol.uses_sasl() || (protocol.authenticates_by_certificate() && acls_by_certificate)
        };
        let any_names_principals = listeners
            .iter()
            .any(|listener| names_principals(listener.config.protocol));

        let mut bound = Vec::with_capacity(listeners.len());
        for BoundListener { config, socket } in listeners {
            let local_addr = socket.local_addr()?;
            let broker = BrokerAdvertise::for_listener(&config, local_addr)?;
            // instance_id is logged because it is the only way to tell from a running process
            // which half of the producer-id space this gateway owns. Two gateways left on the
            // default collide silently, and a config file cannot be diffed against a live
            // deployment.
            info!(
                "kafka {} listener bound on {} (advertised as {}:{}, instance id {})",
                config.protocol, local_addr, broker.host, broker.port, self.config.instance_id
            );
            let protocol = config.protocol;
            bound.push((
                Arc::new(Listener {
                    protocol,
                    broker,
                    acls_enabled: names_principals(protocol),
                    anonymous_without_principal: any_names_principals && !protocol.uses_sasl(),
                }),
                socket,
            ));
        }

        // Cancelled on shutdown so connection tasks exit instead of sitting in idle waits until
        // `idle_timeout` (or forever if that is raised). Created before the state so the group
        // coordinator can take a child token: a parked JoinGroup or SyncGroup waiter would
        // otherwise hold the drain open for a full rebalance timeout.
        let cancel = CancellationToken::new();
        let cluster_id = self.cluster_id().await?;
        let state = Arc::new(self.shared_state(cluster_id, &cancel)?);
        let shared_auth = Arc::new(SharedAuth::new(
            self.authenticator.clone(),
            resolver,
            self.config.max_concurrent_authentications,
        ));
        let tracker = TaskTracker::new();
        let drain_timeout = self.config.shutdown_drain_timeout;

        if let Some(tls) = &tls {
            // Not tracked: it ends on the cancel token, which the drain fires first.
            tokio::spawn(Arc::clone(tls).reload_until_cancelled(cancel.child_token()));
        }

        let mut accept_loops = JoinSet::new();
        for (listener, socket) in bound {
            accept_loops.spawn(accept_loop(AcceptLoop {
                socket,
                listener,
                config: Arc::clone(&self.config),
                state: Arc::clone(&state),
                auth: Arc::clone(&shared_auth),
                tracker: tracker.clone(),
                conn_limiter: Arc::new(Semaphore::new(self.config.max_connections)),
                cancel: cancel.clone(),
                tls: tls.clone(),
            }));
        }

        let outcome = tokio::select! {
            result = shutdown.recv() => {
                match result {
                    Ok(()) => info!("kafka listener shutdown requested"),
                    // Capacity-1 channel: lagged means a signal was sent before we polled -
                    // treat as shutdown.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        info!("kafka listener shutdown requested (lagged)");
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
                Ok(())
            }
            // An accept loop ends early only on a non-transient accept error, since shutdown is
            // what the other branch is for.
            Some(finished) = accept_loops.join_next() => match finished {
                Ok(result) => result,
                Err(join_error) => Err(io::Error::other(join_error).into()),
            },
        };
        // The accept loops stop first, so no connection is spawned after the drain starts waiting.
        cancel.cancel();
        accept_loops.shutdown().await;
        drain(&tracker, &cancel, drain_timeout).await;
        // After the drain, so no connection starts one after it, and before `run` returns, so
        // none still runs when the bridge signs out.
        state
            .shutdown_auto_topic_creation(auto_topic_creation::SHUTDOWN_WAIT)
            .await;
        outcome
    }

    /// The state every connection of [`Self::run`] shares. The group coordinator stops with
    /// `cancel`.
    fn shared_state(
        &self,
        cluster_id: ClusterId,
        cancel: &CancellationToken,
    ) -> Result<GatewayState> {
        GatewayState::new(
            self.bridge.clone(),
            self.config.max_frame_size,
            self.config.instance_id,
            GroupCoordinator::new(self.config.group.clone(), cancel.child_token()),
        )
        .with_cluster_id(cluster_id)
        .with_auto_topic_creation(self.config.auto_topic_creation)
    }

    /// The id set by [`Self::with_cluster_id`], else the one stored in Iggy, else a random one.
    async fn cluster_id(&self) -> Result<ClusterId> {
        if let Some(cluster_id) = &self.cluster_id {
            return Ok(cluster_id.clone());
        }
        let Some(bridge) = &self.bridge else {
            let cluster_id = ClusterId::random();
            info!("no Iggy bridge, so cluster id {cluster_id} lasts only as long as this process");
            return Ok(cluster_id);
        };
        Ok(bridge.load_cluster_id().await?)
    }

    /// Refuses to start when the SASL listeners and the configured authenticator disagree.
    fn check_sasl_wiring(&self, listeners: &[BoundListener]) -> Result<()> {
        let sasl_enabled = listeners
            .iter()
            .any(|listener| listener.config.protocol.uses_sasl());
        if !sasl_enabled && self.authenticator.is_some() {
            // The mirror of the guard below, and the quieter mistake: a verifier attached while no
            // listener runs SASL means every connection is served unauthenticated, with nothing
            // in the log to say so. Refusing to start is the only way that failure is visible.
            return Err(KafkaProtocolError::InvalidConfig(
                "an authenticator is configured but no listener runs SASL; every connection \
                 would be served unauthenticated. Add a SASL_PLAINTEXT or SASL_SSL listener (or \
                 set IGGY_KAFKA_SASL_ENABLED=true), or remove the authenticator"
                    .into(),
            ));
        }
        if sasl_enabled && self.authenticator.is_none() {
            return Err(KafkaProtocolError::InvalidConfig(
                "a listener runs SASL but no authenticator is configured; every client on it \
                 would be rejected. Set IGGY_KAFKA_IGGY_ADDR to the Iggy server that credentials \
                 are verified against, or drop the SASL listener"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Loads the TLS material when a listener needs it, and refuses the combinations in which
    /// something configured would silently go unused.
    fn check_tls_wiring(&self, listeners: &[BoundListener]) -> Result<Option<Arc<TlsState>>> {
        let tls_listener = listeners
            .iter()
            .find(|listener| listener.config.protocol.uses_tls());
        let Some(tls_config) = self.config.tls.clone() else {
            if let Some(listener) = tls_listener {
                return Err(KafkaProtocolError::InvalidConfig(format!(
                    "the {} listener needs a certificate; set IGGY_KAFKA_TLS_CERT_FILE and \
                     IGGY_KAFKA_TLS_KEY_FILE",
                    listener.config.protocol
                )));
            }
            return Ok(None);
        };
        if tls_listener.is_none() {
            // The same mistake the SASL guard catches: material configured and nothing serving
            // it reads as an encrypted listener to whoever set it.
            return Err(KafkaProtocolError::InvalidConfig(
                "TLS is configured but no listener uses it; add an SSL or SASL_SSL listener to \
                 IGGY_KAFKA_LISTENERS, or remove the IGGY_KAFKA_TLS_* settings"
                    .into(),
            ));
        }
        let has_ssl_listener = listeners
            .iter()
            .any(|listener| listener.config.protocol.authenticates_by_certificate());
        if tls_config.client_auth.is_enabled() && !has_ssl_listener {
            return Err(KafkaProtocolError::InvalidConfig(format!(
                "client authentication is '{}' but no SSL listener is configured; it applies to \
                 SSL listeners only, and a SASL_SSL listener asks for no client certificate",
                tls_config.client_auth
            )));
        }
        if tls_config.client_auth.is_enabled()
            && self.principal_resolver.is_none()
            && self.bridge.is_none()
        {
            // A certificate names an Iggy user, and only an Iggy session can say whether that
            // user exists. Admitting the name unchecked would let any certificate from the CA
            // claim any account.
            return Err(KafkaProtocolError::InvalidConfig(
                "the SSL listener maps client certificates to Iggy users, which needs the bridge \
                 to look them up; set IGGY_KAFKA_BRIDGE_ENABLED=true, or set \
                 IGGY_KAFKA_TLS_CLIENT_AUTH=none"
                    .into(),
            ));
        }
        TlsState::load(tls_config).map(|state| Some(Arc::new(state)))
    }
}

/// Everything one listener's accept loop needs, bundled so the loop is one spawnable future.
struct AcceptLoop {
    socket: TcpListener,
    listener: Arc<Listener>,
    config: Arc<GatewayConfig>,
    state: Arc<GatewayState>,
    auth: Arc<SharedAuth>,
    tracker: TaskTracker,
    conn_limiter: Arc<Semaphore>,
    cancel: CancellationToken,
    tls: Option<Arc<TlsState>>,
}

/// Accepts on one listener until the cancel token fires or `accept()` fails for good.
async fn accept_loop(this: AcceptLoop) -> Result<()> {
    loop {
        let accepted = tokio::select! {
            () = this.cancel.cancelled() => return Ok(()),
            accepted = this.socket.accept() => accepted,
        };
        if this.cancel.is_cancelled() {
            return Ok(());
        }
        match accepted {
            Ok((stream, peer)) => {
                let Ok(permit) = Arc::clone(&this.conn_limiter).try_acquire_owned() else {
                    warn!(%peer, protocol = %this.listener.protocol, max_connections = this.config.max_connections, "listener connection limit reached, rejecting");
                    continue;
                };
                if let Err(e) = stream.set_nodelay(true) {
                    warn!(%peer, "TCP_NODELAY failed: {e}");
                }
                if let Err(e) = enable_tcp_keepalive(&stream) {
                    warn!(%peer, "TCP_KEEPALIVE failed: {e}");
                }
                let connection = Connection {
                    peer,
                    listener: Arc::clone(&this.listener),
                    config: Arc::clone(&this.config),
                    state: Arc::clone(&this.state),
                    auth: Arc::clone(&this.auth),
                    cancel: this.cancel.child_token(),
                    tls: this.tls.clone(),
                };
                this.tracker.spawn(async move {
                    let _permit = permit;
                    serve_connection(stream, connection).await;
                });
            }
            Err(e) if is_transient_accept_error(&e) => {
                // Brief backoff on fd exhaustion to avoid busy-spinning.
                if matches!(e.raw_os_error(), Some(23 | 24)) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                warn!(%e, "transient accept error, continuing");
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// What an accepted socket carries into its own task.
struct Connection {
    peer: SocketAddr,
    listener: Arc<Listener>,
    config: Arc<GatewayConfig>,
    state: Arc<GatewayState>,
    auth: Arc<SharedAuth>,
    cancel: CancellationToken,
    tls: Option<Arc<TlsState>>,
}

/// Runs the TLS handshake when the listener calls for one, then the frame loop.
async fn serve_connection(stream: TcpStream, connection: Connection) {
    let peer = connection.peer;
    let result = if connection.listener.protocol.uses_tls() {
        let Some(tls) = connection.tls.clone() else {
            // `check_tls_wiring` refuses to start a TLS listener without material.
            error!(%peer, "TLS listener accepted a connection with no TLS material loaded");
            return;
        };
        // Bounded like everything else a connection does before proving who it is: an
        // unfinished handshake holds a `max_connections` permit.
        let handshake = timeout(
            connection.config.pre_auth_timeout,
            tls.acceptor(connection.listener.protocol).accept(stream),
        );
        let handshake = tokio::select! {
            () = connection.cancel.cancelled() => {
                debug!(%peer, "TLS handshake abandoned by shutdown");
                return;
            }
            handshake = handshake => handshake,
        };
        let tls_stream = match handshake {
            Ok(Ok(tls_stream)) => tls_stream,
            Ok(Err(error)) => {
                // debug!, not warn!: a plaintext client on the TLS port, an untrusted client
                // certificate or a scanner all end here, none of them operator-actionable.
                debug!(%peer, %error, "TLS handshake failed");
                return;
            }
            Err(_elapsed) => {
                debug!(%peer, "TLS handshake did not finish within the pre-authentication budget");
                return;
            }
        };
        let certificate_name = if connection.listener.protocol.authenticates_by_certificate() {
            match tls.principal_name(tls_stream.get_ref().1) {
                Ok(name) => name,
                // warn!: the CA signed this certificate, so a subject no rule maps is a mapping
                // rule the operator has to fix.
                Err(IdentityError::NoMatchingRule { subject }) => {
                    warn!(%peer, subject, "client certificate subject matches no principal mapping rule; closing");
                    return;
                }
                Err(error) => {
                    debug!(%peer, %error, "client certificate names no principal; closing");
                    return;
                }
            }
        } else {
            None
        };
        handle_connection(tls_stream, connection, certificate_name).await
    } else {
        handle_connection(stream, connection, None).await
    };
    if let Err(err) = result {
        // debug!, not warn!: every `KafkaProtocolError` that can reach here is either a
        // malformed/oversized frame from the client (attacker-controlled, not
        // operator-actionable) or a plain TCP reset/EOF - never an internal gateway fault. A
        // flood of either at warn! would drown real signal in logs under adversarial load (same
        // reasoning as the decode-failure downgrades in `protocol/api.rs`).
        debug!(%peer, "connection closed with error: {err}");
    }
}

/// Cancel in-flight connections, close the tracker to new spawns, and wait for tasks to finish
/// (bounded by `deadline`). Cancellation is what actually drops idle sockets; `tracker.wait`
/// alone would leave tasks parked in `read_frame` until `idle_timeout`.
async fn drain(tracker: &TaskTracker, cancel: &CancellationToken, deadline: Duration) {
    cancel.cancel();
    tracker.close();
    if timeout(deadline, tracker.wait()).await.is_err() {
        warn!(
            ?deadline,
            "shutdown drain deadline exceeded; remaining connection tasks will be dropped with the runtime"
        );
    }
}

fn is_transient_accept_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted | io::ErrorKind::WouldBlock
    ) || matches!(
        err.raw_os_error(),
        // EMFILE / ENFILE are common across Unix platforms when fd limits are hit.
        Some(23 | 24)
    )
}

fn enable_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let sock = socket2::SockRef::from(stream);
    sock.set_keepalive(true)?;
    Ok(())
}

/// Per-connection borrows that every frame's routing needs and none of it changes.
///
/// Bundled rather than passed one by one: the routing decision already takes the mutable SASL
/// state, the header and the body, and four more parameters would put it over the argument limit
/// for no gain in clarity.
struct ConnectionContext<'a> {
    config: &'a GatewayConfig,
    listener: &'a Listener,
    state: &'a GatewayState,
    connection: &'a ConnectionState,
    authenticator: Option<&'a dyn SaslAuthenticator>,
    resolver: Option<&'a dyn PrincipalResolver>,
    auth_slots: &'a Semaphore,
    lookup_slots: &'a Semaphore,
    failed_logins: &'a FailedLoginThrottle,
    peer: &'a SocketAddr,
    cancel: &'a CancellationToken,
}

/// Decides what one decoded frame earns, advancing `sasl_state` when it authenticates.
///
/// Split out of [`handle_connection`] so the loop stays about framing and this stays about the
/// SASL state machine.
async fn route_frame(
    ctx: &ConnectionContext<'_>,
    sasl_state: &mut SaslState,
    principal: &mut Option<AuthenticatedPrincipal>,
    req: &RequestHeader,
    body: Bytes,
) -> HandleOutcome {
    let peer = ctx.peer;

    // The mechanism name lives in the handshake body, and the state machine needs it to decide.
    // Decoding it here keeps `classify` free of wire concerns; a body that will not decode yields
    // `None`, which `classify` already treats as an unsupported mechanism.
    let mechanism = if req.request_api_key == API_KEY_SASL_HANDSHAKE {
        decode_sasl_mechanism(req.request_api_version, body.clone()).ok()
    } else {
        None
    };

    let action = sasl_state.classify(
        req.request_api_key,
        req.request_api_version,
        mechanism.as_deref(),
    );
    match action {
        // Answered here rather than in the stateless dispatch, because it describes the principal
        // this connection authenticated as, which only the connection knows. Gated on the
        // listener naming principals as well as on the key: elsewhere there is no principal, and
        // the key is not advertised, so it falls through to ordinary dispatch and is refused as
        // the unlisted key it is.
        SaslAction::Dispatch
            if ctx.listener.acls_enabled && req.request_api_key == API_KEY_DESCRIBE_ACLS =>
        {
            describe_acls(ctx, principal.as_ref(), req.request_api_version, body)
        }
        SaslAction::Dispatch => {
            handle_connection_request(
                ctx.state,
                ctx.connection,
                principal.as_ref(),
                req.request_api_key,
                req.request_api_version,
                body,
            )
            .await
        }
        SaslAction::DispatchPreAuthApiVersions => {
            // Counted whatever the answer says. A refusal the client is entitled to retry at a
            // lower version, which is what the KIP-511 downgrade path does, is covered by the
            // allowance rather than exempt from it: spending it only on a usable answer lets a
            // peer repeat a rejected version forever, resetting the pre-authentication read budget
            // on every frame and holding a `max_connections` permit with it.
            sasl_state.count_api_versions_answer();
            // Pre-authentication by construction: no principal exists yet at this point.
            handle_connection_request(
                ctx.state,
                ctx.connection,
                None,
                req.request_api_key,
                req.request_api_version,
                body,
            )
            .await
        }
        SaslAction::AcceptHandshake(mechanism) => {
            debug!(%peer, %mechanism, "SASL mechanism negotiated");
            *sasl_state = SaslState::AwaitToken(mechanism);
            sasl_handshake_outcome(req.request_api_version, ERROR_NONE, false)
        }
        SaslAction::RejectMechanism => sasl_handshake_outcome(
            req.request_api_version,
            ERROR_UNSUPPORTED_SASL_MECHANISM,
            true,
        ),
        // v0 is the one refusable handshake version that still has an encodable response shape,
        // so it gets a real UNSUPPORTED_VERSION. Anything above the ceiling has no schema at the
        // version the client asked for, and a body shaped for a different one would be misparsed.
        SaslAction::RejectHandshakeVersion => {
            if req.request_api_version < SASL_HANDSHAKE_VERSION {
                sasl_handshake_outcome(req.request_api_version, ERROR_UNSUPPORTED_VERSION, true)
            } else {
                HandleOutcome::Close
            }
        }
        SaslAction::RejectAuthenticateVersion => {
            debug!(
                %peer,
                api_version = req.request_api_version,
                "SaslAuthenticate version out of range; closing connection"
            );
            HandleOutcome::Close
        }
        SaslAction::Authenticate => {
            let (outcome, verified) = authenticate_token(ctx, req.request_api_version, body).await;
            // Only a plain `Respond` is success: every refusal path answers with
            // `RespondThenClose`, so the state advances on exactly the accepting branch.
            if matches!(outcome, HandleOutcome::Respond(_)) {
                *sasl_state = SaslState::Authenticated;
                *principal = verified;
            }
            outcome
        }
        SaslAction::IllegalState | SaslAction::IllegalStateKeepOpen => {
            debug!(
                %peer,
                api_key = req.request_api_key,
                "request rejected: not legal in this connection's SASL state"
            );
            illegal_state_outcome(
                req.request_api_key,
                req.request_api_version,
                matches!(action, SaslAction::IllegalStateKeepOpen),
                ctx.listener.advertisement(),
            )
        }
    }
}

/// Serves one accepted connection, plain or TLS, then ends its producer session.
async fn handle_connection<S>(
    stream: S,
    connection: Connection,
    certificate_name: Option<String>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let connection_state = connection.listener.connection_state();
    let bridge = connection.state.bridge.clone();
    let served = serve_frames(stream, connection, certificate_name, &connection_state).await;
    // A producer without a producer id has its Produce session by connection, so it ends here.
    if let Some(bridge) = bridge {
        bridge.end_connection(connection_state.id());
    }
    served
}

/// The frame loop of one accepted connection.
///
/// `certificate_name` is the Iggy username the client certificate mapped to, on an `SSL`
/// listener that verifies certificates. It is resolved against Iggy before the first frame, and
/// a name that resolves to no user closes the connection, so a certificate from the trusted CA
/// never admits a client as an account that does not exist.
async fn serve_frames<S>(
    mut stream: S,
    connection: Connection,
    certificate_name: Option<String>,
    connection_state: &ConnectionState,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let Connection {
        peer,
        listener,
        config,
        state,
        auth: shared_auth,
        cancel,
        tls: _,
    } = connection;
    debug!(%peer, protocol = %listener.protocol, "connection accepted");

    // A listener without SASL starts every connection already authenticated, so the dispatch
    // path below has one question to ask rather than two.
    let mut sasl_state = if listener.protocol.uses_sasl() {
        SaslState::new()
    } else {
        SaslState::Authenticated
    };
    let ctx = ConnectionContext {
        config: &config,
        listener: &listener,
        state: &state,
        connection: connection_state,
        authenticator: shared_auth.authenticator.as_deref(),
        resolver: shared_auth.resolver.as_deref(),
        auth_slots: &shared_auth.slots,
        lookup_slots: &shared_auth.lookup_slots,
        failed_logins: &shared_auth.failed_logins,
        peer: &peer,
        cancel: &cancel,
    };
    let mut principal = if let Some(username) = certificate_name {
        let Some(resolved) = resolve_certificate_principal(&ctx, &username).await else {
            return Ok(());
        };
        Some(resolved)
    } else if listener.anonymous_without_principal {
        Some(AuthenticatedPrincipal::anonymous())
    } else {
        None
    };

    loop {
        let idle_budget = if sasl_state.is_authenticated() {
            config.idle_timeout
        } else {
            config.pre_auth_timeout
        };
        let Some(frame) =
            read_next_frame(&mut stream, &config, idle_budget, &peer, &cancel).await?
        else {
            return Ok(());
        };

        if frame.len() < 8 {
            return Err(KafkaProtocolError::BufferUnderflow {
                needed: 8,
                remaining: frame.len(),
            });
        }
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let req_hdr_ver = request_header_version(api_key, api_version);
        let resp_hdr_ver = response_header_version(api_key, api_version);

        // `request_header_version` only ever returns 1 or 2, both of which `RequestHeader`
        // supports, so `decode` cannot fail on the version argument itself; any error here is a
        // malformed header and closes the connection.
        let mut body = frame;
        let mut req = RequestHeader::decode(&mut body, req_hdr_ver)
            .map_err(|e| KafkaProtocolError::Malformed(e.to_string()))?;

        debug!(
            %peer,
            api_key = req.request_api_key,
            api_version = req.request_api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );
        // A group request can park for a whole rebalance timeout. `client_id` and the tagged
        // fields are views into the frame, so keeping them would pin the frame for that long
        // after the handler has let go of its own copy.
        req.client_id = None;
        req.unknown_tagged_fields.clear();

        // `RequestHeader::decode` advances `body` past the header fields it consumed via
        // `Buf::advance`, so `body` is already exactly the request payload.
        let outcome = route_frame(&ctx, &mut sasl_state, &mut principal, &req, body).await;
        if dispatch_outcome(
            &mut stream,
            &peer,
            &config,
            &req,
            resp_hdr_ver,
            outcome,
            &cancel,
        )
        .await?
        {
            return Ok(());
        }
    }
}

/// Looks up the Iggy user a client certificate named, within the pre-authentication budget.
///
/// `None` closes the connection. Unlike a SASL refusal there is no response to shape: the client
/// has sent nothing yet, and a certificate the CA signed for an account that does not exist is a
/// provisioning mistake the gateway's log reports, not something the client can correct.
async fn resolve_certificate_principal(
    ctx: &ConnectionContext<'_>,
    username: &str,
) -> Option<AuthenticatedPrincipal> {
    let peer = ctx.peer;
    let Some(resolver) = ctx.resolver else {
        // Unreachable: `check_tls_wiring` refuses to start in this combination. Fail closed
        // anyway, since the alternative is admitting an unverified identity.
        error!(%peer, "a client certificate named a principal but nothing can look it up");
        return None;
    };
    // The lookup is an Iggy call per handshake, so it is bounded like a SASL login, but in slots
    // of its own: it costs no password hash.
    let lookup = within_auth_budget(
        ctx,
        ctx.lookup_slots,
        "certificate identity lookup",
        |slot| async move {
            let _slot = slot;
            resolver.resolve(username).await
        },
    );
    let outcome = tokio::select! {
        () = ctx.cancel.cancelled() => {
            debug!(%peer, "certificate identity lookup abandoned by shutdown");
            return None;
        }
        outcome = lookup => outcome?,
    };
    match outcome {
        Ok(principal) => {
            debug!(%peer, username, "client certificate authenticated");
            Some(principal)
        }
        Err(AuthError::Rejected) => {
            // warn!, not debug!: the certificate chained to the trusted CA, so this is an account
            // the operator meant to exist, or a mapping rule that produced the wrong name.
            warn!(%peer, username, "client certificate names no active Iggy user; closing");
            None
        }
        Err(AuthError::Unavailable) => {
            warn!(%peer, username, "could not look up the certificate's Iggy user; closing");
            None
        }
    }
}

/// Verifies a `SaslAuthenticate` token and answers it.
///
/// Every failure answers with the same code and the same message. A malformed token, an unknown
/// user, a wrong password and an unreachable Iggy are indistinguishable to the client on purpose;
/// the gateway's own log carries the difference.
async fn authenticate_token(
    ctx: &ConnectionContext<'_>,
    api_version: i16,
    body: Bytes,
) -> (HandleOutcome, Option<AuthenticatedPrincipal>) {
    let peer = ctx.peer;
    let failed = || {
        (
            sasl_authenticate_outcome(api_version, ERROR_SASL_AUTHENTICATION_FAILED, true),
            None,
        )
    };

    let Some(authenticator) = ctx.authenticator else {
        // Unreachable: `run` refuses to start in this combination. Fail closed anyway, since the
        // alternative is admitting an unauthenticated connection.
        error!(%peer, "SASL is enabled but no authenticator is configured");
        return failed();
    };
    let Ok(auth_bytes) = decode_sasl_auth_bytes(api_version, body) else {
        debug!(%peer, "malformed SaslAuthenticate token");
        return failed();
    };
    let Ok(credentials) = parse_plain(&auth_bytes) else {
        debug!(%peer, "malformed PLAIN initial response");
        return failed();
    };

    // Checked before a slot is taken, so a throttled peer costs neither a slot nor a hash. Closed
    // rather than answered 58 for the same reason an overload is: the client would treat 58 as
    // fatal, and a correct password retried after the delay must still get through.
    if ctx.failed_logins.is_blocked(peer.ip()) {
        debug!(%peer, "SASL authentication refused: peer is throttled after a rejected login");
        return (HandleOutcome::Close, None);
    }

    // The connection loop only watches the shutdown token between frames, so a verification in
    // flight has to watch it here or a drain waits out the whole budget.
    let verified = tokio::select! {
        () = ctx.cancel.cancelled() => {
            debug!(%peer, "SASL authentication abandoned by shutdown");
            return (HandleOutcome::Close, None);
        }
        verified = within_auth_budget(ctx, ctx.auth_slots, "SASL authentication", |slot| {
            authenticator.authenticate(&credentials, slot)
        }) => verified,
    };

    let Some(result) = verified else {
        // Overloaded or timed out. Close rather than answer 58: a Kafka client treats that code as
        // fatal and surfaces it to the application, and nothing here says the credentials were
        // wrong. A close reads as a transport failure, which is retriable.
        return (HandleOutcome::Close, None);
    };

    match result {
        Ok(authenticated) => {
            debug!(%peer, "SASL authentication succeeded");
            ctx.failed_logins.record_success(peer.ip());
            (
                sasl_authenticate_outcome(api_version, ERROR_NONE, false),
                Some(authenticated),
            )
        }
        // A rejection is the client's problem and is terminal, so it earns a parseable 58.
        Err(AuthError::Rejected) => {
            debug!(%peer, "SASL authentication rejected");
            ctx.failed_logins.record_rejection(peer.ip());
            failed()
        }
        // An outage is not. Kafka clients treat 58 as fatal and surface it to the application, so
        // answering with it would turn a momentary Iggy blip into a permanent authentication error
        // for credentials that were always correct. Closing without a body reads as a transport
        // failure instead, which is retriable, and still tells the client nothing about whether
        // the account exists.
        Err(AuthError::Unavailable) => {
            warn!(%peer, "SASL authentication could not be completed; Iggy is unreachable");
            (HandleOutcome::Close, None)
        }
    }
}

/// Answers `DescribeAcls` from the permissions captured when this connection authenticated.
///
/// Only reachable on a listener that names principals: elsewhere the key is not advertised and
/// falls through to ordinary dispatch, which refuses it. On a SASL listener the SASL gate refuses
/// every key but one before a principal exists, and every other such listener starts each
/// connection with a principal, anonymous when no certificate named one. A missing principal
/// is therefore a gateway fault.
fn describe_acls(
    ctx: &ConnectionContext<'_>,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let peer = ctx.peer;
    let Some(principal) = principal else {
        // `error!` rather than `debug!` on purpose, unlike every other refusal here: reaching
        // this means the routing guards above disagree with each other, which is a gateway
        // fault and not something a client can provoke.
        error!(%peer, "DescribeAcls reached a connection with no authenticated principal");
        // Not `encode_error_for_key`: key 29 is absent from `SUPPORTED_RANGES`, so that helper
        // always returns `Close` here and the code would read as if it answers when it cannot.
        return respond_describe_acls_error(api_version, ERROR_ILLEGAL_SASL_STATE, true);
    };
    // The firewall table cannot cover this key: it is kept out of `SUPPORTED_RANGES` on purpose,
    // so the advertised range would otherwise be enforced only by whatever `kafka_protocol`'s
    // schema happens to accept. A crate bump adding v4 would start answering v4 while ApiVersions
    // still says 3, which is exactly what the sibling SASL keys pin explicitly against.
    if !SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS.contains(&api_version) {
        debug!(%peer, api_version, "DescribeAcls version outside the advertised range");
        return HandleOutcome::Close;
    }
    if !principal.permissions_known {
        // The permission read failed after a successful login, so this connection holds no real
        // answer. Reporting the empty fallback would tell an operator the principal has no access,
        // which is a different statement from "we could not find out".
        warn!(%peer, "DescribeAcls asked on a connection whose permissions were never read");
        return respond_describe_acls_error(api_version, ERROR_UNKNOWN_SERVER_ERROR, false);
    }
    match decode_acl_filter(api_version, body) {
        Ok(filter) => describe_acls_outcome(
            api_version,
            &principal.username,
            &principal.permissions,
            &filter,
        ),
        Err(error) => {
            debug!(%peer, %error, "failed to decode DescribeAcls filter");
            // Kept open: a client that sent one bad filter may send a good one.
            respond_describe_acls_error(api_version, ERROR_INVALID_REQUEST, false)
        }
    }
}

/// Waits for one of `slots`, then runs `work` holding it, each within the pre-authentication
/// budget. `None` means a budget ran out.
///
/// Two budgets rather than one shared deadline. The wait needs its own bound so a queued
/// connection cannot hold a `max_connections` permit for as long as the backlog takes to drain,
/// which is the invariant `pre_auth_timeout` is documented to enforce. The work's budget starts
/// once the slot is held, because cutting it short does not stop the call it started inside Iggy:
/// work that inherited whatever the queue left of a shared deadline would be abandoned mid-hash
/// and hand its slot to the next one while Iggy is still working.
async fn within_auth_budget<'a, T, Work, Fut>(
    ctx: &ConnectionContext<'a>,
    slots: &'a Semaphore,
    operation: &'static str,
    work: Work,
) -> Option<T>
where
    Work: FnOnce(SemaphorePermit<'a>) -> Fut,
    Fut: Future<Output = T>,
{
    let peer = ctx.peer;
    let budget = ctx.config.pre_auth_timeout;
    let Ok(acquired) = timeout(budget, slots.acquire()).await else {
        warn!(%peer, operation, "no authentication slot came free within the pre-authentication budget");
        return None;
    };
    // Acquire fails only once the semaphore is closed, which this gateway never does.
    let Ok(slot) = acquired else {
        error!(%peer, operation, "authentication slots unavailable");
        return None;
    };
    let Ok(result) = timeout(budget, work(slot)).await else {
        warn!(%peer, operation, "did not complete within the pre-authentication budget");
        return None;
    };
    Some(result)
}

/// Answers a request that is well-formed but not legal in this connection's SASL state.
///
/// `keep_open` marks the one case a real broker does not treat as fatal: a SASL request arriving
/// on a connection that already authenticated.
///
/// The version is checked ahead of the encoders rather than left to fail inside one. A SASL key
/// asked for above the version this gateway accepts has no response schema to answer in, so
/// `keep_open` cannot hold there: a body shaped for another version would be misparsed, the same
/// reason [`SaslAction::RejectHandshakeVersion`] closes rather than clamping.
fn illegal_state_outcome(
    api_key: i16,
    api_version: i16,
    keep_open: bool,
    advertisement: Advertisement,
) -> HandleOutcome {
    match api_key {
        API_KEY_SASL_HANDSHAKE if (0..=SASL_HANDSHAKE_VERSION).contains(&api_version) => {
            sasl_handshake_outcome(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        API_KEY_SASL_AUTHENTICATE if (0..=SASL_AUTHENTICATE_MAX_VERSION).contains(&api_version) => {
            sasl_authenticate_outcome(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        // `encode_error_for_key` consults the firewall table, and this key is deliberately absent
        // from it, so routing through there would answer an unauthenticated client with a bodyless
        // close while the unreachable guard above is the one that speaks. Answer it directly.
        API_KEY_DESCRIBE_ACLS
            if advertisement.acls
                && SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS.contains(&api_version) =>
        {
            respond_describe_acls_error(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        API_KEY_SASL_HANDSHAKE | API_KEY_SASL_AUTHENTICATE => HandleOutcome::Close,
        API_KEY_DESCRIBE_ACLS if advertisement.acls => HandleOutcome::Close,
        _ => encode_error_for_key(
            api_key,
            api_version,
            ERROR_ILLEGAL_SASL_STATE,
            advertisement,
        ),
    }
}

/// Returns `Ok(None)` when shutdown cancellation wins; `Ok(Some(frame))` on a full frame.
async fn read_next_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    config: &GatewayConfig,
    idle_timeout: Duration,
    peer: &SocketAddr,
    cancel: &CancellationToken,
) -> Result<Option<bytes::Bytes>> {
    tokio::select! {
        () = cancel.cancelled() => {
            debug!(%peer, "connection cancelled by shutdown");
            Ok(None)
        }
        result = read_frame(
            stream,
            config.max_frame_size,
            idle_timeout,
            config.read_timeout,
        ) => match result {
            Ok(frame) => Ok(Some(frame)),
            Err(KafkaProtocolError::Io(ref e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                info!(%peer, "connection closed by client");
                Ok(None)
            }
            Err(e) => Err(e),
        },
    }
}

/// Applies a [`HandleOutcome`]. Returns `true` when the connection should close.
async fn dispatch_outcome<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    peer: &SocketAddr,
    config: &GatewayConfig,
    req: &RequestHeader,
    resp_hdr_ver: i16,
    outcome: HandleOutcome,
    cancel: &CancellationToken,
) -> Result<bool> {
    match outcome {
        HandleOutcome::NoResponse => {
            // Produce with acks=0: the wire protocol forbids a response.
            Ok(false)
        }
        HandleOutcome::Close => {
            // debug!, not warn!: this fires for every `Close` outcome regardless of cause -
            // unsupported API key, out-of-range version, no encodable response shape at the
            // requested version - all client/attacker-chosen, not operator-actionable, and a
            // version-probing client (or a port scanner) can trigger this once per attempt. The
            // few close reasons worth surfacing distinctly already have their own warn! closer
            // to the decision (`unsupported_version_response`'s "above encoder max" and the
            // Metadata version-firewall log in `protocol/api.rs`); this generic handler doesn't
            // need to double-log every one of those on top of its own record.
            debug!(
                %peer,
                api_key = req.request_api_key,
                api_version = req.request_api_version,
                "closing connection: no parseable response for this request"
            );
            Ok(true)
        }
        HandleOutcome::Respond(body_response) => {
            send_response(
                stream,
                req.correlation_id,
                resp_hdr_ver,
                body_response,
                config.write_timeout,
            )
            .await?;
            Ok(false)
        }
        HandleOutcome::RespondThenClose(body_response) => {
            send_response(
                stream,
                req.correlation_id,
                resp_hdr_ver,
                body_response,
                config.write_timeout,
            )
            .await?;
            debug!(
                %peer,
                api_key = req.request_api_key,
                api_version = req.request_api_version,
                "closing connection after responding"
            );
            close_gracefully(stream, config.write_timeout, cancel).await;
            Ok(true)
        }
    }
}

/// Sends FIN, then discards input until the peer closes, `budget` runs out or shutdown begins.
///
/// Dropping a socket with unread bytes in its receive queue makes Linux send RST instead of FIN,
/// and an RST lets the peer discard the response this close follows before its client reads it.
/// A real broker closes an authentication failure gracefully for the same reason. On a TLS
/// stream the shutdown also sends `close_notify` first.
async fn close_gracefully<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    budget: Duration,
    cancel: &CancellationToken,
) {
    let mut discard = [0u8; 4096];
    let drain_input = async {
        if stream.shutdown().await.is_err() {
            return;
        }
        while let Ok(read) = stream.read(&mut discard).await {
            if read == 0 {
                break;
            }
        }
    };
    tokio::select! {
        () = cancel.cancelled() => {}
        _ = timeout(budget, drain_input) => {}
    }
}

/// Response header size for a given header version: v0 is `correlation_id` only (4 bytes); v1
/// adds an empty tagged-fields byte (5 bytes). Kept inline rather than through
/// `kafka_protocol::messages::ResponseHeader` - that type's `Encodable` impl needs its own
/// `BytesMut` allocation, defeating the single-allocation framing this function exists for.
const fn response_header_size(header_version: i16) -> usize {
    if header_version >= 1 { 5 } else { 4 }
}

/// Write a single length-prefixed Kafka frame using one allocation.
/// Avoids the separate header-encode + payload-concat + length-prefix allocations.
async fn send_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    correlation_id: i32,
    header_version: i16,
    body: Bytes,
    write_timeout: Duration,
) -> Result<()> {
    let header_size = response_header_size(header_version);
    let payload_size = header_size + body.len();
    let payload_len_i32 =
        i32::try_from(payload_size).map_err(|_| KafkaProtocolError::FrameTooLarge {
            max_bytes: i32::MAX as usize,
            actual_bytes: payload_size,
        })?;
    let mut prefix = BytesMut::with_capacity(4 + header_size);
    prefix.put_i32(payload_len_i32);
    prefix.put_i32(correlation_id);
    if header_version >= 1 {
        prefix.put_u8(0); // empty tagged fields
    }
    let mut frame = prefix.freeze().chain(body);
    // The flush matters on TLS: the stream reports a write done once rustls holds the record,
    // and the tail of a large response would otherwise sit unsent until the next write.
    let write = async {
        stream.write_all_buf(&mut frame).await?;
        stream.flush().await
    };
    timeout(write_timeout, write)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

/// Read one length-prefixed Kafka frame from `stream`.
///
/// # Errors
///
/// Returns an error on timeout, invalid length, or I/O failure.
pub async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_frame_size: usize,
    idle_timeout: Duration,
    read_timeout: Duration,
) -> Result<bytes::Bytes> {
    let mut len_buf = [0u8; 4];
    // Idle: bounded wait for the client to start the next frame (or EOF).
    match timeout(idle_timeout, stream.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "idle timeout").into()),
    }

    let frame_len_i32 = i32::from_be_bytes(len_buf);
    if frame_len_i32 <= 0 {
        return Err(KafkaProtocolError::InvalidFrameLength(frame_len_i32));
    }
    // frame_len_i32 is validated > 0 above, so it always fits usize on every
    // platform this crate targets (32-bit and 64-bit).
    #[allow(clippy::cast_sign_loss)]
    let frame_len = frame_len_i32 as usize;
    if frame_len > max_frame_size {
        return Err(KafkaProtocolError::FrameTooLarge {
            max_bytes: max_frame_size,
            actual_bytes: frame_len,
        });
    }

    // In-flight: read_timeout applies only after the length prefix is complete.
    let deadline = tokio::time::Instant::now() + read_timeout;
    // Reserve incrementally, one chunk ahead of what's actually been received, instead of
    // BytesMut::with_capacity(frame_len) up front - frame_len comes straight from the wire
    // (bounded only by max_frame_size), so an attacker who sends a valid length prefix and
    // then no body would otherwise force a full max_frame_size allocation per connection
    // before a single body byte arrives (same amplification class as PREALLOC_HINT).
    let mut data = BytesMut::with_capacity(frame_len.min(READ_CHUNK));
    while data.len() < frame_len {
        let remaining = frame_len - data.len();
        let chunk = remaining.min(READ_CHUNK);
        data.reserve(chunk);
        // `.limit(chunk)` bounds how much of BytesMut's spare capacity read_buf may fill,
        // so a single OS read still can't consume bytes belonging to the next pipelined
        // frame - the same guarantee the old resize()+read() approach had - but without
        // pre-zeroing the chunk first, since read_buf only writes into its own spare
        // capacity via chunk_mut() rather than requiring pre-initialized memory.
        match timeout_at(deadline, stream.read_buf(&mut (&mut data).limit(chunk))).await {
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "read timeout").into()),
            Ok(Ok(0)) => {
                return Err(
                    io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed").into(),
                );
            }
            Ok(Err(e)) => return Err(e.into()),
            Ok(Ok(_)) => {}
        }
    }
    Ok(data.freeze())
}

/// Initializes the global tracing subscriber with a non-blocking stdout writer.
///
/// The default `tracing_subscriber::fmt()` writer is `io::Stdout`, a `LineWriter` behind a
/// `ReentrantLock` - one global lock acquisition and one blocking `write(2)` per log line,
/// serialized across every worker in the runtime. A client looping malformed request bodies on
/// one connection that stays open (decode failures never disconnect) has no rate limit on how
/// often that write happens; at scale it becomes the throughput ceiling for the whole gateway,
/// including well-behaved connections. `tracing_appender::non_blocking` moves the write onto a
/// dedicated thread behind a bounded queue (same pattern as `core/server_common/src/log/logger.rs`).
///
/// Returns the [`WorkerGuard`]; it must be held for the lifetime of `main` (dropping it stops the
/// worker thread and any buffered-but-unflushed log lines are lost) - see `main.rs`.
pub fn init_tracing() -> WorkerGuard {
    // `EnvFilter::new` drops a directive it cannot parse, and `iggy=warn` always parses, so one
    // typo in `RUST_LOG` would otherwise leave every other target with no output at all.
    let rust_log = std::env::var("RUST_LOG").ok();
    let (filter, rejected) = match EnvFilter::try_new(sdk_quieted_filter(rust_log.as_deref())) {
        Ok(filter) => (filter, None),
        Err(error) => (EnvFilter::new(sdk_quieted_filter(None)), Some(error)),
    };
    let (non_blocking_stdout, guard) = tracing_appender::non_blocking(io::stdout());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking_stdout)
        .try_init()
        .map_err(|e| error!("failed to initialize tracing: {e}"));
    if let Some(error) = rejected {
        warn!(%error, "RUST_LOG is invalid; logging at the default level instead");
    }
    guard
}

#[cfg(test)]
mod tests {
    use super::sdk_quieted_filter;

    #[test]
    fn given_no_rust_log_should_quiet_the_sdk() {
        assert_eq!(
            sdk_quieted_filter(None),
            "info,iggy=warn,iggy_gateway_kafka=info"
        );
        assert_eq!(
            sdk_quieted_filter(Some("")),
            "info,iggy=warn,iggy_gateway_kafka=info"
        );
    }

    #[test]
    fn given_a_rust_log_should_still_quiet_the_sdk() {
        // The whole point: reading RUST_LOG verbatim dropped the credential-disclosure control on
        // every run command this repository's own docs give.
        assert_eq!(
            sdk_quieted_filter(Some("info")),
            "info,iggy=warn,iggy_gateway_kafka=info"
        );
        assert_eq!(
            sdk_quieted_filter(Some("debug")),
            "debug,iggy=warn,iggy_gateway_kafka=debug"
        );
    }

    #[test]
    fn given_the_quieted_filter_should_keep_gateway_logs_and_drop_sdk_info() {
        // `iggy=warn` matches `iggy_gateway_kafka` by prefix, so the built string alone cannot
        // show that the gateway's own lines survive. Only the filter's verdict can.
        let filter = EnvFilter::try_new(sdk_quieted_filter(Some("info"))).expect("valid filter");
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::sink)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(
                target: "iggy_gateway_kafka::server",
                tracing::Level::INFO
            ));
            assert!(!tracing::enabled!(
                target: "iggy::clients::client",
                tracing::Level::INFO
            ));
            assert!(tracing::enabled!(
                target: "iggy::clients::client",
                tracing::Level::WARN
            ));
        });
    }

    #[test]
    fn given_a_gateway_directive_should_not_mistake_it_for_the_sdk() {
        // `iggy_gateway_kafka` shares a prefix with `iggy`, and treating it as an SDK directive
        // would leave the SDK at INFO with the username on every successful login.
        assert_eq!(
            sdk_quieted_filter(Some("iggy_gateway_kafka=debug")),
            "iggy_gateway_kafka=debug,iggy=warn"
        );
    }

    #[test]
    fn given_a_bare_gateway_target_should_not_override_it() {
        // A bare target enables every level for it. Appending `iggy_gateway_kafka=info` would
        // cap what the operator explicitly asked for.
        assert_eq!(
            sdk_quieted_filter(Some("iggy_gateway_kafka")),
            "iggy_gateway_kafka,iggy=warn"
        );
    }

    #[test]
    fn given_an_explicit_sdk_directive_should_be_left_alone() {
        assert_eq!(
            sdk_quieted_filter(Some("info,iggy=trace")),
            "info,iggy=trace"
        );
    }

    use serial_test::serial;

    use super::*;

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        let client = client.await.unwrap();
        (client, server)
    }

    /// One plaintext listener on an ephemeral port, the shape every test here runs.
    async fn ephemeral_plaintext() -> (BoundListener, SocketAddr) {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let listener = BoundListener::new(
            ListenerConfig::new(SecurityProtocol::Plaintext, addr.to_string()),
            socket,
        );
        (listener, addr)
    }

    /// `listener_robustness_tests.rs::e2e_frame_exceeding_max_frame_size_closes_connection`
    /// exercises the oversized-frame-rejection *mechanism* with a small custom `max_frame_size`
    /// (sending an 8 MiB+ frame just to hit the real default would be slow for no extra
    /// coverage) - this pins the default's *value* independently, so a change to the default
    /// cap doesn't slip through untested.
    #[test]
    fn default_max_frame_size_is_eight_mebibytes() {
        assert_eq!(GatewayConfig::default().max_frame_size, 8 * 1024 * 1024);
    }

    #[test]
    fn default_config_is_one_plaintext_listener_without_tls() {
        let config = GatewayConfig::default();
        assert_eq!(
            config.listeners,
            vec![ListenerConfig::new(
                SecurityProtocol::Plaintext,
                "127.0.0.1:9093"
            )]
        );
        assert!(config.tls.is_none());
        assert!(!config.sasl_enabled());
        assert!(!config.tls_enabled());
        assert!(!config.maps_certificate_identities());
    }

    #[test]
    fn transient_accept_error_classification_covers_all_branches() {
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(is_transient_accept_error(&io::Error::from(kind)));
        }

        #[cfg(unix)]
        {
            assert!(is_transient_accept_error(&io::Error::from_raw_os_error(23)));
            assert!(is_transient_accept_error(&io::Error::from_raw_os_error(24)));
        }

        assert!(!is_transient_accept_error(&io::Error::from(
            io::ErrorKind::ConnectionRefused,
        )));
    }

    #[tokio::test]
    async fn send_response_writes_header_and_body() {
        let (mut client, mut server) = tcp_pair().await;
        let body = [9u8, 8, 7];

        send_response(
            &mut server,
            0x0102_0304,
            1,
            Bytes::copy_from_slice(&body),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        assert_eq!(i32::from_be_bytes(len), 8);

        let mut payload = [0u8; 8];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload[..4], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(payload[4], 0);
        assert_eq!(&payload[5..], &body);
    }

    #[tokio::test]
    async fn read_frame_rejects_negative_length() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(-1_i32).to_be_bytes()).await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, KafkaProtocolError::InvalidFrameLength(-1)));
    }

    #[tokio::test]
    async fn read_frame_returns_eof_after_prefix_when_body_missing() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("connection closed"));
    }

    #[tokio::test]
    async fn read_frame_times_out_after_partial_body() {
        let (mut client, mut server) = tcp_pair().await;
        client.write_all(&(5_i32).to_be_bytes()).await.unwrap();
        client.write_all(&[1, 2]).await.unwrap();
        let err = read_frame(
            &mut server,
            64,
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("read timeout"));
    }

    #[tokio::test]
    async fn read_frame_times_out_when_client_sends_nothing() {
        let (_client, mut server) = tcp_pair().await;
        let err = read_frame(
            &mut server,
            64,
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("idle timeout"));
    }

    #[tokio::test]
    async fn server_shutdown_does_not_stall_past_drain_timeout() {
        let (listener, addr) = ephemeral_plaintext().await;
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig {
            // Idle timeout is intentionally long - cancellation + drain deadline, not the
            // idle timeout, must bound shutdown here.
            idle_timeout: Duration::from_mins(10),
            shutdown_drain_timeout: Duration::from_millis(200),
            ..GatewayConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(vec![listener], rx).await });

        // Held open, never sends a frame: without cancellation the task would park in
        // read_frame's idle wait for the full 600s idle_timeout.
        let mut held = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("shutdown must return well within the 600s idle_timeout")
            .unwrap();
        assert!(result.is_ok());

        // Cancellation must close the held socket so the client sees EOF, not a silent stall.
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), held.read(&mut buf))
            .await
            .expect("held connection must be closed on shutdown")
            .unwrap();
        assert_eq!(n, 0, "shutdown must deliver EOF to idle clients");
    }

    #[tokio::test]
    async fn server_rejects_connections_beyond_max_connections() {
        let (listener, addr) = ephemeral_plaintext().await;
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig {
            max_connections: 1,
            ..GatewayConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(vec![listener], rx).await });

        // First connection holds the only permit by never sending a frame.
        let held = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection should be accepted at the TCP level (backlog) but closed
        // immediately by the server once the permit acquisition fails.
        let mut rejected = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut buf))
            .await
            .expect("server should close rejected connection promptly")
            .unwrap();
        assert_eq!(n, 0, "rejected connection should be closed with EOF");

        tx.send(()).unwrap();
        drop(held);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn server_run_exits_when_shutdown_channel_closed() {
        let (listener, _addr) = ephemeral_plaintext().await;
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        let server = KafkaGateway::new(GatewayConfig::default());
        assert!(server.run(vec![listener], rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_when_shutdown_receiver_is_lagged() {
        let (listener, _addr) = ephemeral_plaintext().await;
        let (tx, rx) = broadcast::channel(1);
        tx.send(()).unwrap();
        tx.send(()).unwrap();
        let server = KafkaGateway::new(GatewayConfig::default());
        assert!(server.run(vec![listener], rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_on_shutdown_signal_ok() {
        let (listener, addr) = ephemeral_plaintext().await;
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig::default());
        let handle = tokio::spawn(async move { server.run(vec![listener], rx).await });

        let stream = TcpStream::connect(addr).await.unwrap();
        tx.send(()).unwrap();
        drop(stream);
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn server_run_refuses_an_empty_listener_list() {
        let (_tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig::default());
        assert!(server.run(Vec::new(), rx).await.is_err());
    }

    #[tokio::test]
    async fn server_run_refuses_a_tls_listener_without_material() {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener = BoundListener::new(
            ListenerConfig::new(SecurityProtocol::Ssl, "127.0.0.1:0"),
            socket,
        );
        let (_tx, rx) = broadcast::channel(1);
        let error = KafkaGateway::new(GatewayConfig::default())
            .run(vec![listener], rx)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("IGGY_KAFKA_TLS_CERT_FILE"), "got: {error}");
    }

    #[tokio::test]
    async fn server_run_refuses_tls_material_without_a_tls_listener() {
        let (listener, _addr) = ephemeral_plaintext().await;
        let (_tx, rx) = broadcast::channel(1);
        let config = GatewayConfig {
            tls: Some(TlsConfig::new("unused.pem", "unused.pem")),
            ..GatewayConfig::default()
        };
        let error = KafkaGateway::new(config)
            .run(vec![listener], rx)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no listener uses it"), "got: {error}");
    }

    #[tokio::test]
    async fn read_frame_accepts_exact_max_frame_size() {
        let (mut client, mut server) = tcp_pair().await;
        let max_frame_size = 64usize;
        let payload = vec![0xABu8; max_frame_size];
        client
            .write_all(&i32::try_from(max_frame_size).unwrap().to_be_bytes())
            .await
            .unwrap();
        client.write_all(&payload).await.unwrap();
        let frame = read_frame(
            &mut server,
            max_frame_size,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(frame.len(), max_frame_size);
    }

    #[tokio::test]
    async fn read_frame_reassembles_frame_spanning_multiple_read_chunks() {
        // frame_len exceeds READ_CHUNK, forcing the incremental reserve()+read_buf loop
        // through more than one iteration - guards the switch away from a single
        // BytesMut::with_capacity(frame_len) upfront allocation.
        let (mut client, mut server) = tcp_pair().await;
        let frame_len = READ_CHUNK + 1024;
        let payload: Vec<u8> = (0..frame_len)
            .map(|i| u8::try_from(i % 251).expect("i % 251 < 256"))
            .collect();
        client
            .write_all(&i32::try_from(frame_len).unwrap().to_be_bytes())
            .await
            .unwrap();
        client.write_all(&payload).await.unwrap();
        let frame = read_frame(
            &mut server,
            frame_len,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(&frame[..], &payload[..]);
    }

    #[tokio::test]
    async fn read_frame_rejects_frame_larger_than_max() {
        let (mut client, mut server) = tcp_pair().await;
        let max_frame_size = 64usize;
        client.write_all(&65_i32.to_be_bytes()).await.unwrap();
        let err = read_frame(
            &mut server,
            max_frame_size,
            Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            KafkaProtocolError::FrameTooLarge {
                max_bytes: 64,
                actual_bytes: 65,
            }
        ));
    }

    #[tokio::test]
    async fn send_response_v0_writes_correlation_id_only() {
        let (mut client, mut server) = tcp_pair().await;
        let body = [5u8, 6, 7];

        send_response(
            &mut server,
            0x0000_00AB,
            0,
            Bytes::copy_from_slice(&body),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        assert_eq!(i32::from_be_bytes(len), 7);

        let mut payload = [0u8; 7];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload[..4], &[0, 0, 0, 0xAB]);
        assert_eq!(&payload[4..], &body);
    }

    /// `#[serial]`, unkeyed (shares `bridge::config`'s default group - both this module and
    /// `bridge::config` compile into the same lib unit-test binary. `main.rs`'s own `#[serial]`
    /// tests do NOT share this group: `main.rs` is the separate `iggy-gateway-kafka` bin's own
    /// test harness, a different process, and `serial_test`'s mutex is process-local - see the
    /// first of those tests' doc comment for the mirror-image note): `init_tracing` reads `RUST_LOG` with
    /// `std::env::var`, and edition 2024's `env::set_var`/`remove_var` are unsound against *any*
    /// concurrent env read in another thread, not just a write to the same key - a set/remove
    /// elsewhere in this binary racing this read is exactly the hazard, regardless of which var
    /// either side touches.
    #[test]
    #[serial]
    fn init_tracing_is_idempotent() {
        let _first_guard = init_tracing();
        let _second_guard = init_tracing();
    }
}
