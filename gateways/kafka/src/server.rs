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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::RequestHeader;
use kafka_protocol::protocol::Decodable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast};
use tokio::time::{timeout, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use tracing_appender::non_blocking::WorkerGuard;

use crate::auth::{AuthError, AuthenticatedPrincipal, SaslAuthenticator};
use crate::error::{KafkaProtocolError, Result};
use crate::protocol::api::{
    API_KEY_DESCRIBE_ACLS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, BrokerAdvertise,
    DEFAULT_KAFKA_PORT, ERROR_ILLEGAL_SASL_STATE, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_SASL_AUTHENTICATION_FAILED, ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNSUPPORTED_SASL_MECHANISM,
    ERROR_UNSUPPORTED_VERSION, HandleOutcome, SASL_ADVERTISED_DESCRIBE_ACLS_VERSIONS,
    decode_acl_filter, decode_sasl_auth_bytes, decode_sasl_mechanism, describe_acls_outcome,
    encode_error_for_key, handle_request_bounded, respond_describe_acls_error,
    sasl_authenticate_outcome, sasl_handshake_outcome,
};
use crate::protocol::header::{request_header_version, response_header_version};
use crate::protocol::sasl::{SASL_HANDSHAKE_VERSION, SaslAction, SaslState, parse_plain};
use std::io;

const READ_CHUNK: usize = 65536;

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
fn sdk_quieted_filter(rust_log: Option<&str>) -> String {
    let base = rust_log.unwrap_or("info");
    let base = if base.trim().is_empty() { "info" } else { base };
    let names_sdk = base
        .split(',')
        .any(|directive| directive.trim().starts_with("iggy="));
    if names_sdk {
        base.to_string()
    } else {
        format!("{base},iggy=warn")
    }
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub bind_addr: String,
    /// Hostname or IP advertised in Metadata (`IGGY_KAFKA_ADVERTISED_HOST`). Required when
    /// `bind_addr` uses a wildcard address (`0.0.0.0` / `::`).
    pub advertised_host: Option<String>,
    /// Port advertised in Metadata (`IGGY_KAFKA_ADVERTISED_PORT`). Defaults to the bind port.
    pub advertised_port: Option<u16>,
    pub max_frame_size: usize,
    /// Maximum concurrent connections accepted before new ones are rejected.
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
    /// Require SASL authentication before serving any other API.
    ///
    /// Off by default, and switching it on is a breaking change for every client already talking
    /// to this gateway: the two SASL keys only appear in the `ApiVersions` advertisement while it
    /// is on, and unauthenticated clients stop being served.
    ///
    /// While it is off the keys are still answered, with `ILLEGAL_SASL_STATE` and an empty
    /// mechanism list, because a well-formed response exists for them unlike a genuinely unknown
    /// key. They are kept out of the advertisement so nothing is invited into an exchange that
    /// cannot finish.
    pub sasl_enabled: bool,
    /// How long an unauthenticated connection may sit between frames.
    ///
    /// Separate from `idle_timeout` because that one is sized for a well-behaved idle client (10
    /// minutes, matching a real broker) and applies to connections that have proven who they are.
    /// A connection that has proven nothing still holds a `max_connections` permit, so it gets a
    /// budget measured in seconds instead.
    pub pre_auth_timeout: Duration,
    /// Credential verifications allowed to run at once, across every connection.
    ///
    /// Each one costs an Argon2id verify on an Iggy shard thread that has no blocking pool, so
    /// unauthenticated traffic can otherwise saturate the server's request loop with nothing but
    /// shape-valid tokens. `max_connections` alone is not a bound on that: it caps sockets, not
    /// the work each one can ask Iggy to do. Verifications beyond this queue rather than fail,
    /// since a rejected login is indistinguishable from a wrong password to the client.
    pub max_concurrent_authentications: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind_addr: format!("127.0.0.1:{DEFAULT_KAFKA_PORT}"),
            advertised_host: None,
            advertised_port: None,
            max_frame_size: 8 * 1024 * 1024,
            max_connections: 1024,
            idle_timeout: Duration::from_mins(10),
            read_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
            shutdown_drain_timeout: Duration::from_secs(25),
            sasl_enabled: false,
            pre_auth_timeout: Duration::from_secs(15),
            max_concurrent_authentications: 16,
        }
    }
}

impl BrokerAdvertise {
    /// Resolve the broker endpoint advertised in Metadata.
    ///
    /// `local_addr` is the address the listener is actually bound to (from `listener.local_addr()`).
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when `advertised_host` is empty or the listener binds to a wildcard
    /// without an explicit advertised host.
    pub fn from_server_config(config: &GatewayConfig, local_addr: SocketAddr) -> Result<Self> {
        let port = config
            .advertised_port
            .map_or_else(|| i32::from(local_addr.port()), i32::from);

        let host = if let Some(ref advertised) = config.advertised_host {
            let trimmed = advertised.trim();
            if trimmed.is_empty() {
                return Err(KafkaProtocolError::InvalidConfig(
                    "IGGY_KAFKA_ADVERTISED_HOST must not be empty".into(),
                ));
            }
            if trimmed.len() > i16::MAX as usize {
                return Err(KafkaProtocolError::InvalidConfig(
                    "IGGY_KAFKA_ADVERTISED_HOST exceeds Kafka nullable string limit (32767 bytes)"
                        .into(),
                ));
            }
            trimmed.to_string()
        } else if local_addr.ip().is_unspecified() {
            return Err(KafkaProtocolError::InvalidConfig(
                "binding to a wildcard address (0.0.0.0 or ::) requires \
                 IGGY_KAFKA_ADVERTISED_HOST to be set to a reachable hostname or IP for \
                 Metadata broker advertisement"
                    .into(),
            ));
        } else {
            local_addr.ip().to_string()
        };

        Ok(Self { host, port })
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

pub struct KafkaGateway {
    config: Arc<GatewayConfig>,
    authenticator: Option<Arc<dyn SaslAuthenticator>>,
}

/// Owned by [`KafkaGateway::run`] and shared by every connection it serves.
struct SharedAuth {
    authenticator: Option<Arc<dyn SaslAuthenticator>>,
    slots: Semaphore,
}

impl KafkaGateway {
    #[must_use]
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config: Arc::new(config),
            authenticator: None,
        }
    }

    /// Supplies the verifier that SASL credentials are checked against.
    ///
    /// Required when `sasl_enabled` is set: a gateway that demands authentication with nothing to
    /// authenticate against would refuse every client, so [`Self::run`] rejects that combination
    /// at startup rather than at the first login attempt.
    #[must_use]
    pub fn with_authenticator(mut self, authenticator: Arc<dyn SaslAuthenticator>) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// Accept Kafka wire connections until `shutdown` fires, then drain in-flight tasks.
    ///
    /// `listener` must already be bound by the caller. This lets tests and `main` bind
    /// the port before spawning the task, eliminating the TOCTOU race of bind-drop-rebind.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid config or a non-transient `accept()` error.
    pub async fn run(
        self,
        listener: TcpListener,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<()> {
        if !self.config.sasl_enabled && self.authenticator.is_some() {
            // The mirror of the guard below, and the quieter mistake: a verifier attached while the
            // flag is off means every connection is served unauthenticated, with nothing in the log
            // to say so. Refusing to start is the only way that failure is visible.
            return Err(KafkaProtocolError::InvalidConfig(
                "an authenticator is configured but SASL is disabled; every connection would be \
                 served unauthenticated. Set IGGY_KAFKA_SASL_ENABLED=true, or remove the \
                 authenticator"
                    .into(),
            ));
        }
        if self.config.sasl_enabled && self.authenticator.is_none() {
            return Err(KafkaProtocolError::InvalidConfig(
                "SASL is enabled but no authenticator is configured; every client would be \
                 rejected. Set IGGY_KAFKA_IGGY_ADDR and the Iggy credentials, or unset \
                 IGGY_KAFKA_SASL_ENABLED"
                    .into(),
            ));
        }
        let local_addr = listener.local_addr()?;
        let broker = Arc::new(BrokerAdvertise::from_server_config(
            &self.config,
            local_addr,
        )?);
        info!(
            "kafka listener bound on {} (advertised as {}:{})",
            local_addr, broker.host, broker.port
        );

        let shared_auth = Arc::new(SharedAuth {
            authenticator: self.authenticator.clone(),
            slots: Semaphore::new(self.config.max_concurrent_authentications),
        });
        let tracker = TaskTracker::new();
        let conn_limiter = Arc::new(Semaphore::new(self.config.max_connections));
        // Cancelled on shutdown so connection tasks exit instead of sitting in idle waits
        // until `idle_timeout` (or forever if that is raised).
        let cancel = CancellationToken::new();

        let drain_timeout = self.config.shutdown_drain_timeout;

        loop {
            tokio::select! {
                result = shutdown.recv() => {
                    match result {
                        Ok(()) => {
                            info!("kafka listener shutdown requested");
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                        // Capacity-1 channel: lagged means a signal was sent before we polled - treat as shutdown.
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            info!("kafka listener shutdown requested (lagged)");
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            drain(&tracker, &cancel, drain_timeout).await;
                            break;
                        }
                    }
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer)) => {
                            let Ok(permit) = Arc::clone(&conn_limiter).try_acquire_owned() else {
                                warn!(%peer, max_connections = self.config.max_connections, "connection limit reached, rejecting");
                                continue;
                            };
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!(%peer, "TCP_NODELAY failed: {e}");
                            }
                            if let Err(e) = enable_tcp_keepalive(&stream) {
                                warn!(%peer, "TCP_KEEPALIVE failed: {e}");
                            }
                            let cfg = Arc::clone(&self.config);
                            let broker = Arc::clone(&broker);
                            let auth = Arc::clone(&shared_auth);
                            let conn_cancel = cancel.child_token();
                            tracker.spawn(async move {
                                let _permit = permit;
                                if let Err(err) =
                                    handle_connection(stream, cfg, peer, broker, auth, conn_cancel)
                                        .await
                                {
                                    // debug!, not warn!: every `KafkaProtocolError` that can
                                    // reach here is either a malformed/oversized frame from the
                                    // client (attacker-controlled, not operator-actionable) or a
                                    // plain TCP reset/EOF - never an internal gateway fault. A
                                    // flood of either at warn! would drown real signal in logs
                                    // under adversarial load (same reasoning as the decode-
                                    // failure downgrades in `protocol/api.rs`).
                                    debug!(%peer, "connection closed with error: {err}");
                                }
                            });
                        }
                        Err(e) if is_transient_accept_error(&e) => {
                            // Brief backoff on fd exhaustion to avoid busy-spinning.
                            if matches!(e.raw_os_error(), Some(23 | 24)) {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            warn!(%e, "transient accept error, continuing");
                        }
                        Err(e) => {
                            drain(&tracker, &cancel, drain_timeout).await;
                            return Err(e.into());
                        }
                    }
                }

            }
        }
        Ok(())
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
    broker: &'a BrokerAdvertise,
    authenticator: Option<&'a dyn SaslAuthenticator>,
    auth_slots: &'a Semaphore,
    peer: &'a SocketAddr,
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
        // this connection authenticated as, which only the connection knows. Gated on the feature
        // as well as the key: with SASL off there is no principal, and the key is not advertised,
        // so it falls through to ordinary dispatch and is refused as the unlisted key it is.
        SaslAction::Dispatch
            if ctx.config.sasl_enabled && req.request_api_key == API_KEY_DESCRIBE_ACLS =>
        {
            describe_acls(principal.as_ref(), req.request_api_version, body, peer)
        }
        SaslAction::Dispatch => handle_request_bounded(
            req.request_api_key,
            req.request_api_version,
            body,
            ctx.broker,
            ctx.config.max_frame_size,
            ctx.config.sasl_enabled,
        ),
        SaslAction::DispatchFirstApiVersions => {
            let outcome = handle_request_bounded(
                req.request_api_key,
                req.request_api_version,
                body,
                ctx.broker,
                ctx.config.max_frame_size,
                ctx.config.sasl_enabled,
            );
            // Spend the allowance only on an answer the client can use. A refusal it is entitled
            // to retry at a lower version, which is what the KIP-511 downgrade path does, must not
            // consume the single attempt and strand a conformant client on the retry.
            if answered_successfully(&outcome) {
                *sasl_state = SaslState::AwaitHandshake {
                    api_versions_seen: true,
                };
            }
            outcome
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
            let (outcome, verified) = authenticate_token(
                ctx.authenticator,
                ctx.auth_slots,
                ctx.config.pre_auth_timeout,
                req.request_api_version,
                body,
                peer,
            )
            .await;
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
                ctx.config.sasl_enabled,
            )
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<GatewayConfig>,
    peer: SocketAddr,
    broker: Arc<BrokerAdvertise>,
    shared_auth: Arc<SharedAuth>,
    cancel: CancellationToken,
) -> Result<()> {
    debug!(%peer, "connection accepted");

    // A gateway with SASL off starts every connection already authenticated, so the dispatch path
    // below has one question to ask rather than two.
    let mut sasl_state = if config.sasl_enabled {
        SaslState::new()
    } else {
        SaslState::Authenticated
    };
    let mut principal: Option<AuthenticatedPrincipal> = None;
    let ctx = ConnectionContext {
        config: &config,
        broker: &broker,
        authenticator: shared_auth.authenticator.as_deref(),
        auth_slots: &shared_auth.slots,
        peer: &peer,
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
        let req = RequestHeader::decode(&mut body, req_hdr_ver)
            .map_err(|e| KafkaProtocolError::Malformed(e.to_string()))?;

        debug!(
            %peer,
            api_key = req.request_api_key,
            api_version = req.request_api_version,
            correlation_id = req.correlation_id,
            client_id = req.client_id.as_deref().unwrap_or(""),
            "received request"
        );

        // `RequestHeader::decode` advances `body` past the header fields it consumed via
        // `Buf::advance`, so `body` is already exactly the request payload.
        let outcome = route_frame(&ctx, &mut sasl_state, &mut principal, &req, body).await;
        if dispatch_outcome(&mut stream, &peer, &config, &req, resp_hdr_ver, outcome).await? {
            return Ok(());
        }
    }
}

/// Verifies a `SaslAuthenticate` token and answers it.
///
/// Every failure answers with the same code and the same message. A malformed token, an unknown
/// user, a wrong password and an unreachable Iggy are indistinguishable to the client on purpose;
/// the gateway's own log carries the difference.
async fn authenticate_token(
    authenticator: Option<&dyn SaslAuthenticator>,
    auth_slots: &Semaphore,
    budget: Duration,
    api_version: i16,
    body: Bytes,
    peer: &SocketAddr,
) -> (HandleOutcome, Option<AuthenticatedPrincipal>) {
    let failed = || {
        (
            sasl_authenticate_outcome(api_version, ERROR_SASL_AUTHENTICATION_FAILED, true),
            None,
        )
    };

    let Some(authenticator) = authenticator else {
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

    // The wait for a slot and the verification itself share one deadline, and it is the same
    // pre-authentication budget every other unauthenticated read gets. Without it the queue is the
    // bound: an unauthenticated connection would sit in `acquire()` for as long as the backlog
    // takes to drain, holding a `max_connections` permit the whole time, which is precisely the
    // invariant `pre_auth_timeout` is documented to enforce.
    let verified = tokio::time::timeout(budget, async {
        // Acquire fails only once the semaphore is closed, which this gateway never does.
        let Ok(_slot) = auth_slots.acquire().await else {
            error!(%peer, "authentication slots unavailable");
            return None;
        };
        Some(authenticator.authenticate(&credentials).await)
    })
    .await;

    let Ok(Some(result)) = verified else {
        // Overloaded or shutting down. Close rather than answer 58: a Kafka client treats that
        // code as fatal and surfaces it to the application, and nothing here says the credentials
        // were wrong. A close reads as a transport failure, which is retriable.
        warn!(%peer, "authentication did not complete within the pre-authentication budget");
        return (HandleOutcome::Close, None);
    };

    match result {
        Ok(authenticated) => {
            debug!(%peer, "SASL authentication succeeded");
            (
                sasl_authenticate_outcome(api_version, ERROR_NONE, false),
                Some(authenticated),
            )
        }
        // A rejection is the client's problem and is terminal, so it earns a parseable 58.
        Err(AuthError::Rejected) => {
            debug!(%peer, "SASL authentication rejected");
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
/// Only reachable on an authenticated connection: the SASL gate refuses every key but one before a
/// principal exists, and the caller additionally gates this on the feature being on, so a gateway
/// with SASL off never routes here. The `None` arm is a fail-closed guard, not a reachable path.
fn describe_acls(
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
    peer: &SocketAddr,
) -> HandleOutcome {
    let Some(principal) = principal else {
        // `error!` rather than `debug!` on purpose, unlike every other refusal here: reaching this
        // means the routing guards above disagree with each other, which is a gateway fault and not
        // something a client can provoke.
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

/// Whether an `ApiVersions` outcome carries `error_code` 0, meaning the client got a usable
/// answer rather than one it is expected to retry at a lower version.
fn answered_successfully(outcome: &HandleOutcome) -> bool {
    let (HandleOutcome::Respond(body) | HandleOutcome::RespondThenClose(body)) = outcome else {
        return false;
    };
    // `ApiVersions` puts its error code in the first two bytes of the body at every version.
    body.len() >= 2 && i16::from_be_bytes([body[0], body[1]]) == ERROR_NONE
}

/// Answers a request that is well-formed but not legal in this connection's SASL state.
///
/// `keep_open` marks the one case a real broker does not treat as fatal: a SASL request arriving
/// on a connection that already authenticated.
fn illegal_state_outcome(
    api_key: i16,
    api_version: i16,
    keep_open: bool,
    sasl_enabled: bool,
) -> HandleOutcome {
    match api_key {
        API_KEY_SASL_HANDSHAKE => {
            sasl_handshake_outcome(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        API_KEY_SASL_AUTHENTICATE => {
            sasl_authenticate_outcome(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        // `encode_error_for_key` consults the firewall table, and this key is deliberately absent
        // from it, so routing through there would answer an unauthenticated client with a bodyless
        // close while the unreachable guard above is the one that speaks. Answer it directly.
        API_KEY_DESCRIBE_ACLS if sasl_enabled => {
            respond_describe_acls_error(api_version, ERROR_ILLEGAL_SASL_STATE, !keep_open)
        }
        _ => encode_error_for_key(api_key, api_version, ERROR_ILLEGAL_SASL_STATE, sasl_enabled),
    }
}

/// Returns `Ok(None)` when shutdown cancellation wins; `Ok(Some(frame))` on a full frame.
async fn read_next_frame(
    stream: &mut TcpStream,
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
async fn dispatch_outcome(
    stream: &mut TcpStream,
    peer: &SocketAddr,
    config: &GatewayConfig,
    req: &RequestHeader,
    resp_hdr_ver: i16,
    outcome: HandleOutcome,
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
            Ok(true)
        }
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
async fn send_response(
    stream: &mut TcpStream,
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
    timeout(write_timeout, stream.write_all_buf(&mut frame))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;
    Ok(())
}

/// Read one length-prefixed Kafka frame from `stream`.
///
/// # Errors
///
/// Returns an error on timeout, invalid length, or I/O failure.
pub async fn read_frame(
    stream: &mut TcpStream,
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
    let filter = tracing_subscriber::EnvFilter::new(sdk_quieted_filter(
        std::env::var("RUST_LOG").ok().as_deref(),
    ));
    let (non_blocking_stdout, guard) = tracing_appender::non_blocking(io::stdout());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(non_blocking_stdout)
        .try_init()
        .map_err(|e| error!("failed to initialize tracing: {e}"));
    guard
}

#[cfg(test)]
mod tests {
    use super::sdk_quieted_filter;

    #[test]
    fn given_no_rust_log_should_quiet_the_sdk() {
        assert_eq!(sdk_quieted_filter(None), "info,iggy=warn");
        assert_eq!(sdk_quieted_filter(Some("")), "info,iggy=warn");
    }

    #[test]
    fn given_a_rust_log_should_still_quiet_the_sdk() {
        // The whole point: reading RUST_LOG verbatim dropped the credential-disclosure control on
        // every run command this repository's own docs give.
        assert_eq!(sdk_quieted_filter(Some("info")), "info,iggy=warn");
        assert_eq!(sdk_quieted_filter(Some("debug")), "debug,iggy=warn");
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig {
            // Idle timeout is intentionally long - cancellation + drain deadline, not the
            // idle timeout, must bound shutdown here.
            idle_timeout: Duration::from_mins(10),
            shutdown_drain_timeout: Duration::from_millis(200),
            ..GatewayConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig {
            max_connections: 1,
            ..GatewayConfig::default()
        });
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = broadcast::channel(1);
        drop(tx);
        let server = KafkaGateway::new(GatewayConfig::default());
        assert!(server.run(listener, rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_when_shutdown_receiver_is_lagged() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = broadcast::channel(1);
        tx.send(()).unwrap();
        tx.send(()).unwrap();
        let server = KafkaGateway::new(GatewayConfig::default());
        assert!(server.run(listener, rx).await.is_ok());
    }

    #[tokio::test]
    async fn server_run_exits_on_shutdown_signal_ok() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = broadcast::channel(1);
        let server = KafkaGateway::new(GatewayConfig::default());
        let handle = tokio::spawn(async move { server.run(listener, rx).await });

        let stream = TcpStream::connect(addr).await.unwrap();
        tx.send(()).unwrap();
        drop(stream);
        assert!(handle.await.unwrap().is_ok());
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
    /// test does NOT share this group: `main.rs` is the separate `iggy-gateway-kafka` bin's own
    /// test harness, a different process, and `serial_test`'s mutex is process-local - see that
    /// test's own doc comment for the mirror-image note): `init_tracing` reads `RUST_LOG` via
    /// `EnvFilter::try_from_default_env`, and edition 2024's `env::set_var`/`remove_var` are
    /// unsound against *any* concurrent env read in another thread, not just a write to the same
    /// key - a set/remove elsewhere in this binary racing this read is exactly the hazard,
    /// regardless of which var either side touches.
    #[test]
    #[serial]
    fn init_tracing_is_idempotent() {
        let _first_guard = init_tracing();
        let _second_guard = init_tracing();
    }
}
