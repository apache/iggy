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

//! Shard-local message bus with two wire planes.
//!
//! # Plane split
//!
//! - **Replica plane (TCP forever)**: VSR consensus traffic between
//!   replicas. Implemented in [`replica::listener`], [`connector`], and
//!   [`replica::io`]. Datagram or gateway-terminated transports are NOT
//!   supported here and never will be — see
//!   `replica::listener`'s module docs for the rationale.
//! - **SDK-client plane**: ephemeral client connections. Available
//!   transports: TCP, TCP-TLS, WebSocket, WSS, QUIC. Each request
//!   carries a `(client: u128, request: u64)` pair in `RequestHeader`;
//!   downstream consumers in `core/server-ng` are free to use it for
//!   tracing, idempotency, or correlation.
//!
//! # Auth
//!
//! Neither plane is authenticated at the bus layer. Both connect first
//! and let the caller (`core/server-ng`) gate command access via
//! application-level LOGIN commands:
//!
//! - SDK-client plane: `LOGIN_USER` / `LOGIN_WITH_PERSONAL_ACCESS_TOKEN`,
//!   pre-LOGIN allowlist `PING`, `LOGIN_USER`, `LOGIN_WITH_PAT`.
//! - Replica plane: a future `LOGIN_REPLICA` command carries the
//!   cluster's shared secret. Until that command succeeds the caller
//!   MUST NOT honor consensus messages from the connection. The
//!   `Ping` frame at connect time announces `replica_id` and
//!   `cluster_id` (the listener checks `cluster_id` matches the local
//!   cluster and uses `replica_id` to key its registry); it carries
//!   no MAC.
//!
//! # Invariants worth naming
//!
//! - [`send_to_client`](IggyMessageBus::send_to_client) and
//!   [`send_to_replica`](IggyMessageBus::send_to_replica) return
//!   `Ready` on first poll. Consensus code relies on this for
//!   reentrancy reasoning; any `.await` in the body breaks it.
//! - The TCP transport's writer task coalesces up to
//!   `MessageBusConfig::max_batch` (default 256) `Frozen<MESSAGE_ALIGN>`
//!   into one `write_vectored_all`. Don't introduce per-message
//!   syscalls or per-message encryption on the plaintext TCP plane.
//! - fd-delegation ([`fd_transfer`]) is TCP-only. TLS / QUIC
//!   connections have no dupable plaintext fd, so shard 0 terminates
//!   and forwards `Frozen<MESSAGE_ALIGN>` over the existing
//!   inter-shard flume.
//! - 0-RTT stays disabled by default on any future QUIC path. Per-
//!   command opt-in requires a checked-in idempotence audit.
//!
//! # Transport abstraction
//!
//! [`transports`] defines the trait surface every wire plane sits
//! behind: [`transports::TransportListener`] and
//! [`transports::TransportConn`] with its single
//! [`transports::TransportConn::run`] entry point.
//! [`installer::install_replica_conn`] /
//! [`installer::install_client_conn`] are generic over it so every
//! transport (TCP, TCP-TLS, WS, WSS, QUIC) plugs in behind the same
//! registry, fencing, and dispatch logic.

pub mod cache;
pub mod client_listener;
pub mod config;
pub mod connector;
mod error;
pub mod fd_transfer;
pub mod framing;
pub mod installer;
pub mod lifecycle;
pub mod replica;
#[doc(hidden)]
pub mod socket_opts;
pub mod transports;

pub use config::{IOV_MAX_LIMIT, MessageBusConfig, QuicTuning, WebSocketConfig};
pub use error::SendError;
pub use installer::ConnectionInstaller;
pub use installer::conn_info::{
    ClientConnMeta, ClientTransportKind, QuicConnectionInfo, TlsConnectionInfo, WsUpgradeInfo,
};
pub use lifecycle::{
    BusMessage, BusReceiver, BusSender, ConnectionRegistry, DrainOutcome, FusedShutdown,
    RejectedRegistration, ReplicaRegistry, Shutdown, ShutdownToken,
};
pub use transports::tls::TlsServerCredentials;

use compio::runtime::JoinHandle;
use configs::server_ng::ServerNgConfig;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

/// Callback for forwarding a consensus message to a remote shard.
///
/// Provided by the shard layer at bus construction and installed via
/// [`IggyMessageBus::set_replica_forward_fn`] / [`IggyMessageBus::set_client_forward_fn`].
/// Arguments: `(target_shard_id, message)`. Returns `Ok(())` on successful
/// enqueue into the inter-shard channel, or [`SendError::RoutingFailed`] on
/// `try_send` failure.
///
/// Asymmetry vs the `Rc<dyn Fn>` siblings (`AcceptedReplicaFn`,
/// `AcceptedClientFn`, `ConnectionLostFn`): the forward fn has a single
/// owner (the bus) and is installed once at bootstrap; `Box` suffices.
/// The `Rc` siblings are shared with accept loops and connection tasks
/// that outlive the caller and need independent ownership.
pub type ShardForwardFn = Box<dyn Fn(u16, Frozen<MESSAGE_ALIGN>) -> Result<(), SendError>>;

/// Callback invoked on every successful replica handshake.
///
/// Fired by the replica listener / outbound connector. The callback decides
/// whether to install the stream locally or ship it to another shard; this
/// crate does not need to care about that policy. Takes ownership of the
/// `TcpStream`. On the inbound (listener) path the replica id has been
/// validated against `replica_count`, the local `cluster_id`, and the
/// directional rule (peer id strictly less than this replica's id). On
/// the outbound (connector) path the dialer trusts the configured peer
/// list and the callback receives the pre-configured peer id.
pub type AcceptedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, u8)>;

/// Callback invoked on every accepted SDK client connection.
///
/// Takes ownership of the accepted stream and is responsible for minting /
/// assigning the client id as part of its delegation policy.
pub type AcceptedClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

/// Owned bundle of a fully-handshaked QUIC client connection plus its
/// first accepted bidirectional stream pair.
///
/// Wraps the three `compio_quic` types that previously appeared in
/// [`AcceptedQuicClientFn`]'s signature so the bus's public API does
/// not parameterise on `compio_quic`'s exposed types. A future
/// `compio_quic` version bump that renames or restructures
/// `Connection` / `SendStream` / `RecvStream` no longer constitutes a
/// SemVer-major change for `iggy_message_bus`.
pub struct AcceptedQuicConn {
    connection: compio_quic::Connection,
    streams: (compio_quic::SendStream, compio_quic::RecvStream),
}

impl AcceptedQuicConn {
    /// Bundle a freshly-accepted QUIC connection and its first
    /// bidirectional stream pair.
    #[must_use]
    pub const fn new(
        connection: compio_quic::Connection,
        streams: (compio_quic::SendStream, compio_quic::RecvStream),
    ) -> Self {
        Self {
            connection,
            streams,
        }
    }

    /// Unbundle into the underlying `compio_quic` types.
    ///
    /// `pub(crate)` by design: external callers receive an
    /// [`AcceptedQuicConn`] from [`AcceptedQuicClientFn`] and forward
    /// it straight to [`installer::install_client_quic`], which calls
    /// this helper internally. Keeping it crate-private holds
    /// `compio_quic`'s concrete types out of the bus's public `SemVer`
    /// surface so a `compio_quic` version bump does not constitute a
    /// `SemVer`-major change for `iggy_message_bus`.
    #[must_use]
    pub(crate) fn into_parts(
        self,
    ) -> (
        compio_quic::Connection,
        (compio_quic::SendStream, compio_quic::RecvStream),
    ) {
        (self.connection, self.streams)
    }
}

/// Callback invoked on every accepted SDK QUIC client connection.
///
/// Fires after shard 0's QUIC listener drives the handshake to
/// completion AND accepts the first bidirectional stream pair, so the
/// callback receives a ready-for-traffic [`AcceptedQuicConn`]. The
/// callback mints a client id and forwards the conn straight into
/// [`installer::install_client_quic`] on the local bus, which unwraps
/// internally; no caller-side `into_parts` is needed (and the helper
/// is `pub(crate)` for that reason).
///
/// QUIC stays shard-0 terminal: shard 0 owns the
/// `compio_quic::Endpoint`, which demuxes incoming UDP packets to
/// in-flight connections by Connection ID, and `quinn-proto`
/// per-connection TLS / packet-number / congestion state is not
/// serialisable. No cross-shard handover analog exists for this plane.
pub type AcceptedQuicClientFn = std::rc::Rc<dyn Fn(AcceptedQuicConn)>;

/// Callback invoked on every accepted SDK WebSocket client connection.
///
/// Fires after shard 0's WS listener accepts a raw TCP socket. The
/// HTTP-Upgrade handshake has NOT run yet; the callback hands the
/// raw stream off to the owning shard via inter-shard fd-shipping
/// (`ShardFramePayload::ClientWsConnectionSetup`). The owning shard
/// runs the upgrade locally; no subprotocol is negotiated. The
/// shipped fd is plain TCP at ship-time, so fd-delegation (which
/// requires a dupable plaintext fd) stays well-defined.
pub type AcceptedWsClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

/// Callback invoked on every accepted SDK TCP-TLS client connection.
///
/// Fires after shard 0's TCP-TLS listener accepts a raw TCP socket.
/// Neither the rustls handshake nor any application-layer work has run
/// yet — the listener stays cheap so a slow handshake on one peer cannot
/// block subsequent accepts. The callback receives the raw stream plus
/// a clone of the shared [`std::sync::Arc<rustls::ServerConfig>`] built
/// at bind time, mints a `client_id`, and calls
/// [`installer::install_client_tcp_tls`]; the install path drives the
/// rustls handshake on its own task before forwarding the connection to
/// [`installer::install_client_conn`].
///
/// TCP-TLS stays shard-0 terminal: rustls's connection state machine
/// is tied to the local task and not serialisable, and the
/// pre-handshake fd would have to re-handshake on the receiving shard,
/// losing the point of fd-delegation.
pub type AcceptedTlsClientFn =
    std::rc::Rc<dyn Fn(compio::net::TcpStream, std::sync::Arc<rustls::ServerConfig>)>;

/// Callback invoked on every accepted SDK WSS (WebSocket-over-TLS)
/// client connection.
///
/// Fires after shard 0's WSS listener accepts a raw TCP socket. Neither
/// the rustls handshake nor the WebSocket HTTP-Upgrade has run yet — the
/// listener stays cheap so neither handshake on one peer can block
/// subsequent accepts. The callback receives the raw stream plus a clone
/// of the shared [`std::sync::Arc<rustls::ServerConfig>`] built at bind
/// time, mints a `client_id`, and calls
/// [`installer::install_client_wss`]; the install path drives
/// both handshakes on its own task before forwarding the connection to
/// [`installer::install_client_conn`].
///
/// No subprotocol negotiation is performed. WSS stays shard-0 terminal
/// for the same reasons as the TCP-TLS plane.
pub type AcceptedWssClientFn =
    std::rc::Rc<dyn Fn(compio::net::TcpStream, std::sync::Arc<rustls::ServerConfig>)>;

/// Notifier fired when a delegated replica connection dies.
///
/// The delegated replica connection's writer / reader tasks invoke this on
/// abnormal exit (peer closed, write failed). The shard bootstrap wraps
/// this closure around a `try_send` into shard 0's inbox so shard 0 can
/// clear the replica mapping and re-dial.
pub type ConnectionLostFn = std::rc::Rc<dyn Fn(u8)>;

/// Point-to-point message delivery between consensus participants.
///
/// `Ok(())` means "accepted for delivery" - NOT "delivered to peer".
/// The bus never retries. Consensus owns retransmission via the WAL
/// (Prepare) or VSR timeouts (view change).
///
/// The implementation is fire-and-forget: `send_to_*` enqueues the message
/// to a per-peer bounded mpsc and returns immediately. A dedicated writer
/// task per connection drains the queue and writes batched frames in a
/// single `writev` syscall. A slow peer cannot stall sends to other peers.
///
/// # Yield semantics
///
/// For the production `IggyMessageBus` impl, `send_to_*` has zero `.await`
/// points in its body; the `async fn` shell exists solely for trait
/// compatibility with simulator implementations. Callers can assume the
/// send completes synchronously and the returned future is always `Ready`
/// on first poll. Future transports (QUIC, TLS) that would introduce real
/// yields must advertise that change in their own doc; consensus code
/// relies on the no-yield property to reason about reentrancy.
pub trait MessageBus {
    fn send_to_client(
        &self,
        client_id: u128,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>>;

    fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>>;

    /// Install a notifier the bus will invoke when a delegated replica
    /// connection dies abnormally. Used by shard-0 bootstrap to push a
    /// `ShardFramePayload::ConnectionLost` into shard 0's inbox so the
    /// coordinator can forget the replica's mapping.
    ///
    /// Default impl is a no-op for buses that do not delegate real
    /// connections (simulator, test doubles); the production
    /// `IggyMessageBus` overrides it to wire into its internal
    /// `connection_lost_fn` hook.
    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
}

/// Production message bus backed by real TCP connections.
///
/// Owns:
/// - a root [`Shutdown`] / [`ShutdownToken`] that fans cancellation to every
///   accept loop, read loop, periodic task, and per-connection writer task,
/// - a [`ConnectionRegistry<u128>`] for clients (`u128` id is shard-packed),
/// - a [`ReplicaRegistry`] for replicas (`u8` id from the Ping handshake,
///   backed by a fixed-size array to avoid hash lookup on every send),
/// - the `JoinHandle`s of background tasks (accept loops, reconnect periodic).
///
/// Send semantics:
/// - `send_to_*` clones the per-peer `Sender` out of the registry, calls
///   `try_send` on it, and returns. No `.await` happens in the body. Drops
///   on `Full` (returned as [`SendError::Backpressure`]) are recovered by
///   VSR retransmission.
/// - The per-connection writer task batches up to `config.max_batch` messages into
///   a single `writev` syscall on the plaintext TCP plane (see
///   [`transports::tcp`]).
///
/// Interior mutability via `RefCell` / `Cell` is sound because compio is a
/// single-threaded runtime: no other task can execute while we hold a borrow.
pub struct IggyMessageBus {
    shard_id: u16,
    shutdown: Shutdown,
    token: ShutdownToken,
    clients: ConnectionRegistry<u128>,
    replicas: ReplicaRegistry,
    background_tasks: RefCell<Vec<JoinHandle<()>>>,
    config: MessageBusConfig,
    /// For each replica, the shard that owns the TCP connection from this
    /// shard's perspective. Present on ALL shards (owning and non-owning).
    /// On the owning shard, `shard_mapping[replica] == self.shard_id`.
    shard_mapping: RefCell<HashMap<u8, u16>>,
    /// Forwards a replica-bound message to the shard that owns the replica's
    /// TCP connection. `None` on single-shard setups and tests.
    replica_forward_fn: RefCell<Option<ShardForwardFn>>,
    /// Forwards a client-bound message to the shard that owns the client's
    /// TCP connection (the owning shard is encoded in the top 16 bits of
    /// the client id). `None` on single-shard setups and tests.
    client_forward_fn: RefCell<Option<ShardForwardFn>>,
    /// Invoked by a delegated replica connection's writer / reader tasks
    /// when they exit abnormally. `None` when running without a shard-0
    /// coordinator (single-shard deployments and tests).
    connection_lost_fn: RefCell<Option<ConnectionLostFn>>,
    /// Per-connection metadata exposed to the caller via
    /// [`Self::client_meta`]. Populated by the install path on
    /// successful registry insert; removed on connection teardown.
    client_meta: RefCell<ahash::AHashMap<u128, Rc<ClientConnMeta>>>,
}

impl IggyMessageBus {
    /// Construct a bus with default tunables (derived from
    /// [`ServerNgConfig::default`]).
    #[must_use]
    pub fn new(shard_id: u16) -> Self {
        Self::with_tunables(shard_id, MessageBusConfig::default())
    }

    /// Construct a bus with a custom per-peer queue capacity; all other
    /// tunables fall back to defaults.
    ///
    /// Tuning knob for tests and benchmarks. Production should use
    /// [`with_config`](Self::with_config).
    #[must_use]
    pub fn with_capacity(shard_id: u16, peer_queue_capacity: usize) -> Self {
        let cfg = MessageBusConfig {
            peer_queue_capacity,
            ..MessageBusConfig::default()
        };
        Self::with_tunables(shard_id, cfg)
    }

    /// Construct a bus from the validated server-ng schema.
    ///
    /// Production constructor: takes a fully-validated
    /// [`ServerNgConfig`] and derives the runtime [`MessageBusConfig`]
    /// internally. Field conversions ([`iggy_common::IggyDuration`] -> [`Duration`],
    /// [`iggy_common::IggyByteSize`] -> `usize`, schema WS knobs ->
    /// tungstenite [`WebSocketConfig`]) happen once here so hot paths
    /// read pre-converted values.
    ///
    /// # Panics
    ///
    /// Panics if `cfg.message_bus.max_batch == 0` or
    /// `cfg.message_bus.max_batch > IOV_MAX_LIMIT`. Boot-time validation;
    /// surfaces operator misconfiguration loudly rather than letting
    /// every `writev` fail silently with `EMSGSIZE` once traffic starts.
    #[must_use]
    pub fn with_config(shard_id: u16, cfg: &ServerNgConfig) -> Self {
        Self::with_tunables(shard_id, MessageBusConfig::from(cfg))
    }

    /// Construct a bus from already-derived runtime tunables.
    ///
    /// Used by the public constructors above and by tests that need to
    /// patch a single field on the derived [`MessageBusConfig`] without
    /// round-tripping through [`ServerNgConfig`].
    ///
    /// # Panics
    ///
    /// Panics if `config.max_batch == 0` or
    /// `config.max_batch > IOV_MAX_LIMIT`.
    #[must_use]
    pub fn with_tunables(shard_id: u16, config: MessageBusConfig) -> Self {
        assert!(
            config.max_batch > 0 && config.max_batch <= IOV_MAX_LIMIT,
            "MessageBusConfig::max_batch must be in 1..={IOV_MAX_LIMIT} (writev IOV_MAX/2 cap); got {}",
            config.max_batch,
        );
        let (shutdown, token) = Shutdown::new();
        Self {
            shard_id,
            shutdown,
            token,
            clients: ConnectionRegistry::new(),
            replicas: ReplicaRegistry::new(),
            background_tasks: RefCell::new(Vec::new()),
            config,
            shard_mapping: RefCell::new(HashMap::new()),
            replica_forward_fn: RefCell::new(None),
            client_forward_fn: RefCell::new(None),
            connection_lost_fn: RefCell::new(None),
            client_meta: RefCell::new(ahash::AHashMap::new()),
        }
    }

    /// Look up the per-connection metadata recorded for `client_id`.
    ///
    /// Returns `None` if the client never connected on this bus or if
    /// its connection has already been torn down.
    #[must_use]
    pub fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>> {
        self.client_meta.borrow().get(&client_id).map(Rc::clone)
    }

    pub(crate) fn insert_client_meta(&self, meta: Rc<ClientConnMeta>) {
        self.client_meta.borrow_mut().insert(meta.client_id, meta);
    }

    pub(crate) fn remove_client_meta(&self, client_id: u128) {
        self.client_meta.borrow_mut().remove(&client_id);
    }

    /// Install the notifier used by delegated replica connections to tell
    /// shard 0 that a connection died. Single place to inject in tests too.
    pub fn set_connection_lost_fn(&self, f: ConnectionLostFn) {
        *self.connection_lost_fn.borrow_mut() = Some(f);
    }

    /// Invoke the registered connection-lost notifier, if any.
    ///
    /// Clones the `Rc` out of the `RefCell` borrow before invoking the
    /// closure so the closure body is free to call
    /// [`Self::set_connection_lost_fn`] (which takes a `borrow_mut`)
    /// without tripping the runtime borrow check.
    pub(crate) fn notify_connection_lost(&self, replica_id: u8) {
        let cb = self
            .connection_lost_fn
            .borrow()
            .as_ref()
            .map(std::rc::Rc::clone);
        if let Some(f) = cb {
            f(replica_id);
        }
    }

    /// Install the replica-plane inter-shard forward closure.
    ///
    /// Non-owning shards invoke this from `send_to_replica`'s slow path to
    /// push the message into the owning shard's inbox. The owning shard's
    /// router then re-enters `send_to_replica` on the local bus (fast path).
    ///
    /// Takes `&self` so it can be called through an `Rc<IggyMessageBus>`
    /// wrapper after the bus is shared with accept loops and periodic tasks.
    ///
    /// # Panics
    ///
    /// Panics on re-entrant `RefCell::borrow_mut` if called from inside
    /// an in-flight forward-closure invocation. All production call sites
    /// are bootstrap only (single-threaded compio, no re-entry).
    pub fn set_replica_forward_fn(&self, f: ShardForwardFn) {
        *self.replica_forward_fn.borrow_mut() = Some(f);
    }

    /// Install the client-plane inter-shard forward closure.
    ///
    /// Shards invoke this from `send_to_client`'s slow path when the client
    /// connection lives on a different shard (top 16 bits of `client_id`).
    ///
    /// Takes `&self` so it can be called through an `Rc<IggyMessageBus>`
    /// wrapper. Call sites are bootstrap only.
    ///
    /// # Panics
    ///
    /// Panics on re-entrant `RefCell::borrow_mut` if called from inside
    /// an in-flight forward-closure invocation.
    pub fn set_client_forward_fn(&self, f: ShardForwardFn) {
        *self.client_forward_fn.borrow_mut() = Some(f);
    }

    /// Update the shard mapping for a replica.
    ///
    /// Called when the allocation strategy assigns or reassigns connections.
    ///
    /// # Panics
    ///
    /// Panics on re-entrant `RefCell::borrow_mut` if called while
    /// [`IggyMessageBus::owning_shard`] (or any other read-side of
    /// `shard_mapping`) holds an outstanding read borrow on the same
    /// bus instance.
    pub fn set_shard_mapping(&self, replica: u8, owning_shard: u16) {
        self.shard_mapping
            .borrow_mut()
            .insert(replica, owning_shard);
    }

    /// Remove all shard mappings for a replica (on deallocation).
    ///
    /// # Panics
    ///
    /// Same re-entrant-`borrow_mut` constraint as
    /// [`IggyMessageBus::set_shard_mapping`].
    pub fn remove_shard_mapping(&self, replica: u8) {
        self.shard_mapping.borrow_mut().remove(&replica);
    }

    /// Get the owning shard for a replica, if mapped.
    pub fn owning_shard(&self, replica: u8) -> Option<u16> {
        self.shard_mapping.borrow().get(&replica).copied()
    }

    #[must_use]
    pub const fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Per-peer mpsc capacity used when registering new connections.
    #[must_use]
    pub const fn peer_queue_capacity(&self) -> usize {
        self.config.peer_queue_capacity
    }

    /// Runtime tunables in effect on this bus.
    #[must_use]
    pub const fn config(&self) -> &MessageBusConfig {
        &self.config
    }

    /// Cheap clone of the root shutdown token.
    ///
    /// Handed to accept loops, read tasks, writer tasks, and periodic tasks
    /// so they can `select!` on cancellation.
    #[must_use]
    pub fn token(&self) -> ShutdownToken {
        self.token.clone()
    }

    /// Whether [`shutdown`](Self::shutdown) has been called.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_triggered()
    }

    /// Accessor used by the client listener to insert / remove connections
    /// and by `send_to_client` to look up senders.
    #[must_use]
    pub const fn clients(&self) -> &ConnectionRegistry<u128> {
        &self.clients
    }

    /// Accessor used by the replica listener and connector to insert /
    /// remove peer connections and by `send_to_replica` to look up senders.
    #[must_use]
    pub const fn replicas(&self) -> &ReplicaRegistry {
        &self.replicas
    }

    /// Register a background task (accept loop, reconnect periodic) so
    /// [`shutdown`](Self::shutdown) can await it.
    ///
    /// Reaps already-finished handles before pushing the new one so the
    /// vec stays bounded under sustained traffic. Without the reap a
    /// long-running bus accumulates one handle per spawn site over its
    /// lifetime (the most visible source today is the per-WS-connect
    /// upgrade task in `installer::install_client_ws_fd`). Dropping a
    /// finished `compio::runtime::JoinHandle` is a no-op (the task has
    /// already completed); compio's runtime is single-threaded so
    /// `is_finished` cannot flip between the predicate evaluation and
    /// the drop inside the same `retain`.
    ///
    /// The tracking vec grows during shutdown too. `shutdown` drains it
    /// in a loop until empty, so a task pushed mid-shutdown is still
    /// awaited.
    pub fn track_background(&self, handle: JoinHandle<()>) {
        let mut tasks = self.background_tasks.borrow_mut();
        tasks.retain(|h| !h.is_finished());
        tasks.push(handle);
    }

    /// Trigger the root shutdown and drain everything with the given
    /// deadline.
    ///
    /// Order:
    /// 1. Trigger the root shutdown token (every accept loop, read loop,
    ///    writer loop, and periodic task selecting on it observes the
    ///    cancellation and exits).
    /// 2. Drain the client registry (closes each per-peer `Sender` then
    ///    awaits both writer + reader handles).
    /// 3. Drain the replica registry.
    /// 4. Loop-drain every tracked background task. Tasks pushed
    ///    mid-shutdown (e.g. a reader that observed the token and
    ///    spawned its own cleanup) are picked up on the next iteration.
    ///
    /// Connections drain before background tasks so that writer tasks
    /// get the full remaining budget for `write_vectored_all` before any
    /// accept / reconnect / refresh periodic consumes it. Background
    /// tasks hold no in-flight wire frames, so force-cancelling them
    /// cannot truncate a frame on the wire.
    #[allow(clippy::future_not_send)]
    pub async fn shutdown(&self, timeout: Duration) -> DrainOutcome {
        self.shutdown.trigger();

        let deadline = std::time::Instant::now() + timeout;

        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let clients_outcome = self.clients.drain(remaining).await;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let replicas_outcome = self.replicas.drain(remaining).await;

        let mut background_clean = 0usize;
        let mut background_force = 0usize;
        loop {
            let batch: Vec<JoinHandle<()>> = self.background_tasks.borrow_mut().drain(..).collect();
            if batch.is_empty() {
                break;
            }
            for handle in batch {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    drop(handle);
                    background_force += 1;
                    continue;
                }
                match compio::time::timeout(remaining, handle).await {
                    Ok(_) => background_clean += 1,
                    Err(_) => background_force += 1,
                }
            }
        }

        DrainOutcome {
            clean: clients_outcome.clean + replicas_outcome.clean,
            force: clients_outcome.force + replicas_outcome.force,
            background_clean,
            background_force,
        }
    }
}

/// Forwarding impl so `VsrConsensus<Rc<IggyMessageBus>>` (two consensus
/// planes sharing one bus) type-checks without duplicating the bus or
/// taking it by value.
#[allow(clippy::future_not_send)]
impl<T: MessageBus + ?Sized> MessageBus for std::rc::Rc<T> {
    fn send_to_client(
        &self,
        client_id: u128,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>> {
        (**self).send_to_client(client_id, data)
    }

    fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>> {
        (**self).send_to_replica(replica, data)
    }

    fn set_connection_lost_fn(&self, f: ConnectionLostFn) {
        (**self).set_connection_lost_fn(f);
    }
}

#[allow(clippy::future_not_send)]
impl MessageBus for IggyMessageBus {
    async fn send_to_client(
        &self,
        client_id: u128,
        message: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if self.is_shutting_down() {
            return Err(SendError::BusShuttingDown);
        }
        // Owning shard is encoded in the top 16 bits of client_id.
        let owning_shard = client_id_owning_shard(client_id);
        if owning_shard == self.shard_id {
            // Fast path: move `message` straight into `try_send`. On no-slot
            // the registry hands it back; we drop it and surface
            // ClientNotFound (matches prior behaviour: SendError did not
            // preserve payload either).
            return match self.clients.try_send_or_return(client_id, message) {
                Ok(send_result) => send_result.map_err(map_try_send_err),
                Err(_msg) => Err(SendError::ClientNotFound(client_id)),
            };
        }
        let forward = self.client_forward_fn.borrow();
        let forward = forward
            .as_ref()
            .ok_or(SendError::ClientRouteMissing(client_id))?;
        forward(owning_shard, message).map_err(|_| SendError::ClientForwardFailed(client_id))
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        message: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if self.is_shutting_down() {
            return Err(SendError::BusShuttingDown);
        }
        // Fast path: this shard owns a connection to the replica. On no-slot
        // the registry returns the message unchanged so the slow path can
        // forward it via the inter-shard channel without a wasted clone.
        let message = match self.replicas.try_send_or_return(replica, message) {
            Ok(send_result) => return send_result.map_err(map_try_send_err),
            Err(message) => message,
        };
        // Slow path: route via the inter-shard channel to the owning shard.
        let owning_shard = self
            .shard_mapping
            .borrow()
            .get(&replica)
            .copied()
            .ok_or(SendError::ReplicaNotConnected(replica))?;
        let forward = self.replica_forward_fn.borrow();
        let forward = forward
            .as_ref()
            .ok_or(SendError::ReplicaRouteMissing(replica))?;
        forward(owning_shard, message).map_err(|_| SendError::ReplicaForwardFailed(replica))
    }

    fn set_connection_lost_fn(&self, f: ConnectionLostFn) {
        Self::set_connection_lost_fn(self, f);
    }
}

/// Extract the owning shard from a client id.
///
/// Shard 0 mints client ids as `(target_shard_id << 112) | seq`. The top 16
/// bits encode which shard's bus registry holds the connection; any shard
/// that needs to reply to this client uses this accessor to decide between
/// the fast path (local) and the slow path (forward via inter-shard).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub const fn client_id_owning_shard(client_id: u128) -> u16 {
    (client_id >> 112) as u16
}

/// Map an `async_channel::TrySendError` onto the bus-level [`SendError`].
///
/// Shape-matches `Result::map_err` (takes the error by value) so it can be
/// used directly as a function reference rather than a closure.
#[allow(clippy::needless_pass_by_value)] // signature required by map_err
fn map_try_send_err(e: async_channel::TrySendError<Frozen<MESSAGE_ALIGN>>) -> SendError {
    match e {
        async_channel::TrySendError::Full(_) => SendError::Backpressure,
        async_channel::TrySendError::Closed(_) => SendError::ConnectionClosed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, HEADER_SIZE};
    use std::cell::RefCell;

    #[allow(clippy::cast_possible_truncation)]
    fn dummy_message() -> Message<GenericHeader> {
        Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
            h.command = Command2::Prepare;
            h.size = HEADER_SIZE as u32;
        })
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_slow_path_forwards_to_owning_shard() {
        // Bus on shard 5; client id encodes owning shard = 7.
        let bus = IggyMessageBus::new(5);
        let captured: std::rc::Rc<RefCell<Option<u16>>> = std::rc::Rc::new(RefCell::new(None));
        let captured_clone = captured.clone();
        bus.set_client_forward_fn(Box::new(move |target, _msg| {
            *captured_clone.borrow_mut() = Some(target);
            Ok(())
        }));

        let client_id = (7u128 << 112) | 0x2a;
        bus.send_to_client(client_id, dummy_message().into_frozen())
            .await
            .expect("forward_fn should accept");
        assert_eq!(*captured.borrow(), Some(7));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_fast_path_hits_local_when_owning_shard_matches() {
        // shard_id == top-16-bits => fast path, registry miss => ClientNotFound
        let bus = IggyMessageBus::new(3);
        let client_id = (3u128 << 112) | 1;
        let err = bus
            .send_to_client(client_id, dummy_message().into_frozen())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::ClientNotFound(_)));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_returns_route_missing_when_remote_and_no_forward_fn() {
        // shard_id != top-16-bits and no forward_fn installed.
        let bus = IggyMessageBus::new(0);
        let client_id = (9u128 << 112) | 1;
        let err = bus
            .send_to_client(client_id, dummy_message().into_frozen())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::ClientRouteMissing(_)));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_replica_slow_path_uses_shard_mapping_and_forwards() {
        let bus = IggyMessageBus::new(0);
        let captured: std::rc::Rc<RefCell<Option<u16>>> = std::rc::Rc::new(RefCell::new(None));
        let captured_clone = captured.clone();
        bus.set_replica_forward_fn(Box::new(move |target, _msg| {
            *captured_clone.borrow_mut() = Some(target);
            Ok(())
        }));
        bus.set_shard_mapping(5, 3);

        bus.send_to_replica(5, dummy_message().into_frozen())
            .await
            .expect("forward ok");

        assert_eq!(*captured.borrow(), Some(3));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_replica_no_mapping_returns_not_connected() {
        let bus = IggyMessageBus::new(0);
        let err = bus
            .send_to_replica(9, dummy_message().into_frozen())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::ReplicaNotConnected(9)));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn client_id_owning_shard_extracts_top_16_bits() {
        assert_eq!(client_id_owning_shard((7u128 << 112) | 0x2a), 7);
        assert_eq!(client_id_owning_shard(0), 0);
        assert_eq!(
            client_id_owning_shard((u128::from(u16::MAX)) << 112),
            u16::MAX
        );
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn track_background_queues_handles() {
        let bus = IggyMessageBus::new(0);

        let h1 = compio::runtime::spawn(async {});
        let h2 = compio::runtime::spawn(async {});
        bus.track_background(h1);
        bus.track_background(h2);
        assert_eq!(bus.background_tasks.borrow().len(), 2);
    }

    #[test]
    #[should_panic(expected = "MessageBusConfig::max_batch must be in")]
    fn max_batch_oversize_rejected() {
        let cfg = MessageBusConfig {
            max_batch: 4096,
            ..MessageBusConfig::default()
        };
        let _ = IggyMessageBus::with_tunables(0, cfg);
    }

    #[test]
    #[should_panic(expected = "MessageBusConfig::max_batch must be in")]
    fn max_batch_zero_rejected() {
        let cfg = MessageBusConfig {
            max_batch: 0,
            ..MessageBusConfig::default()
        };
        let _ = IggyMessageBus::with_tunables(0, cfg);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn notify_connection_lost_handles_reentrant_install() {
        // Closure swaps itself out via `set_connection_lost_fn`, which
        // calls `borrow_mut`. The pre-fix code held a `Ref` across the
        // closure invocation and panicked here.
        let bus = std::rc::Rc::new(IggyMessageBus::new(0));
        let bus_for_closure = bus.clone();
        let counter: std::rc::Rc<std::cell::Cell<u8>> = std::rc::Rc::new(std::cell::Cell::new(0));
        let counter_inner = counter.clone();
        let cb: ConnectionLostFn = std::rc::Rc::new(move |_replica: u8| {
            counter_inner.set(counter_inner.get() + 1);
            // Reentrant install: replace the closure mid-invoke.
            let counter_replacement = counter_inner.clone();
            bus_for_closure.set_connection_lost_fn(std::rc::Rc::new(move |_| {
                counter_replacement.set(counter_replacement.get() + 10);
            }));
        });
        bus.set_connection_lost_fn(cb);

        bus.notify_connection_lost(1); // first closure runs (+1) and swaps
        assert_eq!(counter.get(), 1);

        bus.notify_connection_lost(1); // second closure runs (+10)
        assert_eq!(counter.get(), 11);
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn shutdown_loop_drains_tasks_added_during_shutdown() {
        // Models the real race: a background task spawned after shutdown
        // has been triggered but before the loop-drain catches it up. The
        // loop must pick the newer handle up on its next iteration.
        let bus = IggyMessageBus::new(0);

        let h1 = compio::runtime::spawn(async {});
        bus.track_background(h1);

        // Simulate mid-shutdown push: trigger, then push a fresh handle
        // via the public API (imitating a reader task that observed the
        // token and registered a cleanup future).
        bus.shutdown.trigger();
        let h2 = compio::runtime::spawn(async {});
        bus.track_background(h2);

        let outcome = bus.shutdown(Duration::from_secs(2)).await;
        assert_eq!(outcome.background_clean, 2);
        assert_eq!(outcome.background_force, 0);
        assert_eq!(
            bus.background_tasks.borrow().len(),
            0,
            "shutdown must leave background_tasks empty",
        );
    }
}
