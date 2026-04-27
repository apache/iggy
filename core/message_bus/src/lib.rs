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
//!   replicas. Implemented in [`replica_listener`], [`connector`], and
//!   [`replica_io`]. Datagram or gateway-terminated transports are NOT
//!   supported here and never will be — see
//!   `replica_listener`'s module docs for the rationale.
//! - **SDK-client plane**: ephemeral client connections. TCP today via
//!   [`client_listener`]; Phase 2+ adds WebSocket / QUIC on a
//!   demand-gated basis. At-most-once semantics come from the
//!   `(client: u128, request: u64)` pair in `RequestHeader`; the dedup
//!   store lives in `core/server-ng/src/dedup.rs` on each shard.
//!
//! # Authenticated handshake
//!
//! Both planes use [`auth::TokenSource`] for identity. The replica
//! plane ships with [`auth::StaticSharedSecret`] (all replicas share
//! one 32-byte key); the client plane will grow per-client token
//! sources in Phase 2+. The envelope lives in
//! `GenericHeader.reserved_command[0..57]` — 32 B BLAKE3 MAC, 8 B
//! timestamp, 16 B nonce, 1 B kind — so the 256 B header layout is
//! unchanged.
//!
//! Each plane prepends a distinct 16-byte LABEL into the MAC input
//! (`auth::LABEL_REPLICA` for the consensus plane;
//! `auth::LABEL_CLIENT` reserved for Phase 2). The label is part of
//! the cryptographic challenge, so a captured handshake on one plane
//! cannot be replayed onto the other even if both share one
//! `StaticSharedSecret`.
//!
//! # Invariants worth naming
//!
//! - [`send_to_client`](IggyMessageBus::send_to_client) and
//!   [`send_to_replica`](IggyMessageBus::send_to_replica) return
//!   `Ready` on first poll. Consensus code relies on this for
//!   reentrancy reasoning; any `.await` in the body breaks it.
//! - [`writer_task`] coalesces up to `max_batch = 256`
//!   `Frozen<MESSAGE_ALIGN>` into one `write_vectored_all`. Don't
//!   introduce per-message syscalls or per-message encryption.
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
//! behind: [`transports::TransportListener`],
//! [`transports::TransportConn`], [`transports::TransportReader`],
//! [`transports::TransportWriter`]. TCP is the only production impl
//! (see [`transports::tcp`]); [`installer::install_replica_conn`] /
//! [`installer::install_client_conn`] and
//! [`writer_task::run_transport`] are generic over it so WS / QUIC
//! plug in behind the same registry, fencing, and batching logic.
//!
//! The full invariant list and the transport-plan design notes live
//! under `Documents/silverhand/iggy/message_bus/transport-plan/`.

pub mod auth;
pub mod auth_config;
pub mod cache;
pub mod client_listener;
pub mod client_listener_quic;
pub mod config;
pub mod connector;
mod error;
pub mod fd_transfer;
pub mod framing;
pub mod installer;
pub mod lifecycle;
pub mod replica_io;
pub mod replica_listener;
pub(crate) mod socket_opts;
pub mod transports;
pub mod writer_task;

pub use config::{IOV_MAX_LIMIT, MessageBusConfig};
pub use error::SendError;
pub use installer::ConnectionInstaller;
pub use lifecycle::{
    BusMessage, BusReceiver, BusSender, ConnectionRegistry, DrainOutcome, RejectedRegistration,
    ReplicaRegistry, Shutdown, ShutdownToken,
};

use auth::ReplicaNonceRing;
use compio::runtime::JoinHandle;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, Message};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

/// Per-replica nonce dedup store, owned by [`IggyMessageBus`] and
/// shared with the listener task via [`Rc`]. Keyed by `replica_id` so
/// each peer's ring is isolated; a flapping or hostile peer cannot
/// evict honest peers' nonce slots. Bus-resident (rather than
/// task-local on the listener) so the nonce window survives a listener
/// task respawn within one process. Process restart still drops the
/// store; cross-process persistence is out of scope.
///
/// Type is `pub` (rather than `pub(crate)`) only because it appears in
/// the public signature of [`replica_listener::run`]; downstream code
/// should obtain a clone via [`IggyMessageBus::replica_nonces`] rather
/// than constructing one directly.
#[doc(hidden)]
pub type ReplicaNonceStore = RefCell<HashMap<u8, ReplicaNonceRing>>;

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
/// `TcpStream`; the replica id has already been validated against the
/// cluster config.
pub type AcceptedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, u8)>;

/// Callback invoked on every accepted SDK client connection.
///
/// Takes ownership of the accepted stream and is responsible for minting /
/// assigning the client id as part of its delegation policy.
pub type AcceptedClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

/// Callback invoked on every accepted SDK QUIC client connection.
///
/// Fires after shard 0's QUIC listener drives the handshake to
/// completion AND accepts the first bidirectional stream pair, so the
/// callback receives a ready-for-traffic [`compio_quic::Connection`]
/// plus its `(SendStream, RecvStream)` pair. The callback mints a
/// client id, then calls [`installer::install_client_quic_conn`] on
/// the local bus.
///
/// QUIC stays shard-0 terminal (per `INVARIANTS.md` I6 and the
/// non-serialisable nature of `quinn-proto::Connection` state); no
/// cross-shard handover analog exists for this plane.
pub type AcceptedQuicClientFn = std::rc::Rc<
    dyn Fn(compio_quic::Connection, (compio_quic::SendStream, compio_quic::RecvStream)),
>;

/// Callback invoked on every accepted SDK WebSocket client connection.
///
/// Fires after shard 0's WS listener accepts a raw TCP socket. The
/// HTTP-Upgrade handshake has NOT run yet; the callback hands the
/// raw stream off to the owning shard via inter-shard fd-shipping
/// (`ShardFramePayload::ClientWsConnectionSetup`). The owning shard
/// runs the upgrade and the subprotocol check locally. This shape
/// preserves invariant I5 (fd-delegation is TCP-only) because the
/// shipped fd is plain TCP at ship-time.
pub type AcceptedWsClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

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
/// - the `JoinHandle`s of background tasks (accept loops, reconnect periodic),
/// - a per-peer [`ReplicaNonceStore`] used by the replica-plane handshake
///   to dedup nonces inside the 30 s timestamp window (see
///   [`Self::replica_nonces`]).
///
/// Send semantics:
/// - `send_to_*` clones the per-peer `Sender` out of the registry, calls
///   `try_send` on it, and returns. No `.await` happens in the body. Drops
///   on `Full` (returned as [`SendError::Backpressure`]) are recovered by
///   VSR retransmission.
/// - The per-connection writer task batches up to `config.max_batch` messages into
///   a single `writev` syscall via [`writer_task::run`].
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
    /// Per-peer replica handshake nonce dedup store. Held in an `Rc` so
    /// the listener task can take a clone across the spawn boundary
    /// without stealing the bus's only handle. See [`ReplicaNonceStore`].
    replica_nonces: Rc<ReplicaNonceStore>,
}

impl IggyMessageBus {
    /// Construct a bus with [`MessageBusConfig::default`] tunables.
    #[must_use]
    pub fn new(shard_id: u16) -> Self {
        Self::with_config(shard_id, MessageBusConfig::default())
    }

    /// Construct a bus with a custom per-peer queue capacity; all other
    /// tunables fall back to [`MessageBusConfig::default`].
    ///
    /// Tuning knob for tests and benchmarks. Production should use
    /// [`with_config`](Self::with_config).
    #[must_use]
    pub fn with_capacity(shard_id: u16, peer_queue_capacity: usize) -> Self {
        let cfg = MessageBusConfig {
            peer_queue_capacity,
            ..MessageBusConfig::default()
        };
        Self::with_config(shard_id, cfg)
    }

    /// Construct a bus with the given [`MessageBusConfig`].
    ///
    /// # Panics
    ///
    /// Panics if `config.max_batch == 0` or
    /// `config.max_batch > IOV_MAX_LIMIT`. Boot-time validation; surfaces
    /// operator misconfiguration loudly rather than letting every writev
    /// fail silently with `EMSGSIZE` once traffic starts.
    #[must_use]
    pub fn with_config(shard_id: u16, config: MessageBusConfig) -> Self {
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
            replica_nonces: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Cheap clone of the bus-resident replica-plane nonce store.
    ///
    /// The listener task takes one clone at boot and reaches into it
    /// from `handshake_verify`; the `Rc` keeps the store alive across
    /// the listener task's lifetime and survives a respawn within one
    /// process. Tests that drive `replica_listener::run` directly use
    /// this accessor to obtain the same store the production bootstrap
    /// would.
    #[doc(hidden)]
    #[must_use]
    pub fn replica_nonces(&self) -> Rc<ReplicaNonceStore> {
        Rc::clone(&self.replica_nonces)
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
    /// The tracking vec grows during shutdown too. `shutdown` drains it in
    /// a loop until empty, so a task pushed mid-shutdown is still awaited.
    pub fn track_background(&self, handle: JoinHandle<()>) {
        self.background_tasks.borrow_mut().push(handle);
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
            if let Some(result) = self
                .clients
                .with_sender(client_id, |s| s.try_send(message.clone()))
            {
                return result.map_err(map_try_send_err);
            }
            return Err(SendError::ClientNotFound(client_id));
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
        // Fast path: this shard owns a connection to the replica.
        if let Some(result) = self
            .replicas
            .with_sender(replica, |s| s.try_send(message.clone()))
        {
            return result.map_err(map_try_send_err);
        }
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

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn replica_nonce_store_isolates_peers() {
        // Per-peer rings: two peers replaying the same nonce must both
        // be admitted (different scopes), and a same-peer replay must
        // be rejected. Review F2.
        let bus = IggyMessageBus::new(0);
        let nonces = bus.replica_nonces();
        let mut store = nonces.borrow_mut();
        assert!(store.entry(1).or_default().insert(0xdead_beef));
        assert!(
            store.entry(2).or_default().insert(0xdead_beef),
            "different peer with same nonce must not be a replay"
        );
        assert!(
            !store.entry(1).or_default().insert(0xdead_beef),
            "same peer + same nonce is a replay"
        );
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn replica_nonce_store_survives_handle_drop() {
        // Listener task drop must NOT clear the nonce window; the store
        // is `Rc`-shared and the bus retains a clone. Proves the
        // restart-survival property the per-task `RefCell<NonceRing>`
        // could not deliver. Review F2.
        let bus = IggyMessageBus::new(0);
        let nonces1 = bus.replica_nonces();
        {
            let mut store = nonces1.borrow_mut();
            assert!(store.entry(1).or_default().insert(0xfeed_face));
        }
        drop(nonces1); // simulates listener task exit

        let nonces2 = bus.replica_nonces();
        let mut store = nonces2.borrow_mut();
        assert!(
            !store.entry(1).or_default().insert(0xfeed_face),
            "post-restart replay must be rejected"
        );
    }

    #[test]
    #[should_panic(expected = "MessageBusConfig::max_batch must be in")]
    fn max_batch_oversize_rejected() {
        let cfg = MessageBusConfig {
            max_batch: 4096,
            ..MessageBusConfig::default()
        };
        let _ = IggyMessageBus::with_config(0, cfg);
    }

    #[test]
    #[should_panic(expected = "MessageBusConfig::max_batch must be in")]
    fn max_batch_zero_rejected() {
        let cfg = MessageBusConfig {
            max_batch: 0,
            ..MessageBusConfig::default()
        };
        let _ = IggyMessageBus::with_config(0, cfg);
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
