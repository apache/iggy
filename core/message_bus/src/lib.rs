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
//!   inter-shard crossfire channel.
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
pub(crate) mod socket_opts;
pub mod transports;

pub use config::{IOV_MAX_LIMIT, MessageBusConfig, QuicTuning, WebSocketConfig};
pub use error::SendError;
pub use installer::ConnectionInstaller;
pub use installer::conn_info::{
    ClientConnMeta, ClientTransportKind, QuicConnectionInfo, TlsConnectionInfo, WsUpgradeInfo,
};
pub use lifecycle::{
    BusMessage, BusReceiver, BusSender, ConnectionRegistry, DrainOutcome, FusedShutdown,
    InstanceToken, ReplicaRegistry, ReplyRoute, ReplySlotError, ReplySlotGuard, Shutdown,
    ShutdownToken,
};
pub use transports::tls::TlsServerCredentials;

pub use compio::runtime::JoinHandle;
use configs::server_ng::ServerNgConfig;
use iggy_binary_protocol::{GenericHeader, ReplyHeader};
use server_common::{MESSAGE_ALIGN, Message, iobuf::Frozen};
use std::array;
use std::cell::{Cell, OnceCell, RefCell};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

/// Maximum number of replicas a single cluster supports. Replica ids are
/// `u8`, so the address space is 0..=255.
pub const MAX_REPLICAS: usize = 256;

/// Hard cap on concurrently in-flight inbound replica handshakes,
/// enforced globally on shard 0 (the sole acceptor) regardless of which
/// shard the delegated handshake runs on.
///
/// A legitimate dialer set is bounded by `self_id`
/// (< `replica_count` <= 255), so the cap only sheds load under a
/// hostile flood; it bounds fd / memory without ever stalling the
/// accept loop.
pub const MAX_INFLIGHT_REPLICA_HANDSHAKES: usize = 256;

/// Extra wall-clock allowance past `handshake_grace` before a shard-0
/// in-flight slot or pending-dial entry expires on its own. Covers the
/// inter-shard inbox latency of the delegation frame and the outcome
/// ack; expiry only matters when the ack is lost (full inbox or a
/// wedged owning shard), where it prevents a permanently leaked slot or
/// a permanently suppressed redial.
const REPLICA_HANDSHAKE_ACK_MARGIN: Duration = Duration::from_secs(5);

/// Sentinel stored in [`ReplicaOwnerTable`] slots that have no current owner.
///
/// `u16::MAX` is reserved by the server bootstrap so it can never be a real
/// shard id: `bootstrap` rejects any server whose shard count does not fit
/// in `u16` or is `>= OWNER_NONE`, returning `ShardsCountOverflow` rather
/// than minting a shard id that collides with this sentinel.
pub const OWNER_NONE: u16 = u16::MAX;

/// Shared atomic owner table mapping `replica_id` to `owning_shard_id`.
///
/// One Arc-cloned instance is allocated per server-ng process at
/// bootstrap and shared across every shard's [`IggyMessageBus`]. The owning shard
/// stamps its id into a slot when an inbound replica connection passes
/// the registry-insert race; the same shard CAS-clears the slot when
/// its connection dies (`notify_connection_lost`). Non-owning shards
/// only ever read.
///
/// Lock-free by construction: every operation is a single relaxed-or-
/// stronger atomic on a fixed `[AtomicU16; MAX_REPLICAS]`. Sized for
/// 256 slots upfront, so install / disconnect cost is `O(1)` and no
/// hashing or allocation happens on the hot path.
///
/// The table is the sole authority for cross-shard replica routing:
/// `send_to_replica`'s slow path and [`IggyMessageBus::owning_shard`]
/// both read it directly. No separate broadcast or reconcile loop is
/// involved.
pub struct ReplicaOwnerTable {
    slots: [AtomicU16; MAX_REPLICAS],
}

impl ReplicaOwnerTable {
    /// Build a fresh table with every slot initialised to [`OWNER_NONE`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            slots: array::from_fn(|_| AtomicU16::new(OWNER_NONE)),
        }
    }

    /// Try to claim `shard_id` as the current owner of `replica_id`.
    ///
    /// Returns `true` when the caller now owns the slot. Two paths
    /// resolve to `true`:
    /// * `compare_exchange(OWNER_NONE -> shard_id)` wins. Common case.
    /// * The CAS fails because the slot already stores `shard_id`. A
    ///   same-shard reclaim during the post-loop clear window
    ///   ([`IggyMessageBus::notify_connection_lost`]) is benign: the
    ///   slot already names us, and the stale post-loop will stand
    ///   down once it observes a live registry entry.
    ///
    /// Returns `false` only when a **different** shard owns the slot.
    /// In that case the caller must drop the inbound fd without
    /// spawning, mirroring the existing local-`replicas().contains`
    /// early-return at [`installer::install_replica_conn`].
    ///
    /// Closes the multi-shard install race where two parallel inbound
    /// shard-delegated installs for the same `replica_id` both pass
    /// their local per-shard registry check and both stamp the
    /// owner-table: the second stamp would clobber the first, leaving
    /// the loser's registry as an orphan that no cross-shard route
    /// reaches. The CAS gates the install before any task is spawned.
    ///
    /// The inverse op is [`Self::clear_if_owned_by`]; both must agree on
    /// `shard_id` to keep the slot self-consistent.
    pub fn try_claim(&self, replica_id: u8, shard_id: u16) -> bool {
        match self.slots[usize::from(replica_id)].compare_exchange(
            OWNER_NONE,
            shard_id,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(actual) => actual == shard_id,
        }
    }

    /// Clear the slot iff its current owner is `shard_id`. Returns
    /// `true` when the CAS succeeded.
    ///
    /// The CAS prevents a stale post-loop on shard A from clobbering a
    /// slot that has since been re-registered on shard B.
    pub fn clear_if_owned_by(&self, replica_id: u8, shard_id: u16) -> bool {
        self.slots[usize::from(replica_id)]
            .compare_exchange(shard_id, OWNER_NONE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Read the current owner of `replica_id`. Returns `None` when the
    /// slot is [`OWNER_NONE`].
    #[must_use]
    pub fn owner(&self, replica_id: u8) -> Option<u16> {
        match self.slots[usize::from(replica_id)].load(Ordering::Acquire) {
            OWNER_NONE => None,
            owner => Some(owner),
        }
    }

    /// Number of slots currently claimed by any shard, i.e. live peer
    /// replica connections on this node.
    #[must_use]
    pub fn owned_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.load(Ordering::Acquire) != OWNER_NONE)
            .count()
    }
}

impl Default for ReplicaOwnerTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Callback for forwarding a consensus replica message to the owning shard.
///
/// Provided by the shard layer at bus construction and installed via
/// [`IggyMessageBus::set_replica_forward_fn`]. Arguments:
/// `(replica_id, owning_shard_id, message)`. Returns `Ok(())` on successful
/// enqueue into the inter-shard channel.
///
/// `replica_id` is carried explicitly so the receiver can rebuild
/// `ShardFrame::Lifecycle`'s `ForwardReplicaSend { replica_id, msg }`
/// payload; the bus' slow path knows the id but the closure body, which lives
/// in the shard layer, would otherwise have to re-derive it from the message.
///
/// Asymmetry vs the `Rc<dyn Fn>` siblings (`AcceptedReplicaFn`,
/// `AcceptedClientFn`, `ConnectionLostFn`): the forward fn has a single
/// owner (the bus) and is installed once at bootstrap; `Box` suffices.
/// The `Rc` siblings are shared with accept loops and connection tasks
/// that outlive the caller and need independent ownership.
pub type ReplicaForwardFn = Box<dyn Fn(u8, u16, Frozen<MESSAGE_ALIGN>) -> Result<(), SendError>>;

/// Callback for forwarding a client-bound message to the owning shard.
///
/// Provided by the shard layer at bus construction and installed via
/// [`IggyMessageBus::set_client_forward_fn`]. Arguments:
/// `(client_id, owning_shard_id, message)`. Returns `Ok(())` on successful
/// enqueue into the inter-shard channel.
///
/// `owning_shard_id` is the value returned by [`client_id_owning_shard`];
/// the slow path passes it pre-resolved so the closure does not redo the
/// shift. `client_id` is carried so the receiver can rebuild the
/// `ShardFrame::Lifecycle`'s `ForwardClientSend { client_id, msg }`
/// payload.
pub type ClientForwardFn = Box<dyn Fn(u128, u16, Frozen<MESSAGE_ALIGN>) -> Result<(), SendError>>;

/// Callback invoked when a client connection metadata entry is removed.
pub type ClientConnectionLostFn = std::rc::Rc<dyn Fn(u128)>;

/// Callback invoked on every accepted inbound replica connection.
///
/// Fired by the replica listener BEFORE any byte is read: the peer id is
/// unknown (blind delegation), validation happens on the owning shard
/// inside [`replica::handshake::acceptor_handshake`]. Takes ownership of
/// the `TcpStream`. The callback enforces the shard-0-global in-flight
/// handshake cap ([`IggyMessageBus::try_acquire_replica_handshake_slot`])
/// and ships the raw fd to the owning shard.
pub type AcceptedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

/// Callback invoked on every successfully dialed outbound replica
/// connection.
///
/// Fired by the outbound connector right after the TCP connect; the
/// dialer handshake runs on the owning shard after delegation. The
/// dialer trusts the configured peer list, so the pre-configured peer id
/// rides along. Takes ownership of the `TcpStream`.
pub type DialedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, u8)>;

/// One-shot ack a delegated replica handshake fires when it finishes,
/// success or failure.
///
/// Built by the owning shard's router around its inter-shard sender to
/// shard 0, where it releases the in-flight cap slot (inbound) or clears
/// the pending-dial entry (outbound). Invoked exactly once at the end of
/// the spawned handshake task. A lost ack (full inbox, wedged shard) is
/// covered by the deadline expiry on the shard-0 entry.
pub type ReplicaHandshakeDoneFn = Box<dyn FnOnce()>;

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
}

impl AcceptedQuicConn {
    /// Bundle a freshly-accepted QUIC connection.
    ///
    /// `pub(crate)` by design: the constructor's signature mentions
    /// `compio_quic::Connection`, kept off the bus's public `SemVer`
    /// surface for the same reason [`Self::into_parts`] is crate-private.
    /// The QUIC listener in [`crate::client_listener::quic`] is the only
    /// mint site.
    ///
    /// Subsequent bidirectional streams are accepted on demand by the
    /// transport's `accept_bi` loop (see `QuicTransportConn::run`); the
    /// iggy SDK opens a fresh bidi per request, so eagerly capturing the
    /// first one at handshake time would lock the transport to a
    /// long-lived bidi pattern the SDK does not use.
    #[must_use]
    pub(crate) const fn new(connection: compio_quic::Connection) -> Self {
        Self { connection }
    }

    /// Remote peer address of this accepted QUIC connection.
    #[must_use]
    pub fn peer_addr(&self) -> SocketAddr {
        self.connection.remote_address()
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
    pub(crate) fn into_parts(self) -> compio_quic::Connection {
        self.connection
    }
}

/// Callback invoked on every accepted SDK QUIC client connection.
///
/// Fires after shard 0's QUIC listener drives the QUIC handshake to
/// completion. The callback receives a ready-for-traffic
/// [`AcceptedQuicConn`] that does NOT carry any pre-accepted
/// bidirectional stream; subsequent bidis are accepted on demand by
/// the transport's `accept_bi` loop (the iggy SDK opens a new bidi per
/// request). The callback mints a client id and forwards the conn
/// straight into [`installer::install_client_quic`] on the local bus,
/// which unwraps internally; no caller-side `into_parts` is needed (and
/// the helper is `pub(crate)` for that reason).
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
/// (`shard::LifecycleFrame::ClientWsConnectionSetup`). The owning shard
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
///
/// # Recovery asymmetry between `send_to_replica` and `send_to_client`
///
/// `send_to_replica` carries VSR-covered frames (`Prepare` / `PrepareOk`
/// / view-change / `Commit`). A `ReplicaForwardFailed` on full
/// inter-shard inbox is recovered by VSR retransmit and is
/// informational.
/// `send_to_client` carries the final Reply payload to the originating
/// client; it has no in-protocol retransmit. A `ClientForwardFailed` is
/// terminal: the client never receives the reply and times out.
/// Operators must size `inbox_capacity` for the worst-case cross-shard
/// reply burst. Each drop is logged at the drop site via `tracing`;
/// alert on those logs today. The `frame_drops_total`
/// `{variant="forward_client_send"}` counter is the structured
/// complement and becomes scrape-able once a per-shard exporter lands.
/// The symmetric `{variant="forward_replica_send"}` counter stays
/// informational only.
///
/// # Setter install contract
///
/// The three setters below do NOT share one install rule:
/// - [`Self::set_connection_lost_fn`] is re-installable (`RefCell`
///   slot); it is a test observation hook some setups swap mid-test.
/// - [`Self::set_replica_forward_fn`] and [`Self::set_client_forward_fn`]
///   are one-shot (`OnceCell` slot) and panic on a second install;
///   they are bootstrap-only wiring invariants.
///
/// A bus impl must preserve this divergence - see each method.
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
    /// connection dies abnormally. Production wiring leaves this unset
    /// because the shard-shared [`ReplicaOwnerTable`] is CAS-cleared
    /// inside `IggyMessageBus::notify_connection_lost` before the
    /// callback runs, so cross-shard routing recovers without any
    /// follow-up message. Test code installs a callback to assert the
    /// notifier path fires exactly once per disconnect.
    ///
    /// No default: every bus must explicitly opt in or stub. A silent
    /// no-op default previously masked a wiring bug where shard 0 never
    /// learned a delegated replica died.
    ///
    /// Re-installing is allowed by design - the backing slot is a
    /// `RefCell`, so a second call overwrites the first. This is the
    /// deliberate exception to the install rules of
    /// [`Self::set_replica_forward_fn`] / [`Self::set_client_forward_fn`]:
    /// the notifier is a test observation hook some setups swap
    /// mid-test, not a one-shot bootstrap invariant.
    fn set_connection_lost_fn(&self, f: ConnectionLostFn);

    /// Install the replica-plane inter-shard forward closure.
    ///
    /// No default: every bus must explicitly opt in or stub. Mirrors the
    /// reason `set_connection_lost_fn` is required - a silent no-op
    /// default could mask a wiring bug where cross-shard replica sends
    /// drop on the floor without anyone noticing.
    ///
    /// # Panics
    ///
    /// Panics on a second install on the same bus. Unlike
    /// [`Self::set_connection_lost_fn`], the forward closure is a
    /// one-shot bootstrap invariant backed by a `OnceCell`, so a
    /// double-install is a wiring bug rather than a supported re-bind.
    fn set_replica_forward_fn(&self, f: ReplicaForwardFn);

    /// Install the client-plane inter-shard forward closure.
    ///
    /// No default: every bus must explicitly opt in or stub. Same
    /// rationale as [`Self::set_replica_forward_fn`].
    ///
    /// # Panics
    ///
    /// Panics on a second install on the same bus, same one-shot
    /// `OnceCell` invariant as [`Self::set_replica_forward_fn`].
    fn set_client_forward_fn(&self, f: ClientForwardFn);

    /// Register a detached `compio::runtime::spawn` handle so
    /// [`shutdown`](IggyMessageBus::shutdown) can await it before the
    /// runtime drops. Production [`IggyMessageBus`] pushes onto a
    /// `RefCell<Vec<JoinHandle>>` drained on shutdown. Required (no default)
    /// so a new impl cannot silently drop detached handles by omission; a
    /// stub that spawns nothing implements it as a no-op.
    fn track_background(&self, handle: JoinHandle<()>);
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
///   VSR retransmission for replica-bound consensus traffic. Client Reply
///   drops returned as [`SendError::ClientForwardFailed`] have no
///   retransmit and are terminal; see the trait-level "Recovery
///   asymmetry" section on [`MessageBus`].
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
    /// Forwards a replica-bound message to the shard that owns the replica's
    /// TCP connection. Empty on single-shard setups and tests. Installed
    /// once at bootstrap via [`Self::set_replica_forward_fn`]; the slow
    /// path reads through [`OnceCell::get`] without a runtime borrow.
    replica_forward_fn: OnceCell<ReplicaForwardFn>,
    /// Forwards a client-bound message to the shard that owns the client's
    /// TCP connection (the owning shard is encoded in the top 16 bits of
    /// the client id). Empty on single-shard setups and tests. Installed
    /// once at bootstrap via [`Self::set_client_forward_fn`]; the slow
    /// path reads through [`OnceCell::get`] without a runtime borrow.
    client_forward_fn: OnceCell<ClientForwardFn>,
    /// Optional disconnect notifier invoked by
    /// `Self::notify_connection_lost` after it CAS-clears the owner
    /// table. Production leaves this unset; tests install a callback to
    /// observe the path.
    connection_lost_fn: RefCell<Option<ConnectionLostFn>>,
    /// Invoked when a client connection metadata entry is removed during
    /// connection teardown.
    client_connection_lost_fn: RefCell<Option<ClientConnectionLostFn>>,
    /// Per-connection metadata exposed to the caller via
    /// [`Self::client_meta`]. Populated by the install path on
    /// successful registry insert; removed on connection teardown.
    client_meta: RefCell<ahash::AHashMap<u128, Rc<ClientConnMeta>>>,
    /// Shard-shared atomic owner table. Stamped by the install path
    /// after a successful registry insert; CAS-cleared by
    /// `Self::notify_connection_lost`. See [`ReplicaOwnerTable`].
    owner_table: Arc<ReplicaOwnerTable>,
    /// Identity + auth parameters for delegated replica handshakes,
    /// installed once at bootstrap via
    /// [`Self::set_replica_handshake_ctx`]. Read by the
    /// `install_replica_{inbound,outbound}` paths on the owning shard.
    replica_handshake_ctx: OnceCell<replica::handshake::ReplicaHandshakeCtx>,
    /// Shard-0-global in-flight inbound handshake slots: slot id ->
    /// expiry deadline. Capped at [`MAX_INFLIGHT_REPLICA_HANDSHAKES`];
    /// expired entries are pruned lazily inside
    /// [`Self::try_acquire_replica_handshake_slot`] so no timer task is
    /// needed. Only shard 0's bus ever populates this.
    replica_handshake_slots: RefCell<ahash::AHashMap<u64, Instant>>,
    /// Monotonic id source for [`Self::replica_handshake_slots`].
    replica_slot_seq: Cell<u64>,
    /// Peers with a delegated outbound handshake still in flight:
    /// replica id -> expiry deadline. Consulted by the reconnect sweep
    /// so a peer is not redialed while its previous handshake is
    /// pending; entries expire lazily inside [`Self::check_dial_pending`].
    /// Only shard 0's bus ever populates this.
    pending_dials: RefCell<ahash::AHashMap<u8, Instant>>,
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

    /// Production constructor for multi-shard server-ng: same as
    /// [`Self::with_config`] but takes a pre-allocated
    /// [`ReplicaOwnerTable`] Arc. Bootstrap allocates one table per
    /// server-ng process and clones the Arc into every shard so all
    /// buses see the same atomic slots.
    #[must_use]
    pub fn with_config_and_owner_table(
        shard_id: u16,
        cfg: &ServerNgConfig,
        owner_table: Arc<ReplicaOwnerTable>,
    ) -> Self {
        Self::with_tunables_and_owner_table(shard_id, MessageBusConfig::from(cfg), owner_table)
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
        Self::with_tunables_and_owner_table(shard_id, config, Arc::new(ReplicaOwnerTable::new()))
    }

    /// Construct a bus with explicit runtime tunables and a pre-allocated
    /// owner table. Server-ng bootstrap uses this so every shard's bus
    /// shares the same atomic slots; tests use [`Self::with_tunables`]
    /// which allocates a fresh table per bus.
    ///
    /// # Panics
    ///
    /// Panics if `config.max_batch == 0` or
    /// `config.max_batch > IOV_MAX_LIMIT`.
    #[must_use]
    pub fn with_tunables_and_owner_table(
        shard_id: u16,
        config: MessageBusConfig,
        owner_table: Arc<ReplicaOwnerTable>,
    ) -> Self {
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
            replica_forward_fn: OnceCell::new(),
            client_forward_fn: OnceCell::new(),
            connection_lost_fn: RefCell::new(None),
            client_connection_lost_fn: RefCell::new(None),
            client_meta: RefCell::new(ahash::AHashMap::new()),
            owner_table,
            replica_handshake_ctx: OnceCell::new(),
            replica_handshake_slots: RefCell::new(ahash::AHashMap::new()),
            replica_slot_seq: Cell::new(0),
            pending_dials: RefCell::new(ahash::AHashMap::new()),
        }
    }

    /// Clone of the shard-shared owner table Arc.
    #[must_use]
    pub fn owner_table(&self) -> Arc<ReplicaOwnerTable> {
        Arc::clone(&self.owner_table)
    }

    /// Try to claim this bus' shard id as owner of `replica_id`.
    ///
    /// Returns `true` when the claim succeeded (the slot now points at
    /// `self.shard_id`, either because it was `OWNER_NONE` or because
    /// it already stored `self.shard_id`). Returns `false` when a
    /// different shard owns the slot; the caller must drop the
    /// inbound connection.
    ///
    /// Called by [`installer::install_replica_conn`] **before**
    /// spawning tasks or inserting into the local registry. See
    /// [`ReplicaOwnerTable::try_claim`] for the multi-shard race this
    /// closes.
    #[must_use]
    pub fn mark_replica_owned(&self, replica_id: u8) -> bool {
        self.owner_table.try_claim(replica_id, self.shard_id)
    }

    /// CAS-clear the owner-table slot for `replica_id` if its current
    /// owner is this bus' shard id. The CAS prevents a stale post-loop
    /// from clobbering a slot that has since been re-registered on a
    /// different shard.
    pub fn clear_replica_owned(&self, replica_id: u8) -> bool {
        self.owner_table
            .clear_if_owned_by(replica_id, self.shard_id)
    }

    /// Install the identity + auth parameters used by delegated replica
    /// handshakes on this shard. Bootstrap-only wiring; same one-shot
    /// contract as [`Self::set_replica_forward_fn`].
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same bus.
    pub fn set_replica_handshake_ctx(&self, ctx: replica::handshake::ReplicaHandshakeCtx) {
        assert!(
            self.replica_handshake_ctx.set(ctx).is_ok(),
            "replica_handshake_ctx installed twice (bootstrap invariant)"
        );
    }

    /// Parameters installed by [`Self::set_replica_handshake_ctx`], or
    /// `None` when the bootstrap never wired the replica plane (client-
    /// only deployments, most tests).
    #[must_use]
    pub fn replica_handshake_ctx(&self) -> Option<&replica::handshake::ReplicaHandshakeCtx> {
        self.replica_handshake_ctx.get()
    }

    /// Expiry deadline for shard-0 in-flight slots and pending-dial
    /// entries: the owning shard bounds the handshake by
    /// `handshake_grace`, plus margin for inter-shard inbox latency.
    fn replica_handshake_deadline(&self) -> Instant {
        Instant::now() + self.config.handshake_grace + REPLICA_HANDSHAKE_ACK_MARGIN
    }

    /// Claim a slot in the shard-0-global in-flight inbound handshake
    /// cap. Returns `None` when [`MAX_INFLIGHT_REPLICA_HANDSHAKES`]
    /// non-expired slots are outstanding; the caller drops the inbound
    /// connection. The slot is released early by the owning shard's
    /// handshake-outcome ack ([`Self::release_replica_handshake_slot`])
    /// or expires on its own after `handshake_grace` plus margin.
    #[must_use]
    pub fn try_acquire_replica_handshake_slot(&self) -> Option<u64> {
        let mut slots = self.replica_handshake_slots.borrow_mut();
        if slots.len() >= MAX_INFLIGHT_REPLICA_HANDSHAKES {
            let now = Instant::now();
            slots.retain(|_, deadline| *deadline > now);
            if slots.len() >= MAX_INFLIGHT_REPLICA_HANDSHAKES {
                return None;
            }
        }
        let slot = self.replica_slot_seq.get();
        self.replica_slot_seq.set(slot.wrapping_add(1));
        slots.insert(slot, self.replica_handshake_deadline());
        Some(slot)
    }

    /// Release an in-flight handshake slot. Idempotent: an ack arriving
    /// after the slot already expired (and was pruned) is a no-op.
    /// Returns whether the slot was still present.
    pub fn release_replica_handshake_slot(&self, slot: u64) -> bool {
        self.replica_handshake_slots
            .borrow_mut()
            .remove(&slot)
            .is_some()
    }

    /// Record that an outbound connection to `replica_id` was delegated
    /// and its dialer handshake is in flight on the owning shard. The
    /// reconnect sweep skips pending peers; the entry is cleared by the
    /// handshake-outcome ack ([`Self::clear_dial_pending`]) or expires
    /// on its own after `handshake_grace` plus margin.
    pub fn mark_dial_pending(&self, replica_id: u8) {
        self.pending_dials
            .borrow_mut()
            .insert(replica_id, self.replica_handshake_deadline());
    }

    /// Whether an outbound handshake to `replica_id` is still in flight.
    /// An expired entry is removed here (lazy expiry) and reported as
    /// not pending, re-enabling the redial.
    #[must_use]
    pub fn check_dial_pending(&self, replica_id: u8) -> bool {
        let mut pending = self.pending_dials.borrow_mut();
        match pending.get(&replica_id) {
            Some(deadline) if *deadline > Instant::now() => true,
            Some(_) => {
                pending.remove(&replica_id);
                false
            }
            None => false,
        }
    }

    /// Clear the pending-dial entry for `replica_id`. Idempotent; returns
    /// whether an entry was present.
    pub fn clear_dial_pending(&self, replica_id: u8) -> bool {
        self.pending_dials
            .borrow_mut()
            .remove(&replica_id)
            .is_some()
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
        if self.client_meta.borrow_mut().remove(&client_id).is_some() {
            self.notify_client_connection_lost(client_id);
        }
    }

    pub fn set_client_connection_lost_fn(&self, f: ClientConnectionLostFn) {
        *self.client_connection_lost_fn.borrow_mut() = Some(f);
    }

    fn notify_client_connection_lost(&self, client_id: u128) {
        let cb = self
            .client_connection_lost_fn
            .borrow()
            .as_ref()
            .map(std::rc::Rc::clone);
        if let Some(f) = cb {
            f(client_id);
        }
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
    pub(crate) fn notify_connection_lost(&self, replica_id: u8, reason: &'static str) {
        // Stand down if a live registry entry exists for this replica. A
        // reconnect that round-robined back to this same shard installs a
        // fresh entry before this stale post-loop runs; the owner-table
        // CAS in `clear_replica_owned` only guards *different*-shard
        // re-registration, so a same-shard reinstall leaves the slot
        // == self.shard_id and the clear below would clobber the live
        // connection's slot, stranding cross-shard `send_to_replica`
        // forever. Every caller removes its own registry entry before
        // invoking this, so a live entry here is always a newer install
        // that now owns the slot. No token needed: the bus is
        // single-threaded (compio), so this check, the clear, and a
        // competing `install_replica_conn` cannot interleave.
        if self.replicas.contains(replica_id) {
            return;
        }
        // CAS-clear first so any reader racing the user callback already
        // sees the slot vacated. The CAS no-ops if a different shard
        // already owns the slot (post-loop arriving after a
        // re-installation on another shard).
        self.clear_replica_owned(replica_id);
        tracing::warn!(
            shard_id = self.shard_id,
            replica_id,
            reason,
            "replica connection lost"
        );
        let expected = self.config.mesh_expected_peers;
        if expected > 0 {
            tracing::warn!(
                connected = self.owner_table.owned_count(),
                expected,
                "replica mesh degraded"
            );
        }
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
    /// Panics if called more than once on the same bus. The bootstrap is
    /// the sole caller and runs exactly once per bus; a second install
    /// signals a wiring bug we want to surface loudly.
    pub fn set_replica_forward_fn(&self, f: ReplicaForwardFn) {
        self.replica_forward_fn
            .set(f)
            .ok()
            .expect("replica_forward_fn installed twice (bootstrap invariant)");
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
    /// Panics if called more than once on the same bus. Same rationale as
    /// [`Self::set_replica_forward_fn`].
    pub fn set_client_forward_fn(&self, f: ClientForwardFn) {
        self.client_forward_fn
            .set(f)
            .ok()
            .expect("client_forward_fn installed twice (bootstrap invariant)");
    }

    /// Get the owning shard for a replica, if any.
    ///
    /// Reads the shard-shared [`ReplicaOwnerTable`], which is stamped
    /// synchronously by the owning shard's installer and CAS-cleared on
    /// disconnect inside `Self::notify_connection_lost`.
    #[must_use]
    pub fn owning_shard(&self, replica: u8) -> Option<u16> {
        self.owner_table.owner(replica)
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
        use futures::FutureExt;
        let mut tasks = self.background_tasks.borrow_mut();
        // compio 0.19's `JoinHandle` dropped `is_finished`; poll each
        // handle once (it is `Unpin` + a `Future`) and reap the ones that
        // have already resolved. `now_or_never` on `&mut h` polls without
        // consuming; `Some` means the task finished (drop it), `None`
        // means still running (keep). Single-threaded runtime, so no flip
        // between this poll and the drop.
        tasks.retain_mut(|h| h.now_or_never().is_none());
        tasks.push(handle);
    }

    /// Number of background-task handles currently retained by the bus.
    ///
    /// Test-only accessor: lets integration tests pin the
    /// reap-on-push invariant in `track_background` without exposing
    /// the underlying `RefCell<Vec<JoinHandle<()>>>` to production
    /// callers. A long-running bus is expected to keep this number
    /// bounded under sustained accept traffic; a leak shows up here as
    /// monotonic growth proportional to total accepts.
    #[doc(hidden)]
    #[must_use]
    pub fn background_tasks_len(&self) -> usize {
        self.background_tasks.borrow().len()
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

    fn set_replica_forward_fn(&self, f: ReplicaForwardFn) {
        (**self).set_replica_forward_fn(f);
    }

    fn set_client_forward_fn(&self, f: ClientForwardFn) {
        (**self).set_client_forward_fn(f);
    }

    fn track_background(&self, handle: JoinHandle<()>) {
        (**self).track_background(handle);
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
            // Route by the entry's reply target. Socket is the unchanged
            // fast path and never decodes. InProcess resolves the waiting
            // request id from the reply header and fires its oneshot; that
            // decode stays off the socket path. No slot surfaces
            // ClientNotFound (prior behaviour: SendError did not preserve
            // the payload either).
            return match self.clients.try_send_or_return(client_id, message) {
                ReplyRoute::Delivered(send_result) => send_result.map_err(map_try_send_err),
                ReplyRoute::InProcess(message) => {
                    let request = reply_request_id(&message);
                    self.clients
                        .fire_in_process(client_id, request, message)
                        .map_err(|_| SendError::ClientNotFound(client_id))
                }
                ReplyRoute::NoSlot(_message) => Err(SendError::ClientNotFound(client_id)),
            };
        }
        let forward = self
            .client_forward_fn
            .get()
            .ok_or(SendError::ClientRouteMissing(client_id))?;
        forward(client_id, owning_shard, message)
            .map_err(|_| SendError::ClientForwardFailed(client_id))
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
        // The shard-shared `owner_table` is the authoritative routing
        // view: `mark_replica_owned` stamps it on install and
        // `clear_replica_owned` CAS-clears it on disconnect.
        let owning_shard = self
            .owner_table
            .owner(replica)
            .ok_or(SendError::ReplicaNotConnected(replica))?;
        // The fast path already failed (no live registry slot), so a
        // self-owner here means the disconnect is mid-flight:
        // `close_peer_if_token_matches` took the slot but
        // `clear_replica_owned` has not yet run. Forwarding now would
        // `try_send` a `ForwardReplicaSend` onto our own inbox and the
        // pump would re-enter this path - a self-inbox ping-pong on the
        // consensus hot path. Treat as not connected, mirroring the
        // `send_to_client` self-owner guard.
        if owning_shard == self.shard_id {
            return Err(SendError::ReplicaNotConnected(replica));
        }
        let forward = self
            .replica_forward_fn
            .get()
            .ok_or(SendError::ReplicaRouteMissing(replica))?;
        forward(replica, owning_shard, message)
            .map_err(|_| SendError::ReplicaForwardFailed(replica))
    }

    fn set_connection_lost_fn(&self, f: ConnectionLostFn) {
        Self::set_connection_lost_fn(self, f);
    }

    fn set_replica_forward_fn(&self, f: ReplicaForwardFn) {
        Self::set_replica_forward_fn(self, f);
    }

    fn set_client_forward_fn(&self, f: ClientForwardFn) {
        Self::set_client_forward_fn(self, f);
    }

    fn track_background(&self, handle: JoinHandle<()>) {
        Self::track_background(self, handle);
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

/// Peek `ReplyHeader.request` (the originating request id) from a reply
/// buffer at its fixed header offset. Only the in-process reply path calls
/// this; the socket path never decodes.
fn reply_request_id(reply: &Frozen<MESSAGE_ALIGN>) -> u64 {
    const OFFSET: usize = std::mem::offset_of!(ReplyHeader, request);
    reply
        .as_slice()
        .get(OFFSET..OFFSET + std::mem::size_of::<u64>())
        .and_then(|bytes| bytes.try_into().ok())
        .map_or(0, u64::from_ne_bytes)
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
        let client_id = (7u128 << 112) | 0x2a;
        let captured_id: std::rc::Rc<RefCell<Option<u128>>> = std::rc::Rc::new(RefCell::new(None));
        let captured_id_clone = captured_id.clone();
        bus.set_client_forward_fn(Box::new(move |id, target, _msg| {
            *captured_id_clone.borrow_mut() = Some(id);
            *captured_clone.borrow_mut() = Some(target);
            Ok(())
        }));

        bus.send_to_client(client_id, dummy_message().into_frozen())
            .await
            .expect("forward_fn should accept");
        assert_eq!(*captured.borrow(), Some(7));
        assert_eq!(*captured_id.borrow(), Some(client_id));
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

    #[allow(clippy::cast_possible_truncation)]
    fn reply_message(request: u64) -> Message<ReplyHeader> {
        Message::<ReplyHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut ReplyHeader| {
            h.command = Command2::Reply;
            h.size = HEADER_SIZE as u32;
            h.request = request;
        })
    }

    /// Full in-process seam: entry + slot installed, `send_to_client`
    /// resolves the request id from the reply header and fires the oneshot.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_in_process_fires_matching_reply_slot() {
        let bus = IggyMessageBus::new(0);
        let client_id = 1u128;
        bus.clients()
            .insert_in_process(client_id)
            .expect("fresh key");
        let (_guard, reply_rx) = bus
            .clients()
            .install_reply_slot(client_id, 42)
            .expect("slot installs");

        bus.send_to_client(client_id, reply_message(42).into_generic().into_frozen())
            .await
            .expect("in-process delivery");

        let received = reply_rx.await.expect("reply delivered");
        assert_eq!(reply_request_id(&received), 42);
    }

    /// Reply whose request id has no waiter (timed-out caller already
    /// removed its slot): shed as `ClientNotFound`, nothing panics.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_in_process_sheds_reply_without_waiter() {
        let bus = IggyMessageBus::new(0);
        let client_id = 1u128;
        bus.clients()
            .insert_in_process(client_id)
            .expect("fresh key");

        let err = bus
            .send_to_client(client_id, reply_message(42).into_generic().into_frozen())
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
    async fn send_to_replica_slow_path_uses_owner_table_and_forwards() {
        let bus = IggyMessageBus::new(0);
        let captured: std::rc::Rc<RefCell<Option<u16>>> = std::rc::Rc::new(RefCell::new(None));
        let captured_clone = captured.clone();
        let captured_id: std::rc::Rc<RefCell<Option<u8>>> = std::rc::Rc::new(RefCell::new(None));
        let captured_id_clone = captured_id.clone();
        bus.set_replica_forward_fn(Box::new(move |id, target, _msg| {
            *captured_id_clone.borrow_mut() = Some(id);
            *captured_clone.borrow_mut() = Some(target);
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(5, 3));

        bus.send_to_replica(5, dummy_message().into_frozen())
            .await
            .expect("forward ok");

        assert_eq!(*captured.borrow(), Some(3));
        assert_eq!(*captured_id.borrow(), Some(5));
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

    /// Disconnect-race window: the registry slot is already gone (fast
    /// path misses) but `owner_table` still points at this shard because
    /// `clear_replica_owned` has not run yet. The slow path must treat a
    /// self-owner as not connected rather than forwarding the frame onto
    /// its own inbox - mirrors the `send_to_client` self-owner guard.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_replica_slow_path_self_owner_returns_not_connected() {
        let bus = IggyMessageBus::new(3);
        assert!(bus.owner_table().try_claim(5, 3));

        let err = bus
            .send_to_replica(5, dummy_message().into_frozen())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::ReplicaNotConnected(5)));
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
        assert_eq!(bus.background_tasks_len(), 2);
    }

    /// Reap invariant: `track_background` drops finished handles before
    /// pushing the new one, so an accept-loop that fires N times over
    /// the bus's lifetime does NOT accumulate N retained handles. This
    /// pins the leak fix in `installer::install_client_ws_fd` (one
    /// upgrade task per WS connect) without needing an end-to-end WS
    /// roundtrip.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn track_background_reaps_finished_handles_on_push() {
        let bus = IggyMessageBus::new(0);

        for _ in 0..32 {
            let h = compio::runtime::spawn(async {});
            // Drive the runtime so the spawned task can complete before
            // we register the next one.
            compio::runtime::time::sleep(std::time::Duration::from_millis(1)).await;
            bus.track_background(h);
        }

        // The most recently pushed handle is the only one that may not
        // yet be finished; everything before it had a chance to complete
        // and the reap on each push should have dropped them.
        let remaining = bus.background_tasks_len();
        assert!(
            remaining <= 1,
            "track_background did not reap finished handles; retained {remaining} of 32 spawns",
        );
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

        bus.notify_connection_lost(1, "test"); // first closure runs (+1) and swaps
        assert_eq!(counter.get(), 1);

        bus.notify_connection_lost(1, "test"); // second closure runs (+10)
        assert_eq!(counter.get(), 11);
    }

    /// Same-shard reconnect race: a stale post-loop from a dead connection
    /// must not CAS-clear the owner-table slot that a live reinstall
    /// (round-robined back to this shard) just stamped. The slot is only
    /// released once no live registry entry remains.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn notify_connection_lost_stands_down_when_replica_reregistered() {
        let bus = IggyMessageBus::new(3);

        // Live reinstall: registry entry present + owner table stamped.
        let (tx, _rx) = async_channel::bounded(8);
        let writer = compio::runtime::spawn(async {});
        let reader = compio::runtime::spawn(async {});
        let (conn_shutdown, _conn_token) = Shutdown::new();
        bus.replicas()
            .insert(7, tx, writer, reader, conn_shutdown)
            .expect("insert ok");
        assert!(bus.mark_replica_owned(7), "owner-table claim must succeed");

        // Stale predecessor post-loop fires while the reinstall is live.
        bus.notify_connection_lost(7, "test");
        assert_eq!(
            bus.owning_shard(7),
            Some(3),
            "stale post-loop cleared a re-registered replica's owner slot",
        );

        // Genuine disconnect: no live entry, the slot is released.
        bus.replicas().remove(7);
        bus.notify_connection_lost(7, "test");
        assert_eq!(
            bus.owning_shard(7),
            None,
            "owner slot must clear once no live connection remains",
        );
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

    #[test]
    fn try_claim_wins_from_none() {
        let table = ReplicaOwnerTable::new();
        assert!(table.try_claim(42, 3), "claim from OWNER_NONE must win");
        assert_eq!(table.owner(42), Some(3));
    }

    #[test]
    fn try_claim_loses_to_different_shard() {
        let table = ReplicaOwnerTable::new();
        assert!(table.try_claim(42, 5));
        assert!(
            !table.try_claim(42, 7),
            "claim from different shard must lose"
        );
        assert_eq!(
            table.owner(42),
            Some(5),
            "losing claim must not mutate the slot",
        );
    }

    #[test]
    fn try_claim_same_shard_returns_true() {
        // Models the same-shard reclaim window: previous install on
        // this shard stamped the slot, post-loop's
        // `clear_replica_owned` has not run yet, a new install for the
        // same replica id arrives. The CAS-from-NONE fails but
        // `try_claim` treats `actual == shard_id` as success so the
        // new install proceeds.
        let table = ReplicaOwnerTable::new();
        assert!(table.try_claim(42, 5));
        assert!(
            table.try_claim(42, 5),
            "same-shard reclaim must be accepted",
        );
        assert_eq!(table.owner(42), Some(5));
    }

    fn bus_with_grace(grace: Duration) -> IggyMessageBus {
        IggyMessageBus::with_tunables(
            0,
            MessageBusConfig {
                handshake_grace: grace,
                ..MessageBusConfig::default()
            },
        )
    }

    #[test]
    fn handshake_slot_cap_enforced_and_released() {
        let bus = IggyMessageBus::new(0);
        let mut slots = Vec::with_capacity(MAX_INFLIGHT_REPLICA_HANDSHAKES);
        for _ in 0..MAX_INFLIGHT_REPLICA_HANDSHAKES {
            slots.push(
                bus.try_acquire_replica_handshake_slot()
                    .expect("below cap must acquire"),
            );
        }
        assert!(
            bus.try_acquire_replica_handshake_slot().is_none(),
            "cap reached: acquire must fail"
        );
        assert!(bus.release_replica_handshake_slot(slots[0]));
        assert!(
            bus.try_acquire_replica_handshake_slot().is_some(),
            "released slot must free capacity"
        );
        assert!(
            !bus.release_replica_handshake_slot(slots[0]),
            "double release must be a no-op"
        );
    }

    #[test]
    fn dial_pending_marks_clears_and_expires() {
        let bus = bus_with_grace(Duration::ZERO);
        bus.mark_dial_pending(7);
        assert!(bus.check_dial_pending(7), "fresh entry must be pending");
        assert!(bus.clear_dial_pending(7), "ack clears the entry");
        assert!(!bus.check_dial_pending(7), "cleared entry must not block");
        assert!(!bus.clear_dial_pending(7), "double clear is a no-op");

        // Force-expire: rewrite the deadline into the past, mimicking a
        // lost ack after grace + margin elapsed.
        bus.mark_dial_pending(9);
        let past = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("test clock past epoch");
        bus.pending_dials.borrow_mut().insert(9, past);
        assert!(
            !bus.check_dial_pending(9),
            "expired entry must report not-pending"
        );
        assert!(
            !bus.clear_dial_pending(9),
            "lazy expiry must have removed the entry"
        );
    }

    #[test]
    fn handshake_slot_lazy_expiry_prunes_at_cap() {
        let bus = IggyMessageBus::new(0);
        for _ in 0..MAX_INFLIGHT_REPLICA_HANDSHAKES {
            let _ = bus.try_acquire_replica_handshake_slot();
        }
        assert!(bus.try_acquire_replica_handshake_slot().is_none());
        // Age every outstanding slot past its deadline; the next acquire
        // at cap must prune them all and succeed.
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("test clock past epoch");
        for deadline in bus.replica_handshake_slots.borrow_mut().values_mut() {
            *deadline = expired;
        }
        assert!(
            bus.try_acquire_replica_handshake_slot().is_some(),
            "expired slots must be pruned at cap"
        );
    }
}
