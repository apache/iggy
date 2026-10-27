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
//!   downstream consumers in `core/server` are free to use it for
//!   tracing, idempotency, or correlation.
//!
//! # Auth
//!
//! - SDK-client plane: the bus does not authenticate clients. The caller
//!   (`core/server`) gates command access with application-level LOGIN
//!   commands: `LOGIN_USER` / `LOGIN_WITH_PERSONAL_ACCESS_TOKEN`,
//!   pre-LOGIN allowlist `PING`, `LOGIN_USER`, `LOGIN_WITH_PAT`.
//! - Replica plane: every link opens with a placement preamble
//!   ([`replica::preamble`]) and then the replica handshake
//!   ([`replica::handshake`]). The handshake checks the cluster id, the
//!   build, the dialer direction and the link placement. With a shared
//!   secret configured, it also authenticates both ends with a MAC
//!   ([`replica::auth`]).
//!
//! # Invariants worth naming
//!
//! - [`send_to_client`](IggyMessageBus::send_to_client) and
//!   [`send_to_replica`](IggyMessageBus::send_to_replica) return
//!   `Ready` on first poll. Consensus code relies on this for
//!   reentrancy reasoning; any `.await` in the body breaks it.
//! - The TCP transport's writer task coalesces up to
//!   `MessageBusConfig::max_batch` (default 256) [`BusMessage`] frames
//!   (their `Frozen<MESSAGE_ALIGN>` fragments flattened, chunked at
//!   `IOV_MAX`) into `write_vectored_all`. Don't introduce per-message
//!   syscalls or per-message encryption on the plaintext TCP plane.
//! - fd-delegation ([`fd_transfer`]) transfers raw TCP sockets for TCP,
//!   WS, TCP-TLS and WSS before handshakes. The destination shard owns
//!   handshake state and I/O. QUIC client callbacks install on shard 0
//!   through its shared UDP endpoint.
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

pub mod accept;
pub mod client_listener;
pub mod config;
pub mod connection_cap;
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
pub use connection_cap::{ConnectionCap, ConnectionPermit};
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
use configs::server::ServerConfig;
use iggy_binary_protocol::{Command, GenericHeader, ReplyHeader};
use replica::bundle::{Bundle, BundleChange, BundlePhase};
use replica::links::{LinkClaim, peer_slot};
pub use replica::links::{LinkLayout, LinkLayoutError, ReplicaLinkId};
use replica::preamble::{Preamble, PreambleError};
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use std::array;
use std::cell::{Cell, OnceCell, RefCell};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Maximum number of replicas a single cluster supports. Replica ids are
/// `u8`, so the address space is 0..=255.
pub const MAX_REPLICAS: usize = 256;

/// Hard cap on concurrently in-flight inbound replica handshakes,
/// enforced globally on shard 0 (the sole acceptor) regardless of which
/// shard the delegated handshake runs on.
///
/// A legitimate dialer set opens at most `self_id` times the link count
/// sockets (`self_id` < `replica_count` <= 255). [`LinkLayout::new`]
/// keeps that product at or below this cap, so the cap only sheds load
/// under a hostile flood. It bounds fd / memory without ever stalling the
/// accept loop.
pub const MAX_INFLIGHT_REPLICA_HANDSHAKES: usize = 256;

/// Extra wall-clock allowance past `handshake_grace` before a shard-0
/// in-flight slot or pending-dial entry expires on its own. Covers the
/// inter-shard inbox latency of the delegation frame and the outcome
/// ack; expiry only matters when the ack is lost (full inbox or a
/// wedged owning shard), where it prevents a permanently leaked slot or
/// a permanently suppressed redial.
const REPLICA_HANDSHAKE_ACK_MARGIN: Duration = Duration::from_secs(5);

/// Owner shard stored in [`ReplicaOwnerTable`] slots that have no current owner.
///
/// `u16::MAX` is reserved by the server bootstrap so it can never be a real
/// shard id: `bootstrap` rejects any server whose shard count does not fit
/// in `u16` or is `>= OWNER_NONE`, returning `ShardsCountOverflow` rather
/// than minting a shard id that collides with this sentinel.
pub const OWNER_NONE: u16 = u16::MAX;

/// A free owner-table slot: no owner, epoch bits zero.
const LINK_FREE: u64 = OWNER_NONE as u64;

/// A slot keeps the owner shard in its low bits and the epoch above them.
const LINK_EPOCH_SHIFT: u32 = u16::BITS;

/// The link that carries every frame the peer routes to the metadata group.
/// That group lives on shard 0 of every node, and shard 0 is in range 0.
const METADATA_LINK: u16 = 0;

const COMMAND_OFFSET: usize = std::mem::offset_of!(GenericHeader, command);

/// Shared atomic owner table of the replica links of this node.
///
/// One Arc-cloned instance is allocated per server process at bootstrap and
/// shared across every shard's [`IggyMessageBus`]. It holds the
/// [`LinkLayout`], one slot per (peer, link) and one Up flag per peer.
///
/// A slot names the shard that owns the link and the bundle epoch the link
/// was placed in. The owner shard claims the slot before it installs the
/// link and releases it when the link closes. Both are CAS operations. A
/// claim only takes a free slot, so a second link for an installed (peer,
/// link) is refused and never replaces the live one. A release only frees
/// the claim it made, so a late release never frees a newer link.
///
/// Only shard 0 writes the Up flags (see [`replica::bundle`]). Every shard
/// reads them on each send, so a peer gets frames only while all the links
/// of its bundle are installed.
///
/// The table is the sole authority for cross-shard replica routing:
/// `send_to_replica`'s slow path reads it directly. No broadcast or
/// reconcile loop is involved.
pub struct ReplicaOwnerTable {
    layout: LinkLayout,
    /// Slot of link `j` to peer `p` at `p * links + j`.
    links: Box<[AtomicU64]>,
    up: [AtomicBool; MAX_REPLICAS],
}

impl ReplicaOwnerTable {
    /// A table for one link per peer on a single shard.
    #[must_use]
    pub fn new() -> Self {
        Self::with_layout(LinkLayout::default())
    }

    #[must_use]
    pub fn with_layout(layout: LinkLayout) -> Self {
        let slots = MAX_REPLICAS * usize::from(layout.links());
        Self {
            layout,
            links: (0..slots).map(|_| AtomicU64::new(LINK_FREE)).collect(),
            up: array::from_fn(|_| AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub const fn layout(&self) -> LinkLayout {
        self.layout
    }

    /// Claim the slot of `link` for `shard_id` in the epoch of `link`.
    ///
    /// Returns `false` when an install of any epoch holds the slot, or when
    /// the link index is out of range. The caller then drops the connection
    /// without spawning a task.
    pub fn try_claim(&self, link: ReplicaLinkId, shard_id: u16) -> bool {
        self.slot(link.replica, link.link).is_some_and(|slot| {
            slot.compare_exchange(
                LINK_FREE,
                pack_claim(shard_id, link.epoch),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        })
    }

    /// Free the slot of `link` if `shard_id` still holds it in the epoch of
    /// `link`. Returns whether the slot was freed.
    pub fn release(&self, link: ReplicaLinkId, shard_id: u16) -> bool {
        self.slot(link.replica, link.link).is_some_and(|slot| {
            slot.compare_exchange(
                pack_claim(shard_id, link.epoch),
                LINK_FREE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        })
    }

    /// The install that holds link `link` to `replica`, if any.
    #[must_use]
    pub fn claim(&self, replica: u8, link: u16) -> Option<LinkClaim> {
        unpack_claim(self.slot(replica, link)?.load(Ordering::Acquire))
    }

    /// Whether frames flow to `replica`: every link of its bundle is
    /// installed in the current epoch.
    #[must_use]
    pub fn is_up(&self, replica: u8) -> bool {
        self.up[usize::from(replica)].load(Ordering::Acquire)
    }

    fn set_up(&self, replica: u8, up: bool) {
        self.up[usize::from(replica)].store(up, Ordering::Release);
    }

    fn slot(&self, replica: u8, link: u16) -> Option<&AtomicU64> {
        let links = self.layout.links();
        if link >= links {
            return None;
        }
        self.links
            .get(usize::from(replica) * usize::from(links) + usize::from(link))
    }
}

impl Default for ReplicaOwnerTable {
    fn default() -> Self {
        Self::new()
    }
}

const fn pack_claim(owner: u16, epoch: u32) -> u64 {
    ((epoch as u64) << LINK_EPOCH_SHIFT) | owner as u64
}

#[allow(clippy::cast_possible_truncation)]
const fn unpack_claim(slot: u64) -> Option<LinkClaim> {
    let owner = slot as u16;
    if owner == OWNER_NONE {
        return None;
    }
    Some(LinkClaim {
        owner,
        epoch: (slot >> LINK_EPOCH_SHIFT) as u32,
    })
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

/// Callback invoked when a client connection metadata entry is removed.
pub type ClientConnectionLostFn = std::rc::Rc<dyn Fn(u128)>;

/// Callback invoked on every accepted inbound replica connection.
///
/// Fired by the replica listener BEFORE any byte is read. Takes ownership
/// of the `TcpStream`. The callback enforces the shard-0-global in-flight
/// handshake cap ([`IggyMessageBus::try_acquire_replica_handshake_slot`]),
/// reads the placement preamble ([`IggyMessageBus::place_replica_inbound`])
/// in a task of its own and ships the raw fd to the shard that owns the
/// link. The owning shard validates the peer inside
/// [`replica::handshake::acceptor_handshake`].
pub type AcceptedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream)>;

/// Callback invoked on every successfully dialed outbound replica link.
///
/// Fired by the outbound connector right after the TCP connect, with the
/// link and the shard that owns it. The dialer handshake runs on that
/// shard after delegation. The dialer trusts the configured peer list, so
/// the pre-configured peer id rides along. Takes ownership of the
/// `TcpStream`.
pub type DialedReplicaFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, ReplicaLinkId, u16)>;

/// One-shot ack a delegated replica handshake fires when it finishes,
/// success or failure.
///
/// Built by the owning shard's router around its inter-shard sender to
/// shard 0, where it releases the in-flight cap slot (inbound) or clears
/// the pending-dial entry (outbound) and refreshes the bundle of the peer.
/// Invoked exactly once at the end of the spawned handshake task. A lost
/// ack (full inbox, wedged shard) is covered by the deadline expiry on the
/// shard-0 entry and by the next reconnect sweep.
pub type ReplicaHandshakeDoneFn = Box<dyn FnOnce()>;

/// Shard 0 tells the owner shard of a replica link to close it.
///
/// Installed once on shard 0's bus at bootstrap via
/// [`IggyMessageBus::set_replica_link_close_fn`]. Arguments: `(owner_shard,
/// link)`. The closure ships the request to the owner shard, which closes
/// the link only while it still holds it in the epoch of `link`. A lost
/// request is sent again by the next bundle refresh.
pub type ReplicaLinkCloseFn = Box<dyn Fn(u16, ReplicaLinkId)>;

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

/// Listener TLS configuration shared with the shard owning each connection.
/// Per-connection TLS state is created only on the destination runtime.
pub type SharedTlsServerConfig = std::sync::Arc<rustls::ServerConfig>;

/// Callback invoked on every accepted SDK TCP-TLS client connection.
///
/// Fires after shard 0's TCP-TLS listener accepts a raw TCP socket.
/// The callback delegates the socket and its listener's configuration
/// before any TLS bytes are consumed. The destination shard runs
/// [`installer::install_client_tcp_tls`], owning the handshake and all I/O.
pub type AcceptedTlsClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, SharedTlsServerConfig)>;

/// Callback invoked on every accepted SDK WSS (WebSocket-over-TLS)
/// client connection.
///
/// Fires before either the TLS handshake or WebSocket upgrade. The callback
/// delegates the raw socket and its listener's configuration to the owning
/// shard, where [`installer::install_client_wss`] runs both handshakes and
/// subsequent I/O. No subprotocol negotiation is performed.
pub type AcceptedWssClientFn = std::rc::Rc<dyn Fn(compio::net::TcpStream, SharedTlsServerConfig)>;

/// Notifier fired when an installed replica link closes.
///
/// The link's writer / reader tasks invoke this once, after the owner
/// shard released the link's owner-table slot. The shard bootstrap wraps
/// this closure around a `try_send` into shard 0's inbox, where the close
/// tears the bundle of the peer down (see [`replica::bundle`]).
pub type ConnectionLostFn = std::rc::Rc<dyn Fn(ReplicaLinkId)>;

/// Socket-read accounting for this shard's plaintext replica links.
///
/// One instance covers every such link the shard owns, so the ratio of
/// decoded frames to completed socket reads is their aggregate batching
/// factor, which is what the read-ahead buffer exists to raise. Only shards
/// that own replica links read replica sockets, so a nonzero value also
/// identifies a shard that owns a replica link.
///
/// `Cell` rather than atomics: the reader task that bumps these and the
/// shard sweep that drains them run on the same compio thread. Shared
/// with the reader task through an `Rc`, on the precedent of
/// `installer::replica`'s `install_aborted`.
#[derive(Debug, Default)]
pub struct ReplicaReadStats {
    reads: Cell<u64>,
    frames: Cell<u64>,
}

impl ReplicaReadStats {
    /// One completed socket read on the replica read half.
    pub fn record_read(&self) {
        self.reads.set(self.reads.get() + 1);
    }

    /// One frame decoded out of the replica read half.
    pub fn record_frame(&self) {
        self.frames.set(self.frames.get() + 1);
    }

    /// Drain both counts, resetting them to zero.
    ///
    /// Take semantics because the caller feeds the result to a cumulative
    /// Prometheus counter's `inc_by`: handing back running totals would
    /// double count every sweep.
    pub const fn take(&self) -> ReplicaReadMetrics {
        ReplicaReadMetrics {
            reads: self.reads.replace(0),
            frames: self.frames.replace(0),
        }
    }
}

/// Deltas since the previous [`ReplicaReadStats::take`].
///
/// `frames >= reads` is not an invariant: a body that arrives in two
/// segments, or one that straddles the end of the read-ahead buffer,
/// costs two reads for one frame.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaReadMetrics {
    pub reads: u64,
    pub frames: u64,
}

/// Replica link events on one shard since the previous
/// [`MessageBus::take_replica_link_stats`].
///
/// Shard 0 tracks the bundles and reads the preambles, so the other shards
/// report zero for those fields.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaLinkStats {
    pub bundle_teardowns: u64,
    /// Accepted sockets closed before delegation: no preamble in time, or
    /// a preamble this node refuses.
    pub preamble_refusals: u64,
    /// Delegated handshakes that failed or ran out of `handshake_grace` on
    /// this shard, refused hellos included.
    pub handshake_failures: u64,
    /// Peers whose bundle is Up at the time of the take. A gauge, not a
    /// delta.
    pub bundles_up: u64,
}

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
/// `send_to_client` carries a Reply by client id; it has no in-protocol
/// retransmit. Real socket and HTTP targets require the installation
/// captured when dispatch admitted the request
/// ([`ConnectionInstaller::send_to_client_instance`]); a client id alone
/// cannot prove reply ownership, so [`IggyMessageBus`] refuses every frame
/// sent by id with `ClientNotFound`, and never forwards it to another shard.
/// A `Backpressure` from the client's own queue closes that connection, so
/// the client fails the pending request at once and recovers on a new
/// connection. A reply sent with
/// [`ConnectionInstaller::send_to_client_instance_waiting`] waits for room
/// instead, up to `client_write_stall_timeout`, and then closes the
/// connection the same way. A request the dispatch host relays to another
/// shard gets its reply back over the request's own channel, and the shard
/// holding the connection sends it as above. The
/// `{variant="forward_replica_send"}` drop counter stays informational
/// only.
///
/// # Setter install contract
///
/// The two setters below do NOT share one install rule:
/// - [`Self::set_connection_lost_fn`] is re-installable (`RefCell`
///   slot): a second install replaces the bundle close report.
/// - [`Self::set_replica_forward_fn`] is one-shot (`OnceCell` slot) and
///   panics on a second install, as a bootstrap-only wiring invariant.
///
/// A bus impl must preserve this divergence - see each method.
pub trait MessageBus {
    /// Process-wide monotonic microseconds, on the same timeline as `sleep`.
    /// Virtual-time implementations must override both methods together.
    fn monotonic_micros(&self) -> u64 {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        u64::try_from(ORIGIN.get_or_init(Instant::now).elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    /// Queue one frame for `client_id`. Takes anything that converts into a
    /// [`BusMessage`]: a single `Frozen<MESSAGE_ALIGN>` buffer or an
    /// already fragmented frame. [`IggyMessageBus`] refuses every such frame
    /// with `ClientNotFound`, because socket and HTTP targets need the
    /// installation captured at admission (see the recovery asymmetry above).
    fn send_to_client(
        &self,
        client_id: u128,
        data: impl Into<BusMessage>,
    ) -> impl Future<Output = Result<(), SendError>>;

    fn send_to_replica(
        &self,
        replica: u8,
        data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>>;

    /// Install a notifier the bus will invoke when an installed replica
    /// link closes. The shard bootstrap installs one on every shard that
    /// reports the close to shard 0, which tears the bundle of the peer
    /// down. The shard-shared [`ReplicaOwnerTable`] slot of the link is
    /// already released when the callback runs. Test code installs a
    /// callback to assert the notifier path fires exactly once per close.
    ///
    /// No default: every bus must explicitly opt in or stub. A silent
    /// no-op default previously masked a wiring bug where shard 0 never
    /// learned a delegated replica died.
    ///
    /// Re-installing is allowed by design: the backing slot is a
    /// `RefCell`, so a second install replaces the first, the bundle close
    /// report included. Tests rely on this to observe closes. This is the
    /// deliberate exception to the install rules of
    /// [`Self::set_replica_forward_fn`].
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

    /// Register a detached `compio::runtime::spawn` handle so
    /// [`shutdown`](IggyMessageBus::shutdown) can await it before the
    /// runtime drops. Production [`IggyMessageBus`] pushes onto a
    /// `RefCell<Vec<JoinHandle>>` drained on shutdown. Required (no default)
    /// so a new impl cannot silently drop detached handles by omission; a
    /// stub that spawns nothing implements it as a no-op.
    fn track_background(&self, handle: JoinHandle<()>);

    /// Timer used by shard-side loops (the message pump's consensus tick).
    ///
    /// Provided default is the wall-clock compio timer, which panics
    /// outside a compio runtime: loud by design, so a non-compio host
    /// that forgot to override cannot silently hang. The simulator
    /// overrides this with a virtual-time sleep so the pump's timing is
    /// a pure function of the simulation seed. Routed through the bus
    /// because the bus is the one dependency the shard already injects
    /// per environment; a separate clock generic would thread a second
    /// seam through every shard constructor for the same effect.
    ///
    /// Returned unboxed as an opaque `impl Future` (RPITIT), matching
    /// [`Self::send_to_client`] and [`Self::send_to_replica`]. The pump
    /// keeps this future in a pinned, re-armed slot (see `run_message_pump`
    /// in `core/shard/src/router.rs`); an opaque type stores there just as
    /// a concrete one does. Unboxed matters because `select_biased!` polls
    /// the tick arm on every frame (~1M/s/shard): a `Box<dyn Future>` would
    /// cost a vtable-dispatched poll each time and lose inlining of the
    /// timer poll, which dwarfs the once-per-tick allocation a box adds.
    ///
    /// # Contract for overrides
    ///
    /// The pump's re-arm slot relies on two behaviours the default and the
    /// simulator override both honour; a third implementor must preserve
    /// them or silently skew tick cadence or leak timers:
    /// - Deadline anchored at creation, not first poll. The future is built
    ///   once and polled many times, so the interval counts from when
    ///   `sleep` returns.
    /// - Drop cancels the timer. The pump replaces the slot on each fire and
    ///   drops it on stop, so a leaked timer would wake a dead task or
    ///   accumulate.
    ///
    /// This is also the one method here with a provided default, unlike the
    /// setters above that require an explicit opt-in. Deliberate: the
    /// default fails loud (compio panics outside its runtime) instead of
    /// silently no-opping, so a bus that forgets to override cannot hang
    /// undetected.
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        compio::time::sleep(duration)
    }

    /// Spawn a detached background task on the ambient runtime.
    ///
    /// The default spawns onto the compio runtime and detaches (fire and
    /// forget); the shell's off-pump poll IO, client-request drains, and
    /// metadata submits use it. Like [`Self::sleep`], the default fails
    /// loud outside a compio runtime rather than silently no-opping. The
    /// simulator overrides this to stage the future on its deterministic
    /// executor, so those tasks are scheduled and replayed by seed instead
    /// of escaping to a real runtime: task code cannot spawn on the sim
    /// executor directly, since that needs `&mut` it can never hold.
    ///
    /// Detached by contract: the join handle is dropped, so a caller that
    /// must observe completion arranges its own signalling. All current
    /// call sites are fire-and-forget.
    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        compio::runtime::spawn(future).detach();
    }

    /// Real (wall) time in microseconds since the Unix epoch.
    ///
    /// The default reads the system clock, which is the correct production
    /// source, so unlike [`Self::sleep`]/[`Self::spawn`] this default is a
    /// real implementation rather than a fail-loud stub. It lives on the bus
    /// for the same reason [`Self::sleep`] does: a shard-side handler that
    /// needs real time (the login path gates PAT expiry on it, and that
    /// accept/reject folds into the reply) must read a clock the environment
    /// injects, or it diverges on replay. The simulator overrides this with
    /// its seed-derived virtual clock.
    ///
    /// Distinct from the consensus prepare-timestamp clock
    /// (`VsrConsensus::clock_realtime_micros`), which exists only on the
    /// metadata owner (shard 0); this seam is present on every shard, so a
    /// request handled on any entry shard still reads deterministic time.
    fn realtime_micros(&self) -> u64 {
        iggy_common::IggyTimestamp::now().as_micros()
    }

    /// Monotonic time for local deadlines, independent of message-pump progress.
    /// Simulator overrides must share one timeline across replicas and restarts.
    fn monotonic_now(&self) -> Instant {
        Instant::now()
    }

    /// Drain this bus's plaintext replica socket-read counters.
    ///
    /// The provided default returns zeros so the simulator bus and the
    /// test buses need no override: they own no replica socket. The
    /// production bus takes from its own [`ReplicaReadStats`], which the
    /// reader task of every plaintext replica link shares.
    fn take_replica_read_stats(&self) -> ReplicaReadMetrics {
        ReplicaReadMetrics::default()
    }

    /// Drain the count of replica frames that [`Self::send_to_replica`]
    /// refused with [`SendError::Backpressure`] since the previous call.
    ///
    /// Zero by default for the same reason as
    /// [`Self::take_replica_read_stats`]: only the production bus owns
    /// replica peer queues.
    fn take_replica_outbound_drops(&self) -> u64 {
        0
    }

    /// Drain the replica link events of this bus. Zero by default for the
    /// same reason as [`Self::take_replica_read_stats`].
    fn take_replica_link_stats(&self) -> ReplicaLinkStats {
        ReplicaLinkStats::default()
    }
}

/// Production message bus backed by real TCP connections.
///
/// Owns:
/// - a root [`Shutdown`] / [`ShutdownToken`] that fans cancellation to every
///   accept loop, read loop, periodic task, and per-connection writer task,
/// - a [`ConnectionRegistry<u128>`] for clients (`u128` id is shard-packed),
/// - a [`ReplicaRegistry`] for replicas (`u8` id from the replica handshake,
///   backed by a fixed-size array to avoid hash lookup on every send),
/// - the `JoinHandle`s of background tasks (accept loops, reconnect periodic).
///
/// Send semantics:
/// - `send_to_*` clones the per-peer `Sender` out of the registry, calls
///   `try_send` on it, and returns. No `.await` happens in the body. Drops
///   on `Full` (returned as [`SendError::Backpressure`]) are recovered by
///   VSR retransmission for replica-bound consensus traffic. A client
///   reply refused on `Full` also closes that client's connection (see
///   [`ConnectionRegistry::try_send_to_instance`]). The one exception is
///   [`Self::send_to_client_instance_waiting`], which waits for room in the
///   client's queue for up to `client_write_stall_timeout`. A client reply
///   sent by id alone is refused, as the trait-level "Recovery asymmetry"
///   section on [`MessageBus`] explains.
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
    /// Close notifier invoked by `Self::notify_connection_lost` after it
    /// releases the link's owner-table slot. The shard bootstrap installs
    /// the report to shard 0; tests install a callback to observe the path.
    connection_lost_fn: RefCell<Option<ConnectionLostFn>>,
    /// Shard 0 only: asks the owner shard of a link to close it. See
    /// [`ReplicaLinkCloseFn`].
    replica_link_close_fn: OnceCell<ReplicaLinkCloseFn>,
    /// Invoked when a client connection metadata entry is removed during
    /// connection teardown.
    client_connection_lost_fn: RefCell<Option<ClientConnectionLostFn>>,
    /// Per-connection metadata exposed to the caller via
    /// [`Self::client_meta`], with the installation it belongs to. Populated
    /// by the install path on successful registry insert; removed on that
    /// installation's teardown only, so a late teardown spares a reinstall.
    client_meta: RefCell<ahash::AHashMap<u128, (InstanceToken, Rc<ClientConnMeta>)>>,
    /// Shard-shared atomic owner table. Claimed by the install path
    /// before a registry insert; released by
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
    /// Links with a delegated outbound handshake still in flight:
    /// (replica id, link) -> expiry deadline. Consulted by the reconnect
    /// sweep so a link is not redialed while its previous handshake is
    /// pending; entries expire lazily inside [`Self::check_dial_pending`].
    /// Only shard 0's bus ever populates this.
    pending_dials: RefCell<ahash::AHashMap<(u8, u16), Instant>>,
    /// Shard 0 only: the bundle of every peer, in both roles. A peer
    /// without an entry is Down in epoch 0.
    bundles: RefCell<ahash::AHashMap<u8, Bundle>>,
    /// Drained through [`MessageBus::take_replica_link_stats`]; the
    /// `bundles_up` field is filled at the take.
    replica_link_stats: Cell<ReplicaLinkStats>,
    /// Socket-read counters shared by the reader task of every plaintext
    /// replica link this shard owns. Drained through
    /// [`MessageBus::take_replica_read_stats`] by the shard's tick;
    /// stays at zero on a shard that owns no such link.
    replica_read_stats: Rc<ReplicaReadStats>,
    /// Replica frames refused at a full peer queue on the fast path of
    /// [`MessageBus::send_to_replica`]. Forwarded frames count here too,
    /// because the link shard re-enters that path for them.
    replica_outbound_drops: Cell<u64>,
}

impl IggyMessageBus {
    /// Construct a bus with default tunables (derived from
    /// [`ServerConfig::default`]).
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

    /// Construct a bus from the validated the server schema.
    ///
    /// Single-bus constructor: takes a fully-validated
    /// [`ServerConfig`] and derives the runtime [`MessageBusConfig`]
    /// internally. Field conversions ([`iggy_common::IggyDuration`] -> [`Duration`],
    /// [`iggy_common::IggyByteSize`] -> `usize`, schema WS knobs ->
    /// tungstenite [`WebSocketConfig`]) happen once here so hot paths
    /// read pre-converted values.
    ///
    /// The bus gets its own owner table with the one-shard, one-link
    /// [`LinkLayout::default`], so `replica_connections_per_peer` has no
    /// effect. The server uses [`Self::with_config_and_owner_table`].
    ///
    /// # Panics
    ///
    /// Panics if `cfg.message_bus.max_batch == 0` or
    /// `cfg.message_bus.max_batch > IOV_MAX_LIMIT`. Boot-time validation;
    /// surfaces operator misconfiguration loudly rather than letting
    /// every `writev` fail silently with `EMSGSIZE` once traffic starts.
    #[must_use]
    pub fn with_config(shard_id: u16, cfg: &ServerConfig) -> Self {
        Self::with_tunables(shard_id, MessageBusConfig::from(cfg))
    }

    /// Production constructor for multi-shard the server: same as
    /// [`Self::with_config`] but takes a pre-allocated
    /// [`ReplicaOwnerTable`] Arc. Bootstrap allocates one table per
    /// server process and clones the Arc into every shard so all
    /// buses see the same atomic slots.
    #[must_use]
    pub fn with_config_and_owner_table(
        shard_id: u16,
        cfg: &ServerConfig,
        owner_table: Arc<ReplicaOwnerTable>,
    ) -> Self {
        Self::with_tunables_and_owner_table(shard_id, MessageBusConfig::from(cfg), owner_table)
    }

    /// Construct a bus from already-derived runtime tunables.
    ///
    /// Used by the public constructors above and by tests that need to
    /// patch a single field on the derived [`MessageBusConfig`] without
    /// round-tripping through [`ServerConfig`].
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
    /// owner table. Server bootstrap uses this so every shard's bus
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
            connection_lost_fn: RefCell::new(None),
            replica_link_close_fn: OnceCell::new(),
            client_connection_lost_fn: RefCell::new(None),
            client_meta: RefCell::new(ahash::AHashMap::new()),
            owner_table,
            replica_handshake_ctx: OnceCell::new(),
            replica_handshake_slots: RefCell::new(ahash::AHashMap::new()),
            replica_slot_seq: Cell::new(0),
            pending_dials: RefCell::new(ahash::AHashMap::new()),
            bundles: RefCell::new(ahash::AHashMap::new()),
            replica_link_stats: Cell::new(ReplicaLinkStats::default()),
            replica_read_stats: Rc::new(ReplicaReadStats::default()),
            replica_outbound_drops: Cell::new(0),
        }
    }

    /// Clone of the shard-shared owner table Arc.
    #[must_use]
    pub fn owner_table(&self) -> Arc<ReplicaOwnerTable> {
        Arc::clone(&self.owner_table)
    }

    /// Placement of the replica links of this node.
    #[must_use]
    pub fn link_layout(&self) -> LinkLayout {
        self.owner_table.layout()
    }

    /// Claim the owner-table slot of `link` for this shard. Returns
    /// `false` when an install of any epoch holds it; the caller must drop
    /// the connection.
    ///
    /// Called by [`installer::install_replica_conn`] **before** spawning
    /// tasks or inserting into the local registry, so two installs of one
    /// (peer, link) never both reach a registry.
    #[must_use]
    pub fn mark_replica_owned(&self, link: ReplicaLinkId) -> bool {
        self.owner_table.try_claim(link, self.shard_id)
    }

    /// Release the owner-table slot of `link` if this shard still holds it
    /// in the epoch of `link`.
    pub fn clear_replica_owned(&self, link: ReplicaLinkId) -> bool {
        self.owner_table.release(link, self.shard_id)
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

    /// Record that link `link` to `replica_id` was dialed and delegated and
    /// its dialer handshake is in flight on the owning shard. The
    /// reconnect sweep skips pending links; the entry is cleared by the
    /// handshake-outcome ack ([`Self::clear_dial_pending`]) or expires
    /// on its own after `handshake_grace` plus margin.
    pub fn mark_dial_pending(&self, replica_id: u8, link: u16) {
        self.pending_dials
            .borrow_mut()
            .insert((replica_id, link), self.replica_handshake_deadline());
    }

    /// Whether an outbound handshake of link `link` to `replica_id` is
    /// still in flight. An expired entry is removed here (lazy expiry) and
    /// reported as not pending, re-enabling the redial.
    #[must_use]
    pub fn check_dial_pending(&self, replica_id: u8, link: u16) -> bool {
        let mut pending = self.pending_dials.borrow_mut();
        match pending.get(&(replica_id, link)) {
            Some(deadline) if *deadline > Instant::now() => true,
            Some(_) => {
                pending.remove(&(replica_id, link));
                false
            }
            None => false,
        }
    }

    /// Clear the pending-dial entry of link `link` to `replica_id`.
    /// Idempotent; returns whether an entry was present.
    pub fn clear_dial_pending(&self, replica_id: u8, link: u16) -> bool {
        self.pending_dials
            .borrow_mut()
            .remove(&(replica_id, link))
            .is_some()
    }

    /// Shard 0: read the placement preamble of an accepted replica socket
    /// and place its link. Returns the link, in the current epoch of the
    /// dialer's bundle, and the shard that owns it.
    ///
    /// Reads exactly the preamble bytes, before `deadline`, so the owner
    /// shard still finds the TLS `ClientHello` or the hello first.
    ///
    /// # Errors
    ///
    /// See [`PreambleError`]. The caller closes the socket.
    #[allow(clippy::future_not_send)]
    pub async fn place_replica_inbound(
        &self,
        stream: &mut compio::net::TcpStream,
        deadline: Instant,
    ) -> Result<(ReplicaLinkId, u16), PreambleError> {
        let placed = self.read_and_place_preamble(stream, deadline).await;
        if placed.is_err() {
            self.count_link_event(|stats| stats.preamble_refusals += 1);
        }
        placed
    }

    #[allow(clippy::future_not_send)]
    async fn read_and_place_preamble(
        &self,
        stream: &mut compio::net::TcpStream,
        deadline: Instant,
    ) -> Result<(ReplicaLinkId, u16), PreambleError> {
        let (self_id, replica_count) = self
            .replica_handshake_ctx()
            .map(|ctx| (ctx.self_id, ctx.replica_count))
            .ok_or(PreambleError::NoHandshakeContext)?;
        let budget = deadline.saturating_duration_since(Instant::now());
        let bytes = replica::preamble::read_preamble(stream, budget).await?;
        let layout = self.link_layout();
        let preamble = Preamble::admit(bytes, self_id, replica_count, layout.links())?;
        let link = ReplicaLinkId {
            replica: preamble.dialer,
            link: preamble.link,
            epoch: self.bundle(preamble.dialer).epoch(),
        };
        let owner = layout.link_owner(preamble.link, peer_slot(self_id, preamble.dialer));
        Ok((link, owner))
    }

    /// Shard 0: the links to `replica` to dial now. Empty while the bundle
    /// of the peer is Up or tearing down.
    #[must_use]
    pub fn replica_links_to_dial(&self, replica: u8) -> Vec<ReplicaLinkId> {
        let epoch = self.bundle(replica).epoch();
        (0..self.link_layout().links())
            .map(|link| ReplicaLinkId {
                replica,
                link,
                epoch,
            })
            .filter(|&link| self.wants_replica_dial(link))
            .collect()
    }

    /// Shard 0: whether `link` is still missing from the current bundle of
    /// its peer and no handshake of it is in flight. A dial that outlived
    /// a teardown fails this check and is dropped.
    #[must_use]
    pub fn wants_replica_dial(&self, link: ReplicaLinkId) -> bool {
        let bundle = self.bundle(link.replica);
        bundle.dials()
            && bundle.epoch() == link.epoch
            && self.owner_table.claim(link.replica, link.link).is_none()
            && !self.check_dial_pending(link.replica, link.link)
    }

    /// Shard 0: move the bundle of `replica` to match the owner table, then
    /// apply what changed. Runs after every handshake outcome and on every
    /// reconnect sweep.
    pub fn refresh_replica_bundle(&self, replica: u8) {
        self.update_bundle(replica, |bundle, claims, now, forming_limit| {
            bundle.refresh(claims, now, forming_limit)
        });
    }

    /// Shard 0: an installed link closed. A close in the current epoch of
    /// the peer's bundle tears the bundle down.
    pub fn replica_link_closed(&self, link: ReplicaLinkId) {
        self.update_bundle(link.replica, |bundle, claims, now, forming_limit| {
            bundle.link_closed(link.epoch, claims, now, forming_limit)
        });
    }

    /// Close `link` if this shard holds it in the epoch of `link`. The
    /// connection's own exit path then releases the slot and reports the
    /// close to shard 0.
    pub fn close_replica_link(&self, link: ReplicaLinkId) {
        let held = Some(LinkClaim {
            owner: self.shard_id,
            epoch: link.epoch,
        });
        // A shard holds at most one link per peer, so while the claim names
        // this shard the registry entry of the peer is this link.
        if self.owner_table.claim(link.replica, link.link) == held
            && self.replicas.shutdown_connection(link.replica)
        {
            info!(
                replica = link.replica,
                link = link.link,
                epoch = link.epoch,
                "closing replica link of a torn down bundle"
            );
        }
    }

    /// Install the closure shard 0 uses to close links on other shards.
    /// Bootstrap-only wiring; same one-shot contract as
    /// [`Self::set_replica_forward_fn`].
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same bus.
    pub fn set_replica_link_close_fn(&self, f: ReplicaLinkCloseFn) {
        assert!(
            self.replica_link_close_fn.set(f).is_ok(),
            "replica_link_close_fn installed twice (bootstrap invariant)"
        );
    }

    fn bundle(&self, replica: u8) -> Bundle {
        self.bundles
            .borrow()
            .get(&replica)
            .copied()
            .unwrap_or_default()
    }

    fn update_bundle(
        &self,
        replica: u8,
        step: impl FnOnce(&mut Bundle, &[Option<LinkClaim>], Instant, Duration) -> BundleChange,
    ) {
        let claims: Vec<Option<LinkClaim>> = (0..self.link_layout().links())
            .map(|link| self.owner_table.claim(replica, link))
            .collect();
        let (epoch, change) = {
            let mut bundles = self.bundles.borrow_mut();
            let bundle = bundles.entry(replica).or_default();
            let change = step(bundle, &claims, Instant::now(), self.config.handshake_grace);
            (bundle.epoch(), change)
        };
        self.apply_bundle_change(replica, epoch, change);
    }

    fn apply_bundle_change(&self, replica: u8, epoch: u32, change: BundleChange) {
        let expected = self.config.mesh_expected_peers;
        if change.tore_down {
            // Before the closes, so no shard queues another frame on them.
            self.owner_table.set_up(replica, false);
            self.count_link_event(|stats| stats.bundle_teardowns += 1);
            warn!(
                replica,
                epoch, "replica bundle torn down, closing its links"
            );
            if expected > 0 {
                warn!(
                    connected = self.bundles_up(),
                    expected, "replica mesh degraded"
                );
            }
        }
        for (link, claim) in change.stale {
            let link = ReplicaLinkId {
                replica,
                link,
                epoch: claim.epoch,
            };
            if claim.owner == self.shard_id {
                self.close_replica_link(link);
            } else if let Some(close) = self.replica_link_close_fn.get() {
                close(claim.owner, link);
            } else {
                warn!(
                    replica,
                    owner = claim.owner,
                    "no replica link close route installed; stale link stays open"
                );
            }
        }
        if change.came_up {
            self.owner_table.set_up(replica, true);
            info!(
                replica,
                epoch,
                links = self.link_layout().links(),
                "replica bundle up"
            );
            if expected > 0 {
                let connected = self.bundles_up();
                if connected == expected {
                    info!(
                        expected,
                        "replica mesh complete: all peer connections established"
                    );
                } else {
                    info!(connected, expected, "replica mesh forming");
                }
            }
        }
    }

    fn bundles_up(&self) -> usize {
        self.bundles
            .borrow()
            .values()
            .filter(|bundle| bundle.phase() == BundlePhase::Up)
            .count()
    }

    pub(crate) fn count_link_event(&self, count: impl FnOnce(&mut ReplicaLinkStats)) {
        let mut stats = self.replica_link_stats.get();
        count(&mut stats);
        self.replica_link_stats.set(stats);
    }

    /// Look up the per-connection metadata recorded for `client_id`.
    ///
    /// Returns `None` if the client never connected on this bus or if
    /// its connection has already been torn down.
    #[must_use]
    pub fn client_meta(&self, client_id: u128) -> Option<Rc<ClientConnMeta>> {
        self.client_meta
            .borrow()
            .get(&client_id)
            .map(|(_, meta)| Rc::clone(meta))
    }

    /// The installation currently serving `client_id` on this shard, or
    /// `None` when no connection is installed under it here.
    #[must_use]
    pub fn client_instance(&self, client_id: u128) -> Option<InstanceToken> {
        self.clients.instance(client_id)
    }

    /// [`MessageBus::send_to_client`] for the installation `instance` names,
    /// as [`Self::client_instance`] captured it on this bus.
    ///
    /// A reply that outlived its connection reports `ClientNotFound` instead
    /// of reaching a later connection under the same id, and a full queue
    /// closes only the installation the reply belongs to. `None` means no
    /// connection was installed at capture, so nothing can take the reply.
    /// Never forwarded: an installation exists only on the shard that owns
    /// the connection.
    ///
    /// # Errors
    ///
    /// The local-path errors of [`MessageBus::send_to_client`].
    pub fn send_to_client_instance(
        &self,
        client_id: u128,
        instance: Option<InstanceToken>,
        message: BusMessage,
    ) -> Result<(), SendError> {
        let instance = self.reply_instance(client_id, instance)?;
        let route = self
            .clients
            .try_send_to_instance(client_id, instance, message);
        self.finish_local_reply(client_id, route)
    }

    /// [`Self::send_to_client_instance`] that waits for room in a full queue
    /// instead of closing the connection. Unlike every other send, it yields.
    /// A queue that stays full for `client_write_stall_timeout` closes the
    /// connection, because its client stopped reading.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::send_to_client_instance`]. `Backpressure` means
    /// the wait stalled and the connection was closed.
    #[allow(clippy::future_not_send)]
    pub async fn send_to_client_instance_waiting(
        &self,
        client_id: u128,
        instance: Option<InstanceToken>,
        message: BusMessage,
    ) -> Result<(), SendError> {
        let instance = self.reply_instance(client_id, instance)?;
        let route = self
            .clients
            .send_to_instance(
                client_id,
                instance,
                message,
                self.config.client_write_stall_timeout,
            )
            .await;
        self.finish_local_reply(client_id, route)
    }

    /// The installation a reply goes to, while the bus still sends.
    fn reply_instance(
        &self,
        client_id: u128,
        instance: Option<InstanceToken>,
    ) -> Result<InstanceToken, SendError> {
        if self.is_shutting_down() {
            return Err(SendError::BusShuttingDown);
        }
        instance.ok_or(SendError::ClientNotFound(client_id))
    }

    /// Map a socket send onto the bus error, or fire the in-process waiter
    /// of the reply's request.
    fn finish_local_reply(&self, client_id: u128, route: ReplyRoute) -> Result<(), SendError> {
        match route {
            ReplyRoute::Delivered(send_result) => send_result.map_err(map_try_send_err),
            ReplyRoute::InProcess(mut message) => {
                let request = reply_request_id(&message);
                let receipt = message.take_write_receipt();
                self.clients
                    .fire_in_process(client_id, request, message)
                    .map_err(|_| SendError::ClientNotFound(client_id))?;
                if let Some(receipt) = receipt {
                    receipt.complete();
                }
                Ok(())
            }
            ReplyRoute::NoSlot(_message) => Err(SendError::ClientNotFound(client_id)),
            ReplyRoute::Stalled => Err(SendError::Backpressure),
        }
    }

    pub(crate) fn insert_client_meta(&self, meta: Rc<ClientConnMeta>, instance: InstanceToken) {
        self.client_meta
            .borrow_mut()
            .insert(meta.client_id, (instance, meta));
    }

    /// Remove the metadata of the installation `instance` names and report
    /// its connection lost. A no-op once a reinstall holds `client_id`: the
    /// metadata, and the session the dispatch host keys by that id, belong
    /// to the reinstall.
    pub(crate) fn remove_client_meta(&self, client_id: u128, instance: InstanceToken) {
        let removed = {
            let mut metas = self.client_meta.borrow_mut();
            let owned = metas
                .get(&client_id)
                .is_some_and(|(token, _)| *token == instance);
            owned && metas.remove(&client_id).is_some()
        };
        if removed {
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

    /// Release the slot of a closed link, then invoke the registered
    /// connection-lost notifier, if any.
    ///
    /// Every caller removed the link's registry entry first. No newer
    /// install of the link can sit in the registry here: it needs the slot,
    /// and the slot is still claimed until the release below.
    ///
    /// Clones the `Rc` out of the `RefCell` borrow before invoking the
    /// closure so the closure body is free to call
    /// [`Self::set_connection_lost_fn`] (which takes a `borrow_mut`)
    /// without tripping the runtime borrow check.
    pub(crate) fn notify_connection_lost(&self, link: ReplicaLinkId, reason: &'static str) {
        // Release first so any reader racing the callback already sees the
        // slot vacated.
        self.clear_replica_owned(link);
        warn!(
            shard_id = self.shard_id,
            replica_id = link.replica,
            link = link.link,
            epoch = link.epoch,
            reason,
            "replica connection lost"
        );
        let cb = self
            .connection_lost_fn
            .borrow()
            .as_ref()
            .map(std::rc::Rc::clone);
        if let Some(f) = cb {
            f(link);
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

    /// The shard that owns link `link` to `replica`, if it is installed.
    ///
    /// Reads the shard-shared [`ReplicaOwnerTable`], which the owning
    /// shard's installer claims synchronously and
    /// `Self::notify_connection_lost` releases on close.
    #[must_use]
    pub fn owning_shard(&self, replica: u8, link: u16) -> Option<u16> {
        self.owner_table
            .claim(replica, link)
            .map(|claim| claim.owner)
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

    /// Handle every plaintext replica reader task on this shard bumps.
    #[must_use]
    pub fn replica_read_stats(&self) -> Rc<ReplicaReadStats> {
        Rc::clone(&self.replica_read_stats)
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
        // Connection tasks skip per-client cleanup during bus shutdown.
        self.client_meta.borrow_mut().clear();
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
        data: impl Into<BusMessage>,
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

    fn track_background(&self, handle: JoinHandle<()>) {
        (**self).track_background(handle);
    }

    // Forward the defaulted methods too. Without this, a `Rc`-wrapped bus that
    // overrides `sleep`/`spawn`/clock reads (the simulator's virtual timer,
    // spawn queue, and virtual clock) would be silently reverted to the defaults
    // through the wrapper. No-op for production, whose `IggyMessageBus` uses those
    // defaults anyway.
    fn monotonic_micros(&self) -> u64 {
        (**self).monotonic_micros()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        (**self).sleep(duration)
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        (**self).spawn(future);
    }

    fn realtime_micros(&self) -> u64 {
        (**self).realtime_micros()
    }

    fn monotonic_now(&self) -> Instant {
        (**self).monotonic_now()
    }

    fn take_replica_read_stats(&self) -> ReplicaReadMetrics {
        (**self).take_replica_read_stats()
    }

    fn take_replica_outbound_drops(&self) -> u64 {
        (**self).take_replica_outbound_drops()
    }

    fn take_replica_link_stats(&self) -> ReplicaLinkStats {
        (**self).take_replica_link_stats()
    }
}

#[allow(clippy::future_not_send)]
impl MessageBus for IggyMessageBus {
    async fn send_to_client(
        &self,
        client_id: u128,
        _message: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        if self.is_shutting_down() {
            return Err(SendError::BusShuttingDown);
        }
        Err(SendError::ClientNotFound(client_id))
    }

    async fn send_to_replica(
        &self,
        replica: u8,
        message: Frozen<MESSAGE_ALIGN>,
    ) -> Result<(), SendError> {
        if self.is_shutting_down() {
            return Err(SendError::BusShuttingDown);
        }
        if !self.owner_table.is_up(replica) {
            return Err(SendError::ReplicaNotConnected(replica));
        }
        let own_link = self.link_layout().range_of(self.shard_id);
        let link = if routes_to_metadata(&message) {
            METADATA_LINK
        } else {
            own_link
        };
        // Fast path: this shard owns the link. A shard owns at most one link
        // per peer, the link of its own range, so the registry entry of the
        // peer is that link. On no-slot the registry returns the message
        // unchanged so the slow path can forward it via the inter-shard
        // channel without a wasted clone.
        let message = if link == own_link {
            match self.replicas.try_send_or_return(replica, message.into()) {
                Ok(send_result) => {
                    let result = send_result.map_err(map_try_send_err);
                    if matches!(result, Err(SendError::Backpressure)) {
                        self.replica_outbound_drops
                            .set(self.replica_outbound_drops.get() + 1);
                    }
                    return result;
                }
                Err(message) => message.into_contiguous(),
            }
        } else {
            message
        };
        // Slow path: route via the inter-shard channel to the owning shard.
        // The shard-shared `owner_table` is the authoritative routing
        // view: `mark_replica_owned` claims it on install and
        // `clear_replica_owned` releases it on close.
        let owning_shard = self
            .owning_shard(replica, link)
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

    fn track_background(&self, handle: JoinHandle<()>) {
        Self::track_background(self, handle);
    }

    fn take_replica_read_stats(&self) -> ReplicaReadMetrics {
        self.replica_read_stats.take()
    }

    fn take_replica_outbound_drops(&self) -> u64 {
        self.replica_outbound_drops.replace(0)
    }

    fn take_replica_link_stats(&self) -> ReplicaLinkStats {
        ReplicaLinkStats {
            bundles_up: self.bundles_up() as u64,
            ..self.replica_link_stats.take()
        }
    }
}

/// Whether the peer routes `message` to its metadata group by command alone.
/// An unknown command byte is not such a frame; the peer refuses it anyway.
fn routes_to_metadata(message: &Frozen<MESSAGE_ALIGN>) -> bool {
    message
        .as_slice()
        .get(COMMAND_OFFSET)
        .and_then(|&byte| bytemuck::checked::try_cast::<u8, Command>(byte).ok())
        .is_some_and(MessageBag::routes_to_metadata)
}

/// Extract the owning shard from a client id.
///
/// Shard 0 mints client ids with `target_shard_id` in the top 16 bits. Those
/// bits encode which shard's bus registry holds the connection; any shard
/// that needs to reply to this client uses this accessor to decide between
/// the fast path (local) and the slow path (forward via inter-shard).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub const fn client_id_owning_shard(client_id: u128) -> u16 {
    (client_id >> 112) as u16
}

/// Reserved client id stamped on server-generated auto-commit
/// `StoreConsumerOffset` ops (a poll's `auto_commit` replicated for failover).
///
/// Never belongs to a live connection: `mint_client_id` puts the shard in the
/// top 16 bits and no real shard is `u16::MAX`, so `u128::MAX` is
/// unreachable. The commit path recognises it and skips the (unwaited) reply,
/// keeping an unrequested frame off a real client's lockstep stream. Nonzero,
/// so it still satisfies the wire header's `client != 0` validation.
pub const AUTO_COMMIT_CLIENT_ID: u128 = u128::MAX;

/// Internal clients cannot collide with minted ids, whose shard is below `u16::MAX`.
const SERVER_INTERNAL_CLIENT_HIGH: u128 = u128::MAX << u64::BITS;

/// Keep completions for one key on the same internal client across retries.
#[must_use]
pub const fn server_internal_client_id(key: u64) -> u128 {
    SERVER_INTERNAL_CLIENT_HIGH | key as u128
}

/// Recognize the reserved high half shared by completions and auto-commits.
#[must_use]
pub const fn is_server_internal_client(client_id: u128) -> bool {
    client_id & SERVER_INTERNAL_CLIENT_HIGH == SERVER_INTERNAL_CLIENT_HIGH
}

/// Whether `client_id` is the reserved [`AUTO_COMMIT_CLIENT_ID`] sentinel.
#[must_use]
pub const fn is_auto_commit_client(client_id: u128) -> bool {
    client_id == AUTO_COMMIT_CLIENT_ID
}

/// Map an `async_channel::TrySendError` onto the bus-level [`SendError`].
///
/// Shape-matches `Result::map_err` (takes the error by value) so it can be
/// used directly as a function reference rather than a closure.
#[allow(clippy::needless_pass_by_value)] // signature required by map_err
fn map_try_send_err(e: async_channel::TrySendError<BusMessage>) -> SendError {
    match e {
        async_channel::TrySendError::Full(_) => SendError::Backpressure,
        async_channel::TrySendError::Closed(_) => SendError::ConnectionClosed,
    }
}

/// Peek `ReplyHeader.request` (the originating request id) from a reply
/// buffer at its fixed header offset. Only the in-process reply path calls
/// this; the socket path never decodes.
fn reply_request_id(reply: &BusMessage) -> u64 {
    const OFFSET: usize = std::mem::offset_of!(ReplyHeader, request);
    reply
        .first()
        .as_slice()
        .get(OFFSET..OFFSET + std::mem::size_of::<u64>())
        .and_then(|bytes| bytes.try_into().ok())
        // Native-endian is safe: this id never leaves the process, so writer and
        // reader share endianness. A wire reader uses little-endian instead (see
        // the SDK's `read_reply_status`).
        .map_or(0, u64::from_ne_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command, HEADER_SIZE};
    use std::cell::RefCell;

    #[test]
    fn given_internal_client_keys_when_encoded_should_remain_reserved() {
        for key in [0, 1, u64::MAX / 2, u64::MAX] {
            let client_id = server_internal_client_id(key);
            assert!(is_server_internal_client(client_id));
            assert_eq!(client_id & u128::from(u64::MAX), u128::from(key));
        }
        assert!(is_server_internal_client(AUTO_COMMIT_CLIENT_ID));
        assert!(!is_server_internal_client(0));
        assert!(!is_server_internal_client(SERVER_INTERNAL_CLIENT_HIGH - 1));
    }

    #[allow(clippy::cast_possible_truncation)]
    fn command_message(command: Command) -> Message<GenericHeader> {
        Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
            h.command = command;
            h.size = HEADER_SIZE as u32;
        })
    }

    fn dummy_message() -> Message<GenericHeader> {
        command_message(Command::Prepare)
    }

    const fn link(replica: u8, link: u16, epoch: u32) -> ReplicaLinkId {
        ReplicaLinkId {
            replica,
            link,
            epoch,
        }
    }

    fn bus_with_layout(shard_id: u16, shards: u16, links: u16) -> IggyMessageBus {
        let layout = LinkLayout::new(shards, links, true, 2).expect("valid layout");
        IggyMessageBus::with_tunables_and_owner_table(
            shard_id,
            MessageBusConfig::default(),
            Arc::new(ReplicaOwnerTable::with_layout(layout)),
        )
    }

    /// Records `(replica, owner shard)` of every forwarded frame.
    fn capture_forwards(bus: &IggyMessageBus) -> Rc<RefCell<Vec<(u8, u16)>>> {
        let forwards = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&forwards);
        bus.set_replica_forward_fn(Box::new(move |replica, owner, _message| {
            sink.borrow_mut().push((replica, owner));
            Ok(())
        }));
        forwards
    }

    /// Records `(owner shard, link)` of every close request shard 0 sends.
    fn capture_close_requests(bus: &IggyMessageBus) -> Rc<RefCell<Vec<(u16, ReplicaLinkId)>>> {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let sink = Rc::clone(&requests);
        bus.set_replica_link_close_fn(Box::new(move |owner, link| {
            sink.borrow_mut().push((owner, link));
        }));
        requests
    }

    /// A registry entry for `replica` whose per-connection token the test
    /// can observe.
    fn insert_replica_entry(bus: &IggyMessageBus, replica: u8) -> ShutdownToken {
        let (sender, _receiver) = async_channel::bounded(8);
        let writer = compio::runtime::spawn(async {});
        let reader = compio::runtime::spawn(async {});
        let (conn_shutdown, conn_token) = Shutdown::new();
        bus.replicas()
            .insert(replica, sender, writer, reader, conn_shutdown)
            .expect("free registry slot");
        conn_token
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
            h.command = Command::Reply;
            h.size = HEADER_SIZE as u32;
            h.request = request;
        })
    }

    /// An HTTP reply needs its captured installation as well as the request
    /// id. A raw client id must leave the legitimate waiter untouched.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_instance_in_process_fires_matching_reply_slot() {
        let bus = IggyMessageBus::new(0);
        let client_id = 1u128;
        bus.clients()
            .insert_in_process(client_id)
            .expect("fresh key");
        let (_guard, reply_rx) = bus
            .clients()
            .install_reply_slot(client_id, 42)
            .expect("slot installs");

        let message = reply_message(42).into_generic().into_frozen();
        assert!(matches!(
            bus.send_to_client(client_id, message.clone()).await,
            Err(SendError::ClientNotFound(_))
        ));
        bus.send_to_client_instance(client_id, bus.client_instance(client_id), message.into())
            .expect("in-process delivery");

        let received = reply_rx.await.expect("reply delivered");
        assert_eq!(reply_request_id(&received), 42);
    }

    /// Reply whose request id has no waiter (timed-out caller already
    /// removed its slot): shed as `ClientNotFound`, nothing panics.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_client_instance_in_process_sheds_reply_without_waiter() {
        let bus = IggyMessageBus::new(0);
        let client_id = 1u128;
        bus.clients()
            .insert_in_process(client_id)
            .expect("fresh key");

        let err = bus
            .send_to_client_instance(
                client_id,
                bus.client_instance(client_id),
                reply_message(42).into_generic().into_frozen().into(),
            )
            .unwrap_err();
        assert!(matches!(err, SendError::ClientNotFound(_)));
    }

    /// A socket takes only replies addressed to its installation. A reply by
    /// client id carries the id a request header names, which a client picks
    /// when it registers, so it must neither reach a socket installed under
    /// that id nor close it on a full queue.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_socket_client_when_replying_by_client_id_should_neither_deliver_nor_close() {
        const STALLED_QUEUE_CAPACITY: usize = 1;
        let bus = IggyMessageBus::new(0);
        let client_id = 0x2a_u128;
        let (sender, queue) = async_channel::bounded(STALLED_QUEUE_CAPACITY);
        let (conn_shutdown, conn_token) = Shutdown::new();
        let writer_token = conn_token.clone();
        let writer = compio::runtime::spawn(async move { writer_token.wait().await });
        let reader_token = conn_token.clone();
        let reader = compio::runtime::spawn(async move { reader_token.wait().await });
        let instance = bus
            .clients()
            .insert(client_id, sender, writer, reader, conn_shutdown)
            .expect("the client id is free");

        let by_id = bus
            .send_to_client(client_id, dummy_message().into_frozen())
            .await;
        assert!(matches!(by_id, Err(SendError::ClientNotFound(_))));
        assert!(queue.is_empty(), "a reply by id must not reach a socket");

        bus.send_to_client_instance(
            client_id,
            Some(instance),
            dummy_message().into_frozen().into(),
        )
        .expect("a reply addressed to the installation is delivered");
        let by_id = bus
            .send_to_client(client_id, dummy_message().into_frozen())
            .await;
        assert!(matches!(by_id, Err(SendError::ClientNotFound(_))));
        assert!(
            !conn_token.is_triggered(),
            "a reply by id must not close a socket whose queue is full"
        );

        let refused = bus.send_to_client_instance(
            client_id,
            Some(instance),
            dummy_message().into_frozen().into(),
        );
        assert!(matches!(refused, Err(SendError::Backpressure)));
        assert!(
            conn_token.is_triggered(),
            "a full queue still closes the installation the reply belongs to"
        );
    }

    /// The owning shard refuses a frame without its installation too, so a
    /// by-id frame for a client homed elsewhere is refused here, not forwarded.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn given_client_homed_on_another_shard_when_sending_by_id_should_refuse_without_forwarding()
     {
        let bus = IggyMessageBus::new(0);
        let client_id = (9u128 << 112) | 1;
        let err = bus
            .send_to_client(client_id, dummy_message().into_frozen())
            .await
            .unwrap_err();
        assert!(matches!(err, SendError::ClientNotFound(id) if id == client_id));
    }

    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_replica_slow_path_uses_owner_table_and_forwards() {
        let bus = IggyMessageBus::new(0);
        let forwards = capture_forwards(&bus);
        assert!(bus.owner_table().try_claim(link(5, 0, 0), 3));
        bus.refresh_replica_bundle(5);

        bus.send_to_replica(5, dummy_message().into_frozen())
            .await
            .expect("forward ok");

        assert_eq!(*forwards.borrow(), [(5, 3)]);
    }

    /// Frames flow only once every link of the bundle is installed, a group
    /// frame takes the link of the sending shard's range, and a frame the
    /// peer routes to its metadata group takes link 0.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn send_to_replica_waits_for_the_bundle_and_routes_metadata_over_link_0() {
        const PEER: u8 = 1;
        // 4 shards, 2 links: range 0 = shards {0, 1}, range 1 = {2, 3}.
        let bus = bus_with_layout(3, 4, 2);
        let forwards = capture_forwards(&bus);
        assert!(bus.owner_table().try_claim(link(PEER, 1, 0), 2));
        bus.refresh_replica_bundle(PEER);
        assert!(
            matches!(
                bus.send_to_replica(PEER, dummy_message().into_frozen())
                    .await,
                Err(SendError::ReplicaNotConnected(PEER))
            ),
            "a bundle with a missing link must refuse frames"
        );

        assert!(bus.owner_table().try_claim(link(PEER, 0, 0), 1));
        bus.refresh_replica_bundle(PEER);
        assert!(bus.owner_table().is_up(PEER));
        bus.send_to_replica(PEER, dummy_message().into_frozen())
            .await
            .expect("group frame forwarded");
        bus.send_to_replica(
            PEER,
            command_message(Command::SessionRetirementProgress).into_frozen(),
        )
        .await
        .expect("metadata frame forwarded");
        assert_eq!(*forwards.borrow(), [(PEER, 2), (PEER, 1)]);
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
        assert!(bus.owner_table().try_claim(link(5, 0, 0), 3));
        bus.refresh_replica_bundle(5);

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
        let cb: ConnectionLostFn = std::rc::Rc::new(move |_link: ReplicaLinkId| {
            counter_inner.set(counter_inner.get() + 1);
            // Reentrant install: replace the closure mid-invoke.
            let counter_replacement = counter_inner.clone();
            bus_for_closure.set_connection_lost_fn(std::rc::Rc::new(move |_| {
                counter_replacement.set(counter_replacement.get() + 10);
            }));
        });
        bus.set_connection_lost_fn(cb);

        bus.notify_connection_lost(link(1, 0, 0), "test"); // first closure runs (+1) and swaps
        assert_eq!(counter.get(), 1);

        bus.notify_connection_lost(link(1, 0, 0), "test"); // second closure runs (+10)
        assert_eq!(counter.get(), 11);
    }

    /// A close releases only the claim of its own epoch, so the late close
    /// of an old link never frees the slot of the link that replaced it.
    #[test]
    fn notify_connection_lost_releases_only_its_own_epoch() {
        let bus = IggyMessageBus::new(3);
        assert!(bus.mark_replica_owned(link(7, 0, 1)));

        bus.notify_connection_lost(link(7, 0, 0), "late close of an older link");
        assert_eq!(bus.owning_shard(7, 0), Some(3));

        bus.notify_connection_lost(link(7, 0, 1), "test");
        assert_eq!(bus.owning_shard(7, 0), None);
    }

    /// A current-epoch close tears the bundle down: frames stop, every other
    /// link of the bundle gets a close request at its owner, and the sweep
    /// dials the next epoch only once they are gone.
    #[test]
    fn closing_a_link_tears_the_bundle_down_and_closes_the_others() {
        const PEER: u8 = 1;
        let bus = bus_with_layout(0, 4, 2);
        let requests = capture_close_requests(&bus);
        assert!(bus.owner_table().try_claim(link(PEER, 0, 0), 1));
        assert!(bus.owner_table().try_claim(link(PEER, 1, 0), 2));
        bus.refresh_replica_bundle(PEER);
        assert!(bus.owner_table().is_up(PEER));
        assert_eq!(bus.replica_links_to_dial(PEER), Vec::<ReplicaLinkId>::new());

        assert!(bus.owner_table().release(link(PEER, 1, 0), 2));
        bus.replica_link_closed(link(PEER, 1, 0));
        assert!(!bus.owner_table().is_up(PEER));
        assert_eq!(*requests.borrow(), [(1, link(PEER, 0, 0))]);
        assert!(
            bus.replica_links_to_dial(PEER).is_empty(),
            "no dial while a link of the old epoch is open"
        );

        assert!(bus.owner_table().release(link(PEER, 0, 0), 1));
        bus.replica_link_closed(link(PEER, 0, 0));
        assert_eq!(
            bus.replica_links_to_dial(PEER),
            [link(PEER, 0, 1), link(PEER, 1, 1)]
        );
        assert_eq!(
            bus.take_replica_link_stats(),
            ReplicaLinkStats {
                bundle_teardowns: 1,
                ..ReplicaLinkStats::default()
            }
        );
    }

    /// The owner closes a link only while it holds the link in the epoch of
    /// the request, so a late request never closes a newer link.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn close_replica_link_closes_only_the_held_epoch() {
        let bus = IggyMessageBus::new(2);
        let conn_token = insert_replica_entry(&bus, 4);
        assert!(bus.mark_replica_owned(link(4, 0, 1)));

        bus.close_replica_link(link(4, 0, 0));
        assert!(
            !conn_token.is_triggered(),
            "an older epoch must not close it"
        );

        bus.close_replica_link(link(4, 0, 1));
        assert!(conn_token.is_triggered());
    }

    /// Shard 0 reads exactly the preamble and leaves the next bytes, the TLS
    /// `ClientHello` or the hello, to the owner shard.
    #[compio::test]
    #[allow(clippy::future_not_send)]
    async fn place_replica_inbound_reads_only_the_preamble() {
        const NEXT: &[u8] = b"hello";
        let bus = bus_with_layout(0, 4, 2);
        bus.set_replica_handshake_ctx(replica::handshake::ReplicaHandshakeCtx {
            binary_identity: [0; replica::auth::IDENTITY_LEN],
            cluster_id: 1,
            self_id: 2,
            replica_count: 3,
            auth: None,
            tls: None,
        });
        let listener = compio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let mut dialer = compio::net::TcpStream::connect(addr)
            .await
            .expect("connect");
        let (mut accepted, _) = listener.accept().await.expect("accept");
        let mut bytes = Preamble { dialer: 1, link: 1 }.encode().to_vec();
        bytes.extend_from_slice(NEXT);
        compio::io::AsyncWriteExt::write_all(&mut dialer, bytes)
            .await
            .0
            .expect("write");

        let deadline = Instant::now() + bus.config().handshake_grace;
        let (placed, owner) = bus
            .place_replica_inbound(&mut accepted, deadline)
            .await
            .expect("valid preamble");
        assert_eq!(placed, link(1, 1, 0));
        // Range 1 = shards {2, 3}; peer 1 sits in slot 1 of node 2.
        assert_eq!(owner, 3);
        let compio::BufResult(read, rest) =
            compio::io::AsyncReadExt::read_exact(&mut accepted, Vec::with_capacity(NEXT.len()))
                .await;
        read.expect("read");
        assert_eq!(rest, NEXT);
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
    fn try_claim_wins_a_free_slot() {
        let table = ReplicaOwnerTable::new();
        assert!(
            table.try_claim(link(42, 0, 7), 3),
            "a free slot must be won"
        );
        assert_eq!(table.claim(42, 0), Some(LinkClaim { owner: 3, epoch: 7 }));
    }

    /// A second install of a held link loses, from any shard and in any
    /// epoch, and leaves the slot untouched.
    #[test]
    fn try_claim_refuses_a_held_link() {
        let table = ReplicaOwnerTable::new();
        assert!(table.try_claim(link(42, 0, 0), 5));
        for (shard, epoch) in [(7, 0), (5, 0), (5, 1)] {
            assert!(
                !table.try_claim(link(42, 0, epoch), shard),
                "shard {shard} epoch {epoch} must lose"
            );
        }
        assert_eq!(table.claim(42, 0), Some(LinkClaim { owner: 5, epoch: 0 }));
    }

    #[test]
    fn release_frees_only_the_matching_claim() {
        let table = ReplicaOwnerTable::new();
        assert!(table.try_claim(link(42, 0, 2), 5));
        assert!(!table.release(link(42, 0, 2), 6), "another shard");
        assert!(!table.release(link(42, 0, 1), 5), "another epoch");
        assert!(table.release(link(42, 0, 2), 5));
        assert_eq!(table.claim(42, 0), None);
    }

    #[test]
    fn links_past_the_layout_have_no_slot() {
        let table =
            ReplicaOwnerTable::with_layout(LinkLayout::new(4, 2, true, 2).expect("valid layout"));
        assert!(table.try_claim(link(u8::MAX, 1, u32::MAX), 3));
        assert_eq!(
            table.claim(u8::MAX, 1),
            Some(LinkClaim {
                owner: 3,
                epoch: u32::MAX
            })
        );
        assert!(!table.try_claim(link(0, 2, 0), 3));
        assert_eq!(table.claim(0, 2), None);
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
        bus.mark_dial_pending(7, 1);
        assert!(bus.check_dial_pending(7, 1), "fresh entry must be pending");
        assert!(!bus.check_dial_pending(7, 0), "other links stay dialable");
        assert!(bus.clear_dial_pending(7, 1), "ack clears the entry");
        assert!(
            !bus.check_dial_pending(7, 1),
            "cleared entry must not block"
        );
        assert!(!bus.clear_dial_pending(7, 1), "double clear is a no-op");

        // Force-expire: rewrite the deadline into the past, mimicking a
        // lost ack after grace + margin elapsed.
        bus.mark_dial_pending(9, 0);
        let past = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("test clock past epoch");
        bus.pending_dials.borrow_mut().insert((9, 0), past);
        assert!(
            !bus.check_dial_pending(9, 0),
            "expired entry must report not-pending"
        );
        assert!(
            !bus.clear_dial_pending(9, 0),
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
