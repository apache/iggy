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

pub mod builder;
pub mod config;
pub mod coordinator;
pub mod metrics;
mod router;
pub mod shards_table;

pub use config::CoordinatorConfig;

use consensus::{
    LocalPipeline, MetadataHandle, MuxPlane, PartitionsHandle, Pipeline, Plane, PlaneKind,
    Sequencer, VsrAction, VsrConsensus,
};
use iggy_binary_protocol::{
    Command2, CommitHeader, DoViewChangeHeader, GenericHeader, PrepareHeader, PrepareOkHeader,
    RequestHeader, StartViewChangeHeader, StartViewHeader,
};
use iggy_common::PartitionStats;
use iggy_common::variadic;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::conn_info::ClientConnMeta;
use message_bus::replica::listener::MessageHandler;
use metadata::IggyMetadata;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;
use partitions::{IggyPartition, IggyPartitions};
use server_common::sharding::IggyNamespace;
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use shards_table::ShardsTable;
use std::rc::Rc;
use std::sync::Arc;

pub type ShardPlane<B, J, S, M> =
    MuxPlane<variadic!(IggyMetadata<VsrConsensus<B>, J, S, M>, IggyPartitions<B>)>;

pub struct ShardIdentity {
    pub id: u16,
    pub name: String,
}

impl ShardIdentity {
    #[must_use]
    pub const fn new(id: u16, name: String) -> Self {
        Self { id, name }
    }
}

pub struct PartitionConsensusConfig<B>
where
    B: MessageBus,
{
    pub cluster_id: u128,
    pub replica_count: u8,
    pub bus: B,
}

impl<B> PartitionConsensusConfig<B>
where
    B: MessageBus,
{
    #[must_use]
    pub const fn new(cluster_id: u128, replica_count: u8, bus: B) -> Self {
        Self {
            cluster_id,
            replica_count,
            bus,
        }
    }
}

/// Bounded mpsc channel sender (blocking send).
pub type Sender<T> = crossfire::MTx<crossfire::mpsc::Array<T>>;

/// Bounded mpsc channel receiver (async recv).
pub type Receiver<T> = crossfire::AsyncRx<crossfire::mpsc::Array<T>>;

/// Create a bounded mpsc channel with a blocking sender and async receiver.
#[must_use]
pub fn channel<T: Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    crossfire::mpsc::bounded_blocking_async(capacity)
}

/// Cross-shard metadata consensus submit.
///
/// The metadata consensus group lives only on shard 0. When a client
/// connection homes on a peer shard, that shard verifies credentials and
/// owns the session locally, but the consensus proposal (`Register` /
/// `Logout`) must execute on shard 0. The peer hands just that step here
/// and awaits the committed op number over `reply` (`None` = transient
/// submit failure; all `RegisterSubmitError` variants are transient by
/// contract, so the caller retries rather than distinguishing them).
pub enum MetadataSubmit {
    Register {
        vsr_client_id: u128,
        reply: Sender<Option<u64>>,
    },
    Logout {
        vsr_client_id: u128,
        session: u64,
        request: u64,
        reply: Sender<Option<u64>>,
    },
}

/// Handler shard 0 runs for an inbound [`MetadataSubmit`]. server-ng wires
/// it to `submit_register_in_process` / `submit_logout_in_process` and
/// sends the result back over the frame's `reply` sender. `None` on a peer
/// shard (no consensus): a peer must never receive this frame.
pub type MetadataSubmitHandler = Rc<dyn Fn(MetadataSubmit)>;

/// Create a bounded inter-shard channel whose sender is tagged with the
/// owning shard.
///
/// Bootstrap uses this to build the per-shard sender `Vec` such that
/// `vec[i]` necessarily reaches shard `i`.
#[must_use]
pub fn shard_channel(owner_shard: u16, capacity: usize) -> (TaggedSender, Receiver<ShardFrame>) {
    let (tx, rx) = channel::<ShardFrame>(capacity);
    (TaggedSender::new(owner_shard, tx), rx)
}

/// Build canonical-ordered `(senders, inboxes)` pair for an N-shard mesh.
///
/// Each `inboxes[i]` drains exclusively on the runtime owning shard `i`. The
/// returned `senders` Vec satisfies `senders[i].shard_id() == i` by
/// construction; clone it into every shard before spawning so all shards
/// share the same mesh.
///
/// Receivers are wrapped in `Option` because [`Receiver`] (crossfire
/// `AsyncRx`) is non-cloneable on purpose; bootstrap takes the slot for
/// shard `i` exactly once when spawning the owning thread.
#[must_use]
pub fn shard_mesh_channels(
    total_shards: u16,
    capacity: usize,
) -> (Vec<TaggedSender>, Vec<Option<Receiver<ShardFrame>>>) {
    let mut senders = Vec::with_capacity(total_shards as usize);
    let mut inboxes = Vec::with_capacity(total_shards as usize);
    for shard_id in 0..total_shards {
        let (tx, rx) = shard_channel(shard_id, capacity);
        senders.push(tx);
        inboxes.push(Some(rx));
    }
    (senders, inboxes)
}

/// A [`Sender`] annotated with the id of the shard whose paired receiver it
/// feeds.
///
/// Inter-shard routing indexes `senders[i]` with `i == target_shard`. The
/// plain `Sender` form has no way to verify that invariant at runtime, so a
/// permuted `Vec<Sender<_>>` would silently misroute every setup, mapping,
/// and forward frame. Construct senders through [`shard_channel`] (or
/// [`TaggedSender::new`]) at the channel-creation site; the coordinator and
/// [`IggyShard`] ctors then validate `senders[i].shard_id() == i`,
/// returning [`ShardCtorError`] if violated.
pub struct TaggedSender {
    shard_id: u16,
    inner: Sender<ShardFrame>,
}

impl TaggedSender {
    /// Wrap an already-constructed sender with the id of the shard whose
    /// paired receiver drains it. Prefer [`shard_channel`] unless an
    /// existing sender is being re-tagged (e.g., tests that build senders
    /// manually and know the ordering is correct).
    #[must_use]
    pub const fn new(shard_id: u16, inner: Sender<ShardFrame>) -> Self {
        Self { shard_id, inner }
    }

    #[must_use]
    pub const fn shard_id(&self) -> u16 {
        self.shard_id
    }
}

impl Clone for TaggedSender {
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id,
            inner: self.inner.clone(),
        }
    }
}

impl std::ops::Deref for TaggedSender {
    type Target = Sender<ShardFrame>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Error returned by [`IggyShard::new`] and the shard builder when ctor
/// preconditions are violated.
///
/// Both are bootstrap programming errors: the surrounding crate either
/// built the `senders` vec out of canonical order, or produced more
/// shards than the inter-shard addressing scheme supports. Surfaced as
/// `Err` instead of panicking so the host process can log and abort with
/// a typed error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShardCtorError {
    #[error(
        "senders[{index}] carries shard_id {actual}; inter-shard vec must be in canonical \
         order (senders[i].shard_id() == i)"
    )]
    SenderOrderingInvalid {
        index: usize,
        expected: u16,
        actual: u16,
    },
    #[error("shard count {count} does not fit in u16; inter-shard frame addressing is u16-indexed")]
    ShardCountOverflow { count: usize },
    #[error(
        "shard-0 coordinator senders length {senders} does not match total_shards {total_shards} \
         (total_shards must be >= 1 and equal senders.len())"
    )]
    CoordinatorSendersMismatch { senders: usize, total_shards: u16 },
}

/// Validate the canonical ordering `senders[i].shard_id() == i`.
/// Returns `Err` for the first index that violates the invariant.
pub(crate) fn validate_sender_ordering(senders: &[TaggedSender]) -> Result<(), ShardCtorError> {
    for (idx, sender) in senders.iter().enumerate() {
        let expected = u16::try_from(idx).map_err(|_| ShardCtorError::ShardCountOverflow {
            count: senders.len(),
        })?;
        let actual = sender.shard_id();
        if actual != expected {
            return Err(ShardCtorError::SenderOrderingInvalid {
                index: idx,
                expected,
                actual,
            });
        }
    }
    Ok(())
}

/// Lifecycle frame variants.
///
/// Connection setup and cross-shard forwards: every frame the inter-shard
/// channel carries that is NOT a consensus protocol message lives here.
/// Splitting these out from [`ShardFrame::Consensus`] keeps the consensus
/// dispatch path hot and cache-tight while leaving lifecycle traffic on
/// the same single channel (preserving relative ordering between consensus
/// and lifecycle frames at near-zero cost).
///
/// Trade-off: consensus and lifecycle traffic compete for one bounded
/// inbox. A consensus burst or retransmit storm can fill it exactly when
/// a terminal-drop [`LifecycleFrame::ForwardClientSend`] needs the space;
/// `inbox_capacity` is a single knob and cannot isolate the two frame
/// classes.
#[non_exhaustive]
pub enum LifecycleFrame {
    /// Shard 0 distributes an inbound replica TCP connection fd to the owning
    /// shard. The receiving shard wraps the fd in a `TcpStream` and spawns
    /// writer + reader tasks on its own compio runtime. The `fd` is an
    /// owning [`DupedFd`] so that a frame dropped unprocessed (shutdown,
    /// pump drain abort, router panic before `install_*_fd`) closes the
    /// dup instead of leaking it.
    ReplicaConnectionSetup { fd: DupedFd, replica_id: u8 },
    /// Shard 0 distributes an inbound SDK client TCP connection fd to the
    /// owning shard. The receiving shard wraps the fd and installs client
    /// reader / writer tasks locally. The owning shard is encoded in the top
    /// 16 bits of `meta.client_id`.
    ClientConnectionSetup { fd: DupedFd, meta: ClientConnMeta },
    /// Shard 0 distributes an inbound SDK WebSocket client's pre-upgrade
    /// TCP connection fd to the owning shard. The HTTP-Upgrade handshake
    /// has NOT run yet at this point: the fd is plain TCP, the dup is
    /// safe (cross-shard fd-delegation only happens for plain TCP), and
    /// `compio_ws::WebSocketStream<TcpStream>`'s `!Send` constraint
    /// (compio `Rc<...>` driver state, post-upgrade) does not apply.
    /// The receiving shard wraps the fd, runs `compio_ws::accept_async`,
    /// then installs client reader / writer tasks locally via
    /// `message_bus::installer::install_client_ws_fd`. Owning shard is
    /// encoded in the top 16 bits of `meta.client_id`.
    ///
    /// QUIC clients deliberately do NOT get an analog variant: a
    /// `compio_quic::Endpoint` binds one UDP socket and demuxes incoming
    /// packets to per-connection `quinn-proto::Connection` objects by
    /// Connection ID. Per-connection TLS / packet-number / congestion
    /// state is non-serialisable and tied to the endpoint's reactor.
    /// Shard 0 therefore terminates QUIC locally and uses the existing
    /// `ForwardClientSend` variant for outbound traffic.
    ClientWsConnectionSetup { fd: DupedFd, meta: ClientConnMeta },
    /// A non-owning shard forwards a replica send to the owning shard's
    /// local bus; the owning shard then takes the fast path.
    ForwardReplicaSend {
        replica_id: u8,
        msg: Frozen<MESSAGE_ALIGN>,
    },
    /// A shard that doesn't hold the client's TCP connection forwards a
    /// client send to the owning shard (top 16 bits of `client_id`).
    ForwardClientSend {
        client_id: u128,
        msg: Frozen<MESSAGE_ALIGN>,
    },
    /// A peer shard hands a metadata consensus submit (login/logout) to
    /// shard 0, the metadata consensus owner. The committed op returns over
    /// the `reply` sender carried in [`MetadataSubmit`]. Always addressed to
    /// shard 0; processing it on a peer is a routing bug.
    MetadataSubmit(MetadataSubmit),
}

/// Inter-shard channel envelope.
///
/// Concrete enum; no generic. Consensus dispatches are fire-and-forget by
/// VSR design (replies travel as their own wire-level messages: `Reply`
/// to clients, `PrepareOk` to the primary), so no response channel rides
/// in the frame.
#[non_exhaustive]
pub enum ShardFrame {
    /// A consensus protocol message (Request / Prepare / `PrepareOk` /
    /// view-change family / Commit). Fire-and-forget. Drops on full inbox
    /// are recovered by VSR retransmit timers.
    ///
    /// `target_shard` is stamped by the sender at enqueue time, so the
    /// receiving pump never re-derives routing in release builds. The
    /// receiver still validates `target_shard == self.id` and drops
    /// frames stamped for the wrong shard (`MISROUTED`) to preserve the
    /// single-pump invariant under any caller bug.
    Consensus {
        target_shard: u16,
        message: Message<GenericHeader>,
    },
    /// A connection setup or cross-shard forward frame. Drop recovery
    /// depends on the frame class: [`LifecycleFrame::ForwardReplicaSend`]
    /// is VSR-covered, connection-setup frames are recovered by the
    /// connector's periodic reconnect sweep, but
    /// [`LifecycleFrame::ForwardClientSend`] is terminal - no retransmit,
    /// the client never receives the reply.
    Lifecycle(LifecycleFrame),
}

impl ShardFrame {
    /// Create a consensus frame addressed to `target_shard`. The sender
    /// is the routing authority; `accept_frame_for_self` compares this
    /// stamp against the receiving shard id in O(1).
    #[must_use]
    pub const fn consensus(target_shard: u16, message: Message<GenericHeader>) -> Self {
        Self::Consensus {
            target_shard,
            message,
        }
    }

    /// Create a lifecycle frame.
    #[must_use]
    pub const fn lifecycle(payload: LifecycleFrame) -> Self {
        Self::Lifecycle(payload)
    }
}

pub struct IggyShard<B, MJ, S, M, T = ()>
where
    B: MessageBus,
{
    pub id: u16,
    pub name: String,
    pub plane: ShardPlane<B, MJ, S, M>,

    /// Handle to the local bus. Retained alongside the bus owned by every
    /// consensus plane so the router can reach the `ConnectionInstaller`
    /// surface without going through consensus.
    pub bus: B,

    /// Callback attached to every delegated replica connection installed
    /// on this shard. The bus' reader task invokes this for each inbound
    /// consensus message; the callback is typically `|_, msg| shard.dispatch(msg)`.
    on_replica_message: MessageHandler,

    /// Callback attached to every delegated client connection installed on
    /// this shard. Invoked for each inbound `Request` frame.
    on_client_request: RequestHandler,

    /// Handler for inbound [`MetadataSubmit`] frames. Only shard 0 receives
    /// these (it owns the metadata consensus group); peers send them here
    /// via [`Self::forward_metadata_submit`]. Defaults to a no-op for the
    /// simulator stub ctor.
    on_metadata_submit: MetadataSubmitHandler,

    /// Channel senders to every shard, indexed by shard id.
    /// Includes a sender to self so that local routing goes through the
    /// same channel path as remote routing.
    ///
    /// [`assert_sender_ordering`] is invoked in the ctor so `senders[i]`
    /// is guaranteed to feed the shard whose `id == i`. Call sites can
    /// therefore index by `target_shard` without re-checking.
    senders: Vec<TaggedSender>,

    /// Total shard count, cached from `senders.len()` at construction.
    /// `senders` is immutable post-ctor, so consensus routing reads this
    /// rather than recomputing the `usize -> u32` conversion per frame.
    shard_count: u32,

    /// Receiver end of this shard's inbox.  Peer shards (and self) send
    /// messages here via the corresponding sender.
    inbox: Receiver<ShardFrame>,

    /// Partition namespace -> owning shard lookup.
    shards_table: T,

    partition_consensus: PartitionConsensusConfig<B>,

    /// Shard 0 coordinator, supplied at construction. Holds round-robin
    /// state for replica and client delegation. `None` on non-zero shards
    /// and in single-shard tests that bypass the coordinator.
    coordinator: Option<Rc<crate::coordinator::ShardZeroCoordinator>>,

    /// Per-shard observability counters. Cloned at metric increment sites,
    /// so cheap (`Arc` clone) regardless of label cardinality.
    metrics: crate::metrics::ShardMetrics,
}

impl<B, MJ, S, M, T> IggyShard<B, MJ, S, M, T>
where
    B: MessageBus,
    T: ShardsTable,
{
    /// Create a new shard with channel links and a shards table.
    ///
    /// * `bus` - shard-local bus handle (kept alongside the buses owned
    ///   by the consensus planes so the router can reach the
    ///   `ConnectionInstaller` surface directly).
    /// * `senders` - one [`TaggedSender`] per shard. The ctor asserts
    ///   `senders[i].shard_id() == i`; use [`shard_channel`] at
    ///   construction time so every sender carries the id of the shard
    ///   whose receiver drains it.
    /// * `inbox` - the receiver that this shard drains in its message pump.
    /// * `shards_table` - namespace -> shard routing table.
    /// * `coordinator` - `Some` on shard 0 (supplied by the builder when
    ///   `is_shard_zero`), `None` everywhere else. Immutable post-ctor:
    ///   the coordinator is injected at construction time so an
    ///   `IggyShard` cannot appear half-wired to a reader.
    /// * `metrics` - per-shard observability handle; currently the
    ///   `frame_drops_total` counter.
    ///
    /// # Errors
    ///
    /// Returns [`ShardCtorError::SenderOrderingInvalid`] if `senders` is
    /// not in canonical order (any `senders[i].shard_id() != i`) and
    /// [`ShardCtorError::ShardCountOverflow`] if `senders.len()` does not
    /// fit in `u16`. Both are bootstrap programming errors: the
    /// permutation would silently misroute every inter-shard frame, or
    /// addressing space (u16) would wrap.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ShardIdentity,
        bus: B,
        on_replica_message: MessageHandler,
        on_client_request: RequestHandler,
        on_metadata_submit: MetadataSubmitHandler,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        senders: Vec<TaggedSender>,
        inbox: Receiver<ShardFrame>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
        coordinator: Option<Rc<crate::coordinator::ShardZeroCoordinator>>,
        metrics: crate::metrics::ShardMetrics,
    ) -> Result<Self, ShardCtorError> {
        validate_sender_ordering(&senders)?;
        let shard_count =
            u32::try_from(senders.len()).map_err(|_| ShardCtorError::ShardCountOverflow {
                count: senders.len(),
            })?;
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        Ok(Self {
            id,
            name,
            plane,
            bus,
            on_replica_message,
            on_client_request,
            on_metadata_submit,
            senders,
            shard_count,
            inbox,
            shards_table,
            partition_consensus,
            coordinator,
            metrics,
        })
    }

    /// Hand a metadata consensus submit (login/logout) to shard 0.
    ///
    /// Sends a [`LifecycleFrame::MetadataSubmit`] into shard 0's inbox. The
    /// caller owns the matching [`Receiver`] (paired with the `reply` sender
    /// inside `submit`) and awaits the committed op there. On a full /
    /// disconnected shard-0 inbox the frame is dropped; the dropped `reply`
    /// sender then surfaces as a recv error the caller maps to a transient
    /// failure.
    pub fn forward_metadata_submit(&self, submit: MetadataSubmit) {
        let frame = ShardFrame::lifecycle(LifecycleFrame::MetadataSubmit(submit));
        if let Err(error) = self.senders[0].try_send(frame) {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::CONSENSUS,
                crate::coordinator::classify_try_send_err(&error),
            );
            tracing::warn!(
                shard = self.id,
                "forward_metadata_submit: shard-0 inbox rejected frame: {error:?}"
            );
        }
    }

    /// Return a clone of the shard-0 coordinator handle, if attached.
    /// Bootstrap uses this to wire the listener accept callbacks
    /// (replica + client) to coordinator-driven fd-delegation instead
    /// of installing connections locally on shard 0.
    #[must_use]
    pub fn coordinator(&self) -> Option<Rc<crate::coordinator::ShardZeroCoordinator>> {
        self.coordinator.clone()
    }

    /// Create a shard without inter-shard channels or delegated connections.
    ///
    /// Useful for the simulator where inbound messages are delivered
    /// directly via [`on_message`](Self::on_message) instead of the TCP /
    /// fd-transfer path. Installs no-op connection handlers because the
    /// simulator never receives a `ReplicaConnectionSetup` frame.
    #[must_use]
    pub fn without_inbox(
        identity: ShardIdentity,
        bus: B,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M>,
        partitions: IggyPartitions<B>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
    ) -> Self {
        // TODO(hubcio): crossfire's Flavor trait blocks unbounded channels
        // with the current type setup; revisit when crossfire grows an
        // unbounded variant or we replace it.
        let (_tx, inbox) = channel(1);
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        Self {
            id,
            name,
            bus,
            on_replica_message: std::rc::Rc::new(|_, _| {}),
            on_client_request: std::rc::Rc::new(|_, _| {}),
            on_metadata_submit: std::rc::Rc::new(|_| {}),
            plane,
            coordinator: None,
            senders: Vec::new(),
            // The simulator delivers inbound messages straight to
            // `on_message`, bypassing the inter-shard router. The router's
            // `shard_count` should therefore never be read on this path,
            // but `pub fn dispatch` is still reachable; pinning to 1 keeps
            // `% shard_count` from panicking if a future caller slips
            // through, while preserving single-shard routing semantics.
            shard_count: 1,
            inbox,
            shards_table,
            partition_consensus,
            metrics: crate::metrics::ShardMetrics::for_shard(),
        }
    }

    #[must_use]
    pub const fn shards_table(&self) -> &T {
        &self.shards_table
    }
}

/// Local message processing — these methods handle messages that have been
/// routed to this shard via the message pump.
impl<B, MJ, S, M, T> IggyShard<B, MJ, S, M, T>
where
    B: MessageBus,
{
    /// Dispatch an incoming network message to the appropriate consensus plane.
    ///
    /// Routes requests, replication messages, and acks to either the metadata
    /// plane or the partitions plane based on `PlaneIdentity::is_applicable`.
    //
    // TODO(hubcio): perf - this `MessageBag::try_from` is the second parse of
    // the same frame; the first ran in `IggyShard::dispatch` (router.rs ~85)
    // to extract (operation, namespace) for routing. The work here re-runs
    // `bytemuck::checked::try_from_bytes` + per-header `validate()` on bytes
    // already validated upstream. See the matching TODO in router.rs for the
    // fix: thread the classified `MessageBag` through `ShardFrame::Consensus`
    // so this function takes the bag directly and the match below dispatches
    // without re-parsing.
    #[allow(clippy::future_not_send)]
    pub async fn on_message(&self, message: Message<GenericHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        match MessageBag::try_from(message) {
            Ok(MessageBag::Request(request)) => self.on_request(request).await,
            Ok(MessageBag::Prepare(prepare)) => self.on_replicate(prepare).await,
            Ok(MessageBag::PrepareOk(prepare_ok)) => self.on_ack(prepare_ok).await,
            Ok(MessageBag::StartViewChange(msg)) => self.on_start_view_change(msg).await,
            Ok(MessageBag::DoViewChange(msg)) => self.on_do_view_change(msg).await,
            Ok(MessageBag::StartView(msg)) => self.on_start_view(msg).await,
            Ok(MessageBag::Commit(ref msg)) => self.on_commit(msg).await,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
            }
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_request(&self, request: Message<RequestHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_request(request).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_replicate(&self, prepare: Message<PrepareHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_replicate(prepare).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&self, prepare_ok: Message<PrepareOkHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        self.plane.on_ack(prepare_ok).await;
    }

    /// Drain and dispatch loopback messages for each consensus plane.
    ///
    /// Each plane's loopback is dispatched directly to that plane's `on_ack`,
    /// avoiding a flat merge that would require re-routing through `on_message`.
    ///
    /// Invariant: planes do not produce loopback messages for each other.
    /// `on_ack` commits and applies but never calls `push_loopback`, so
    /// draining metadata before partitions is order-independent.
    ///
    /// # Panics
    /// Panics if a loopback message is not a valid `PrepareOk` message.
    #[allow(clippy::future_not_send)]
    pub async fn process_loopback(
        &self,
        buf: &mut Vec<Message<GenericHeader>>,
        namespace_scratch: &mut Vec<IggyNamespace>,
    ) -> usize
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        debug_assert!(buf.is_empty(), "buf must be empty on entry");
        debug_assert!(
            namespace_scratch.is_empty(),
            "namespace_scratch must be empty on entry",
        );

        let mut total = 0;
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus {
            consensus.drain_loopback_into(buf);
            let count = buf.len();
            total += count;
            for msg in buf.drain(..) {
                let typed: Message<PrepareOkHeader> = msg
                    .try_into_typed()
                    .expect("loopback queue must only contain PrepareOk messages");
                planes.0.on_ack(typed).await;
            }
        }

        namespace_scratch.extend(planes.1.0.namespaces().copied());
        for namespace in namespace_scratch.drain(..) {
            let partition = planes
                .1
                .0
                .get_by_ns(&namespace)
                .expect("partition namespace must resolve during loopback drain");
            partition.consensus().drain_loopback_into(buf);
        }
        let count = buf.len();
        total += count;
        for msg in buf.drain(..) {
            let typed: Message<PrepareOkHeader> = msg
                .try_into_typed()
                .expect("loopback queue must only contain PrepareOk messages");
            planes.1.0.on_ack(typed).await;
        }

        total
    }

    /// Initializes a partition and its dedicated consensus instance on this shard.
    ///
    /// Used only by the `simulator` crate to seed partitions on the
    /// in-process replica array. Production boots via the
    /// `load_partition` helper in `server-ng` bootstrap, which takes the
    /// cluster `self_replica_id` explicitly from `topology` and never
    /// folds the local shard id into the consensus replica space.
    ///
    /// # Panics
    /// Panics if `self.id > u8::MAX` (256+). The simulator currently
    /// caps replica count at `MAX_REPLICAS = 256`, so the cast always
    /// succeeds in the only caller; the constraint is recorded here so
    /// any future caller in production code reads it before the cast.
    pub fn init_partition(&mut self, namespace: IggyNamespace)
    where
        B: MessageBus + Clone,
    {
        let partitions = self.plane.partitions_mut();
        if partitions.contains(&namespace) {
            return;
        }

        let replica_id =
            u8::try_from(self.id).expect("shard id must fit in u8 for partition consensus");
        let consensus = VsrConsensus::new(
            self.partition_consensus.cluster_id,
            replica_id,
            self.partition_consensus.replica_count,
            namespace.inner(),
            self.partition_consensus.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();

        let stats = Arc::new(PartitionStats::default());
        let partition = IggyPartition::with_in_memory_storage(
            stats,
            consensus,
            partitions.config().segment_size,
            partitions.config().enforce_fsync,
        );
        partitions.insert(namespace, partition);
    }

    /// Handle incoming view-change/control message. Metadata use metadata
    /// consensus. Partitions loop all partitions, use partition consensus.
    ///
    // TODO(hubcio): every VSR callback below
    // (`on_start_view_change`, `on_do_view_change`, `on_start_view`,
    // `on_commit`, `tick_partitions`) materialises
    // `planes.1.0.namespaces().copied().collect::<Vec<_>>()` per call to
    // avoid borrowing the partitions plane across the partition-consensus
    // `.await` inside the loop. Allocs scale with VSR traffic, not with
    // useful work: a quiet cluster still pays one Vec per heartbeat tick.
    // Convert to the `namespace_scratch: RefCell<Vec<IggyNamespace>>`
    // pattern already used by `process_loopback` (see :646-684) so the
    // scratch is reused across calls. Asserts on entry/exit keep the
    // "empty on entry, drained on exit" invariant explicit.
    //
    // Reproducible in `core/simulator`: sim drives these callbacks via
    // `tick()` against `IggyShard`, so an allocation-counting variant of
    // `MemStorage`/test harness can pin the per-VSR-cb alloc count and
    // fail on regression after the scratch refactor lands.
    #[allow(clippy::future_not_send)]
    async fn on_start_view_change(&self, msg: Message<StartViewChangeHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_start_view_change(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            return;
        }

        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_start_view_change(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping StartViewChange: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_do_view_change(&self, msg: Message<DoViewChangeHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StreamsFrontend
            + StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
            {
                planes.0.commit_journal().await;
            }
            return;
        }

        let config = planes.1.0.config().clone();
        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_do_view_change(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
            {
                partition.commit_journal(&config).await;
            }
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping DoViewChange: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_start_view(&self, msg: Message<StartViewHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_start_view(PlaneKind::Metadata, &header);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            return;
        }

        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            let actions = consensus.handle_start_view(PlaneKind::Partitions, &header);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping StartView: namespace matches neither metadata nor partition consensus"
        );
    }

    #[allow(clippy::future_not_send)]
    async fn on_commit(&self, msg: &Message<CommitHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StreamsFrontend
            + StateMachine<
                Input = Message<PrepareHeader>,
                Output = bytes::Bytes,
                Error = iggy_common::IggyError,
            >,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            if consensus.handle_commit(&header) {
                planes.0.commit_journal().await;
            }
            return;
        }

        let config = planes.1.0.config().clone();
        let namespaces: Vec<_> = planes.1.0.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            if consensus.namespace() != header.namespace {
                continue;
            }

            if consensus.handle_commit(&header) {
                partition.commit_journal(&config).await;
            }
            return;
        }

        tracing::warn!(
            shard = self.id,
            namespace = header.namespace,
            view = header.view,
            replica = header.replica,
            "dropping Commit: namespace matches neither metadata nor partition consensus"
        );
    }

    /// Tick partition consensuses. Loop partitions. No partitions-plane journal.
    #[allow(clippy::future_not_send)]
    pub async fn tick_partitions(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();

        for namespace in namespaces {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                continue;
            };

            let consensus = partition.consensus();
            let current_op = consensus.sequencer().current_sequence();
            let current_commit = consensus.commit_min();
            let actions = consensus.tick(PlaneKind::Partitions, current_op, current_commit);
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn tick_metadata(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        let metadata = self.plane.metadata();
        let Some(ref consensus) = metadata.consensus else {
            return;
        };

        let current_op = consensus.sequencer().current_sequence();
        let current_commit = consensus.commit_min();
        let actions = consensus.tick(PlaneKind::Metadata, current_op, current_commit);

        dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &actions).await;
    }
}

/// Dispatch a list of `VsrAction`s by constructing the appropriate
/// protocol messages and sending them via the consensus message bus.
#[allow(
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn dispatch_vsr_actions<B, P, J>(
    consensus: &VsrConsensus<B, P>,
    journal: Option<&J>,
    actions: &[VsrAction],
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
    J: JournalHandle,
    <J as JournalHandle>::Target: Journal<
            <J as JournalHandle>::Storage,
            Entry = Message<PrepareHeader>,
            Header = PrepareHeader,
        >,
{
    use std::mem::size_of;

    let bus = consensus.message_bus();
    let self_id = consensus.replica();
    let cluster = consensus.cluster();
    let replica_count = consensus.replica_count();

    let send = |target: u8, msg: Frozen<MESSAGE_ALIGN>| async move {
        if let Err(e) = bus.send_to_replica(target, msg).await {
            tracing::debug!(replica = self_id, target, "bus send failed: {e}");
        }
    };

    let broadcast = async |frozen: Frozen<MESSAGE_ALIGN>| {
        // Freeze once at the primary; each target just bumps the atomic
        // refcount on the underlying ControlBlock.
        for target in 0..replica_count {
            if target != self_id {
                send(target, frozen.clone()).await;
            }
        }
    };

    for action in actions {
        match action {
            VsrAction::SendStartViewChange { view, namespace } => {
                let msg = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                    .transmute_header(|_, h: &mut StartViewChangeHeader| {
                        h.command = Command2::StartViewChange;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.namespace = *namespace;
                        h.size = size_of::<StartViewChangeHeader>() as u32;
                    });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendDoViewChange {
                view,
                target,
                log_view,
                op,
                commit,
                namespace,
            } => {
                let msg = Message::<DoViewChangeHeader>::new(size_of::<DoViewChangeHeader>())
                    .transmute_header(|_, h: &mut DoViewChangeHeader| {
                        h.command = Command2::DoViewChange;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.log_view = *log_view;
                        h.op = *op;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.size = size_of::<DoViewChangeHeader>() as u32;
                    });
                send(*target, msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendStartView {
                view,
                op,
                commit,
                namespace,
            } => {
                let msg = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
                    .transmute_header(|_, h: &mut StartViewHeader| {
                        h.command = Command2::StartView;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.op = *op;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.size = size_of::<StartViewHeader>() as u32;
                    });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendPrepareOk {
                view,
                from_op,
                to_op,
                target,
                namespace,
            } => {
                let Some(journal) = journal else {
                    continue;
                };
                for op in *from_op..=*to_op {
                    let Some(prepare_header) = journal.handle().header(op as usize) else {
                        continue;
                    };
                    let prepare_header = *prepare_header;
                    let msg = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                        .transmute_header(|_, h: &mut PrepareOkHeader| {
                            h.command = Command2::PrepareOk;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.op = op;
                            h.commit = consensus.commit_max();
                            h.timestamp = prepare_header.timestamp;
                            h.parent = prepare_header.parent;
                            h.prepare_checksum = prepare_header.checksum;
                            h.request = prepare_header.request;
                            h.operation = prepare_header.operation;
                            h.namespace = *namespace;
                            h.size = size_of::<PrepareOkHeader>() as u32;
                        });
                    send(*target, msg.into_generic().into_frozen()).await;
                }
            }
            VsrAction::RetransmitPrepares { targets } => {
                let Some(journal) = journal else {
                    continue;
                };
                for (header, replicas) in targets {
                    let Some(prepare) = journal.handle().entry(header).await else {
                        continue;
                    };
                    // Freeze the retransmit payload once; clone per target.
                    let frozen = prepare.into_generic().into_frozen();
                    for replica in replicas {
                        send(*replica, frozen.clone()).await;
                    }
                }
            }
            VsrAction::RebuildPipeline { from_op, to_op } => {
                let Some(journal) = journal else {
                    continue;
                };
                // Collect headers before borrowing the pipeline to avoid
                // holding borrow_mut() across journal reads.
                let mut gap_at = None;
                let entries: Vec<_> = (*from_op..=*to_op)
                    .map_while(|op| {
                        let Some(header) = journal.handle().header(op as usize) else {
                            gap_at = Some(op);
                            return None;
                        };
                        let mut entry = consensus::PipelineEntry::new(*header);
                        entry.add_ack(self_id);
                        Some(entry)
                    })
                    .collect();
                if let Some(missing_op) = gap_at {
                    // Journal repair is not yet implemented.Truncate the sequencer
                    // to the last op we could rebuild so the next client
                    // prepare chains correctly. Ops above the
                    // gap are lost until journal repair is added.
                    let rebuilt_up_to = missing_op.saturating_sub(1);
                    tracing::warn!(
                        replica = self_id,
                        missing_op,
                        range_start = from_op,
                        range_end = to_op,
                        rebuilt = entries.len(),
                        "RebuildPipeline: journal gap at op {missing_op}, \
                         truncating sequencer from {to_op} to {rebuilt_up_to} \
                         ({}/{} ops rebuilt)",
                        entries.len(),
                        to_op - from_op + 1,
                    );
                    consensus.sequencer().set_sequence(rebuilt_up_to);
                }
                let mut pipeline = consensus.pipeline().borrow_mut();
                for entry in entries {
                    pipeline.push(entry);
                }
            }
            // Handled by the caller (shard view change handlers) since it
            // requires access to the plane's commit_journal method.
            VsrAction::CommitJournal => {}
            VsrAction::SendCommit {
                view,
                commit,
                namespace,
                timestamp_monotonic,
            } => {
                let msg = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
                    |_, h: &mut CommitHeader| {
                        h.command = Command2::Commit;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.commit = *commit;
                        h.namespace = *namespace;
                        h.timestamp_monotonic = *timestamp_monotonic;
                        h.size = size_of::<CommitHeader>() as u32;
                    },
                );
                broadcast(msg.into_generic().into_frozen()).await;
            }
        }
    }
}

#[allow(
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]
async fn dispatch_partition_journal_actions<B, P>(
    consensus: &VsrConsensus<B, P>,
    partition: &IggyPartition<B>,
    actions: &[VsrAction],
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    use std::mem::size_of;

    let bus = consensus.message_bus();
    let self_id = consensus.replica();
    let cluster = consensus.cluster();
    let journal = &partition.log.journal().inner;

    let send = |target: u8, msg: Frozen<MESSAGE_ALIGN>| async move {
        if let Err(e) = bus.send_to_replica(target, msg).await {
            tracing::debug!(replica = self_id, target, "bus send failed: {e}");
        }
    };

    for action in actions {
        match action {
            VsrAction::SendPrepareOk {
                view,
                from_op,
                to_op,
                target,
                namespace,
            } => {
                for op in *from_op..=*to_op {
                    let Some(prepare_header) = journal.header_by_op(op) else {
                        continue;
                    };
                    let msg = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                        .transmute_header(|_, h: &mut PrepareOkHeader| {
                            h.command = Command2::PrepareOk;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.op = op;
                            h.commit = consensus.commit_max();
                            h.timestamp = prepare_header.timestamp;
                            h.parent = prepare_header.parent;
                            h.prepare_checksum = prepare_header.checksum;
                            h.request = prepare_header.request;
                            h.operation = prepare_header.operation;
                            h.namespace = *namespace;
                            h.size = size_of::<PrepareOkHeader>() as u32;
                        });
                    send(*target, msg.into_generic().into_frozen()).await;
                }
            }
            VsrAction::RetransmitPrepares { targets } => {
                // DURABILITY CAVEAT: the only `Storage` impl on
                // `PartitionJournal` right now is the in-memory
                // `PartitionJournalMemStorage`. After a process restart
                // the journal is empty and every `journal.entry` below
                // returns `None`, so retransmit silently drops the
                // request and peers stall until a view change. The bus
                // and consensus plumbing is correct; only the storage
                // needs to become durable before cluster workloads go to
                // production. Server boot emits a loud warning to the
                // operator (see `main.rs`).
                for (header, replicas) in targets {
                    let Some(prepare) = journal.entry(header).await else {
                        continue;
                    };
                    // The partition journal already stores the wire-format
                    // `Frozen<4096>` (PrepareHeader followed by payload),
                    // so `send_to_replica` can take it directly and `clone`
                    // is a refcount bump. Matches the metadata-plane path
                    // above and avoids both the per-target 4 KiB memcpy
                    // and the prior `.expect` that would panic the shard
                    // on a corrupted journal entry.
                    for replica in replicas {
                        send(*replica, prepare.clone()).await;
                    }
                }
            }
            VsrAction::RebuildPipeline { from_op, to_op } => {
                let mut gap_at = None;
                let entries: Vec<_> = (*from_op..=*to_op)
                    .map_while(|op| {
                        let Some(header) = journal.header_by_op(op) else {
                            gap_at = Some(op);
                            return None;
                        };
                        let mut entry = consensus::PipelineEntry::new(header);
                        entry.add_ack(self_id);
                        Some(entry)
                    })
                    .collect();
                if let Some(missing_op) = gap_at {
                    let rebuilt_up_to = missing_op.saturating_sub(1);
                    tracing::warn!(
                        replica = self_id,
                        missing_op,
                        range_start = from_op,
                        range_end = to_op,
                        rebuilt = entries.len(),
                        "RebuildPipeline: journal gap at op {missing_op}, \
                         truncating sequencer from {to_op} to {rebuilt_up_to} \
                         ({}/{} ops rebuilt)",
                        entries.len(),
                        to_op - from_op + 1,
                    );
                    consensus.sequencer().set_sequence(rebuilt_up_to);
                }
                let mut pipeline = consensus.pipeline().borrow_mut();
                for entry in entries {
                    pipeline.push(entry);
                }
            }
            _ => {}
        }
    }
}
