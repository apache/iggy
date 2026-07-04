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

#[cfg(any(test, feature = "simulator"))]
use consensus::LocalPipeline;
use consensus::{
    Consensus, MetadataHandle, MuxPlane, PartitionsHandle, Pipeline, Plane, PlaneKind, Sequencer,
    VsrAction, VsrConsensus,
};
use iggy_binary_protocol::{
    Command2, CommitHeader, DoViewChangeHeader, GenericHeader, Operation, PrepareHeader,
    PrepareOkHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
};
#[cfg(any(test, feature = "simulator"))]
use iggy_common::PartitionStats;
use iggy_common::variadic;
use iggy_common::{IggyExpiry, IggyTimestamp};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::listener::MessageHandler;
use metadata::IggyMetadata;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;
use partitions::{IggyPartition, IggyPartitions, PollFragments, PollingArgs, PollingConsumer};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use shards_table::ShardsTable;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
#[cfg(any(test, feature = "simulator"))]
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
    /// Cluster-wide VSR replica id; independent of `IggyShard::id`.
    pub self_replica_id: u8,
    pub replica_count: u8,
    pub bus: B,
}

/// Replica id + count bundle.
///
/// Adjacent `u8` params (`self_replica_id`, `replica_count`) were a
/// silent-swap hazard at the call site; the named struct gives the type
/// system a chance to catch a misorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaTopology {
    pub self_replica_id: u8,
    pub replica_count: u8,
}

impl ReplicaTopology {
    #[must_use]
    pub const fn new(self_replica_id: u8, replica_count: u8) -> Self {
        Self {
            self_replica_id,
            replica_count,
        }
    }
}

impl<B> PartitionConsensusConfig<B>
where
    B: MessageBus,
{
    #[must_use]
    pub const fn new(cluster_id: u128, topology: ReplicaTopology, bus: B) -> Self {
        Self {
            cluster_id,
            self_replica_id: topology.self_replica_id,
            replica_count: topology.replica_count,
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
/// submit failure; all `MetadataSubmitError` variants are transient by
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
    /// A peer (home) shard relays a client's replicated request to shard 0
    /// and awaits the committed reply over `reply` (`None` on a transient
    /// submit failure). The home shard then writes the reply to the
    /// originating socket -- it owns the connection and the
    /// `vsr -> transport` mapping, which shard 0 cannot reconstruct from the
    /// consensus client id.
    ClientRequest {
        request: Message<GenericHeader>,
        reply: Sender<Option<Message<GenericHeader>>>,
    },
    /// A shard's partition reconciler asks shard 0 to complete a cooperative
    /// consumer-group revocation (the source drained the partition or it timed
    /// out). Server-originated: shard 0 proposes it through metadata consensus
    /// with no client session. Fire-and-forget + idempotent -- `reply` carries
    /// the commit op (or `None` on a transient submit failure) for logging only.
    CompleteRevocation {
        stream_id: u32,
        topic_id: u32,
        group_id: u64,
        source_client_id: u128,
        partition_id: u32,
        reply: Sender<Option<u64>>,
    },
}

/// Handler shard 0 runs for an inbound [`MetadataSubmit`].
///
/// server-ng wires it to `submit_register_in_process` /
/// `submit_logout_in_process` / `submit_request_in_process` and sends the
/// result back over the frame's `reply` sender. A peer shard (no consensus)
/// must never receive this frame.
pub type MetadataSubmitHandler = Rc<dyn Fn(MetadataSubmit)>;

/// One connected client's identity, as seen by the shard that homes it.
///
/// Gathered from every shard for `get_clients` (shared-nothing: each shard
/// knows only its own connections, so the full list requires a broadcast
/// -- see [`IggyShard::list_all_clients`]).
#[derive(Debug, Clone)]
pub struct ConnectedClientInfo {
    /// Transport (coordinator-minted) client id; top 16 bits are the home
    /// shard. The wire `client_id` is the `u32` seq tail.
    pub client_id: u128,
    /// Bound VSR client id, if the connection completed register. Keys the
    /// connection to its consumer-group memberships (stored by VSR id, not
    /// transport id).
    pub vsr_client_id: Option<u128>,
    pub user_id: Option<u32>,
    pub transport: ClientTransportKind,
    pub address: std::net::SocketAddr,
    /// SDK identity from the login version prefix; `None` pre-login.
    /// In-memory only: the `get_clients` wire response is shared with the
    /// legacy server, so exposing these on the wire is a follow-up.
    pub sdk_name: Option<String>,
    pub sdk_version: Option<String>,
    /// Packed protocol version, see `iggy_binary_protocol::ProtocolVersion`.
    pub protocol_version: Option<u32>,
}

/// Handler each shard runs for an inbound [`LifecycleFrame::ListClients`].
/// server-ng wires it to read the shard's `SessionManager` and push its
/// connected clients back over the carried reply sender.
pub type ListClientsHandler = Rc<dyn Fn(Sender<Vec<ConnectedClientInfo>>)>;

/// Per-shard reply budget for the `list_all_clients` gather. A shard that
/// doesn't answer within this window is skipped (partial result) so one
/// wedged shard can't hang the read.
const LIST_CLIENTS_GATHER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// A read executed on the shard that owns a partition: a message poll or a
/// consumer-offset lookup. Carried by [`LifecycleFrame::PartitionRead`];
/// see [`IggyShard::partition_read`].
#[derive(Debug)]
pub enum PartitionRead {
    Poll {
        consumer: PollingConsumer,
        args: PollingArgs,
    },
    ConsumerOffset {
        consumer: PollingConsumer,
    },
    /// Cooperative-rebalance classification: the group's last-polled and
    /// committed offsets on this partition, so the join enrichment can tell an
    /// in-flight partition (committed < last-polled) from a never-polled/drained
    /// one. `group_id` is the monotonic consumer-group id (offset key).
    GroupOffsetState {
        group_id: u64,
    },
    /// Drop the group's ephemeral `last_polled` mark on this partition. The
    /// join-time gather issues this when it finds an uncommitted `last_polled`
    /// for a partition no live member owns: the residue of a since-removed
    /// member (reconnect). Clearing it stops a later join in the same restart
    /// from misreading the dead mark as a live in-flight hold. `group_id` is the
    /// monotonic consumer-group id (offset key).
    ClearGroupLastPolled {
        group_id: u64,
    },
    /// Resolve a client `DeleteSegments` count into a concrete truncation
    /// offset: the `end_offset` of the `count`-th oldest sealed segment. Run on
    /// the owning shard, which alone holds the partition's segment state.
    ResolveSegmentDeleteOffset {
        count: u32,
    },
}

/// Reply to a [`PartitionRead`].
#[derive(Debug)]
pub enum PartitionReadReply {
    Poll {
        fragments: PollFragments,
        current_offset: u64,
    },
    ConsumerOffset {
        stored: Option<u64>,
        current_offset: u64,
    },
    /// Reply to [`PartitionRead::GroupOffsetState`]: the group's last-polled and
    /// committed offsets on this partition (each `None` if absent).
    GroupOffsetState {
        last_polled: Option<u64>,
        committed: Option<u64>,
    },
    /// Acknowledges a [`PartitionRead::ClearGroupLastPolled`].
    Ack,
    /// Reply to [`PartitionRead::ResolveSegmentDeleteOffset`]: the resolved
    /// truncation offset, or `None` when the partition has no sealed segments
    /// to delete.
    SegmentDeleteOffset { up_to_offset: Option<u64> },
    /// The owning shard has no materialised partition for the namespace
    /// (unknown, tombstoned, or mid-reconcile). Callers surface an error
    /// instead of an empty result.
    NotFound,
}

/// Handler the owning shard runs for an inbound
/// [`LifecycleFrame::PartitionRead`]. server-ng wires it to its partitions
/// plane; the handler pushes the result back over the carried reply sender.
pub type PartitionReadHandler =
    Rc<dyn Fn(IggyNamespace, PartitionRead, Sender<PartitionReadReply>)>;

/// Reply budget for a cross-shard [`IggyShard::partition_read`]. Bounds a
/// wedged owning shard; the caller maps expiry to a client-visible error.
///
/// 10s, not lower: a disk poll over tiny segments opens one file per
/// segment, so a 1024-message read can legitimately take several seconds
/// on an oversubscribed host (8 parallel test clusters). Expiry is masked
/// as an empty poll downstream while the abandoned walk keeps running, so
/// a too-small budget turns slow reads into missing data plus duplicated
/// walks from client retries. Must stay below the SDK's 30s request
/// deadline.
const PARTITION_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

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
    /// Shard 0 distributes an inbound replica TCP connection fd to the
    /// owning shard BEFORE any byte is read (blind delegation - the peer
    /// id is unknown until the `ReplicaHello` is read). The receiving
    /// shard wraps the fd, runs the acceptor handshake in its own
    /// spawned task (`message_bus::replica::handshake`), installs the
    /// connection on success, and answers shard 0 with
    /// [`LifecycleFrame::ReplicaInboundHandshakeDone`] echoing `slot`.
    /// The `fd` is an owning [`DupedFd`] so that a frame dropped
    /// unprocessed (shutdown, pump drain abort, router panic before
    /// `install_*_fd`) closes the dup instead of leaking it.
    ReplicaInboundSetup { fd: DupedFd, slot: u64 },
    /// Shard 0 dialed the higher-id peer `replica_id` and delegates the
    /// raw connection; the receiving shard runs the dialer handshake
    /// half, installs on success, and answers shard 0 with
    /// [`LifecycleFrame::ReplicaOutboundHandshakeDone`] so the
    /// pending-dial entry clears and the reconnect sweep may redial on
    /// failure.
    ReplicaOutboundSetup { fd: DupedFd, replica_id: u8 },
    /// Owning shard -> shard 0: a delegated inbound handshake finished
    /// (any outcome). Releases the global in-flight cap slot. Lost acks
    /// are covered by the slot's deadline expiry on shard 0.
    ReplicaInboundHandshakeDone { slot: u64 },
    /// Owning shard -> shard 0: a delegated outbound handshake finished
    /// (any outcome). Clears the pending-dial entry for `replica_id`.
    /// Lost acks are covered by the entry's deadline expiry on shard 0.
    ReplicaOutboundHandshakeDone { replica_id: u8 },
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
    /// Broadcast query for `get_clients`: every shard replies with the
    /// clients whose connections it homes, over `reply`. Unlike
    /// [`MetadataSubmit`] this is sent to ALL shards (shared-nothing: each
    /// shard knows only its own connections). See
    /// [`IggyShard::list_all_clients`].
    ListClients {
        reply: Sender<Vec<ConnectedClientInfo>>,
    },
    /// Execute a partition read (message poll / consumer-offset lookup) on
    /// the shard that owns `namespace` and push the result back over
    /// `reply`. See [`IggyShard::partition_read`].
    PartitionRead {
        namespace: IggyNamespace,
        read: PartitionRead,
        reply: Sender<PartitionReadReply>,
    },
    /// Shard 0 broadcasts after a partition-shaped metadata commit; wakes
    /// the per-shard reconciler. No payload: reconciler re-reads target
    /// state. Drops covered by the periodic safety tick.
    MetadataCommitTick,
    /// Wake marker for the reconciler-to-pump funnel. Pump drains the
    /// shard's `reconcile_queue` on receipt; tail drain on every frame
    /// catches dropped markers.
    ReconcileApply,
    /// Per-shard segment-cleaner request: delete expired / over-budget sealed
    /// segments of `namespace` on the pump, serialized with reads. The timer
    /// task resolves `message_expiry` / `max_bytes` from metadata and stamps
    /// `now`; the pump only mutates. Local and unreplicated — each replica
    /// trims its own log (divergence is invisible: reads hit the primary).
    CleanPartition {
        namespace: IggyNamespace,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    },
    /// Reconciler-staged enforcement of a committed `TruncatePartition`
    /// watermark: delete sealed segments up to `up_to_offset` on the pump,
    /// serialized with reads. Each replica applies the committed offset
    /// locally and idempotently.
    TruncatePartition {
        namespace: IggyNamespace,
        up_to_offset: u64,
    },
    /// Reconciler-staged enforcement of a committed `PurgeTopic`: reset the
    /// partition to a single empty segment at offset 0 and clear consumer
    /// offsets on the pump, serialized with reads. `generation` is the
    /// committed purge generation; the pump no-ops if the partition already
    /// applied it, so a redundant reconcile pass never re-wipes live data.
    PurgePartition {
        namespace: IggyNamespace,
        generation: u64,
    },
}

/// Reconciler-staged partition mutation.
///
/// Funnelling through the pump keeps `IggyPartitions` single-writer:
/// without it the cooperative `.await` scheduler would race
/// `insert` / `remove` against the pump's live `&mut IggyPartition` (UB).
pub enum ReconcileOp<B>
where
    B: MessageBus,
{
    /// Materialise an owned partition. Boxed to keep variants size-balanced
    /// (`clippy::large_enum_variant`). `epoch` is the committed
    /// `Partition::created_revision`, stored on the routing row so a later
    /// reconcile pass can detect a slab-key-reused stale partition.
    InsertOwned {
        namespace: IggyNamespace,
        partition: Box<IggyPartition<B>>,
        epoch: u64,
    },
    /// Seed a routing row for a partition owned by a peer shard.
    InsertRouted {
        namespace: IggyNamespace,
        owner: ShardId,
        epoch: u64,
    },
    /// Final phase of teardown: drop the `IggyPartition` value and clear
    /// the tombstone. The reconciler sets the tombstone + removes the
    /// `shards_table` row synchronously *before* awaiting the disk delete
    /// (writers are fenced via [`IggyPartitions::is_tombstoned`]), so by
    /// the time this op runs the disk hierarchy is already gone.
    ConfirmRemove { namespace: IggyNamespace },
    /// Drop a routing row (peer's partition gone from committed metadata).
    RemoveRouted { namespace: IggyNamespace },
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

    /// Handler for inbound [`LifecycleFrame::ListClients`] broadcast
    /// queries. Every shard receives these (not just shard 0); server-ng
    /// wires it to its per-shard `SessionManager`. Defaults to a no-op for
    /// the simulator stub ctor.
    on_list_clients: ListClientsHandler,

    /// Handler for inbound [`LifecycleFrame::PartitionRead`] queries.
    /// server-ng wires it to this shard's partitions plane. Defaults to a
    /// no-op for the simulator stub ctor.
    on_partition_read: PartitionReadHandler,

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

    /// Stored for `init_partition` (simulator-only). Production materialises
    /// VSR replicas through `partition_helpers::build_partition_fresh`, which
    /// passes the topology + cluster id directly.
    #[cfg_attr(not(any(test, feature = "simulator")), allow(dead_code))]
    partition_consensus: PartitionConsensusConfig<B>,

    /// Shard 0 coordinator, supplied at construction. Holds round-robin
    /// state for replica and client delegation. `None` on non-zero shards
    /// and in single-shard tests that bypass the coordinator.
    coordinator: Option<Rc<crate::coordinator::ShardZeroCoordinator>>,

    /// Per-shard observability counters. Cloned at metric increment sites,
    /// so cheap (`Arc` clone) regardless of label cardinality.
    metrics: crate::metrics::ShardMetrics,

    /// Late-bound `MetadataCommitTick` handler. `None` until reconciler
    /// wires it; pre-wire ticks drop with a metric bump.
    metadata_tick_handler: RefCell<Option<Rc<dyn Fn()>>>,

    /// Reconciler → pump funnel. Borrow discipline: every push / drain
    /// runs without `.await` inside the borrow.
    reconcile_queue: RefCell<VecDeque<ReconcileOp<B>>>,

    /// Partition-plane frames that arrived before this shard's reconciler
    /// materialised the namespace (post-`CreateTopic` convergence window).
    /// Parked here instead of dropped -- there is no consensus retransmit
    /// driver in production yet -- and re-dispatched when the matching
    /// `ReconcileOp::InsertOwned` lands. Bounded per namespace; overflow
    /// drops the frame (at-least-once: client/primary retries recover).
    pending_partition_frames: RefCell<HashMap<IggyNamespace, Vec<Message<GenericHeader>>>>,
}

impl<B, MJ, S, M, T> IggyShard<B, MJ, S, M, T>
where
    B: MessageBus + 'static,
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
        on_list_clients: ListClientsHandler,
        on_partition_read: PartitionReadHandler,
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
            on_list_clients,
            on_partition_read,
            senders,
            shard_count,
            inbox,
            shards_table,
            partition_consensus,
            coordinator,
            metrics,
            metadata_tick_handler: RefCell::new(None),
            reconcile_queue: RefCell::new(VecDeque::new()),
            pending_partition_frames: RefCell::new(HashMap::new()),
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

    /// Gather every shard's connected clients (the `get_clients`
    /// scatter-gather). Broadcasts [`LifecycleFrame::ListClients`] to all
    /// shards -- including self, so the local shard answers over the same
    /// channel path -- and collects their replies.
    ///
    /// Bounded: a shard that doesn't reply within
    /// [`LIST_CLIENTS_GATHER_TIMEOUT`] is skipped and the partial result is
    /// logged, so one wedged shard cannot hang the read. Callers should
    /// treat the result as best-effort-complete.
    #[allow(clippy::future_not_send)]
    pub async fn list_all_clients(&self) -> Vec<ConnectedClientInfo> {
        let shard_count = self.shard_count as usize;
        let (reply_tx, reply_rx) = channel::<Vec<ConnectedClientInfo>>(shard_count.max(1));
        let mut expected = 0usize;
        for sender in &self.senders {
            let frame = ShardFrame::lifecycle(LifecycleFrame::ListClients {
                reply: reply_tx.clone(),
            });
            if let Err(error) = sender.try_send(frame) {
                tracing::warn!(
                    shard = self.id,
                    target = sender.shard_id(),
                    "list_all_clients: inbox rejected ListClients frame: {error:?}"
                );
            } else {
                expected += 1;
            }
        }
        // Drop the local handle so `recv` returns `Err` once every shard's
        // reply sender is dropped (defensive; we also bound by count).
        drop(reply_tx);

        let mut clients = Vec::new();
        let mut received = 0usize;
        // One deadline across the whole gather: each `recv` waits only the
        // remaining budget, so total wall time is bounded by
        // LIST_CLIENTS_GATHER_TIMEOUT (not expected * timeout), while the
        // partial results gathered so far are still returned on expiry.
        let deadline = std::time::Instant::now() + LIST_CLIENTS_GATHER_TIMEOUT;
        while received < expected {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    shard = self.id,
                    received,
                    expected,
                    "list_all_clients: gather budget exhausted; returning partial result"
                );
                break;
            }
            match compio::time::timeout(remaining, reply_rx.recv()).await {
                Ok(Ok(batch)) => {
                    clients.extend(batch);
                    received += 1;
                }
                Ok(Err(_)) => break, // all reply senders dropped
                Err(_) => {
                    tracing::warn!(
                        shard = self.id,
                        received,
                        expected,
                        "list_all_clients: gather timed out; returning partial result"
                    );
                    break;
                }
            }
        }
        clients
    }

    /// Run a partition read (message poll / consumer-offset lookup) on the
    /// shard owning `namespace` and await the reply.
    ///
    /// Routes a [`LifecycleFrame::PartitionRead`] through the shards table
    /// (self-sends included, so a locally-owned partition takes the same
    /// path). `None` = unroutable namespace, full owning-shard inbox,
    /// dropped reply sender, or [`PARTITION_READ_TIMEOUT`] expiry; the
    /// caller maps it to a client-visible error.
    #[allow(clippy::future_not_send)]
    pub async fn partition_read(
        &self,
        namespace: IggyNamespace,
        read: PartitionRead,
    ) -> Option<PartitionReadReply> {
        let Some(target) = self.shards_table.shard_for(namespace) else {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "partition_read: namespace not routable (not materialised yet or deleted)"
            );
            return None;
        };
        let (reply_tx, reply_rx) = channel::<PartitionReadReply>(1);
        let frame = ShardFrame::lifecycle(LifecycleFrame::PartitionRead {
            namespace,
            read,
            reply: reply_tx,
        });
        let sender = self.senders.get(target as usize)?;
        if let Err(error) = sender.try_send(frame) {
            tracing::warn!(
                shard = self.id,
                target,
                "partition_read: inbox rejected PartitionRead frame: {error:?}"
            );
            return None;
        }
        match compio::time::timeout(PARTITION_READ_TIMEOUT, reply_rx.recv()).await {
            Ok(Ok(reply)) => Some(reply),
            Ok(Err(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    "partition_read: reply sender dropped (handler not wired / shutdown)"
                );
                None
            }
            Err(_) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    "partition_read: owning shard did not reply within budget"
                );
                None
            }
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
    /// simulator never receives a replica connection-setup frame.
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
            on_list_clients: std::rc::Rc::new(|_| {}),
            on_partition_read: std::rc::Rc::new(|_, _, _| {}),
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
            metadata_tick_handler: RefCell::new(None),
            reconcile_queue: RefCell::new(VecDeque::new()),
            pending_partition_frames: RefCell::new(HashMap::new()),
        }
    }

    #[must_use]
    pub const fn shards_table(&self) -> &T {
        &self.shards_table
    }

    #[must_use]
    pub const fn metrics(&self) -> &crate::metrics::ShardMetrics {
        &self.metrics
    }

    /// `None` removes the handler; subsequent ticks drop with a metric bump.
    pub fn set_metadata_tick_handler(&self, handler: Option<Rc<dyn Fn()>>) {
        *self.metadata_tick_handler.borrow_mut() = handler;
    }

    /// Returns `true` if a handler ran. Pump bumps the drop metric on `false`.
    pub fn dispatch_metadata_commit_tick(&self) -> bool {
        self.signal_reconcile_wake()
    }

    /// Internal: invoke the installed wake handler (same channel the
    /// metadata commit tick uses). Called from `ConfirmRemove` so the
    /// reconciler re-runs immediately after the pump drops a tombstoned
    /// partition, tightening the delete-recreate-same-ns latency window
    /// from one `reconcile_periodic_interval` to one pump-iter.
    fn signal_reconcile_wake(&self) -> bool {
        let handler = self.metadata_tick_handler.borrow().clone();
        handler.is_some_and(|handler| {
            handler();
            true
        })
    }

    /// Stage a partition mutation for the pump.
    ///
    /// Marker `try_send` is best-effort; the pump's tail drain on every
    /// frame catches dropped markers, so the queue never strands ops.
    pub fn enqueue_reconcile_op(&self, op: ReconcileOp<B>) {
        self.reconcile_queue.borrow_mut().push_back(op);
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply));
    }

    /// Stage a segment-cleaner pass for `namespace` on this shard's pump. The
    /// timer task resolves retention config off-pump and stamps `now`; the pump
    /// is the single writer of partition state, so the deletion runs there,
    /// serialized with reads.
    pub fn request_clean_partition(
        &self,
        namespace: IggyNamespace,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::CleanPartition {
            namespace,
            now,
            message_expiry,
            max_bytes,
        }));
    }

    /// Stage a `TruncatePartition` enforcement for `namespace` on this shard's
    /// pump: delete sealed segments up to `up_to_offset`. The reconciler calls
    /// this after observing a committed delete watermark for an owned partition.
    pub fn request_truncate_partition(&self, namespace: IggyNamespace, up_to_offset: u64) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::TruncatePartition {
            namespace,
            up_to_offset,
        }));
    }

    /// Stage a `PurgePartition` enforcement for `namespace` on this shard's
    /// pump: reset the partition to empty at offset 0 and clear consumer
    /// offsets. The reconciler calls this after observing a committed purge
    /// generation newer than the partition's locally applied one.
    pub fn request_purge_partition(&self, namespace: IggyNamespace, generation: u64) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PurgePartition {
            namespace,
            generation,
        }));
    }

    /// Drain and apply staged [`ReconcileOp`]s on the pump task.
    /// Synchronous: every arm is in-memory only. `ConfirmRemove`'s fsync +
    /// blocking close is offloaded to a detached task so the pump doesn't
    /// stall on bulk teardown.
    pub fn apply_reconcile_ops(&self)
    where
        B: MessageBus + 'static,
    {
        let staged: Vec<ReconcileOp<B>> = {
            let mut q = self.reconcile_queue.borrow_mut();
            if q.is_empty() {
                return;
            }
            q.drain(..).collect()
        };
        let self_shard_id = self.id;
        let partitions = self.plane.partitions();
        let mut confirmed_remove = false;
        for op in staged {
            match op {
                ReconcileOp::InsertOwned {
                    namespace,
                    partition,
                    epoch,
                } => {
                    // Idempotent apply, mirroring `ConfirmRemove` (idempotent
                    // via `remove`'s `None` early-return). The reconciler
                    // stages this from a task separate from the pump, so under
                    // a commit burst two passes can each observe
                    // `!contains(ns)` and build the same namespace before
                    // either drains here. A second unconditional `insert`
                    // would push a duplicate partition and overwrite the
                    // `ns -> idx` entry, orphaning the first (its VSR group +
                    // segment writers leak and `len` inflates). The discarded
                    // build is a fresh empty incarnation over the same on-disk
                    // path the kept one owns, so dropping it just closes a few
                    // fds.
                    if partitions.contains(&namespace) {
                        drop(partition);
                        continue;
                    }
                    partitions.insert(namespace, *partition);
                    self.shards_table.insert(
                        namespace,
                        PartitionLocation::new(ShardId::new(self_shard_id), epoch),
                    );
                    self.metrics.record_partition_materialised();
                    // Re-dispatch frames that arrived before this partition
                    // materialised (see `park_if_unmaterialised`). `dispatch`
                    // re-routes them onto our own inbox, so the pump
                    // processes them after this drain completes.
                    let parked = self
                        .pending_partition_frames
                        .borrow_mut()
                        .remove(&namespace);
                    if let Some(frames) = parked {
                        tracing::debug!(
                            shard = self_shard_id,
                            namespace_raw = namespace.inner(),
                            count = frames.len(),
                            "re-dispatching parked partition frames after materialisation"
                        );
                        for frame in frames {
                            if let Some(sender) = self.senders.get(self_shard_id as usize)
                                && let Err(error) =
                                    sender.try_send(ShardFrame::consensus(self_shard_id, frame))
                            {
                                tracing::warn!(
                                    shard = self_shard_id,
                                    namespace_raw = namespace.inner(),
                                    "dropping parked partition frame: inbox rejected: {error:?}"
                                );
                            }
                        }
                    }
                }
                ReconcileOp::InsertRouted {
                    namespace,
                    owner,
                    epoch,
                } => {
                    self.shards_table
                        .insert(namespace, PartitionLocation::new(owner, epoch));
                }
                ReconcileOp::ConfirmRemove { namespace } => {
                    // Tombstone bit set + shards_table row removed synchronously
                    // by the reconciler before this op was enqueued, so no
                    // in-flight frame can reach the partition between `remove`
                    // and the drop here. Teardown already unlinked the on-disk
                    // hierarchy via `delete_partitions_from_disk`, so the
                    // partition drops inline: its compio file handles close
                    // through io_uring without blocking, and no fsync is wanted
                    // on data that is already gone.
                    let removed = partitions.remove(&namespace);
                    partitions.untombstone(&namespace);
                    // A topic created then deleted before its `InsertOwned`
                    // pass never drains parked frames the normal way; reclaim
                    // them here so they cannot leak across many create-delete
                    // races (the partition is gone, so the frames are moot).
                    self.discard_parked_partition_frames(namespace);
                    self.metrics.record_partition_removed();
                    confirmed_remove = true;
                    if removed.is_none() {
                        tracing::trace!(
                            shard = self_shard_id,
                            namespace_raw = namespace.inner(),
                            "ConfirmRemove with no in-memory partition (retry after disk-delete failure)"
                        );
                    }
                }
                ReconcileOp::RemoveRouted { namespace } => {
                    self.shards_table.remove(&namespace);
                    self.discard_parked_partition_frames(namespace);
                }
            }
        }

        if confirmed_remove {
            // Re-wake the reconciler once per drain batch so a delete→recreate
            // of a namespace that landed in STM while the unlink was in-flight
            // materialises within one pump-iter, not one
            // `reconcile_periodic_interval`. The wake channel is capacity-1, so
            // a per-op wake would coalesce anyway; firing once avoids K
            // redundant handler borrows on a bulk DeleteStream.
            self.signal_reconcile_wake();
        }
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
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        match MessageBag::try_from(message) {
            Ok(MessageBag::Request(request)) => {
                let routing = (request.header().operation, request.header().namespace);
                if let Some(request) = self.park_if_unmaterialised(request, routing.0, routing.1) {
                    self.on_request(request).await;
                }
            }
            Ok(MessageBag::Prepare(prepare)) => {
                let routing = (prepare.header().operation, prepare.header().namespace);
                if let Some(prepare) = self.park_if_unmaterialised(prepare, routing.0, routing.1) {
                    self.on_replicate(prepare).await;
                    // A follower learns the cluster commit point from the
                    // commit_max piggybacked on each prepare; the Commit
                    // heartbeat carries commit_min and stops advancing
                    // commit_max once the piggyback has raced ahead, so
                    // on_commit alone never drains a follower's journal. Drive
                    // it here off the prepare, as the metadata plane does inside
                    // its own on_replicate.
                    if routing.0.is_partition() {
                        let planes = self.plane.inner();
                        let config = planes.1.0.config();
                        let namespace = IggyNamespace::from_raw(routing.1);
                        if let Some(partition) = planes.1.0.get_mut_by_ns(&namespace)
                            && partition.consensus().is_follower()
                        {
                            partition.commit_journal(config).await;
                        }
                    }
                }
            }
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

    /// Drop parked frames for a namespace that will never materialise (it was
    /// removed before its `ReconcileOp::InsertOwned`), so the pending entry is
    /// reclaimed instead of leaking until process exit.
    fn discard_parked_partition_frames(&self, namespace: IggyNamespace) {
        if let Some(frames) = self
            .pending_partition_frames
            .borrow_mut()
            .remove(&namespace)
            && !frames.is_empty()
        {
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                count = frames.len(),
                "discarding parked partition frames for removed namespace"
            );
        }
    }

    /// Park a partition-plane frame whose namespace this shard has not yet
    /// materialised (post-`CreateTopic` convergence window: the metadata
    /// commit precedes the reconciler pass that builds the local replica).
    ///
    /// Returns `Some(message)` when the frame should be processed normally:
    /// non-partition operation, namespace materialised, or namespace
    /// tombstoned (the plane's own tombstone guard handles the drop).
    /// Returns `None` when the frame was parked (or dropped on overflow);
    /// [`Self::apply_reconcile_ops`] re-dispatches parked frames once the
    /// matching `ReconcileOp::InsertOwned` lands.
    fn park_if_unmaterialised<H>(
        &self,
        message: Message<H>,
        operation: Operation,
        namespace_raw: u64,
    ) -> Option<Message<H>>
    where
        H: iggy_binary_protocol::ConsensusHeader,
    {
        const MAX_PARKED_PER_NAMESPACE: usize = 128;
        if !operation.is_partition() {
            return Some(message);
        }
        let namespace = IggyNamespace::from_raw(namespace_raw);
        let partitions = self.plane.partitions();
        if partitions.contains(&namespace) || partitions.is_tombstoned(&namespace) {
            return Some(message);
        }
        let mut pending = self.pending_partition_frames.borrow_mut();
        let parked = pending.entry(namespace).or_default();
        if parked.len() >= MAX_PARKED_PER_NAMESPACE {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "parked-frame buffer full; dropping partition frame"
            );
            return None;
        }
        tracing::debug!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            operation = ?operation,
            "parking partition frame until namespace materialises"
        );
        parked.push(message.into_generic());
        None
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
                Output = metadata::stm::result::ApplyReply,
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
                Output = metadata::stm::result::ApplyReply,
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
                Output = metadata::stm::result::ApplyReply,
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
    /// Invariant: planes do not produce loopback messages FOR EACH OTHER.
    /// `on_ack` never pushes to another plane's loopback, so draining
    /// metadata before partitions is order-independent. Within its own
    /// plane, `on_ack` CAN push loopback entries (a metadata commit promotes
    /// buffered requests, and each promoted prepare self-acks through
    /// `send_or_loopback(self)`) -- `repair_primary_self_acks` drains those
    /// residuals itself; see its interleaved drain.
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
                Output = metadata::stm::result::ApplyReply,
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
            // `get_by_ns` returns `None` for tombstoned namespaces: skip
            // draining their loopback queue so we don't surface PrepareOk
            // frames targeting a partition the reconciler is tearing down.
            let Some(partition) = planes.1.0.get_by_ns(&namespace) else {
                continue;
            };
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

    /// Simulator-only. Mutates `IggyPartitions` off the pump task,
    /// bypassing the reconciler's `ReconcileOp::InsertOwned` funnel (the
    /// production runtime path; bootstrap recovery uses `load_partition`),
    /// so it must never run in production. VSR replica id comes from
    /// `PartitionConsensusConfig`, not `self.id` (the local shard index). A
    /// `-p iggy-server-ng` build excludes the `simulator` feature and this
    /// method; `cargo build --workspace` compiles it in but with no
    /// production caller.
    #[cfg(any(test, feature = "simulator"))]
    pub fn init_partition(&self, namespace: IggyNamespace)
    where
        B: MessageBus + Clone,
    {
        let partitions = self.plane.partitions();
        if partitions.contains(&namespace) {
            return;
        }

        let consensus = VsrConsensus::new(
            self.partition_consensus.cluster_id,
            self.partition_consensus.self_replica_id,
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

    /// Resolve the single partition a VSR control frame addresses, keyed by
    /// `header.namespace`. Warns and returns `None` when the namespace matches
    /// neither metadata nor a live partition consensus. Returns `&mut` because
    /// `on_do_view_change` / `on_commit` need it for `commit_journal`; the read-
    /// only callers reborrow `&`. Pump-only (sole mutator), so the `&mut` formed
    /// here via interior mutability cannot alias a concurrent reconcile.
    #[allow(clippy::mut_from_ref)]
    fn resolve_partition_target<'a>(
        &self,
        partitions: &'a IggyPartitions<B>,
        namespace: u64,
        view: u32,
        replica: u8,
        frame: &'static str,
    ) -> Option<&'a mut IggyPartition<B>>
    where
        B: MessageBus,
    {
        let Some(partition) = partitions.get_mut_by_ns(&IggyNamespace::from_raw(namespace)) else {
            tracing::warn!(
                shard = self.id,
                namespace,
                view,
                replica,
                frame,
                "dropping VSR control frame: namespace matches neither metadata nor partition consensus"
            );
            return None;
        };
        debug_assert_eq!(
            partition.consensus().namespace(),
            namespace,
            "keyed partition lookup must match the frame namespace"
        );
        Some(partition)
    }

    /// Handle an incoming VSR control frame. A metadata frame uses the metadata
    /// consensus; a partition frame addresses exactly one partition, resolved by
    /// [`Self::resolve_partition_target`].
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

        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.namespace,
            header.view,
            header.replica,
            "StartViewChange",
        ) else {
            return;
        };
        let consensus = partition.consensus();
        let actions = consensus.handle_start_view_change(PlaneKind::Partitions, &header);
        dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
        dispatch_partition_journal_actions(consensus, partition, &actions).await;
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
                Output = metadata::stm::result::ApplyReply,
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

        let config = planes.1.0.config();
        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.namespace,
            header.view,
            header.replica,
            "DoViewChange",
        ) else {
            return;
        };
        let consensus = partition.consensus();
        let actions = consensus.handle_do_view_change(PlaneKind::Partitions, &header);
        dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
        dispatch_partition_journal_actions(consensus, partition, &actions).await;
        if actions
            .iter()
            .any(|action| matches!(action, VsrAction::CommitJournal))
        {
            partition.commit_journal(config).await;
        }
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

        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.namespace,
            header.view,
            header.replica,
            "StartView",
        ) else {
            return;
        };
        let consensus = partition.consensus();
        let actions = consensus.handle_start_view(PlaneKind::Partitions, &header);
        dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
        dispatch_partition_journal_actions(consensus, partition, &actions).await;
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
                Output = metadata::stm::result::ApplyReply,
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

        let config = planes.1.0.config();
        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.namespace,
            header.view,
            header.replica,
            "Commit",
        ) else {
            return;
        };
        let consensus = partition.consensus();
        if consensus.handle_commit(&header) {
            partition.commit_journal(config).await;
        }
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
        // Fan out over every group (each partition's heartbeat/retransmit timer
        // must advance), so the keyed single-namespace lookup the control-frame
        // handlers use does not apply here. The namespaces are snapshotted into
        // an owned Vec so no partitions-plane borrow is held across the tick
        // `.await`.
        // TODO(hubcio): reuse the pump's `namespace_scratch` (as
        // `process_loopback` does) to drop this per-tick alloc; a quiet cluster
        // still pays one Vec per heartbeat.
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();

        for namespace in namespaces {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                continue;
            };

            let consensus = partition.consensus();
            let actions = consensus.tick(PlaneKind::Partitions);
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
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
    {
        let metadata = self.plane.metadata();
        let Some(ref consensus) = metadata.consensus else {
            return;
        };

        let actions = consensus.tick(PlaneKind::Metadata);

        dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &actions).await;

        // Repair a lost primary self-ack: `RetransmitPrepares` to self is a
        // no-op, so the timer-driven retransmit above cannot recover the
        // primary's own missing vote. Without this the commit prefix can pin
        // forever (commit_min stuck below commit_max). See
        // `IggyMetadata::repair_primary_self_acks`.
        metadata.repair_primary_self_acks().await;
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
