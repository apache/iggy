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
pub use router::CONSENSUS_TICK_INTERVAL;

#[cfg(any(test, feature = "simulator"))]
use consensus::LocalPipeline;
use consensus::{
    CommitOutcome, Consensus, ConsensusClock, MetadataHandle, MuxPlane, PartitionsHandle, Pipeline,
    Plane, PlaneKind, Sequencer, VsrAction, VsrConsensus, build_deny_reply_from_request_header,
};
#[cfg(any(test, feature = "simulator"))]
use crossfire::AsyncRxTrait;
use futures::FutureExt;
use iggy_binary_protocol::{
    Command2, CommitHeader, DoViewChangeHeader, GenericHeader, Operation, PrepareHeader,
    PrepareOkHeader, RepairPrepareHeader, RepairRangeReplyHeader, RequestHeader,
    RequestPreparesHeader, RequestStartViewHeader, RequestStateChunkHeader,
    RequestStateTransferHeader, StartViewChangeHeader, StartViewHeader, StateChunkHeader,
    StateTransferTargetHeader,
};
#[cfg(any(test, feature = "simulator"))]
use iggy_common::PartitionStats;
use iggy_common::variadic;
use iggy_common::{IggyError, IggyExpiry, IggyTimestamp};
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::listener::MessageHandler;
use metadata::IggyMetadata;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;
use metadata::{BoundSession, MetadataSubmitError};
use partitions::{IggyPartition, IggyPartitions, PollFragments, PollingArgs, PollingConsumer};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use shards_table::ShardsTable;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::rc::Rc;
#[cfg(any(test, feature = "simulator"))]
use std::sync::Arc;

pub type ShardPlane<B, J, S, M, SB = PingPongSuperblock> =
    MuxPlane<variadic!(IggyMetadata<VsrConsensus<B>, J, S, M, SB>, IggyPartitions<B, SB>)>;

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
    /// Time source handed to every partition consensus group built from
    /// this config (`init_partition`, simulator-only). Production groups
    /// are built by `partition_helpers::build_partition_fresh` on the
    /// system-clock default instead.
    pub clock: ConsensusClock,
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
    pub fn new(cluster_id: u128, topology: ReplicaTopology, bus: B) -> Self {
        Self::with_clock(cluster_id, topology, bus, ConsensusClock::system())
    }

    /// [`Self::new`] with an explicit time source for the partition
    /// consensus groups; the simulator passes its virtual clock here.
    #[must_use]
    pub const fn with_clock(
        cluster_id: u128,
        topology: ReplicaTopology,
        bus: B,
        clock: ConsensusClock,
    ) -> Self {
        Self {
            cluster_id,
            self_replica_id: topology.self_replica_id,
            replica_count: topology.replica_count,
            bus,
            clock,
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
/// and awaits the outcome over `reply`. `Register` carries the submit error
/// verbatim because one variant
/// (`MetadataSubmitError::ClientIdOwnedByAnotherUser`) is terminal and must
/// not be retried; the remaining variants are transient by contract.
pub enum MetadataSubmit {
    Register {
        vsr_client_id: u128,
        user_id: u32,
        /// The committed bind, or the submit error verbatim. The error must
        /// survive the hop: the ownership refusal is TERMINAL, and flattening it
        /// into "no reply" makes the login look transient, which costs the
        /// client a retry storm of full password verifications.
        reply: Sender<Result<BoundSession, MetadataSubmitError>>,
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
    /// to delete. `lagging` means this replica has not converged on the
    /// replicated log (follower, mid-view-change, or `commit_min` behind
    /// `commit_max`): a `None` offset is then transient rather than a settled
    /// no-op, since sealed segments may exist that this replica has not
    /// learned about. A converged replica's committed-but-unflushed resident
    /// tail does NOT make the no-op transient.
    SegmentDeleteOffset {
        up_to_offset: Option<u64>,
        lagging: bool,
    },
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

/// Race `future` against a bus timer: `Some` if it finishes within `budget`,
/// `None` if the timer fires first. Uses [`MessageBus::sleep`] (virtual under
/// the simulator, wall-clock in production) rather than `compio::time::timeout`,
/// which panics outside a compio runtime and so cannot run under the
/// deterministic executor.
#[allow(clippy::future_not_send)]
async fn bus_timeout<B, F>(bus: &B, budget: std::time::Duration, future: F) -> Option<F::Output>
where
    B: MessageBus,
    F: Future,
{
    let future = future.fuse();
    let timer = bus.sleep(budget).fuse();
    futures::pin_mut!(future, timer);
    futures::select_biased! {
        output = future => Some(output),
        () = timer => None,
    }
}

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
pub enum ReconcileOp<B, SB = PingPongSuperblock>
where
    B: MessageBus,
{
    /// Materialise an owned partition. Boxed to keep variants size-balanced
    /// (`clippy::large_enum_variant`). `epoch` is the committed
    /// `Partition::created_revision`, stored on the routing row so a later
    /// reconcile pass can detect a slab-key-reused stale partition.
    InsertOwned {
        namespace: IggyNamespace,
        partition: Box<IggyPartition<B, SB>>,
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

/// Prepares served per `RequestPrepares` round.
///
/// The per-peer bus queues are bounded (`peer_queue_capacity`, 256 by default)
/// and overrun frames drop silently, so an unbounded burst loses its own tail;
/// the receiver pulls the window chunk by chunk instead (each walked
/// `RepairDone` immediately requests the next chunk while progress holds).
///
/// Runtime default; server-ng overrides the live ceiling per shard from
/// `[cluster] repair_chunk_max` at bootstrap.
pub const REPAIR_CHUNK_MAX: u64 = 128;

/// One in-flight metadata journal-repair stream (shard 0 only).
#[derive(Debug, Clone, Copy)]
struct MetadataRepairSession {
    nonce: u128,
    to_op: u64,
    /// Re-request target on stall.
    peer: u8,
    /// Ticks since the stream last made progress; at
    /// [`partitions::REPAIR_RETRY_TICKS`] the remaining window is
    /// re-requested from `peer`.
    idle_ticks: u32,
}

/// The metadata state machine, as every handler that walks or restores it
/// needs it.
///
/// A blanket-implemented alias for a three-part bound that was pasted verbatim
/// at eleven sites across this file and `router.rs`. No API change: anything
/// satisfying the parts satisfies this.
pub trait MetadataStm:
    StreamsFrontend
    + StateMachine<
        Input = Message<PrepareHeader>,
        Output = metadata::stm::result::ApplyReply,
        Error = iggy_common::IggyError,
    >
{
}

impl<M> MetadataStm for M where
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = metadata::stm::result::ApplyReply,
            Error = iggy_common::IggyError,
        >
{
}

/// [`MetadataStm`] plus in-place snapshot restore: the additional capability a
/// state-transfer install needs over a plain commit walk.
pub trait RestorableMetadataStm:
    MetadataStm
    + metadata::stm::snapshot::RestoreSnapshotInPlace<metadata::stm::snapshot::MetadataSnapshot>
{
}

impl<M> RestorableMetadataStm for M where
    M: MetadataStm
        + metadata::stm::snapshot::RestoreSnapshotInPlace<metadata::stm::snapshot::MetadataSnapshot>
{
}

/// Chunk size for state-transfer artifact pulls. Lockstep (one in flight),
/// so the bounded per-peer bus queue can never drop a burst tail. Clamped
/// against the live bus ceiling by
/// [`IggyShard::state_chunk_len_max`] rather than assumed to fit.
const STATE_CHUNK_LEN: u32 = 256 * 1024;

/// Bus frame ceiling assumed before bootstrap overrides it. Matches the
/// shipped `[message_bus] max_message_size` so the simulator and unit tests
/// clamp the same way a default deployment does.
const DEFAULT_BUS_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Stall rounds a receiver spends on ONE peer before abandoning the transfer
/// and falling back to journal repair. The retry has no peer re-selection, so
/// this is what keeps a peer that died mid-transfer from wedging the rejoining
/// node; repair then re-picks a target and re-arms a transfer if the gap is
/// still below the new peer's retained floor.
const STATE_TRANSFER_MAX_STALL_RETRIES: u32 = 5;

/// Decode-failure rounds a receiver spends on ONE snapshot generation before
/// refusing to pull it again. Keyed on the offered `snapshot_seq`: a peer that
/// checkpoints resets the budget (new bytes are worth full retries), while a
/// generation this build cannot decode ends up costing one refused descriptor
/// per repair round instead of a full snapshot pull.
const STATE_TRANSFER_MAX_DECODE_RETRIES: u32 = 5;

/// Serving-side offer lifetime, as a multiple of the repair-retry interval. An
/// offer resets its counter on every chunk it serves, so this only expires one
/// that stopped being pulled -- a receiver that finished installing (the
/// protocol has no completion frame) or gave up.
const STATE_TRANSFER_OFFER_EXPIRY_MULTIPLE: u32 = 10;

/// Lifetime of a FULLY SERVED offer, as a multiple of the repair-retry
/// interval. It only has to outlive the receiver re-requesting a lost final
/// chunk, but the receiver's stall re-request fires at exactly one such
/// interval, so a one-interval grace is a coin flip against its own retry plus
/// a network hop -- and losing the race costs a full re-pull (`UnknownOffer`
/// drops the session with every byte already downloaded).
const STATE_TRANSFER_SERVED_EXPIRY_MULTIPLE: u32 = 3;

/// One artifact of an accepted transfer target: its manifest entry plus the
/// bytes received so far (chunks are sequential, so `buf.len()` doubles as
/// the next request offset).
#[derive(Debug)]
struct ArtifactProgress {
    entry: consensus::StateArtifact,
    buf: Vec<u8>,
}

impl ArtifactProgress {
    const fn complete(&self) -> bool {
        self.buf.len() as u64 == self.entry.len
    }
}

/// One in-flight metadata state transfer (shard 0 only): a cluster-restart
/// rejoin replacing its snapshot-shaped state (metadata snapshot + client
/// table) from the live primary before tail repair.
#[derive(Debug)]
struct MetadataTransferSession {
    nonce: u128,
    /// Serving primary; also the stall re-request target.
    peer: u8,
    /// Serving peer's applied frontier from the accepted descriptor.
    commit_op: u64,
    /// Empty until the `StateTransferTarget` manifest is accepted, then one
    /// entry per offered artifact, pulled in manifest order.
    artifacts: Vec<ArtifactProgress>,
    /// Whether a descriptor has been accepted (an accepted EMPTY manifest is
    /// distinguishable from "still waiting").
    target_accepted: bool,
    /// Ticks with no frame progress; at the configured repair-retry
    /// threshold the missing piece is re-requested.
    idle_ticks: u32,
}

/// A cached serving-side state-transfer offer (shard 0 of the serving
/// primary). Keyed by requester replica id so a rebooted requester's fresh
/// nonce replaces the stale offer; chunks must all come from ONE offer or
/// the artifact checksums cannot hold.
///
/// The offer itself is refcounted, so simultaneous rejoiners on the same
/// snapshot generation share one copy of the snapshot bytes.
struct ServedStateTransfer {
    nonce: u128,
    offer: Rc<metadata::StateTransferOffer>,
    /// Ticks since this offer last served a chunk. An offer owns a full copy of
    /// the snapshot and the encoded client table, so a completed or abandoned
    /// transfer must not pin them for the process lifetime. There is no
    /// completion frame in the protocol (the receiver installs and goes quiet),
    /// so the serving side ages the offer out instead.
    idle_ticks: u32,
    /// Set once the final chunk of the final artifact has been served, which is
    /// the closest thing to a completion signal this side gets. Such an offer
    /// expires after a single retry interval instead of the full idle window:
    /// the short grace still covers the receiver re-requesting a dropped last
    /// chunk, while releasing the snapshot copy an order of magnitude sooner
    /// than waiting out the abandoned-transfer timeout.
    fully_served: bool,
}

/// What `on_request_state_chunk` decided inside its offers borrow; the wire
/// sends run after the borrow drops.
enum ChunkReply {
    Chunk(Message<StateChunkHeader>),
    /// Offer evicted (e.g. the serving process restarted): the requester
    /// gets an unavailable descriptor and restarts its session.
    UnknownOffer,
}

pub struct IggyShard<B, MJ, S, M, T = (), SB = PingPongSuperblock>
where
    B: MessageBus,
{
    pub id: u16,
    pub name: String,
    pub plane: ShardPlane<B, MJ, S, M, SB>,

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

    /// In-flight metadata journal repair: set when the recovery
    /// handshake finds this replica's WAL behind the group frontier, cleared
    /// at `RepairDone`. Metadata never needs a commit floor -- its WAL keeps
    /// the full prefix -- so only the stream identity is tracked.
    metadata_repair: RefCell<Option<MetadataRepairSession>>,

    /// In-flight metadata state transfer (cluster-restart rejoin); tail
    /// repair takes over at install. See [`MetadataTransferSession`].
    metadata_transfer: RefCell<Option<MetadataTransferSession>>,

    /// Serving-side cache of state-transfer offers, keyed by requester
    /// replica id. Bounded by the replica count; replaced per fresh nonce.
    metadata_transfer_offers: RefCell<HashMap<u8, ServedStateTransfer>>,

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
    reconcile_queue: RefCell<VecDeque<ReconcileOp<B, SB>>>,

    /// Partition-plane frames that arrived before this shard's reconciler
    /// materialised the namespace (post-`CreateTopic` convergence window).
    /// Parked here instead of dropped -- there is no consensus retransmit
    /// driver in production yet -- and re-dispatched when the matching
    /// `ReconcileOp::InsertOwned` lands. Bounded per namespace; overflow
    /// drops the frame (at-least-once: client/primary retries recover).
    pending_partition_frames: RefCell<HashMap<IggyNamespace, Vec<Message<GenericHeader>>>>,

    /// Live ceiling on prepares served per `RequestPrepares` round. Defaults
    /// to [`REPAIR_CHUNK_MAX`]; server-ng overrides it from
    /// `[cluster] repair_chunk_max` at bootstrap.
    repair_chunk_max: Cell<u64>,

    /// Live stalled-repair retry threshold in consensus ticks. Defaults to
    /// [`partitions::REPAIR_RETRY_TICKS`]; server-ng overrides it from
    /// `[cluster] repair_retry_interval` at bootstrap.
    repair_retry_ticks: Cell<u32>,

    /// Live `[message_bus] max_message_size`. Bounds a served state chunk: a
    /// frame above this is rejected by the RECEIVING transport, which tears
    /// down the whole replica connection. Defaults to a value that leaves
    /// [`STATE_CHUNK_LEN`] usable; server-ng overrides it at bootstrap.
    bus_max_message_size: Cell<usize>,

    /// Consecutive metadata state-transfer rounds that made no progress.
    ///
    /// Deliberately NOT on [`MetadataTransferSession`]: three of the four
    /// arming sites mint a fresh session, so a per-session counter bounded
    /// nothing. Held here it survives the abandon -> repair -> re-arm cycle,
    /// and chunk arrival resets it (see
    /// [`IggyShard::note_metadata_transfer_progress`]) so scattered transient
    /// stalls cannot accumulate into abandoning a nearly-complete transfer.
    /// That reset also means it bounds SILENT peers only: decode failures keep
    /// frames flowing, and are bounded separately by
    /// [`Self::metadata_transfer_decode_failures`].
    metadata_transfer_attempts: Cell<u32>,

    /// Decode failures charged against one snapshot generation, as
    /// `(snapshot_seq, failures)`. `None` until a pulled artifact set first
    /// fails to decode; cleared by a successful install. Past
    /// [`STATE_TRANSFER_MAX_DECODE_RETRIES`] the generation's descriptors are
    /// refused outright -- without that gate every repair round would re-pull
    /// the full snapshot just to fail the same way, since each pulled chunk
    /// legitimately resets [`Self::metadata_transfer_attempts`].
    metadata_transfer_decode_failures: Cell<Option<(u64, u32)>>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Depth of this shard's inbound frame queue.
    ///
    /// Diagnostic accessor for the simulator's lost-wake tripwire: at
    /// executor quiescence a live pump must have drained its inbox, so a
    /// non-zero depth means a frame reached the channel without waking the
    /// pump. Gated to test/simulator builds (sole caller is the sim), matching
    /// the sibling `ShardMetrics::frame_drops_value`.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn inbox_len(&self) -> usize {
        self.inbox.len()
    }

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
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M, SB>,
        partitions: IggyPartitions<B, SB>,
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
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            metadata_transfer_offers: RefCell::new(HashMap::new()),
            repair_chunk_max: Cell::new(REPAIR_CHUNK_MAX),
            repair_retry_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            bus_max_message_size: Cell::new(DEFAULT_BUS_MAX_MESSAGE_SIZE),
            metadata_transfer_attempts: Cell::new(0),
            metadata_transfer_decode_failures: Cell::new(None),
        })
    }

    /// Override the stalled-repair retry threshold (consensus ticks) from
    /// configuration. Called once per shard at bootstrap; the simulator and
    /// tests keep the compile-time [`partitions::REPAIR_RETRY_TICKS`] default.
    pub fn set_repair_retry_ticks(&self, ticks: u32) {
        self.repair_retry_ticks.set(ticks);
    }

    /// Override the per-round repair-serving chunk ceiling from configuration.
    /// Called once per shard at bootstrap; the simulator and tests keep the
    /// compile-time [`REPAIR_CHUNK_MAX`] default.
    pub fn set_repair_chunk_max(&self, chunk: u64) {
        self.repair_chunk_max.set(chunk);
    }

    /// Override the message-bus frame ceiling from configuration
    /// (`[message_bus] max_message_size`). Called once per shard at bootstrap;
    /// the simulator and tests keep the compile-time default.
    pub fn set_bus_max_message_size(&self, max_message_size: usize) {
        self.bus_max_message_size.set(max_message_size);
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
        // One deadline across the whole gather, timed on the injected clock
        // (virtual under the simulator, wall-clock in production) via a single
        // `bus.sleep` raced against collecting every reply. Reading
        // `Instant::now` for the budget instead would desync the deterministic
        // executor, whose schedule must be a pure function of the seed; the
        // bus sleep is the clock the rest of the pump already times against.
        // Total time stays bounded by LIST_CLIENTS_GATHER_TIMEOUT and the
        // partial results gathered so far are still returned on expiry.
        let gather = async {
            while received < expected {
                match reply_rx.recv().await {
                    Ok(batch) => {
                        clients.extend(batch);
                        received += 1;
                    }
                    Err(_) => break, // all reply senders dropped
                }
            }
        };
        if bus_timeout(&self.bus, LIST_CLIENTS_GATHER_TIMEOUT, gather)
            .await
            .is_none()
        {
            tracing::warn!(
                shard = self.id,
                received,
                expected,
                "list_all_clients: gather timed out; returning partial result"
            );
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
        match bus_timeout(&self.bus, PARTITION_READ_TIMEOUT, reply_rx.recv()).await {
            Some(Ok(reply)) => Some(reply),
            Some(Err(_)) => {
                tracing::warn!(
                    shard = self.id,
                    target,
                    "partition_read: reply sender dropped (handler not wired / shutdown)"
                );
                None
            }
            None => {
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
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M, SB>,
        partitions: IggyPartitions<B, SB>,
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
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            metadata_transfer_offers: RefCell::new(HashMap::new()),
            repair_chunk_max: Cell::new(REPAIR_CHUNK_MAX),
            repair_retry_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            bus_max_message_size: Cell::new(DEFAULT_BUS_MAX_MESSAGE_SIZE),
            metadata_transfer_attempts: Cell::new(0),
            metadata_transfer_decode_failures: Cell::new(None),
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
    /// frame and its consensus-tick drain catch dropped markers, so the
    /// queue never strands ops for longer than one tick.
    pub fn enqueue_reconcile_op(&self, op: ReconcileOp<B, SB>) {
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
        let staged: Vec<ReconcileOp<B, SB>> = {
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

/// Routing verdict of [`IggyShard::park_if_unmaterialised`].
enum ParkOutcome<H> {
    /// Namespace is materialised (or the frame is not a partition op):
    /// process normally.
    Deliver(Message<H>),
    /// Frame was parked until the namespace materialises (or dropped on
    /// park overflow).
    Parked,
    /// Namespace is mid-teardown. Client requests must be denied with a
    /// transient status; replicated traffic still flows to the plane, whose
    /// own tombstone guards drop it.
    Tombstoned(Message<H>),
}

/// Local message processing — these methods handle messages that have been
/// routed to this shard via the message pump.
impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
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
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
        T: ShardsTable,
    {
        match MessageBag::try_from(message) {
            Ok(MessageBag::Request(request)) => {
                let routing = (request.header().operation, request.header().namespace);
                match self.park_if_unmaterialised(request, routing.0, routing.1) {
                    // The incarnation fence runs only here, on client traffic.
                    // A backup denying what the primary admitted would diverge
                    // the replicas, so replicated frames are never fenced.
                    ParkOutcome::Deliver(request)
                        if !self.serves_committed_incarnation(routing.0, routing.1) =>
                    {
                        self.deny_partition_request_transient(request.header())
                            .await;
                    }
                    ParkOutcome::Deliver(request) => self.on_request(request).await,
                    // Deny instead of forwarding into the partition plane's
                    // tombstone guard: that guard drops the frame without a
                    // reply, and the transports decode replies in lockstep,
                    // so silence wedges the connection until the SDK's
                    // response read-timeout.
                    ParkOutcome::Tombstoned(request) => {
                        self.deny_partition_request_transient(request.header())
                            .await;
                    }
                    ParkOutcome::Parked => {}
                }
            }
            Ok(MessageBag::Prepare(prepare)) => {
                let routing = (prepare.header().operation, prepare.header().namespace);
                // A tombstoned prepare still flows to the plane: replicated
                // traffic has no client awaiting a reply on this node, and
                // the plane's own tombstone guard drops it.
                match self.park_if_unmaterialised(prepare, routing.0, routing.1) {
                    ParkOutcome::Deliver(prepare) | ParkOutcome::Tombstoned(prepare) => {
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
                    ParkOutcome::Parked => {}
                }
            }
            Ok(MessageBag::PrepareOk(prepare_ok)) => self.on_ack(prepare_ok).await,
            Ok(MessageBag::StartViewChange(msg)) => self.on_start_view_change(msg).await,
            Ok(MessageBag::DoViewChange(msg)) => self.on_do_view_change(msg).await,
            Ok(MessageBag::StartView(msg)) => self.on_start_view(msg).await,
            Ok(MessageBag::Commit(ref msg)) => self.on_commit(msg).await,
            Ok(MessageBag::RequestStartView(ref msg)) => self.on_request_start_view(msg).await,
            Ok(MessageBag::RequestPrepares(ref msg)) => self.on_request_prepares(msg).await,
            Ok(MessageBag::RepairPrepare(msg)) => self.on_repair_prepare(msg).await,
            Ok(MessageBag::RepairRangeReply(ref msg)) => self.on_repair_range_reply(msg).await,
            Ok(MessageBag::RequestStateTransfer(ref msg)) => {
                self.on_request_state_transfer(msg).await;
            }
            Ok(MessageBag::StateTransferTarget(ref msg)) => {
                self.on_state_transfer_target(msg).await;
            }
            Ok(MessageBag::RequestStateChunk(ref msg)) => self.on_request_state_chunk(msg).await,
            Ok(MessageBag::StateChunk(ref msg)) => self.on_state_chunk(msg).await,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
            }
        }
    }

    /// Does the partition materialised under `namespace_raw` belong to the
    /// incarnation the committed metadata denotes?
    ///
    /// A delete + recreate of the same stream / topic / partition tuple recycles
    /// the freed slab keys, so the namespace is byte-identical across
    /// incarnations and presence proves nothing: a request admitted against the
    /// prior incarnation is journaled and acked, then erased when the reconciler
    /// tears that incarnation down. `created_revision` is the sole
    /// discriminator - the committed value must equal the epoch this shard
    /// stored on the routing row when it materialised the partition.
    ///
    /// Either side missing is a failed proof, not a pass: the row may lag the
    /// plane or vanish entirely, but it never runs ahead, so an unverifiable
    /// pairing means the reconciler has yet to converge. Non-partition
    /// operations address no incarnation and always pass.
    #[must_use]
    pub fn serves_committed_incarnation(&self, operation: Operation, namespace_raw: u64) -> bool
    where
        M: StreamsFrontend,
        T: ShardsTable,
    {
        if !operation.is_partition() {
            return true;
        }
        let namespace = IggyNamespace::from_raw(namespace_raw);
        let committed = self
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace);
        let row = self.shards_table.epoch_for(namespace);
        if committed.is_some() && committed == row {
            return true;
        }
        tracing::debug!(
            shard = self.id,
            namespace_raw,
            operation = ?operation,
            committed_revision = ?committed,
            row_epoch = ?row,
            "denying partition request against an unverified incarnation"
        );
        false
    }

    /// Drop parked frames for a namespace that will never materialise (it was
    /// removed before its `ReconcileOp::InsertOwned`), so the pending entry is
    /// reclaimed instead of leaking until process exit. Parked client requests
    /// are denied with a transient status rather than dropped: the transports
    /// decode replies in lockstep, so silence wedges the connection until the
    /// SDK's response read-timeout.
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
            for frame in frames {
                if frame.header().command == Command2::Request
                    && let Ok(request) = frame.try_into_typed::<RequestHeader>()
                {
                    // Callers are synchronous (`apply_reconcile_ops`), so the
                    // deny rides the pump's outbound lifecycle path instead of
                    // an inline bus send.
                    self.stage_transient_deny(request.header());
                }
            }
        }
    }

    /// Park a partition-plane frame whose namespace this shard has not yet
    /// materialised (post-`CreateTopic` convergence window: the metadata
    /// commit precedes the reconciler pass that builds the local replica).
    ///
    /// Tombstoned namespaces (teardown fence set by the reconciler before the
    /// disk delete) report [`ParkOutcome::Tombstoned`] so the caller can deny
    /// client requests instead of feeding them to the plane's silent-drop
    /// guard, while replicated traffic still flows there. Parked frames are
    /// re-dispatched by [`Self::apply_reconcile_ops`] once the matching
    /// `ReconcileOp::InsertOwned` lands; overflow drops the frame
    /// (at-least-once: client/primary retries recover).
    fn park_if_unmaterialised<H>(
        &self,
        message: Message<H>,
        operation: Operation,
        namespace_raw: u64,
    ) -> ParkOutcome<H>
    where
        H: iggy_binary_protocol::ConsensusHeader,
    {
        const MAX_PARKED_PER_NAMESPACE: usize = 128;
        if !operation.is_partition() {
            return ParkOutcome::Deliver(message);
        }
        let namespace = IggyNamespace::from_raw(namespace_raw);
        let partitions = self.plane.partitions();
        // Tombstone outranks presence: the partition value stays in the vec
        // until `ConfirmRemove` drains, but the fence already forbids serving
        // it.
        if partitions.is_tombstoned(&namespace) {
            return ParkOutcome::Tombstoned(message);
        }
        if partitions.contains(&namespace) {
            return ParkOutcome::Deliver(message);
        }
        let mut pending = self.pending_partition_frames.borrow_mut();
        let parked = pending.entry(namespace).or_default();
        if parked.len() >= MAX_PARKED_PER_NAMESPACE {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "parked-frame buffer full; dropping partition frame"
            );
            return ParkOutcome::Parked;
        }
        tracing::debug!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            operation = ?operation,
            "parking partition frame until namespace materialises"
        );
        parked.push(message.into_generic());
        ParkOutcome::Parked
    }

    /// Deny a client partition request with `TransientNotAccepted`: the frame
    /// never reached journal admission, so the SDK can safely replay it
    /// anywhere, and partition rebuild completes well inside the replay
    /// budget. Sent directly over the bus; delivery failure is terminal for
    /// this reply (the client recovers via its own read-timeout).
    #[allow(clippy::future_not_send)]
    async fn deny_partition_request_transient(&self, request_header: &RequestHeader) {
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::TransientNotAccepted.as_code(),
        );
        if let Err(error) = self
            .bus
            .send_to_client(request_header.client, reply.into_generic().into_frozen())
            .await
        {
            tracing::warn!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                error = %error,
                "failed to send transient deny for partition request"
            );
        }
    }

    /// [`Self::deny_partition_request_transient`] for synchronous callers:
    /// hand the deny to this shard's own pump as a
    /// [`LifecycleFrame::ForwardClientSend`], whose handler performs the bus
    /// send (same funnel the parked-frame re-dispatch uses).
    fn stage_transient_deny(&self, request_header: &RequestHeader) {
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::TransientNotAccepted.as_code(),
        );
        let frame = ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
            client_id: request_header.client,
            msg: reply.into_generic().into_frozen(),
        });
        if let Some(sender) = self.senders.get(self.id as usize)
            && let Err(error) = sender.try_send(frame)
        {
            tracing::warn!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                "dropping transient deny for discarded partition frame: inbox rejected: {error:?}"
            );
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
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
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
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
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
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
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
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
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

        let consensus = VsrConsensus::with_clock(
            self.partition_consensus.cluster_id,
            self.partition_consensus.self_replica_id,
            self.partition_consensus.replica_count,
            namespace.inner(),
            self.partition_consensus.bus.clone(),
            LocalPipeline::new(),
            self.partition_consensus.clock.clone(),
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
        partitions: &'a IggyPartitions<B, SB>,
        namespace: u64,
        view: u32,
        replica: u8,
        frame: &'static str,
    ) -> Option<&'a mut IggyPartition<B, SB>>
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
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            }
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
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
        }
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
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_do_view_change(PlaneKind::Metadata, &header);
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            }
            // Same transfer gate as `on_start_view` and `on_commit`: the
            // pre-install STM must not walk while a transfer is in flight.
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
                && !consensus.is_transferring()
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
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
        }
        // Outside the gate: the persist fences the SEND, not the local commit
        // walk (state a crash forgets is state no peer ever saw).
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
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let actions = consensus.handle_start_view(PlaneKind::Metadata, &header);
            // Every rejection path (wrong primary, old view, stale incarnation,
            // below the commit floor, self-sent) returns no actions, and an
            // adopted StartView always emits at least `CommitJournal`. That
            // makes emptiness the adoption signal -- and the arms below must
            // not fire on a StartView this replica did not adopt.
            let adopted = !actions.is_empty();
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            }
            // State transfer (rejoin behind the peers' retained floor): the
            // adopted view names a live primary to fetch snapshot-shaped state
            // from. The commit walk and journal repair are deferred until the
            // install lands -- walking the pre-transfer STM would apply ops the
            // snapshot already contains, and the transfer replaces the table
            // anyway.
            //
            // Gated on `adopted`: a stale StartView leaves `header.replica`
            // pointing at a replica that need not be primary, and re-arming on
            // one would re-mint the nonce (dropping the descriptor already in
            // flight through the nonce filter) and, before the budget moved off
            // the session, reset the retry bound as well.
            //
            // Outside the superblock gate above: that gate fail-closes the VSR
            // actions this replica would VOUCH with (notably `PrepareOk`) until
            // the adopted view is durable. Requesting a transfer vouches for
            // nothing -- it only pulls state -- and a transferring replica
            // withholds `PrepareOk` on its own (`is_transferring`). Gating it
            // would also wedge the one path that repairs a replica whose gap
            // sits below every peer's floor.
            if adopted
                && consensus.state_transfer_stage() == consensus::StateTransferStage::AwaitingTarget
            {
                tracing::info!(
                    shard = self.id,
                    peer = header.replica,
                    "adopted a live view while awaiting transfer; requesting metadata state transfer"
                );
                self.arm_metadata_transfer(consensus, header.replica).await;
                return;
            }
            // Mid-transfer the pre-install STM must not walk: the snapshot
            // being installed already contains those ops, and a walk that
            // advances `commit_min` past the incoming `snapshot_seq` flips the
            // install to table-only (no STM restore, no persist, no pairing)
            // while still reporting success. Landing inside the install's
            // superblock await instead trips `set_commit_floor`'s anti-rewind
            // assert. The `AwaitingTarget` return above covers only that one
            // stage; `Fetching` and `Installing` fall through to here.
            if consensus.is_transferring() {
                return;
            }
            // `dispatch_vsr_actions` deliberately no-ops `CommitJournal` (it
            // needs the plane); without this the ops a StartView marks
            // committed stay journaled-but-unapplied forever, because the
            // follow-up heartbeats see commit_max already advanced and skip
            // their own commit_journal.
            if actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
            {
                planes.0.commit_journal().await;
            }
            // Adoption can leave this replica knowing a frontier its WAL
            // cannot reach (StartView carries numbers, not entries): the
            // walk above gap-stops. Fill the hole through journal repair
            // from the announcing primary.
            self.maybe_request_metadata_repair(consensus, header.replica)
                .await;
            return;
        }

        let config = planes.1.0.config();
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
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
            dispatch_partition_journal_actions(consensus, partition, &actions).await;
        }
        // Outside the gate: the persist fences the SEND, not the local commit
        // walk or the repair fetch below (a fetch asks to LEARN, it does not
        // advertise this replica's view).
        if actions
            .iter()
            .any(|action| matches!(action, VsrAction::CommitJournal))
        {
            partition.commit_journal(config).await;
        }
        // Same gap-fill as the metadata arm: a journal-less rejoiner that
        // adopted the new view still lacks the window's entries; repair from
        // the announcing primary, floor settled by its RangeEvicted.
        let consensus = partition.consensus();
        if consensus.is_normal()
            && consensus.commit_min() < consensus.commit_max()
            && partition.repair.is_none()
        {
            let nonce = iggy_common::random_id::get_uuid();
            let to_op = consensus.commit_max();
            let from_op = consensus.commit_min() + 1;
            let cluster = consensus.cluster();
            let self_id = consensus.replica();
            partition.repair = Some(partitions::RepairSession {
                nonce,
                to_op,
                floor: None,
                peer: header.replica,
                first_batch_offset: None,
                idle_ticks: 0,
            });
            self.send_request_prepares(
                cluster,
                self_id,
                header.replica,
                nonce,
                from_op,
                to_op,
                header.namespace,
            )
            .await;
        }
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
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            match consensus.handle_commit(&header) {
                CommitOutcome::Advanced => {
                    // Mid-transfer the pre-install STM must not walk: the
                    // snapshot being installed already contains those ops.
                    // `commit_max` still advanced inside `handle_commit`, so
                    // the post-install repair targets the right frontier.
                    if !consensus.is_transferring() {
                        planes.0.commit_journal().await;
                        // A heartbeat is the only signal a behind-but-same-view
                        // replica gets that the frontier moved: it advances
                        // `commit_max`, but the walk above cannot cross a gap in
                        // its own WAL (a late joiner missed the ops below the
                        // primary's active window; the primary only retransmits
                        // uncommitted ops, never the committed prefix). Without
                        // this, such a replica learns it is behind and does
                        // nothing about it -- metadata repair is otherwise only
                        // rooted at StartView adoption, which a same-view
                        // late joiner never sees. Request repair from the
                        // primary; if it has checkpointed past the gap the
                        // repair floor evicts and the handler above converts to
                        // state transfer. Idempotent: `maybe_request_metadata_repair`
                        // no-ops when caught up, already transferring, or a
                        // session is live, so a caught-up replica and a
                        // cold-start node (commit_max == commit_min == 0) both
                        // skip it.
                        self.maybe_request_metadata_repair(consensus, header.replica)
                            .await;
                    }
                }
                CommitOutcome::RespondStartView => {
                    // Durable-before-send: the StartView advertises this replica's
                    // current view, so persist before answering, as the view-change
                    // dispatch gate does. Withhold on failure; the stale peer keeps
                    // heartbeating, so it re-triggers once the tick persists.
                    if planes.0.persist_superblock_if_needed(consensus).await {
                        respond_start_view::<B, _, MJ>(consensus).await;
                    }
                }
                CommitOutcome::Accepted => {}
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
        match consensus.handle_commit(&header) {
            CommitOutcome::Advanced => partition.commit_journal(config).await,
            CommitOutcome::RespondStartView => {
                // Durable-before-send, as the metadata arm above: the StartView
                // advertises this replica's current view. Withhold on failure;
                // the stale peer keeps heartbeating, so it re-triggers once a
                // later persist succeeds.
                if partition.persist_superblock_if_needed().await {
                    respond_start_view::<B, _, MJ>(consensus).await;
                }
            }
            CommitOutcome::Accepted => {}
        }
    }

    /// `RequestStartView` probe from a restarted peer: the probed group's
    /// current primary answers with a `StartView`; a probe from the replica
    /// that IS the current primary-by-index makes backups elect immediately
    /// (the consensus handler decides; everyone else stays silent).
    #[allow(clippy::future_not_send)]
    async fn on_request_start_view(&self, msg: &Message<RequestStartViewHeader>)
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
            let actions = consensus.handle_request_start_view(PlaneKind::Metadata, &header);
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &actions).await;
            }
            return;
        }
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        let consensus = partition.consensus();
        let actions = consensus.handle_request_start_view(PlaneKind::Partitions, &header);
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
        }
    }

    /// Serve a repair range from this replica's journal: stream
    /// `RepairPrepare` frames (stored prepares verbatim, command byte
    /// rewritten) in op order, prefixed by `RangeEvicted` when the front of
    /// the range is no longer retained, terminated by `RepairDone`.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_request_prepares(&self, msg: &Message<RequestPreparesHeader>)
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
        let target = header.replica;
        // Snapshot the config-overridable chunk ceiling once; both plane
        // branches below serve the same per-round window.
        let repair_chunk_max = self.repair_chunk_max.get();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            if !consensus.is_normal() {
                return;
            }
            let Some(journal) = planes.0.journal.as_ref() else {
                return;
            };
            let journal = journal.handle();
            let cluster = consensus.cluster();
            let self_id = consensus.replica();
            let to_op = header.to_op.min(consensus.commit_max());
            // Skip the compacted prefix (below the snapshot floor) in one
            // RangeEvicted notice, then serve contiguously until the range
            // ends or the WAL runs out.
            let mut from_op = header.from_op;
            #[allow(clippy::cast_possible_truncation)]
            while from_op <= to_op && journal.header(from_op as usize).is_none() {
                from_op += 1;
            }
            if from_op > to_op {
                // Nothing in the requested range is retained. Answer the
                // eviction honestly: a bare `RepairDone(to_op)` here would
                // claim full coverage while serving zero prepares, and the
                // requester would clear its session and gap-stop silently.
                self.send_repair_range_reply(
                    cluster,
                    self_id,
                    target,
                    Command2::RangeEvicted,
                    header.nonce,
                    from_op,
                    header.namespace,
                )
                .await;
                self.send_repair_range_reply(
                    cluster,
                    self_id,
                    target,
                    Command2::RepairDone,
                    header.nonce,
                    header.from_op.saturating_sub(1),
                    header.namespace,
                )
                .await;
                return;
            }
            if from_op > header.from_op {
                self.send_repair_range_reply(
                    cluster,
                    self_id,
                    target,
                    Command2::RangeEvicted,
                    header.nonce,
                    from_op,
                    header.namespace,
                )
                .await;
            }
            let chunk_end = to_op.min(from_op.saturating_add(repair_chunk_max - 1));
            let mut served_through = from_op.saturating_sub(1);
            for op in from_op..=chunk_end {
                #[allow(clippy::cast_possible_truncation)]
                let Some(entry_header) = journal.header(op as usize).map(|h| *h) else {
                    break;
                };
                let Some(entry) = journal.entry(&entry_header).await else {
                    break;
                };
                if !self
                    .send_repair_prepare(target, entry.into_generic().into_frozen())
                    .await
                {
                    break;
                }
                served_through = op;
            }
            self.send_repair_range_reply(
                cluster,
                self_id,
                target,
                Command2::RepairDone,
                header.nonce,
                served_through,
                header.namespace,
            )
            .await;
            return;
        }
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        if !partition.consensus().is_normal() {
            return;
        }
        let cluster = partition.consensus().cluster();
        let self_id = partition.consensus().replica();
        let to_op = header.to_op.min(partition.consensus().commit_max());
        let retained_from = partition.log.journal().inner.repair_retained_from();
        let mut from_op = header.from_op;
        if let Some(retained_from) = retained_from
            && retained_from > from_op
        {
            self.send_repair_range_reply(
                cluster,
                self_id,
                target,
                Command2::RangeEvicted,
                header.nonce,
                retained_from,
                header.namespace,
            )
            .await;
            from_op = retained_from;
        }
        let chunk_end = to_op.min(from_op.saturating_add(repair_chunk_max - 1));
        let mut served_through = from_op.saturating_sub(1);
        for op in from_op..=chunk_end {
            let Some(entry) = partition.log.journal().inner.repair_entry(op) else {
                break;
            };
            if !self.send_repair_prepare(target, entry).await {
                break;
            }
            served_through = op;
        }
        self.send_repair_range_reply(
            cluster,
            self_id,
            target,
            Command2::RepairDone,
            header.nonce,
            served_through,
            header.namespace,
        )
        .await;
        tracing::info!(
            shard = self.id,
            namespace_raw = header.namespace,
            target,
            from_op = header.from_op,
            to_op,
            served_through,
            "served partition repair range"
        );
    }

    /// Ingest one repaired prepare. Metadata journals it into the WAL (the
    /// commit walk at `RepairDone` applies it); partitions journal + stage it
    /// through the same apply path as live replication, minus fence and ack.
    #[allow(clippy::future_not_send)]
    async fn on_repair_prepare(&self, msg: Message<RepairPrepareHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
    {
        tracing::debug!(
            shard = self.id,
            op = msg.header().0.op,
            namespace_raw = msg.header().0.namespace,
            "repair prepare received"
        );
        // Convert to a live-prepare frame exactly once, here at the apply
        // site (the frame must stay `RepairPrepare` up to this point: the
        // router round-trips bags through generic bytes, and a live-Prepare
        // command byte would land the re-parse on the view fence). The
        // inner layout IS a stored prepare; downstream journal/apply paths
        // run full prepare validation on it.
        let msg = msg.transmute_header(|old: RepairPrepareHeader, new: &mut PrepareHeader| {
            *new = old.0;
            new.command = Command2::Prepare;
        });
        let header = *msg.header();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let session = *self.metadata_repair.borrow();
            let Some(session) = session else {
                return;
            };
            if header.op > session.to_op || header.op <= consensus.commit_min() {
                return;
            }
            let Some(journal) = planes.0.journal.as_ref() else {
                return;
            };
            let journal = journal.handle();
            #[allow(clippy::cast_possible_truncation)]
            if journal.header(header.op as usize).is_some() {
                return;
            }
            if let Err(error) = journal.append(msg).await {
                tracing::warn!(
                    shard = self.id,
                    op = header.op,
                    %error,
                    "failed to journal repaired metadata prepare"
                );
                return;
            }
            // Contiguous-frontier advance, mirroring
            // `apply_repaired_prepare`: DVC advertises the sequencer, so a
            // hole below a repaired op must stall the advance rather than
            // mint an election candidate with an unwalkable log.
            let mut frontier = consensus.sequencer().current_sequence();
            #[allow(clippy::cast_possible_truncation)]
            while journal.header((frontier + 1) as usize).is_some() {
                frontier += 1;
            }
            if frontier > consensus.sequencer().current_sequence() {
                consensus.sequencer().set_sequence(frontier);
            }
            consensus.set_last_prepare_checksum(header.checksum);
            return;
        }
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        partition.apply_repaired_prepare(msg).await;
    }

    /// Repair stream terminator: `RangeEvicted` settles the partition commit
    /// floor candidate; `RepairDone` runs the commit walk over the repaired
    /// window and closes the session.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_repair_range_reply(&self, msg: &Message<RepairRangeReplyHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.namespace() == header.namespace
        {
            let session = *self.metadata_repair.borrow();
            let Some(session) = session else {
                return;
            };
            if header.nonce != session.nonce {
                return;
            }
            match header.command {
                Command2::RepairDone => {
                    let before = consensus.commit_min();
                    planes.0.commit_journal().await;
                    // Completion is decided by the LOCAL walk, not the
                    // peer's served-through claim: repair frames ride a
                    // lossy best-effort bus, so a fully-served stream can
                    // still arrive with holes. Anything short keeps the
                    // session armed; while the walk is making progress the
                    // next chunk is pulled immediately (the window is served
                    // in `REPAIR_CHUNK_MAX` slices), and a stalled one is
                    // left to the retry timer.
                    let commit_min = consensus.commit_min();
                    let done = commit_min >= session.to_op;
                    tracing::info!(
                        shard = self.id,
                        through_op = header.op,
                        commit_min,
                        done,
                        "metadata journal repair walked"
                    );
                    if done {
                        *self.metadata_repair.borrow_mut() = None;
                    } else if commit_min > before {
                        self.send_request_prepares(
                            consensus.cluster(),
                            consensus.replica(),
                            session.peer,
                            session.nonce,
                            commit_min + 1,
                            session.to_op,
                            header.namespace,
                        )
                        .await;
                    }
                }
                Command2::RangeEvicted => {
                    // Journal repair cannot close this gap: the serving peer
                    // compacted past it, so the ops this replica is missing no
                    // longer exist as WAL entries anywhere. This is the one
                    // authoritative "repair is impossible" signal, and it is
                    // shape-identical for every way a replica falls behind a
                    // checkpoint -- a fresh node joining an already-checkpointed
                    // cluster, a node whose partition healed after the quorum
                    // moved on, or a restart whose gap sits below the floor. All
                    // three convert here to state transfer against the peer that
                    // just announced the eviction (it has the checkpoint by
                    // definition), which replaces the snapshot-shaped state
                    // wholesale rather than replaying ops that are gone.
                    //
                    // Drop the repair session and arm the transfer only from
                    // `Idle`: a transfer already in flight owns the stage, and
                    // its own post-install tail repair can legitimately hit
                    // `RangeEvicted` again if the primary checkpointed mid
                    // transfer -- that reraises through the same path, and each
                    // round lifts the local floor, so it converges.
                    if consensus.state_transfer_stage() == consensus::StateTransferStage::Idle {
                        *self.metadata_repair.borrow_mut() = None;
                        consensus.begin_state_transfer_await();
                        tracing::info!(
                            shard = self.id,
                            peer = header.replica,
                            retained_from = header.op,
                            local_commit = consensus.commit_min(),
                            attempts = self.metadata_transfer_attempts.get(),
                            "metadata repair floor evicted; converting to state transfer"
                        );
                        self.arm_metadata_transfer(consensus, header.replica).await;
                    } else {
                        tracing::debug!(
                            shard = self.id,
                            retained_from = header.op,
                            stage = ?consensus.state_transfer_stage(),
                            "metadata repair range evicted while a transfer is already in flight"
                        );
                    }
                }
                _ => {}
            }
            return;
        }
        let config = planes.1.0.config().clone();
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        let Some(session) = partition.repair else {
            return;
        };
        if header.nonce != session.nonce {
            return;
        }
        match header.command {
            Command2::RangeEvicted => {
                if let Some(repair) = partition.repair.as_mut() {
                    repair.floor = Some(header.op.saturating_sub(1));
                }
            }
            Command2::RepairDone => {
                // `complete_repair` walks the window and clears the session
                // only when the LOCAL commit frontier reached the requested
                // op (the peer's served-through claim proves nothing about
                // delivery on a lossy bus). While the walk makes progress
                // the next chunk is pulled immediately; a stalled window is
                // left to the retry timer.
                let before = partition.consensus().commit_min();
                partition.complete_repair(&config).await;
                if partition.repair.is_none() {
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = header.namespace,
                        through_op = header.op,
                        "partition journal repair complete"
                    );
                } else {
                    let commit_min = partition.consensus().commit_min();
                    let next = partition.repair.as_ref().and_then(|live| {
                        (commit_min > before).then_some((live.peer, live.nonce, live.to_op))
                    });
                    let cluster = partition.consensus().cluster();
                    let self_id = partition.consensus().replica();
                    if let Some((peer, nonce, to_op)) = next {
                        self.send_request_prepares(
                            cluster,
                            self_id,
                            peer,
                            nonce,
                            commit_min + 1,
                            to_op,
                            header.namespace,
                        )
                        .await;
                    }
                }
            }
            _ => {}
        }
    }

    /// Ask `target` to stream its journaled prepares in `[from_op, to_op]`
    /// for `namespace`; answered by `RepairPrepare` frames terminated with
    /// `RepairDone` (prefixed by `RangeEvicted` when the front of the range
    /// is no longer retained).
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_request_prepares(
        &self,
        cluster: u128,
        self_id: u8,
        target: u8,
        nonce: u128,
        from_op: u64,
        to_op: u64,
        namespace: u64,
    ) where
        B: MessageBus,
    {
        let msg = Message::<RequestPreparesHeader>::new(size_of::<RequestPreparesHeader>())
            .transmute_header(|_, h: &mut RequestPreparesHeader| {
                h.command = Command2::RequestPrepares;
                h.cluster = cluster;
                h.replica = self_id;
                h.nonce = nonce;
                h.from_op = from_op;
                h.to_op = to_op;
                h.namespace = namespace;
                h.size = size_of::<RequestPreparesHeader>() as u32;
            });
        if self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await
            .is_err()
        {
            // The stall retry re-requests; without this line a dead peer
            // channel makes repair look like a silent server-side refusal.
            tracing::warn!(
                shard = self.id,
                target,
                from_op,
                to_op,
                namespace_raw = namespace,
                "request-prepares send failed; stall retry will re-request"
            );
        }
    }

    /// Send a stored prepare (raw journal bytes) as a `RepairPrepare` frame:
    /// the command byte is rewritten on an owned copy, because a verbatim
    /// `Prepare` would hit the live view fence on the receiver while
    /// `RepairPrepare` routes to the fence-free repair ingest.
    /// Returns whether the frame was handed to the bus: `send_to_replica`
    /// is a non-blocking try-send, so under a queue-full burst op N can be
    /// dropped while N+1 lands. Callers must not advance their
    /// served-through watermark past a failed send, or the terminating
    /// `RepairDone` reports ops that were never delivered.
    #[allow(clippy::future_not_send)]
    async fn send_repair_prepare(&self, target: u8, entry: Frozen<MESSAGE_ALIGN>) -> bool
    where
        B: MessageBus,
    {
        const COMMAND_OFFSET: usize = std::mem::offset_of!(GenericHeader, command);
        let mut owned =
            server_common::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(entry.as_slice());
        owned.as_mut_slice()[COMMAND_OFFSET] = Command2::RepairPrepare as u8;
        let Ok(message) = Message::<GenericHeader>::try_from(owned) else {
            tracing::warn!(
                shard = self.id,
                "repair prepare bytes failed message framing"
            );
            return false;
        };
        self.bus
            .send_to_replica(target, message.into_frozen())
            .await
            .is_ok()
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_repair_range_reply(
        &self,
        cluster: u128,
        self_id: u8,
        target: u8,
        command: Command2,
        nonce: u128,
        op: u64,
        namespace: u64,
    ) where
        B: MessageBus,
    {
        let msg = Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
            .transmute_header(|_, h: &mut RepairRangeReplyHeader| {
                h.command = command;
                h.cluster = cluster;
                h.replica = self_id;
                h.nonce = nonce;
                h.op = op;
                h.namespace = namespace;
                h.size = size_of::<RepairRangeReplyHeader>() as u32;
            });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    /// Start metadata tail journal-repair from `peer` when the commit walk
    /// gap-stopped below the known frontier. Shared by `StartView` adoption
    /// and the post-install step of a state transfer.
    #[allow(clippy::future_not_send)]
    async fn maybe_request_metadata_repair<P>(&self, consensus: &VsrConsensus<B, P>, peer: u8)
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        if consensus.is_normal()
            && !consensus.is_transferring()
            && consensus.commit_min() < consensus.commit_max()
            && self.metadata_repair.borrow().is_none()
        {
            let nonce = iggy_common::random_id::get_uuid();
            let to_op = consensus.commit_max();
            let from_op = consensus.commit_min() + 1;
            *self.metadata_repair.borrow_mut() = Some(MetadataRepairSession {
                nonce,
                to_op,
                peer,
                idle_ticks: 0,
            });
            tracing::info!(
                shard = self.id,
                from_op,
                to_op,
                "metadata behind the group frontier; requesting repair"
            );
            self.send_request_prepares(
                consensus.cluster(),
                consensus.replica(),
                peer,
                nonce,
                from_op,
                to_op,
                consensus.namespace(),
            )
            .await;
        }
    }

    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn send_request_state_transfer<P>(
        &self,
        consensus: &VsrConsensus<B, P>,
        target: u8,
        nonce: u128,
    ) where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        let msg =
            Message::<RequestStateTransferHeader>::new(size_of::<RequestStateTransferHeader>())
                .transmute_header(|_, h: &mut RequestStateTransferHeader| {
                    h.command = Command2::RequestStateTransfer;
                    h.cluster = consensus.cluster();
                    h.replica = consensus.replica();
                    h.nonce = nonce;
                    h.namespace = consensus.namespace();
                    h.size = size_of::<RequestStateTransferHeader>() as u32;
                });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    /// Answer a `RequestStateTransfer`: `offer = None` sends a header-only
    /// `available = 0` (the requester falls back to journal repair or
    /// retries elsewhere); an offer ships its encoded state manifest as the
    /// frame body.
    #[allow(
        clippy::future_not_send,
        clippy::cast_possible_truncation,
        clippy::too_many_arguments
    )]
    async fn send_state_transfer_target(
        &self,
        cluster: u128,
        self_id: u8,
        target: u8,
        nonce: u128,
        namespace: u64,
        offer: Option<&metadata::StateTransferOffer>,
    ) where
        B: MessageBus,
    {
        let manifest = offer.map(|offer| consensus::encode_state_manifest(&offer.manifest()));
        let total_size =
            size_of::<StateTransferTargetHeader>() + manifest.as_ref().map_or(0, Vec::len);
        let mut msg = Message::<StateTransferTargetHeader>::new(total_size);
        if let Some(manifest) = &manifest {
            msg.as_mut_slice()[size_of::<StateTransferTargetHeader>()..].copy_from_slice(manifest);
        }
        let msg = msg.transmute_header(|_, h: &mut StateTransferTargetHeader| {
            h.command = Command2::StateTransferTarget;
            h.cluster = cluster;
            h.replica = self_id;
            h.nonce = nonce;
            h.namespace = namespace;
            h.size = total_size as u32;
            if let Some(offer) = offer {
                h.available = 1;
                h.commit_op = offer.commit_op;
            }
        });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    #[allow(
        clippy::future_not_send,
        clippy::cast_possible_truncation,
        clippy::too_many_arguments
    )]
    async fn send_request_state_chunk(
        &self,
        cluster: u128,
        self_id: u8,
        target: u8,
        nonce: u128,
        namespace: u64,
        artifact: u32,
        offset: u64,
        len: u32,
    ) where
        B: MessageBus,
    {
        let msg = Message::<RequestStateChunkHeader>::new(size_of::<RequestStateChunkHeader>())
            .transmute_header(|_, h: &mut RequestStateChunkHeader| {
                h.command = Command2::RequestStateChunk;
                h.cluster = cluster;
                h.replica = self_id;
                h.nonce = nonce;
                h.namespace = namespace;
                h.artifact = artifact;
                h.offset = offset;
                h.len = len;
                h.size = size_of::<RequestStateChunkHeader>() as u32;
            });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    /// Serve one `RequestStateTransfer`: build a fresh offer (or refuse),
    /// cache it for the chunk pulls, and answer with the descriptor.
    #[allow(clippy::future_not_send)]
    async fn on_request_state_transfer(&self, msg: &Message<RequestStateTransferHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        if consensus.namespace() != header.namespace {
            return;
        }
        let cluster = consensus.cluster();
        let self_id = consensus.replica();

        // First-wins per (requester, nonce). A stall-retry `RequestStateTransfer`
        // reuses the session nonce, and rebuilding under it would replace a
        // manifest the receiver may already have accepted: the client table is
        // encoded live, so a rebuild that is SHORTER (a client logged out between
        // the two builds) lands the receiver's cursor exactly at the new length
        // and it re-requests an empty tail forever. Re-answering with the SAME
        // offer is also what makes the retry idempotent.
        let cached = self
            .metadata_transfer_offers
            .borrow_mut()
            .get_mut(&header.replica)
            .filter(|served| served.nonce == header.nonce)
            .map(|served| {
                // A descriptor retry proves the requester is alive and still
                // wants THIS offer, so it counts as liveness: without the reset
                // the offer could age out mid-retry and the rebuild that
                // replaced it is exactly what first-wins exists to prevent.
                served.idle_ticks = 0;
                Rc::clone(&served.offer)
            });
        if let Some(offer) = cached {
            tracing::debug!(
                shard = self.id,
                requester = header.replica,
                "re-answering a state transfer request from the offer already served"
            );
            self.send_state_transfer_target(
                cluster,
                self_id,
                header.replica,
                header.nonce,
                header.namespace,
                Some(&offer),
            )
            .await;
            return;
        }

        match planes.0.state_transfer_offer() {
            Ok(offer) => {
                tracing::info!(
                    shard = self.id,
                    requester = header.replica,
                    commit_op = offer.commit_op,
                    snapshot_seq = offer.snapshot_seq,
                    artifacts = offer.len(),
                    total_len = offer.total_len(),
                    "serving metadata state transfer"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.namespace,
                    Some(&offer),
                )
                .await;
                self.metadata_transfer_offers.borrow_mut().insert(
                    header.replica,
                    ServedStateTransfer {
                        nonce: header.nonce,
                        offer,
                        idle_ticks: 0,
                        fully_served: false,
                    },
                );
            }
            Err(reason) => {
                // Log the ACTUAL reason: "no snapshot yet" is routine and the
                // requester recovers through journal repair, while an unreadable
                // or corrupt `snapshot.bin` is an operator-visible fault on THIS
                // node that the old catch-all message actively misattributed.
                tracing::info!(
                    shard = self.id,
                    requester = header.replica,
                    %reason,
                    "cannot serve metadata state transfer; requester falls back"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.namespace,
                    None,
                )
                .await;
            }
        }
    }

    /// Receiver side of the descriptor: accept it and start pulling chunks,
    /// or fall back to journal repair when the peer cannot serve.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_state_transfer_target(&self, msg: &Message<StateTransferTargetHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: RestorableMetadataStm,
    {
        /// Alloc cap per artifact: a corrupt length field must not OOM the
        /// shard. Far above any real metadata snapshot or client table.
        const ARTIFACT_LEN_MAX: u64 = 1 << 30;

        /// Alloc cap across the WHOLE manifest. The per-artifact cap alone does
        /// not bound the total: `STATE_MANIFEST_ENTRIES_MAX` allows 65k entries,
        /// and the buffers below are reserved eagerly, so per-artifact limits
        /// would still admit a 64 TiB reservation and abort the process. The
        /// manifest checksum only proves it arrived intact, not that the peer
        /// computed it sanely, so bound what this side is willing to reserve.
        const MANIFEST_TOTAL_LEN_MAX: u64 = 4 << 30;

        let header = *msg.header();
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        if consensus.namespace() != header.namespace {
            return;
        }
        let session_matches = self
            .metadata_transfer
            .borrow()
            .as_ref()
            .is_some_and(|session| session.nonce == header.nonce);
        if !session_matches {
            return;
        }

        if header.available == 0 {
            // The peer cannot serve. If we have never installed anything the
            // local recovery stands; run the deferred commit walk and let
            // journal repair cover the gap (a peer that never checkpointed
            // retains its full WAL, so repair CAN cover it).
            tracing::info!(
                shard = self.id,
                peer = header.replica,
                "state transfer unavailable; falling back to journal repair"
            );
            *self.metadata_transfer.borrow_mut() = None;
            if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
                consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
            }
            planes.0.commit_journal().await;
            self.maybe_request_metadata_repair(consensus, header.replica)
                .await;
            return;
        }

        // The manifest rides the body; a well-formed available=1 descriptor
        // always carries one (an empty manifest still encodes its envelope).
        let manifest = match consensus::decode_state_manifest(
            &msg.as_slice()[size_of::<StateTransferTargetHeader>()..header.size as usize],
        ) {
            Ok(manifest) => manifest,
            Err(error) => {
                tracing::error!(
                    shard = self.id,
                    peer = header.replica,
                    %error,
                    "state transfer descriptor manifest undecodable; ignoring"
                );
                return;
            }
        };
        if let Some(oversized) = manifest.iter().find(|entry| entry.len > ARTIFACT_LEN_MAX) {
            tracing::error!(
                shard = self.id,
                kind = oversized.kind,
                len = oversized.len,
                "state transfer descriptor exceeds artifact cap; ignoring"
            );
            return;
        }
        let declared_total = manifest
            .iter()
            .fold(0u64, |total, entry| total.saturating_add(entry.len));
        if declared_total > MANIFEST_TOTAL_LEN_MAX {
            tracing::error!(
                shard = self.id,
                peer = header.replica,
                artifacts = manifest.len(),
                declared_total,
                "state transfer descriptor exceeds the total manifest cap; ignoring"
            );
            return;
        }
        // The snapshot artifact's frontier is the generation the decode budget
        // is keyed on; a manifest without one cannot install on this plane
        // anyway. Left armed, the session falls to the stall sweep.
        let Some(generation) = manifest
            .iter()
            .find(|entry| entry.kind == consensus::artifact_kind::METADATA_SNAPSHOT)
            .map(|entry| entry.frontier)
        else {
            tracing::error!(
                shard = self.id,
                peer = header.replica,
                "state transfer descriptor carries no metadata snapshot artifact; ignoring"
            );
            return;
        };
        if self.decode_budget_exhausted(generation) {
            // Pulling this generation again cannot end differently; refuse the
            // descriptor so the failure costs one frame per stall round, not a
            // full snapshot. Only a plain return: dropping the session here
            // and re-requesting repair would loop repair -> `RangeEvicted` ->
            // re-arm -> refuse at network rate, while the armed session is
            // paced by the stall sweep. The budget resets the moment the peer
            // offers a new generation.
            tracing::error!(
                shard = self.id,
                peer = header.replica,
                snapshot_seq = generation,
                "state transfer generation kept failing to decode; refusing it \
                 until the peer checkpoints a new one"
            );
            return;
        }

        {
            let mut session = self.metadata_transfer.borrow_mut();
            let Some(session) = session.as_mut() else {
                return;
            };
            if session.target_accepted {
                // Duplicate descriptor (stall retry crossed the original).
                return;
            }
            session.target_accepted = true;
            session.commit_op = header.commit_op;
            // Under ARTIFACT_LEN_MAX (checked above), so the casts hold.
            #[allow(clippy::cast_possible_truncation)]
            {
                session.artifacts = manifest
                    .iter()
                    .map(|&entry| ArtifactProgress {
                        entry,
                        buf: Vec::with_capacity(entry.len as usize),
                    })
                    .collect();
            }
            session.idle_ticks = 0;
        }
        if consensus.state_transfer_stage() == consensus::StateTransferStage::AwaitingTarget {
            consensus.set_state_transfer_stage(consensus::StateTransferStage::Fetching);
        }
        tracing::info!(
            shard = self.id,
            peer = header.replica,
            artifacts = manifest.len(),
            total_len = manifest.iter().map(|entry| entry.len).sum::<u64>(),
            commit_op = header.commit_op,
            "state transfer target accepted; fetching"
        );
        self.on_transfer_progress().await;
    }

    /// Ask for the next missing chunk of the in-flight transfer (artifacts
    /// pulled in manifest order). No-op when nothing is missing or no
    /// manifest is accepted yet; also the stall-retry re-request.
    #[allow(clippy::future_not_send)]
    async fn request_pending_state_chunk(&self)
    where
        B: MessageBus,
    {
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        // Same clamp the serving side applies, so a bus ceiling below
        // `STATE_CHUNK_LEN` shrinks the ask instead of leaving the server to
        // silently serve less than was requested.
        let chunk_len_max = self.state_chunk_len_max() as u64;
        let request = {
            let session = self.metadata_transfer.borrow();
            session.as_ref().and_then(|session| {
                if !session.target_accepted {
                    return None;
                }
                let (index, artifact) = session
                    .artifacts
                    .iter()
                    .enumerate()
                    .find(|(_, artifact)| !artifact.complete())?;
                let offset = artifact.buf.len() as u64;
                let remaining = artifact.entry.len - offset;
                #[allow(clippy::cast_possible_truncation)]
                let len = remaining.min(chunk_len_max) as u32;
                #[allow(clippy::cast_possible_truncation)]
                Some((session.nonce, session.peer, index as u32, offset, len))
            })
        };
        if let Some((nonce, peer, artifact, offset, len)) = request {
            self.send_request_state_chunk(
                consensus.cluster(),
                consensus.replica(),
                peer,
                nonce,
                consensus.namespace(),
                artifact,
                offset,
                len,
            )
            .await;
        }
    }

    /// Arm a fresh metadata transfer session against `peer` and request its
    /// descriptor.
    ///
    /// Every arming site goes through here. Three near-identical session
    /// literals had already drifted on the retry budget, which is why that
    /// budget now lives on the shard ([`Self::metadata_transfer_attempts`])
    /// instead of being re-minted with each session.
    #[allow(clippy::future_not_send)]
    async fn arm_metadata_transfer<P>(&self, consensus: &VsrConsensus<B, P>, peer: u8)
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        let nonce = iggy_common::random_id::get_uuid();
        *self.metadata_transfer.borrow_mut() = Some(MetadataTransferSession {
            nonce,
            peer,
            commit_op: 0,
            artifacts: Vec::new(),
            target_accepted: false,
            idle_ticks: 0,
        });
        self.send_request_state_transfer(consensus, peer, nonce)
            .await;
    }

    /// Largest state-chunk PAYLOAD this side will put on the wire.
    ///
    /// Clamped so header + payload stays inside the bus ceiling. Above it the
    /// RECEIVING transport rejects the frame and tears down the entire replica
    /// connection, which surfaces to an operator as an unexplained link flap.
    /// Both ends derive their chunk size from this same function, so a bus cap
    /// below [`STATE_CHUNK_LEN`] shrinks the chunk rather than making large
    /// artifacts untransferable.
    fn state_chunk_len_max(&self) -> usize {
        let budget = self
            .bus_max_message_size
            .get()
            .saturating_sub(size_of::<StateChunkHeader>());
        // A bus cap at or below one header cannot carry a chunk at all. Serve
        // one byte at a time rather than zero: a zero-length chunk is the
        // livelock `on_request_state_chunk` refuses, and the boot validator
        // rejects this configuration anyway.
        budget.clamp(1, STATE_CHUNK_LEN as usize)
    }

    /// Burn one retry round; `true` once the budget is exhausted.
    fn burn_metadata_transfer_attempt(&self) -> bool {
        let attempts = self.metadata_transfer_attempts.get() + 1;
        self.metadata_transfer_attempts.set(attempts);
        attempts > STATE_TRANSFER_MAX_STALL_RETRIES
    }

    /// Real progress: reset the retry budget.
    ///
    /// The budget bounds CONSECUTIVE failures, not lifetime ones. Without this
    /// five stalls scattered across a large transfer would abandon one that was
    /// nearly done, throwing away every byte already pulled.
    fn note_metadata_transfer_progress(&self) {
        self.metadata_transfer_attempts.set(0);
    }

    /// Charge one decode failure against `snapshot_seq`'s generation; `true`
    /// once that generation's budget is spent. A different generation restarts
    /// the count: the peer checkpointed since, so the artifacts are new bytes
    /// worth full retries.
    fn burn_decode_failure(&self, snapshot_seq: u64) -> bool {
        let failures = match self.metadata_transfer_decode_failures.get() {
            Some((seq, failures)) if seq == snapshot_seq => failures + 1,
            _ => 1,
        };
        self.metadata_transfer_decode_failures
            .set(Some((snapshot_seq, failures)));
        failures > STATE_TRANSFER_MAX_DECODE_RETRIES
    }

    /// Whether `snapshot_seq`'s generation already spent its decode budget.
    /// Gates descriptor acceptance, so an exhausted generation costs one
    /// refused descriptor per repair round instead of a full pull.
    const fn decode_budget_exhausted(&self, snapshot_seq: u64) -> bool {
        matches!(
            self.metadata_transfer_decode_failures.get(),
            Some((seq, failures))
                if seq == snapshot_seq && failures > STATE_TRANSFER_MAX_DECODE_RETRIES
        )
    }

    /// Serve one chunk out of the cached offer. An unknown nonce (offer
    /// evicted, e.g. the serving process restarted) answers with an
    /// `available = 0` descriptor so the requester restarts its session.
    #[allow(clippy::future_not_send, clippy::cast_possible_truncation)]
    async fn on_request_state_chunk(&self, msg: &Message<RequestStateChunkHeader>)
    where
        B: MessageBus,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        if consensus.namespace() != header.namespace {
            return;
        }
        let cluster = consensus.cluster();
        let self_id = consensus.replica();

        // Never serve a frame the receiving transport will reject: anything past
        // `max_message_size` tears down the whole replica connection, which reads
        // as an unexplained link flap. Bounded by the requester's own ask, this
        // side's chunk size, and what the bus will carry.
        let chunk_len_max = self.state_chunk_len_max();

        // Frame built inside the borrow; every send runs after it drops (a
        // RefCell borrow must not cross an await on the shard).
        // Out-of-bounds requests are dropped silently inside the block.
        let reply = {
            let mut offers = self.metadata_transfer_offers.borrow_mut();
            let served = offers
                .get_mut(&header.replica)
                .filter(|served| served.nonce == header.nonce);
            served.map_or(Some(ChunkReply::UnknownOffer), |served| {
                // Manifest-index addressing: an index past the offer is a
                // requester bug (or a stale frame) and is dropped below.
                let last_artifact = served.offer.len().saturating_sub(1);
                let artifact_bytes = served.offer.payload(header.artifact as usize)?;
                let start = header.offset as usize;
                // A request AT the end of an artifact has nothing left to serve.
                // Answering it with `Some(&[])` -- which `get(len..len)` happily
                // returns -- would extend nothing on the receiver, reset both
                // sides' idle counters, and be re-requested at the same offset
                // forever: an unbounded empty-frame ping-pong with the rejoining
                // replica withholding `PrepareOk` for the life of the process.
                // Reachable when a rebuilt offer is SHORTER than the manifest the
                // receiver accepted (a client logged out between the two builds).
                if start >= artifact_bytes.len() {
                    return None;
                }
                let end = start
                    .saturating_add((header.len as usize).min(chunk_len_max))
                    .min(artifact_bytes.len());
                let payload = artifact_bytes.get(start..end)?;
                // Only now that bytes are actually going out: an out-of-bounds or
                // stale frame must not flip a live offer onto the short expiry.
                // Tail of the final artifact means the receiver holds everything
                // the manifest promised, so the offer only has to outlive a
                // possible re-request of this very chunk.
                if header.artifact as usize == last_artifact && end >= artifact_bytes.len() {
                    served.fully_served = true;
                }
                // Serving a chunk is the only liveness signal the offer gets;
                // the expiry sweep drops it once these stop arriving. Set here
                // rather than on entry so a request that serves NOTHING cannot
                // keep an abandoned offer alive.
                served.idle_ticks = 0;
                let total_size = size_of::<StateChunkHeader>() + payload.len();
                let mut chunk = Message::<StateChunkHeader>::new(total_size);
                chunk.as_mut_slice()[size_of::<StateChunkHeader>()..].copy_from_slice(payload);
                Some(ChunkReply::Chunk(chunk.transmute_header(
                    |_, h: &mut StateChunkHeader| {
                        h.command = Command2::StateChunk;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.nonce = header.nonce;
                        h.namespace = header.namespace;
                        h.artifact = header.artifact;
                        h.offset = header.offset;
                        h.size = total_size as u32;
                    },
                )))
            })
        };
        match reply {
            Some(ChunkReply::Chunk(chunk)) => {
                let _ = self
                    .bus
                    .send_to_replica(header.replica, chunk.into_generic().into_frozen())
                    .await;
            }
            Some(ChunkReply::UnknownOffer) => {
                tracing::info!(
                    shard = self.id,
                    requester = header.replica,
                    "state chunk request for an unknown offer; telling requester to restart"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.namespace,
                    None,
                )
                .await;
            }
            None => {
                tracing::warn!(
                    shard = self.id,
                    requester = header.replica,
                    artifact = header.artifact,
                    offset = header.offset,
                    "state chunk request out of artifact bounds; ignoring"
                );
            }
        }
    }

    /// Receive one chunk; on the last one, verify + install + hand the tail
    /// to journal repair.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_state_chunk(&self, msg: &Message<StateChunkHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: RestorableMetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        if consensus.namespace() != header.namespace {
            return;
        }

        {
            let mut session = self.metadata_transfer.borrow_mut();
            let Some(session) = session.as_mut() else {
                return;
            };
            if session.nonce != header.nonce || !session.target_accepted {
                return;
            }
            let Some(artifact) = session.artifacts.get_mut(header.artifact as usize) else {
                return;
            };
            let payload = &msg.as_slice()[size_of::<StateChunkHeader>()..header.size as usize];
            // Chunks are pulled sequentially with one in flight; anything
            // else is a duplicate or reorder and is dropped (the stall retry
            // re-requests from the current frontier).
            if header.offset != artifact.buf.len() as u64 {
                return;
            }
            if artifact.buf.len() as u64 + payload.len() as u64 > artifact.entry.len {
                tracing::warn!(
                    shard = self.id,
                    artifact = header.artifact,
                    "state chunk overruns the declared artifact length; dropping frame"
                );
                return;
            }
            // A zero-byte payload is not progress: it extends nothing and the
            // same offset is re-requested immediately. Resetting the liveness
            // counters on one is what turned a short rebuilt offer into an
            // unbounded empty-frame ping-pong. The serving side refuses to
            // produce these now; the guard stays because a peer running an
            // older build still can.
            if payload.is_empty() {
                return;
            }
            artifact.buf.extend_from_slice(payload);
            session.idle_ticks = 0;
        }
        self.note_metadata_transfer_progress();
        self.on_transfer_progress().await;
    }

    /// Drive the in-flight transfer forward: request the next missing chunk,
    /// or - once every artifact is complete - verify, decode, and install.
    /// Shared by descriptor acceptance and chunk arrival, so a manifest whose
    /// artifacts are already complete (all empty) installs without waiting
    /// for a chunk that will never come.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_transfer_progress(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: RestorableMetadataStm,
    {
        let planes = self.plane.inner();
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        // The stage is the authority on whether this transfer is still wanted,
        // and it can be cleared from OUTSIDE this file: the probe-exhausted
        // election fallback lives in the consensus crate, which cannot reach
        // `metadata_transfer`, so it drops the stage to `Idle` (legal from
        // `Fetching`, hence silent) and leaves the session armed with its nonce
        // intact. Chunks then keep arriving, the pull completes, and the
        // `Installing` transition below asserts on an `Idle -> Installing` edge
        // that takes down shard 0. Drop the abandoned session here instead --
        // this is the single funnel both descriptor acceptance and chunk
        // arrival pass through.
        let stage = consensus.state_transfer_stage();
        if stage != consensus::StateTransferStage::Fetching {
            if self.metadata_transfer.borrow().is_some() {
                tracing::info!(
                    shard = self.id,
                    ?stage,
                    "metadata state transfer was abandoned out from under its session; \
                     dropping it"
                );
                *self.metadata_transfer.borrow_mut() = None;
            }
            return;
        }
        let complete = {
            let session = self.metadata_transfer.borrow();
            match session.as_ref() {
                Some(session) if session.target_accepted => {
                    session.artifacts.iter().all(ArtifactProgress::complete)
                }
                _ => return,
            }
        };
        if !complete {
            self.request_pending_state_chunk().await;
            return;
        }

        // All bytes in: verify, decode, install.
        let session = self
            .metadata_transfer
            .borrow_mut()
            .take()
            .expect("session checked above");
        let peer = session.peer;
        let commit_op = session.commit_op;

        // Per-artifact integrity, then pick the pieces this plane installs.
        // Unknown kinds are refused rather than skipped: an artifact the
        // serving peer thought worth shipping but this receiver cannot
        // install would otherwise be silently dropped.
        let mut snapshot: Option<Vec<u8>> = None;
        let mut table: Option<(Vec<u8>, u64)> = None;
        // Captured before the integrity checks so a damaged pull still knows
        // which generation to charge the decode budget against.
        let mut generation: Option<u64> = None;
        let mut damaged = false;
        for (index, artifact) in session.artifacts.into_iter().enumerate() {
            if artifact.entry.kind == consensus::artifact_kind::METADATA_SNAPSHOT {
                generation = Some(artifact.entry.frontier);
            }
            let actual = consensus::state_artifact_checksum(&artifact.buf);
            if actual != artifact.entry.checksum {
                tracing::error!(
                    shard = self.id,
                    artifact = index,
                    kind = artifact.entry.kind,
                    "state transfer artifact checksum mismatch"
                );
                damaged = true;
                break;
            }
            match artifact.entry.kind {
                consensus::artifact_kind::METADATA_SNAPSHOT => snapshot = Some(artifact.buf),
                consensus::artifact_kind::CLIENT_TABLE => {
                    table = Some((artifact.buf, artifact.entry.frontier));
                }
                kind => {
                    tracing::error!(
                        shard = self.id,
                        kind,
                        "state transfer manifest carries a kind this plane cannot install"
                    );
                    damaged = true;
                    break;
                }
            }
        }

        let decoded = if damaged {
            None
        } else if let (Some(snapshot), Some((table_bytes, table_frontier))) = (snapshot, table) {
            // The live table's capacity is only the floor: `decode` grows to
            // the received entry count (bounded by the slot ceiling), because
            // the serving primary can legitimately hold more sessions than
            // this node's cap and a cold-boot receiver sits at exactly the raw
            // config value -- rejecting on the local figure made a join under
            // cap reduction fail deterministically.
            let capacity = planes.0.client_table_capacity();
            match consensus::ClientTable::decode(&table_bytes, capacity) {
                Ok(table) => Some((snapshot, table, table_frontier)),
                Err(error) => {
                    tracing::error!(
                        shard = self.id,
                        capacity,
                        %error,
                        "transferred client table undecodable"
                    );
                    None
                }
            }
        } else {
            tracing::error!(
                shard = self.id,
                "state transfer manifest is missing the snapshot or client table artifact"
            );
            None
        };

        let Some((snapshot, table, table_frontier)) = decoded else {
            // Damage is usually transit corruption, which a re-fetch fixes. But
            // it can also be permanent -- a peer whose artifacts this build
            // cannot decode, or an unknown artifact kind -- and that re-offers
            // identically every round. The stall sweep can never bound this
            // path (frames ARE flowing, so `idle_ticks` never accumulates, and
            // every accepted chunk legitimately resets that budget), so decode
            // failures are charged per snapshot generation instead: a
            // generation past its budget is refused at descriptor time until
            // the peer checkpoints a new one.
            let exhausted =
                generation.is_some_and(|generation| self.burn_decode_failure(generation));
            if exhausted {
                tracing::warn!(
                    shard = self.id,
                    peer,
                    snapshot_seq = generation,
                    "state transfer artifacts kept failing to decode; abandoning \
                     and falling back to journal repair"
                );
                if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
                    consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
                }
                planes.0.commit_journal().await;
                self.maybe_request_metadata_repair(consensus, peer).await;
                return;
            }
            // Restart the session from scratch against the same peer (fresh
            // nonce; the peer re-offers).
            if consensus.state_transfer_stage() == consensus::StateTransferStage::Fetching {
                consensus.set_state_transfer_stage(consensus::StateTransferStage::AwaitingTarget);
            }
            self.arm_metadata_transfer(consensus, peer).await;
            return;
        };

        consensus.set_state_transfer_stage(consensus::StateTransferStage::Installing);
        match planes
            .0
            .install_state_transfer(&snapshot, table, table_frontier, commit_op)
            .await
        {
            Ok(outcome) => {
                consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
                // A completed install: both budgets start fresh for any later
                // rejoin rather than carrying this one's failures forward.
                self.note_metadata_transfer_progress();
                self.metadata_transfer_decode_failures.set(None);
                if outcome.pairing_durable {
                    // `applied_frontier`, not the transferred snapshot's op: the install
                    // returns `max(snapshot_seq, local_applied)`, which differs whenever a
                    // serving peer offers a snapshot BEHIND this replica (checkpoints are
                    // node-local) and the local state machine is kept instead.
                    tracing::info!(
                        shard = self.id,
                        applied_frontier = outcome.applied_frontier,
                        commit_op,
                        table_frontier,
                        "metadata state transfer installed; handing tail to journal repair"
                    );
                } else {
                    // Deliberately NOT prefixed with the success line's text:
                    // the specs match log substrings, so a shared prefix would
                    // let every one of them pass on the degraded path.
                    tracing::warn!(
                        shard = self.id,
                        applied_frontier = outcome.applied_frontier,
                        commit_op,
                        table_frontier,
                        "metadata state transfer landed WITHOUT a durable checkpoint \
                         pairing; the next superblock write records it"
                    );
                }
                // Walk whatever is already walkable, then let repair fetch
                // the (snapshot_seq, commit_max] tail.
                planes.0.commit_journal().await;
                self.maybe_request_metadata_repair(consensus, peer).await;
            }
            Err(error) => {
                tracing::error!(
                    shard = self.id,
                    %error,
                    "state transfer install failed; falling back to journal repair"
                );
                consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
                planes.0.commit_journal().await;
                self.maybe_request_metadata_repair(consensus, peer).await;
            }
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
        let repair_retry_ticks = self.repair_retry_ticks.get();
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
            // The tick emits view-scoped sends (heartbeats, view-change
            // retransmits), so it persists first like every dispatch site;
            // it is also what retries a persist an earlier site withheld on.
            if partition.persist_superblock_if_needed().await {
                dispatch_vsr_actions::<B, _, MJ>(consensus, None, &actions).await;
                dispatch_partition_journal_actions(consensus, partition, &actions).await;
            }

            // Stall retry: repair frames are fire-and-forget, so a lost
            // frame (or a peer that went silent mid-stream) would leave the
            // session armed forever with commit_min pinned below commit_max.
            // Re-request the remaining window from the serving peer; the
            // ingest path skips duplicates, so overlap is harmless.
            let stalled = {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                let consensus_normal = partition.consensus().is_normal();
                let commit_min = partition.consensus().commit_min();
                let cluster = partition.consensus().cluster();
                let self_id = partition.consensus().replica();
                partition.repair.as_mut().and_then(|session| {
                    if !consensus_normal {
                        return None;
                    }
                    session.idle_ticks += 1;
                    if session.idle_ticks < repair_retry_ticks {
                        return None;
                    }
                    session.idle_ticks = 0;
                    Some((
                        session.peer,
                        session.nonce,
                        commit_min + 1,
                        session.to_op,
                        cluster,
                        self_id,
                    ))
                })
            };
            if let Some((peer, nonce, from_op, to_op, cluster, self_id)) = stalled
                && from_op <= to_op
            {
                tracing::info!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    from_op,
                    to_op,
                    peer,
                    "partition repair stalled; re-requesting remaining window"
                );
                self.send_request_prepares(
                    cluster,
                    self_id,
                    peer,
                    nonce,
                    from_op,
                    to_op,
                    namespace.inner(),
                )
                .await;
            }
        }
    }

    /// Flush every owned partition's committed journal prefix to segment
    /// storage. Pump-shutdown counterpart of the commit-time persist gate:
    /// a graceful stop must not lose committed messages still resident in
    /// the in-memory journal (mirrors the legacy pump's final flush).
    #[allow(clippy::future_not_send)]
    pub async fn flush_partitions(&self)
    where
        B: MessageBus,
    {
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();
        tracing::info!(
            shard = self.id,
            partitions = namespaces.len(),
            "shutdown flush: draining committed journals to segment storage"
        );
        for namespace in namespaces {
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            if let Err(error) = partition
                .flush_committed_messages(partitions.config())
                .await
            {
                tracing::warn!(
                    namespace_raw = namespace.inner(),
                    %error,
                    "failed to flush partition journal on shutdown"
                );
            }
        }
    }

    /// Drop serving-side state-transfer offers that stopped being pulled.
    ///
    /// Each offer owns a whole snapshot plus the encoded client table, and the
    /// protocol has no completion frame (a receiver installs and goes quiet), so
    /// without this a primary that ever served a transfer pins that memory for
    /// the rest of the process. Generous relative to the chunk cadence: a live
    /// puller resets the counter on every chunk it fetches, so only an abandoned
    /// or finished transfer ages out.
    fn expire_idle_state_transfer_offers(&self) {
        // `max(1)`: the retry interval is operator-configurable, and a zero would
        // make the expiry zero, dropping every offer on the tick after it was
        // built and breaking transfers outright.
        let retry_ticks = self.repair_retry_ticks.get().max(1);
        let idle_expiry_ticks = retry_ticks.saturating_mul(STATE_TRANSFER_OFFER_EXPIRY_MULTIPLE);
        let served_expiry_ticks = retry_ticks.saturating_mul(STATE_TRANSFER_SERVED_EXPIRY_MULTIPLE);
        let mut offers = self.metadata_transfer_offers.borrow_mut();
        offers.retain(|requester, served| {
            served.idle_ticks += 1;
            // A fully-served offer only has to outlive a re-request of its last
            // chunk, so it goes on the short clock; anything else is an
            // abandoned transfer and waits out the full idle window.
            let expiry_ticks = if served.fully_served {
                served_expiry_ticks
            } else {
                idle_expiry_ticks
            };
            let live = served.idle_ticks < expiry_ticks;
            if !live {
                tracing::debug!(
                    shard = self.id,
                    requester,
                    fully_served = served.fully_served,
                    "dropping a state-transfer offer"
                );
            }
            live
        });
        // Nobody is pulling: release the cached snapshot copy too, rather than
        // pinning it for the life of the process.
        if offers.is_empty() {
            self.plane.metadata().clear_state_transfer_offer_cache();
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
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
    {
        let metadata = self.plane.metadata();
        let Some(ref consensus) = metadata.consensus else {
            return;
        };

        let actions = consensus.tick(PlaneKind::Metadata);

        if metadata.persist_superblock_if_needed(consensus).await {
            dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &actions).await;
        }

        // Repair a lost primary self-ack: `RetransmitPrepares` to self is a
        // no-op, so the timer-driven retransmit above cannot recover the
        // primary's own missing vote. Without this the commit prefix can pin
        // forever (commit_min stuck below commit_max). See
        // `IggyMetadata::repair_primary_self_acks`.
        metadata.repair_primary_self_acks().await;

        // Backstop for commit work stranded by a canceled `on_ack` driver
        // (a future dropped at its journal-read or wire-reply await): no
        // further ack re-drives an already-advanced `commit_max`, so on an
        // idle primary committed-but-unapplied ops and queued requests
        // would otherwise wait for unrelated traffic. Quiet no-op when
        // nothing is stranded.
        metadata.resume_stranded_commits().await;

        self.expire_idle_state_transfer_offers();

        // Stall retry for an in-flight state transfer: descriptor or chunk
        // frames are fire-and-forget, so a lost one must not wedge the
        // session (and the boot flow behind it) forever.
        let transfer_stalled = {
            let mut session = self.metadata_transfer.borrow_mut();
            session.as_mut().and_then(|session| {
                session.idle_ticks += 1;
                if session.idle_ticks < self.repair_retry_ticks.get() {
                    return None;
                }
                session.idle_ticks = 0;
                Some((session.peer, session.nonce, session.target_accepted))
            })
        };
        if let Some((peer, nonce, target_accepted)) = transfer_stalled {
            let exhausted = self.burn_metadata_transfer_attempt();
            let attempts = self.metadata_transfer_attempts.get();
            // Retrying the same peer forever is a wedge when that peer is the
            // thing that died: nothing in this loop re-selects a target. Give up
            // after a bounded number of rounds and fall back to journal repair,
            // which re-picks a peer and, if the gap is still below its retained
            // floor, answers `RangeEvicted` and arms a fresh transfer against
            // whoever is primary now.
            if exhausted {
                tracing::warn!(
                    shard = self.id,
                    peer,
                    attempts,
                    "metadata state transfer stalled past its retry budget; \
                     abandoning and falling back to journal repair"
                );
                *self.metadata_transfer.borrow_mut() = None;
                if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
                    consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
                }
                metadata.commit_journal().await;
                let current_primary = consensus.primary_index(consensus.view());
                self.maybe_request_metadata_repair(consensus, current_primary)
                    .await;
                return;
            }
            tracing::info!(
                shard = self.id,
                peer,
                target_accepted,
                attempts,
                "metadata state transfer stalled; re-requesting"
            );
            if target_accepted {
                self.request_pending_state_chunk().await;
            } else {
                self.send_request_state_transfer(consensus, peer, nonce)
                    .await;
            }
        }

        // Stall retry, mirroring `tick_partitions`: a lost repair frame must
        // not wedge the session forever.
        let repair_retry_ticks = self.repair_retry_ticks.get();
        let stalled = {
            let mut session = self.metadata_repair.borrow_mut();
            session.as_mut().and_then(|session| {
                if !consensus.is_normal() {
                    return None;
                }
                session.idle_ticks += 1;
                if session.idle_ticks < repair_retry_ticks {
                    return None;
                }
                session.idle_ticks = 0;
                Some((session.peer, session.nonce, session.to_op))
            })
        };
        if let Some((peer, nonce, to_op)) = stalled {
            let from_op = consensus.commit_min() + 1;
            if from_op <= to_op {
                tracing::info!(
                    shard = self.id,
                    from_op,
                    to_op,
                    peer,
                    "metadata repair stalled; re-requesting remaining window"
                );
                self.send_request_prepares(
                    consensus.cluster(),
                    consensus.replica(),
                    peer,
                    nonce,
                    from_op,
                    to_op,
                    consensus.namespace(),
                )
                .await;
            }
        }
    }
}

/// Broadcast a `StartView` for the current view, answering a replica that
/// still heartbeats an older view (see `CommitOutcome::RespondStartView`).
#[allow(clippy::future_not_send)]
async fn respond_start_view<B, P, J>(consensus: &VsrConsensus<B, P>)
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
    J: JournalHandle,
    <J as JournalHandle>::Target: Journal<
            <J as JournalHandle>::Storage,
            Entry = Message<PrepareHeader>,
            Header = PrepareHeader,
        >,
{
    tracing::info!(
        view = consensus.view(),
        op = consensus.sequencer().current_sequence(),
        commit = consensus.commit_max(),
        namespace = consensus.namespace(),
        "answering stale-view heartbeat with StartView"
    );
    // Unsolicited, answering a stale-view heartbeat rather than a probe, so there is
    // no incarnation to echo; freshness comes from the receiver's view checks. Sent to
    // every backup, since a replica heartbeating an older view has peers that missed
    // the view change with it.
    let action = VsrAction::SendStartView {
        view: consensus.view(),
        op: consensus.sequencer().current_sequence(),
        commit: consensus.commit_max(),
        incarnation: 0,
        target: None,
        namespace: consensus.namespace(),
    };
    dispatch_vsr_actions::<B, P, J>(consensus, None, &[action]).await;
}

/// Re-stamp a stored prepare with the current view before retransmission.
/// After a view change the primary re-sends its uncommitted suffix as its
/// own prepares (VSR), but the journal keeps the original view stamp and
/// `replicate_preflight` fences `header.view < view` as deposed-primary
/// traffic -- a verbatim replay of the stored bytes would be ignored
/// forever, wedging the commit walk on every peer. The stored buffer is
/// shared with the journal, so the patch runs on an owned copy.
fn restamp_prepare_view(stored: &[u8], view: u32) -> Option<Frozen<MESSAGE_ALIGN>> {
    const VIEW_OFFSET: usize = std::mem::offset_of!(PrepareHeader, view);
    let mut owned = server_common::iobuf::Owned::<MESSAGE_ALIGN>::copy_from_slice(stored);
    owned.as_mut_slice()[VIEW_OFFSET..VIEW_OFFSET + std::mem::size_of::<u32>()]
        .copy_from_slice(&view.to_ne_bytes());
    Message::<GenericHeader>::try_from(owned)
        .ok()
        .map(Message::into_frozen)
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

    // Centralized durable-before-send tripwire: a view-scoped message must never
    // advertise a (view, log_view) the superblock has not recorded, or a crash could
    // recover an older view than one a peer already saw, splitting the brain or
    // losing a commit. Every caller on BOTH planes persists first (the view-change
    // dispatch sites, the tick, and each plane's PrepareOk send gate), so this
    // asserts they did rather than letting a future bypass through silently.
    // `RequestStartView` is exempt, being a probe that asks to LEARN the view rather
    // than advertise it. Partitions without an attached superblock (in-memory,
    // simulated) pass vacuously: their persist gate records "durable = current"
    // instead of writing, precisely so this assert stays meaningful for the groups
    // that do have a store.
    #[cfg(debug_assertions)]
    for action in actions {
        let advertises_view = matches!(
            action,
            VsrAction::SendStartViewChange { .. }
                | VsrAction::SendDoViewChange { .. }
                | VsrAction::SendStartView { .. }
                | VsrAction::SendPrepareOk { .. }
                // A backup drops a Commit whose view differs from its own, and a
                // primary answers an older-view one with a StartView, so the
                // heartbeat advertises a view like the rest. Gated today only
                // because its sole emitter rides the tick, which persists first.
                | VsrAction::SendCommit { .. }
        );
        debug_assert!(
            !advertises_view || !consensus.needs_superblock_persist(),
            "durable-before-send violated: dispatching a view-scoped action for \
             namespace {} while the superblock is behind the in-memory view {}",
            consensus.namespace(),
            consensus.view(),
        );
    }

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
            VsrAction::SendRequestStartView { view, namespace } => {
                // Stamp this replica's incarnation so the answering StartView can
                // echo it, proving to us the reply post-dates our restart.
                let incarnation = consensus.incarnation();
                let msg =
                    Message::<RequestStartViewHeader>::new(size_of::<RequestStartViewHeader>())
                        .transmute_header(|_, h: &mut RequestStartViewHeader| {
                            h.command = Command2::RequestStartView;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.incarnation = incarnation;
                            h.namespace = *namespace;
                            h.size = size_of::<RequestStartViewHeader>() as u32;
                        });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendStartView {
                view,
                op,
                commit,
                incarnation,
                target,
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
                        h.incarnation = *incarnation;
                        h.namespace = *namespace;
                        h.size = size_of::<StartViewHeader>() as u32;
                    });
                let frozen = msg.into_generic().into_frozen();
                // A probe echo is addressed to its requester: the incarnation it
                // carries is that replica's freshness proof, and a peer recovering
                // at the same time would read it as foreign and reject a current
                // StartView.
                match target {
                    Some(replica) => send(*replica, frozen).await,
                    None => broadcast(frozen).await,
                }
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
                let current_view = consensus.view();
                for (header, replicas) in targets {
                    let Some(prepare) = journal.handle().entry(header).await else {
                        continue;
                    };
                    // Freeze the retransmit payload once; clone per target.
                    let frozen = if prepare.header().view == current_view {
                        prepare.into_generic().into_frozen()
                    } else {
                        let Some(restamped) =
                            restamp_prepare_view(prepare.as_slice(), current_view)
                        else {
                            continue;
                        };
                        restamped
                    };
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
                        // New-primary path: lift the monotonic timestamp
                        // floor to the rebuilt log so post-view-change
                        // prepares cannot stamp below committed ones.
                        consensus.observe_prepare_timestamp(header.timestamp);
                        let mut entry = consensus::PipelineEntry::new(*header);
                        entry.add_ack(self_id);
                        Some(entry)
                    })
                    .collect();
                if let Some(missing_op) = gap_at {
                    // A primary's own uncommitted suffix has no repair
                    // source: peers ack'd nothing above the gap or the DVC
                    // merge would have carried it, so the range is decided
                    // lost. Truncate the sequencer to the last op we could
                    // rebuild so the next client prepare chains correctly.
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
async fn dispatch_partition_journal_actions<B, P, SB>(
    consensus: &VsrConsensus<B, P>,
    partition: &IggyPartition<B, SB>,
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
                let current_view = consensus.view();
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
                    let prepare = if header.view == current_view {
                        prepare
                    } else {
                        let Some(restamped) =
                            restamp_prepare_view(prepare.as_slice(), current_view)
                        else {
                            continue;
                        };
                        restamped
                    };
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
                        // New-primary path: lift the monotonic timestamp
                        // floor to the rebuilt log so post-view-change
                        // prepares cannot stamp below committed ones.
                        consensus.observe_prepare_timestamp(header.timestamp);
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
