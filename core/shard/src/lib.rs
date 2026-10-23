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
pub mod host;
pub mod metrics;
mod partition_io;
pub use partition_io::{PartitionIoLimits, PartitionIoLimitsError};
mod poll;
mod primary_rebalance;
mod router;
pub mod shards_table;

pub use config::CoordinatorConfig;
pub use host::{NoopHost, ShardHost};
pub use poll::{ConsumerAttachment, PollCompleted};
pub use router::CONSENSUS_TICK_INTERVAL;

#[cfg(feature = "simulator")]
use consensus::LocalPipeline;
use consensus::{
    ChunkProgress, CommitOutcome, Consensus, ConsensusClock, DVC_HEADERS_MAX, DvcHeaderKind,
    DvcSuffix, FatalReason, MergedLog, MetadataHandle, MuxPlane, PartitionsHandle, Pipeline, Plane,
    PlaneKind, RepairEvidence, STATE_TRANSFER_MAX_DECODE_RETRIES, STATE_TRANSFER_MAX_STALL_RETRIES,
    Sequencer, StartViewRefusal, Status, VsrAction, VsrConsensus,
    build_deny_reply_from_request_header, dvc_blank, dvc_header_kind, encode_prepare_headers,
    fatal, repaired_frontier_update, repaired_prepare_evidence, restamp_prepare_view,
    verify_prepare_integrity,
};
#[cfg(any(test, feature = "simulator"))]
use crossfire::AsyncRxTrait;
use futures::FutureExt;
use iggy_binary_protocol::{
    CHECKSUM_UNSEALED, Command, CommitHeader, ConsensusHeader, ConsumerSession,
    ConsumerSessionHeartbeatHeader, DoViewChangeHeader, ForwardCompletionHeader,
    ForwardLogoutHeader, ForwardLogoutResultHeader, ForwardRegisterHeader,
    ForwardRegisterResultHeader, GenericHeader, Operation, PrepareHeader, PrepareOkHeader,
    RepairPrepareHeader, RepairRangeReplyHeader, RequestPreparesHeader, RequestStartViewHeader,
    RequestStateChunkHeader, RequestStateTransferHeader, RoutedRequestHeader, SegmentDeleteHeader,
    SegmentDeleteKind, StartViewChangeHeader, StartViewHeader, StateChunkHeader,
    StateTransferTargetHeader, WireDecode,
};
#[cfg(feature = "simulator")]
use iggy_common::PartitionStats;
use iggy_common::variadic;
use iggy_common::{ConsumerKind, EncryptorKind, IggyError, IggyExpiry, IggyTimestamp};
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use journal::{Journal, JournalHandle};
use message_bus::client_listener::RequestHandler;
use message_bus::fd_transfer::DupedFd;
use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
use message_bus::replica::listener::MessageHandler;
use message_bus::{BusMessage, ConnectionPermit, MessageBus, SharedTlsServerConfig};
use metadata::IggyMetadata;
use metadata::impls::metadata::{PartitionPlacementView, StreamsFrontend, metadata_wal_head};
use metadata::stm::StateMachine;
use metadata::{BoundSession, CommitWalkStop, MetadataSubmitError};
use partitions::state_transfer::TransferArtifact;
use partitions::{
    FatalCommit, IggyPartition, IggyPartitions, PollFragments, PollingArgs, PollingConsumer,
    RepairIngest,
};
use server_common::sharding::{
    IggyNamespace, LIST_CLIENTS_GATHER_TIMEOUT, PartitionLocation, ShardId,
};
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use shards_table::ShardsTable;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

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

/// Committed metadata the simulator carries into one partition
/// materialisation. Named because both values are `u64`-compatible revision or
/// view stamps and swapping positional arguments would compile.
#[cfg(feature = "simulator")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionMaterialisation {
    epoch: u64,
    created_view: u32,
    first_initialization: bool,
    consumer_offsets_max: usize,
}

#[cfg(feature = "simulator")]
impl PartitionMaterialisation {
    #[must_use]
    pub const fn new(epoch: u64, created_view: u32, first_initialization: bool) -> Self {
        Self {
            epoch,
            created_view,
            first_initialization,
            consumer_offsets_max: partitions::DEFAULT_CONSUMER_OFFSETS_MAX,
        }
    }

    #[must_use]
    pub const fn with_consumer_offsets_max(mut self, consumer_offsets_max: usize) -> Self {
        self.consumer_offsets_max = consumer_offsets_max;
        self
    }
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

struct PendingSegmentDelete {
    query: SegmentDeleteHeader,
    target: u8,
    reply: Sender<SegmentDeleteHeader>,
}

const MAX_SEGMENT_DELETE_FORWARDS: usize = 128;

/// Cross-shard metadata consensus submit.
///
/// The metadata consensus group lives only on shard 0. When a client
/// connection homes on a peer shard, that shard verifies credentials and
/// owns the session locally, but the consensus proposal (`Register` /
/// `Logout`) must execute on shard 0. The peer hands just that step here
/// and awaits the outcome over `reply`. `Register` carries the submit error
/// verbatim because one variant
/// (`MetadataSubmitError::ClientIdOwnedByAnotherUser`) is terminal and must
/// not be retried. The remaining variants are transient by contract, and
/// Logout preserves them so its caller can distinguish an unknown outcome
/// from a request that never entered the primary pipeline.
pub enum MetadataSubmit {
    ResolveSegmentDelete {
        namespace: IggyNamespace,
        count: u32,
        created_revision: u64,
        minimum_metadata_op: u64,
        reply: Sender<Result<u64, IggyError>>,
    },
    ForwardedSegmentDelete(Message<SegmentDeleteHeader>),
    /// Replica-link liveness report routed to shard 0; no proposal or reply.
    ConsumerSessionHeartbeat(Message<ConsumerSessionHeartbeatHeader>),
    BindSession {
        vsr_client_id: u128,
        session: u64,
        secret: iggy_binary_protocol::requests::users::login_register::BindSecret,
        reply: Sender<Result<(u32, u64, consensus::client_table::SessionAttachment), IggyError>>,
    },
    Register {
        vsr_client_id: u128,
        user_id: u32,
        registration: iggy_binary_protocol::requests::users::login_register::RegisterRequest,
        /// The committed bind, or the submit error verbatim. The error must
        /// survive the hop: the ownership refusal is TERMINAL, and flattening it
        /// into "no reply" makes the login look transient, which costs the
        /// client a retry storm of full password verifications.
        reply: Sender<Result<BoundSession, MetadataSubmitError>>,
    },
    /// A backup node authenticated a login and asks this node -- which it
    /// believes is the primary -- to run only the `Register` proposal. The
    /// verdict travels back over the replica interconnect as a
    /// `ForwardRegisterResult`, not over a channel: the awaiting login lives
    /// in another process.
    ///
    /// Handled by proposing IN PROCESS, never by forwarding again. That is
    /// what bounds a forward at one hop: a node that has since lost
    /// primaryship answers `NotPrimary`, and the client's SDK replays.
    ForwardedRegister {
        vsr_client_id: u128,
        user_id: u32,
        registration: iggy_binary_protocol::requests::users::login_register::RegisterRequest,
        /// Correlation the origin minted; echoed verbatim in the result.
        nonce: u128,
        /// Replica the result frame goes back to.
        origin_replica: u8,
    },
    /// A backup relays the committed evidence of a partition completion. Like
    /// [`Self::ForwardedRegister`], it is proposed in process and never
    /// relayed again.
    ForwardedCompletion(Message<ForwardCompletionHeader>),
    /// A backup owns a bound client connection and asks the metadata primary
    /// to commit its Logout. The result returns over the replica interconnect.
    ForwardedLogout {
        vsr_client_id: u128,
        session: u64,
        request: u64,
        nonce: u128,
        origin_replica: u8,
    },
    Logout {
        vsr_client_id: u128,
        session: u64,
        request: u64,
        reply: Sender<Result<u64, MetadataSubmitError>>,
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
    /// A shard's partition reconciler reports the committed partition fences
    /// of one lifecycle intent to shard 0, which proposes them as one batch
    /// through metadata consensus with no client session, or relays them to
    /// the primary on a backup. `reply` carries `true` when the reconciler
    /// reports again at once: the batch committed and applied, or shard 0
    /// could not commit it and its wait ended. That wait is one relay
    /// interval on a backup and after a view change refused or canceled the
    /// batch. A primary still catching up answers once it caught up, checked
    /// every consensus tick, and after one relay interval at the latest. A
    /// dropped frame drops `reply`, which reads as `false`. The reconciler
    /// keeps one batch in flight and resends while the intent stays pending.
    CompleteLifecycle {
        request: metadata::stm::lifecycle::CompleteLifecycleRequest,
        reporter: u16,
        reply: Sender<bool>,
    },
    /// A shard's partition reconciler asks shard 0 to complete a cooperative
    /// consumer-group revocation (the source drained the partition or it timed
    /// out). Server-originated: shard 0 proposes it through metadata consensus
    /// with no client session. Fire-and-forget + idempotent -- `reply` carries
    /// the commit op (or `None` on a transient submit failure) for logging only.
    CompleteRevocation {
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        installation: iggy_binary_protocol::requests::partitions::InstallConsumerGroupOwnerRequest,
        partition_op: u64,
        reply: Sender<Option<u64>>,
    },
}

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

#[derive(Debug)]
pub enum ListClientsReply {
    Clients(Sender<Vec<ConnectedClientInfo>>),
    Sessions(Sender<Vec<ConsumerSession>>),
    /// The number of connected clients only, for the readers that need no
    /// client details.
    Count(Sender<usize>),
}

/// Best-effort client list plus whether every shard answered.
pub struct GatheredClients<T = ConnectedClientInfo> {
    pub clients: Vec<T>,
    /// False if any shard rejected the request or failed to reply before the deadline.
    pub complete: bool,
}

/// A read executed on the shard that owns a partition: a message poll or a
/// consumer-offset lookup. Carried by [`LifecycleFrame::PartitionRead`];
/// see [`IggyShard::partition_read`].
#[derive(Debug)]
pub enum PartitionRead {
    Primary,
    SessionRetired {
        identity: iggy_binary_protocol::requests::system::SessionIdentity,
        created_revision: u64,
    },
    /// A bound poll. A group read is served only to the partition's installed
    /// owner.
    PollOnPrimary {
        consumer: PollingConsumer,
        args: PollingArgs,
        attachment: poll::ConsumerAttachment,
    },
    /// An HTTP poll with no bound session, served only by the primary.
    UnboundPollOnPrimary {
        consumer: PollingConsumer,
        args: PollingArgs,
        user_id: u32,
        user_created_at: u64,
        metadata: metadata::stm::stream::PollMetadata,
    },
    /// A read with no owner check. In production only plain consumers use it.
    /// Tests and the simulator also read groups through it.
    Poll {
        consumer: PollingConsumer,
        args: PollingArgs,
        metadata: Option<metadata::stm::stream::PollMetadata>,
    },
    ConsumerOffset {
        consumer: PollingConsumer,
    },
    ConsumerOffsetOnPrimary {
        /// None means the named group does not exist, so no offset can exist.
        consumer: Option<PollingConsumer>,
        metadata: metadata::stm::stream::PollMetadata,
    },
    /// An external group's stored offset. Not a `PollingConsumer`: the kind is never polled.
    ExternalGroupOffsetOnPrimary {
        group_id: u32,
        metadata: metadata::stm::stream::PollMetadata,
    },
    /// Cooperative-rebalance classification: the group's last-polled and
    /// committed offsets on this partition, so the join enrichment can tell an
    /// in-flight partition (committed < last-polled) from a never-polled/drained
    /// one. `group_id` is the monotonic consumer-group id (offset key).
    GroupOffsetState {
        group_id: u64,
    },
    /// Resolve a client `DeleteSegments` count into a concrete truncation
    /// offset: the `end_offset` of the `count`-th oldest sealed segment. Run on
    /// the owning shard, which alone holds the partition's segment state.
    ResolveSegmentDeleteOffset {
        count: u32,
    },
}

impl PartitionRead {
    pub(crate) const fn requires_primary(&self) -> bool {
        matches!(
            self,
            Self::PollOnPrimary { .. }
                | Self::UnboundPollOnPrimary { .. }
                | Self::ConsumerOffsetOnPrimary { .. }
                | Self::ExternalGroupOffsetOnPrimary { .. }
                | Self::ResolveSegmentDeleteOffset { .. }
        )
    }

    /// Whether the read returns polled messages or a stored consumer offset.
    #[must_use]
    pub(crate) const fn reads_messages_or_offset(&self) -> bool {
        match self {
            Self::ConsumerOffset { .. }
            | Self::ConsumerOffsetOnPrimary { .. }
            | Self::ExternalGroupOffsetOnPrimary { .. }
            | Self::PollOnPrimary { .. }
            | Self::UnboundPollOnPrimary { .. }
            | Self::Poll { .. } => true,
            Self::Primary
            | Self::SessionRetired { .. }
            | Self::GroupOffsetState { .. }
            | Self::ResolveSegmentDeleteOffset { .. } => false,
        }
    }
}

/// Reply to a [`PartitionRead`].
#[derive(Debug)]
pub enum PartitionReadReply {
    /// The partition group's view on this replica and the primary of that view.
    Primary {
        view: u32,
        primary: u8,
    },
    SessionRetired(bool),
    /// Permanent failure of this incarnation. It is not a retirement proof
    /// until the caller durably fences the incarnation against future serving.
    SessionRetirementFailed {
        created_revision: u64,
    },
    Poll {
        context: iggy_binary_protocol::primitives::partition_history::PartitionContext,
        fragments: PollFragments,
        current_offset: u64,
        /// The owning partition's encryptor, captured with the fragments.
        encryptor: Option<Arc<EncryptorKind>>,
    },
    ConsumerOffset {
        stored: Option<u64>,
        current_offset: u64,
    },
    /// The read was refused and returns no messages. A poll that needs a new
    /// consumer offset key beyond the configured limit returns
    /// `TooManyConsumerOffsets`. A completion whose history changed, whose owner
    /// inbox is unavailable, or whose automatic commit cannot be admitted returns
    /// `TransientNotAccepted`, allowing the client to retry. Reads also return
    /// `TransientNotAccepted` when submission fails before reaching the owner
    /// or while the partition requires state transfer. A primary also returns
    /// it to a poll or a read of a stored offset until it applied the view it
    /// started.
    /// A poll of an incarnation whose delete fence committed returns
    /// `HistoryUnavailable`. A primary read on a normal backup returns
    /// `NotPrimary`.
    Rejected(IggyError),
    /// Reply to [`PartitionRead::GroupOffsetState`]: the group's last-polled and
    /// committed offsets on this partition (each `None` if absent).
    GroupOffsetState {
        last_polled: Option<u64>,
        committed: Option<u64>,
    },
    /// Reply to [`PartitionRead::ResolveSegmentDeleteOffset`]: the resolved
    /// truncation offset, or `None` when the partition has no sealed segments
    /// to delete. `lagging` means the answer is transient: this replica is not
    /// a normal primary that applied the view it started, it installs a state
    /// transfer, or it has nothing sealed while commits above `commit_min`
    /// can still seal a segment. Those commits never change a sealed segment,
    /// so they do not make an offset transient. A committed-but-unflushed
    /// resident tail does NOT make the no-op transient.
    SegmentDeleteOffset {
        up_to_offset: Option<u64>,
        lagging: bool,
        created_revision: u64,
    },
    /// The owning shard has no materialised partition for the namespace
    /// (unknown, tombstoned, or mid-reconcile). Callers surface an error
    /// instead of an empty result.
    NotFound,
}

/// Reply budget for [`IggyShard::partition_read`]. Bounds a wedged owning shard;
/// the caller maps expiry to an error with unknown acceptance.
///
/// 10s, not lower: a disk poll over tiny segments opens one file per
/// segment, so a read of 1024 messages can legitimately take several seconds
/// on an oversubscribed host (8 parallel test clusters). Disk I/O continues
/// after expiry, so a short budget can waste completed reads. Acceptance racing
/// with expiry can still leave an unknown outcome. Must stay below the SDK's
/// 30s request deadline.
const PARTITION_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Budget for a partition write's wait on its committed reply. Longer than a
/// read: the wait spans replication quorum plus any park-and-promote the
/// request rides through, and a view change mid-flight re-proposes under the
/// new primary. Shorter than the SDK's operation deadline, so the unknown
/// outcome the relay answers on expiry still reaches a client that can replay
/// the request id within its deadline.
const PARTITION_SUBMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const _: () =
    assert!(PARTITION_SUBMIT_TIMEOUT.as_nanos() < iggy_common::BINARY_OPERATION_TIMEOUT.as_nanos());

/// A partition write admitted onto its owning shard's inbox, awaiting the
/// committed reply. Redeem with [`IggyShard::await_partition_submit`].
pub struct PartitionSubmitTicket {
    receiver: Receiver<Option<Message<GenericHeader>>>,
    target: u16,
}

/// The write never reached the owning shard.
///
/// No sender existed for the target, or its inbox refused the frame. Either
/// way the outcome is known, unlike a reply that fails to arrive, so the caller
/// may deny the client outright.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionSubmitRefused;

/// Race `future` against a bus timer.
///
/// `Some` if it finishes within `budget`, `None` if the timer fires first.
/// Uses [`MessageBus::sleep`] (virtual under the simulator, wall-clock in
/// production) rather than `compio::time::timeout`, which panics outside a
/// compio runtime and so cannot run under the deterministic executor.
#[allow(clippy::future_not_send)]
pub async fn bus_timeout<B, F>(bus: &B, budget: std::time::Duration, future: F) -> Option<F::Output>
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

/// Create the bounded inter-shard channel pair (main lane + reply lane)
/// whose sender is tagged with the owning shard.
///
/// Bootstrap uses this to build the per-shard sender `Vec` such that
/// `vec[i]` necessarily reaches shard `i`. The second receiver is the reply
/// lane: cross-shard client `Reply` forwards, whose drops are terminal, ride
/// a channel of their own so a consensus burst filling the main lane cannot
/// evict them (see `[sharding] reply_inbox_capacity`).
#[must_use]
pub fn shard_channel(
    owner_shard: u16,
    capacity: usize,
    reply_capacity: usize,
) -> (TaggedSender, Receiver<ShardFrame>, Receiver<ShardFrame>) {
    let (tx, rx) = channel::<ShardFrame>(capacity);
    let (reply_tx, reply_rx) = channel::<ShardFrame>(reply_capacity);
    (TaggedSender::new(owner_shard, tx, reply_tx), rx, reply_rx)
}

/// Build canonical-ordered `(senders, inboxes, reply_inboxes)` for an
/// N-shard mesh.
///
/// Each `inboxes[i]` / `reply_inboxes[i]` drains exclusively on the runtime
/// owning shard `i`. The returned `senders` Vec satisfies
/// `senders[i].shard_id() == i` by construction; clone it into every shard
/// before spawning so all shards share the same mesh.
///
/// Receivers are wrapped in `Option` because [`Receiver`] (crossfire
/// `AsyncRx`) is non-cloneable on purpose; bootstrap takes the slots for
/// shard `i` exactly once when spawning the owning thread.
#[must_use]
pub fn shard_mesh_channels(total_shards: u16, capacity: usize, reply_capacity: usize) -> ShardMesh {
    let mut senders = Vec::with_capacity(total_shards as usize);
    let mut inboxes = Vec::with_capacity(total_shards as usize);
    let mut reply_inboxes = Vec::with_capacity(total_shards as usize);
    for shard_id in 0..total_shards {
        let (tx, rx, reply_rx) = shard_channel(shard_id, capacity, reply_capacity);
        senders.push(tx);
        inboxes.push(Some(rx));
        reply_inboxes.push(Some(reply_rx));
    }
    (senders, inboxes, reply_inboxes)
}

/// The canonical N-shard mesh: lane senders plus the per-shard receivers
/// (`inboxes[i]` / `reply_inboxes[i]` drain on the runtime owning shard `i`).
pub type ShardMesh = (
    Vec<TaggedSender>,
    Vec<Option<Receiver<ShardFrame>>>,
    Vec<Option<Receiver<ShardFrame>>>,
);

/// The pair of lane [`Sender`]s annotated with the id of the shard whose
/// paired receivers they feed.
///
/// Inter-shard routing indexes `senders[i]` with `i == target_shard`. The
/// plain `Sender` form has no way to verify that invariant at runtime, so a
/// permuted `Vec<Sender<_>>` would silently misroute every setup, mapping,
/// and forward frame. Construct senders through [`shard_channel`] (or
/// [`TaggedSender::new`]) at the channel-creation site; the coordinator and
/// [`IggyShard`] ctors then validate `senders[i].shard_id() == i`,
/// returning [`ShardCtorError`] if violated.
///
/// `Deref` targets the main lane; [`Self::reply_sender`] exposes the reply
/// lane (cross-shard client `Reply` forwards, terminal on drop).
pub struct TaggedSender {
    shard_id: u16,
    inner: Sender<ShardFrame>,
    reply: Sender<ShardFrame>,
}

impl TaggedSender {
    /// Wrap already-constructed lane senders with the id of the shard whose
    /// paired receivers drain them. Prefer [`shard_channel`] unless existing
    /// senders are being re-tagged (e.g., tests that build senders manually
    /// and know the ordering is correct).
    #[must_use]
    pub const fn new(shard_id: u16, inner: Sender<ShardFrame>, reply: Sender<ShardFrame>) -> Self {
        Self {
            shard_id,
            inner,
            reply,
        }
    }

    #[must_use]
    pub const fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// The reply lane's sender. Client `Reply` forwards go here so the main
    /// lane's consensus traffic cannot evict them; everything else stays on
    /// the main lane via `Deref`.
    #[must_use]
    pub const fn reply_sender(&self) -> &Sender<ShardFrame> {
        &self.reply
    }
}

impl Clone for TaggedSender {
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id,
            inner: self.inner.clone(),
            reply: self.reply.clone(),
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
    #[error(transparent)]
    PartitionIoLimits(#[from] PartitionIoLimitsError),
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

/// The low half of this boot's consensus incarnation: the starting point for
/// [`IggyShard::next_forward_nonce`] and the boot field of every client id
/// shard 0 mints ([`coordinator::ShardZeroCoordinator`]).
///
/// Forward nonces are node-local and never persisted, so a counter that starts
/// at zero every boot re-mints the exact sequence the previous boot used. A
/// forward answer still in flight across a restart would then match a nonce a
/// DIFFERENT login now holds and confirm a login that never committed. The
/// incarnation is fresh per boot, which moves the whole sequence.
///
/// Zero on shards owning no metadata consensus (they never forward) and
/// wherever nothing set an incarnation. Production shard 0 always gets a
/// nonzero nonce, since boot draws the incarnation at random with the low bit
/// set. It must: the client ids shard 0 mints stay unique across boots only
/// through this nonce.
#[allow(clippy::cast_possible_truncation)]
fn boot_nonce<B: MessageBus>(consensus: Option<&VsrConsensus<B>>) -> u64 {
    consensus.map_or(0, VsrConsensus::incarnation) as u64
}

/// Lifecycle frame variants.
///
/// Connection setup and cross-shard forwards: every frame the inter-shard
/// channels carry that is NOT a consensus protocol message lives here.
/// Splitting these out from [`ShardFrame::Consensus`] keeps the consensus
/// dispatch path hot and cache-tight.
///
/// Lane placement: every variant rides the main inbox EXCEPT
/// [`LifecycleFrame::ForwardClientSend`], which rides the dedicated reply
/// lane (`reply_inbox_capacity`) because its drops are terminal while every
/// main-lane variant's loss is recovered by some retry (VSR retransmit,
/// reconnect sweep, periodic tick). The two lanes are independent queues:
/// there is NO relative ordering between a consensus frame and a client
/// reply forward, which is safe because a reply forward never
/// order-couples with consensus traffic (it requires a served request,
/// and per-client reply order is preserved within the reply lane itself).
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
    ReplicaInboundSetup {
        fd: DupedFd,
        slot: u64,
    },
    /// Shard 0 dialed the higher-id peer `replica_id` and delegates the
    /// raw connection; the receiving shard runs the dialer handshake
    /// half, installs on success, and answers shard 0 with
    /// [`LifecycleFrame::ReplicaOutboundHandshakeDone`] so the
    /// pending-dial entry clears and the reconnect sweep may redial on
    /// failure.
    ReplicaOutboundSetup {
        fd: DupedFd,
        replica_id: u8,
    },
    /// Owning shard -> shard 0: a delegated inbound handshake finished
    /// (any outcome). Releases the global in-flight cap slot. Lost acks
    /// are covered by the slot's deadline expiry on shard 0.
    ReplicaInboundHandshakeDone {
        slot: u64,
    },
    /// Owning shard -> shard 0: a delegated outbound handshake finished
    /// (any outcome). Clears the pending-dial entry for `replica_id`.
    /// Lost acks are covered by the entry's deadline expiry on shard 0.
    ReplicaOutboundHandshakeDone {
        replica_id: u8,
    },
    /// Shard 0 distributes an inbound SDK client TCP connection fd to the
    /// owning shard. The receiving shard wraps the fd and installs client
    /// reader / writer tasks locally. The owning shard is encoded in the top
    /// 16 bits of `meta.client_id`. `permit` is the socket's slot in the
    /// node's connection cap. It travels with the fd, so a frame dropped
    /// unprocessed frees the slot with the socket.
    ClientConnectionSetup {
        fd: DupedFd,
        meta: ClientConnMeta,
        permit: ConnectionPermit,
    },
    /// Shard 0 distributes an inbound SDK WebSocket client's pre-upgrade
    /// TCP connection fd to the owning shard. The HTTP-Upgrade handshake
    /// has NOT run yet at this point: the fd is plain TCP, the dup is
    /// safe before any transport state is created, and
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
    /// Shard 0 therefore terminates QUIC locally. A request it hands to
    /// another shard ([`LifecycleFrame::PartitionSubmit`],
    /// [`LifecycleFrame::PartitionRead`]) carries the channel its reply
    /// returns on, and shard 0 writes that reply to the connection.
    ClientWsConnectionSetup {
        fd: DupedFd,
        meta: ClientConnMeta,
        permit: ConnectionPermit,
    },
    /// Delegate TCP-TLS before reading TLS bytes. The destination wraps the
    /// fd on its runtime and owns the handshake and connection tasks.
    ClientTcpTlsConnectionSetup {
        fd: DupedFd,
        meta: ClientConnMeta,
        config: SharedTlsServerConfig,
        permit: ConnectionPermit,
    },
    /// Delegate WSS before either handshake. The listener's configuration
    /// travels with the socket; all TLS and WebSocket state stays local to
    /// the destination runtime.
    ClientWssConnectionSetup {
        fd: DupedFd,
        meta: ClientConnMeta,
        config: SharedTlsServerConfig,
        permit: ConnectionPermit,
    },
    /// A non-owning shard forwards a replica send to the owning shard's
    /// local bus; the owning shard then takes the fast path.
    ForwardReplicaSend {
        replica_id: u8,
        msg: Frozen<MESSAGE_ALIGN>,
    },
    /// A deny a synchronous caller staged for its own shard's pump to send by
    /// client id (`MessageBus::send_to_client`). The production bus refuses
    /// every frame sent by id, because a client socket or HTTP reply slot
    /// takes a reply only addressed to its installation. Only a bus that
    /// delivers by id, such as the raw simulator, answers with it.
    ForwardClientSend {
        client_id: u128,
        msg: BusMessage,
    },
    /// A peer shard hands a metadata consensus submit (login/logout) to
    /// shard 0, the metadata consensus owner. The committed op returns over
    /// the `reply` sender carried in [`MetadataSubmit`]. Always addressed to
    /// shard 0; processing it on a peer is a routing bug.
    MetadataSubmit(MetadataSubmit),
    /// Broadcast query for `get_clients`: every shard replies with the
    /// clients whose connections it homes, or only their number, over
    /// `reply`. Unlike
    /// [`MetadataSubmit`] this is sent to ALL shards (shared-nothing: each
    /// shard knows only its own connections). See
    /// [`IggyShard::list_all_clients`].
    ListClients {
        reply: ListClientsReply,
    },
    /// Execute a partition read (message poll / consumer-offset lookup) on
    /// the shard that owns `namespace` and push the result back over
    /// `reply`. See [`IggyShard::partition_read`].
    PartitionRead {
        namespace: IggyNamespace,
        read: PartitionRead,
        reply: Sender<PartitionReadReply>,
    },
    /// Admit a partition write (`SendMessages` / consumer-offset write) on
    /// the shard owning its namespace, carrying the channel its committed
    /// reply travels back on. The partition plane cannot route a reply by
    /// `header.client` -- that field is the VSR consensus id, whose bits
    /// carry no home-shard routing -- so the reply returns to the
    /// connection-owning shard, which writes it to the socket it holds.
    /// See [`IggyShard::partition_submit`].
    PartitionSubmit {
        request: Message<RoutedRequestHeader>,
        reply: Sender<Option<Message<GenericHeader>>>,
        attachment: Option<ConsumerAttachment>,
    },
    /// Shard 0 broadcasts after a partition-shaped metadata commit; wakes
    /// the per-shard reconciler. No payload: reconciler re-reads target
    /// state. Drops covered by the periodic safety tick.
    MetadataCommitTick,
    /// Last-known serving views, retried by a bounded sweep on each owning shard.
    PartitionPlacementViews(Vec<PartitionPlacementView>),
    PrimaryRebalance(Box<primary_rebalance::LocalRebalance>),
    PartitionPersistenceCompleted(partitions::PersistenceCompletion),
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
    ///
    /// Carries the bag the router already classified, not the raw frame: the
    /// receiving pump dispatches straight off the variant instead of re-running
    /// `bytemuck::checked::try_from_bytes` plus the header's `validate()` on
    /// bytes this process validated one hop ago.
    Consensus {
        target_shard: u16,
        message: MessageBag,
    },
    /// A connection setup or cross-shard forward frame. Drop recovery
    /// depends on the frame class: [`LifecycleFrame::ForwardReplicaSend`]
    /// is VSR-covered, connection-setup frames are recovered by the
    /// connector's periodic reconnect sweep, but
    /// [`LifecycleFrame::ForwardClientSend`] is terminal - no retransmit,
    /// the client never receives the reply.
    Lifecycle(LifecycleFrame),
}

// Carrying the classified bag widens the Consensus variant (32 B against the
// 24 B of the `Message<GenericHeader>` it was classified from), which is free
// only while `LifecycleFrame` remains what sizes the union. Every shard inbox
// holds thousands of these, so a regression would surface as queue memory
// rather than as a failing test.
const _: () = assert!(std::mem::size_of::<ShardFrame>() == std::mem::size_of::<LifecycleFrame>());

// Every inbox slot pays for the largest variant, including consensus traffic.
const MAX_SHARD_FRAME_SIZE: usize = 160;
const _: () = assert!(std::mem::size_of::<ShardFrame>() <= MAX_SHARD_FRAME_SIZE);

impl ShardFrame {
    /// Create a consensus frame addressed to `target_shard`. The sender
    /// is the routing authority; `accept_frame_for_self` compares this
    /// stamp against the receiving shard id in O(1).
    #[must_use]
    pub const fn consensus(target_shard: u16, message: MessageBag) -> Self {
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
/// The per-peer bus queues are bounded (`peer_queue_capacity`)
/// and overrun frames drop silently, so an unbounded burst loses its own tail;
/// the receiver pulls the window chunk by chunk instead (each walked
/// `RepairDone` immediately requests the next chunk while progress holds).
///
/// Runtime default; the server overrides the live ceiling per shard from
/// `[cluster] repair_chunk_max` at bootstrap.
pub const REPAIR_CHUNK_MAX: u64 = 128;

/// One in-flight metadata journal-repair stream (shard 0 only).
#[derive(Debug, Clone, Copy)]
struct MetadataRepairSession {
    nonce: u128,
    /// Repair cursor: the backward proof hole, otherwise the lowest requested op.
    ///
    /// Not re-derivable from `commit_min + 1`: the merged-log scan opens at
    /// [`merged_log_scan_floor`], above the snapshot floor, while its
    /// `committed_elsewhere` fallback reports ops below even that. A recomputed
    /// retry asks for a different window than the one reported missing, and never
    /// re-asks for the op that was.
    from_op: u64,
    to_op: u64,
    /// Consensus view this session was armed in. A later view decides the log
    /// again, so the window this names may no longer be the one to fetch;
    /// `partitions::RepairSession::view` fences the partition twin the same way.
    view: u32,
    /// Re-request target on stall.
    peer: u8,
    /// Ticks since the stream last made progress; at
    /// [`partitions::REPAIR_RETRY_TICKS`] the remaining window is
    /// re-requested from `peer`.
    idle_ticks: u32,
    /// Set when `peer` refused this replica a state transfer non-transiently
    /// and the refusal fell back to this session. Its `RangeEvicted` then takes
    /// the cannot-serve path instead of converting again, which would only ask
    /// the same peer for the transfer it just refused. A session minted by
    /// rotation or the gap detector starts clear, so each visit to a peer
    /// converts at most once.
    transfer_refused: bool,
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

const STATE_CHUNK_LEN: u32 = 256 * 1024;

/// Bus frame ceiling assumed before bootstrap overrides it. Matches the
/// shipped `[message_bus] max_message_size` so the simulator and unit tests
/// clamp the same way a default deployment does.
const DEFAULT_BUS_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

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
    /// Snapshot generation of the ACCEPTED descriptor, the key the decode
    /// budget is charged against.
    ///
    /// Recorded at accept because the install-time scan can fail to find it --
    /// a manifest whose snapshot entry is absent, or a checksum mismatch on an
    /// earlier artifact aborting the scan -- and an uncharged failure re-armed
    /// the same peer forever: an unbounded full-manifest re-pull loop. The
    /// descriptor cannot be accepted without one, so it is always present here.
    generation: u64,
    /// Empty until the `StateTransferTarget` manifest is accepted, then one
    /// entry per offered artifact, pulled in manifest order.
    artifacts: Vec<consensus::ArtifactProgress>,
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
/// Which plane's offer a served entry holds, and -- for partitions -- the
/// at-most-one segment payload currently resident. Partition offers address
/// segment bytes by path; the serving side loads one artifact at a time at
/// chunk-serve time, so n retained gigabytes never pin n resident gigabytes.
enum ServedOffer {
    Metadata(Rc<metadata::StateTransferOffer>),
    Partition(Rc<partitions::state_transfer::PartitionStateTransferOffer>),
}

/// Largest `segment.size` any configuration can set, mirroring
/// `configs::server_config::validators::SEGMENT_MAX_SIZE_BYTES` (the `configs`
/// crate is not a dependency here). Both the served-payload budget and the
/// per-artifact alloc cap are derived from it rather than hand-tuned.
const SEGMENT_SIZE_CEILING_BYTES: u64 = 1 << 30;

/// The most one segment can overshoot its size cap: rotation checks the cap
/// AFTER appending, so a segment closes at most one maximum-size batch past it.
///
/// Derived from the BUS frame cap, not `MAX_PAYLOAD_SIZE`: the server never
/// enforces the latter (its only enforcement sites are the legacy server and the
/// SDK batch types), so the largest appendable batch is whatever the message bus
/// will frame. This tracks the shipped `message_bus.max_message_size` default; an
/// operator raising that is caught by the config validator, which requires
/// `partition.transfer_artifact_bytes_max` to cover the topic's `segment_size` plus
/// the configured bus cap.
const SEGMENT_SIZE_OVERSHOOT_BYTES: u64 = 64 * 1024 * 1024;

/// Default alloc ceiling for ONE received state-transfer artifact.
///
/// Mirrors `[partition] transfer_artifact_bytes_max`. Free const so the config
/// crate's copy can be pinned to it by a `const _: () = assert!(..)` at the
/// server build edge, the way every other runtime default is.
pub const PARTITION_ARTIFACT_LEN_DEFAULT: u64 =
    SEGMENT_SIZE_CEILING_BYTES + SEGMENT_SIZE_OVERSHOOT_BYTES;

/// Default per-shard resident budget for served segment payloads
/// (`[partition] transfer_served_cache_bytes_max`). Pinned like
/// [`PARTITION_ARTIFACT_LEN_DEFAULT`].
pub const SERVED_SEGMENT_CACHE_BYTES_DEFAULT: u64 =
    PARTITION_ARTIFACT_LEN_DEFAULT * CONCURRENT_SERVED_SEGMENTS;

/// Distinct max-size segments the served-payload budget holds at once.
///
/// TWO, not the receiver's in-flight cap of four: the budget is PER SHARD and
/// shard count defaults to core count, so each segment here multiplies by the
/// core count during a whole-node rejoin, on top of page cache and the receive
/// side's own in-flight artifacts.
///
/// The gap between this and the in-flight cap is closed by ADMITTING fewer
/// concurrent transfers rather than by holding more bytes: see
/// `IggyShard::partition_transfer_admission_cap`, which derives its cap from
/// this budget so the two can never disagree. Overrunning the budget does not
/// degrade gracefully -- distinct groups are distinct cache keys, so a surplus
/// pull evicts the others on every chunk and none of them converge -- and an
/// operator who wants more concurrency raises the knob, which raises the cap
/// with it.
const CONCURRENT_SERVED_SEGMENTS: u64 = 2;

/// Shard-wide cache of segment payloads loaded to serve partition chunks,
/// content-addressed by `(namespace, manifest checksum)` so every requester
/// pulling the same offer generation shares ONE resident copy (per-requester
/// slots pinned R copies on whole-node rejoins), while requesters on
/// different generations never alias. LRU-evicted under a byte budget; an
/// oversized single segment still loads (the serve could not proceed
/// otherwise) and simply owns the budget until aged out.
#[derive(Default)]
struct ServedSegmentCache {
    entries: HashMap<(u64, u64), CachedSegmentPayload>,
    resident_bytes: u64,
    use_seq: u64,
    /// Idle sweeps run so far; entries carry the reading at their last use, so
    /// their age is measured on the OFFER clock rather than the raw tick.
    sweeps: u64,
}

/// One resident payload, its last-use sequence (the LRU key), and the sweep
/// reading at that use (the age key).
struct CachedSegmentPayload {
    payload: Rc<Vec<u8>>,
    last_use: u64,
    last_use_sweep: u64,
}

impl ServedSegmentCache {
    /// Byte budget across all resident segment payloads on ONE shard, so the
    /// process-wide bound is this times the shard count. LRU pressure from new
    /// inserts plus the idle sweep below reclaim it; a single segment larger than
    /// the budget still loads (the serve could not proceed otherwise) and owns
    /// the budget until it ages out. A config knob can follow if operators need
    /// to trade it against page cache.
    ///
    /// Sized for CONCURRENT pulls, not one: at exactly one max-size segment
    /// (`segment.size` defaults to and is capped at 1 GiB) a single receiver
    /// arming its `PARTITION_TRANSFERS_INFLIGHT_MAX` transfers thrashes the
    /// cache by itself -- distinct partitions are distinct keys, so the pulls
    /// evict each other on every chunk, and each miss re-reads and re-hashes a
    /// whole segment to serve one 256 KiB chunk. That is the 4096:1 read
    /// amplification this cache exists to prevent, plus an offer eviction per
    /// failed re-verify feeding the hard-failure backoff.
    /// Drop every payload that has served nothing for `idle_sweeps_max` sweeps.
    ///
    /// The budget comes from the caller because the two clocks differ: this
    /// sweep runs on the raw 10 ms consensus tick while the offers these
    /// payloads back expire on `retry_ticks * MULTIPLE`. Counting bare sweeps
    /// gave a payload ~100 ms against an offer's ~10 s, so one dropped chunk
    /// frame -- whose only re-drive is the 1 s stall sweep -- evicted the
    /// payload and made the resume re-read and re-hash the whole segment to
    /// serve the next 256 KiB. The trade in the other direction: an abandoned
    /// pull now pins its resident payload for the full offer window.
    ///
    /// Runs from the same place offers expire: without it, one rejoin leaves a
    /// permanent high-water of resident bytes (nothing else releases the cache
    /// once the pulls stop).
    fn expire_idle(&mut self, idle_sweeps_max: u64) {
        self.sweeps += 1;
        // Strictly BELOW the floor: at `<=` an entry stamped on sweep 0 matches
        // `0 <= 0` on the very first sweep and is dropped whatever the budget
        // says, and every other entry loses one sweep of its lifetime. Harmless
        // in production, but it makes the budget untestable at its boundary.
        let floor = self.sweeps.saturating_sub(idle_sweeps_max);
        let stale: Vec<(u64, u64)> = self
            .entries
            .iter()
            .filter(|(_, cached)| cached.last_use_sweep < floor)
            .map(|(&key, _)| key)
            .collect();
        for key in stale {
            if let Some(evicted) = self.entries.remove(&key) {
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(evicted.payload.len() as u64);
            }
        }
    }

    /// Drop every payload cached for `namespace`, crediting their bytes back.
    ///
    /// Retention, a truncate or an install unlinks the segments these payloads
    /// copy, and the cache key is the manifest checksum over the removed bytes,
    /// so nothing about a hit can notice: the serve path answers from the
    /// resident copy without touching disk, and every served chunk resets the
    /// expiry clock, so an active puller keeps deleted data alive indefinitely.
    fn evict_namespace(&mut self, namespace: u64) {
        let stale: Vec<(u64, u64)> = self
            .entries
            .keys()
            .filter(|(entry_namespace, _)| *entry_namespace == namespace)
            .copied()
            .collect();
        for key in stale {
            if let Some(evicted) = self.entries.remove(&key) {
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(evicted.payload.len() as u64);
            }
        }
    }

    fn get(&mut self, namespace: u64, checksum: u64) -> Option<Rc<Vec<u8>>> {
        self.use_seq += 1;
        let use_seq = self.use_seq;
        let sweeps = self.sweeps;
        let cached = self.entries.get_mut(&(namespace, checksum))?;
        cached.last_use = use_seq;
        cached.last_use_sweep = sweeps;
        Some(Rc::clone(&cached.payload))
    }

    fn insert(&mut self, namespace: u64, checksum: u64, payload: Rc<Vec<u8>>, budget: u64) {
        let incoming = payload.len() as u64;
        // Credited BEFORE the eviction scan: re-inserting an existing key frees
        // its own slot, and charging that only afterwards evicted neighbours to
        // make room for bytes that were about to be released.
        if let Some(replaced) = self.entries.remove(&(namespace, checksum)) {
            self.resident_bytes = self
                .resident_bytes
                .saturating_sub(replaced.payload.len() as u64);
        }
        while self.resident_bytes.saturating_add(incoming) > budget && !self.entries.is_empty() {
            let Some((&key, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_use)
            else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&key) {
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(evicted.payload.len() as u64);
            }
        }
        self.use_seq += 1;
        // The key was removed above, so this never replaces an entry whose bytes
        // still need crediting back.
        self.entries.insert(
            (namespace, checksum),
            CachedSegmentPayload {
                payload,
                last_use: self.use_seq,
                last_use_sweep: self.sweeps,
            },
        );
        self.resident_bytes = self.resident_bytes.saturating_add(incoming);
    }
}

struct ServedStateTransfer {
    nonce: u128,
    offer: ServedOffer,
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

/// One `StateTransferTarget` descriptor: the offer if there is one, plus what
/// the serving replica knows about its own progress.
///
/// The progress fields ride along even on a refusal, so a receiver can tell a
/// peer that is momentarily behind from one that knows less than it does. They
/// are CONSTRUCTOR arguments rather than an optional builder step: as an
/// optional step every one of the eight construction sites had to remember it,
/// and two did not.
struct TransferDescriptor<'a> {
    /// `Some((manifest, commit_op))` when the peer can serve.
    offer: Option<(&'a [consensus::StateArtifact], u64)>,
    /// Serving replica's view and commit frontier at build time.
    view: u32,
    commit_max: u64,
    /// A refusal the requester should retry soon WITHOUT charging its
    /// consecutive-failure count. Always false when `offer` is `Some`.
    transient: bool,
}

impl<'a> TransferDescriptor<'a> {
    const fn available(
        offer: &'a [consensus::StateArtifact],
        commit_op: u64,
        view: u32,
        commit_max: u64,
    ) -> Self {
        Self {
            offer: Some((offer, commit_op)),
            view,
            commit_max,
            transient: false,
        }
    }

    const fn unavailable(transient: bool, view: u32, commit_max: u64) -> Self {
        Self {
            offer: None,
            view,
            commit_max,
            transient,
        }
    }
}

/// What `on_request_state_chunk` decided inside its offers borrow; the wire
/// sends run after the borrow drops.
enum ChunkReply {
    Chunk(Message<StateChunkHeader>),
    /// Offer evicted (e.g. the serving process restarted, or the segment it
    /// named can no longer be served): the requester gets an unavailable
    /// descriptor and restarts its session. `transient` carries whether the
    /// cause was this node's fault, which is what decides if the requester
    /// charges a failure.
    Unavailable {
        transient: bool,
    },
}

/// Shard ticks between two lines of one paced warning: ten seconds of
/// [`CONSENSUS_TICK_INTERVAL`] ticks.
const WARN_PACE_TICKS: u64 = 1_000;

/// Rate limit for one recurring warning: at most one line per
/// [`WARN_PACE_TICKS`]. Paced by shard ticks rather than wall-clock time, so a
/// simulator replay logs the same lines.
#[derive(Clone, Copy, Debug, Default)]
struct WarnPace {
    logged_at: Option<u64>,
}

impl WarnPace {
    /// Whether a line may print at `tick`. Admitting one starts the next quiet
    /// window.
    fn admit(&mut self, tick: u64) -> bool {
        if self
            .logged_at
            .is_some_and(|logged_at| tick.saturating_sub(logged_at) < WARN_PACE_TICKS)
        {
            return false;
        }
        self.logged_at = Some(tick);
        true
    }
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

    /// The process embedding this shard: serves client requests, runs
    /// metadata submits, answers the list-clients query. See [`ShardHost`].
    host: Rc<dyn ShardHost>,

    /// [`ShardHost::on_replica_message`] as the bus installs it on every
    /// delegated replica connection; built once so an install clones one `Rc`.
    on_replica_message: MessageHandler,

    /// [`ShardHost::on_client_request`] as the bus installs it on every
    /// delegated client connection, and as shard 0 hands it to the transports
    /// it terminates locally (see [`Self::client_request_handler`]).
    on_client_request: RequestHandler,

    /// In-flight metadata journal repair: set when the recovery
    /// handshake finds this replica's WAL behind the group frontier, cleared
    /// at `RepairDone`. Metadata never needs a commit floor -- its WAL keeps
    /// the full prefix -- so only the stream identity is tracked.
    metadata_repair: RefCell<Option<MetadataRepairSession>>,

    /// In-flight metadata state transfer (cluster-restart rejoin); tail
    /// repair takes over at install. See [`MetadataTransferSession`].
    metadata_transfer: RefCell<Option<MetadataTransferSession>>,

    /// Consecutive ticks the metadata group has been seen gap-stopped
    /// (committed ops it cannot walk to, because the op one past its commit
    /// frontier is missing from the WAL). Debounces `tick_metadata`'s
    /// level-triggered repair arm; the partition twin is
    /// `IggyPartition::gap_ticks`. `Cell` because the tick drives it through
    /// `&self`, and shard-level rather than plane-level because there is one
    /// metadata group per node (precedent: [`Self::metadata_transfer_attempts`]).
    metadata_gap_ticks: Cell<u32>,

    /// Op the last `BodyMissing` drop targeted, or `0`. Recorded before
    /// `truncate_from` and kept if it fails, so a storage error is not retried
    /// every tick. The gap probe counts it as absent so repair can refetch it,
    /// and it goes inert once `commit_min` passes it.
    metadata_walk_stuck_op: Cell<u64>,

    /// Serving-side cache of state-transfer offers, both planes, keyed by
    /// `(namespace, requester replica id)`. Bounded by the replica count times
    /// the groups this shard serves; replaced per fresh nonce.
    state_transfer_offers: RefCell<HashMap<(u64, u8), ServedStateTransfer>>,
    /// Partition groups with an offer build under way but no offer yet, keyed
    /// by namespace and carrying ticks since the last request that advanced it.
    ///
    /// A build spans rounds (the checksum pass is budgeted per frame body), and
    /// during those rounds nothing in `state_transfer_offers` names the group,
    /// so admission control cannot see it without this. Aged out on the same
    /// clock as an idle offer, since a requester that walked away leaves
    /// nothing else to release the slot.
    partition_offer_builds: RefCell<HashMap<u64, u32>>,

    /// See [`ServedSegmentCache`].
    served_segment_cache: RefCell<ServedSegmentCache>,

    /// Logins this node forwarded to the primary and is still waiting on, keyed
    /// by the `(nonce, client)` pair stamped into the `ForwardRegister` frame.
    /// Shard 0 only, since that is where the forward is issued and where the
    /// result routes back to. Entries are removed at exactly three points --
    /// result delivery, forward timeout, and a failed send -- so an abandoned
    /// login cannot leak one.
    ///
    /// The client id is part of the key rather than payload the ingest compares:
    /// an answer echoing a client the nonce was never parked for is then exactly
    /// as unroutable as one carrying an unknown nonce, and the miss leaves the
    /// legitimate entry parked instead of evicting it.
    register_forwards: RefCell<HashMap<(u128, u128), Sender<ForwardRegisterResultHeader>>>,

    /// Logouts this node forwarded to the primary and is still waiting on.
    logout_forwards: RefCell<HashMap<(u128, u128), Sender<ForwardLogoutResultHeader>>>,
    segment_delete_forwards: RefCell<HashMap<u128, PendingSegmentDelete>>,

    /// Monotonic source of forwarding nonces. Node-local: the
    /// nonce only has to distinguish this node's own in-flight forwards, across
    /// its restarts as well as within one boot. Seeded by
    /// [`boot_nonce`].
    forward_nonce: Cell<u64>,

    /// Channel senders to every shard, indexed by shard id.
    /// Includes a sender to self so that local routing goes through the
    /// same channel path as remote routing.
    ///
    /// [`validate_sender_ordering`] runs during construction so `senders[i]`
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

    /// Receiver end of this shard's reply lane: cross-shard client `Reply`
    /// forwards, split off the main inbox because their drops are terminal
    /// (no in-protocol retransmit) while a consensus burst can legitimately
    /// fill the main lane. Fed via [`TaggedSender::reply_sender`].
    reply_inbox: Receiver<ShardFrame>,

    /// Disk reads reserve this lane before I/O so ordinary frames cannot
    /// displace their results. Only the owner pump validates completions.
    poll_completions: poll::completion::PollCompletionLane,
    partition_io: partition_io::PartitionIoLane<SB>,
    cooperative_events: Cell<usize>,

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

    /// Observability counters shared with the metrics registry.
    metrics: crate::metrics::ShardMetrics,

    /// Late-bound `MetadataCommitTick` handler. `None` until reconciler
    /// wires it; pre-wire ticks drop with a metric bump.
    metadata_tick_handler: RefCell<Option<Rc<dyn Fn()>>>,

    /// Reconciler → pump funnel. Borrow discipline: every push / drain
    /// runs without `.await` inside the borrow.
    reconcile_queue: RefCell<VecDeque<ReconcileOp<B, SB>>>,
    shutting_down: Cell<bool>,

    /// Partition-plane frames that arrived before this shard's reconciler
    /// materialised the namespace (post-`CreateTopic` convergence window).
    /// Parked here instead of dropped -- there is no consensus retransmit
    /// driver in production yet -- and re-dispatched when the matching
    /// `ReconcileOp::InsertOwned` lands with the epoch they were stamped
    /// against. Bounded per namespace; a full buffer sheds via
    /// [`ParkOutcome::Overflow`] so the caller can still answer.
    ///
    /// An entry only drains when the namespace materialises or leaves committed
    /// metadata, so the reconciler reclaims the ones that will do neither -- see
    /// `partition_reconciler::reconcile_parked_frames`. Without that sweep a
    /// namespace whose build keeps failing would hold its frames for the process
    /// lifetime while every client waited out its read timeout.
    ///
    /// [`BTreeMap`], not `HashMap`: [`Self::parked_namespaces`] feeds the
    /// reconciler sweep, which answers frames in the order it walks them.
    /// `std::collections::HashMap` seeds its hasher per process, so iteration
    /// order would vary run to run for identical committed state, making the
    /// simulator's deny ordering unreproducible for a fixed seed -- the same
    /// hazard `router.rs` documents as its reason for `select_biased!`.
    pending_partition_frames: RefCell<BTreeMap<IggyNamespace, ParkEntry>>,

    /// Running sum of [`ParkEntry::bytes`], maintained at each mutation site.
    ///
    /// Recomputing per arriving frame is O(all parked frames). Footprints floor
    /// at [`MESSAGE_ALIGN`], so the budget admits 4096 entries: ~8.4M visits per
    /// admission, on the reactor thread inside the map's `borrow_mut`.
    parked_partition_bytes: Cell<usize>,

    /// Frames [`Self::redispatch_parked_frames`] handed back for the pump to
    /// deliver, in park order.
    ///
    /// Staging exists because re-dispatch runs inside the synchronous
    /// [`Self::apply_reconcile_ops`] while the plane is reachable only through
    /// an async path. A biased select arm takes one staged frame per pump
    /// iteration and ranks above the inbox, so a parked op reaches the plane
    /// ahead of a later op already sitting there. One-at-a-time delivery lets
    /// consensus ticks and commit broadcasts run between frames instead of
    /// stalling behind the whole bounded queue.
    redispatch_queue: RefCell<VecDeque<ParkedFrame>>,

    /// Set while the shard-wide budget is shedding for namespaces holding no
    /// park entry of their own, which have no [`ParkEntry::shed`] to warn once
    /// from. Cleared when the park map empties, so one episode warns once.
    shard_park_shedding: Cell<bool>,

    /// Set once a partition submit has waited out its budget and warned;
    /// cleared by the next reply that arrives. Gates the timeout warning to
    /// one line per stall episode (see [`Self::await_partition_submit`]).
    partition_submit_stalled: Cell<bool>,

    /// Live ceiling on prepares served per `RequestPrepares` round. Defaults
    /// to [`REPAIR_CHUNK_MAX`]; the server overrides it from
    /// `[cluster] repair_chunk_max` at bootstrap.
    repair_chunk_max: Cell<u64>,

    /// Live stalled-repair retry threshold in consensus ticks. Defaults to
    /// [`partitions::REPAIR_RETRY_TICKS`]; the server overrides it from
    /// `[cluster] repair_retry_interval` at bootstrap.
    repair_retry_ticks: Cell<u32>,

    /// Live repair sessions on this shard, republished by every partition sweep
    /// and incremented as sessions open, for
    /// [`PARTITION_REPAIRS_INFLIGHT_MAX`]. A tally rather than a scan because
    /// the arming funnel holds a `&mut` to one partition, which a scan over the
    /// plane would alias; one sweep stale at worst.
    partition_repairs_inflight: Cell<usize>,

    /// Live gap debounce in consensus ticks: how long a group holds a hole
    /// before the tick opens a repair session for it. Shared by both planes.
    /// Defaults to [`partitions::REPAIR_RETRY_TICKS`]; the server overrides it
    /// from `[cluster] repair_gap_debounce_interval` at bootstrap.
    repair_gap_debounce_ticks: Cell<u32>,

    /// Namespace the next partition sweep starts from: the first group the
    /// per-tick WALK budget turned away last pass, `None` to start at the front.
    ///
    /// The sweep visits namespaces in `BTreeMap` order, so without a carried
    /// cursor the leading groups would spend the whole budget on every pass and
    /// the tail would never be reached. See [`rotate_sweep_to_cursor`].
    partition_walk_cursor: Cell<Option<IggyNamespace>>,
    partition_placement_cursor: Cell<Option<IggyNamespace>>,
    /// Namespaces still owed a placement report in the current interval.
    partition_placement_owed: Cell<usize>,
    primary_rebalance: RefCell<primary_rebalance::PrimaryRebalance>,

    /// Consecutive metadata superblock write failures tolerated before the
    /// process fail-stops. Defaults to 0 (disabled) so the simulator and tests
    /// keep a wedged-but-fenced replica alive; the server arms it from
    /// `[cluster] superblock_wedged_fatal_timeout` at bootstrap.
    superblock_wedged_fatal_failures: Cell<u64>,

    /// Live `[partition] transfer_served_cache_bytes_max`: the byte budget for
    /// segment payloads this shard keeps resident to serve chunk requests.
    /// Defaults to [`SERVED_SEGMENT_CACHE_BYTES_DEFAULT`]; the server
    /// overrides it at bootstrap.
    served_segment_cache_bytes_max: Cell<u64>,

    /// Live `[partition] transfer_artifact_bytes_max`: the alloc ceiling for one
    /// RECEIVED artifact. Defaults to [`PARTITION_ARTIFACT_LEN_DEFAULT`];
    /// the server overrides it at bootstrap.
    partition_artifact_len_max: Cell<u64>,

    /// Live `[message_bus] max_message_size`. Bounds a served state chunk: a
    /// frame above this is rejected by the RECEIVING transport, which tears
    /// down the whole replica connection. Defaults to a value that leaves
    /// [`STATE_CHUNK_LEN`] usable; the server overrides it at bootstrap.
    bus_max_message_size: Cell<usize>,

    /// Consecutive metadata state-transfer rounds that made no progress.
    ///
    /// Deliberately NOT on [`MetadataTransferSession`]: three of the four
    /// arming sites mint a fresh session, so a per-session counter bounded
    /// nothing. Held here it survives the abandon -> repair -> re-arm cycle,
    /// and chunk arrival resets it (see
    /// [`IggyShard::note_metadata_transfer_progress`]) so scattered transient
    /// stalls cannot accumulate into abandoning a nearly-complete transfer.
    /// That reset also means it bounds only peers that serve no chunk, silent
    /// or refusing as busy: decode failures keep frames flowing, and are bounded
    /// separately by [`Self::metadata_transfer_decode_failures`].
    metadata_transfer_attempts: Cell<u32>,

    /// Consecutive stall rounds burned by the metadata repair session, against
    /// [`partitions::REPAIR_MAX_STALL_RETRIES`]. Bounds how long one quiet peer
    /// pins the commit walk.
    ///
    /// On the shard, not the session: rotation mints a fresh session, so a
    /// per-session counter would reset itself. Nothing on the rotation path may
    /// clear it either.
    ///
    /// Cleared only by [`Self::note_metadata_repair_walked`], which takes evidence
    /// attributable to the targeted peer. A repair prepare carries no sender and
    /// no nonce, so it restarts the stall clock only. Net effect: this bounds peers
    /// that go silent, serve unusable bytes, or terminate without closing the gap.
    /// It does not bound merely slow peers, whose chunks keep the clock from
    /// firing.
    ///
    /// Paired with the view the rounds were charged in, and spent per view. The
    /// merged-log arm refuses to re-arm once the budget is out, and the exhausting
    /// path leaves no session behind, so nothing would ever be superseded or walk:
    /// unfenced, one spent budget would refuse merged-log repair for every later
    /// view for the life of the process, and a replica that keeps winning
    /// elections would never repair again.
    metadata_repair_attempts: Cell<(u32, u32)>,

    /// Repaired metadata prepares dropped because their slot holds an entry
    /// with a different checksum, all of them, for the paced warning: reconcile
    /// is meant to remove a divergent entry before repair refills its slot, so
    /// a count that keeps growing names a slot repair can never fill.
    metadata_repair_conflicts: Cell<u64>,
    metadata_repair_conflict_warn: Cell<WarnPace>,

    /// Pacing of the incarnation-mismatch warning, per dropped frame's command
    /// discriminant (`Command` derives no `Hash`).
    incarnation_mismatch_warns: RefCell<HashMap<u8, WarnPace>>,

    /// Shard ticks so far: the clock of every [`WarnPace`].
    ticks: Cell<u64>,

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

    /// [`Self::inbox_len`] for the reply lane, so the simulator's lost-wakeup
    /// tripwire covers both queues: a frame stranded in either lane at
    /// quiescence is a missed wake.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn reply_inbox_len(&self) -> usize {
        self.reply_inbox.len()
    }

    /// Queued disk completions covered by the simulator's lost wakeup check.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn poll_completion_inbox_len(&self) -> usize {
        self.poll_completions.len()
    }

    /// The armed metadata repair window as `(to_op, peer)`, `None` when no session
    /// is running.
    ///
    /// Diagnostic accessor, like the two above. `maybe_request_metadata_repair`
    /// refuses to arm while any session exists, so a stale one reads as repairing
    /// forever and only this separates that from real progress.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn metadata_repair_window(&self) -> Option<(u64, u8)> {
        self.metadata_repair
            .borrow()
            .as_ref()
            .map(|session| (session.to_op, session.peer))
    }

    /// Create a new shard with channel links and a shards table.
    ///
    /// * `bus` - shard-local bus handle (kept alongside the buses owned
    ///   by the consensus planes so the router can reach the
    ///   `ConnectionInstaller` surface directly).
    /// * `host` - the embedding process' handlers, see [`ShardHost`].
    ///   Wrapped once here into the `Rc<dyn Fn>` adapters the bus installs
    ///   per delegated connection.
    /// * `senders` - one [`TaggedSender`] per shard. The ctor asserts
    ///   `senders[i].shard_id() == i`; use [`shard_channel`] at
    ///   construction time so every sender carries the id of the shard
    ///   whose receiver drains it.
    /// * `inbox` - the receiver that this shard drains in its message pump.
    /// * `reply_inbox` - the reply lane's receiver, drained by the same
    ///   pump (client `Reply` forwards only; see [`TaggedSender::reply_sender`]).
    /// * `poll_completion_capacity` - separate limit on running disk polls plus
    ///   results awaiting dequeue by this shard's owner pump. Must be nonzero.
    /// * `partition_io_limits` - validated file-job limits, or the default limits.
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
    /// Returns [`ShardCtorError::PartitionIoLimits`] if the default I/O limits
    /// cannot represent the compiled job sizes.
    ///
    /// # Panics
    ///
    /// Panics if `poll_completion_capacity` is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ShardIdentity,
        bus: B,
        host: Rc<dyn ShardHost>,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M, SB>,
        partitions: IggyPartitions<B, SB>,
        senders: Vec<TaggedSender>,
        inbox: Receiver<ShardFrame>,
        reply_inbox: Receiver<ShardFrame>,
        poll_completion_capacity: usize,
        partition_io_limits: Option<PartitionIoLimits>,
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
        let nonce_seed = boot_nonce(metadata.consensus.as_ref());
        // Metadata admits a stream's wrap set with the keys its partitions open.
        let metadata = metadata.with_stream_key_context(partitions.key_context());
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        let poll_completions =
            poll::completion::PollCompletionLane::new(poll_completion_capacity, &metrics);
        let partition_io = partition_io::PartitionIoLane::new(
            match partition_io_limits {
                Some(limits) => limits,
                None => PartitionIoLimits::new(partition_io::DEFAULT_PARTITION_IO_CAPACITY, None)?,
            },
            &metrics,
        );
        Ok(Self {
            id,
            name,
            plane,
            bus,
            on_replica_message: host::replica_message_handler(&host),
            on_client_request: host::client_request_handler(&host),
            host,
            senders,
            shard_count,
            inbox,
            reply_inbox,
            poll_completions,
            partition_io,
            cooperative_events: Cell::new(0),
            shards_table,
            partition_consensus,
            coordinator,
            metrics,
            metadata_tick_handler: RefCell::new(None),
            reconcile_queue: RefCell::new(VecDeque::new()),
            shutting_down: Cell::new(false),
            pending_partition_frames: RefCell::new(BTreeMap::new()),
            parked_partition_bytes: Cell::new(0),
            redispatch_queue: RefCell::new(VecDeque::new()),
            shard_park_shedding: Cell::new(false),
            partition_submit_stalled: Cell::new(false),
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            metadata_gap_ticks: Cell::new(0),
            metadata_walk_stuck_op: Cell::new(0),
            state_transfer_offers: RefCell::new(HashMap::new()),
            partition_offer_builds: RefCell::new(HashMap::new()),
            served_segment_cache: RefCell::new(ServedSegmentCache::default()),
            register_forwards: RefCell::new(HashMap::new()),
            logout_forwards: RefCell::new(HashMap::new()),
            segment_delete_forwards: RefCell::new(HashMap::new()),
            forward_nonce: Cell::new(nonce_seed),
            served_segment_cache_bytes_max: Cell::new(SERVED_SEGMENT_CACHE_BYTES_DEFAULT),
            partition_artifact_len_max: Cell::new(PARTITION_ARTIFACT_LEN_DEFAULT),
            repair_chunk_max: Cell::new(REPAIR_CHUNK_MAX),
            repair_retry_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            repair_gap_debounce_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            partition_repairs_inflight: Cell::new(0),
            partition_walk_cursor: Cell::new(None),
            partition_placement_cursor: Cell::new(None),
            partition_placement_owed: Cell::new(0),
            primary_rebalance: RefCell::default(),
            superblock_wedged_fatal_failures: Cell::new(0),
            bus_max_message_size: Cell::new(DEFAULT_BUS_MAX_MESSAGE_SIZE),
            metadata_transfer_attempts: Cell::new(0),
            metadata_repair_attempts: Cell::new((0, 0)),
            metadata_repair_conflicts: Cell::new(0),
            metadata_repair_conflict_warn: Cell::default(),
            incarnation_mismatch_warns: RefCell::default(),
            ticks: Cell::new(0),
            metadata_transfer_decode_failures: Cell::new(None),
        })
    }

    /// Override the stalled-repair retry threshold (consensus ticks) from
    /// configuration. Called once per shard at bootstrap; the simulator and
    /// tests keep the compile-time [`partitions::REPAIR_RETRY_TICKS`] default.
    pub fn set_repair_retry_ticks(&self, ticks: u32) {
        self.repair_retry_ticks.set(ticks);
    }

    /// Override the tick gap debounce (consensus ticks) from configuration,
    /// for both planes' detectors. Called once per shard at bootstrap; the
    /// simulator and tests keep the compile-time
    /// [`partitions::REPAIR_RETRY_TICKS`] default.
    /// [`REPAIR_GAP_DEBOUNCE_TICKS_MIN`] still floors whatever is set.
    pub fn set_repair_gap_debounce_ticks(&self, ticks: u32) {
        self.repair_gap_debounce_ticks.set(ticks);
    }

    /// Arm the superblock fail-stop bound (consecutive write failures).
    /// Called once per shard at bootstrap; the simulator and tests keep the
    /// disabled default (0) so a wedged-but-fenced replica stays observable
    /// in-process.
    pub fn set_superblock_wedged_fatal_failures(&self, failures: u64) {
        self.superblock_wedged_fatal_failures.set(failures);
    }

    /// Bound running file jobs in pump ticks. Zero disables the deadline.
    pub fn set_partition_io_timeout(&self, timeout: Duration) {
        self.partition_io.set_timeout(timeout);
    }

    /// Override the serving-side resident payload budget from configuration.
    /// Called once per shard at bootstrap.
    pub fn set_served_segment_cache_bytes_max(&self, bytes: u64) {
        self.served_segment_cache_bytes_max.set(bytes);
    }

    /// Override the per-artifact receive ceiling from configuration. Called once
    /// per shard at bootstrap.
    pub fn set_partition_artifact_len_max(&self, bytes: u64) {
        self.partition_artifact_len_max.set(bytes);
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

    /// The configured `[message_bus] max_message_size`. Also the largest batch
    /// the bus will frame, and so the most a sealed segment can overshoot
    /// `segment_size`: rotation fires after the append that crosses the cap.
    #[must_use]
    pub const fn bus_max_message_size(&self) -> usize {
        self.bus_max_message_size.get()
    }

    /// Mint a fresh, never-zero nonce for a register or logout forward.
    ///
    /// `replica` rides the high half, which separates the nonce spaces of
    /// different NODES: a result frame that somehow arrives from the wrong node
    /// cannot collide with a live entry. Successive boots of THIS node are
    /// separated by the counter's incarnation seed instead.
    ///
    /// The counter skips zero on wrap: both forwarding headers reject a zero
    /// nonce in `validate`, so a wrapped counter would have the origin build a
    /// frame the primary drops.
    pub fn next_forward_nonce(&self, replica: u8) -> u128 {
        let counter = self.forward_nonce.get().wrapping_add(1).max(1);
        self.forward_nonce.set(counter);
        (u128::from(replica) << 64) | u128::from(counter)
    }

    /// Park a forwarded login under `(nonce, client)` until the primary answers.
    pub fn park_register_forward(
        &self,
        nonce: u128,
        client: u128,
        reply: Sender<ForwardRegisterResultHeader>,
    ) {
        self.register_forwards
            .borrow_mut()
            .insert((nonce, client), reply);
    }

    /// Drop a parked login (timeout, or a forward that never left the node).
    pub fn cancel_register_forward(&self, nonce: u128, client: u128) {
        self.register_forwards.borrow_mut().remove(&(nonce, client));
    }

    /// Park a forwarded logout under `(nonce, client)` until the primary answers.
    pub fn park_logout_forward(
        &self,
        nonce: u128,
        client: u128,
        reply: Sender<ForwardLogoutResultHeader>,
    ) {
        self.logout_forwards
            .borrow_mut()
            .insert((nonce, client), reply);
    }

    /// Drop a parked logout after timeout or a failed send.
    pub fn cancel_logout_forward(&self, nonce: u128, client: u128) {
        self.logout_forwards.borrow_mut().remove(&(nonce, client));
    }

    /// Bound outstanding resolution requests, reclaiming canceled receivers.
    pub fn park_segment_delete_forward(
        &self,
        query: SegmentDeleteHeader,
        target: u8,
        reply: Sender<SegmentDeleteHeader>,
    ) -> bool {
        let mut pending = self.segment_delete_forwards.borrow_mut();
        pending.retain(|_, entry| !entry.reply.is_disconnected());
        if pending.len() >= MAX_SEGMENT_DELETE_FORWARDS {
            return false;
        }
        pending.insert(
            query.nonce,
            PendingSegmentDelete {
                query,
                target,
                reply,
            },
        );
        true
    }

    pub fn cancel_segment_delete_forward(&self, nonce: u128) {
        self.segment_delete_forwards.borrow_mut().remove(&nonce);
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
    /// `LIST_CLIENTS_GATHER_TIMEOUT` is skipped and the partial result is
    /// logged, so one wedged shard cannot hang the read. Callers should
    /// treat the result as best-effort-complete.
    #[allow(clippy::future_not_send)]
    pub async fn list_all_clients(&self) -> Vec<ConnectedClientInfo> {
        self.gather_clients().await.clients
    }

    /// Count every shard's connected clients, bounded like
    /// [`Self::list_all_clients`]. Each shard replies with one number, so the
    /// periodic readers build no client records.
    #[allow(clippy::future_not_send)]
    pub async fn count_all_clients(&self) -> usize {
        let (count, _complete) = self
            .gather_replies(ListClientsReply::Count, 0usize, |count, shard_clients| {
                *count = count.saturating_add(shard_clients);
            })
            .await;
        count
    }

    /// Gather administrative client details with completeness information.
    #[allow(clippy::future_not_send)]
    pub async fn gather_clients(&self) -> GatheredClients {
        self.gather_client_info(ListClientsReply::Clients).await
    }

    /// Gather bound session identities with completeness information for expiry decisions.
    #[allow(clippy::future_not_send)]
    pub async fn gather_consumer_sessions(&self) -> GatheredClients<ConsumerSession> {
        self.gather_client_info(ListClientsReply::Sessions).await
    }

    #[allow(clippy::future_not_send)]
    async fn gather_client_info<ClientInfo: Send + 'static>(
        &self,
        reply: fn(Sender<Vec<ClientInfo>>) -> ListClientsReply,
    ) -> GatheredClients<ClientInfo> {
        let (clients, complete) = self.gather_replies(reply, Vec::new(), Extend::extend).await;
        GatheredClients { clients, complete }
    }

    /// Broadcast one [`LifecycleFrame::ListClients`] and fold each shard's
    /// reply into `gathered`. The flag is false if a shard did not answer.
    #[allow(clippy::future_not_send)]
    async fn gather_replies<Reply: Send + 'static, Gathered>(
        &self,
        reply: fn(Sender<Reply>) -> ListClientsReply,
        mut gathered: Gathered,
        mut fold: impl FnMut(&mut Gathered, Reply),
    ) -> (Gathered, bool) {
        let shard_count = self.shard_count as usize;
        let (reply_tx, reply_rx) = channel::<Reply>(shard_count.max(1));
        let mut expected = 0usize;
        for sender in &self.senders {
            let frame = ShardFrame::lifecycle(LifecycleFrame::ListClients {
                reply: reply(reply_tx.clone()),
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
                    Ok(shard_reply) => {
                        fold(&mut gathered, shard_reply);
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
        (gathered, received == shard_count)
    }

    /// Run a partition read on the shard owning `namespace` and await the reply.
    ///
    /// Routes a [`LifecycleFrame::PartitionRead`] through the shards table
    /// (including sends to this shard). An unroutable namespace, missing sender,
    /// or rejected inbox submission returns [`PartitionReadReply::Rejected`]
    /// with [`IggyError::TransientNotAccepted`]: the owner never received the
    /// request, so this read cannot advance progress and is safe to retry.
    ///
    /// `None` means submission succeeded but the reply sender was dropped or
    /// `PARTITION_READ_TIMEOUT` expired. A timeout drops the reply receiver
    /// without canceling a queued request or detached read. The owner discards
    /// a poll completion if it observes disconnection before admission. If
    /// timeout races with that check, completion can still advance progress and
    /// admit an automatic commit. Callers must not treat a missing reply as an
    /// accepted empty poll or as evidence that retrying cannot advance progress
    /// again.
    #[allow(clippy::future_not_send)]
    pub async fn partition_read(
        &self,
        namespace: IggyNamespace,
        read: PartitionRead,
    ) -> Option<PartitionReadReply> {
        let target = self.shards_table.shard_for(namespace).or_else(|| {
            matches!(read, PartitionRead::SessionRetired { .. }).then(|| {
                crate::shards_table::calculate_shard_assignment(&namespace, self.shard_count)
            })
        });
        let Some(target) = target else {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "partition_read: namespace not routable (not materialised yet or deleted)"
            );
            return Some(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        };
        let (reply_tx, reply_rx) = channel::<PartitionReadReply>(1);
        let frame = ShardFrame::lifecycle(LifecycleFrame::PartitionRead {
            namespace,
            read,
            reply: reply_tx,
        });
        let Some(sender) = self.senders.get(target as usize) else {
            return Some(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        };
        if let Err(error) = sender.try_send(frame) {
            tracing::warn!(
                shard = self.id,
                target,
                "partition_read: inbox rejected PartitionRead frame: {error:?}"
            );
            return Some(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
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

    /// Admit a partition write on the shard owning `namespace`. Routes through
    /// the shards table exactly like [`Self::partition_read`], self-sends
    /// included, so a locally-owned partition takes the same path.
    ///
    /// Synchronous up to the inbox `try_send`, so two writes a caller admits
    /// back to back reach the owning shard in that order; the committed reply
    /// is awaited separately through [`Self::await_partition_submit`], which a
    /// connection's drain loop spawns rather than blocks on.
    ///
    /// # Errors
    /// [`PartitionSubmitRefused`] when the frame provably never reached the
    /// owning shard (no sender for the target, or a full inbox), so the caller
    /// can deny the client outright instead of leaving it to a read-timeout
    /// for an outcome that is already known.
    pub fn partition_submit(
        &self,
        namespace: IggyNamespace,
        request: Message<RoutedRequestHeader>,
    ) -> Result<PartitionSubmitTicket, PartitionSubmitRefused> {
        self.partition_submit_attached(namespace, request, None)
    }

    /// Submit an offset write with its parent consumer's admission fence.
    ///
    /// # Errors
    /// Returns [`PartitionSubmitRefused`] before admission when the inbox is unavailable.
    pub fn partition_submit_attached(
        &self,
        namespace: IggyNamespace,
        request: Message<RoutedRequestHeader>,
        attachment: Option<ConsumerAttachment>,
    ) -> Result<PartitionSubmitTicket, PartitionSubmitRefused> {
        let target = self.shards_table.shard_for(namespace).unwrap_or_else(|| {
            // Same fallback as `route_typed`: a miss means "not seeded yet",
            // not "unroutable", and the owning shard parks what arrives early.
            crate::shards_table::calculate_shard_from_consensus_ns(
                namespace.inner(),
                self.shard_count,
            )
        });
        let (reply_tx, reply_rx) = channel::<Option<Message<GenericHeader>>>(1);
        let frame = ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
            request,
            reply: reply_tx,
            attachment,
        });
        let Some(sender) = self.senders.get(target as usize) else {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::metrics::frame_drop_reason::UNROUTABLE,
            );
            return Err(PartitionSubmitRefused);
        };
        if let Err(error) = sender.try_send(frame) {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::coordinator::classify_try_send_err(&error),
            );
            tracing::warn!(
                shard = self.id,
                target,
                "partition_submit: inbox rejected PartitionSubmit frame: {error:?}"
            );
            return Err(PartitionSubmitRefused);
        }
        Ok(PartitionSubmitTicket {
            receiver: reply_rx,
            target,
        })
    }

    /// Wait out a submitted write's committed reply.
    ///
    /// `None` = reply channel dropped before a reply (view-change reset, park
    /// eviction, shutdown) or budget expiry. The outcome is unknown, so a
    /// synthesized failure could contradict a write that commits moments
    /// later: a caller that answers a client sends `TransientNotCommitted`,
    /// which licenses only a replay of the same request id. Both exits count under
    /// `frame_drops_total{variant=partition}` with their own reasons. The
    /// timeout warning fires once per stall episode, reset by the next reply
    /// that does arrive: one wedged group would otherwise log a line per
    /// request, and the counter carries the volume.
    #[allow(clippy::future_not_send)]
    pub async fn await_partition_submit(
        &self,
        ticket: PartitionSubmitTicket,
    ) -> Option<Message<GenericHeader>> {
        let PartitionSubmitTicket { receiver, target } = ticket;
        match bus_timeout(&self.bus, PARTITION_SUBMIT_TIMEOUT, receiver.recv()).await {
            Some(Ok(Some(reply))) => {
                self.partition_submit_stalled.set(false);
                Some(reply)
            }
            Some(Ok(None) | Err(_)) => {
                self.metrics.record_frame_drop(
                    crate::metrics::frame_drop_variant::PARTITION,
                    crate::metrics::frame_drop_reason::SUBMIT_ABANDONED,
                );
                tracing::debug!(
                    shard = self.id,
                    target,
                    "partition_submit: reply channel dropped before commit"
                );
                None
            }
            None => {
                self.metrics.record_frame_drop(
                    crate::metrics::frame_drop_variant::PARTITION,
                    crate::metrics::frame_drop_reason::SUBMIT_TIMEOUT,
                );
                if self.partition_submit_stalled.replace(true) {
                    tracing::debug!(
                        shard = self.id,
                        target,
                        "partition_submit: owning shard did not reply within budget"
                    );
                } else {
                    tracing::warn!(
                        shard = self.id,
                        target,
                        "partition_submit: owning shard did not reply within budget; \
                         further expiries log at debug until a reply arrives"
                    );
                }
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

    /// The adapter this shard installs on its delegated client connections,
    /// for the transports shard 0 terminates locally. What every transport
    /// on a shard must share is the host behind it (the per-client queues
    /// and the disconnect hook live there); a second adapter over the same
    /// host would be harmless, this one just saves building it.
    #[must_use]
    pub fn client_request_handler(&self) -> RequestHandler {
        Rc::clone(&self.on_client_request)
    }

    /// Create a shard without inter-shard channels or delegated connections.
    ///
    /// Useful for the simulator where inbound messages are delivered
    /// directly via [`on_message`](Self::on_message) instead of the TCP /
    /// fd-transfer path. Hosted by [`NoopHost`] because the simulator never
    /// receives a connection-setup or host-bound frame.
    ///
    /// # Panics
    /// Panics when the target cannot address the largest supported partition I/O job.
    #[must_use]
    pub fn without_inbox(
        identity: ShardIdentity,
        bus: B,
        metadata: IggyMetadata<VsrConsensus<B>, MJ, S, M, SB>,
        partitions: IggyPartitions<B, SB>,
        shards_table: T,
        partition_consensus: PartitionConsensusConfig<B>,
    ) -> Self {
        // Direct owner tests can keep one disk poll outstanding. Tests needing
        // concurrent reads construct a lane with their chosen capacity.
        const POLL_COMPLETION_CAPACITY: usize = 1;

        // Placeholder lanes: the simulator delivers frames straight to
        // `on_message` (see the `shard_count` note below), so nothing ever
        // sends here and capacity 1 exists only to satisfy the fields. The
        // real lanes are bounded on purpose (`inbox_capacity` /
        // `reply_inbox_capacity` are the shard's backpressure), so no
        // unbounded variant is wanted here either.
        let (_tx, inbox) = channel(1);
        let (_reply_tx, reply_inbox) = channel(1);
        let metrics = crate::metrics::ShardMetrics::for_shard();
        let nonce_seed = boot_nonce(metadata.consensus.as_ref());
        // Metadata admits a stream's wrap set with the keys its partitions open.
        let metadata = metadata.with_stream_key_context(partitions.key_context());
        let plane = MuxPlane::new(variadic!(metadata, partitions));
        let ShardIdentity { id, name } = identity;
        let host: Rc<dyn ShardHost> = Rc::new(NoopHost);
        Self {
            id,
            name,
            bus,
            on_replica_message: host::replica_message_handler(&host),
            on_client_request: host::client_request_handler(&host),
            host,
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
            reply_inbox,
            poll_completions: poll::completion::PollCompletionLane::new(
                POLL_COMPLETION_CAPACITY,
                &metrics,
            ),
            partition_io: partition_io::PartitionIoLane::new(
                PartitionIoLimits::new(partition_io::DEFAULT_PARTITION_IO_CAPACITY, None)
                    .expect("simulator supports the production partition I/O limits"),
                &metrics,
            ),
            cooperative_events: Cell::new(0),
            shards_table,
            partition_consensus,
            metrics,
            metadata_tick_handler: RefCell::new(None),
            reconcile_queue: RefCell::new(VecDeque::new()),
            shutting_down: Cell::new(false),
            pending_partition_frames: RefCell::new(BTreeMap::new()),
            parked_partition_bytes: Cell::new(0),
            redispatch_queue: RefCell::new(VecDeque::new()),
            shard_park_shedding: Cell::new(false),
            partition_submit_stalled: Cell::new(false),
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            metadata_gap_ticks: Cell::new(0),
            metadata_walk_stuck_op: Cell::new(0),
            state_transfer_offers: RefCell::new(HashMap::new()),
            partition_offer_builds: RefCell::new(HashMap::new()),
            served_segment_cache: RefCell::new(ServedSegmentCache::default()),
            register_forwards: RefCell::new(HashMap::new()),
            logout_forwards: RefCell::new(HashMap::new()),
            segment_delete_forwards: RefCell::new(HashMap::new()),
            forward_nonce: Cell::new(nonce_seed),
            served_segment_cache_bytes_max: Cell::new(SERVED_SEGMENT_CACHE_BYTES_DEFAULT),
            partition_artifact_len_max: Cell::new(PARTITION_ARTIFACT_LEN_DEFAULT),
            repair_chunk_max: Cell::new(REPAIR_CHUNK_MAX),
            repair_retry_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            repair_gap_debounce_ticks: Cell::new(partitions::REPAIR_RETRY_TICKS),
            partition_repairs_inflight: Cell::new(0),
            partition_walk_cursor: Cell::new(None),
            partition_placement_cursor: Cell::new(None),
            partition_placement_owed: Cell::new(0),
            primary_rebalance: RefCell::default(),
            superblock_wedged_fatal_failures: Cell::new(0),
            bus_max_message_size: Cell::new(DEFAULT_BUS_MAX_MESSAGE_SIZE),
            metadata_transfer_attempts: Cell::new(0),
            metadata_repair_attempts: Cell::new((0, 0)),
            metadata_repair_conflicts: Cell::new(0),
            metadata_repair_conflict_warn: Cell::default(),
            incarnation_mismatch_warns: RefCell::default(),
            ticks: Cell::new(0),
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

    /// Attach the sender mesh to a shard built by [`Self::without_inbox`], which
    /// leaves it empty.
    ///
    /// Exists for out-of-crate tests: the paths that hand work back to the pump
    /// (`stage_transient_deny`, the parked-frame re-dispatch) index
    /// `senders[self.id]`, so without a mesh they silently no-op and a test
    /// asserting on them proves nothing. The caller must keep the paired
    /// receivers alive; dropping one turns every `try_send` into `Disconnected`.
    ///
    /// Whole mesh, not one sender: consumers index by shard id and
    /// `forward_metadata_submit` indexes `senders[0]` unconditionally, so a
    /// one-element vec is correct only for shard 0. `shard_count` tracks it, as
    /// in both constructors.
    ///
    /// # Panics
    /// If the mesh is not ordered `senders[i].shard_id() == i` or does not cover
    /// this shard. Either routes frames to the wrong pump.
    #[cfg(any(test, feature = "simulator"))]
    pub fn attach_senders(&mut self, senders: Vec<TaggedSender>) {
        assert!(
            (self.id as usize) < senders.len(),
            "attach_senders: mesh of {} does not cover shard {}",
            senders.len(),
            self.id
        );
        validate_sender_ordering(&senders).expect("attach_senders: mesh must be ordered by shard");
        self.shard_count = u32::try_from(senders.len()).expect("shard count fits u32");
        self.senders = senders;
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
        self.wake_reconcile_apply();
    }

    /// Wake the pump after off-pump work becomes visible. A refused marker is
    /// safe because a full inbox has already woken the pump, whose frame and
    /// tick arms both apply staged reconciliation work.
    fn wake_reconcile_apply(&self) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply));
    }

    /// `true` when an `InsertOwned` for `namespace` is built and queued but not
    /// yet applied.
    ///
    /// The reconciler's own "already handled" test is `IggyPartitions::contains`,
    /// which only turns true once the pump applies, so without this a pass run
    /// during that lag rebuilds a namespace an earlier pass already built. The
    /// queue IS the record of that in-flight work, so asking it cannot drift
    /// from reality the way a parallel set would: every op leaves the queue
    /// through `apply_reconcile_ops`, which either inserts or discards.
    ///
    /// Deliberately blind to `epoch`. Matching it would let a delete + recreate
    /// landing inside the lag build a second incarnation over the queued one's
    /// on-disk path, which is the case this exists to prevent; the recreate is
    /// not lost, it costs one pass. The queued (dead-epoch) op applies, and the
    /// next pass reads the epoch mismatch off the routing row and takes the
    /// stale-incarnation teardown into a clean rebuild.
    pub fn has_staged_insert_owned(&self, namespace: IggyNamespace) -> bool {
        self.reconcile_queue.borrow().iter().any(|op| {
            matches!(
                op,
                ReconcileOp::InsertOwned {
                    namespace: staged_namespace,
                    ..
                } if *staged_namespace == namespace
            )
        })
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
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::metrics::frame_drop_reason::UNROUTABLE,
            );
            return;
        };
        // Fire-and-forget: a refused pass is picked up by the cleaner's next
        // maintenance tick, so the drop only has to be visible, not recovered.
        if let Err(error) = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::CleanPartition {
            namespace,
            now,
            message_expiry,
            max_bytes,
        })) {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::coordinator::classify_try_send_err(&error),
            );
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "segment cleaner pass refused by own inbox: {error:?}"
            );
        }
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

    /// Drain and apply staged [`ReconcileOp`]s on the pump task.
    ///
    /// Synchronous: every arm is in-memory only. `ConfirmRemove`'s fsync and
    /// blocking close are offloaded to a detached task so the pump does not
    /// stall on bulk teardown. An `InsertOwned` can stage parked frames, so a
    /// live-pump caller must return to the ranked redispatch select arm before
    /// reading the inbox again. The graceful-shutdown caller drains the queue
    /// explicitly because it has already left the select loop.
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
                    // The shutdown drain owns a fixed set of partition owners.
                    if self.shutting_down.get() {
                        continue;
                    }
                    // Idempotent apply, mirroring `ConfirmRemove` (idempotent
                    // via `remove`'s `None` early-return). An unconditional
                    // `insert` over a live namespace would push a duplicate
                    // partition and overwrite the `ns -> idx` entry, orphaning
                    // the first: its VSR group + segment writers leak and `len`
                    // inflates.
                    //
                    // A backstop, not the mechanism. `reconcile_additions`
                    // skips a namespace whose `InsertOwned` is already staged
                    // ([`Self::has_staged_insert_owned`]), so a second op for a
                    // live namespace should not be built at all. Dropping one
                    // here is damage control rather than a free no-op: the
                    // build already planted its initial segment over the live
                    // incarnation's path and folded that into the namespace's
                    // shared stats.
                    if partitions.contains(&namespace) {
                        tracing::error!(
                            shard = self_shard_id,
                            ns_raw = namespace.inner(),
                            epoch,
                            "discarding duplicate InsertOwned for a live namespace: the \
                             staged-op guard was bypassed and the build re-planted segment 0 \
                             over the live incarnation's path"
                        );
                        self.metrics.record_duplicate_partition_build_discarded();
                        drop(partition);
                        continue;
                    }
                    // A standing tombstone is a damage verdict or an
                    // unfinished teardown, and it lifts only through
                    // `ConfirmRemove` below, i.e. proof the disk delete
                    // completed. Inserting would route the namespace while
                    // the verdict stands: the plane drops requests for
                    // tombstoned namespaces without replying, so clients
                    // would hang to their read timeout over data declared
                    // lost. The reconciler skips tombstoned namespaces
                    // before building, so reaching this means the op was
                    // staged before the fence landed. Damage control like
                    // the guard above: the build already planted its
                    // initial segment on disk.
                    if partitions.is_tombstoned(&namespace) {
                        tracing::error!(
                            shard = self_shard_id,
                            ns_raw = namespace.inner(),
                            epoch,
                            "discarding InsertOwned for a tombstoned namespace: the \
                             tombstone lifts only via ConfirmRemove, never by routing a \
                             fresh build over it"
                        );
                        drop(partition);
                        continue;
                    }
                    partitions.insert(namespace, *partition);
                    self.shards_table.insert(
                        namespace,
                        PartitionLocation::new(ShardId::new(self_shard_id), epoch),
                    );
                    self.metrics.record_partition_materialised();
                    self.redispatch_parked_frames(namespace, epoch);
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
                    // before this op was enqueued -- by the reconciler on a real
                    // delete, by `fence_partition_for_rebuild` when a partition
                    // is retired for rebuild -- so no in-flight frame can reach
                    // the partition between `remove` and the drop here. On the
                    // delete path teardown already unlinked the on-disk hierarchy
                    // via `delete_partitions_from_disk`, so the partition drops
                    // inline: its compio file handles close through io_uring
                    // without blocking, and no fsync is wanted on data that is
                    // already gone.
                    let removed = partitions.remove(&namespace);
                    partitions.untombstone(&namespace);
                    // A topic created then deleted before its `InsertOwned`
                    // pass never drains parked frames the normal way; reclaim
                    // them here so they cannot leak across many create-delete
                    // races (the partition is gone, so the frames are moot).
                    self.discard_parked_partition_frames(namespace);
                    self.metrics.record_partition_removed();
                    confirmed_remove = true;
                    if let Some(partition) = removed {
                        // Tail of the gap-drop count. The tick sweep drains it
                        // per pass, but `get_by_ns` stops answering the moment
                        // the reconciler tombstones the namespace, so whatever
                        // the last pass before the tombstone left would go to
                        // the floor with the partition value.
                        self.metrics
                            .record_partition_prepare_gap_drops(partition.take_prepare_gap_drops());
                        self.metrics.record_partition_applied_log_contradictions(
                            partition.consensus().take_applied_log_contradictions(),
                        );
                        // Roll whatever this partition still counts out of its
                        // parent topic and stream. Here and not in the
                        // reconciler's teardown: a handler suspended mid-append
                        // holds its own `Arc` past the tombstone, and an earlier
                        // settle leaves its increment in the parents with the
                        // partition already gone. This is the drop point, so
                        // nothing can add through that handle afterwards. The
                        // rollback clamps, so the usual case -- the metadata
                        // apply already zeroed these counters at commit -- takes
                        // nothing.
                        partition.stats.zero_out_all();
                    } else {
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

/// The serving replica's `(view, commit_max)` for a descriptor.
///
/// Sampled per branch, always AFTER any offer build: the build force-flushes and
/// hashes a budgeted slice of the un-memoized segments (a first multi-GiB
/// serve takes several rounds to complete an offer at all) while
/// reading its `commit_op` post-flush, so a pre-build sample could advertise a
/// `commit_max` below the descriptor's own `commit_op`. Harmless on the receiver
/// (the values are only compared against its own locals) but it makes its gate
/// refuse, and refusals feed a backoff.
const fn serving_progress<B, SB>(partition: &IggyPartition<B, SB>) -> (u32, u64)
where
    B: MessageBus,
    SB: SuperblockStore,
{
    (
        partition.consensus().view(),
        partition.consensus().commit_max(),
    )
}

/// The next replica to try after a transfer against `failed_peer` failed.
///
/// Prefers the view's primary: it is the only replica that can pass the serving
/// side's primary gate, so rotating by ring index alone can spend a
/// full backoff round on a backup that must refuse -- and, worse, can land on a
/// phantom view-0 primary of an empty group. Falls back to the ring step of
/// [`next_repair_peer`].
const fn next_transfer_peer(self_id: u8, failed_peer: u8, replica_count: u8, primary: u8) -> u8 {
    if replica_count > 1 && primary != self_id && primary != failed_peer {
        return primary;
    }
    next_repair_peer(self_id, failed_peer, replica_count)
}

/// The next replica to repair from after `failed_peer` could not serve: one
/// step around the ring, skipping this replica. A cluster of two has no
/// alternative and names the same peer back.
///
/// Never jumps back to the primary as [`next_transfer_peer`] does: any replica
/// holding the ops serves journal repair, and returning to the primary after
/// each backup alternates between two peers, so in a group of five the others
/// would never be asked.
///
/// `failed_peer` is bounded by `replica_count` at its ingress, which is what
/// keeps the `+ 1` here from wrapping a peer id of 255 onto replica 0.
const fn next_repair_peer(self_id: u8, failed_peer: u8, replica_count: u8) -> u8 {
    if replica_count <= 1 {
        return failed_peer;
    }
    let mut candidate = (failed_peer + 1) % replica_count;
    if candidate == self_id {
        candidate = (candidate + 1) % replica_count;
    }
    if candidate == self_id {
        failed_peer
    } else {
        candidate
    }
}

/// The primary to transfer from once journal repair stalled on the whole
/// ring, or `None` while the spent session rotates as before.
///
/// Called when a session spent its stall budget. The ring is spent once the
/// rotations since the last progress, with this session, reach the number of
/// other replicas. Only a rotation that opened a session counts. Each rotation
/// steps one replica around the ring ([`next_repair_peer`]), so a lap from the
/// primary asks every other replica once. That still counts sessions, not
/// peers: when a rotation cannot open its session, the detector re-arms against
/// the primary and the walk starts over without restarting the count.
///
/// Only the primary serves a partition transfer, so only a backup in `Normal`
/// that misses committed ops converts: a primary-elect repairs toward its
/// merged log, and a transfer carries no uncommitted suffix. A transfer stage,
/// a transfer session or a scheduled re-arm owns recovery already. A solo
/// group has no backup, so it never converts.
fn spent_repair_ring_transfer_peer<B, SB>(partition: &IggyPartition<B, SB>) -> Option<u8>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    let consensus = partition.consensus();
    let other_replicas = u32::from(consensus.replica_count().saturating_sub(1));
    (partition.repair_rotations().saturating_add(1) >= other_replicas
        && consensus.is_normal_backup()
        && consensus.commit_min() < consensus.commit_max()
        && partition.transfer.is_none()
        && partition.transfer_rearm.is_none())
    .then_some(consensus.primary_index(consensus.view()))
}

/// Consecutive transient refusals before the re-arm starts logging at `error`,
/// and the interval it re-logs at afterwards. Sized so a peer that is briefly
/// behind stays quiet while a partition that never rejoins becomes loud.
const TRANSFER_REFUSALS_BEFORE_ESCALATION: u32 = 10;

/// Exponential re-arm backoff, scaled by the consecutive-failure count and
/// capped at 1024x the base so a long outage settles into a slow poll
/// instead of climbing forever.
fn transfer_rearm_backoff(base_ticks: u32, failures: u32) -> u32 {
    base_ticks.saturating_mul(1 << failures.min(10))
}

/// Split a handler's action list into `(local, wire)`. A failed superblock
/// persist must fence only the WIRE sends: the local actions -- pipeline
/// rebuild, commit walk -- flip no externally visible view state, and
/// dropping them can wedge the group permanently. Concretely,
/// `complete_view_change_as_primary` clears its pipeline before emitting
/// `RebuildPipeline`; dropping that rebuild leaves a primary that drops
/// every backup `PrepareOk` for the orphaned window as `UnknownPrepare`, and
/// once the persist heals (backoff ceiling ~1s) the probing backups adopt
/// this primary and stop escalating, so the 5s election that would rescue
/// the group never fires -- writes are accepted and never commit again. The
/// DVC quorum latch does not re-emit on retried DVCs, making the drop
/// permanent. A short persist hiccup must not be worse than a sustained
/// outage.
///
/// Partition-plane callers must route BOTH halves through BOTH dispatchers:
/// the partition `RebuildPipeline` executes in
/// `dispatch_partition_journal_actions` (its journal lives on the
/// partition), while `dispatch_vsr_actions` runs it only for the metadata
/// plane -- locals sent to one dispatcher alone silently skip the rebuild.
fn split_local_actions(actions: Vec<VsrAction>) -> (Vec<VsrAction>, Vec<VsrAction>) {
    actions.into_iter().partition(|action| {
        matches!(
            action,
            VsrAction::RebuildPipeline { .. } | VsrAction::CommitJournal
        )
    })
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
    /// Namespace is unmaterialised and its park buffer is at capacity. Client
    /// requests must be denied with a transient status: the frame is gone, and
    /// silence would leave a lockstep transport waiting out its response
    /// read-timeout. Replicated traffic is dropped and recovers through either
    /// retransmit or the same-view repair armed by a later commit heartbeat.
    Overflow(Message<H>),
}

/// A partition frame held until its namespace materialises.
///
/// `epoch` is the committed `created_revision` observed when the frame was
/// parked, or `None` when the namespace had no committed partition to read one
/// from. Delete + recreate recycles the slab keys, so the namespace alone cannot
/// distinguish incarnations: without this stamp a frame parked against the dead
/// incarnation would be drained into its replacement by `InsertOwned` and
/// served, because `serves_committed_incarnation` compares the committed
/// revision against the routing row - both of which describe the NEW
/// incarnation - and never the frame's provenance.
struct ParkedFrame {
    epoch: Option<u64>,
    /// Reconciler passes survived. `reconcile_parked_frames` increments it and
    /// answers CLIENT REQUESTS past [`MAX_PARKED_PASSES`], in units the
    /// simulator's virtual clock controls.
    ///
    /// Never expires a replicated prepare: no client can retry it, and forcing
    /// the same-view repair backstop to recover a gap is worse than retaining
    /// the frame. Byte budgets bound those instead.
    ///
    /// Bounds RESIDENCY, not staleness. The SDK replays the identical request
    /// for the rest of its response timeout, so an absolute-offset
    /// `StoreConsumerOffset` rewinds the group on the replay anyway. What it
    /// buys: a buffer that cannot grow without limit, and a client that learns
    /// the outcome from a reply rather than a timeout.
    passes: u32,
    message: Message<GenericHeader>,
    /// Channel the committed reply travels back on, for a frame that arrived
    /// as a [`LifecycleFrame::PartitionSubmit`]. `None` for replicated
    /// prepares and for writes admitted without a waiter. Dropping the frame
    /// (expiry, teardown, shutdown) drops this, which wakes the awaiting
    /// dispatch with a receive error it maps to silence.
    reply: Option<Sender<Option<Message<GenericHeader>>>>,
    /// Revalidated at admission after the partition materializes.
    attachment: Option<ConsumerAttachment>,
}

impl ParkedFrame {
    fn footprint(&self) -> usize {
        parked_footprint(self.message.as_slice().len())
    }

    /// No client on this node: nothing to answer, nothing recovers it.
    fn is_replicated(&self) -> bool {
        self.message.header().command != Command::Request
    }
}

/// One staged frame classified for re-delivery.
///
/// `reply` is the submit channel the frame parked with, if any: it decides which
/// admission path the pump re-enters, since a submit's committed reply cannot be
/// routed by `header.client` (that field is the VSR consensus id).
struct RedispatchedFrame {
    message: MessageBag,
    provenance: ParkProvenance,
    reply: Option<Sender<Option<Message<GenericHeader>>>>,
    attachment: Option<ConsumerAttachment>,
}

/// What a frame keeps if it parks again after the pump re-delivers it.
///
/// Production prevents that race by ranking redispatch above inbox work and by
/// applying reconcile operations only on the pump. Carrying the original stamp
/// is defence in depth for off-pump staging such as simulator materialisation.
/// There, re-deriving on a second park could stamp the frame with a replacement
/// incarnation. `None` also stays `None`, since absence of a committed revision
/// is not evidence that the frame belongs to a prior incarnation.
#[derive(Clone, Copy)]
struct ParkProvenance {
    epoch: Option<u64>,
    passes: u32,
}

/// One namespace's parked frames plus their running footprint.
///
/// Carried, not re-summed: `park_if_unmaterialised` reads it per arriving frame
/// over an entry up to [`MAX_PARKED_PER_NAMESPACE`] deep, so a rescan makes
/// admission quadratic in the depth it exists to bound.
#[derive(Default)]
struct ParkEntry {
    frames: Vec<ParkedFrame>,
    bytes: usize,
    /// Frames shed since the entry was created. Only the first warns.
    shed: u64,
}

impl ParkEntry {
    fn push(&mut self, frame: ParkedFrame) {
        self.bytes = self.bytes.saturating_add(frame.footprint());
        self.frames.push(frame);
    }

    /// Remove the selected frames, returning them and the footprint freed so the
    /// caller can debit the shard-wide total.
    fn extract(
        &mut self,
        predicate: impl FnMut(&mut ParkedFrame) -> bool,
    ) -> (Vec<ParkedFrame>, usize) {
        let taken: Vec<ParkedFrame> = self.frames.extract_if(.., predicate).collect();
        let freed: usize = taken.iter().map(ParkedFrame::footprint).sum();
        self.bytes = self.bytes.saturating_sub(freed);
        (taken, freed)
    }
}

/// Per-namespace ceiling on parked CLIENT REQUESTS.
///
/// Requests only, like the byte budgets: a header-only frame charges
/// [`MESSAGE_ALIGN`], so 128 of them is 512 KiB against a 4 MiB per-namespace
/// budget. Applied to prepares this would be the binding constraint for every
/// footprint under 32 KiB and would shed them long before any byte budget could,
/// which is the loss class the split exists to remove. A prepare is bounded by
/// [`MAX_PARKED_BYTES_PER_NAMESPACE`] instead: 1024 header-only frames.
const MAX_PARKED_PER_NAMESPACE: usize = 128;

/// Shard-wide ceiling on parked bytes, measured as resident footprint (see
/// [`parked_footprint`]).
///
/// The per-namespace cap counts frames, and `Message::into_generic` is a retag
/// rather than a copy, so each entry retains its whole buffer -- up to
/// `message_bus::framing::MAX_MESSAGE_SIZE` (64 MiB). Frames alone therefore
/// bound nothing useful: 128 × 64 MiB is 8 GiB for a single namespace, and
/// nothing capped the namespace count. This is the budget that actually bounds
/// residency, so a burst against many un-materialised namespaces sheds instead
/// of exhausting the host.
///
/// Deliberately well below `MAX_MESSAGE_SIZE`. Sized equal to it, one legal
/// max-size frame consumes the entire shard-wide budget and head-of-line-blocks
/// every other namespace's convergence window.
const MAX_PARKED_BYTES: usize = 16 * 1024 * 1024;

/// Per-namespace ceiling on parked bytes, so one un-materialised namespace
/// cannot spend the whole shard's budget and shed everyone else's frames.
///
/// Applied only to an entry that already holds something. Sized against an
/// empty entry a larger frame could never park at all. For a prepare, shedding
/// also forces a later commit heartbeat to discover the gap and run same-view
/// repair. Shipped `message_bus.max_message_size` is 64 MiB, so an ordinary
/// batched append exceeds this. The waiver costs one convergence window of
/// shard budget and avoids unnecessary recovery work.
const MAX_PARKED_BYTES_PER_NAMESPACE: usize = MAX_PARKED_BYTES / 4;

/// Resident cost of parking a frame of `len` bytes.
///
/// A parked frame retains its whole [`server_common::iobuf`] buffer, which is
/// allocated at [`MESSAGE_ALIGN`] granularity, so a 256-byte frame occupies
/// 4 KiB. Charging the logical length instead under-counts RSS by up to 16x for
/// header-only frames, which would let an accounted 16 MiB grow to ~256 MiB
/// resident per shard.
const fn parked_footprint(len: usize) -> usize {
    len.next_multiple_of(MESSAGE_ALIGN)
}

/// Whether consecutive superblock write failures crossed the fail-stop bound.
/// `fatal_after == 0` disables the fail-stop.
const fn superblock_wedged(failures: u64, fatal_after: u64) -> bool {
    fatal_after != 0 && failures >= fatal_after
}

/// Reconciler passes a frame may survive before it is answered rather than held.
///
/// Passes, not seconds, and deliberately not described in seconds: a pass fires
/// on the periodic interval OR on a commit-tick wake, so the wall-clock window
/// this maps to spans orders of magnitude. `reconcile_periodic_interval` legally
/// reaches 30s, which would put four passes at 120s -- four times the SDK's
/// response read-timeout, so the client times out first and the bound stops
/// being the thing that answers it. Commit-tick wakes collapse it the other way,
/// to tens of milliseconds. It bounds residency in units the simulator's virtual
/// clock governs; it is not a latency guarantee.
///
/// TODO(krishna): derive this from `reconcile_periodic_interval` and the SDK
/// response timeout so the bound tracks the configured interval instead of
/// assuming one.
const MAX_PARKED_PASSES: u32 = 3;

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
    ///
    /// Takes the bag `IggyShard::dispatch` classified, so the frame is parsed
    /// once per hop rather than once for routing and again for dispatch.
    #[allow(clippy::future_not_send)]
    pub async fn on_message(&self, message: MessageBag)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        self.dispatch_message(message, None).await;
    }

    /// Remove and classify one staged frame for the pump's ranked redispatch
    /// arm. The queue borrow ends before dispatch awaits, so simulator
    /// materialisation can append off-pump without colliding with a suspended
    /// `RefCell` guard.
    fn pop_redispatched_frame(&self) -> Option<RedispatchedFrame> {
        loop {
            let ParkedFrame {
                epoch,
                passes,
                message,
                reply,
                attachment,
            } = self.redispatch_queue.borrow_mut().pop_front()?;
            let provenance = ParkProvenance { epoch, passes };
            // Parked frames are stored generic (the buffer holds every variant
            // in one Vec), so re-entering the pump costs one classify. That is
            // the rare path - a post-`CreateTopic` convergence window, not the
            // per-message steady state the bag handoff exists for.
            match MessageBag::try_from(message) {
                Ok(message) => {
                    return Some(RedispatchedFrame {
                        message,
                        provenance,
                        reply,
                        attachment,
                    });
                }
                Err(error) => {
                    // The frame classified once already, on the way in, so this
                    // is unreachable short of memory corruption. The consumed
                    // bytes can no longer produce a client deny, but the drop
                    // still needs the same operator-visible record as any other
                    // parked frame retired unserved.
                    self.metrics.record_frame_drop(
                        crate::metrics::frame_drop_variant::PARTITION,
                        crate::metrics::frame_drop_reason::PARK_DROPPED,
                    );
                    tracing::error!(
                        shard = self.id,
                        %error,
                        "re-dispatched partition frame no longer classifies; dropping it"
                    );
                }
            }
        }
    }

    /// Deliver one staged frame, through the admission path it arrived on.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn dispatch_redispatched_frame(&self, frame: RedispatchedFrame)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        let RedispatchedFrame {
            message,
            provenance,
            reply,
            attachment,
        } = frame;
        match (reply, message) {
            (Some(reply), MessageBag::Request(request)) => {
                self.dispatch_partition_submit(request, reply, Some(provenance), attachment)
                    .await;
            }
            (Some(_), _) => {
                // Only a client request parks with a waiter attached, so this is
                // unreachable short of a classify that disagrees with the one
                // the frame passed on the way in. Dropping the sender wakes the
                // awaiting shard, which maps the receive error to silence.
                tracing::error!(
                    shard = self.id,
                    "staged partition frame carries a reply channel but is not a client request; \
                     dropping it"
                );
            }
            (None, message) => self.dispatch_message(message, Some(provenance)).await,
        }
    }

    /// Test-only delivery of one staged frame. Production obtains frames through
    /// the router's ranked select arm, which also processes loopback after each
    /// one. This hook exists for the reconciler's defence-in-depth interleaving.
    #[cfg(feature = "simulator")]
    #[allow(clippy::future_not_send)]
    pub async fn dispatch_one_redispatched_frame_for_test(&self) -> bool
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        let Some(frame) = self.pop_redispatched_frame() else {
            return false;
        };
        self.dispatch_redispatched_frame(frame).await;
        true
    }

    /// Retire staged frames the pump is no longer going to deliver, on its way
    /// out. Client requests get a transient deny; the rest are counted as drops,
    /// which is the only record a replicated frame leaves.
    fn retire_redispatched_frames(&self) {
        let staged: Vec<ParkedFrame> = self.redispatch_queue.borrow_mut().drain(..).collect();
        if staged.is_empty() {
            return;
        }
        let (answered, dropped) = self.retire_parked_frames(staged);
        tracing::debug!(
            shard = self.id,
            answered,
            dropped,
            "retiring re-dispatched partition frames the pump will not deliver"
        );
    }

    /// [`Self::on_message`] carrying the park provenance of a frame the pump is
    /// re-delivering, so a second park keeps the stamp and age the first one
    /// derived instead of deriving them again against newer committed state.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn dispatch_message(&self, message: MessageBag, provenance: Option<ParkProvenance>)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        self.fence_rebalance_authority(&message);
        match message {
            MessageBag::PrimaryRebalance(message) => {
                self.on_primary_rebalance(*message.header()).await;
            }
            MessageBag::Request(request) => {
                // One header read for the pair; `header()` casts on every call.
                let routing = {
                    let header = request.header();
                    (header.operation, header.group)
                };
                // Ahead of the park and the incarnation fence, which both read
                // the frame as a partition request.
                if !routing.0.is_plane_routable() {
                    self.deny_unroutable_request(request.header()).await;
                    return;
                }
                match self.park_if_unmaterialised(
                    request, routing.0, routing.1, provenance, &mut None, &mut None,
                ) {
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
                    ParkOutcome::Tombstoned(request) | ParkOutcome::Overflow(request) => {
                        self.deny_partition_request_transient(request.header())
                            .await;
                    }
                    ParkOutcome::Parked => {}
                }
            }
            MessageBag::Prepare(prepare) => {
                let routing = {
                    let header = prepare.header();
                    (header.operation, header.group, header.op)
                };
                if !routing.0.is_plane_routable() {
                    self.drop_unroutable_replicated(
                        Command::Prepare,
                        routing.0,
                        routing.1,
                        routing.2,
                    );
                    return;
                }
                // A tombstoned prepare still flows to the plane: replicated
                // traffic has no client awaiting a reply on this node, and
                // the plane's own tombstone guard drops it.
                match self.park_if_unmaterialised(
                    prepare, routing.0, routing.1, provenance, &mut None, &mut None,
                ) {
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
                            // Same transfer gate as the view-change walks: a
                            // walk during a transfer can advance commit_min
                            // past the incoming frontier and trip the
                            // install's StaleTransfer refusal after the full
                            // pull. This is the highest-frequency walk (one
                            // per replicated prepare), so it needs the gate
                            // most.
                            if let Some(partition) = planes.1.0.get_mut_by_ns(&namespace)
                                && partition.consensus().is_follower()
                                && !partition.consensus().is_transferring()
                            {
                                partition.commit_journal(config).await;
                            }
                        }
                    }
                    // Shed under a full park buffer, or parked. Either way there
                    // is no client awaiting a reply on this node; the primary's
                    // retransmit redelivers.
                    ParkOutcome::Overflow(_) | ParkOutcome::Parked => {}
                }
            }
            MessageBag::PrepareOk(prepare_ok) => {
                let routing = {
                    let header = prepare_ok.header();
                    (header.operation, header.group, header.op)
                };
                if !routing.0.is_plane_routable() {
                    self.drop_unroutable_replicated(
                        Command::PrepareOk,
                        routing.0,
                        routing.1,
                        routing.2,
                    );
                    return;
                }
                self.on_ack(prepare_ok).await;
            }
            MessageBag::StartViewChange(msg) => self.on_start_view_change(msg).await,
            MessageBag::DoViewChange(msg) => self.on_do_view_change(msg).await,
            MessageBag::StartView(msg) => self.on_start_view(msg).await,
            MessageBag::Commit(ref msg) => self.on_commit(msg).await,
            MessageBag::RequestStartView(ref msg) => self.on_request_start_view(msg).await,
            MessageBag::RequestPrepares(ref msg) => self.on_request_prepares(msg).await,
            MessageBag::RepairPrepare(msg) => self.on_repair_prepare(msg).await,
            MessageBag::RepairRangeReply(ref msg) => self.on_repair_range_reply(msg).await,
            MessageBag::RequestStateTransfer(ref msg) => {
                self.on_request_state_transfer(msg).await;
            }
            MessageBag::StateTransferTarget(ref msg) => {
                self.on_state_transfer_target(msg).await;
            }
            MessageBag::RequestStateChunk(ref msg) => self.on_request_state_chunk(msg).await,
            MessageBag::StateChunk(ref msg) => self.on_state_chunk(msg).await,
            // A forwarded proposal must leave the pump because its commit is
            // driven by this same pump. The metadata-submit handler spawns it.
            MessageBag::ForwardRegister(ref msg) => self.on_forward_register(msg),
            MessageBag::ForwardRegisterResult(ref msg) => {
                self.on_forward_register_result(*msg.header());
            }
            MessageBag::ConsumerSessionHeartbeat(msg) => {
                if self.peer_is_known(msg.header().replica, "ConsumerSessionHeartbeat")
                    && control_suffix_body_verified(&msg, msg.header().checksum_body).is_some()
                {
                    self.host
                        .on_metadata_submit(MetadataSubmit::ConsumerSessionHeartbeat(msg));
                }
            }
            MessageBag::SegmentDelete(message) => self.on_segment_delete(message),
            MessageBag::ForwardLogout(ref msg) => self.on_forward_logout(*msg.header()),
            MessageBag::ForwardLogoutResult(ref msg) => {
                self.on_forward_logout_result(*msg.header());
            }
            MessageBag::ForwardCompletion(msg) => {
                if self.peer_is_known(msg.header().replica, "ForwardCompletion")
                    && control_suffix_body_verified(&msg, msg.header().checksum_body).is_some()
                {
                    self.host
                        .on_metadata_submit(MetadataSubmit::ForwardedCompletion(msg));
                }
            }
        }
    }

    fn on_forward_register(&self, message: &Message<ForwardRegisterHeader>) {
        let header = message.header();
        if !self.peer_is_known(header.replica, "ForwardRegister") {
            return;
        }
        let Some(body) = control_suffix_body_verified(message, header.checksum_body) else {
            return;
        };
        let Ok(registration) =
            iggy_binary_protocol::requests::users::login_register::RegisterRequest::decode_from(
                body,
            )
        else {
            return;
        };
        debug_assert_eq!(
            self.id, 0,
            "ForwardRegister routes to the metadata consensus owner"
        );
        self.host
            .on_metadata_submit(MetadataSubmit::ForwardedRegister {
                vsr_client_id: header.client,
                user_id: header.user_id,
                registration,
                nonce: header.nonce,
                origin_replica: header.replica,
            });
    }

    fn on_forward_register_result(&self, header: ForwardRegisterResultHeader) {
        let waiter = self
            .register_forwards
            .borrow_mut()
            .remove(&(header.nonce, header.client));
        if let Some(waiter) = waiter {
            let _ = waiter.try_send(header);
        } else {
            tracing::debug!(
                shard = self.id,
                nonce = header.nonce,
                client = header.client,
                "dropping forward-register result with no parked login"
            );
        }
    }

    fn on_segment_delete(&self, message: Message<SegmentDeleteHeader>) {
        let header = *message.header();
        let Some(consensus) = self.plane.metadata().consensus.as_ref() else {
            return;
        };
        if header.cluster != consensus.cluster()
            || !self.peer_is_known(header.replica, "SegmentDelete")
        {
            return;
        }
        if header.kind == SegmentDeleteKind::Query {
            self.host
                .on_metadata_submit(MetadataSubmit::ForwardedSegmentDelete(message));
            return;
        }
        let mut pending = self.segment_delete_forwards.borrow_mut();
        let Some(entry) = pending.get(&header.nonce) else {
            return;
        };
        if header.replica != entry.target
            || header.namespace != entry.query.namespace
            || header.created_revision != entry.query.created_revision
            || header.minimum_metadata_op != entry.query.minimum_metadata_op
            || header.segments_count != entry.query.segments_count
            || header.view != entry.query.view
        {
            return;
        }
        let _ = entry.reply.try_send(header);
        pending.remove(&header.nonce);
    }

    fn on_forward_logout(&self, header: ForwardLogoutHeader) {
        if !self.peer_is_known(header.replica, "ForwardLogout") {
            return;
        }
        debug_assert_eq!(
            self.id, 0,
            "ForwardLogout routes to the metadata consensus owner"
        );
        self.host
            .on_metadata_submit(MetadataSubmit::ForwardedLogout {
                vsr_client_id: header.client,
                session: header.session,
                request: header.request,
                nonce: header.nonce,
                origin_replica: header.replica,
            });
    }

    fn on_forward_logout_result(&self, header: ForwardLogoutResultHeader) {
        let waiter = self
            .logout_forwards
            .borrow_mut()
            .remove(&(header.nonce, header.client));
        if let Some(waiter) = waiter {
            let _ = waiter.try_send(header);
        } else {
            tracing::debug!(
                shard = self.id,
                nonce = header.nonce,
                client = header.client,
                "dropping forward-logout result with no parked request"
            );
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

    /// Discard every frame parked under a namespace this shard can never serve:
    /// gone from committed metadata, mid-teardown, or not hashing here. Client
    /// requests get a transient deny, not silence; transports decode replies in
    /// lockstep, so silence wedges the connection until the SDK read-timeout.
    ///
    /// The one retirement path a prepare still travels. It is retained
    /// everywhere else (see `ParkedFrame::passes`); here the namespace itself
    /// is unreachable, so holding it buys nothing.
    pub fn discard_parked_partition_frames(&self, namespace: IggyNamespace) {
        // Bound the borrow to this statement: the guard in an `if let`
        // scrutinee otherwise lives to the end of the then-block, holding a
        // shard-global map locked across the outbound sends below.
        let parked = self.take_parked_partition_frames(namespace);
        if let Some(frames) = parked
            && !frames.is_empty()
        {
            let (answered, dropped) = self.retire_parked_frames(frames);
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                answered,
                dropped,
                "discarding parked partition frames for an unreachable namespace"
            );
        }
    }

    /// Remove a namespace's entry, debiting [`Self::parked_partition_bytes`].
    /// Single place an entry leaves the map, so the two cannot drift out of step
    /// with each other.
    fn take_parked_partition_frames(&self, namespace: IggyNamespace) -> Option<Vec<ParkedFrame>> {
        let (entry, converged) = {
            let mut pending = self.pending_partition_frames.borrow_mut();
            let entry = pending.remove(&namespace)?;
            let converged = pending.is_empty();
            (entry, converged)
        };
        self.parked_partition_bytes.set(
            self.parked_partition_bytes
                .get()
                .saturating_sub(entry.bytes),
        );
        if converged {
            // Episode over: the next entryless shed is a new one and warns.
            self.shard_park_shedding.set(false);
        }
        Some(entry.frames)
    }

    /// Answer client requests, destroy the rest, report `(answered, dropped)`.
    /// A destroyed frame has nobody to reply to, so
    /// `frame_drops_total{variant=partition,reason=park_dropped}` is the only
    /// record it existed.
    fn retire_parked_frames(&self, frames: Vec<ParkedFrame>) -> (usize, usize) {
        let mut answered = 0;
        let mut dropped = 0;
        for frame in frames {
            if self.retire_parked_frame(frame) {
                answered += 1;
            } else {
                dropped += 1;
            }
        }
        (answered, dropped)
    }

    /// Answer one parked request or count one replicated frame as destroyed.
    /// Returns `true` only when a client deny reached the pump.
    fn retire_parked_frame(&self, frame: ParkedFrame) -> bool {
        if self.deny_parked_client_request(frame) {
            return true;
        }
        self.metrics.record_frame_drop(
            crate::metrics::frame_drop_variant::PARTITION,
            crate::metrics::frame_drop_reason::PARK_DROPPED,
        );
        false
    }

    /// Whether any frame is parked. Cheap enough for the reconciler's per-tick
    /// fast-skip guard: a non-empty buffer means the shard is by definition not
    /// converged, so the skip must not fire.
    ///
    /// Read from the map, not the byte cell: an empty entry is never left
    /// behind, but keying convergence off bytes makes that a silent invariant.
    #[must_use]
    pub fn has_parked_partition_frames(&self) -> bool {
        !self.pending_partition_frames.borrow().is_empty()
    }

    /// Namespaces currently holding parked frames. The reconciler pairs this
    /// against committed metadata to find the ones that will never materialise,
    /// which no `ConfirmRemove` / `RemoveRouted` can reach: a namespace that was
    /// never built is in neither `IggyPartitions` nor the routing table, so
    /// nothing else names it.
    #[must_use]
    pub fn parked_namespaces(&self) -> Vec<IggyNamespace> {
        self.pending_partition_frames
            .borrow()
            .keys()
            .copied()
            .collect()
    }

    /// Hand the frames parked for `namespace` back to the pump, in park order,
    /// now that its partition exists at `epoch`.
    ///
    /// A frame stamped with a DIFFERENT incarnation never makes it back: the
    /// namespace is byte-identical across incarnations, so serving it would land
    /// a dead topic's write inside the topic that recycled its keys, and the
    /// downstream fence cannot see it -- that compares the committed revision
    /// against the routing row, both of which now describe THIS incarnation.
    ///
    /// An UNSTAMPED frame (`epoch: None`) is served. `None` means this node's
    /// metadata held no committed partition for the namespace when the frame
    /// arrived, which on a metadata-lagging backup is the ordinary case the park
    /// buffer exists to absorb -- the partition primary materialises and
    /// replicates as soon as its own metadata commits, well before a lagging
    /// backup applies the same commit. Treating that as "prior incarnation"
    /// destroys live traffic: a replicated prepare has no client to answer, so
    /// it would be dropped and the backup left gap-stopped until
    /// `tick_partitions`' level-triggered driver notices and repairs it.
    /// The residual is unchanged from before the stamp existed -- a frame parked
    /// while the namespace was absent, then recreated under a new incarnation,
    /// is served against the replacement -- and closing it needs a wire-level
    /// discriminator (see the `TODO(krishna)` in
    /// `partition_reconciler`'s module docs), not a `None`-means-stale rule.
    ///
    /// Staged onto [`Self::redispatch_queue`] rather than sent: the shard's own
    /// inbox can already hold a LATER op of this partition, and the plane's
    /// backup gap check drops anything that is not `current_op + 1`, so
    /// appending would strand the parked op behind an op that will be dropped
    /// for arriving too early. The pump's biased redispatch arm ranks above its
    /// inbox arm and delivers one staged frame per iteration.
    ///
    /// [`MAX_PARKED_PASSES`] does not bound a staged frame: it has left the park
    /// buffer, and the pump selects the queue on the iteration after it was
    /// filled. Returns whether at least one frame was staged.
    fn redispatch_parked_frames(&self, namespace: IggyNamespace, epoch: u64) -> bool {
        let Some(frames) = self.take_parked_partition_frames(namespace) else {
            return false;
        };
        tracing::debug!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            count = frames.len(),
            epoch,
            "re-dispatching parked partition frames after materialisation"
        );
        let mut servable: Vec<ParkedFrame> = Vec::with_capacity(frames.len());
        for frame in frames {
            // Only a stamp that exists and disagrees is evidence of a prior
            // incarnation; see this function's docs on why `None` is not.
            if let Some(parked_epoch) = frame.epoch
                && parked_epoch != epoch
            {
                self.reject_stale_parked_frame(namespace, epoch, frame);
            } else {
                servable.push(frame);
            }
        }
        let staged = !servable.is_empty();
        self.redispatch_queue.borrow_mut().extend(servable);
        staged
    }

    /// Age every frame under `namespace` by one pass, answering CLIENT REQUESTS
    /// past `MAX_PARKED_PASSES`. Returns the number answered.
    ///
    /// Prepares age but never expire. Expiry would manufacture a gap that a
    /// later commit heartbeat must repair (see `ParkedFrame::passes`), and
    /// passes are commit-driven: a non-empty buffer defeats the reconciler
    /// fast-skip, so a create burst elapses four in milliseconds across every
    /// parked namespace rather than the one it concerns. Byte budgets bound
    /// them instead. Only [`Self::discard_parked_partition_frames`] still
    /// destroys a prepare.
    ///
    /// Passes, not wall-clock, so the simulator's virtual clock governs it.
    pub fn age_parked_partition_frames(&self, namespace: IggyNamespace) -> usize {
        let expired = {
            let mut pending = self.pending_partition_frames.borrow_mut();
            let Some(entry) = pending.get_mut(&namespace) else {
                return 0;
            };
            for frame in &mut entry.frames {
                frame.passes = frame.passes.saturating_add(1);
            }
            let (expired, freed) =
                entry.extract(|frame| !frame.is_replicated() && frame.passes > MAX_PARKED_PASSES);
            let emptied = entry.frames.is_empty();
            drop(pending);
            if emptied {
                // Through the shared remover so the shed-episode flag clears
                // with it; the entry is already empty, so this only unhooks it.
                self.take_parked_partition_frames(namespace);
            }
            self.parked_partition_bytes
                .set(self.parked_partition_bytes.get().saturating_sub(freed));
            expired
        };
        let count = expired.len();
        if count > 0 {
            // Never replicated traffic (the predicate excludes it), so this is
            // a request whose deny the pump refused: destroyed, and counted.
            let (answered, unanswered) = self.retire_parked_frames(expired);
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                answered,
                unanswered,
                "answering parked partition requests that outlived their admission window"
            );
        }
        count
    }

    /// How many frames are parked under `namespace`. Client requests are bounded
    /// by `MAX_PARKED_PER_NAMESPACE`, prepares by
    /// `MAX_PARKED_BYTES_PER_NAMESPACE`; a shed frame must never grow either.
    ///
    /// Test/simulator accessor: nothing in production branches on a per-namespace
    /// park depth, and gating keeps it that way.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn parked_frame_count(&self, namespace: IggyNamespace) -> usize {
        self.pending_partition_frames
            .borrow()
            .get(&namespace)
            .map_or(0, |entry| entry.frames.len())
    }

    /// How many frames are staged for the pump to re-deliver.
    ///
    /// Test/simulator accessor, gated for the same reason as
    /// [`Self::parked_frame_count`]: the pump consumes this queue through a
    /// dedicated select arm, so no production caller has a depth to branch on.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn redispatched_frame_count(&self) -> usize {
        self.redispatch_queue.borrow().len()
    }

    /// Retire a frame that will never be served: a client request gets a
    /// transient deny, replicated traffic is destroyed. Returns `true` only when
    /// a reply reached the pump.
    ///
    /// Callers are synchronous (`apply_reconcile_ops`, the reconciler sweep), so
    /// the deny rides the pump's lifecycle path, not an inline bus send. A shard
    /// with no sender stages nothing, hence forwarding
    /// [`Self::stage_transient_deny`]'s verdict rather than assuming success.
    ///
    /// No reply must not mean no record: the primary may no longer retransmit an
    /// op that reached quorum, so a destroyed prepare creates a gap that later
    /// repair must fill. The `false` return is what makes callers bump
    /// `frame_drops_total{variant=partition,reason=park_dropped}`.
    fn deny_parked_client_request(&self, frame: ParkedFrame) -> bool {
        let ParkedFrame { message, reply, .. } = frame;
        if message.header().command != Command::Request {
            return false;
        }
        let Ok(request) = message.try_into_typed::<RoutedRequestHeader>() else {
            return false;
        };
        // A submit cannot be answered through `stage_transient_deny`: it routes
        // by `header.client`, which on a partition request is the VSR consensus
        // id and addresses no connection. Its own channel reaches the shard
        // holding the socket.
        if reply.is_some() {
            return Self::answer_partition_submit_deny(
                request.header(),
                reply,
                &IggyError::TransientNotAccepted,
            );
        }
        self.stage_transient_deny(request.header())
    }

    /// A parked frame addressed an incarnation this shard no longer holds.
    /// Answering the client is what keeps it from waiting out its read timeout;
    /// a stale prepare is dropped, since applying it would write a dead
    /// incarnation's op into its replacement and diverge this replica.
    fn reject_stale_parked_frame(
        &self,
        namespace: IggyNamespace,
        materialised_epoch: u64,
        frame: ParkedFrame,
    ) {
        // Both directions reject: a frame stamped AHEAD must not be applied into
        // the incarnation the staleness teardown is about to erase either. Only
        // BEHIND is the anomaly `partition_frames_rejected_stale_total` is
        // alerted on. Ahead means the recreate committed between the reconciler
        // snapshotting `epoch` and the pump applying `InsertOwned`: expected
        // churn, and counting it there fires the alert on a race by design.
        let ahead = frame
            .epoch
            .is_some_and(|parked_epoch| parked_epoch > materialised_epoch);
        let replicated = frame.is_replicated();
        if ahead {
            self.metrics.record_partition_frame_rejected_ahead();
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                parked_epoch = ?frame.epoch,
                materialised_epoch,
                replicated,
                "rejecting parked partition frame stamped ahead of the materialised incarnation"
            );
        } else {
            self.metrics.record_partition_frame_rejected_stale();
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                parked_epoch = ?frame.epoch,
                materialised_epoch,
                replicated,
                "rejecting parked partition frame from a prior incarnation"
            );
        }
        self.retire_parked_frame(frame);
    }

    /// Park a partition-plane frame whose namespace this shard has not yet
    /// materialised (post-`CreateTopic` convergence window: the metadata
    /// commit precedes the reconciler pass that builds the local replica).
    ///
    /// Tombstoned namespaces (teardown fence set by the reconciler before the
    /// disk delete) report [`ParkOutcome::Tombstoned`] so the caller can deny
    /// client requests instead of feeding them to the plane's silent-drop
    /// guard, while replicated traffic still flows there. Parked frames are
    /// staged for the pump by [`Self::apply_reconcile_ops`] once the matching
    /// `ReconcileOp::InsertOwned` lands, and only if the epoch stamped here
    /// still matches (see [`ParkedFrame`]); a full buffer reports
    /// [`ParkOutcome::Overflow`] so the caller can answer rather than shed
    /// silently.
    ///
    /// `provenance` is `None` for a frame arriving off the wire and `Some` for
    /// one the pump is re-delivering, which must keep the stamp and the age it
    /// parked with (see [`ParkProvenance`]).
    fn park_if_unmaterialised<H>(
        &self,
        message: Message<H>,
        operation: Operation,
        namespace_raw: u64,
        provenance: Option<ParkProvenance>,
        reply: &mut Option<Sender<Option<Message<GenericHeader>>>>,
        attachment: &mut Option<ConsumerAttachment>,
    ) -> ParkOutcome<H>
    where
        H: iggy_binary_protocol::ConsensusHeader,
        M: StreamsFrontend,
    {
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
        // Read the committed revision before taking the borrow below: the frame
        // is stamped with the incarnation it was addressed to, so a later drain
        // can tell it apart from a same-key replacement. A re-delivered frame
        // brings its own, since by now the committed revision can describe the
        // replacement rather than the incarnation the frame was addressed to.
        let ParkProvenance { epoch, passes } = provenance.unwrap_or_else(|| ParkProvenance {
            epoch: self
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(namespace),
            passes: 0,
        });
        let frame_cost = parked_footprint(message.as_slice().len());
        let replicated = message.header().command() != Command::Request;
        let mut pending = self.pending_partition_frames.borrow_mut();
        let parked_bytes = self.parked_partition_bytes.get();
        // Read the entry without `entry().or_default()`: inserting first would
        // leave an empty entry behind on the overflow path below, which reads as
        // a parked namespace to the reconciler sweep and its fast-skip guard.
        let existing = pending.get_mut(&namespace);
        let parked_len = existing.as_ref().map_or(0, |entry| entry.frames.len());
        let namespace_bytes = existing.as_ref().map_or(0, |entry| entry.bytes);
        // A prepare is never shed on a byte budget. No client to answer, and
        // recovery is slow: `consensus::retransmit_targets` skips an op that
        // already reached quorum, so shedding one gap-stops the backup until
        // `tick_partitions`' driver repairs it, where shedding a request costs
        // one retry. A request is refused the moment admitting it
        // would cross a budget; a prepare only once one is already spent. Caps
        // prepare residency at one frame of overshoot per budget (worst case
        // `MAX_PARKED_BYTES` + `max_message_size`, 80 MiB per shard) instead of
        // at the budget, and is what makes an oversize frame parkable at all.
        let namespace_budget_spent = parked_len > 0
            && if replicated {
                namespace_bytes >= MAX_PARKED_BYTES_PER_NAMESPACE
            } else {
                namespace_bytes.saturating_add(frame_cost) > MAX_PARKED_BYTES_PER_NAMESPACE
            };
        let shard_budget_spent = if replicated {
            parked_bytes >= MAX_PARKED_BYTES
        } else {
            parked_bytes.saturating_add(frame_cost) > MAX_PARKED_BYTES
        };
        // The frame cap is request-only for the same reason. Applied to both it
        // would be the binding constraint for any footprint under
        // `MAX_PARKED_BYTES_PER_NAMESPACE / MAX_PARKED_PER_NAMESPACE` (32 KiB),
        // so header-only prepares would shed at 128 frames, 512 KiB into a 4 MiB
        // budget, and the byte budgets above would never get a say.
        let frame_cap_spent = !replicated && parked_len >= MAX_PARKED_PER_NAMESPACE;
        if frame_cap_spent || namespace_budget_spent || shard_budget_spent {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::metrics::frame_drop_reason::PARK_OVERFLOW,
            );
            // Warn once per namespace on entering the shed, then `debug`: a
            // full buffer is this branch's trigger, not a rate limit, so every
            // later frame lands here too, and one formatted `warn` apiece makes
            // the non-blocking appender shed unrelated lines. The counter
            // carries the volume.
            //
            // An entryless namespace has no `ParkEntry::shed` to gate on and is
            // reachable only via the shard-wide budget (the other two conditions
            // need a non-empty entry), which is the many-namespace burst
            // `MAX_PARKED_BYTES` is sized for. Hence the shard-level gate, and
            // not `entry().or_default()`, which leaves the empty entry the read
            // above avoids.
            let first_shed = match existing {
                Some(entry) => {
                    let first = entry.shed == 0;
                    entry.shed = entry.shed.saturating_add(1);
                    first
                }
                None => !self.shard_park_shedding.replace(true),
            };
            if first_shed {
                tracing::warn!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    parked_frames = parked_len,
                    namespace_bytes,
                    parked_bytes,
                    frame_cost,
                    replicated,
                    "park buffer at capacity; shedding partition frames"
                );
            } else {
                tracing::debug!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    parked_bytes,
                    frame_cost,
                    replicated,
                    "park buffer still at capacity; shedding partition frame"
                );
            }
            return ParkOutcome::Overflow(message);
        }
        tracing::debug!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            operation = ?operation,
            epoch = ?epoch,
            "parking partition frame until namespace materialises"
        );
        pending.entry(namespace).or_default().push(ParkedFrame {
            epoch,
            passes,
            message: message.into_generic(),
            reply: reply.take(),
            attachment: attachment.take(),
        });
        drop(pending);
        self.parked_partition_bytes
            .set(parked_bytes.saturating_add(frame_cost));
        ParkOutcome::Parked
    }

    /// Deny a client partition request with `TransientNotAccepted`: the frame
    /// never reached journal admission, so the SDK can safely replay it
    /// anywhere, and partition rebuild completes well inside the replay
    /// budget. Sent by client id, which only a raw request without a reply
    /// channel reaches, and which the production bus refuses.
    #[allow(clippy::future_not_send)]
    async fn deny_partition_request_transient(&self, request_header: &RoutedRequestHeader) {
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::TransientNotAccepted.as_code(),
        );
        // Count only what the bus accepted, matching `stage_transient_deny` and
        // `record_partition_request_denied_transient`'s contract: a refused deny
        // is a shed frame, and crediting it hides the silent shed this counter
        // exists to expose.
        if let Err(error) = self
            .bus
            .send_to_client(request_header.client, reply.into_generic().into_frozen())
            .await
        {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::metrics::frame_drop_reason::DELIVERY_FAILED,
            );
            tracing::debug!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                error = %error,
                "transient deny for partition request has no route by client id"
            );
            return;
        }
        self.metrics.record_partition_request_denied_transient();
    }

    /// Deny a request no consensus plane claims, and count the frame it drops.
    /// `InvalidCommand` because no retry makes an operation routable. Counted as
    /// a drop even though the client is answered: nothing was routed or
    /// journaled, and `unroutable` is the counter a simulator run asserts on.
    #[allow(clippy::future_not_send)]
    async fn deny_unroutable_request(&self, request_header: &RoutedRequestHeader) {
        self.metrics.record_frame_drop(
            crate::metrics::frame_drop_variant::CONSENSUS,
            crate::metrics::frame_drop_reason::UNROUTABLE,
        );
        tracing::error!(
            shard = self.id,
            client = request_header.client,
            operation = ?request_header.operation,
            namespace_raw = request_header.group,
            "request operation is claimed by no consensus plane; denying it"
        );
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::InvalidCommand.as_code(),
        );
        if let Err(error) = self
            .bus
            .send_to_client(request_header.client, reply.into_generic().into_frozen())
            .await
        {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::CONSENSUS,
                crate::metrics::frame_drop_reason::DELIVERY_FAILED,
            );
            tracing::debug!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                error = %error,
                "deny for unroutable request has no route by client id"
            );
        }
    }

    /// Drop a replicated frame no consensus plane claims, and count it.
    ///
    /// No reply, unlike [`Self::deny_unroutable_request`]: a prepare or an ack
    /// has no client waiting on this node. Terminal for the frame's group here,
    /// as the unknown-discriminant drop in [`Self::dispatch`] is: nothing
    /// journals or acks an operation no plane owns, so every later op in that
    /// group waits behind the gap while quorum hides it. Nothing fences the
    /// sending peer, so the counter and this log are the whole signal.
    fn drop_unroutable_replicated(
        &self,
        command: Command,
        operation: Operation,
        namespace_raw: u64,
        op: u64,
    ) {
        self.metrics.record_frame_drop(
            crate::metrics::frame_drop_variant::CONSENSUS,
            crate::metrics::frame_drop_reason::UNROUTABLE,
        );
        tracing::error!(
            shard = self.id,
            command = ?command,
            operation = ?operation,
            namespace_raw,
            op,
            "replicated frame operation is claimed by no consensus plane; dropping it"
        );
    }

    /// [`Self::deny_partition_request_transient`] for synchronous callers:
    /// hand the deny to this shard's own pump as a
    /// [`LifecycleFrame::ForwardClientSend`], whose handler performs the bus
    /// send (same funnel the parked-frame re-dispatch uses).
    ///
    /// Returns whether the pump took it. A shard with no sender stages nothing,
    /// so assuming success logs an answer for a request destroyed unanswered.
    fn stage_transient_deny(&self, request_header: &RoutedRequestHeader) -> bool {
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::TransientNotAccepted.as_code(),
        );
        let frame = ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
            client_id: request_header.client,
            msg: reply.into_generic().into_frozen().into(),
        });
        let Some(sender) = self.senders.get(self.id as usize) else {
            return false;
        };
        // Count only what was actually handed to the pump: crediting before the
        // send reports an answer to a client that never received one, which is
        // the opposite of what this counter is read for.
        if let Err(error) = sender.reply_sender().try_send(frame) {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PARTITION,
                crate::coordinator::classify_try_send_err(&error),
            );
            tracing::warn!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                "dropping transient deny for discarded partition frame: inbox rejected: {error:?}"
            );
            return false;
        }
        self.metrics.record_partition_request_denied_transient();
        true
    }

    /// Single entry point for first and re-delivered partition submits.
    /// Session and operation checks precede parking. Partition readiness and
    /// metadata fences are rechecked when the owning partition materializes.
    #[allow(clippy::future_not_send)]
    async fn dispatch_partition_submit(
        &self,
        request: Message<RoutedRequestHeader>,
        reply: Sender<Option<Message<GenericHeader>>>,
        provenance: Option<ParkProvenance>,
        mut attachment: Option<ConsumerAttachment>,
    ) where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        if let Some(attachment) = &attachment
            && let Err(error) = attachment.validate_write(request.header().operation)
        {
            Self::answer_partition_submit_deny(request.header(), Some(reply), &error);
            return;
        }
        let routing = {
            let header = request.header();
            (header.operation, header.group)
        };
        // The frame takes a clone of the sender only when it parks; every other
        // outcome answers on the original below, so no arm can lose the waiter
        // to a `None` it would have to guard against. The clone is an `Rc`
        // bump, and whichever half is not used drops with this scope.
        match self.park_if_unmaterialised(
            request,
            routing.0,
            routing.1,
            provenance,
            &mut Some(reply.clone()),
            &mut attachment,
        ) {
            ParkOutcome::Deliver(request)
                if !self.serves_committed_incarnation(routing.0, routing.1) =>
            {
                Self::answer_partition_submit_deny(
                    request.header(),
                    Some(reply),
                    &IggyError::TransientNotAccepted,
                );
            }
            ParkOutcome::Deliver(request) => {
                if let Some(attachment) = attachment
                    && let Err(error) = self.validate_partition_attachment(&request, &attachment)
                {
                    Self::answer_partition_submit_deny(request.header(), Some(reply), &error);
                    return;
                }
                let (sender, receiver) = consensus::oneshot_channel();
                self.plane
                    .partitions()
                    .on_request_with_reply(request, Some(sender))
                    .await;
                // Await OFF the pump: the commit that fires this receiver needs
                // the pump to keep draining acks, so blocking here would
                // deadlock the very reply being waited on. The task holds only
                // owned channel halves, never a partitions borrow.
                //
                // Through the bus, not the runtime directly: the simulator
                // supplies its own executor and virtual clock.
                self.bus.spawn(async move {
                    let committed = receiver.await.ok().map(Message::into_generic);
                    let _ = reply.try_send(committed);
                });
            }
            ParkOutcome::Tombstoned(request) | ParkOutcome::Overflow(request) => {
                Self::answer_partition_submit_deny(
                    request.header(),
                    Some(reply),
                    &IggyError::TransientNotAccepted,
                );
            }
            // The clone travelled with the parked frame; it answers on drain
            // or wakes the awaiter with a receive error when the frame expires.
            ParkOutcome::Parked => {}
        }
    }

    /// Answer a refused `PartitionSubmit` over its own channel. A failed send
    /// still counts as a dropped frame.
    fn answer_partition_submit_deny(
        request_header: &RoutedRequestHeader,
        reply: Option<Sender<Option<Message<GenericHeader>>>>,
        error: &IggyError,
    ) -> bool {
        let Some(reply) = reply else { return false };
        let deny = build_deny_reply_from_request_header(request_header, error.as_code());
        reply.try_send(Some(deny.into_generic())).is_ok()
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_request(&self, request: Message<RoutedRequestHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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

    /// Append ready self-acks in metadata/namespace/FIFO order on every call.
    /// Backup acks can commit and refill a partition while older entries wait.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn process_loopback(&self, round: &mut router::LoopbackRound) -> usize
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend
            + metadata::stm::snapshot::RestoreSnapshotInPlace<
                metadata::stm::snapshot::MetadataSnapshot,
            >,
    {
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus {
            consensus.drain_loopback_into(&mut round.scratch);
            round
                .entries
                .extend(round.scratch.drain(..).map(|message| (None, message)));
        }
        let ready = planes.1.0.take_ready_loopbacks();
        for (namespace, incarnation) in ready {
            let Some(partition) = planes
                .1
                .0
                .get_by_ns(&namespace)
                .filter(|partition| partition.incarnation() == incarnation)
            else {
                continue;
            };
            partition
                .consensus()
                .drain_loopback_into(&mut round.scratch);
            round.entries.extend(
                round
                    .scratch
                    .drain(..)
                    .map(|message| (Some(incarnation), message)),
            );
        }

        let mut serviced = 0;
        while serviced < router::COOPERATIVE_EVENT_BUDGET {
            let Some((incarnation, message)) = round.entries.pop_front() else {
                break;
            };
            let message: Message<PrepareOkHeader> = message
                .try_into_typed()
                .expect("loopback queue must only contain PrepareOk messages");
            match incarnation {
                None => planes.0.on_ack(message).await,
                Some(incarnation) => {
                    let namespace = IggyNamespace::from_raw(message.header().group);
                    if planes
                        .1
                        .0
                        .get_by_ns(&namespace)
                        .is_some_and(|partition| partition.incarnation() == incarnation)
                    {
                        planes.1.0.on_ack(message).await;
                    }
                }
            }
            serviced += 1;
            self.cooperate().await;
        }
        serviced
    }

    fn has_pending_loopback(&self, round: &router::LoopbackRound) -> bool {
        !round.entries.is_empty()
            || self.plane.partitions().has_ready_loopbacks()
            || self
                .plane
                .metadata()
                .consensus
                .as_ref()
                .is_some_and(VsrConsensus::has_loopback)
    }

    /// Simulator-only: mutates `IggyPartitions` off the pump task, bypassing the
    /// reconciler's `ReconcileOp::InsertOwned` funnel (production's runtime path;
    /// bootstrap recovery uses `load_partition`). VSR replica id comes from
    /// `PartitionConsensusConfig`, not `self.id` (the local shard index).
    ///
    /// `superblock` is this group's durable `(view, log_view)` store. `None` takes
    /// the storeless branch, where the persist gate marks every view durable
    /// without writing, leaving the gate, its write-failure fence and view
    /// recovery unexercised. Passing one in gets the production contract: a
    /// recorded view is restored before the group joins, and a failed write
    /// withholds every view-scoped send. `recovered_state` is that store's last
    /// record, read by the caller because the store's read is async and this is
    /// not.
    ///
    /// `retained` is the log a previous incarnation left behind, standing in for
    /// the segments a real boot recovers from. `None` is right for a first
    /// materialisation and wrong for a restart: a rebuilt partition with no data
    /// reports `commit_offset` 0, which reads as a regression rather than a
    /// harness that discarded the log.
    ///
    /// `persistence` is this group's WAL, reopened by the caller from its
    /// filesystem model. Its certificate restores `log_view` as boot's does, and
    /// every acknowledgement and view-scoped send waits for it.
    ///
    /// `materialisation` carries the committed `created_revision` and this
    /// partition's assigned initial view; see `fresh_group_start`.
    ///
    /// Once inserted, this also runs the same parked-frame redispatch as
    /// `ReconcileOp::InsertOwned`. The simulator bypasses the reconciler build,
    /// but it must not bypass the pump handoff that follows materialisation.
    // `feature = "simulator"` alone, unlike its neighbours: the body names items
    // `partitions` gates the same way, and a `test` arm cannot turn those on.
    // Under `cargo test -p shard` that arm fires from shard's own `cfg(test)`
    // while `partitions` builds as a plain dependency, so `RetainedPartitionLog`
    // and `adopt_retained_log` are configured out and the crate does not compile.
    // The feature forwards to `partitions/simulator` instead.
    /// The WAL runs on the simulator's filesystem model. The retained log and
    /// receipts stand in for segment and receipt files, which it does not model.
    #[cfg(feature = "simulator")]
    pub fn init_partition(
        &self,
        namespace: IggyNamespace,
        superblock: Option<Rc<SB>>,
        recovered_state: Option<&consensus::VsrState>,
        retained: Option<partitions::RetainedPartitionState>,
        persistence: Rc<partitions::PartitionPersistence>,
        materialisation: PartitionMaterialisation,
    ) where
        B: MessageBus + Clone + 'static,
        T: ShardsTable,
        M: metadata::impls::metadata::StreamsFrontend,
    {
        let PartitionMaterialisation {
            epoch,
            created_view,
            first_initialization,
            consumer_offsets_max,
        } = materialisation;
        let partitions = self.plane.partitions();
        if partitions.contains(&namespace) {
            return;
        }

        let mut consensus = VsrConsensus::with_clock(
            self.partition_consensus.cluster_id,
            self.partition_consensus.self_replica_id,
            self.partition_consensus.replica_count,
            namespace.inner(),
            self.partition_consensus.bus.clone(),
            LocalPipeline::new(),
            self.partition_consensus.clock.clone(),
        );
        let durable_view = recovered_state.map(|state| (state.view, state.log_view));
        // The harness's incarnation-qualified store is its initialization ledger.
        // Recovered metadata can precede the group's first materialization;
        // missing retained history alone cannot distinguish that from data loss.
        consensus.set_recovery_election_allowed(first_initialization || retained.is_some());
        let restarted = (retained.is_some() || !first_initialization)
            && self.partition_consensus.replica_count > 1;
        let consensus::FreshGroupStart {
            join,
            view_fallback,
            seed_view,
        } = consensus::fresh_group_start(restarted, durable_view, created_view);

        // Recorded view first, exactly as the two boot paths order it: restoring
        // after `init` would advertise a view older than the recorded one.
        consensus.restore_view(consensus::ViewRestore {
            durable_view,
            view_fallback,
            seed_view,
        });
        // A rebuilt replica cannot know the group's `(op, commit)`: the
        // partition journal is in-memory and segments carry no op numbers. So
        // in a cluster it joins quorum-invisible and asks the view's primary
        // rather than resuming as a primary its peers may have replaced. Plain
        // `init` would set `Status::Normal` and arm the commit broadcast on
        // whichever replica is primary-by-index, the split-brain
        // `init_as_backup` exists to prevent.
        consensus.join(join);

        let stats = Arc::new(PartitionStats::default());
        let mut partition = IggyPartition::with_in_memory_storage(
            stats,
            consensus,
            partitions.config().segment_size,
        );
        partition.set_created_revision(epoch);
        let runtime_options = self.plane.metadata().mux_stm.streams().read(|inner| {
            inner
                .items
                .get(namespace.stream_id())
                .and_then(|stream| stream.topics.get(namespace.topic_id()))
                .map(|topic| {
                    iggy_common::TopicRuntimeOptions::from_resource_options(&topic.options)
                })
                .unwrap_or_default()
        });
        partition.set_runtime_options(runtime_options);
        partition.set_consumer_offsets_max(consumer_offsets_max);
        if let Some(superblock) = superblock {
            partition.set_superblock(superblock, recovered_state);
        }
        // Retained log before the frontier restore, so the restore maxes against
        // the offsets the log proved rather than the zeroes of an empty one.
        // `restore_offset_frontier` takes each counter's own max against what is
        // already loaded, so neither can be lowered here -- but only if the log is
        // adopted first, or those maxes are taken against the zeroes of a
        // partition that has not got its offsets back yet.
        if let Some(state) = retained {
            let head_op = state.head_op;
            let applied_op = state.applied_op;
            let prepare_checksum = state.prepare_checksum;
            partition.adopt_retained_log(state);
            let consensus = partition.consensus();
            consensus.sequencer().set_sequence(head_op);
            consensus.restore_commit_state(applied_op, applied_op);
            consensus.set_last_prepare_checksum(prepare_checksum);
        }
        // The SAME call the boot paths make, not a copy of it: this restore is
        // a max against what the segments already proved, and a harness running
        // a divergent copy of that rule cannot catch a violation of it. Without
        // the restore at all, a simulator replica rebuilt against a retained
        // store resumes minting at 0 while its group is at N.
        partition.restore_offset_frontier(recovered_state);
        partition.attach_simulated_persistence(persistence);
        partition.consensus().resume_adopted_view();
        // Match production's solo reservation handling. A replicated WAL may
        // retain a suffix the next view discards; anchoring at its dirty offset
        // would prevent truncation from restoring the earlier append point.
        if self.partition_consensus.replica_count == 1 && !partition.durability().is_persisted() {
            partition.reanchor_in_memory_to_mint_frontier(partitions.config().segment_size);
        }
        partitions.insert(namespace, partition);
        if self.redispatch_parked_frames(namespace, epoch) {
            // This mutation occurs outside the pump, unlike production's
            // `InsertOwned`. Wake the ranked redispatch arm so quiescence does
            // not leave real work staged without a poll source.
            self.wake_reconcile_apply();
        }
    }

    fn partition_incarnation_matches(
        &self,
        partition: &IggyPartition<B, SB>,
        partition_incarnation: u64,
        frame: Command,
    ) -> bool {
        if partition_incarnation == partition.created_revision() {
            return true;
        }
        let drops = self.metrics.record_partition_incarnation_mismatch(frame);
        // Paced per command: delete + recreate churn drops these for every frame
        // a peer on the other incarnation sends until it converges.
        if self
            .incarnation_mismatch_warns
            .borrow_mut()
            .entry(frame as u8)
            .or_default()
            .admit(self.ticks.get())
        {
            tracing::warn!(
                shard = self.id,
                namespace = partition.consensus().group(),
                ?frame,
                partition_incarnation,
                hosted_incarnation = partition.created_revision(),
                drops,
                "dropping partition control frame of another incarnation"
            );
        }
        false
    }

    /// Resolve the single partition a VSR control frame addresses, keyed by
    /// `header.group`. Warns and returns `None` when the namespace matches
    /// neither metadata nor a live partition consensus. Returns `&mut` because
    /// `on_do_view_change` / `on_commit` need it for `commit_journal`; the read-
    /// only callers reborrow `&`. Pump-only (sole mutator), so the `&mut` formed
    /// here via interior mutability cannot alias a concurrent reconcile.
    #[allow(clippy::mut_from_ref, clippy::too_many_arguments)]
    fn resolve_partition_target<'a>(
        &self,
        partitions: &'a IggyPartitions<B, SB>,
        namespace: u64,
        partition_incarnation: u64,
        view: u32,
        replica: u8,
        frame: Command,
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
                ?frame,
                "dropping VSR control frame: namespace matches neither metadata nor partition consensus"
            );
            return None;
        };
        debug_assert_eq!(
            partition.consensus().group(),
            namespace,
            "keyed partition lookup must match the frame namespace"
        );
        if !self.partition_incarnation_matches(partition, partition_incarnation, frame) {
            return None;
        }
        // A stale primary's heartbeat asks the current primary to announce its view.
        if view < partition.consensus().view() && frame != Command::Commit {
            return None;
        }
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            refresh_metadata_dvc_suffix(consensus, planes.0.journal.as_ref());
            let actions = consensus.handle_start_view_change(PlaneKind::Metadata, &header);
            let (local_actions, wire_actions) = split_local_actions(actions);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &local_actions).await;
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &wire_actions).await;
            }
            return;
        }

        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.group,
            header.partition_incarnation,
            header.view,
            header.replica,
            Command::StartViewChange,
        ) else {
            return;
        };
        if header.view == partition.consensus().view() && partition.consensus().is_normal() {
            return;
        }
        if partition.history_is_busy() {
            partition.defer_view_transition(MessageBag::StartViewChange(msg));
            return;
        }
        refresh_partition_dvc_suffix(partition);
        let consensus = partition.consensus();
        let actions = consensus.handle_start_view_change(PlaneKind::Partitions, &header);
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions).await;
    }

    #[allow(clippy::future_not_send)]
    async fn on_do_view_change(&self, msg: Message<DoViewChangeHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            refresh_metadata_dvc_suffix(consensus, planes.0.journal.as_ref());
            let Some(suffix_body) = control_suffix_body_verified(&msg, header.checksum_body) else {
                tracing::warn!(
                    shard = self.id,
                    from_replica = header.replica,
                    view = header.view,
                    "dropping do_view_change whose body failed its checksum"
                );
                return;
            };
            let actions =
                consensus.handle_do_view_change(PlaneKind::Metadata, &header, suffix_body);
            let (local_actions, wire_actions) = split_local_actions(actions);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &local_actions).await;
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &wire_actions).await;
            }
            // Same transfer gate as `on_start_view` and `on_commit`: the
            // pre-install STM must not walk while a transfer is in flight.
            if local_actions
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
            header.group,
            header.partition_incarnation,
            header.view,
            header.replica,
            Command::DoViewChange,
        ) else {
            return;
        };
        if partition.history_is_busy() {
            partition.defer_view_transition(MessageBag::DoViewChange(msg));
            return;
        }
        refresh_partition_dvc_suffix(partition);
        let consensus = partition.consensus();
        let Some(suffix_body) = control_suffix_body_verified(&msg, header.checksum_body) else {
            tracing::warn!(
                shard = self.id,
                from_replica = header.replica,
                view = header.view,
                "dropping do_view_change whose body failed its checksum"
            );
            return;
        };
        let actions = consensus.handle_do_view_change(PlaneKind::Partitions, &header, suffix_body);
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions).await;
        // Outside the gate: the persist fences the SEND, not the local commit
        // walk (state a crash forgets is state no peer ever saw). Same
        // transfer gate as the metadata arm: no walk while transferring.
        if local_actions
            .iter()
            .any(|action| matches!(action, VsrAction::CommitJournal))
            && !partition.consensus().is_transferring()
        {
            partition.commit_journal(config).await;
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_start_view(&self, msg: Message<StartViewHeader>)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            let Some(suffix_body) = control_suffix_body_verified(&msg, header.checksum_body) else {
                tracing::warn!(
                    shard = self.id,
                    from_replica = header.replica,
                    view = header.view,
                    "dropping start_view whose body failed its checksum"
                );
                return;
            };
            let Some(actions) = consensus.handle_start_view_guarded(
                PlaneKind::Metadata,
                &header,
                suffix_body,
                |op| planes.0.applied_header(op),
            ) else {
                return;
            };
            let adopted = !actions.is_empty();
            if !consensus.is_transferring() && consensus.view_log_unproven() {
                // First chance to spot a local entry disagreeing with the view's log.
                // Ahead of the local dispatch below: it truncates the journal that
                // `RebuildPipeline` reads back, so a rebuild before it would seed the
                // pipeline from the entries this is about to drop.
                self.reconcile_metadata_view_divergence().await;
                if consensus.view_log_unproven() {
                    self.maybe_request_metadata_repair(consensus, header.replica)
                        .await;
                    return;
                }
            }
            let (local_actions, wire_actions) = split_local_actions(actions);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &local_actions).await;
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &wire_actions).await;
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
            if local_actions
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

        let Some(partition) = self.resolve_partition_target(
            &planes.1.0,
            header.group,
            header.partition_incarnation,
            header.view,
            header.replica,
            Command::StartView,
        ) else {
            return;
        };
        if partition.history_is_busy() {
            partition.defer_view_transition(MessageBag::StartView(msg));
            return;
        }
        let Some(suffix_body) = control_suffix_body_verified(&msg, header.checksum_body) else {
            tracing::warn!(
                shard = self.id,
                from_replica = header.replica,
                view = header.view,
                "dropping start_view whose body failed its checksum"
            );
            return;
        };
        partition.ensure_materialization_recovery();
        let actions = match partition.consensus().try_handle_start_view(
            PlaneKind::Partitions,
            &header,
            suffix_body,
            |op| partition.applied_header(op),
        ) {
            Ok(actions) => actions,
            // Refusing alone leaves this replica refusing every retransmit of the
            // view, out of the group for good. The refused log still names the
            // history both sides share, which is where the rebuild resumes.
            Err(StartViewRefusal::ContradictsAppliedLog(refused)) => {
                if let Some((diverged_op, agreed_op)) =
                    partition_applied_divergence(partition, &refused)
                {
                    self.resync_diverged_partition(
                        IggyNamespace::from_raw(header.group),
                        partition,
                        diverged_op,
                        agreed_op,
                    );
                }
                return;
            }
            Err(StartViewRefusal::InvalidEvidence) => return,
        };
        // Checked on every adoption, transferring or not: a transfer that later
        // fails leaves the applied history the view contradicts in place.
        if !actions.is_empty()
            && let Some((diverged_op, agreed_op)) = partition
                .consensus()
                .with_pending_view_log(|pending| partition_applied_divergence(partition, pending))
                .flatten()
        {
            self.resync_diverged_partition(
                IggyNamespace::from_raw(header.group),
                partition,
                diverged_op,
                agreed_op,
            );
            return;
        }
        if !partition.consensus().is_transferring() && partition.consensus().view_log_unproven() {
            // Any stream armed before this adoption belongs to the superseded
            // view. Repair bodies carry no nonce, so drop the receiving session
            // before reconciling or arming the new view's canonical range.
            partition.repair = None;
            // Ahead of the local dispatch, which rebuilds the pipeline out of the
            // journal this rewrites. Same position as the metadata arm's twin, and
            // like it, pending-less adoptions (empty StartView suffix) still sweep
            // the relics above the adopted head.
            let pending = partition.consensus().pending_view_log();
            if partition.has_io_dispatcher()
                && let Some(from_op) = partition_view_divergence_from(partition, pending.as_ref())
            {
                partition.defer_view_truncation(from_op..=u64::MAX, actions, Some(header.replica));
                return;
            }
            reconcile_partition_view_divergence(self.id, partition, pending.as_ref()).await;
            if partition.consensus().view_log_unproven() {
                self.maybe_request_partition_repair(partition, header.replica)
                    .await;
                return;
            }
        }
        self.finish_partition_view_adoption(
            IggyNamespace::from_raw(header.group),
            header.replica,
            actions,
        )
        .await;
    }

    #[allow(clippy::future_not_send)]
    async fn finish_partition_view_adoption(
        &self,
        namespace: IggyNamespace,
        peer: u8,
        actions: Vec<VsrAction>,
    ) where
        MJ: JournalHandle,
        MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        // Count before borrowing one owner mutably: the scan visits every owner.
        let transfers_inflight =
            if Self::may_arm_partition_transfer(self.plane.partitions(), namespace.inner()) {
                self.partition_transfers_inflight()
            } else {
                0
            };
        // A rejected StartView must not replace an existing transfer session.
        let adopted = !actions.is_empty();
        let partitions = self.plane.partitions();
        let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
            return;
        };
        let consensus = partition.consensus();
        let (local_actions, wire_actions) = split_local_actions(actions);
        // RebuildPipeline needs the partition dispatcher; metadata has no journal here.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions).await;
        if adopted
            && partition.consensus().state_transfer_stage()
                == consensus::StateTransferStage::AwaitingTarget
        {
            tracing::info!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                peer,
                "adopted a live view while awaiting transfer; requesting partition state transfer"
            );
            if self.peer_is_known(peer, "StartView") {
                let _ = self
                    .arm_partition_transfer(partition, peer, transfers_inflight)
                    .await;
            }
            return;
        }
        if partition.consensus().is_transferring() {
            return;
        }
        if local_actions
            .iter()
            .any(|action| matches!(action, VsrAction::CommitJournal))
        {
            partition.commit_journal(partitions.config()).await;
        }
        self.maybe_request_partition_repair(partition, peer).await;
        partition.notify_io();
    }

    #[allow(clippy::future_not_send)]
    async fn on_commit(&self, msg: &Message<CommitHeader>)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
        T: ShardsTable,
    {
        let header = *msg.header();
        let planes = self.plane.inner();

        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
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
                        // this, such a replica waits out `tick_metadata`'s
                        // debounced gap detector; this edge is the fast path,
                        // for the runs where a heartbeat does land as
                        // `Advanced`. Request repair from the primary; if it
                        // has checkpointed past the gap the repair floor evicts
                        // and the handler above converts to state transfer.
                        // Idempotent: `maybe_request_metadata_repair` no-ops
                        // when caught up, already transferring, or a session is
                        // live, so a caught-up replica and a cold-start node
                        // (commit_max == commit_min == 0) both skip it.
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
                        refresh_metadata_dvc_suffix(consensus, planes.0.journal.as_ref());
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
            header.group,
            header.partition_incarnation,
            header.view,
            header.replica,
            Command::Commit,
        ) else {
            return;
        };
        let consensus = partition.consensus();
        match consensus.handle_commit(&header) {
            CommitOutcome::Advanced => {
                if !partition.consensus().is_transferring() {
                    partition.commit_journal(config).await;
                    // Same-view late-joiner backstop: a lagging backup drops
                    // out-of-order prepares silently and StartView adoption
                    // is otherwise the only repair-arming site, so without
                    // this a same-view gap wedges until a view change. If
                    // the primary compacted past the gap, repair answers
                    // RangeEvicted and the refusal path converts to
                    // transfer.
                    self.maybe_request_partition_repair(partition, header.replica)
                        .await;
                }
            }
            CommitOutcome::RespondStartView => {
                // Durable-before-send, as the metadata arm above: the StartView
                // advertises this replica's current view. Withhold on failure;
                // the stale peer keeps heartbeating, so it re-triggers once a
                // later persist succeeds.
                if !partition.requires_state_transfer()
                    && partition.persist_superblock_if_needed().await
                {
                    refresh_partition_dvc_suffix(partition);
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            refresh_metadata_dvc_suffix(consensus, planes.0.journal.as_ref());
            let actions = consensus.handle_request_start_view(PlaneKind::Metadata, &header);
            let (local_actions, wire_actions) = split_local_actions(actions);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &local_actions).await;
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &wire_actions).await;
            }
            return;
        }
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.group))
        else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::RequestStartView,
        ) {
            return;
        }
        let consensus = partition.consensus();
        let serving_primary = consensus.is_primary()
            && consensus.is_normal()
            && consensus.log_view() == consensus.view()
            && !partition.read_history_is_changing();
        if partition.history_is_busy() && !serving_primary {
            partition.defer_view_transition(MessageBag::RequestStartView(msg.clone()));
            return;
        }
        refresh_partition_dvc_suffix(partition);
        let consensus = partition.consensus();
        let actions = consensus.handle_request_start_view(PlaneKind::Partitions, &header);
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        // Wire to BOTH, like every other partition site: the journal
        // dispatcher owns SendPrepareOk and the debug durable-before-send
        // tripwire, and skipping it would drop both silently the day this
        // handler emits one.
        dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions).await;
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: StreamsFrontend,
    {
        let header = *msg.header();
        let target = header.replica;
        // Snapshot the config-overridable chunk ceiling once; both plane
        // branches below serve the same per-round window.
        let repair_chunk_max = self.repair_chunk_max.get();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            // A primary-elect can need bodies held only by ViewChange peers.
            // Other requesters may receive only those peers' committed prefix.
            if consensus.is_transferring() {
                // Transfer can replace these entries.
                return;
            }
            if !matches!(consensus.status(), Status::Normal | Status::ViewChange) {
                return;
            }
            let Some(journal) = planes.0.journal.as_ref() else {
                return;
            };
            let journal = journal.handle();
            let cluster = consensus.cluster();
            let self_id = consensus.replica();
            let to_op = repair_serve_ceiling(&header, consensus);
            // Skip the compacted prefix (below the snapshot floor) in one
            // RangeEvicted notice, then serve contiguously until the range
            // ends or the WAL runs out.
            //
            // Floored, not just walked up to: `validate` asks only for
            // `1 <= from_op <= to_op`, and the walk steps op by op with no `.await`,
            // so a peer sending `from_op = 1` against a large compacted frontier pins
            // the pump against a 10 ms tick. Nothing at or below the watermark is
            // servable anyway. The partition arm jumps to `retained_from` likewise.
            let mut from_op = header.from_op.max(journal.snapshot_op() + 1);
            if !consensus.view_log_entry_proven(from_op) {
                return;
            }
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
                    Command::RangeEvicted,
                    header.nonce,
                    from_op,
                    header.group,
                    consensus.partition_incarnation(),
                )
                .await;
                self.send_repair_range_reply(
                    cluster,
                    self_id,
                    target,
                    Command::RepairDone,
                    header.nonce,
                    header.from_op.saturating_sub(1),
                    header.group,
                    consensus.partition_incarnation(),
                )
                .await;
                return;
            }
            if from_op > header.from_op {
                self.send_repair_range_reply(
                    cluster,
                    self_id,
                    target,
                    Command::RangeEvicted,
                    header.nonce,
                    from_op,
                    header.group,
                    consensus.partition_incarnation(),
                )
                .await;
            }
            let chunk_end = to_op.min(from_op.saturating_add(repair_chunk_max - 1));
            let mut served_through = from_op.saturating_sub(1);
            for op in from_op..=chunk_end {
                if !consensus.view_log_entry_proven(op) {
                    break;
                }
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
                Command::RepairDone,
                header.nonce,
                served_through,
                header.group,
                consensus.partition_incarnation(),
            )
            .await;
            return;
        }
        let namespace = IggyNamespace::from_raw(header.group);
        let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::RequestPrepares,
        ) {
            return;
        }
        if !matches!(
            partition.consensus().status(),
            Status::Normal | Status::ViewChange
        ) || partition.view_truncation_pending()
        {
            return;
        }
        let cluster = partition.consensus().cluster();
        let self_id = partition.consensus().replica();
        // The frontier bounds the serve, not `commit_max` alone, mirroring the
        // metadata twin: a rejoining backup needs the BODIES of the adopted
        // suffix above the commit point. Its ack for those ops is withheld
        // until the body is journaled, and the primary's retransmit is dropped
        // by the backup gap check (adoption already advanced its sequencer to
        // the head), so repair is the only channel that can deliver them.
        let to_op = repair_serve_ceiling(&header, partition.consensus());
        // `None` means the journal holds NOTHING, not "nothing was evicted":
        // the partition journal is memory-only and `clear_all` wipes the
        // evicted ring with it, so a freshly installed or freshly restarted
        // peer answers `None` for every op it once had. Reading that as "no
        // eviction" served a bare `RepairDone`, left the requester's floor at
        // `None`, and `FloorRefused` (the route that arms a partition state
        // transfer once repair proves a gap unreachable) never fired: a
        // lagging replica on an idle partition spun repair forever against a
        // peer-sticky retry. An empty journal instead reports eviction from
        // the commit frontier, which refuses the floor into a transfer (the
        // empty window passes the completeness check) and heals in one round.
        let retained_from = partition
            .log
            .journal()
            .inner
            .repair_retained_from()
            .unwrap_or_else(|| partition.consensus().commit_min().saturating_add(1));
        let mut from_op = header.from_op;
        if !partition.consensus().view_log_entry_proven(from_op) {
            return;
        }
        if retained_from > from_op {
            self.send_repair_range_reply(
                cluster,
                self_id,
                target,
                Command::RangeEvicted,
                header.nonce,
                retained_from,
                header.group,
                partition.created_revision(),
            )
            .await;
            from_op = retained_from;
        }
        let chunk_end = to_op.min(from_op.saturating_add(repair_chunk_max - 1));
        let mut served_through = from_op.saturating_sub(1);
        for op in from_op..=chunk_end {
            if !partition.consensus().view_log_entry_proven(op) {
                break;
            }
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
            Command::RepairDone,
            header.nonce,
            served_through,
            header.group,
            partition.created_revision(),
        )
        .await;
        tracing::info!(
            shard = self.id,
            namespace_raw = header.group,
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
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_repair_prepare(&self, msg: Message<RepairPrepareHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        tracing::debug!(
            shard = self.id,
            op = msg.header().0.op,
            namespace_raw = msg.header().0.group,
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
            new.command = Command::Prepare;
        });
        let header = *msg.header();
        let planes = self.plane.inner();
        // Legacy acceptance: pre-upgrade metadata WAL entries were journaled
        // before prepares stamped `consensus.group()`, and repair ships
        // stored bytes verbatim, so without it a mixed-version metadata repair
        // re-ships the same 0-stamped entries forever.
        //
        // Keyed on the OPERATION, not on whether partition 0/0/0 exists: raw
        // namespace 0 is `IggyNamespace::new(0, 0, 0)` and ids slab-allocate
        // from 0, so 0/0/0 is the first partition every cluster creates -- on a
        // single-shard node a "no partition 0 materialised" conjunct goes false
        // the moment one topic exists and disables this migration exactly where
        // it is needed. `is_metadata_plane` is the plane's OWN applicability
        // predicate (the session ops `Register`/`Logout` replicate here without
        // being metadata mutations, so `is_metadata` alone is too narrow), which
        // is why both sites share it rather than re-deriving the set.
        let metadata_plane_op = header.operation.is_metadata_plane();
        let legacy_metadata_claim = header.group == 0 && metadata_plane_op;
        if let Some(ref consensus) = planes.0.consensus
            && (consensus.group() == header.group || legacy_metadata_claim)
        {
            let session = *self.metadata_repair.borrow();
            let Some(session) = session else {
                return;
            };
            if header.op > session.to_op {
                return;
            }
            let is_primary_elect = consensus.is_primary_elect();
            let commit_min = consensus.commit_min();
            let closes_proof_gap = consensus
                .view_log_repair_op()
                .is_none_or(|op| op == header.op);
            // In place: runs once per repaired prepare, and the clone is two Vecs.
            let in_scope = consensus
                .with_pending_view_log(|pending| {
                    repair_op_in_scope(Some(pending), is_primary_elect, commit_min, header.op)
                })
                .unwrap_or_else(|| {
                    repair_op_in_scope(None, is_primary_elect, commit_min, header.op)
                });
            if !in_scope {
                return;
            }
            // Applies to both planes, and is why a backup parks a log at all. The
            // view already decided which prepare belongs at this op; a different
            // one forks the log. An op the parked log omits is unconstrained.
            //
            // Which covers most of the range under `pending.commit_max`: the merged
            // log names headers only from the DVC suffixes, and those span
            // `commit..=op` per sender. Identity below that rests on crash-stop --
            // a committed op is the quorum's op. `verify_prepare_integrity` below
            // guards corruption; neither guards Byzantine faults.
            let disagrees = consensus
                .with_pending_view_log(|pending| {
                    pending
                        .headers
                        .iter()
                        .chain(pending.committed_elsewhere.iter())
                        .find(|expected| {
                            expected.op == header.op
                                && dvc_header_kind(expected) != DvcHeaderKind::Blank
                        })
                        .is_some_and(|expected| expected.checksum != header.checksum)
                })
                .unwrap_or(false);
            if disagrees {
                tracing::warn!(
                    shard = self.id,
                    op = header.op,
                    "discarding repaired prepare that disagrees with the merged log"
                );
                return;
            }
            // Recompute both integrity fields before durable storage: everything
            // above treats `header.checksum` as an opaque token, so a corrupted
            // frame passes whenever its flipped value satisfies the comparisons.
            if let Err(reason) = verify_prepare_integrity(&header, msg.as_slice()) {
                tracing::warn!(
                    shard = self.id,
                    op = header.op,
                    "discarding repaired prepare: {reason}"
                );
                return;
            }
            let Some(journal) = planes.0.journal.as_ref() else {
                return;
            };
            let journal = journal.handle();
            #[allow(clippy::cast_possible_truncation)]
            let resident_checksum = journal.header(header.op as usize).map(|held| held.checksum);
            // Even a resident body needs current-view evidence before it can close a proof gap.
            #[allow(clippy::cast_possible_truncation)]
            let held_at = |op: u64| {
                journal
                    .header(op as usize)
                    .map(|held| (*held, consensus.view_log_entry_proven(op)))
            };
            let evidence = consensus
                .with_pending_view_log(|pending| {
                    repaired_prepare_evidence(&header, consensus.view(), Some(pending), held_at)
                })
                .unwrap_or_else(|| {
                    repaired_prepare_evidence(&header, consensus.view(), None, held_at)
                });
            if evidence == RepairEvidence::Contradicted
                || (evidence == RepairEvidence::Unproven
                    && !consensus.view_log_proof_covers(header.op))
            {
                tracing::debug!(
                    shard = self.id,
                    op = header.op,
                    ?evidence,
                    "refusing a repaired metadata prepare the view log does not vouch for"
                );
                self.retarget_metadata_repair(consensus).await;
                return;
            }
            // Below the divergence, integrity and evidence returns, above the two
            // under it.
            //
            // Only silence should age the stream, and only a frame this replica
            // would have accepted proves anything is being served. A forked or
            // corrupted frame is neither: the peer re-serves the same stored bytes
            // every re-request, so crediting those holds off the retry forever.
            //
            // Still above the dedup return: a re-request re-serves ops already
            // held, and a stream re-covering ground is still a stream.
            //
            // Clock only -- the frame has no sender and no nonce, so it cannot be
            // attributed. The budget is cleared from the terminator and the walk.
            self.note_metadata_repair_clock();
            if let Some(resident_checksum) = resident_checksum {
                consensus.note_repaired_view_header(header, evidence);
                if resident_checksum != header.checksum {
                    let conflicts = self.metadata_repair_conflicts.get() + 1;
                    self.metadata_repair_conflicts.set(conflicts);
                    let mut pace = self.metadata_repair_conflict_warn.get();
                    // Paced: a slot nothing removes is re-served every retry round.
                    let admitted = pace.admit(self.ticks.get());
                    self.metadata_repair_conflict_warn.set(pace);
                    if admitted {
                        tracing::warn!(
                            shard = self.id,
                            op = header.op,
                            resident_checksum,
                            repaired_checksum = header.checksum,
                            conflicts,
                            "dropping a repaired metadata prepare whose slot holds a different \
                             entry; repair cannot fill it until reconcile removes that entry"
                        );
                    }
                }
                // Also on a conflict: a divergent removal that failed (a busy or
                // failed WAL rewrite) leaves the fence on this op, and on a backup
                // only this op's repair proves the log again.
                if closes_proof_gap {
                    self.reconcile_metadata_view_divergence().await;
                }
                return;
            }
            // Journal only in a state the superblock holds. A refill toward a view
            // this replica has not proven is no history of its certified view, and a
            // restart drops it only when it finds that later view recorded.
            if !planes.0.persist_superblock_if_needed(consensus).await {
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
            consensus.extend_history_certificate(header.op, header.checksum);
            // The body landing is invisible to the head and the commit point: a
            // backup repairing under a `StartView` it already adopted sits at the
            // announced head with its commit point unmoved. Leave the suffix
            // snapshot tagged as current and the next `DoViewChange` reports this
            // op header-only, which the merge reads as proof this replica never
            // journaled it, one nack away from truncating an op it is about to
            // acknowledge.
            consensus.note_journal_mutation();
            consensus.note_repaired_view_header(header, evidence);
            // Contiguous-frontier advance, mirroring
            // `apply_repaired_prepare`: DVC advertises the sequencer, so a
            // hole below a repaired op must stall the advance rather than
            // mint an election candidate with an unwalkable log.
            let previous_frontier = consensus.sequencer().current_sequence();
            #[allow(clippy::cast_possible_truncation)]
            let update = repaired_frontier_update(previous_frontier, |op| {
                journal.header(op as usize).map(|header| *header)
            });
            if let Some((frontier, frontier_checksum)) = update {
                consensus.sequencer().set_sequence(frontier);
                consensus.set_last_prepare_checksum(frontier_checksum);
            }
            if closes_proof_gap {
                self.reconcile_metadata_view_divergence().await;
            }
            return;
        }
        // A metadata-plane op that did not match above (no metadata consensus on
        // this shard, or a namespace neither plane claims) is DROPPED, never
        // offered to the partition arm. Falling through would let a metadata
        // prepare reach `apply_repaired_prepare`: it journals nothing and never
        // reaches the frontier update, but it resets the partition repair
        // session's idle ticks, masking a genuine stall.
        if metadata_plane_op {
            tracing::debug!(
                shard = self.id,
                op = header.op,
                operation = ?header.operation,
                namespace_raw = header.group,
                "dropping a metadata-plane repair prepare this shard cannot journal"
            );
            return;
        }
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.group))
        else {
            return;
        };
        // The partition arm reaches the WAL via `apply_repaired_prepare` with no
        // view fence and no ack, so this is its only integrity gate. Without it a
        // repaired partition prepare is journaled on the serving peer's word alone.
        if let Err(reason) = verify_prepare_integrity(&header, msg.as_slice()) {
            tracing::warn!(
                shard = self.id,
                op = header.op,
                namespace_raw = header.group,
                "discarding repaired partition prepare: {reason}"
            );
            return;
        }
        let closes_proof_gap = partition
            .consensus()
            .view_log_repair_op()
            .is_none_or(|op| op == header.op);
        let ingest = partition.apply_repaired_prepare(msg).await;
        if closes_proof_gap {
            let pending = partition.consensus().pending_view_log();
            reconcile_partition_view_divergence(self.id, partition, pending.as_ref()).await;
        }
        if ingest == RepairIngest::Unvouched {
            tracing::debug!(
                shard = self.id,
                op = header.op,
                namespace_raw = header.group,
                "refusing a repaired partition prepare the view log does not vouch for"
            );
            self.retarget_partition_repair(partition).await;
        }
    }

    /// Repair stream terminator: `RangeEvicted` settles the partition commit
    /// floor candidate; `RepairDone` runs the commit walk over the repaired
    /// window and closes the session.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_repair_range_reply(&self, msg: &Message<RepairRangeReplyHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        if let Some(ref consensus) = planes.0.consensus
            && consensus.group() == header.group
        {
            let session = *self.metadata_repair.borrow();
            let Some(session) = session else {
                return;
            };
            if (header.replica, header.nonce) != (session.peer, session.nonce) {
                return;
            }
            match header.command {
                Command::RepairDone => {
                    let before = consensus.commit_min();
                    planes.0.commit_journal().await;
                    // Completion is decided by the LOCAL walk, not the
                    // peer's served-through claim: repair frames ride a
                    // lossy best-effort bus, so a fully-served stream can
                    // still arrive with holes. Anything short keeps the
                    // session armed; the next chunk is pulled as soon as this
                    // one is walked (the window is served in
                    // `REPAIR_CHUNK_MAX` slices), and a window still holed
                    // below `served_through` is left to the retry timer.
                    let commit_min = consensus.commit_min();
                    let done = commit_min >= session.to_op;
                    let proof_advanced = consensus
                        .view_log_repair_op()
                        .is_some_and(|op| op != session.from_op);
                    tracing::info!(
                        shard = self.id,
                        through_op = header.op,
                        commit_min,
                        done,
                        "metadata journal repair walked"
                    );
                    // The one attributable credit: fenced on nonce and peer
                    // above, so it came from the targeted peer, and the walk moved.
                    // Gating on the walk and not the frame keeps a peer that
                    // terminates every round while serving nothing useful from
                    // clearing its own budget and becoming un-rotatable.
                    if done || commit_min > before || proof_advanced {
                        self.note_metadata_repair_walked();
                    }
                    if done {
                        *self.metadata_repair.borrow_mut() = None;
                    } else if proof_advanced || repair_chunk_walked(before, commit_min, header.op) {
                        let from_op = consensus.view_log_repair_op().unwrap_or(commit_min + 1);
                        if let Some(live) = self.metadata_repair.borrow_mut().as_mut() {
                            live.from_op = from_op;
                        }
                        self.send_request_prepares(
                            consensus,
                            session.peer,
                            session.nonce,
                            from_op,
                            session.to_op,
                            header.group,
                            consensus.partition_incarnation(),
                        )
                        .await;
                    }
                }
                Command::RangeEvicted => {
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
                    //
                    // Never as primary-elect. A transfer replaces snapshot-shaped
                    // state wholesale, and this replica has a merged log parked
                    // against that state naming ops it was just told it cannot
                    // serve; installing under it starts the view over a log the new
                    // state no longer matches. Re-target instead, and let the
                    // view-change timeout escalate if nobody can serve it.
                    //
                    // DROP the session before returning. The serving peer follows
                    // `RangeEvicted` with `RepairDone(from_op - 1)` on the same
                    // nonce, and a `RepairDone` at or below `commit_min` walks
                    // nothing, so `repair_chunk_walked` is trivially true and that
                    // arm re-requests at once: no tick gate, no debounce, no attempt
                    // burned -- an unthrottled request/reply loop across two pumps.
                    // Dropping first lands the trailing frame on the `is_none`
                    // guard, as it did before this arm existed.
                    if consensus.is_primary_elect() {
                        tracing::warn!(
                            shard = self.id,
                            peer = header.replica,
                            retained_from = header.op,
                            local_commit = consensus.commit_min(),
                            "merged-log repair peer evicted the requested range; \
                             re-targeting rather than transferring state mid view change"
                        );
                        // Definitive, not a stall: this sender has said it cannot
                        // serve the window, so rotate now rather than spend a retry
                        // interval on a stream that will not come. Still charge a
                        // round, so a quorum that all answer this way stops asking
                        // instead of cycling the ring until the timeout.
                        if self.burn_metadata_repair_attempt(consensus.view()) {
                            *self.metadata_repair.borrow_mut() = None;
                            tracing::warn!(
                                shard = self.id,
                                from_op = session.from_op,
                                to_op = session.to_op,
                                "merged-log repair exhausted its senders; leaving the view \
                                 change to its timeout"
                            );
                            return;
                        }
                        self.rotate_stalled_metadata_repair(
                            consensus,
                            header.replica,
                            session.from_op,
                            session.to_op,
                        )
                        .await;
                        return;
                    }

                    // The floor must also be ABOVE the op this replica needs. A
                    // peer behind the requested window walks its serve range off
                    // the end and answers `RangeEvicted` at the requested floor
                    // itself, having retained nothing and evicted nothing;
                    // converting on that arms a transfer against a replica with
                    // less state than this one and fences repair for a full
                    // transfer backoff. Drop the session and let the level trigger
                    // re-request from the primary instead.
                    //
                    // The same for a peer that has just refused this replica a
                    // transfer (see `MetadataRepairSession::transfer_refused`),
                    // whatever its floor: converting asks it again.
                    if header.op <= consensus.commit_min() + 1 || session.transfer_refused {
                        // Charge the round, and ACT on exhaustion. `gap_repair_peer`
                        // re-picks the primary deterministically, so dropping the
                        // session on its own re-arms the same peer at the debounce
                        // interval forever: the stall path never runs, so rotation
                        // is never reached and the state-transfer escalation this
                        // guard replaced stays out of reach.
                        let budget_spent = self.burn_metadata_repair_attempt(consensus.view());
                        tracing::warn!(
                            shard = self.id,
                            peer = header.replica,
                            retained_from = header.op,
                            local_commit = consensus.commit_min(),
                            transfer_refused = session.transfer_refused,
                            budget_spent,
                            "metadata repair peer cannot serve the requested range; dropping \
                             the session, or rotating after one retry interval once the budget \
                             is spent"
                        );
                        if budget_spent {
                            // Not at once: when no peer can serve, each rotation ends in
                            // another direct refusal and the ring cycles at network rate.
                            // Hold the session for one stall round, as against a silent
                            // peer, under a nonce no reply carries: the trailing
                            // `RepairDone` cannot re-request, and only the stall sweep
                            // rotates it.
                            if let Some(live) = self.metadata_repair.borrow_mut().as_mut() {
                                live.nonce = iggy_common::random_id::get_uuid();
                                live.idle_ticks = 0;
                            }
                            return;
                        }
                        *self.metadata_repair.borrow_mut() = None;
                        return;
                    }
                    if consensus.state_transfer_stage() == consensus::StateTransferStage::Idle {
                        *self.metadata_repair.borrow_mut() = None;
                        consensus.begin_state_transfer_await();
                        self.primary_rebalance.borrow_mut().reset_authority();
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
        // Counted BEFORE the `&mut partition` below exists, and only when an arm
        // is possible at all: see the StartView site for why the scan is gated
        // rather than replaced with a counter.
        let transfers_inflight = if Self::may_arm_partition_transfer(&planes.1.0, header.group) {
            self.partition_transfers_inflight()
        } else {
            0
        };
        let config = planes.1.0.config().clone();
        let namespace = IggyNamespace::from_raw(header.group);
        let Some(partition) = planes.1.0.get_mut_by_ns(&namespace) else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            header.command,
        ) {
            return;
        }
        // Repair replies do not replace history. complete_repair waits for
        // file results; fencing live prepares here would create another gap.
        let Some(session) = partition.repair else {
            return;
        };
        if (header.replica, header.nonce) != (session.peer, session.nonce) {
            return;
        }
        // Twin of the `apply_repaired_prepare` gate: a primary-elect's merged-log
        // session legitimately runs outside `Normal`, and dropping it here would
        // discard the terminator that closes the window it is repairing.
        if !consensus::repair_session_live(partition.consensus())
            || partition.consensus().view() != session.view
        {
            partition.repair = None;
            return;
        }
        match header.command {
            Command::RangeEvicted => partition.note_repair_floor(header.op.saturating_sub(1)),
            Command::RepairDone => {
                // `complete_repair` walks the window and clears the session
                // only when the LOCAL commit frontier reached the requested
                // op (the peer's served-through claim proves nothing about
                // delivery on a lossy bus). Proof moves backward through gaps,
                // then may need a higher canonical header's body. Either change
                // pulls the next chunk without waiting for the retry timer.
                let before = partition.consensus().commit_min();
                if let partitions::RepairConclusion::FloorRefused { floor, to_op } =
                    partition.complete_repair(&config).await
                {
                    if partition.consensus().state_transfer_stage()
                        == consensus::StateTransferStage::Idle
                        && partition.transfer_rearm.is_none()
                    {
                        // Repair proved the gap below the floor is neither
                        // locally durable nor repairable: the one authoritative
                        // "repair is impossible" signal. Arm from Idle only; a
                        // transfer already in flight owns the stage, and its own
                        // post-install tail repair can re-raise through this
                        // path, each round lifting the floor, so it converges.
                        // A pending scheduled re-arm owns recovery likewise --
                        // arming here would defeat its backoff.
                        tracing::info!(
                            shard = self.id,
                            namespace_raw = header.group,
                            floor,
                            to_op,
                            peer = header.replica,
                            attempts = partition.transfer_attempts(),
                            "partition repair floor unreachable; converting to state transfer"
                        );
                        // Same reason as the StartView arm: this id becomes
                        // `session.peer` and later reaches the peer rotation.
                        if self.peer_is_known(header.replica, "RepairRangeReply") {
                            partition.consensus().begin_state_transfer_await();
                            let _ = self
                                .arm_partition_transfer(
                                    partition,
                                    header.replica,
                                    transfers_inflight,
                                )
                                .await;
                        }
                    } else {
                        // The refusal cleared the repair session, so falling
                        // through would log "repair complete" right after
                        // the refusal diagnostic. The in-flight transfer (or
                        // the scheduled re-arm) owns recovery from here.
                        tracing::info!(
                            shard = self.id,
                            namespace_raw = header.group,
                            floor,
                            to_op,
                            "partition repair floor refused; transfer in flight or scheduled"
                        );
                    }
                    return;
                }
                if partition.repair.is_none() {
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = header.group,
                        through_op = header.op,
                        "partition journal repair complete"
                    );
                } else {
                    let commit_min = partition.consensus().commit_min();
                    let proof_op = partition.consensus().view_log_repair_op();
                    let next = partition.repair.as_mut().and_then(|live| {
                        let from_op = proof_op.filter(|&op| op != live.from_op).or_else(|| {
                            partition_repair_next_chunk(before, commit_min, live.fetch_to_op)
                        })?;
                        live.from_op = from_op;
                        Some((live.peer, live.nonce, from_op, live.fetch_to_op))
                    });
                    if let Some((peer, nonce, from_op, to_op)) = next {
                        self.send_request_prepares(
                            partition.consensus(),
                            peer,
                            nonce,
                            from_op,
                            to_op,
                            header.group,
                            partition.created_revision(),
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
    async fn send_request_prepares<P>(
        &self,
        consensus: &VsrConsensus<B, P>,
        target: u8,
        nonce: u128,
        from_op: u64,
        to_op: u64,
        namespace: u64,
        partition_incarnation: u64,
    ) where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        // Proof walks backward. End the chunk at its hole instead of re-serving
        // the already held suffix for every missing predecessor.
        let (from_op, to_op) = consensus
            .view_log_repair_op()
            .map_or((from_op, to_op), |missing| {
                if consensus.view_log_entry_proven(missing) {
                    return (missing, to_op.max(missing));
                }
                (
                    missing
                        .saturating_sub(self.repair_chunk_max.get() - 1)
                        .max(consensus.commit_min() + 1),
                    missing,
                )
            });
        if from_op == 0 || from_op > to_op {
            return;
        }
        let msg = Message::<RequestPreparesHeader>::new(size_of::<RequestPreparesHeader>())
            .transmute_header(|_, h: &mut RequestPreparesHeader| {
                h.command = Command::RequestPrepares;
                h.cluster = consensus.cluster();
                h.replica = consensus.replica();
                h.view = consensus.view();
                h.nonce = nonce;
                h.from_op = from_op;
                h.to_op = to_op;
                h.group = namespace;
                h.partition_incarnation = partition_incarnation;
                h.size = size_of::<RequestPreparesHeader>() as u32;
                h.seal();
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
        owned.as_mut_slice()[COMMAND_OFFSET] = Command::RepairPrepare as u8;
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
        command: Command,
        nonce: u128,
        op: u64,
        namespace: u64,
        partition_incarnation: u64,
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
                h.group = namespace;
                h.partition_incarnation = partition_incarnation;
                h.size = size_of::<RepairRangeReplyHeader>() as u32;
                h.seal();
            });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    /// Partition-plane twin of [`Self::advance_pending_metadata_view`].
    ///
    /// No `RequestPrepares` stream to arm: the partition journal is not durable
    /// yet, so coverage either holds or a peer must retransmit. Same invariant
    /// either way: the view does not start until this replica can serve its log.
    #[allow(clippy::future_not_send)]
    async fn advance_pending_partition_view(&self, namespace: IggyNamespace)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        let partitions = self.plane.partitions();
        let started = {
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                return;
            };
            if partition.history_is_busy() {
                return;
            }
            if !partition.consensus().is_primary_elect() {
                return;
            }
            let Some(pending) = partition.consensus().pending_view_log() else {
                return;
            };
            if let Some((diverged_op, agreed_op)) =
                partition_applied_divergence(partition, &pending)
            {
                self.resync_diverged_partition(namespace, partition, diverged_op, agreed_op);
                return;
            }
            // Before the scan can mean anything: the repair ingest skips an op it
            // already holds a header for, so the scan would report a gap nothing
            // fills. Backups reach this on StartView adoption; a primary-elect has no
            // adoption to hang it off.
            if partition.has_io_dispatcher()
                && let Some(from_op) = partition_view_divergence_from(partition, Some(&pending))
            {
                partition.defer_view_truncation(from_op..=u64::MAX, Vec::new(), None);
                return;
            }
            reconcile_partition_view_divergence(self.id, partition, Some(&pending)).await;
            if partition.view_truncation_pending()
                || (partition.consensus().view_log_unproven()
                    && partition.consensus().view_log_repair_op().is_none())
            {
                return;
            }
            if let Some(missing_op) = partition.consensus().view_log_repair_op() {
                self.request_partition_view_repair(partition, missing_op, pending.op_head, None)
                    .await;
                return;
            }
            let consensus = partition.consensus();
            // Identity, not presence: see the metadata twin. The floor is the local
            // commit point, the partition twin of the metadata snapshot floor:
            // `evict_prefix` clears the header vec for the flushed (committed)
            // prefix, so a survivor that flushes on every commit holds NO resident
            // header for the op the merged window opens on. Demanding one parks the
            // primary-elect in `ViewChange` forever, and the rotation lands
            // primaryship on whichever replica still has its window resident -- a
            // fresh rejoiner with nothing but repair-ingested entries, which then
            // cannot serve the state transfer it itself needs. A committed op
            // cannot diverge from the merged log, and its bytes stay serveable
            // from the evicted ring or the flushed segments.
            let floor = ScanFloor {
                repair_floor: consensus.commit_min(),
                commit_min: consensus.commit_min(),
            };
            // Ring AND resident, one pass. `header_by_op` reads the resident vec
            // only, while `commit_messages` evicts up to `commit_max` (the cluster
            // frontier), so a primary-elect with an apply backlog reads `None` for
            // ops it holds in the repair ring and parks on a hole that is not one.
            let missing = {
                let journal = partition.log.journal();
                let window = journal
                    .inner
                    .repair_headers_in(floor.opens_at(&pending)..=pending.op_head);
                first_op_not_covered(&pending, floor, |op| window.get(&op).copied())
            };
            if let Some(missing_op) = missing {
                tracing::debug!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    missing_op,
                    op_head = pending.op_head,
                    "partition view change waiting on op {missing_op} before starting the view"
                );
                // And a way out of the wait: nothing else fetches this op.
                // `maybe_request_partition_repair` refuses outside `Normal` and
                // the sweep's gap detector needs `probe.normal`. Partition twin of
                // the metadata plane's view repair.
                self.request_partition_view_repair(partition, missing_op, pending.op_head, None)
                    .await;
                return;
            }

            let actions = consensus.start_pending_view(PlaneKind::Partitions);
            let (local_actions, wire_actions) = split_local_actions(actions);
            // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
            // executes there (`dispatch_vsr_actions` bails on `journal: None`)
            // and `CommitJournal` is a no-op in both.
            dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
            // `start_pending_view` flips this replica into `Normal` for the new
            // view, so the `StartView` it emits advertises a view the superblock
            // must already record. Same gate as the `on_do_view_change` and
            // `on_start_view` partition arms.
            dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions)
                .await;
            local_actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
        };
        if started {
            let config = partitions.config();
            if let Some(partition) = partitions.get_mut_by_ns(&namespace) {
                partition.commit_journal(config).await;
            }
        }
    }

    /// Re-request the remaining repair window when the stream has gone quiet.
    ///
    /// Repair frames are fire-and-forget, so a lost one leaves the session armed
    /// forever with the commit walk pinned below the frontier.
    ///
    /// Re-requests from the SAME peer while its stall budget holds. A peer that
    /// never answers is a different problem, and
    /// [`Self::rotate_stalled_metadata_repair`] owns it: the target lives on the
    /// session, so it must be replaced there rather than shadowed for one send.
    #[allow(clippy::future_not_send)]
    async fn retry_stalled_metadata_repair<P>(&self, consensus: &VsrConsensus<B, P>)
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        // `ViewChange` too: a parked view change repairs toward its merged log
        // and cannot start until the window fills. Gating on `Normal` alone
        // defers a dropped frame to the 500-tick escalation, and closes the one
        // session that legitimately runs outside `Normal`.
        let repairing_view = consensus.is_primary_elect();

        // Closed at the TOP of the tick, not after an idle window: a standing
        // session fences every arming site and holds the gap debounce at zero
        // (`recovery_owned`), so waiting a full retry interval to notice costs
        // that interval on every arm behind it.
        let superseded = self.metadata_repair.borrow().is_some_and(|session| {
            metadata_repair_superseded(
                &session,
                consensus.commit_min(),
                consensus.view(),
                consensus.is_normal(),
                repairing_view,
            )
        });
        if superseded {
            tracing::debug!(
                shard = self.id,
                commit_min = consensus.commit_min(),
                view = consensus.view(),
                "metadata repair session walked or superseded; closing it"
            );
            *self.metadata_repair.borrow_mut() = None;
            self.note_metadata_repair_walked();
            return;
        }

        // Stall retry (mirrors `tick_partitions`): a lost frame must not wedge it.
        let repair_retry_ticks = self.repair_retry_ticks.get();
        let stalled = {
            let mut session = self.metadata_repair.borrow_mut();
            session.as_mut().and_then(|session| {
                session.idle_ticks += 1;
                if session.idle_ticks < repair_retry_ticks {
                    return None;
                }
                session.idle_ticks = 0;
                Some((session.peer, session.nonce, session.from_op, session.to_op))
            })
        };
        if let Some((peer, nonce, session_from_op, to_op)) = stalled {
            let from_op = consensus.view_log_repair_op().unwrap_or_else(|| {
                stalled_repair_from_op(session_from_op, consensus.commit_min(), repairing_view)
            });
            if from_op > to_op {
                // `from_op` past `to_op` without `commit_min` reaching it: the
                // primary-elect window above starts at the merged log's commit
                // point, which can sit above what this replica has walked. The
                // top-of-tick check closes the ordinary case; this closes the
                // one it cannot see. Leaving it armed wedges the replica: no
                // `RepairDone` clears a window the walk is already past, and the
                // `is_none` gate then blocks the session the ops above it need.
                tracing::info!(
                    shard = self.id,
                    to_op,
                    peer,
                    "metadata repair window fully requested; closing the stalled session"
                );
                *self.metadata_repair.borrow_mut() = None;
                self.note_metadata_repair_walked();
            } else if self.burn_metadata_repair_attempt(consensus.view()) {
                self.rotate_stalled_metadata_repair(consensus, peer, from_op, to_op)
                    .await;
            } else {
                tracing::info!(
                    shard = self.id,
                    from_op,
                    to_op,
                    peer,
                    "metadata repair stalled; re-requesting remaining window"
                );
                self.send_request_prepares(
                    consensus,
                    peer,
                    nonce,
                    from_op,
                    to_op,
                    consensus.group(),
                    consensus.partition_incarnation(),
                )
                .await;
            }
        }
    }

    /// Re-arm a repair session that spent its stall budget against another replica.
    ///
    /// A session pins its peer and fences every arming site while it stands, so a
    /// peer that cannot answer wedges the walk harder than having no session at
    /// all. Past the budget the session is dropped and re-armed one step on; an
    /// ordinary lost frame is re-requested long before that. Mirrors the partition
    /// rotation in [`Self::tick_partitions`].
    ///
    /// Two rings, because two things decide who can serve. A `Normal` backup is
    /// repairing its committed tail and any replica ahead of it will do, so it
    /// walks the whole cluster ring ([`next_repair_peer`]). A primary-elect is
    /// repairing toward a merged log, and only the `DoViewChange` senders that
    /// named the op can serve it: walking the whole ring lands on a replica that
    /// answers `RangeEvicted` for a range it never held.
    ///
    /// Does NOT spend the budget. It lives on the shard so rotation cannot reset it
    /// (see [`Self::metadata_repair_attempts`]); clearing it here would bound one
    /// round and re-target forever.
    #[allow(clippy::future_not_send)]
    async fn rotate_stalled_metadata_repair<P>(
        &self,
        consensus: &VsrConsensus<B, P>,
        peer: u8,
        from_op: u64,
        to_op: u64,
    ) where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        *self.metadata_repair.borrow_mut() = None;

        if consensus.is_primary_elect() {
            let sources = view_repair_sources(consensus, from_op);
            let Some(next_peer) = next_view_repair_peer(&sources, Some(peer)) else {
                // Only the quiet peer named this op. The session is dropped either
                // way: `advance_pending_metadata_view` re-scans on the next tick and
                // re-requests it, and a peer that never comes back leaves the
                // view-change timeout to escalate.
                tracing::warn!(
                    shard = self.id,
                    peer,
                    from_op,
                    "no other replica offers op {from_op} for the merged log; \
                     view change is stalled"
                );
                return;
            };
            tracing::warn!(
                shard = self.id,
                peer,
                next_peer,
                from_op,
                to_op,
                "merged-log repair stalled past its retry budget; re-arming from \
                 another do_view_change sender"
            );
            self.arm_metadata_repair_session(consensus, next_peer, from_op, to_op)
                .await;
            return;
        }

        let next_peer = next_repair_peer(consensus.replica(), peer, consensus.replica_count());
        if next_peer == peer {
            // The ring had nobody else to offer (a solo group, or a two-replica
            // cluster whose only peer went quiet). Dropping the session is still
            // right: it unfences the level trigger below, which re-requests the
            // window on the next tick.
            return;
        }
        tracing::warn!(
            shard = self.id,
            peer,
            next_peer,
            from_op,
            to_op,
            "metadata repair stalled past its retry budget; re-arming from another replica"
        );
        self.maybe_request_metadata_repair(consensus, next_peer)
            .await;
    }

    /// Re-request the metadata repair window from the view's primary after a
    /// repaired prepare the view log does not vouch for.
    ///
    /// The frame names no sender, so it cannot be blamed on the session's peer,
    /// but the primary's log is the view's log. A fresh nonce fences the replaced
    /// peer's late terminator, and the window restarts where a stall retry would.
    /// Nothing to do once the primary serves the session: a refused frame then
    /// comes from an earlier peer's late stream.
    #[allow(clippy::future_not_send)]
    async fn retarget_metadata_repair<P>(&self, consensus: &VsrConsensus<B, P>)
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        let primary = consensus.primary_index(consensus.view());
        if !consensus.is_normal() || primary == consensus.replica() {
            return;
        }
        let retarget = self
            .metadata_repair
            .borrow_mut()
            .as_mut()
            .filter(|session| session.peer != primary)
            .map(|session| {
                session.peer = primary;
                session.nonce = iggy_common::random_id::get_uuid();
                session.idle_ticks = 0;
                session.transfer_refused = false;
                (session.nonce, session.from_op, session.to_op)
            });
        let Some((nonce, session_from_op, to_op)) = retarget else {
            return;
        };
        // Only a primary-elect repairs toward its own merged log, and it never
        // gets here.
        let from_op = consensus.view_log_repair_op().unwrap_or_else(|| {
            stalled_repair_from_op(session_from_op, consensus.commit_min(), false)
        });
        tracing::warn!(
            shard = self.id,
            primary,
            from_op,
            to_op,
            "metadata repair served a prepare the view log does not vouch for; re-requesting \
             the window from the primary"
        );
        self.send_request_prepares(
            consensus,
            primary,
            nonce,
            from_op,
            to_op,
            consensus.group(),
            consensus.partition_incarnation(),
        )
        .await;
    }

    // The shard pump serializes these rewrites with append. The journal's drain
    // guard excludes concurrent compaction, but does not exclude append itself.
    #[allow(clippy::future_not_send)]
    async fn reconcile_metadata_view_divergence(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        let metadata = self.plane.metadata();
        let (Some(consensus), Some(journal)) = (&metadata.consensus, &metadata.journal) else {
            return;
        };
        while reconcile_metadata_view_log(self.id, consensus, journal.handle()).await
            == MetadataViewLogStep::PersistFirst
        {
            if !metadata.persist_superblock_if_needed(consensus).await {
                return;
            }
        }
    }

    /// Drive a parked view change to completion.
    ///
    /// A DVC quorum decides the log before this replica necessarily holds it, so
    /// the merged log parks in consensus and this replica stays in `ViewChange`,
    /// announcing and preparing nothing: `StartView` promises it can serve every
    /// op it names, and a backup adopting that head asks for the bodies at once.
    ///
    /// Check coverage, then start the view or pull missing bodies from a peer that
    /// offered them in its DVC. Only those peers: a cleared present bit means the
    /// body was never held or cannot be read back.
    #[allow(clippy::future_not_send)]
    async fn advance_pending_metadata_view(&self)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
    {
        let metadata = self.plane.metadata();
        let Some(ref consensus) = metadata.consensus else {
            return;
        };
        // Primary-elect only. A backup's parked `StartView` suffix is only what
        // its ingest verifies bodies against; driving repair from it would put a
        // rejoining node on the tail-repair path when its gap sits below every
        // peer's retention floor, racing the view probe that picks state transfer.
        if !consensus.is_primary_elect() {
            return;
        }
        // Before the coverage scan, and a primary-elect's only shot at it: the repair
        // ingest skips an op it already holds a header for, so a diverging entry would
        // never be replaced. No-op when nothing diverges.
        self.reconcile_metadata_view_divergence().await;
        if consensus.view_log_unproven() && consensus.view_log_repair_op().is_none() {
            return;
        }
        let Some(pending) = consensus.pending_view_log() else {
            return;
        };
        let Some(journal) = metadata.journal.as_ref() else {
            return;
        };

        // Floor on what this replica can be asked to hold before starting the view.
        // Entries at or below the snapshot watermark are compacted, so no repair puts
        // one back: demanding one parks the view change forever on an op already
        // applied and durable in the snapshot.
        let repair_floor = journal.handle().snapshot_op();
        let floor = ScanFloor {
            repair_floor,
            commit_min: consensus.commit_min(),
        };
        let missing = consensus.view_log_repair_op().or_else(|| {
            first_op_not_covered(&pending, floor, |op| {
                usize::try_from(op)
                    .ok()
                    .and_then(|slot| journal.handle().header(slot))
                    .map(|header| *header)
            })
        });

        let Some(missing_op) = missing else {
            let actions = consensus.start_pending_view(PlaneKind::Metadata);
            let (local_actions, wire_actions) = split_local_actions(actions);
            tracing::info!(
                shard = self.id,
                view = consensus.view(),
                op_head = pending.op_head,
                commit_max = pending.commit_max,
                "merged log is locally serveable; starting the view"
            );
            // Locals run BEFORE the persist await. `start_pending_view` has
            // already flipped this replica into a Normal primary, so yielding
            // with a still-empty pipeline lets a concurrent client submit mint
            // the next op below the inherited suffix, and the later rebuild
            // then pushes out of sequence. The locals must also survive a
            // failed persist, which fences only the wire sends.
            dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &local_actions).await;
            if metadata.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &wire_actions).await;
            }
            if local_actions
                .iter()
                .any(|action| matches!(action, VsrAction::CommitJournal))
                && !consensus.is_transferring()
            {
                metadata.commit_journal().await;
            }
            return;
        };

        if self.metadata_repair.borrow().is_some() {
            // Stream already running; the stall retry covers it drying up.
            return;
        }
        // Level-triggered, so it would re-arm against the head of the same source
        // list next tick -- including the sender that just answered `RangeEvicted`,
        // which is how the budget got spent. Once every sender has been asked and
        // charged, asking again is not progress; the view-change timeout is.
        //
        // Per view. A later view is a new merged log from a new quorum, so the
        // senders that refused this one say nothing about it.
        if self.metadata_repair_exhausted(consensus.view()) {
            tracing::debug!(
                shard = self.id,
                missing_op,
                view = consensus.view(),
                "merged-log repair is out of retries for this view; not re-arming"
            );
            return;
        }
        let sources = view_repair_sources(consensus, missing_op);
        let Some(peer) = sources.first().copied() else {
            // The merge only returns a startable log when some replica offered each
            // body, so an empty source list means that offer was withdrawn (peer
            // restarted, or moved on). Let the view-change timeout escalate.
            tracing::warn!(
                shard = self.id,
                missing_op,
                "no replica offers op {missing_op} for the merged log; view change is stalled"
            );
            return;
        };

        tracing::info!(
            shard = self.id,
            missing_op,
            peer,
            to_op = pending.op_head,
            "repairing toward the merged log before starting the view"
        );
        self.arm_metadata_repair_session(consensus, peer, missing_op, pending.op_head)
            .await;
    }

    /// Mint a metadata repair session and send its first request.
    ///
    /// Three sites arm one (the view-change scan, the tail-repair funnel, the
    /// stall rotation). Keeping the invariant here -- fresh nonce, arming view,
    /// clock from zero, a `from_op` the retry can reopen at -- stops a re-arm from
    /// shipping a window its own retry cannot reproduce.
    ///
    /// Callers decide whether to arm; this decides what an armed session is.
    #[allow(clippy::future_not_send)]
    async fn arm_metadata_repair_session<P>(
        &self,
        consensus: &VsrConsensus<B, P>,
        peer: u8,
        from_op: u64,
        to_op: u64,
    ) where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
    {
        let nonce = iggy_common::random_id::get_uuid();
        *self.metadata_repair.borrow_mut() = Some(MetadataRepairSession {
            nonce,
            from_op,
            to_op,
            view: consensus.view(),
            peer,
            idle_ticks: 0,
            transfer_refused: false,
        });
        self.send_request_prepares(
            consensus,
            peer,
            nonce,
            from_op,
            to_op,
            consensus.group(),
            consensus.partition_incarnation(),
        )
        .await;
    }

    /// Start metadata tail journal-repair from `peer` when the commit walk
    /// gap-stopped below the known frontier.
    ///
    /// Every TAIL arming site funnels through here -- `StartView` adoption, the
    /// commit-heartbeat backstop, the state-transfer fallbacks, and
    /// `tick_metadata`'s gap detector -- so the guards below are what make the
    /// level-triggered one idempotent.
    ///
    /// It does not decide the merged-log sessions:
    /// [`Self::advance_pending_metadata_view`] arms them and
    /// [`Self::rotate_stalled_metadata_repair`] re-targets them. Those repair
    /// toward a parked merged log rather than the commit frontier, from a sender
    /// that named the op rather than the primary, so none of the guards below fit.
    /// All three share [`Self::arm_metadata_repair_session`].
    #[allow(clippy::future_not_send)]
    async fn maybe_request_metadata_repair<P>(&self, consensus: &VsrConsensus<B, P>, peer: u8)
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        if !consensus.is_transferring()
            && consensus.view_log_unproven()
            && consensus.view_log_repair_op().is_none()
        {
            self.reconcile_metadata_view_divergence().await;
            if consensus.view_log_unproven() && consensus.view_log_repair_op().is_none() {
                return;
            }
        }
        // Never against self. A self-addressed `RequestPrepares` cannot be
        // delivered (the replica registry holds no entry for this node), and the
        // send fails AFTER the session is recorded, so the session would stand
        // forever: nothing advances `commit_min` to close it, the stall retry
        // re-sends to the same place, and `metadata_repair.is_some()` fences
        // every other arming site meanwhile.
        if peer == consensus.replica() {
            return;
        }
        if consensus.is_normal()
            && !consensus.is_transferring()
            && (consensus.commit_min() < consensus.commit_max() || consensus.view_log_unproven())
            && self.metadata_repair.borrow().is_none()
        {
            let from_op = consensus
                .view_log_repair_op()
                .unwrap_or_else(|| consensus.commit_min() + 1);
            let to_op = if consensus.view_log_unproven() {
                consensus
                    .sequencer()
                    .current_sequence()
                    .max(
                        consensus
                            .with_pending_view_log(|pending| pending.op_head)
                            .unwrap_or(0),
                    )
                    .max(from_op)
            } else {
                consensus.commit_max()
            };
            // Spent here rather than at the detector, so the edge-triggered
            // sites spend it too: an edge-armed repair that completes before
            // the next tick would otherwise leave the count saturated and hand
            // the next real gap an arm on its first tick.
            self.metadata_gap_ticks.set(0);
            tracing::info!(
                shard = self.id,
                from_op,
                to_op,
                peer,
                "metadata behind the group frontier; requesting repair"
            );
            self.arm_metadata_repair_session(consensus, peer, from_op, to_op)
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
                    h.command = Command::RequestStateTransfer;
                    h.cluster = consensus.cluster();
                    h.replica = consensus.replica();
                    h.nonce = nonce;
                    h.group = consensus.group();
                    h.partition_incarnation = consensus.partition_incarnation();
                    h.size = size_of::<RequestStateTransferHeader>() as u32;
                    h.seal();
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
        partition_incarnation: u64,
        descriptor: TransferDescriptor<'_>,
    ) where
        B: MessageBus,
    {
        let manifest = descriptor
            .offer
            .map(|(entries, _)| consensus::encode_state_manifest(entries));
        let total_size =
            size_of::<StateTransferTargetHeader>() + manifest.as_ref().map_or(0, Vec::len);
        let mut msg = Message::<StateTransferTargetHeader>::new(total_size);
        if let Some(manifest) = &manifest {
            msg.as_mut_slice()[size_of::<StateTransferTargetHeader>()..].copy_from_slice(manifest);
        }
        let msg = msg.transmute_header(|_, h: &mut StateTransferTargetHeader| {
            h.command = Command::StateTransferTarget;
            h.cluster = cluster;
            h.replica = self_id;
            h.nonce = nonce;
            h.group = namespace;
            h.partition_incarnation = partition_incarnation;
            h.size = total_size as u32;
            // The serving replica's own progress travels with every descriptor,
            // available or not: it is what lets a receiver refuse an offer from
            // a replica that knows less than it does.
            h.view = descriptor.view;
            h.commit_max = descriptor.commit_max;
            h.unavailable_transient = u8::from(descriptor.transient);
            if let Some((_, commit_op)) = descriptor.offer {
                h.available = 1;
                h.commit_op = commit_op;
            }
            h.seal();
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
        partition_incarnation: u64,
        artifact: u32,
        offset: u64,
        len: u32,
    ) where
        B: MessageBus,
    {
        let msg = Message::<RequestStateChunkHeader>::new(size_of::<RequestStateChunkHeader>())
            .transmute_header(|_, h: &mut RequestStateChunkHeader| {
                h.command = Command::RequestStateChunk;
                h.cluster = cluster;
                h.replica = self_id;
                h.nonce = nonce;
                h.group = namespace;
                h.partition_incarnation = partition_incarnation;
                h.artifact = artifact;
                h.offset = offset;
                h.len = len;
                h.size = size_of::<RequestStateChunkHeader>() as u32;
                h.seal();
            });
        let _ = self
            .bus
            .send_to_replica(target, msg.into_generic().into_frozen())
            .await;
    }

    /// Serve one `RequestStateTransfer`: build a fresh offer (or refuse),
    /// cache it for the chunk pulls, and answer with the descriptor.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_request_state_transfer(&self, msg: &Message<RequestStateTransferHeader>)
    where
        B: MessageBus,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: MetadataStm,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.group() == header.group);
        if !metadata_frame {
            return self.on_partition_request_state_transfer(msg).await;
        }
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
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
            .state_transfer_offers
            .borrow_mut()
            .get_mut(&(header.group, header.replica))
            .filter(|served| served.nonce == header.nonce)
            .and_then(|served| {
                let ServedOffer::Metadata(offer) = &served.offer else {
                    return None;
                };
                // A descriptor retry proves the requester is alive and still
                // wants THIS offer, so it counts as liveness: without the reset
                // the offer could age out mid-retry and the rebuild that
                // replaced it is exactly what first-wins exists to prevent.
                served.idle_ticks = 0;
                Some(Rc::clone(offer))
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
                header.group,
                consensus.partition_incarnation(),
                TransferDescriptor::available(
                    &offer.manifest(),
                    offer.commit_op,
                    consensus.view(),
                    consensus.commit_max(),
                ),
            )
            .await;
            return;
        }

        match planes.0.state_transfer_offer().await {
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
                    header.group,
                    consensus.partition_incarnation(),
                    TransferDescriptor::available(
                        &offer.manifest(),
                        offer.commit_op,
                        consensus.view(),
                        consensus.commit_max(),
                    ),
                )
                .await;
                self.state_transfer_offers.borrow_mut().insert(
                    (header.group, header.replica),
                    ServedStateTransfer {
                        nonce: header.nonce,
                        offer: ServedOffer::Metadata(offer),
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
                    "cannot serve metadata state transfer"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.group,
                    consensus.partition_incarnation(),
                    TransferDescriptor::unavailable(
                        matches!(reason, metadata::impls::metadata::StateTransferUnavailable::CheckpointInProgress),
                        consensus.view(),
                        consensus.commit_max(),
                    ),
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
        B: MessageBus + 'static,
        T: ShardsTable,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.group() == header.group);
        if !metadata_frame {
            return self.on_partition_state_transfer_target(msg).await;
        }
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
        let session_matches = self
            .metadata_transfer
            .borrow()
            .as_ref()
            .is_some_and(|session| session.nonce == header.nonce);
        if !session_matches {
            return;
        }

        if header.available == 0 {
            if header.unavailable_transient == 1 {
                let mut transfer = self.metadata_transfer.borrow_mut();
                if let Some(session) = transfer.as_mut()
                    && !session.target_accepted
                {
                    // The checkpoint writer owns the snapshot temporarily.
                    // Keep the nonce and let the tick pace the next request.
                    // The budget is not refilled: a refusal is no progress, and
                    // a checkpoint that never finishes would otherwise hold
                    // this replica on one peer in `AwaitingTarget` for good.
                    session.idle_ticks = 0;
                    return;
                }
            }
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
            // Marked after the arm, which the pump finishes before it takes the
            // repair's reply off the inbox.
            if let Some(session) = self.metadata_repair.borrow_mut().as_mut()
                && session.peer == header.replica
            {
                session.transfer_refused = true;
            }
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
            session.generation = generation;
            // Under ARTIFACT_LEN_MAX (checked above), so the casts hold.
            #[allow(clippy::cast_possible_truncation)]
            {
                session.artifacts = manifest
                    .iter()
                    .map(|&entry| consensus::ArtifactProgress {
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
                consensus::next_pending_chunk(&session.artifacts, chunk_len_max).map(
                    |(artifact, offset, len)| (session.nonce, session.peer, artifact, offset, len),
                )
            })
        };
        if let Some((nonce, peer, artifact, offset, len)) = request {
            self.send_request_state_chunk(
                consensus.cluster(),
                consensus.replica(),
                peer,
                nonce,
                consensus.group(),
                consensus.partition_incarnation(),
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
            // Set when a descriptor is accepted; a session with no accepted
            // descriptor never reaches the install path that reads it.
            generation: 0,
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

    /// Burn one stalled repair round; `true` once the budget is exhausted and
    /// the session should be re-armed against a different peer.
    ///
    /// The partition twin is `IggyPartition::burn_repair_attempt`, and it lives
    /// on the shard here for the same reason `metadata_transfer_attempts` does:
    /// one metadata group per node. It has to outlive the SESSION either way,
    /// or the rotation that mints a new one would reset the count and re-target
    /// forever without giving up on a peer. Only
    /// [`Self::note_metadata_repair_walked`] clears it.
    fn burn_metadata_repair_attempt(&self, view: u32) -> bool {
        let (charged_view, attempts) = self.metadata_repair_attempts.get();
        let attempts = if charged_view == view {
            attempts + 1
        } else {
            1
        };
        self.metadata_repair_attempts.set((view, attempts));
        attempts > partitions::REPAIR_MAX_STALL_RETRIES
    }

    /// Whether this view has already spent its merged-log repair budget.
    const fn metadata_repair_exhausted(&self, view: u32) -> bool {
        let (charged_view, attempts) = self.metadata_repair_attempts.get();
        charged_view == view && attempts > partitions::REPAIR_MAX_STALL_RETRIES
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

    /// A usable repair frame landed in the window: restart the stall clock.
    ///
    /// A window is served in `REPAIR_CHUNK_MAX` slices and nothing else resets
    /// `idle_ticks`, so a healthy multi-chunk stream would cross the retry interval
    /// on its own and rotate off a peer that is answering.
    ///
    /// Clock only. A repair prepare carries no sender and no session nonce (the
    /// frame IS the stored prepare, and its identity checksum covers every byte
    /// that could hold one), so a peer this session already rotated away from can
    /// land in-flight frames here and be credited to its successor. On the clock
    /// that costs one retry interval and is bounded, since nothing re-requests from
    /// that peer. On the budget it would cost rotation itself. See
    /// [`Self::note_metadata_repair_walked`].
    fn note_metadata_repair_clock(&self) {
        if let Some(session) = self.metadata_repair.borrow_mut().as_mut() {
            session.idle_ticks = 0;
        }
    }

    /// The repair this session asked for is landing: restart the clock and the
    /// budget.
    ///
    /// Takes only attributable signals, which a bare frame is not. Terminators are
    /// fenced on `session.nonce` before reaching here, so they came from the
    /// targeted peer; an advanced `commit_min` is the gap actually closing,
    /// whoever supplied the bytes.
    ///
    /// Stricter than the "any frame" rule it replaced: a re-request re-serves ops
    /// already held, so a peer answering with nothing new used to clear its own
    /// budget and could never be rotated away from.
    fn note_metadata_repair_walked(&self) {
        self.metadata_repair_attempts.set((0, 0));
        self.note_metadata_repair_clock();
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
    #[allow(
        clippy::future_not_send,
        clippy::cast_possible_truncation,
        clippy::too_many_lines
    )]
    async fn on_request_state_chunk(&self, msg: &Message<RequestStateChunkHeader>)
    where
        B: MessageBus,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.group() == header.group);
        if !metadata_frame {
            return self.on_partition_request_state_chunk(msg).await;
        }
        let Some(ref consensus) = planes.0.consensus else {
            return;
        };
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
            let mut offers = self.state_transfer_offers.borrow_mut();
            let served = offers
                .get_mut(&(header.group, header.replica))
                .filter(|served| served.nonce == header.nonce);
            served.map_or(
                Some(ChunkReply::Unavailable { transient: true }),
                |served| {
                    let ServedOffer::Metadata(offer) = &served.offer else {
                        return Some(ChunkReply::Unavailable { transient: true });
                    };
                    // Manifest-index addressing: an index past the offer is a
                    // requester bug (or a stale frame) and is dropped below.
                    let last_artifact = offer.len().saturating_sub(1);
                    let artifact_bytes = offer.payload(header.artifact as usize)?;
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
                            h.command = Command::StateChunk;
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.nonce = header.nonce;
                            h.group = header.group;
                            h.partition_incarnation = consensus.partition_incarnation();
                            h.artifact = header.artifact;
                            h.offset = header.offset;
                            h.size = total_size as u32;
                            h.seal();
                        },
                    )))
                },
            )
        };
        match reply {
            Some(ChunkReply::Chunk(chunk)) => {
                let _ = self
                    .bus
                    .send_to_replica(header.replica, chunk.into_generic().into_frozen())
                    .await;
            }
            Some(ChunkReply::Unavailable { transient }) => {
                tracing::info!(
                    shard = self.id,
                    requester = header.replica,
                    transient,
                    "state chunk request for an unknown offer; telling requester to restart"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.group,
                    consensus.partition_incarnation(),
                    TransferDescriptor::unavailable(
                        transient,
                        consensus.view(),
                        consensus.commit_max(),
                    ),
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
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.group() == header.group);
        if !metadata_frame {
            return self.on_partition_state_chunk(msg).await;
        }

        {
            let mut session = self.metadata_transfer.borrow_mut();
            let Some(session) = session.as_mut() else {
                return;
            };
            if session.nonce != header.nonce || !session.target_accepted {
                return;
            }
            let payload = &msg.as_slice()[size_of::<StateChunkHeader>()..header.size as usize];
            // Sequential-offset, overrun, and zero-byte-payload guards live in
            // the shared session math so both planes keep the exact invariants.
            if !consensus::append_chunk(
                &mut session.artifacts,
                header.artifact,
                header.offset,
                payload,
            ) {
                return;
            }
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
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
                Some(session) if session.target_accepted => session
                    .artifacts
                    .iter()
                    .all(consensus::ArtifactProgress::complete),
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
        // From the ACCEPTED descriptor, not re-derived from the artifacts: a
        // scan that aborts before the snapshot entry (unknown kind first, or a
        // checksum mismatch ahead of it) would leave nothing to charge, and an
        // uncharged decode failure re-arms the same peer for the same manifest
        // forever.
        let generation = session.generation;

        // Per-artifact integrity, then pick the pieces this plane installs.
        // Unknown kinds are refused rather than skipped: an artifact the
        // serving peer thought worth shipping but this receiver cannot
        // install would otherwise be silently dropped.
        let mut snapshot: Option<Vec<u8>> = None;
        let mut table: Option<(Vec<u8>, u64)> = None;
        let mut damaged = false;
        for (index, artifact) in session.artifacts.into_iter().enumerate() {
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
            match consensus::ClientTable::decode(&table_bytes) {
                Ok(table) => Some((snapshot, table, table_frontier)),
                Err(error) => {
                    tracing::error!(
                        shard = self.id,
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
            if self.burn_decode_failure(generation) {
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
                    // `installed_frontier`, not the transferred snapshot's op: the install
                    // returns `max(snapshot_seq, local_applied)`, which differs whenever a
                    // serving peer offers a snapshot BEHIND this replica (checkpoints are
                    // node-local) and the local state machine is kept instead.
                    tracing::info!(
                        shard = self.id,
                        installed_frontier = outcome.installed_frontier,
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
                        installed_frontier = outcome.installed_frontier,
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
    #[allow(clippy::too_many_lines)]
    /// Returns the commit fault that fenced a partition on this shard, if one
    /// has. The pump turns it into a server shutdown: a fenced partition is
    /// divergent from the cluster and can never advance again, so the tick
    /// stops driving it and the node stops rather than serving a prefix the
    /// cluster has moved past.
    pub async fn tick_partitions(
        &self,
        namespace_scratch: &mut Vec<IggyNamespace>,
    ) -> Option<FatalCommit>
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        debug_assert!(
            namespace_scratch.is_empty(),
            "namespace_scratch must be empty on entry",
        );
        self.ticks.set(self.ticks.get().wrapping_add(1));
        let partitions = self.plane.partitions();
        let repair_retry_ticks = self.repair_retry_ticks.get();
        self.partition_io.tick();
        for identity in self.partition_io.interrupted() {
            if let Some(partition) = partitions.get_io_owner(&identity.namespace) {
                partition.interrupt_io(identity);
            }
        }
        let gap_debounce_ticks = self.repair_gap_debounce_ticks.get();
        // Fan out over every group (each partition's heartbeat/retransmit timer
        // must advance), so the keyed single-namespace lookup the control-frame
        // handlers use does not apply here. The namespaces are snapshotted into
        // the pump's owned scratch (as `process_loopback` does) so no
        // partitions-plane borrow is held across the tick `.await`.
        namespace_scratch.extend(partitions.namespaces().copied());
        self.publish_partition_views(namespace_scratch);
        // Resume where the last sweep ran out of walk budget, so the cap below
        // spreads over every group instead of replaying the same prefix.
        rotate_sweep_to_cursor(namespace_scratch, self.partition_walk_cursor.get());

        for namespace in namespace_scratch.iter() {
            if let Some(partition) = partitions.get_by_ns(namespace) {
                if partition.needs_offset_reservation_extension() {
                    let _ = partition.extend_offset_reservation().await;
                }
                let _ = partition.persist_superblock_if_needed().await;
            }
            if let Some(partition) = partitions.get_io_owner(namespace)
                && partition.needs_io_retry()
            {
                self.partition_io.retry(*namespace, partition.incarnation());
            }
        }

        let mut persistence_metrics = partitions::PersistenceMetrics::default();
        let mut repair_ring_entries = 0usize;
        let mut repair_ring_bytes = 0u64;
        let mut untrusted_logs = 0usize;
        for namespace in namespace_scratch.iter() {
            if let Some(partition) = partitions.get_mut_by_ns(namespace) {
                partition.drive_persistence().await;
                untrusted_logs += usize::from(partition.consensus().has_untrusted_log());
                let (entries, bytes) = partition.repair_ring_occupancy();
                repair_ring_entries += entries;
                repair_ring_bytes += bytes;
                if let Some(metrics) = partition.take_persistence_metrics() {
                    persistence_metrics.disk_bytes += metrics.disk_bytes;
                    persistence_metrics.retained_bytes += metrics.retained_bytes;
                    persistence_metrics.queued_bytes += metrics.queued_bytes;
                    persistence_metrics.in_flight_bytes += metrics.in_flight_bytes;
                    persistence_metrics.checkpoints_pending += metrics.checkpoints_pending;
                    persistence_metrics.completed_batches += metrics.completed_batches;
                    persistence_metrics.batched_prepares += metrics.batched_prepares;
                    persistence_metrics.group_commit_waits += metrics.group_commit_waits;
                    persistence_metrics.completed_checkpoints += metrics.completed_checkpoints;
                    persistence_metrics.failed_writes += metrics.failed_writes;
                }
            }
        }
        self.metrics.record_persistence(&persistence_metrics);
        self.partition_io.record_metrics(&self.metrics);
        self.metrics
            .record_replica_reads(&self.bus.take_replica_read_stats());
        self.metrics
            .set_repair_ring(repair_ring_entries, repair_ring_bytes);
        self.metrics.set_partitions_untrusted_log(untrusted_logs);

        // Counted at most ONCE per sweep and only if a re-arm actually fires,
        // then tracked locally as arms land. Counting per namespace is a full
        // scan per partition, so with per-partition groups the sweep would be
        // O(P^2) exactly when every group is re-arming at once (node-wide view
        // change or rejoin) -- and counting eagerly every tick pays that scan on
        // every quiet tick too, since the re-arm branch is rare. A slot freed
        // mid-sweep is seen on the next tick, the same latency a capped arm
        // already accepts.
        let mut transfers_inflight: Option<usize> = None;
        // Repair sessions this sweep has opened, against
        // `PARTITION_REPAIR_ARMS_PER_TICK_MAX`.
        let mut repair_arms = 0usize;
        // Commit walks this sweep has run, against
        // `PARTITION_WALKS_PER_TICK_MAX`.
        let mut walks = 0usize;
        // First group the WALK budget turned away, which becomes the next
        // sweep's starting point. Recorded per SWEEP, not per group: the cursor
        // only has to name where the budget ran out, and every group after it
        // is reached on the next pass by the rotation above.
        //
        // The walk cap alone, because only its eligible set regenerates: a
        // walked group is walk-stalled again on the next produce, so a fixed
        // start would re-spend the budget on the same prefix forever. An ARMED
        // group leaves the gap-stopped set for the life of its session, so the
        // arm cap drains its own queue in namespace order with no cursor, and
        // letting an arm deferral move this one would pull the walk's resume
        // point backwards and break the `ceil(groups / cap)` bound below.
        //
        // It always advances: the walk budget is fresh at the group the sweep
        // starts on, so the first group can never be the deferred one, and a
        // cursor that stood still would re-skip the same tail forever.
        let mut walk_cursor: Option<IggyNamespace> = None;

        let mut fatal: Option<FatalCommit> = None;
        for namespace in namespace_scratch.drain(..) {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                if fatal.is_none() {
                    fatal = partitions
                        .get_io_owner(&namespace)
                        .and_then(|owner| owner.fatal().cloned());
                }
                continue;
            };
            // Ahead of the fence check and every `continue` below: the count is
            // the only record those prepares existed, and a partition that
            // fences here never ticks again.
            let gap_drops = partition.take_prepare_gap_drops();
            if gap_drops > 0 {
                self.metrics.record_partition_prepare_gap_drops(gap_drops);
            }
            let contradictions = partition.consensus().take_applied_log_contradictions();
            if contradictions > 0 {
                self.metrics
                    .record_partition_applied_log_contradictions(contradictions);
            }
            // A fenced partition must not tick: its consensus would emit
            // view-scoped sends for a log the cluster has already passed.
            if let Some(fault) = partition.fatal() {
                if fatal.is_none() {
                    fatal = Some(fault.clone());
                }
                continue;
            }
            // Same bound the metadata plane fail-stops on, applied per group,
            // and it exits the NODE rather than fencing the group: a partition
            // whose superblock keeps refusing withholds every view-scoped send,
            // and on a solo group refuses every append too, so it serves nothing
            // while the process still reports healthy.
            let superblock_failures = partition.superblock_write_failures();
            if superblock_wedged(
                superblock_failures,
                self.superblock_wedged_fatal_failures.get(),
            ) {
                consensus::fatal(
                    FatalReason::SuperblockWedged,
                    &format!(
                        "partition superblock persist failed {superblock_failures} consecutive \
                         times for namespace {}, past the [cluster] \
                         superblock_wedged_fatal_timeout window; exiting so a supervisor handles \
                         the wedge instead of the replica limping fenced",
                        namespace.inner()
                    ),
                );
            }

            let consensus = partition.consensus();
            // Only while a view change is live. A `Normal` tick has no consumer:
            // `start_election` records no DoViewChange, and every path that does
            // either refreshes at its own call site (the SVC and DVC handlers,
            // still `Normal` at that point) or runs in `ViewChange`.
            //
            // Ungated, this rebuilt a 128-entry window every 10 ms per advancing
            // partition: a linear `header_by_op` scan per entry plus 32 KiB.
            if consensus.status() != Status::Normal {
                refresh_partition_dvc_suffix(partition);
            }
            partition.ensure_materialization_recovery();
            let actions = consensus
                .tick_with_history_fence(PlaneKind::Partitions, partition.history_writes_pending());
            partition.ensure_materialization_recovery();
            // The tick emits view-scoped sends (heartbeats, view-change
            // retransmits), so it persists first like every dispatch site;
            // it is also what retries a persist an earlier site withheld on.
            let (local_actions, wire_actions) = split_local_actions(actions);
            // Locals to the partition dispatcher only; see the view-change
            // sites for the rationale.
            dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
            dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire_actions)
                .await;

            // Finish a view change whose quorum decided ahead of the local log.
            self.advance_pending_partition_view(namespace).await;

            // Stall retry: repair frames are fire-and-forget, so a lost
            // frame (or a peer that went silent mid-stream) would leave the
            // session armed forever with commit_min pinned below commit_max.
            // Re-request the remaining window from the serving peer; the
            // ingest path skips duplicates, so overlap is harmless.
            let stalled = {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                partition.retry_consumer_offset_reservations();
                if partition.queued_requests_ready() {
                    if walks < PARTITION_WALKS_PER_TICK_MAX {
                        walks += 1;
                        partition.resume_queued_requests().await;
                    } else {
                        walk_cursor.get_or_insert(namespace);
                    }
                }
                if partition.needs_persistence_checkpoint() {
                    if walks < PARTITION_WALKS_PER_TICK_MAX {
                        walks += 1;
                        partition.checkpoint_persistence(partitions.config()).await;
                    } else {
                        walk_cursor.get_or_insert(namespace);
                    }
                }
                if let Some(fault) = partition.fatal() {
                    if fatal.is_none() {
                        fatal = Some(fault.clone());
                    }
                    continue;
                }
                let consensus_view = partition.consensus().view();
                let commit_min = partition.consensus().commit_min();
                // A primary-elect's merged-log session legitimately runs outside
                // `Normal` (`request_partition_view_repair`). Same predicate as the
                // two ingest sites, so the arming side and the ingest side cannot
                // drift apart.
                let session_live = consensus::repair_session_live(partition.consensus());
                let repair_finished = partition.repair.is_some_and(|session| {
                    if !session_live || consensus_view != session.view {
                        return true;
                    }
                    // Floored at the LIVE commit point, like `complete_repair`:
                    // committing past `commit_to_op` evicts exactly the suffix
                    // headers this shape would look for, and ops at or below
                    // `commit_min` are committed and applied, a monotone fact
                    // the flush cannot erase.
                    let fetch_complete = session.fetch_to_op <= session.commit_to_op
                        || partition
                            .log
                            .journal()
                            .inner
                            .repaired_window_shape(
                                session.commit_to_op.max(commit_min),
                                session.fetch_to_op,
                            )
                            .complete;
                    commit_min >= session.commit_to_op && fetch_complete
                });
                if repair_finished {
                    partition.repair = None;
                    partition.transfer_rearm = None;
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        commit_min,
                        consensus_view,
                        "partition journal repair completed or was superseded"
                    );
                    continue;
                }
                let repair_from = partition
                    .consensus()
                    .view_log_repair_op()
                    .unwrap_or_else(|| commit_min.saturating_add(1));
                let due = partition.repair.as_mut().and_then(|session| {
                    if !session_live {
                        return None;
                    }
                    session.idle_ticks += 1;
                    if session.idle_ticks < repair_retry_ticks {
                        return None;
                    }
                    session.idle_ticks = 0;
                    session.from_op = repair_from;
                    Some((
                        session.peer,
                        session.nonce,
                        repair_from,
                        session.fetch_to_op,
                    ))
                });
                // A session pins its peer and fences every arming site while it
                // stands, so a peer that cannot answer wedges the group harder
                // than having no session at all -- and the gap-stopped-primary
                // rotation above can pick a peer that is simply down. Past the
                // budget the session is dropped and re-armed one step around
                // the ring with a fresh budget, until the ring is spent
                // (`spent_repair_ring_transfer_peer`); an ordinary lost frame
                // is re-requested long before that.
                //
                // A resident next op leaves progress to the walk backstop
                // below, or to the I/O lane while a commit is pending. The
                // re-request then starts at an op this journal holds, so a
                // correct peer can have nothing new to send, and a charge
                // would rotate a served session away while this replica's own
                // walk stalls.
                due.map(|stalled| {
                    let walk_owns_progress = group_is_walk_stalled(&partition_gap_probe(partition));
                    (
                        stalled,
                        !walk_owns_progress && partition.burn_repair_attempt(),
                    )
                })
            };
            if let Some(((peer, nonce, from_op, to_op), rotate)) = stalled
                && from_op <= to_op
            {
                if rotate
                    && let Some(primary) = partitions
                        .get_by_ns(&namespace)
                        .and_then(spent_repair_ring_transfer_peer)
                {
                    // Counted here, before the `&mut partition` below exists: the
                    // scan takes shared borrows of every partition.
                    let inflight = *transfers_inflight
                        .get_or_insert_with(|| self.partition_transfers_inflight());
                    let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                        continue;
                    };
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        peer = primary,
                        from_op,
                        to_op,
                        rotations = partition.repair_rotations(),
                        "partition repair stalled past the ring budget; converting to state transfer"
                    );
                    partition.consensus().begin_state_transfer_await();
                    if self
                        .arm_partition_transfer(partition, primary, inflight)
                        .await
                    {
                        transfers_inflight = Some(inflight + 1);
                    }
                    continue;
                }
                if rotate {
                    let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                        continue;
                    };
                    let consensus = partition.consensus();
                    let next_peer =
                        next_repair_peer(consensus.replica(), peer, consensus.replica_count());
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        peer,
                        next_peer,
                        from_op,
                        to_op,
                        "partition repair stalled past its retry budget; re-arming from \
                         another replica"
                    );
                    partition.repair = None;
                    // A parked view change re-arms from the merged log's senders,
                    // not around the cluster ring:
                    // `maybe_request_partition_repair` refuses outside `Normal`,
                    // and a replica that never named the op answers `RangeEvicted`
                    // for a range it never held.
                    if partition.consensus().is_primary_elect() {
                        self.request_partition_view_repair(partition, from_op, to_op, Some(peer))
                            .await;
                        continue;
                    }
                    if next_peer == peer {
                        // The ring had nobody else to offer (a solo group, or a
                        // two-replica group whose only peer is the one that
                        // went quiet). Dropping the session is still the right
                        // move: it unfences the detector, which re-arms after
                        // its debounce and logs the state each interval.
                        continue;
                    }
                    // The last recount or arming site counted this session.
                    // Keep its slot until the ring is spent; rings that cannot
                    // convert must then leave capacity for waiting groups.
                    let other_replicas =
                        u32::from(partition.consensus().replica_count().saturating_sub(1));
                    if partition.repair_rotations().saturating_add(1) < other_replicas {
                        self.partition_repairs_inflight
                            .set(self.partition_repairs_inflight.get().saturating_sub(1));
                    }
                    if self
                        .maybe_request_partition_repair(partition, next_peer)
                        .await
                    {
                        partition.note_repair_rotation();
                    }
                    continue;
                }
                tracing::info!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    from_op,
                    to_op,
                    peer,
                    "partition repair stalled; re-requesting remaining window"
                );
                self.send_request_prepares(
                    partition.consensus(),
                    peer,
                    nonce,
                    from_op,
                    to_op,
                    namespace.inner(),
                    partition.created_revision(),
                )
                .await;
            }

            // Level-triggered gap detector. Every other partition arming site
            // is edge-triggered and the edges are starvable: the commit-heartbeat
            // backstop needs `CommitOutcome::Advanced`, and a follower has
            // already advanced `commit_max` from each prepare header in
            // `replicate_preflight` before the gap check dropped the prepare, so
            // under produce load the heartbeat lands as `Accepted` and the gap
            // wedges until an unrelated view change. Those edges stay the fast
            // path; this is the ~1s floor under them.
            //
            // Runs entirely on the shared borrow: the in-flight scan below reads
            // every partition on the shard, so it must not run under a `&mut`,
            // and `gap_ticks` is a `Cell` for exactly that reason.
            let (walk_stalled, arm_peer) = {
                let Some(partition) = partitions.get_by_ns(&namespace) else {
                    continue;
                };
                let probe = partition_gap_probe(partition);
                let walk_stalled = !partition.has_pending_commit() && group_is_walk_stalled(&probe);
                // The RATE cap only. The concurrency cap lives in the arm fn,
                // which is the funnel every arming site goes through; resolved
                // before the debounce either way, so a refusal keeps the group
                // due rather than spending its arm.
                let may_arm = group_is_gap_stopped(&probe)
                    && repair_arms < PARTITION_REPAIR_ARMS_PER_TICK_MAX;
                let mut gap_ticks = partition.gap_ticks.get();
                let verdict =
                    drive_group_gap_debounce(&probe, &mut gap_ticks, gap_debounce_ticks, may_arm);
                partition.gap_ticks.set(gap_ticks);
                let arm_peer = match verdict {
                    GapArm::NotDue | GapArm::Deferred => None,
                    GapArm::Arm => {
                        let consensus = partition.consensus();
                        let peer = gap_repair_peer(
                            consensus.replica(),
                            consensus.replica_count(),
                            consensus.primary_index(consensus.view()),
                        );
                        if peer.is_none() {
                            // Restart the debounce so this repeats at its
                            // interval rather than every tick.
                            partition.gap_ticks.set(0);
                            tracing::warn!(
                                shard = self.id,
                                namespace_raw = namespace.inner(),
                                commit_min = probe.commit_min,
                                commit_max = probe.commit_max,
                                "partition is gap-stopped below its own commit frontier with no \
                                 peer to repair from"
                            );
                        }
                        peer
                    }
                };
                (walk_stalled, arm_peer)
            };
            if let Some(peer) = arm_peer {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                // Logged by `maybe_request_partition_repair` at info, with the
                // same fields plus the window it settled on. A refusal there
                // (the concurrency cap, or a guard the probe cannot see) spends
                // no rate budget and leaves the debounce satisfied, so the group
                // is due again next pass.
                if self.maybe_request_partition_repair(partition, peer).await {
                    repair_arms += 1;
                }
            }

            // Capped like the repair arm, and for the same reason: a node-wide
            // rejoin leaves every group on the shard walk-stalled in the same
            // tick, and each walk reaches a segment flush. Undebounced, though
            // -- the predicate guarantees the walk finds at least the next op,
            // so it cannot spin: `group_is_walk_stalled` reads residency off
            // `op_to_storage_offset` while the walk reads `headers`, and those
            // two are written and cleared together (see `Journal::holds_op`), so
            // a group the predicate admits has an op for the walk to take.
            if walk_stalled {
                if walks >= PARTITION_WALKS_PER_TICK_MAX {
                    // Deferred, not dropped: this group becomes the next
                    // sweep's starting point, so a shard with more owed walks
                    // than budget drains them round-robin. Without the cursor
                    // the leading groups would take the whole budget every
                    // pass and the tail would keep its committed ops resident
                    // indefinitely.
                    walk_cursor.get_or_insert(namespace);
                } else {
                    let config = partitions.config();
                    let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                        continue;
                    };
                    let consensus = partition.consensus();
                    // Debug, not info: an in-flight repair journals bodies
                    // without walking them, so this is the steady state for the
                    // whole duration of a rejoin and would be one line per group
                    // per tick.
                    tracing::debug!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        commit_min = consensus.commit_min(),
                        commit_max = consensus.commit_max(),
                        "partition commit walk parked over resident committed ops; resuming"
                    );
                    partition.commit_journal(config).await;
                    walks += 1;
                    // Re-read, because the aggregate above was sampled BEFORE
                    // this walk: a local commit failure fences the partition
                    // here, and reporting the stale verdict would let the pump
                    // keep serving a divergent replica until the next tick
                    // noticed.
                    if let Some(fault) = partition.fatal() {
                        if fatal.is_none() {
                            fatal = Some(fault.clone());
                        }
                        continue;
                    }
                }
            }

            // Transfer stall retry: descriptor and chunk frames are
            // fire-and-forget, so a lost one must not wedge the session (and
            // the rejoin behind it) forever. Budget-bounded: a peer that died
            // mid-transfer is abandoned back to journal repair, which
            // re-targets the current primary.
            let transfer_stalled = {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                partition.transfer.as_mut().and_then(|session| {
                    session.idle_ticks += 1;
                    if session.idle_ticks < repair_retry_ticks {
                        return None;
                    }
                    session.idle_ticks = 0;
                    Some((session.peer, session.nonce, session.target_accepted))
                })
            };
            if let Some((peer, nonce, target_accepted)) = transfer_stalled {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                if partition.burn_transfer_attempt() {
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace.inner(),
                        peer,
                        "partition state transfer stalled past its retry budget; abandoning with a backed-off re-arm"
                    );
                    // Staging files are KEPT: a later attempt adopts every
                    // segment whose manifest entry still matches. The shared
                    // path charges the failure, rotates the peer, schedules
                    // the re-arm, and re-arms journal repair meanwhile.
                    self.abandon_or_rearm_partition_transfer(partition, peer)
                        .await;
                } else if target_accepted {
                    self.request_pending_partition_chunk(namespace.inner())
                        .await;
                } else {
                    self.send_request_state_transfer(partition.consensus(), peer, nonce)
                        .await;
                }
            }

            // Scheduled transfer re-arm: count the backoff down and fire
            // once nothing else recovered the partition in the meantime (a
            // live session or a non-Idle stage owns the slot; the pending
            // entry is then dropped as superseded).
            let rearm_peer = {
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                match partition.transfer_rearm.as_mut() {
                    Some(pending) if pending.after_ticks > 0 => {
                        pending.after_ticks -= 1;
                        None
                    }
                    Some(pending) => {
                        let peer = pending.peer;
                        partition.transfer_rearm = None;
                        if partition.transfer.is_none()
                            && partition.consensus().state_transfer_stage()
                                == consensus::StateTransferStage::Idle
                            && (partition.requires_state_transfer()
                                || partition.consensus().commit_min()
                                    < partition.consensus().commit_max())
                        {
                            Some(peer)
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            };
            if let Some(peer) = rearm_peer {
                // Counted here, before the `&mut partition` below exists: the
                // scan takes shared borrows of every partition.
                let inflight =
                    *transfers_inflight.get_or_insert_with(|| self.partition_transfers_inflight());
                let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                    continue;
                };
                partition.consensus().begin_state_transfer_await();
                let armed = self.arm_partition_transfer(partition, peer, inflight).await;
                if armed {
                    transfers_inflight = Some(inflight + 1);
                }
            }
        }

        // The first group the walk cap turned away is where the next sweep
        // enters the group set; a sweep that turned nobody away resets to the
        // front, since a stale cursor would keep re-entering at a point no cap
        // chose.
        self.partition_walk_cursor.set(walk_cursor);
        // Count after every rotation, transfer abandonment and scheduled arm,
        // including paths that skip the rest of their partition's sweep.
        let mut repairs_live = 0usize;
        // A stranded key stays until its own store or delete succeeds, so a
        // gauge that never falls is the operator's only signal.
        let mut stranded = [0usize; ConsumerKind::COUNT];
        for namespace in partitions.namespaces() {
            if let Some(partition) = partitions.get_by_ns(namespace) {
                repairs_live += usize::from(partition.repair.is_some());
                for kind in ConsumerKind::ALL {
                    stranded[kind.index()] += partition.stranded_consumer_offset_count(kind);
                }
            }
        }
        self.partition_repairs_inflight.set(repairs_live);
        for kind in ConsumerKind::ALL {
            self.metrics
                .set_consumer_offsets_stranded(kind, stranded[kind.index()]);
        }

        fatal
    }

    /// Re-read a bounded range on retry so a full inbox retains no stale payload.
    /// Each interval visits every namespace once, in batches over consecutive ticks.
    /// A batch holds `PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN` namespaces, or more
    /// when the shard owns too many for that pace to finish within the interval.
    fn publish_partition_views(&self, namespaces: &[IggyNamespace]) {
        if namespaces.is_empty() {
            self.partition_placement_cursor.set(None);
            return;
        }
        if u128::from(self.ticks.get().saturating_sub(1))
            .is_multiple_of(PARTITION_PLACEMENT_REPORT_INTERVAL_TICKS)
        {
            self.partition_placement_owed.set(namespaces.len());
        }
        let owed = self.partition_placement_owed.get();
        if owed == 0 {
            return;
        }
        let Some(sender) = self.senders.first() else {
            return;
        };
        let start = self.partition_placement_cursor.get().map_or(0, |cursor| {
            namespaces.partition_point(|namespace| *namespace < cursor) % namespaces.len()
        });
        let batch = namespaces
            .len()
            .div_ceil(
                usize::try_from(PARTITION_PLACEMENT_REPORT_INTERVAL_TICKS).unwrap_or(usize::MAX),
            )
            .max(PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN);
        let visited = owed.min(namespaces.len()).min(batch);
        let partitions = self.plane.partitions();
        let mut reports = Vec::with_capacity(visited);
        for namespace in namespaces[start..]
            .iter()
            .chain(&namespaces[..start])
            .take(visited)
        {
            let Some(partition) = partitions.get_by_ns(namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            let view = consensus.view();
            if partition.fatal().is_none()
                && consensus.is_normal()
                && !consensus.is_transferring()
                && !(consensus.has_ceded_primaryship()
                    && consensus.primary_index(view) == consensus.replica())
            {
                reports.push(PartitionPlacementView {
                    namespace: *namespace,
                    created_revision: partition.created_revision(),
                    view,
                });
            }
        }
        if !reports.is_empty()
            && let Err(error) = sender.try_send(ShardFrame::lifecycle(
                LifecycleFrame::PartitionPlacementViews(reports),
            ))
        {
            self.metrics.record_frame_drop(
                metrics::frame_drop_variant::PARTITION_PLACEMENT_VIEWS,
                coordinator::classify_try_send_err(&error),
            );
            // The cursor stays, so the next interval resumes at this range.
            self.partition_placement_owed.set(0);
            return;
        }
        self.partition_placement_cursor
            .set(Some(namespaces[(start + visited) % namespaces.len()]));
        self.partition_placement_owed.set(owed - visited);
    }

    /// Whether this shard may build a partition offer for `namespace` without
    /// pushing the served-payload working set past its byte budget.
    ///
    /// Counts DISTINCT groups rather than requesters: the payload cache is
    /// content-addressed, so every requester pulling one group's offer shares
    /// one resident copy, and it is the group count that decides how many
    /// segments must be resident at once. A group already being served always
    /// passes, so admission cannot revoke a transfer midway.
    ///
    /// BOTH inputs are the configured ones. Dividing by the compile-time
    /// segment ceiling instead of the deployed the topic's `segment_size` would make
    /// the numerator the only thing an operator controls: on a 64 MiB-segment
    /// deployment the same budget holds sixteen times as many payloads as a cap
    /// derived from the 1 GiB ceiling would admit, and rejoins serialise for no
    /// reason.
    ///
    /// The divisor is the size a SEALED segment actually reaches, not the
    /// configured target: rotation fires after the append that crosses it, so a
    /// sealed segment runs up to one maximum batch past `segment.size`. Dividing
    /// by the bare target says two payloads fit a two-target budget when they do
    /// not, and `ServedSegmentCache::insert` then evicts one per chunk -- the
    /// thrash this cap exists to prevent, reintroduced through the arithmetic.
    ///
    /// That size is `partition_artifact_len_max`, which the config validator
    /// floors at `segment.size` plus the CONFIGURED `message_bus.max_message_size`.
    /// The compile-time [`SEGMENT_SIZE_OVERSHOOT_BYTES`] only tracks the shipped
    /// bus cap, so using it would restore the same thrash on any deployment that
    /// raised that knob: the sealed segment grows with the bus cap while the
    /// divisor would not. It is kept as a floor for the case where an operator
    /// sets the artifact ceiling below what a segment can reach.
    ///
    /// At least one is always admitted, since refusing every rejoin is worse
    /// than re-reading for a single one; the quotient rather than the divisor
    /// carries that clamp, so a zero segment size fails CLOSED at one slot
    /// instead of disabling admission control.
    fn partition_transfer_admission_cap(&self) -> usize {
        let segment_size = self.plane.partitions().config().segment_size.as_bytes_u64();
        let resident_len = self
            .partition_artifact_len_max
            .get()
            .max(segment_size.saturating_add(SEGMENT_SIZE_OVERSHOOT_BYTES));
        let slots = self
            .served_segment_cache_bytes_max
            .get()
            .checked_div(resident_len)
            .unwrap_or(1);
        usize::try_from(slots).unwrap_or(usize::MAX).max(1)
    }

    fn may_serve_another_partition_transfer(&self, namespace: u64) -> bool {
        let builds = self.partition_offer_builds.borrow();
        if builds.contains_key(&namespace) {
            return true;
        }
        let offers = self.state_transfer_offers.borrow();
        let mut served: Vec<u64> = offers
            .iter()
            .filter(|(_, served)| matches!(served.offer, ServedOffer::Partition(_)))
            .map(|((offer_namespace, _), _)| *offer_namespace)
            .collect();
        if served.contains(&namespace) {
            return true;
        }
        // Builds count too. A multi-round checksum pass holds no offer yet, so
        // counting only completed offers admitted every requester's whole
        // in-flight set at once and let each run its own pass: the frame bodies
        // stay bounded, but the pump carries N budgets per round-cycle and
        // every other frame, produce included, queues behind them.
        served.extend(builds.keys().copied());
        served.sort_unstable();
        served.dedup();
        served.len() < self.partition_transfer_admission_cap()
    }

    /// Serve one partition `RequestStateTransfer`: build (or re-serve) this
    /// group's offer and answer with the descriptor.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_partition_request_state_transfer(&self, msg: &Message<RequestStateTransferHeader>)
    where
        B: MessageBus,
    {
        let header = *msg.header();
        // Before anything keyed by the requester: the offer map's documented
        // bound is the replica count, and an unvalidated id makes it 256 entries
        // per served group, each pinning an offer for its full expiry.
        if !self.peer_is_known(header.replica, "RequestStateTransfer") {
            return;
        }
        let planes = self.plane.inner();
        let config = planes.1.0.config().clone();
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.group))
        else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::RequestStateTransfer,
        ) {
            return;
        }
        let cluster = partition.consensus().cluster();
        let self_id = partition.consensus().replica();
        // First-wins per (requester, nonce), exactly as the metadata arm: a
        // stall retry reuses the nonce, and rebuilding under it could hand
        // the receiver chunks from a different offer than the manifest it
        // accepted. Re-answering with the SAME offer keeps the retry
        // idempotent.
        let cached = self
            .state_transfer_offers
            .borrow_mut()
            .get_mut(&(header.group, header.replica))
            .filter(|served| served.nonce == header.nonce)
            .and_then(|served| {
                let ServedOffer::Partition(offer) = &served.offer else {
                    return None;
                };
                served.idle_ticks = 0;
                Some(Rc::clone(offer))
            });

        // One resolve, one send: the three outcomes differ only in the
        // descriptor they produce, and duplicating the send made it possible for
        // them to drift on the progress they advertise.
        let offer = match cached {
            Some(offer) => {
                tracing::debug!(
                    shard = self.id,
                    namespace_raw = header.group,
                    requester = header.replica,
                    "re-answering a partition state transfer request from the offer \
                     already served"
                );
                Some(offer)
            }
            None if !self.may_serve_another_partition_transfer(header.group) => {
                // Admission control, because the served-payload budget is a
                // BYTE budget and the pulls that overrun it do not degrade
                // gracefully. Each concurrent pull holds a different segment
                // resident, so admitting more distinct groups than the budget
                // has max-size slots makes them evict each other on every
                // chunk: every request then re-reads and re-hashes a whole
                // segment to serve one 256 KiB range, and a per-chunk serve
                // that outruns the requester's stall interval exhausts its
                // retry budget, so the pull rotates peers and never converges.
                // Refusing the surplus is what makes the admitted ones finish.
                tracing::info!(
                    shard = self.id,
                    namespace_raw = header.group,
                    requester = header.replica,
                    "already serving as many partition transfers as the served-payload \
                     budget holds; refusing until one completes"
                );
                let (view, commit_max) = serving_progress(partition);
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.group,
                    partition.created_revision(),
                    TransferDescriptor::unavailable(true, view, commit_max),
                )
                .await;
                return;
            }
            None => {
                // Claim the admission slot for the whole build, not just for a
                // completed offer: the checksum pass runs over several rounds
                // and holds nothing in the offers map meanwhile.
                self.partition_offer_builds
                    .borrow_mut()
                    .insert(header.group, 0);
                match partition.state_transfer_offer(&config).await {
                    Ok(offer) => {
                        self.partition_offer_builds
                            .borrow_mut()
                            .remove(&header.group);
                        tracing::info!(
                            shard = self.id,
                            namespace_raw = header.group,
                            requester = header.replica,
                            commit_op = offer.commit_op,
                            artifacts = offer.artifact_count(),
                            total_len = offer.total_len(),
                            "serving partition state transfer"
                        );
                        self.state_transfer_offers.borrow_mut().insert(
                            (header.group, header.replica),
                            ServedStateTransfer {
                                nonce: header.nonce,
                                offer: ServedOffer::Partition(Rc::clone(&offer)),
                                idle_ticks: 0,
                                fully_served: false,
                            },
                        );
                        Some(offer)
                    }
                    Err(reason) => {
                        // The ACTUAL reason: "not the caught-up primary" is routine
                        // (the requester re-targets), an unreadable segment is an
                        // operator-visible fault on THIS node. The requester cannot
                        // see the reason, only whether it was transient, which is
                        // what keeps a routine refusal from charging its failure
                        // count.
                        //
                        // The slot survives ONLY a round the build resumes from (a
                        // pending flush or a spent hash budget); every other
                        // refusal abandons the build and must not keep the group
                        // admitted.
                        if !reason.resumable() {
                            self.partition_offer_builds
                                .borrow_mut()
                                .remove(&header.group);
                        }
                        let transient = reason.transient();
                        tracing::info!(
                            shard = self.id,
                            namespace_raw = header.group,
                            requester = header.replica,
                            transient,
                            %reason,
                            "cannot serve partition state transfer; requester falls back"
                        );
                        let (view, commit_max) = serving_progress(partition);
                        self.send_state_transfer_target(
                            cluster,
                            self_id,
                            header.replica,
                            header.nonce,
                            header.group,
                            partition.created_revision(),
                            TransferDescriptor::unavailable(transient, view, commit_max),
                        )
                        .await;
                        return;
                    }
                }
            }
        };
        let Some(offer) = offer else {
            return;
        };
        // Sampled AFTER any build: that build force-flushes and hashes a
        // budgeted slice of the un-memoized segments (a first multi-GiB serve
        // spans several rounds before an offer exists) while reading
        // `commit_op` post-flush, so a pre-build sample could advertise a
        // `commit_max` below the descriptor's own `commit_op` -- which only
        // makes the receiver's gate refuse, and refusals feed a backoff.
        let (view, commit_max) = serving_progress(partition);
        self.send_state_transfer_target(
            cluster,
            self_id,
            header.replica,
            header.nonce,
            header.group,
            partition.created_revision(),
            TransferDescriptor::available(&offer.manifest(), offer.commit_op, view, commit_max),
        )
        .await;
    }

    /// Serve one partition chunk. Segment payloads are loaded from disk on
    /// demand into the shard-wide [`ServedSegmentCache`] (content-addressed,
    /// so simultaneous rejoiners share one resident copy); a load or
    /// re-verification failure (GC unlinked the file, bytes changed) evicts
    /// the offer and tells the requester to restart with a fresh one --
    /// which then reflects the current segment set, so the retry converges.
    #[allow(
        clippy::future_not_send,
        clippy::cast_possible_truncation,
        clippy::too_many_lines
    )]
    async fn on_partition_request_state_chunk(&self, msg: &Message<RequestStateChunkHeader>)
    where
        B: MessageBus,
    {
        // TODO: Move partition offer hashing, segment loads and receiver spills
        // into bounded file-lane jobs so they cannot stall consensus ticks.
        // Pass 1 inside the borrow decides; a segment artifact that is not
        // resident exits with its path and is loaded OUTSIDE the borrow (a
        // RefCell borrow must not be held across an await), then pass 2
        // stores and serves it.
        enum ChunkAttempt {
            Reply(Option<ChunkReply>),
            Load {
                log_path: String,
                entry: consensus::StateArtifact,
            },
        }

        let header = *msg.header();
        // The requester id keys the offer map, whose bound is the replica count.
        if !self.peer_is_known(header.replica, "RequestStateChunk") {
            return;
        }
        let planes = self.plane.inner();
        let Some(partition) = planes.1.0.get_by_ns(&IggyNamespace::from_raw(header.group)) else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::RequestStateChunk,
        ) {
            return;
        }
        let cluster = partition.consensus().cluster();
        let self_id = partition.consensus().replica();
        let serving_view = partition.consensus().view();
        let serving_commit_max = partition.consensus().commit_max();
        let chunk_len_max = self.state_chunk_len_max();
        let reply = loop {
            let attempt = 'attempt: {
                let mut offers = self.state_transfer_offers.borrow_mut();
                let served = offers
                    .get_mut(&(header.group, header.replica))
                    .filter(|served| served.nonce == header.nonce);
                let Some(served) = served else {
                    break 'attempt ChunkAttempt::Reply(Some(ChunkReply::Unavailable {
                        transient: true,
                    }));
                };
                let ServedOffer::Partition(offer) = &served.offer else {
                    break 'attempt ChunkAttempt::Reply(Some(ChunkReply::Unavailable {
                        transient: true,
                    }));
                };
                let last_artifact = offer.artifact_count().saturating_sub(1);
                let artifact = header.artifact as usize;
                // Keeps a segment payload alive past the cache borrow below.
                let segment_payload: Rc<Vec<u8>>;
                let artifact_bytes: &[u8] = match offer.artifact_at(artifact) {
                    Some(partitions::state_transfer::PartitionArtifactSource::Offsets(bytes)) => {
                        bytes
                    }
                    Some(partitions::state_transfer::PartitionArtifactSource::Segment(source)) => {
                        match self
                            .served_segment_cache
                            .borrow_mut()
                            .get(header.group, source.entry.checksum)
                        {
                            Some(payload) => {
                                segment_payload = payload;
                                &segment_payload
                            }
                            None => {
                                break 'attempt ChunkAttempt::Load {
                                    log_path: source.log_path.clone(),
                                    entry: source.entry,
                                };
                            }
                        }
                    }
                    // Index past the manifest: requester bug or stale frame.
                    None => break 'attempt ChunkAttempt::Reply(None),
                };
                let start = header.offset as usize;
                // `start >= len` is the empty-chunk livelock refusal; see the
                // metadata arm for the full story.
                if start >= artifact_bytes.len() {
                    break 'attempt ChunkAttempt::Reply(None);
                }
                let end = start
                    .saturating_add((header.len as usize).min(chunk_len_max))
                    .min(artifact_bytes.len());
                let Some(payload) = artifact_bytes.get(start..end) else {
                    break 'attempt ChunkAttempt::Reply(None);
                };
                if artifact == last_artifact && end >= artifact_bytes.len() && !served.fully_served
                {
                    served.fully_served = true;
                    // Once per transfer, at the last byte of the last artifact.
                    // The descriptor log only proves a REQUEST arrived; this is
                    // the serving side's proof that the pull ran to completion.
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = header.group,
                        requester = header.replica,
                        "partition state transfer fully served"
                    );
                }
                served.idle_ticks = 0;
                let total_size = size_of::<StateChunkHeader>() + payload.len();
                let mut chunk = Message::<StateChunkHeader>::new(total_size);
                chunk.as_mut_slice()[size_of::<StateChunkHeader>()..].copy_from_slice(payload);
                ChunkAttempt::Reply(Some(ChunkReply::Chunk(chunk.transmute_header(
                    |_, h: &mut StateChunkHeader| {
                        h.command = Command::StateChunk;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.nonce = header.nonce;
                        h.group = header.group;
                        h.partition_incarnation = partition.created_revision();
                        h.artifact = header.artifact;
                        h.offset = header.offset;
                        h.size = total_size as u32;
                        // `StateChunk` is `FRAME_SEALED`: the receiver's router
                        // drops an unsealed frame before any handler sees it, so
                        // a missing seal starves the pull silently.
                        h.seal();
                    },
                ))))
            };
            match attempt {
                ChunkAttempt::Reply(reply) => break reply,
                ChunkAttempt::Load { log_path, entry } => {
                    // Chunked read + incremental hash: this runs on the pump to
                    // answer ONE 256 KiB chunk request, so a whole-file read
                    // plus a single hash pass over up to a segment would be one
                    // long uninterruptible CPU+IO block. The chunking keeps the
                    // REACTOR moving; this shard's consensus ticks are a sibling
                    // select arm of the same task and stay frozen either way.
                    let loaded = partitions::state_transfer::load_verified_segment_artifact(
                        &log_path, &entry,
                    )
                    .await;
                    let reason = match loaded {
                        Ok(bytes) => {
                            self.served_segment_cache.borrow_mut().insert(
                                header.group,
                                entry.checksum,
                                Rc::new(bytes),
                                self.served_segment_cache_bytes_max.get(),
                            );
                            continue;
                        }
                        Err(reason) => reason,
                    };
                    // The CAUSE decides what the requester is told: a racing GC
                    // or a stale offer is transient and costs it nothing, while
                    // an unreadable device is this node's fault and must charge,
                    // or a dying disk reads as a momentary blip forever.
                    let transient = reason.transient();
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = header.group,
                        artifact = header.artifact,
                        path = %log_path,
                        transient,
                        %reason,
                        "cannot serve the requested segment; evicting the offer"
                    );
                    self.state_transfer_offers
                        .borrow_mut()
                        .remove(&(header.group, header.replica));
                    // The builder cache too: it is keyed by commit_op alone,
                    // and GC unlinks files WITHOUT a commit, so the restarted
                    // requester would otherwise be handed the same offer with
                    // the same dead path, forever.
                    partition.clear_state_transfer_offer_cache();
                    break Some(ChunkReply::Unavailable { transient });
                }
            }
        };
        match reply {
            Some(ChunkReply::Chunk(chunk)) => {
                let _ = self
                    .bus
                    .send_to_replica(header.replica, chunk.into_generic().into_frozen())
                    .await;
            }
            Some(ChunkReply::Unavailable { transient }) => {
                tracing::info!(
                    shard = self.id,
                    namespace_raw = header.group,
                    requester = header.replica,
                    transient,
                    "partition chunk request for an unknown offer; telling requester to restart"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.group,
                    partition.created_revision(),
                    // Usually TRANSIENT -- retention GC'd a served segment, or
                    // the offer aged out between two chunks, and the restarted
                    // session converges -- but a load that failed on a local
                    // fault says so, or a dying disk would read as a momentary
                    // blip forever.
                    TransferDescriptor::unavailable(transient, serving_view, serving_commit_max),
                )
                .await;
            }
            None => {
                tracing::warn!(
                    shard = self.id,
                    namespace_raw = header.group,
                    requester = header.replica,
                    artifact = header.artifact,
                    offset = header.offset,
                    "partition chunk request out of artifact bounds; ignoring"
                );
            }
        }
    }

    /// Sanity cap across a partition manifest. Segment artifacts spill to
    /// disk as they complete, so this bounds corruption, not memory.
    const PARTITION_TRANSFER_TOTAL_LEN_MAX: u64 = 1 << 40;

    /// Bound the buffered offset and dedup state plus one maximum-sized
    /// checkpoint prepare and its length prefix before allocating the artifact.
    const CONSUMER_OFFSETS_ARTIFACT_LEN_MAX: u64 =
        (32 << 20) + journal::partition_journal::PREPARE_BYTES_MAX as u64 + 4;

    /// Concurrent partition transfers this shard will run as a RECEIVER. A
    /// whole-node rejoin arms one per lagging partition; unbounded, the sum
    /// of in-flight buffers and staging writes is partitions x segment
    /// size. Capped-out arms retry via the scheduled re-arm sweep.
    const PARTITION_TRANSFERS_INFLIGHT_MAX: usize = 4;

    /// Whether arming a transfer for `namespace` is even possible right now.
    ///
    /// Takes a SHARED borrow and drops it before returning, so a caller may form
    /// its `&mut partition` afterwards. The point is to keep the in-flight scan
    /// -- which borrows every partition on the shard -- off frames that cannot
    /// arm anything: a namespace this shard does not own, and the ordinary case
    /// of a group that is neither awaiting a transfer nor idle-with-no-re-arm.
    fn may_arm_partition_transfer(partitions: &IggyPartitions<B, SB>, namespace_raw: u64) -> bool
    where
        B: MessageBus,
    {
        partitions
            .get_by_ns(&IggyNamespace::from_raw(namespace_raw))
            .is_some_and(|partition| {
                partition.transfer.is_none()
                    && matches!(
                        partition.consensus().state_transfer_stage(),
                        consensus::StateTransferStage::AwaitingTarget
                            | consensus::StateTransferStage::Idle
                    )
            })
    }

    /// Receiving-side transfers currently in flight on this shard.
    ///
    /// One scan per call, so callers hoist it: with per-partition groups a
    /// per-namespace call inside the tick sweep is O(P^2) exactly during a
    /// node-wide view change or rejoin, and capped arms reschedule on the flat
    /// retry interval, so the losers stay phase-locked and the sweep repeats
    /// every interval for the whole rejoin.
    fn partition_transfers_inflight(&self) -> usize {
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();
        namespaces
            .iter()
            .filter(|namespace| {
                partitions
                    .get_by_ns(namespace)
                    .is_some_and(|partition| partition.transfer.is_some())
            })
            .count()
    }

    /// Drop every trace of `namespace`'s current bytes from the serving side:
    /// the partition's own offer cache, this shard's cached offers, and the
    /// resident payloads behind them.
    ///
    /// Called wherever a partition's segments stop being the bytes an offer
    /// describes -- retention cleaning, a committed truncate, an install. None of
    /// the caches can detect that themselves: the builder cache is keyed on
    /// `commit_op` (which a metadata-plane truncate never moves), the shard's
    /// offers on the requester, and the payloads on a checksum over the bytes
    /// that just went away -- so a puller mid-transfer keeps receiving deleted
    /// data and keeps both expiry clocks reset while doing it.
    pub(crate) fn drop_partition_transfer_state(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) where
        B: MessageBus,
    {
        partition.clear_state_transfer_offer_cache();
        self.drop_served_state_for(namespace.inner());
    }

    fn drop_served_state_for(&self, namespace: u64) {
        // Including the build slot: the bytes a partial checksum pass covered are
        // gone with the chain, so the slot behind it is no longer resumable
        // work and must stop counting against other namespaces' admission.
        self.partition_offer_builds.borrow_mut().remove(&namespace);
        self.state_transfer_offers
            .borrow_mut()
            .retain(|(served_namespace, _), _| *served_namespace != namespace);
        self.served_segment_cache
            .borrow_mut()
            .evict_namespace(namespace);
    }

    /// Whether a peer-supplied source replica id names a replica of this
    /// cluster.
    ///
    /// `header.replica` arrives unvalidated on every frame, and the partition
    /// transfer paths turn it into ring arithmetic ([`next_transfer_peer`], where
    /// id 255 panics in debug and wraps to replica 0 in release -- a silent
    /// retarget) and into the served-offer map key, whose documented bound is the
    /// replica count rather than 256 entries per served group. One check at the
    /// frame's ingress closes both.
    fn peer_is_known(&self, replica: u8, frame: &'static str) -> bool {
        let replica_count = self.partition_consensus.replica_count;
        if replica < replica_count {
            return true;
        }
        tracing::warn!(
            shard = self.id,
            frame,
            replica,
            replica_count,
            "dropping a partition frame whose source replica is outside this cluster"
        );
        false
    }

    /// Replace a partition whose applied history the view contradicts with a
    /// fresh copy from its peers, instead of letting the WAL's parent check
    /// stop the whole node on the view's next prepare.
    ///
    /// The rebuild quarantines the local segments as evidence and comes back
    /// with no materialization, so it serves, acks and votes nothing until a
    /// state transfer from a settled primary installs the view's history.
    ///
    /// `agreed_op` is the highest applied op the view confirms. The recorded
    /// frontier may sit below the group's but never above it: too high refuses
    /// every offer on a topic nobody writes to. Falling back to `0` is safe
    /// because the quarantined partition holds no data to rewind, and an offer
    /// from a lagging peer is still a consistent prefix of the agreed history.
    fn resync_diverged_partition(
        &self,
        namespace: IggyNamespace,
        partition: &mut IggyPartition<B, SB>,
        diverged_op: u64,
        agreed_op: Option<u64>,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        tracing::error!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            diverged_op,
            view = partition.consensus().view(),
            commit_min = partition.consensus().commit_min(),
            "the view contradicts applied partition history at op {diverged_op}; \
             quarantining this partition and resyncing it by state transfer"
        );
        self.metrics.record_partition_applied_divergence();
        // The rebuild refuses any offer below the frontier it records, and the
        // view's history past `agreed_op` can be shorter than this replica's.
        // Counting this replica's own divergent mints would refuse every offer
        // until the group wrote past them, which an idle topic never does.
        let frontier = agreed_op
            .and_then(|agreed| partition.offset_frontier_before(agreed.saturating_add(1)))
            .unwrap_or_else(|| {
                tracing::warn!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    diverged_op,
                    ?agreed_op,
                    "no retained applied op is confirmed by the view below op {diverged_op}; \
                     the rebuild records no offset frontier and accepts any offer"
                );
                0
            });
        self.drop_partition_transfer_state(namespace, partition);
        self.fence_partition_for_rebuild(namespace, partition, frontier);
    }

    /// Fence one partition for rebuild: quarantine its segment files, drop it
    /// from routing, and queue the retirement the reconciler re-materialises
    /// from committed metadata.
    ///
    /// Used wherever a partition is left without a serviceable segment chain (a
    /// failed state-transfer install whose convergence also failed): the next
    /// append or poll would panic on `active_segment()`'s expect.
    ///
    /// `IggyPartitions` mandates external removals go through `ConfirmRemove` --
    /// a direct `remove()` would invalidate the `&mut` the caller still holds --
    /// but the tombstone and the routing row drop SYNCHRONOUSLY here, because
    /// the tombstone is the only gate in `get_mut_by_ns` and the queue does not
    /// drain until the end of the pump iteration.
    ///
    /// `intended_frontier` is the offset frontier the group committed, not the
    /// LIVE counter: a failed install can leave the counter at its pre-install
    /// value, below that frontier.
    fn fence_partition_for_rebuild(
        &self,
        namespace: IggyNamespace,
        partition: &mut IggyPartition<B, SB>,
        intended_frontier: u64,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        // The tick exports this count through the tombstone gate, so a refusal
        // that fenced the partition would otherwise surface only at its removal.
        self.metrics.record_partition_applied_log_contradictions(
            partition.consensus().take_applied_log_contradictions(),
        );
        partition.begin_quarantine(intended_frontier);
        self.plane.partitions().tombstone(namespace);
        self.shards_table.remove(&namespace);
    }

    /// Arm a fresh partition transfer session against `peer` and request its
    /// descriptor. Every partition arming site goes through here. Drops any
    /// repair session (transfer supersedes repair; a transfer-unavailable
    /// fallback re-arms repair fresh) and leaves the stage to its callers (they
    /// own the `AwaitingTarget` transition).
    ///
    /// Refuses past [`Self::PARTITION_TRANSFERS_INFLIGHT_MAX`]: the arm
    /// converts to a scheduled re-arm (no failure charged -- the local slot
    /// shortage is not the peer's fault) and the stage returns to Idle so
    /// journal repair keeps the gap visible meanwhile.
    ///
    /// `transfers_inflight` is computed by the caller BEFORE it formed its
    /// `&mut partition`: the counting scan takes shared borrows of every
    /// partition, and deriving a sibling `&` to the element the caller's
    /// protected `&mut` points at is UB under both stacked and tree borrows,
    /// however innocuous the generated code is today. Returns whether a session
    /// was armed, so a caller sweeping many groups can carry the count forward
    /// instead of re-scanning per group.
    #[allow(clippy::future_not_send)]
    async fn arm_partition_transfer(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
        transfers_inflight: usize,
    ) -> bool
    where
        B: MessageBus,
    {
        if self.shutting_down.get() || partition.is_shutting_down() {
            return false;
        }
        if partition.transfer.is_none()
            && transfers_inflight >= Self::PARTITION_TRANSFERS_INFLIGHT_MAX
        {
            tracing::info!(
                shard = self.id,
                namespace_raw = partition.consensus().group(),
                cap = Self::PARTITION_TRANSFERS_INFLIGHT_MAX,
                "partition transfer slots exhausted; deferring this arm"
            );
            let consensus = partition.consensus();
            if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
                consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
            }
            partition.note_transfer_rearm_scheduled();
            partition.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
                peer,
                after_ticks: self.repair_retry_ticks.get(),
            });
            return false;
        }
        partition.repair = None;
        partition.restart_repair_budget();
        let nonce = iggy_common::random_id::get_uuid();
        let armed = partition.transfer.is_none();
        partition.transfer = Some(partitions::state_transfer::PartitionTransferSession {
            nonce,
            peer,
            commit_op: 0,
            artifacts: Vec::new(),
            target_accepted: false,
            idle_ticks: 0,
        });
        self.send_request_state_transfer(partition.consensus(), peer, nonce)
            .await;
        armed
    }

    /// Arm partition journal repair when this replica is lagging its group
    /// and nothing else is recovering it. The idempotence guards mirror
    /// `maybe_request_metadata_repair`: no-op unless Normal, not
    /// transferring, behind the frontier, and no session live.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    /// Open a journal-repair session against `peer`, if this partition needs
    /// one and the shard has room for it. `true` when a session was recorded.
    ///
    /// THE funnel: the tick sweep and the four edge-triggered sites
    /// (`StartView` adoption, the commit heartbeat, the post-transfer tail, the
    /// post-repair walk) all arrive here, so the concurrency ceiling and the
    /// debounce reset live here rather than in any one caller. A node-wide view
    /// change drives `on_start_view` for every group at once, which is exactly
    /// the burst the sweep's own rate cap would not see.
    async fn maybe_request_partition_repair(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
    ) -> bool
    where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        if !partition.consensus().is_normal()
            || partition.consensus().is_transferring()
            || partition.repair.is_some()
            || partition.view_truncation_pending()
        {
            return false;
        }
        if partition.consensus().view_log_unproven()
            && partition.consensus().view_log_repair_op().is_none()
        {
            let pending = partition.consensus().pending_view_log();
            // Truncation below only reaches ops above the applied floor.
            if let Some((diverged_op, agreed_op)) = pending
                .as_ref()
                .and_then(|pending| partition_applied_divergence(partition, pending))
            {
                self.resync_diverged_partition(
                    IggyNamespace::from_raw(partition.consensus().group()),
                    partition,
                    diverged_op,
                    agreed_op,
                );
                return false;
            }
            if partition.has_io_dispatcher()
                && let Some(from_op) = partition_view_divergence_from(partition, pending.as_ref())
            {
                partition.defer_view_truncation(from_op..=u64::MAX, Vec::new(), Some(peer));
                return false;
            }
            reconcile_partition_view_divergence(self.id, partition, pending.as_ref()).await;
            if partition.view_truncation_pending()
                || (partition.consensus().view_log_unproven()
                    && partition.consensus().view_log_repair_op().is_none())
            {
                return false;
            }
        }
        let consensus = partition.consensus();
        // Read, never scanned: the tally is republished by each sweep (see
        // `tick_partitions`), so it is at worst one tick stale, which is all a
        // concurrency ceiling needs. Callers here hold a `&mut` to one
        // partition, so a scan over the plane would alias it.
        if self.partition_repairs_inflight.get() >= PARTITION_REPAIRS_INFLIGHT_MAX {
            tracing::debug!(
                shard = self.id,
                namespace_raw = consensus.group(),
                peer,
                "partition repair not armed: shard is at its live-session ceiling"
            );
            return false;
        }
        // Never against self. The session is recorded below BEFORE the send,
        // and a self-addressed `RequestPrepares` cannot be delivered (the
        // replica registry holds no entry for this node), so the session would
        // stand forever: `repair_finished` needs a `commit_min` only the reply
        // can advance, the stall retry re-sends to the same peer, and
        // `repair.is_some()` fences every other arming site meanwhile.
        if peer == consensus.replica() {
            return false;
        }
        // The window ends at the group head when suffix bodies are missing,
        // not at the commit point. A backup that adopted a StartView holds
        // suffix HEADERS above `commit_max` whose bodies it may never have
        // received: its ack for them is withheld until the body is journaled,
        // and the primary's retransmit is dropped by the backup gap check
        // because adoption already advanced the sequencer to the head. With a
        // commit-bounded window nothing ever delivers those bodies, the
        // primary cannot gather quorum for the suffix, and the group wedges
        // one op below its head with the client write never confirmed.
        let commit_to_op = consensus.commit_max();
        let commit_lag = consensus.commit_min() < commit_to_op;
        let head = consensus.sequencer().current_sequence();
        if !commit_lag && head <= commit_to_op && !consensus.view_log_unproven() {
            return false;
        }
        let missing_suffix = partition_missing_suffix_through(partition);
        // Fetch the adopted suffix even while committed operations lag. Later
        // live prepares can advance the head while an adopted body is missing.
        let Some(fetch_to_op) = consensus.view_log_repair_op().map(|_| head).or_else(|| {
            partition_repair_fetch_to_op(consensus.commit_min(), commit_to_op, missing_suffix)
        }) else {
            return false;
        };
        // A resident window may only be waiting for persistence or a bounded commit walk.
        if !consensus.view_log_unproven()
            && partition
                .log
                .journal()
                .inner
                .repaired_window_shape(consensus.commit_min(), fetch_to_op)
                .complete
        {
            return false;
        }
        let nonce = iggy_common::random_id::get_uuid();
        let from_op = consensus
            .view_log_repair_op()
            .unwrap_or_else(|| consensus.commit_min() + 1);
        // Spent here for the same reason the ceiling is: the four edge-triggered
        // sites never touch it, so a short edge-armed repair would leave the
        // count saturated and hand the next real gap an arm on its first tick.
        partition.gap_ticks.set(0);
        self.partition_repairs_inflight
            .set(self.partition_repairs_inflight.get() + 1);
        partition.repair = Some(partitions::RepairSession {
            nonce,
            view: consensus.view(),
            from_op,
            commit_to_op,
            fetch_to_op,
            floor: None,
            peer,
            idle_ticks: 0,
        });
        tracing::info!(
            shard = self.id,
            namespace_raw = partition.consensus().group(),
            from_op,
            commit_to_op,
            fetch_to_op,
            peer,
            "partition behind the group frontier; requesting repair"
        );
        self.send_request_prepares(
            partition.consensus(),
            peer,
            nonce,
            from_op,
            fetch_to_op,
            partition.consensus().group(),
            partition.created_revision(),
        )
        .await;
        true
    }

    /// Partition twin of [`Self::retarget_metadata_repair`]: re-request the
    /// window from the view's primary after a repaired prepare the view log does
    /// not vouch for.
    #[allow(clippy::future_not_send)]
    async fn retarget_partition_repair(&self, partition: &mut IggyPartition<B, SB>)
    where
        B: MessageBus,
    {
        let consensus = partition.consensus();
        let primary = consensus.primary_index(consensus.view());
        if !consensus.is_normal() || primary == consensus.replica() {
            return;
        }
        let from_op = consensus
            .view_log_repair_op()
            .unwrap_or_else(|| consensus.commit_min() + 1);
        let retarget = partition
            .repair
            .as_mut()
            .filter(|session| session.peer != primary)
            .map(|session| {
                session.peer = primary;
                session.nonce = iggy_common::random_id::get_uuid();
                session.from_op = from_op;
                session.idle_ticks = 0;
                (session.nonce, session.fetch_to_op)
            });
        let Some((nonce, to_op)) = retarget else {
            return;
        };
        tracing::warn!(
            shard = self.id,
            namespace_raw = partition.consensus().group(),
            primary,
            from_op,
            to_op,
            "partition repair served a prepare the view log does not vouch for; re-requesting \
             the window from the primary"
        );
        self.send_request_prepares(
            partition.consensus(),
            primary,
            nonce,
            from_op,
            to_op,
            partition.consensus().group(),
            partition.created_revision(),
        )
        .await;
    }

    /// Repair a primary-elect's merged log before it starts the view.
    ///
    /// Sibling of [`Self::maybe_request_partition_repair`], which refuses outside
    /// `Normal` because its window comes from the live commit frontier. This window
    /// comes from the parked merged log, so it runs in `ViewChange` for the replica
    /// that parked it. Without it the coverage scan in
    /// [`Self::advance_pending_partition_view`] reports an op that nothing fetches.
    /// The sweep's gap detector also requires `probe.normal`, so only the view
    /// change timeout moves the replica.
    ///
    /// `avoid` is the peer a stall just gave up on, so rotation lands on a
    /// different sender instead of the head of the same list.
    #[allow(clippy::future_not_send)]
    async fn request_partition_view_repair(
        &self,
        partition: &mut IggyPartition<B, SB>,
        from_op: u64,
        to_op: u64,
        avoid: Option<u8>,
    ) where
        B: MessageBus,
    {
        if partition.repair.is_some() || from_op > to_op {
            return;
        }
        if self.partition_repairs_inflight.get() >= PARTITION_REPAIRS_INFLIGHT_MAX {
            return;
        }
        let consensus = partition.consensus();
        let sources = view_repair_sources(consensus, from_op);
        let Some(peer) = next_view_repair_peer(&sources, avoid) else {
            // Nobody else named this op. The view-change timeout escalates;
            // re-arming the same silent sender would pin the scan behind a
            // session for nothing.
            tracing::warn!(
                shard = self.id,
                namespace_raw = consensus.group(),
                from_op,
                to_op,
                "no replica offers op {from_op} for the merged partition log; view change is \
                 stalled"
            );
            return;
        };
        let nonce = iggy_common::random_id::get_uuid();
        let namespace = consensus.group();
        let view = consensus.view();
        self.partition_repairs_inflight
            .set(self.partition_repairs_inflight.get() + 1);
        partition.repair = Some(partitions::RepairSession {
            nonce,
            view,
            from_op,
            // Merged-log numbers, not the live frontier: `commit_to_op` is what
            // the walk must reach to finish the session, `fetch_to_op` the head
            // the view will announce.
            commit_to_op: pending_commit_max(consensus),
            fetch_to_op: to_op,
            floor: None,
            peer,
            idle_ticks: 0,
        });
        tracing::info!(
            shard = self.id,
            namespace_raw = namespace,
            from_op,
            to_op,
            peer,
            "repairing toward the merged partition log before starting the view"
        );
        self.send_request_prepares(
            partition.consensus(),
            peer,
            nonce,
            from_op,
            to_op,
            namespace,
            partition.created_revision(),
        )
        .await;
    }

    /// Receiver side of a partition descriptor: accept the manifest, adopt
    /// any reusable staged segments from an earlier attempt, and start
    /// pulling, or fall back to journal repair when the peer cannot serve.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_partition_state_transfer_target(&self, msg: &Message<StateTransferTargetHeader>)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        M: StreamsFrontend,
    {
        let header = *msg.header();
        // The peer id reaches `next_transfer_peer`'s ring arithmetic through the
        // re-arm below, so it is validated before anything uses it.
        if !self.peer_is_known(header.replica, "StateTransferTarget") {
            return;
        }
        let planes = self.plane.inner();
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.group))
        else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::StateTransferTarget,
        ) {
            return;
        }
        if partition.history_is_busy() {
            partition.defer_view_transition(MessageBag::StateTransferTarget(msg.clone()));
            return;
        }
        let session_matches = partition
            .transfer
            .as_ref()
            .is_some_and(|session| session.nonce == header.nonce);
        if !session_matches {
            return;
        }
        if header.available == 0 {
            // A refusal the peer marked transient (its offer build is still
            // flushing or hashing, which is routine under produce load) must
            // not charge the consecutive-failure count:
            // that count is reset only by a completed install, so ten routine
            // refusals pin the re-arm backoff at its 1024x ceiling while nothing
            // else recovers the partition -- repair keeps hitting the refused
            // floor and will not arm while a re-arm is pending. A hard refusal
            // (unreadable segment, failed flush) still charges.
            let transient = header.unavailable_transient == 1;
            tracing::info!(
                shard = self.id,
                namespace_raw = header.group,
                peer = header.replica,
                transient,
                "partition transfer peer cannot serve; backing off before re-arming"
            );
            if transient {
                // The peer that refused is the node that would otherwise serve,
                // and on the partition arm only the primary can. Keep
                // asking it unless it is not the primary this replica knows: a
                // rotation spends the next round on a backup that can only
                // refuse, and the serving side's partial offer-build progress
                // is memoized per node, so that round advances no hashing.
                let primary = {
                    let consensus = partition.consensus();
                    consensus.primary_index(consensus.view())
                };
                self.rearm_partition_transfer_after_refusal(
                    partition,
                    header.replica,
                    header.replica != primary,
                )
                .await;
            } else {
                self.abandon_or_rearm_partition_transfer(partition, header.replica)
                    .await;
            }
            return;
        }
        // The serving replica's own progress, carried by every descriptor: an
        // offer from a replica that knows LESS than this one does is the phantom
        // view-0 primary signature (a group whose directory vanished boots
        // `init()`, comes up Normal at view 0, and an empty log is trivially
        // caught up). Installing it would unlink a chain this replica already
        // holds; nonce match alone cannot tell the two apart.
        let local_view = partition.consensus().view();
        let local_commit_max = partition.consensus().commit_max();
        // `commit_op` past the sender's OWN `commit_max` is self-contradictory:
        // the offer cannot be built past the frontier its builder had. Nothing
        // downstream bounds it above -- the install only refuses values BELOW
        // the local floor, and the offsets-artifact cross-check compares two
        // numbers the same peer chose -- so without this a peer offering
        // `commit_op = u64::MAX` drives this replica's commit floor, sequencer
        // and `commit_max` there and it reports itself fully committed.
        if header.commit_op > header.commit_max {
            tracing::warn!(
                shard = self.id,
                namespace_raw = header.group,
                peer = header.replica,
                serving_commit_op = header.commit_op,
                serving_commit_max = header.commit_max,
                "refusing a partition transfer offer whose commit_op exceeds the sender's \
                 own commit frontier"
            );
            self.abandon_or_rearm_partition_transfer(partition, header.replica)
                .await;
            return;
        }
        if header.view < local_view || header.commit_max < local_commit_max {
            tracing::warn!(
                shard = self.id,
                namespace_raw = header.group,
                peer = header.replica,
                serving_view = header.view,
                serving_commit_max = header.commit_max,
                local_view,
                local_commit_max,
                "refusing a partition transfer offer from a replica behind this one"
            );
            // ALWAYS rotate: this refusal is evidence about the peer, not about
            // its timing, so re-asking it is the one thing that cannot help.
            self.rearm_partition_transfer_after_refusal(partition, header.replica, true)
                .await;
            return;
        }
        let manifest_bytes =
            &msg.as_slice()[size_of::<StateTransferTargetHeader>()..header.size as usize];
        let entries = match consensus::decode_state_manifest(manifest_bytes) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(
                    shard = self.id,
                    namespace_raw = header.group,
                    %error,
                    "partition transfer manifest rejected"
                );
                return;
            }
        };
        // Saturating: 65k entries of hostile lengths must refuse, not
        // overflow-panic the debug-build sum before the cap check fires.
        let total_len = entries
            .iter()
            .fold(0u64, |total, entry| total.saturating_add(entry.len));
        // Per-KIND ceilings: only SEGMENT_LOG artifacts spill to disk as
        // they complete, so anything else accumulates whole in memory and
        // must be bounded by what its decoder could ever accept, not by the
        // segment cap. An unknown kind is refused here rather than pulled:
        // the install cannot represent it anyway.
        let kind_capped = entries.iter().all(|entry| match entry.kind {
            consensus::artifact_kind::SEGMENT_LOG => {
                entry.len <= self.partition_artifact_len_max.get()
            }
            consensus::artifact_kind::CONSUMER_OFFSETS => {
                entry.len <= Self::CONSUMER_OFFSETS_ARTIFACT_LEN_MAX
            }
            _ => false,
        });
        if !kind_capped || total_len > Self::PARTITION_TRANSFER_TOTAL_LEN_MAX {
            tracing::warn!(
                shard = self.id,
                namespace_raw = header.group,
                total_len,
                "partition transfer manifest exceeds artifact caps; refusing descriptor"
            );
            return;
        }
        if partition
            .transfer
            .as_ref()
            .is_some_and(|session| session.target_accepted)
        {
            // A crossed stall-retry descriptor: first-wins already served the
            // same offer, and re-accepting would discard in-flight progress
            // and re-run the staging scan for nothing.
            return;
        }
        let reused = partition.reuse_staged_segments(&entries).await;
        if !reused.is_empty() {
            tracing::info!(
                shard = self.id,
                namespace_raw = header.group,
                peer = header.replica,
                adopted = reused.len(),
                artifacts = entries.len(),
                "adopted staged segments from an earlier transfer attempt"
            );
        }
        // Re-check the nonce AFTER the await: the staging scan yields, and a
        // session re-minted underneath it must not get stamped with this
        // (now stale) offer's commit_op and manifest.
        let Some(session) = partition
            .transfer
            .as_mut()
            .filter(|session| session.nonce == header.nonce)
        else {
            return;
        };
        session.target_accepted = true;
        session.commit_op = header.commit_op;
        // No reservation here: the manifest's total is bounded only by
        // `PARTITION_TRANSFER_TOTAL_LEN_MAX` (1 TiB), so reserving every
        // artifact up front is an eager address-space commit of the whole
        // manifest -- times the in-flight cap -- which turns fatal under strict
        // overcommit, `RLIMIT_AS`, or cgroup accounting, and contradicts the
        // session's own promise to bound receiver memory to ONE in-flight
        // artifact. Artifacts adopted by the reuse scan below would also be
        // reserved and then overwritten with `Staged`, making the retry path's
        // reservation pure waste. `append_chunk` reserves the declared length on
        // an artifact's FIRST chunk instead, and only ever for the artifact the
        // cursor is actually pulling.
        session.artifacts = entries
            .iter()
            .map(|&entry| {
                TransferArtifact::Pending(consensus::ArtifactProgress {
                    entry,
                    buf: Vec::new(),
                })
            })
            .collect();
        session.idle_ticks = 0;
        for (index, meta) in reused {
            session.artifacts[index as usize] = TransferArtifact::Staged(meta);
        }
        let consensus = partition.consensus();
        if consensus.state_transfer_stage() == consensus::StateTransferStage::AwaitingTarget {
            consensus.set_state_transfer_stage(consensus::StateTransferStage::Fetching);
        }
        self.on_partition_transfer_progress(header.group).await;
    }

    /// Receive one partition chunk; spill a completed segment artifact, and
    /// on the last artifact verify + install + hand the tail to repair.
    #[allow(clippy::future_not_send)]
    async fn on_partition_state_chunk(&self, msg: &Message<StateChunkHeader>)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        M: StreamsFrontend,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(header.group))
        else {
            return;
        };
        if !self.partition_incarnation_matches(
            partition,
            header.partition_incarnation,
            Command::StateChunk,
        ) {
            return;
        }
        {
            let Some(session) = partition.transfer.as_mut() else {
                return;
            };
            if session.nonce != header.nonce || !session.target_accepted {
                return;
            }
            // The sender too, not the nonce alone. The other three
            // partition-transfer handlers all validate theirs; this one
            // authenticated payload bytes by a 128-bit capability only, which
            // is thin but real once a peer has seen one frame -- a rotated-away
            // peer still holds the nonce until the session is re-minted. Its
            // own `if` because folded into the condition above, clippy's
            // `suspicious_operation_groupings` reads the operand asymmetry as a
            // typo and proposes a `header.peer` that does not exist.
            if session.peer != header.replica {
                return;
            }
            let payload = &msg.as_slice()[size_of::<StateChunkHeader>()..header.size as usize];
            if !consensus::append_chunk(
                &mut session.artifacts,
                header.artifact,
                header.offset,
                payload,
            ) {
                return;
            }
            session.idle_ticks = 0;
        }
        partition.note_transfer_progress();
        self.on_partition_transfer_progress(header.group).await;
    }

    /// Drive an in-flight partition transfer: spill newly completed segment
    /// artifacts, request the next missing chunk, or -- with everything
    /// complete -- install and hand the tail to journal repair.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn on_partition_transfer_progress(&self, namespace: u64)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        M: StreamsFrontend,
    {
        let planes = self.plane.inner();
        let config = planes.1.0.config().clone();
        let target_namespace = IggyNamespace::from_raw(namespace);
        let Some(partition) = planes.1.0.get_mut_by_ns(&target_namespace) else {
            return;
        };
        if self.shutting_down.get() || partition.is_shutting_down() {
            return;
        }
        // Stage/session desync bail: the probe-exhausted election fallback in
        // core/consensus clears the stage without being able to reach this
        // session; completing into an illegal Idle -> Installing transition
        // would assert. Detect the out-from-under abandon and drop the
        // session here (staging files are KEPT for reuse).
        if partition.consensus().state_transfer_stage() != consensus::StateTransferStage::Fetching {
            if partition.transfer.is_some() {
                tracing::info!(
                    shard = self.id,
                    namespace_raw = namespace,
                    "partition state transfer was abandoned out from under its session; dropping it"
                );
                partition.transfer = None;
            }
            return;
        }
        let Some(session) = partition.transfer.as_ref() else {
            return;
        };
        if !session.target_accepted {
            return;
        }

        // Spill any segment artifact that just completed, freeing its buffer.
        let spill_candidate = session
            .artifacts
            .iter()
            .enumerate()
            .find_map(|(index, artifact)| {
                artifact
                    .pending()
                    .is_some_and(|progress| {
                        progress.entry.kind == consensus::artifact_kind::SEGMENT_LOG
                            && progress.complete()
                    })
                    .then_some(index)
            });
        if let Some(index) = spill_candidate {
            let (entry, bytes, peer, nonce) = {
                let Some(session) = partition.transfer.as_mut() else {
                    return;
                };
                let Some(progress) = session.artifacts[index].pending_mut() else {
                    return;
                };
                let bytes = std::mem::take(&mut progress.buf);
                (progress.entry, bytes, session.peer, session.nonce)
            };
            match partition.spill_transfer_segment(&entry, bytes).await {
                Ok(meta) => {
                    // Nonce re-check across the spill await: a session
                    // re-minted underneath it has a fresh (possibly empty)
                    // artifact vec, and the stale index would panic. Pump-
                    // serial today, but nothing enforces that.
                    if let Some(session) = partition
                        .transfer
                        .as_mut()
                        .filter(|session| session.nonce == nonce)
                    {
                        session.artifacts[index] = TransferArtifact::Staged(meta);
                    }
                }
                Err(reason) => {
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace,
                        artifact = index,
                        %reason,
                        "partition transfer segment failed validation at spill"
                    );
                    self.abandon_or_rearm_partition_transfer(partition, peer)
                        .await;
                    return;
                }
            }
            // Tail-call for the next candidate / chunk request.
            return Box::pin(self.on_partition_transfer_progress(namespace)).await;
        }

        let Some(session) = partition.transfer.as_ref() else {
            return;
        };
        let all_done = session.artifacts.iter().all(ChunkProgress::complete);
        if !all_done {
            self.request_pending_partition_chunk(namespace).await;
            return;
        }

        if partition.history_is_busy() {
            partition.notify_io();
            return;
        }

        // Everything present: verify + decode the offsets artifact, install.
        let Some(session) = partition.transfer.take() else {
            return;
        };
        let commit_op = session.commit_op;
        let peer = session.peer;
        let mut offsets_bytes: Option<Vec<u8>> = None;
        let mut offsets_frontier: Option<u64> = None;
        let mut damaged = false;
        let mut staged = Vec::new();
        for artifact in session.artifacts {
            let progress = match artifact {
                TransferArtifact::Staged(meta) => {
                    staged.push(meta);
                    continue;
                }
                TransferArtifact::Pending(progress) => progress,
            };
            match progress.entry.kind {
                consensus::artifact_kind::CONSUMER_OFFSETS
                    if consensus::verify_state_artifact(&progress.entry, &progress.buf) =>
                {
                    // Exactly one offsets table per manifest; a second one is
                    // a peer bug and is refused, never last-wins.
                    if offsets_bytes.is_some() {
                        damaged = true;
                    } else {
                        offsets_frontier = Some(progress.entry.frontier);
                        offsets_bytes = Some(progress.buf);
                    }
                }
                // An unknown kind is refused, never skipped: skipping would
                // install a state this build cannot fully represent.
                _ => damaged = true,
            }
        }
        // Free self-consistency check on a durable input: the builder sets the
        // descriptor's `commit_op` and the offsets artifact's frontier from ONE
        // binding, and `commit_op` goes on to drive `set_commit_floor`,
        // `set_sequence`, `advance_commit_max` and the reported
        // `applied_commit_op`, while nothing else ever reads that frontier back.
        if let Some(frontier) = offsets_frontier
            && frontier != commit_op
        {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace,
                commit_op,
                offsets_frontier = frontier,
                "descriptor commit_op disagrees with its offsets artifact frontier; \
                 refusing the install"
            );
            damaged = true;
        }
        let Some(offsets_bytes) = offsets_bytes.filter(|_| !damaged) else {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace,
                "partition transfer artifacts failed verification; refusing install"
            );
            self.abandon_or_rearm_partition_transfer(partition, peer)
                .await;
            return;
        };
        partition
            .consensus()
            .set_state_transfer_stage(consensus::StateTransferStage::Installing);
        let outcome = if partition.has_io_dispatcher() {
            match partition.queue_state_transfer_install(commit_op, staged, &offsets_bytes, peer) {
                Ok(()) => return,
                Err(error) => Err(error),
            }
        } else {
            partition
                .install_state_transfer(&config, commit_op, staged, &offsets_bytes)
                .await
        };
        self.finish_partition_install(namespace, peer, outcome)
            .await;
    }

    #[allow(clippy::future_not_send)]
    async fn finish_partition_install(
        &self,
        namespace: u64,
        peer: u8,
        outcome: Result<
            partitions::state_transfer::PartitionInstallOutcome,
            partitions::state_transfer::PartitionInstallError,
        >,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        let partitions = self.plane.partitions();
        let config = partitions.config().clone();
        let Some(partition) = partitions.get_mut_by_ns(&IggyNamespace::from_raw(namespace)) else {
            return;
        };
        partition
            .consensus()
            .set_state_transfer_stage(consensus::StateTransferStage::Idle);
        match outcome {
            Ok(outcome) => {
                partition.note_transfer_progress();
                partition.note_transfer_installed();
                partition.transfer_rearm = None;
                tracing::info!(
                    shard = self.id,
                    namespace_raw = namespace,
                    applied_commit_op = outcome.applied_commit_op,
                    "partition state transfer installed; handing tail to journal repair"
                );
                partition.commit_journal(&config).await;
                self.maybe_request_partition_repair(partition, peer).await;
            }
            Err(
                error @ partitions::state_transfer::PartitionInstallError::ConvergeFailed {
                    frontier,
                    ..
                },
            ) => {
                // The partition holds no serviceable segment chain and its
                // next append or poll would panic the shard. Fence exactly
                // this group (a failed converge sweep can leave strays that
                // `build_partition_fresh` would never clear and the boot
                // contiguity guard would then trip on). The reconciler
                // re-materialises a fresh partition from committed metadata;
                // its first repair floor refusal re-arms a transfer, which
                // re-seeds the offset frontier from the offsets artifact.
                tracing::error!(
                    shard = self.id,
                    namespace_raw = namespace,
                    %error,
                    "partition unserviceable after failed install; fencing it for rebuild"
                );
                // Served state first: the quarantine below moves the chain
                // those offers and cached payloads describe into `.fenced.N`,
                // and a requester holding one would otherwise pull bytes that
                // no longer exist.
                self.drop_partition_transfer_state(IggyNamespace::from_raw(namespace), partition);
                self.fence_partition_for_rebuild(
                    IggyNamespace::from_raw(namespace),
                    partition,
                    frontier,
                );
            }
            Err(error) => {
                tracing::error!(
                    shard = self.id,
                    namespace_raw = namespace,
                    %error,
                    "partition state transfer install failed; falling back to journal repair"
                );
                self.abandon_or_rearm_partition_transfer(partition, peer)
                    .await;
            }
        }
    }

    /// Charge one transfer failure and schedule a backed-off re-arm against
    /// the NEXT peer in the ring. Immediate same-peer retries were a
    /// failure amplifier: a deterministic local failure (ENOSPC, an
    /// undecodable artifact) re-ran the full pull -- including the serving
    /// primary's whole-segment reads -- at network round-trip rate, and the
    /// generation-keyed budget never exhausted on a committing cluster.
    /// Journal repair is re-armed in the meantime so the gap stays visible
    /// and anything repairable heals without waiting out the backoff.
    #[allow(clippy::future_not_send)]
    async fn abandon_or_rearm_partition_transfer(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        let failures = partition.record_transfer_failure();
        let after_ticks = transfer_rearm_backoff(self.repair_retry_ticks.get(), failures);
        self.schedule_partition_transfer_rearm(partition, peer, failures, after_ticks, true)
            .await;
    }

    /// Re-arm after a refusal the serving peer marked TRANSIENT: schedule the
    /// next attempt on a flat interval and charge nothing.
    ///
    /// "The peer's offer build is still flushing or hashing" is the common case
    /// under produce load, and `transfer_failures` is reset only by a completed
    /// install, so charging it turns a transient into a stall measured in re-arm
    /// ceilings: nothing else recovers the partition meanwhile, since repair
    /// keeps hitting the refused floor and will not arm while a re-arm is
    /// pending.
    ///
    /// `rotate` belongs to the CALLER because the two refusal sites mean
    /// opposite things by it. A peer saying "not right now" is the node that
    /// would otherwise serve, so staying on it is right. This replica refusing
    /// a descriptor from a peer that knows LESS than it does is the one case
    /// where the peer is provably the wrong one, and rotating is the whole
    /// remedy: a restarted primary comes back at `commit_max = 0` (the
    /// partition journal is memory-only), so a rejoining backup would otherwise
    /// pin itself to it at a flat interval until the group's next election.
    #[allow(clippy::future_not_send)]
    async fn rearm_partition_transfer_after_refusal(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
        rotate: bool,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        // Deliberately NOT `transfer_rearm_backoff`: a flat interval, so a peer
        // that spends a minute catching up costs a minute of retries rather than
        // a climb to the 1024x ceiling.
        let after_ticks = self.repair_retry_ticks.get();
        // The flat interval means a partition can sit here for hours without
        // charging anything, so the ONLY operator signal is this count: it
        // escalates the log level and feeds a metric, and it never touches the
        // backoff.
        let refusals = partition.record_transfer_refusal();
        self.metrics.record_partition_transfer_refusal();
        if refusals >= TRANSFER_REFUSALS_BEFORE_ESCALATION
            && refusals.is_multiple_of(TRANSFER_REFUSALS_BEFORE_ESCALATION)
        {
            // Deliberately not phrased as "not rejoining": a serving primary
            // building a large offer refuses one round per budget slice, so a
            // healthy multi-GiB rejoin reaches this count while progressing
            // normally. The descriptor carries no reason code, so this side
            // cannot tell the two apart; the serving node's own logs can.
            tracing::warn!(
                shard = self.id,
                namespace_raw = partition.consensus().group(),
                peer,
                refusals,
                "partition state transfer has been refused {refusals} times in a row; the peer \
                 may be building a large offer or rate-limiting concurrent transfers, or it may \
                 be unable to serve at all -- check its logs before intervening"
            );
        }
        self.schedule_partition_transfer_rearm(partition, peer, 0, after_ticks, rotate)
            .await;
    }

    /// Drop the session, pick the next peer, and schedule the re-arm; shared by
    /// the charged and uncharged paths.
    ///
    /// `rotate` is false where the refusing peer is the only one that could
    /// have served: only the primary serves, so rotating off it asks a backup
    /// that can answer nothing but another refusal, and the serving side's
    /// partial offer-build progress is memoized PER NODE, so the round spent on
    /// the backup also advances no hashing.
    #[allow(clippy::future_not_send)]
    async fn schedule_partition_transfer_rearm(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
        failures: u32,
        after_ticks: u32,
        rotate: bool,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        partition.transfer = None;
        let consensus = partition.consensus();
        if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
            consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
        }
        let next_peer = if rotate {
            next_transfer_peer(
                consensus.replica(),
                peer,
                consensus.replica_count(),
                consensus.primary_index(consensus.view()),
            )
        } else {
            peer
        };
        tracing::info!(
            shard = self.id,
            namespace_raw = partition.consensus().group(),
            failures,
            next_peer,
            after_ticks,
            "partition transfer did not land; scheduling a re-arm"
        );
        // The stall budget belongs to ONE attempt: carried across, an exhausted
        // count left every later session a single retry-interval window to land
        // its first response, against a backoff climbing to 1024x. Livelock
        // across attempts is bounded by `transfer_failures` and that backoff.
        partition.note_transfer_rearm_scheduled();
        partition.transfer_rearm = Some(partitions::state_transfer::PendingTransferRearm {
            peer: next_peer,
            after_ticks,
        });
        let config = self.plane.partitions().config().clone();
        partition.commit_journal(&config).await;
        self.maybe_request_partition_repair(partition, peer).await;
    }

    /// Ask for the next missing partition chunk (first unspilled, incomplete
    /// artifact in manifest order).
    ///
    /// LOCKSTEP by design: one chunk in flight, re-driven per reply, so transfer
    /// throughput is `state_chunk_len_max / RTT` -- roughly 26 MB/s at a 10 ms
    /// link, about 41 s for a 1 GiB segment. `state_chunk_len_max` only clamps
    /// downward, so no operator knob raises that ceiling; it is worth knowing
    /// when sizing `segment.size` and retention, since rejoin time scales with
    /// retained bytes per partition. A small in-flight window would lift it, but
    /// it has to grow `[partition] transfer_served_cache_bytes_max` in step -- that
    /// budget is sized for exactly the concurrent lockstep pulls the in-flight
    /// cap allows.
    #[allow(clippy::future_not_send)]
    async fn request_pending_partition_chunk(&self, namespace: u64)
    where
        B: MessageBus,
    {
        let planes = self.plane.inner();
        let chunk_len_max = self.state_chunk_len_max() as u64;
        let Some(partition) = planes
            .1
            .0
            .get_mut_by_ns(&IggyNamespace::from_raw(namespace))
        else {
            return;
        };
        let request = partition.transfer.as_ref().and_then(|session| {
            if !session.target_accepted {
                return None;
            }
            let (index, offset, len) =
                consensus::next_pending_chunk(&session.artifacts, chunk_len_max)?;
            Some((session.nonce, session.peer, index, offset, len))
        });
        let consensus_ids = {
            let consensus = partition.consensus();
            (consensus.cluster(), consensus.replica())
        };
        if let Some((nonce, peer, artifact, offset, len)) = request {
            self.send_request_state_chunk(
                consensus_ids.0,
                consensus_ids.1,
                peer,
                nonce,
                namespace,
                partition.created_revision(),
                artifact,
                offset,
                len,
            )
            .await;
        }
    }

    /// Drop serving-side state-transfer offers that stopped being pulled.
    ///
    /// An offer pins its plane's payload for as long as it lives -- the metadata
    /// snapshot plus the encoded client table, or a partition manifest and the
    /// resident segment payloads behind it -- and the protocol has no completion
    /// frame (a receiver installs and goes quiet), so without this a primary
    /// that ever served a transfer holds that memory for the rest of the
    /// process. Generous relative to the chunk cadence: a live puller resets the
    /// counter on every chunk it fetches, so only an abandoned or finished
    /// transfer ages out.
    fn expire_idle_state_transfer_offers(&self) {
        // Same clock the offers below age on: `retry_ticks * MULTIPLE` ticks,
        // and this sweep runs once per tick.
        let payload_idle_sweeps = u64::from(self.repair_retry_ticks.get().max(1))
            * u64::from(STATE_TRANSFER_OFFER_EXPIRY_MULTIPLE);
        self.served_segment_cache
            .borrow_mut()
            .expire_idle(payload_idle_sweeps);
        // `max(1)`: the retry interval is operator-configurable, and a zero would
        // make the expiry zero, dropping every offer on the tick after it was
        // built and breaking transfers outright.
        let retry_ticks = self.repair_retry_ticks.get().max(1);
        let idle_expiry_ticks = retry_ticks.saturating_mul(STATE_TRANSFER_OFFER_EXPIRY_MULTIPLE);
        let served_expiry_ticks = retry_ticks.saturating_mul(STATE_TRANSFER_SERVED_EXPIRY_MULTIPLE);
        // A build slot is released by the round that completes the offer, so a
        // requester that walked away mid-build would otherwise hold admission
        // forever. Same idle window as an abandoned offer.
        let mut abandoned = Vec::new();
        self.partition_offer_builds
            .borrow_mut()
            .retain(|namespace, idle_ticks| {
                *idle_ticks += 1;
                let live = *idle_ticks < idle_expiry_ticks;
                if !live {
                    tracing::debug!(
                        shard = self.id,
                        namespace_raw = namespace,
                        "dropping an abandoned partition offer build slot"
                    );
                    abandoned.push(*namespace);
                }
                live
            });
        let mut offers = self.state_transfer_offers.borrow_mut();
        let namespaces_before: Vec<u64> = offers.keys().map(|(namespace, _)| *namespace).collect();
        offers.retain(|(namespace, requester), served| {
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
                    namespace_raw = namespace,
                    requester,
                    fully_served = served.fully_served,
                    "dropping a state-transfer offer"
                );
            }
            live
        });
        // Nobody is pulling from the metadata plane: release its cached
        // snapshot copy too, rather than pinning it for the life of the
        // process. Runs on every shard, but only shard 0 ever populates the
        // metadata cache, so it is a no-op elsewhere.
        let metadata = self.plane.metadata();
        let metadata_served = metadata.consensus.as_ref().is_some_and(|consensus| {
            offers
                .keys()
                .any(|(namespace, _)| *namespace == consensus.group())
        });
        if !metadata_served {
            metadata.clear_state_transfer_offer_cache();
        }
        // Partition offer caches: release each namespace whose LAST offer
        // just aged out, so a served-once partition does not pin its offer
        // (manifest + offsets table) for the process lifetime, and each whose
        // build was abandoned, so its pinned plan cannot answer a much later
        // request with stale state.
        let mut vanished = namespaces_before;
        vanished.retain(|namespace| !offers.keys().any(|(live, _)| live == namespace));
        vanished.extend(abandoned);
        vanished.sort_unstable();
        vanished.dedup();
        drop(offers);
        let partitions = self.plane.partitions();
        for namespace in vanished {
            if let Some(partition) = partitions.get_by_ns(&IggyNamespace::from_raw(namespace)) {
                partition.clear_state_transfer_offer_cache();
            }
        }
    }

    /// Drop the WAL entry at `stuck_op` and the suffix above it, so repair can
    /// refill a header whose body the commit walk cannot read.
    ///
    /// Nothing else clears it: `on_repair_prepare` returns early for an op
    /// whose header is resident, and the append under it is refused anyway.
    /// `stuck_op` is the op the walk stopped on: `commit_min + 1` once this
    /// tick's walk has advanced, and at or under `commit_max`, so a quorum
    /// holds it and repair can serve it back.
    ///
    /// SERIALIZATION: same argument as `reconcile_metadata_view_divergence`,
    /// which is the other shard-side `truncate_from` caller. This runs on the
    /// pump between frames, so no append is in flight for these ops.
    #[allow(clippy::future_not_send)]
    async fn drop_unwalkable_metadata_entry<P>(
        &self,
        consensus: &VsrConsensus<B, P>,
        journal: &MJ,
        stuck_op: u64,
    ) where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        if consensus.view_log_unproven()
            || consensus.pipeline_head_header().is_some()
            || self.metadata_walk_stuck_op.get() == stuck_op
        {
            return;
        }
        let Ok(slot) = usize::try_from(stuck_op) else {
            return;
        };
        let Some(header) = journal.handle().header(slot).map(|header| *header) else {
            return;
        };
        let view = consensus.view();
        if journal.handle().entry(&header).await.is_some() {
            return;
        }
        // The read can yield to a pipeline driver or a view change. Never
        // truncate a suffix another driver owns, even when this body is absent.
        if consensus.view() != view
            || consensus.view_log_unproven()
            || consensus.is_transferring()
            || consensus.commit_min().checked_add(1) != Some(stuck_op)
            || consensus.pipeline_head_header().is_some()
            || journal
                .handle()
                .header(slot)
                .is_none_or(|resident| resident.checksum != header.checksum)
        {
            return;
        }
        let (op, checksum) = metadata_wal_head(journal.handle(), stuck_op);
        consensus.lower_history_certificate(stuck_op, op, checksum);
        // The tick persists the lowered binding, and a later walk removes the
        // entry the durable certificate named.
        if consensus.history_certificate_needs_persist() {
            return;
        }
        // A failed truncation keeps the header. Retrying it every tick only
        // repeats the same storage error.
        self.metadata_walk_stuck_op.set(stuck_op);
        match journal.handle().truncate_from(stuck_op).await {
            Ok(removed) => {
                // The snapshot's head and commit point do not move when entries
                // are removed under them, so without this the next `DoViewChange`
                // would advertise headers this replica can no longer serve.
                consensus.note_journal_mutation();
                tracing::warn!(
                    shard = self.id,
                    stuck_op,
                    removed,
                    "metadata commit walk found a resident header with no body at op {stuck_op}; \
                     dropped {removed} entries from it so repair can refill the range"
                );
            }
            Err(error) => {
                tracing::error!(
                    shard = self.id,
                    stuck_op,
                    %error,
                    "could not drop the unwalkable entry at op {stuck_op}; journal repair skips \
                     ops it already holds a header for, so this replica will not walk past \
                     it until it is restarted"
                );
            }
        }
    }

    /// Read the gap probe off the metadata plane; [`partition_gap_probe`]'s
    /// twin. A shard method because the recovery slots live here, on the shard,
    /// not on the plane.
    fn metadata_gap_probe<P>(&self, consensus: &VsrConsensus<B, P>, journal: &MJ) -> GapProbe
    where
        B: MessageBus,
        P: Pipeline<Entry = consensus::PipelineEntry>,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    {
        let commit_min = consensus.commit_min();
        let commit_max = consensus.commit_max();
        let normal = consensus.is_normal();
        let transferring = consensus.is_transferring();
        let recovery_owned =
            self.metadata_transfer.borrow().is_some() || self.metadata_repair.borrow().is_some();
        // Residency last, and only once the guards both predicates share hold,
        // as in `partition_gap_probe`: a caught-up plane would otherwise pay a
        // journal lookup whose answer both predicates discard.
        //
        // Safe against the snapshot floor: a checkpoint drains only to
        // `commit_min`, so `commit_min + 1` never sits below it and a `None` is
        // a real hole.
        //
        // The header ring is only half of what the walk needs. `commit_journal`
        // reads the BODY through `entry()`, which answers `None` for an op the
        // ring names but the WAL cannot produce, and stops there with
        // `CommitWalkStop::BodyMissing`. Reading the body here instead is not an
        // option (it is an async WAL read, per tick, on the walk's fast path),
        // so the walk reports the op it stopped on and this treats that op as
        // absent, which it is for every purpose this probe serves. Without it
        // the two disagree forever: the walk cannot move, the probe keeps
        // calling the group walk-stalled, the debounce keeps resetting, and
        // repair never arms.
        //
        // Self-clearing: any path that advances `commit_min` past the stuck op
        // leaves `stuck_op != commit_min + 1`, so nothing has to retract it.
        //
        // The walk also refuses an unproven view log, so while the proof waits on
        // a body the resident op is no more walkable than a hole. Counting it
        // walkable leaves a backup whose repair window closed below the op the
        // proof names with no site that arms the next one.
        let next_op = commit_min.saturating_add(1);
        #[allow(clippy::cast_possible_truncation)]
        let next_op_resident = normal
            && !transferring
            && commit_min < commit_max
            && self.metadata_walk_stuck_op.get() != next_op
            && consensus.view_log_repair_op().is_none()
            && journal.handle().header(next_op as usize).is_some();
        let missing_suffix = normal
            && !transferring
            && !recovery_owned
            && consensus
                .view_log_repair_op()
                .is_some_and(|op| op > commit_max);
        GapProbe {
            normal,
            transferring,
            recovery_owned,
            commit_min,
            commit_max,
            next_op_resident,
            missing_suffix,
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn tick_metadata(&self)
    where
        B: MessageBus + 'static,
        T: ShardsTable,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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

        // See the partition tick: no snapshot consumer on a `Normal` tick.
        if consensus.status() != Status::Normal {
            refresh_metadata_dvc_suffix(consensus, metadata.journal.as_ref());
        }
        let actions = consensus.tick(PlaneKind::Metadata);
        if !consensus.is_primary() || !primary_rebalance::consensus_ready(consensus) {
            self.primary_rebalance.borrow_mut().reset_authority();
        }

        let (local_actions, wire_actions) = split_local_actions(actions);
        dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &local_actions).await;
        if metadata.persist_superblock_if_needed(consensus).await {
            dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &wire_actions).await;
        }
        let superblock_failures = metadata.superblock_write_failures();
        if superblock_wedged(
            superblock_failures,
            self.superblock_wedged_fatal_failures.get(),
        ) {
            fatal(
                FatalReason::SuperblockWedged,
                &format!(
                    "metadata superblock persist failed {superblock_failures} consecutive times, \
                     past the [cluster] superblock_wedged_fatal_timeout window; exiting so a \
                     supervisor handles the wedge instead of the replica limping fenced"
                ),
            );
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
        metadata.refresh_placement().await;
        self.tick_primary_rebalance().await;

        self.advance_pending_metadata_view().await;
        self.expire_idle_state_transfer_offers();

        // Level-triggered gap detector, the metadata twin of the one in
        // `tick_partitions`, and starvable in exactly the same way:
        // `replicate_preflight` advances `commit_max` before the gap check
        // drops the prepare, so under sustained traffic the heartbeat lands as
        // `Accepted` and the `Advanced`-gated arm in `on_commit` never fires.
        //
        // Placed before the transfer-stall block below: that block's exhausted
        // branch returns early, so a detector after it would be skipped on the
        // tick that abandons a transfer.
        let contradictions = consensus.take_applied_log_contradictions();
        if contradictions > 0 {
            self.metrics
                .record_metadata_applied_log_contradictions(contradictions);
        }
        if let Some(journal) = metadata.journal.as_ref() {
            let gap_drops = metadata.take_prepare_gap_drops();
            if gap_drops > 0 {
                self.metrics.record_metadata_prepare_gap_drops(gap_drops);
            }
            let probe = self.metadata_gap_probe(consensus, journal);
            let mut gap_ticks = self.metadata_gap_ticks.get();
            // Always budgeted: one metadata group per node, so there is no
            // correlated fan-out for a per-tick rate cap to spread.
            let verdict = drive_group_gap_debounce(
                &probe,
                &mut gap_ticks,
                self.repair_gap_debounce_ticks.get(),
                true,
            );
            self.metadata_gap_ticks.set(gap_ticks);
            if verdict == GapArm::Arm {
                match gap_repair_peer(
                    consensus.replica(),
                    consensus.replica_count(),
                    consensus.primary_index(consensus.view()),
                ) {
                    None => {
                        // Restart the debounce so this repeats at its interval,
                        // not every tick.
                        self.metadata_gap_ticks.set(0);
                        tracing::warn!(
                            shard = self.id,
                            commit_min = probe.commit_min,
                            commit_max = probe.commit_max,
                            "metadata is gap-stopped below its own commit frontier with no peer \
                             to repair from"
                        );
                    }
                    // Always repair, never classify the gap up front: a window
                    // below the peer's retention floor is answered
                    // `RangeEvicted`, and `on_repair_range_reply` converts that
                    // to a state transfer. The floor is only ever learned
                    // through that refusal. The arm logs the window it settled
                    // on, so nothing is logged here.
                    Some(peer) => self.maybe_request_metadata_repair(consensus, peer).await,
                }
            }
            // Undebounced, like the partition walk arm, and unrated: there is
            // one group to walk here rather than a shard-wide fan-out, so
            // nothing needs spreading across ticks. How FAR one walk goes is
            // still capped, inside `commit_journal` itself.
            //
            // Both roles, like the partition arm. `resume_stranded_commits`
            // above re-drives a primary's pipeline. An inherited prefix or
            // the tail of a capped journal walk still needs this re-driver;
            // `commit_journal` stops at any pipeline rebuilt by promotion
            // after the earlier resume.
            //
            // Not gated on `recovery_owned` (repaired prepares are journaled
            // without being walked, so gating parks the walk for the whole
            // session); `group_is_walk_stalled` itself refuses mid-transfer,
            // where a walk past the incoming `snapshot_seq` would break the
            // install.
            if group_is_walk_stalled(&probe) {
                // Debug, not info: a repair stream journals its prepares without
                // walking them, so this is the steady state for the whole
                // duration of a rejoin and would be one line per tick.
                tracing::debug!(
                    shard = self.id,
                    commit_min = probe.commit_min,
                    commit_max = probe.commit_max,
                    "metadata commit walk parked over resident committed ops; resuming"
                );
                // Truncate on `BodyMissing` alone, which names the unreadable op
                // even after this walk advanced `commit_min` part of the way.
                // Promotion above can hand the pipeline ownership after the
                // stranded-commit driver ran, so the drop re-verifies the absent
                // body and the pipeline before removing any header.
                //
                // Recording the op stops the detector calling this a parked
                // walk, but arming repair alone cannot refill it: the ingest
                // skips an op whose header is resident and `append` refuses the
                // slot under it, so the header has to go first.
                if let CommitWalkStop::BodyMissing { op: stuck_op } =
                    metadata.commit_journal().await
                {
                    self.drop_unwalkable_metadata_entry(consensus, journal, stuck_op)
                        .await;
                }
            }
        }

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
                    "metadata state transfer stalled past its retry budget; abandoning and falling back to journal repair"
                );
                *self.metadata_transfer.borrow_mut() = None;
                if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
                    consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
                }
                metadata.commit_journal().await;
                // Rotated, not `primary_index` raw: this replica can BE the
                // primary here (a leading replica that transferred to catch up
                // on a checkpoint it lacked), and the arm refuses self.
                if let Some(next_peer) = gap_repair_peer(
                    consensus.replica(),
                    consensus.replica_count(),
                    consensus.primary_index(consensus.view()),
                ) {
                    self.maybe_request_metadata_repair(consensus, next_peer)
                        .await;
                }
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

        self.retry_stalled_metadata_repair(consensus).await;
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
    <J as JournalHandle>::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    tracing::info!(
        view = consensus.view(),
        op = consensus.sequencer().current_sequence(),
        commit = consensus.commit_max(),
        namespace = consensus.group(),
        "answering stale-view heartbeat with StartView"
    );
    // Unsolicited, answering a stale-view heartbeat rather than a probe, so there is
    // no incarnation to echo; freshness comes from the receiver's view checks. Sent to
    // every backup, since a replica heartbeating an older view has peers that missed
    // the view change with it.
    let action = VsrAction::SendStartView {
        view: consensus.view(),
        op: consensus.sequencer().current_sequence(),
        commit: consensus.dvc_commit(),
        incarnation: 0,
        target: None,
        group: consensus.group(),
        // The headers, not just the frontier. Repair skips an op whose header is
        // already resident, so a peer holding a DIFFERENT entry at an op under
        // this commit point never learns of it from repair alone: it adopts the
        // commit point and applies what it already has.
        suffix: consensus.local_dvc_suffix().headers().to_vec(),
    };
    dispatch_vsr_actions::<B, P, J>(consensus, None, &[action]).await;
}

/// Rebuild the new primary's pipeline over `from_op..=to_op` from local journal
/// headers.
///
/// A gap means the caller started the view before its journal could serve the
/// merged log: a bug in the transition, not a data condition. Nothing is
/// truncated, because truncating to the last findable op discards ops committed
/// on a quorum and already acknowledged. The pipeline is left short, the commit
/// walk stalls at the gap, and repair fills it in.
fn rebuild_pipeline_entries<B, P>(
    consensus: &VsrConsensus<B, P>,
    self_id: u8,
    from_op: u64,
    to_op: u64,
    header_at: impl Fn(u64) -> Option<PrepareHeader>,
    local_ack: impl Fn(&PrepareHeader) -> bool,
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    let mut gap_at = None;
    let entries: Vec<_> = (from_op..=to_op)
        .map_while(|op| {
            let header = header_at(op).or_else(|| {
                gap_at = Some(op);
                None
            })?;
            // Lift the monotonic timestamp floor to the rebuilt log so
            // post-view-change prepares cannot stamp below committed ones.
            consensus.observe_prepare_timestamp(header.timestamp);
            let mut entry = consensus::PipelineEntry::new(header);
            if local_ack(&header) {
                entry.add_ack(self_id);
            }
            Some(entry)
        })
        .collect();

    if let Some(missing_op) = gap_at {
        tracing::error!(
            replica = self_id,
            missing_op,
            range_start = from_op,
            range_end = to_op,
            rebuilt = entries.len(),
            "RebuildPipeline: journal gap at op {missing_op} while starting a view; leaving the \
             sequencer at {to_op} and stalling the commit walk. Truncating here would discard ops \
             the view change proved recoverable."
        );
    }

    consensus.with_pipeline_mut(|pipeline| {
        for entry in entries {
            pipeline.push(entry);
        }
    });
}

/// Snapshot this replica's uncommitted suffix into consensus, if the journal has
/// moved since the last snapshot.
///
/// Called before every handler that could start or join a view change: consensus
/// records its own `DoViewChange` there and has no journal to read. A stale
/// snapshot is never reused; consensus tags it with the journal's head, commit
/// point and mutation count, and falls back to an empty suffix rather than
/// nacking an op since acquired. The empty suffix is no safe vote either: from a
/// canonical sender it lets every lower-`log_view` copy of an op count as a nack,
/// so every vote must follow a refresh.
fn refresh_metadata_dvc_suffix<B, P, MJ>(consensus: &VsrConsensus<B, P>, journal: Option<&MJ>)
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
    MJ: JournalHandle,
    <MJ as JournalHandle>::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    if !consensus.local_dvc_suffix_stale() {
        return;
    }
    let op = consensus.sequencer().current_sequence();
    let commit = consensus.commit_max().min(op);
    let pending = adopted_view_headers(consensus);
    consensus.set_local_dvc_suffix(build_metadata_dvc_suffix(
        journal,
        commit,
        op,
        pending.as_ref().map(|pending| pending.headers.as_slice()),
        consensus.view_log_unproven_range(),
    ));
}

/// The adopted view's headers, when they describe a log this replica has NOT itself
/// decided.
///
/// Never the primary-elect's parked log: that is the proposal its own merge
/// produced, still being repaired toward, and it may contain ops a later view
/// truncated, so stitching it into its own `DoViewChange` would re-assert them.
///
/// A backup's parked log is the opposite: headers the view already decided and
/// announced, which this replica acknowledged and is repairing to hold. A later
/// view change drops the parked copy, so past it the retained one answers, for a
/// primary-elect too.
fn adopted_view_headers<B, P>(consensus: &VsrConsensus<B, P>) -> Option<consensus::MergedLog>
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    if consensus.is_primary_for_view(consensus.view()) {
        return consensus.adopted_view_log();
    }
    consensus
        .pending_view_log()
        .or_else(|| consensus.adopted_view_log())
}

/// Snapshot a partition's uncommitted suffix into its consensus.
///
/// Same contract as [`refresh_metadata_dvc_suffix`]. The partition journal
/// is in-memory only, so after a restart it reads empty and this replica votes
/// all-nack: correct, since the ops really are lost and the merge needs a peer
/// that still holds them.
///
/// Read through `repair_header`, not the resident headers: the committed prefix
/// leaves those as soon as its bytes reach a segment, which on a caught-up
/// replica includes the commit point itself.
fn refresh_partition_dvc_suffix<B, SB>(partition: &partitions::IggyPartition<B, SB>)
where
    B: MessageBus,
    SB: SuperblockStore,
{
    let consensus = partition.consensus();
    if !consensus.local_dvc_suffix_stale() {
        return;
    }
    let op = consensus.sequencer().current_sequence();
    let commit = consensus.commit_max().min(op);
    let journal = partition.log.journal();
    let pending = adopted_view_headers(consensus);
    // The window materialized once: probing `repair_header` per op is two linear
    // scans each, up to `DVC_HEADERS_MAX` of them, on every SVC/DVC arrival and
    // non-Normal tick, on the pump. The internal clamp only narrows this range.
    let head = op.max(
        pending
            .as_ref()
            .and_then(|pending| pending.headers.first())
            .map_or(0, |header| header.op),
    );
    let window = journal.inner.repair_headers_in(commit.max(1)..=head);
    let suffix = build_dvc_suffix(
        commit,
        op,
        |entry_op| window.get(&entry_op).copied(),
        pending.as_ref().map(|pending| pending.headers.as_slice()),
        consensus.view_log_unproven_range(),
    );
    consensus.set_local_dvc_suffix(suffix);
}

/// The suffix headers a `DoViewChange` or `StartView` carries, as raw bytes.
///
/// `size` is attacker-controlled, so it is clamped to what arrived; a short read
/// decodes as a malformed suffix and the DVC is dropped.
fn control_suffix_body<H>(msg: &Message<H>) -> &[u8]
where
    H: iggy_binary_protocol::ConsensusHeader,
{
    let slice = msg.as_slice();
    let start = size_of::<H>();
    let end = (msg.header().size() as usize).min(slice.len());
    if end <= start {
        return &[];
    }
    &slice[start..end]
}

/// Seal a control-message body. Zero for an empty body, which is the unsealed
/// sentinel every other integrity field in this protocol uses.
fn control_body_checksum(body: &[u8]) -> u128 {
    if body.is_empty() {
        return 0;
    }
    u128::from(iggy_common::calculate_checksum(body))
}

/// The body of a control frame, once it matches the checksum its header carries.
///
/// `None` means corruption in transit and the frame must be dropped whole: the
/// header numbers describe a body that did not arrive intact, so neither half is
/// trustworthy. This is what covers a body-carrying control message end to end.
///
/// Keyed on whether a body is present, NOT on whether `checksum_body` looks
/// sealed: skipping the check when that field reads zero makes the layer
/// bypassable by clearing the one field that decides whether anything is checked.
/// A frame legitimately carries no body (a sender with nothing uncommitted, a
/// probe-answer `StartView`), so emptiness is the only exemption. A non-empty body
/// always came from a sender that seals it, and a zero checksum there is corruption.
fn control_suffix_body_verified<H>(msg: &Message<H>, checksum_body: u128) -> Option<&[u8]>
where
    H: iggy_binary_protocol::ConsensusHeader,
{
    let body = control_suffix_body(msg);
    if body.is_empty() {
        // Nothing to verify. `checksum_body` is irrelevant either way.
        return Some(body);
    }
    if control_body_checksum(body) == checksum_body {
        Some(body)
    } else {
        None
    }
}

/// Lowest op of a primary-elect's merged log this replica can be held to.
///
/// The merged commit point is what the cluster committed, `commit_min` what this
/// replica applied. They diverge whenever this replica has not applied the merged
/// commit point: a hole in the local prefix, or plain apply lag. Taking the lower
/// keeps coverage, repair scope and the stall retry asking about the same ops.
fn merged_log_scan_floor(pending: &MergedLog, commit_min: u64) -> u64 {
    pending.commit_max.min(commit_min + 1).max(1)
}

/// The two floors on a merged-log coverage scan.
///
/// Named, not positional: both are `u64`, they sit next to each other, swapping
/// them compiles, and the partition site passes the same value for both.
#[derive(Debug, Clone, Copy)]
struct ScanFloor {
    /// Ops at or below this are gone AND already settled: compacted under a
    /// snapshot (metadata) or at or below the local commit point (partitions).
    /// Neither can diverge from the merged log and no repair puts the entry back,
    /// so demanding one parks the view change forever.
    repair_floor: u64,
    /// Highest op this replica has applied. See [`merged_log_scan_floor`].
    commit_min: u64,
}

impl ScanFloor {
    /// Lowest op the scan probes.
    fn opens_at(self, pending: &MergedLog) -> u64 {
        merged_log_scan_floor(pending, self.commit_min).max(self.repair_floor + 1)
    }
}

/// Replicas a primary-elect can ask for `op` while its merged log is parked.
///
/// Offered bodies first: those senders proved they hold the entry. A gap below
/// every sender's commit point has none, since a DVC suffix spans `commit..=op`
/// and says nothing underneath, so fall back to senders that committed the op.
/// They hold it or compacted it, and `RangeEvicted` says which.
///
/// Both planes: a partition primary-elect parks a merged log the same way.
fn view_repair_sources<B, P>(consensus: &VsrConsensus<B, P>, op: u64) -> Vec<u8>
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    let offered = consensus.pending_view_body_sources(op);
    if offered.is_empty() {
        consensus.pending_view_commit_sources(op)
    } else {
        offered
    }
}

/// Where a stalled repair session reopens its window.
///
/// A merged-log session reopens exactly where it was armed. Its floor is COVERAGE,
/// not the walk: `first_op_not_covered` reports ops whose journal entry is absent
/// or diverging, and an op can be applied (`commit_min` past it) while its entry is
/// gone. Raising the floor to `commit_min + 1` there skips the very op the scan
/// reported, and the view change parks on it forever.
///
/// A tail-repair session is the other way round. Its window IS the commit gap, so
/// ops the walk has since consumed must not be asked for again. `session.from_op`
/// still floors it, carrying the initial arm's snapshot clamp so no retry asks for
/// compacted ops.
fn stalled_repair_from_op(session_from_op: u64, commit_min: u64, repairing_view: bool) -> u64 {
    if repairing_view {
        session_from_op
    } else {
        session_from_op.max(commit_min + 1)
    }
}

/// Walk a merged-log source list one step past `avoid`, wrapping.
///
/// A ring, not a filter. The list is `log_view`-ordered and identical on every
/// call, so `find(|c| *c != avoid)` yields the head for every peer but the head
/// itself and a third sender is never reached.
///
/// `None` when the list is empty or `avoid` is its only entry.
fn next_view_repair_peer(sources: &[u8], avoid: Option<u8>) -> Option<u8> {
    let Some(avoid) = avoid else {
        return sources.first().copied();
    };
    let Some(index) = sources.iter().position(|candidate| *candidate == avoid) else {
        // The peer that stalled is not in this list at all (the DVC quorum moved
        // under it), so nothing has been tried yet from where we now stand.
        return sources.first().copied();
    };
    let next = sources[(index + 1) % sources.len()];
    if next == avoid { None } else { Some(next) }
}

/// The merged log's commit point while a view change is parked; the live frontier
/// otherwise.
fn pending_commit_max<B, P>(consensus: &VsrConsensus<B, P>) -> u64
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    consensus
        .with_pending_view_log(|pending| pending.commit_max)
        .unwrap_or_else(|| consensus.commit_max())
}

/// Whether a repaired prepare at `op` falls inside the range this replica is
/// currently repairing.
///
/// A parked log means two things depending on who parked it, and only one is a
/// repair window. The primary-elect parked the log its merge decided and repairs
/// toward exactly that range, so the range IS its scope, including ops at or
/// below `commit_min`: those are the headers inherited from senders behind the
/// canonical `log_view`, which the ordinary rule would reject and header repair
/// cannot walk back to. A backup's parked `StartView` suffix is only what its
/// ingest verifies bodies against, and its repair runs for the whole view, so
/// reading that range as a scope would discard every later op.
fn repair_op_in_scope(
    pending: Option<&MergedLog>,
    is_primary_elect: bool,
    commit_min: u64,
    op: u64,
) -> bool {
    pending
        .filter(|_| is_primary_elect)
        .map_or(op > commit_min, |pending| {
            (op >= merged_log_scan_floor(pending, commit_min) && op <= pending.op_head)
                || pending
                    .committed_elsewhere
                    .iter()
                    .any(|expected| expected.op == op)
        })
}

/// Ceiling on the op range a repair request may ask this replica to walk.
///
/// Only peers in this view may receive an uncommitted suffix. During view
/// change only its primary may receive it to repair its parked merged log.
/// An ordinary backup can accept an unnamed op as committed history.
///
/// Bounded by the local frontier all the same. `RequestPreparesHeader::validate`
/// accepts any `from_op <= to_op`, so `u64::MAX` is legal, and the metadata serve
/// path then walks op by op with no `.await` -- on a single-threaded shard pump
/// that ends the shard rather than merely serving slowly. Nothing above the
/// frontier is servable, so the clamp costs nothing.
fn repair_serve_ceiling<B, P>(
    request: &RequestPreparesHeader,
    consensus: &VsrConsensus<B, P>,
) -> u64
where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
{
    let suffix_allowed = request.view == consensus.view()
        && (consensus.is_normal() || request.replica == consensus.primary_index(consensus.view()));
    let ceiling = if suffix_allowed {
        consensus
            .commit_max()
            .max(consensus.sequencer().current_sequence())
    } else {
        consensus.commit_min()
    };
    request.to_op.min(ceiling)
}

/// Repair sessions the partition tick sweep will OPEN per pass.
///
/// The RATE half of the pair: it spreads the cost of OPENING sessions, while
/// [`PARTITION_REPAIRS_INFLIGHT_MAX`] bounds how many stand at once. One arm is
/// a `RequestPrepares` plus a repair stream the serving peer walks
/// synchronously, and a node-wide gap (a rejoin, a lossy link) makes every group
/// on this shard due in the same tick.
///
/// Over-cap groups stay due with their debounce satisfied and arm on a later
/// pass. No cursor: an armed group leaves the gap-stopped set for the life of
/// its session, so the queue drains in namespace order on its own, and letting
/// a deferred arm move the walk cursor would pull the walk's resume point
/// backwards.
const PARTITION_REPAIR_ARMS_PER_TICK_MAX: usize = 3;

/// Live repair sessions this shard will hold at once.
///
/// The concurrency ceiling the rate cap above is not: without it a node-wide
/// rejoin puts every group's stream in flight within `groups / arms` ticks, and
/// each one is a window the SERVING peer walks on its own pump, so the cost
/// lands on a node that has nothing wrong with it. Sized at twice
/// [`IggyShard::PARTITION_TRANSFERS_INFLIGHT_MAX`]: a repair streams journal
/// entries the peer already holds resident, where a transfer reads and hashes
/// whole segments, so more of them fit in the same serving budget.
///
/// Applied inside `maybe_request_partition_repair`, not at any one caller: the
/// four edge-triggered sites arm from frame handlers, and a node-wide view
/// change drives `on_start_view` for every group on the shard at once, which no
/// per-sweep budget can see. Over-cap groups stay gap-stopped with their
/// debounce satisfied, so they arm as sessions complete.
const PARTITION_REPAIRS_INFLIGHT_MAX: usize = 8;

/// Commit walks the partition tick sweep will RUN per pass.
///
/// Same correlated-fan-out argument as the repair arm, and the walk is the
/// costlier half: `commit_journal` applies committed state and schedules
/// segment persistence.
///
/// The two caps together are what bound the tick: this one bounds how many
/// groups a sweep walks, [`partitions::COMMIT_WALK_OPS_MAX`] bounds how far
/// each walk goes (for every caller of `commit_journal`, not just this one),
/// and the product is the sweep's worst case. File jobs are bounded separately
/// by the partition I/O lane; commit walks over budget wait for the next tick.
///
/// Capping cannot starve a partition: the walk carries no debounce counter and
/// clears its own predicate (a walk either advances `commit_min` or fences the
/// partition), and [`rotate_sweep_to_cursor`] resumes the next sweep at the
/// first group this one turned away, so the eligible set drains in
/// `ceil(groups / cap)` ticks however many groups are owed at once.
const PARTITION_WALKS_PER_TICK_MAX: usize = 16;

/// Placement observations need only keep pace with rebalance probes.
const PARTITION_PLACEMENT_REPORT_INTERVAL_TICKS: u128 =
    primary_rebalance::PROBE_INTERVAL.as_millis() / CONSENSUS_TICK_INTERVAL.as_millis();

/// The smallest lifecycle batch sent per tick until the interval's pass ends.
/// The cursor also bounds visits to ineligible groups.
const PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN: usize = 64;

/// Floor under the gap detector's debounce, in ticks.
///
/// The debounce reads `[cluster] repair_gap_debounce_interval`, and
/// `duration_to_ticks` floors that at one tick. One tick of lag is ordinary
/// pipelining, so without a floor of its own a shortened interval would arm
/// repair against a single reordered prepare.
///
/// Public because it bounds what that operator knob can do: gap recovery starts
/// after `max(repair_gap_debounce_interval, this)`, which the `[cluster]`
/// documentation states.
pub const REPAIR_GAP_DEBOUNCE_TICKS_MIN: u32 = 50;

/// What a tick driver reads off one consensus group to decide whether it is
/// gap-stopped. Split out so the guards, the debounce and the per-tick cap are
/// testable without a shard, a bus, or a journal.
///
/// Both planes fill it: `partition_gap_probe` off a live partition, and
/// `IggyShard::metadata_gap_probe` off the metadata plane's consensus and WAL.
///
/// The flags are independent readings of one instant, not states of one
/// machine, and the exhaustive predicate test below enumerates them as such, so
/// the lint's two-variant enums would only rename `true` and `false`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy)]
struct GapProbe {
    normal: bool,
    transferring: bool,
    /// Whether a repair session, a transfer, or a scheduled transfer re-arm
    /// already owns this group's recovery. Arming a second one would race it,
    /// or defeat the re-arm's backoff as `arm_partition_transfer` documents.
    /// The re-arm shape is the partition plane's alone; metadata has no
    /// re-arm state, so its probe reads the other two.
    recovery_owned: bool,
    commit_min: u64,
    commit_max: u64,
    /// Whether `commit_min + 1` is resident in the local journal.
    ///
    /// Read only when the guards above already hold, and `false` otherwise:
    /// both predicates test the lag first, so a probe that fails it is
    /// answered without touching the journal at all. See
    /// [`partition_gap_probe`].
    next_op_resident: bool,
    /// Whether this replica adopted suffix headers above `commit_max` whose
    /// bodies never arrived. Its own recovery shape, disjoint from the lag
    /// below the frontier: the group cannot gather quorum for that suffix until
    /// the bodies land, and the only other site that notices is the single
    /// `on_start_view` edge that adopted them. See [`partition_missing_suffix_through`].
    ///
    /// A metadata probe reads only the op a view-log proof waits on above
    /// `commit_max`. The walk refuses the unproven log and a replicated prepare
    /// never re-runs the proof, so once the window armed at `StartView`
    /// adoption closes below that op, a replica with no lag has no other site
    /// to arm its repair.
    missing_suffix: bool,
}

/// Whether this replica holds committed ops it cannot walk to, because the op
/// one past its commit frontier is missing from its journal.
///
/// The journal-hole half is not redundant: a follower advances `commit_max`
/// from every prepare header in `replicate_preflight`, so `commit_min <
/// commit_max` is transiently true on every healthy pipelined tick and a bare
/// lag test would arm repair against ordinary traffic.
const fn group_is_gap_stopped(probe: &GapProbe) -> bool {
    if !probe.normal || probe.transferring || probe.recovery_owned {
        return false;
    }
    // The lag decides first, and a walkable lag wins outright. A replica that
    // is BOTH short of a suffix and behind its own frontier would otherwise arm
    // over `(commit_min, head]` -- refetching a committed prefix it already
    // holds resident -- and would claim this predicate and the walk at once.
    // The walk closes the lag within a tick or two (the suffix cannot commit
    // meanwhile, so `commit_max` stands still), and the suffix arms on the pass
    // after that.
    if probe.commit_min < probe.commit_max {
        return !probe.next_op_resident;
    }
    probe.missing_suffix
}

/// The gap predicate's disjoint sibling, not its complement: everything the
/// walk needs is resident, it just never ran (a heartbeat carrying a known
/// commit is `Accepted`, and an idle group offers no other edge).
///
/// The two split on `next_op_resident` while a lag stands, and
/// [`group_is_gap_stopped`] defers to that split even for a missing suffix,
/// so they cannot both hold. Both are false whenever a shared guard fails. Not
/// gated on `recovery_owned`: repair fetches bodies without walking them, so
/// gating parks the walk all session.
const fn group_is_walk_stalled(probe: &GapProbe) -> bool {
    probe.normal
        && !probe.transferring
        && probe.commit_min < probe.commit_max
        && probe.next_op_resident
}

/// What the debounce says about one group on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GapArm {
    /// Not gap-stopped, or gap-stopped for less than the debounce.
    NotDue,
    /// Due, but this sweep's arm budget is spent. The debounce stays satisfied,
    /// so the group is due again on the next pass rather than serving a fresh
    /// interval. It moves no cursor: the sweep resumes where the WALK budget
    /// ran out, and arms drain their own queue as sessions open.
    ///
    /// Partition-plane only. The metadata driver holds one group per node, so
    /// it always passes a budget and never sees this.
    Deferred,
    /// Open a repair session now.
    Arm,
}

/// Count one tick against `gap_ticks` and answer whether this group may arm
/// repair now.
///
/// Level-triggered, because every edge-triggered arming site is starvable, on
/// both planes: the commit-heartbeat backstop fires only on
/// `CommitOutcome::Advanced`, and under sustained traffic the prepares consume
/// the advance in preflight before the gap check drops them, so the heartbeat
/// lands as `Accepted` and the gap wedges until an unrelated view change.
///
/// `budget_available` is the partition sweep's per-tick arm rate; the
/// live-session ceiling is applied by `maybe_request_partition_repair`, which
/// every partition arming site funnels through. A refused arm keeps its
/// debounce satisfied rather than starting over, so the group arms on the next
/// pass with a slot free.
///
/// Spending the count is the arm function's job, not this one's, and it resets
/// `gap_ticks` for EVERY arming site rather than only the tick: an edge-armed
/// repair that completes before the next tick would otherwise leave the count
/// saturated and hand the next gap an arm on its first tick. Metadata's twin of
/// that reset lives in `maybe_request_metadata_repair`.
const fn drive_group_gap_debounce(
    probe: &GapProbe,
    gap_ticks: &mut u32,
    debounce_ticks: u32,
    budget_available: bool,
) -> GapArm {
    if !group_is_gap_stopped(probe) {
        *gap_ticks = 0;
        return GapArm::NotDue;
    }
    let debounce_ticks = if debounce_ticks < REPAIR_GAP_DEBOUNCE_TICKS_MIN {
        REPAIR_GAP_DEBOUNCE_TICKS_MIN
    } else {
        debounce_ticks
    };
    *gap_ticks = gap_ticks.saturating_add(1);
    if *gap_ticks < debounce_ticks {
        return GapArm::NotDue;
    }
    if budget_available {
        GapArm::Arm
    } else {
        GapArm::Deferred
    }
}

/// The peer a gap-stopped replica asks for repair, or `None` when there is
/// nobody to ask.
///
/// The primary, except when this replica IS the primary: no site re-drives a
/// settled primary's own hole, so leaving it to warn wedges the group, and the
/// next op to commit walks `advance_commit_min` into its sequential assert. Any
/// replica in `Normal` or `ViewChange` serves `RequestPrepares`, and a
/// gap-stopped replica's window is its COMMITTED prefix, which every peer that
/// holds those ops holds identically.
///
/// Positional, not liveness-aware. A dead pick is corrected by the stall
/// budget on either plane, which drops the session and rotates one step further
/// around the ring rather than re-requesting from it forever.
///
/// Shared by both planes so the rule cannot drift: the partition sweep and
/// `tick_metadata` arm off the same predicate and owe the same answer.
const fn gap_repair_peer(self_id: u8, replica_count: u8, primary: u8) -> Option<u8> {
    let peer = if primary == self_id {
        next_transfer_peer(self_id, self_id, replica_count, primary)
    } else {
        primary
    };
    // A solo group (or a ring with nobody else live to name) rotates back to
    // self, which no session can be opened against.
    if peer == self_id { None } else { Some(peer) }
}

/// Whether a standing metadata repair session should be closed at the top of
/// the tick: its window is walked, the view that decided that window has
/// moved, or this replica has left the status the session belongs to.
///
/// `repairing_view` is the primary-elect repairing toward its merged log, the
/// one session that runs outside `Normal`. Pinned by
/// `metadata_repair_session_tests`.
const fn metadata_repair_superseded(
    session: &MetadataRepairSession,
    commit_min: u64,
    view: u32,
    normal: bool,
    repairing_view: bool,
) -> bool {
    commit_min >= session.to_op || session.view != view || !(normal || repairing_view)
}

/// Whether a walked `RepairDone` should pull the next chunk of the window.
///
/// `served_through` is the terminator's own op. Chunk progress, not this
/// walk's: `tick_metadata` walks the same journal, so it can consume a chunk
/// between the chunk's last prepare and its terminator, and requiring
/// `commit_min` to move HERE idles the session a full retry interval on every
/// such landing.
const fn repair_chunk_walked(before: u64, commit_min: u64, served_through: u64) -> bool {
    commit_min > before || commit_min >= served_through
}

/// Rotate a sweep's namespace snapshot so it resumes at `cursor`.
///
/// The per-tick caps are what make this necessary: the snapshot is in ascending
/// namespace order, so a shard whose leading groups stay eligible would spend
/// the whole budget on them every pass and never reach the tail. `cursor` names
/// the first group a cap turned away last pass, so every eligible group is
/// served within `ceil(groups / cap)` sweeps.
///
/// A cursor whose namespace was removed meanwhile resumes at its successor, and
/// one past the last namespace wraps to the front. `None` means the previous
/// sweep turned nobody away.
fn rotate_sweep_to_cursor(namespaces: &mut [IggyNamespace], cursor: Option<IggyNamespace>) {
    let Some(cursor) = cursor else {
        return;
    };
    debug_assert!(
        namespaces.is_sorted(),
        "the sweep snapshot must be in namespace order for the cursor to resume in it",
    );
    // `partition_point` answers in `0..=len`, and `rotate_left(len)` is the
    // no-op that wraps a cursor past the last namespace back to the front.
    namespaces.rotate_left(namespaces.partition_point(|namespace| *namespace < cursor));
}

fn partition_repair_fetch_to_op(
    commit_min: u64,
    commit_max: u64,
    missing_suffix: Option<u64>,
) -> Option<u64> {
    (commit_min < commit_max || missing_suffix.is_some())
        .then(|| missing_suffix.unwrap_or(commit_max))
}

/// Start of the next chunk to pull after a `RepairDone`, or `None` when the
/// walk made no progress (the stall retry owns the remainder) or already
/// stands at the session's fetch ceiling. The sweep closes such a session,
/// because `fetch_to_op` never sits below `commit_to_op`.
///
/// A session can outlive its last fetchable op until that close, and
/// `from_op > to_op` fails `RequestPreparesHeader::validate` on the serving
/// peer, which drops the frame as unparsable.
fn partition_repair_next_chunk(before: u64, commit_min: u64, fetch_to_op: u64) -> Option<u64> {
    (commit_min > before && commit_min < fetch_to_op).then_some(commit_min + 1)
}

/// Highest adopted suffix op whose bodies are not all present above `commit_max`.
///
/// The shape `maybe_request_partition_repair` widens its window for, read here
/// so the sweep's detector and the arm agree by construction. A backup that
/// adopted a `StartView` withholds its ack for those ops until the body is
/// journaled, and the primary's retransmit is dropped by the backup gap check
/// because adoption already advanced the sequencer to the head: nothing else
/// delivers them, and the group wedges one op below its head.
///
/// Ordered cheapest-first, because it runs per group per tick: no suffix at all
/// is one comparison, and a suffix nobody adopted is one `Option` check. Only a
/// group that has both pays the header-vec walk. Later live prepares can raise
/// the sequencer without extending the adopted canonical header list.
fn partition_missing_suffix_through<B, SB>(partition: &IggyPartition<B, SB>) -> Option<u64>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    let consensus = partition.consensus();
    let commit_max = consensus.commit_max();
    let head = consensus.sequencer().current_sequence();
    if head <= commit_max {
        return None;
    }
    let adopted_head = consensus
        .with_pending_view_log(|pending| adopted_suffix_head(pending, commit_max, head))
        .flatten()?;
    (!partition
        .log
        .journal()
        .inner
        .repaired_window_shape(commit_max, adopted_head)
        .complete)
        .then_some(adopted_head)
}

fn adopted_suffix_head(pending: &MergedLog, commit_max: u64, current_head: u64) -> Option<u64> {
    let adopted_head = pending.op_head.min(current_head);
    (adopted_head > commit_max && pending_covers_suffix(pending, commit_max, adopted_head))
        .then_some(adopted_head)
}

/// Read the gap probe off a live partition.
fn partition_gap_probe<B, SB>(partition: &IggyPartition<B, SB>) -> GapProbe
where
    B: MessageBus,
    SB: SuperblockStore,
{
    let consensus = partition.consensus();
    let commit_min = consensus.commit_min();
    let commit_max = consensus.commit_max();
    let recovery_owned = partition.transfer.is_some()
        || partition.transfer_rearm.is_some()
        || partition.repair.is_some();
    let normal = consensus.is_normal();
    let transferring = consensus.is_transferring();
    // Residency last, and only once the guards both predicates share already
    // hold. This runs for every group on the shard on every tick, and the
    // caught-up steady state (`commit_min == commit_max`) would otherwise pay
    // a journal lookup whose answer both predicates discard.
    //
    // A proof waiting on a body makes the resident op unwalkable, as in
    // `metadata_gap_probe`: `commit_journal` refuses an unproven view log.
    let next_op_resident = normal
        && !transferring
        && commit_min < commit_max
        && consensus.view_log_repair_op().is_none()
        && partition
            .log
            .journal()
            .inner
            .holds_op(commit_min.saturating_add(1));
    // Same discipline, one guard deeper: the suffix test walks the header vec,
    // so it runs only for a group that HAS an unfinished suffix and already
    // owes nothing else.
    let missing_suffix = normal
        && !transferring
        && !recovery_owned
        && partition_missing_suffix_through(partition).is_some();
    GapProbe {
        normal,
        transferring,
        recovery_owned,
        commit_min,
        commit_max,
        next_op_resident,
        missing_suffix,
    }
}

/// Whether the parked `StartView` log names every op in the uncommitted
/// suffix `(commit_max, head]`, in descending order. Only this canonical list
/// makes fetching bodies above the commit point safe.
fn pending_covers_suffix(pending: &MergedLog, commit_max: u64, head: u64) -> bool {
    if head <= commit_max || pending.commit_max > commit_max || pending.op_head != head {
        return false;
    }
    // Live commits can advance inside an adopted suffix. Its remaining
    // canonical headers still authorize repair above the new commit point.
    let mut expected = head;
    for header in pending
        .headers
        .iter()
        .filter(|header| header.op > commit_max)
    {
        if header.op != expected {
            return false;
        }
        expected -= 1;
    }
    expected == commit_max
}

/// Read this replica's uncommitted suffix out of the metadata journal, for the
/// window `commit..=op`.
///
/// The nack bit is load-bearing, and is set only where absence *proves* this
/// replica never prepared the op:
/// * Above the commit point, a missing header is proof: the WAL refuses to boot
///   on interior corruption, so a hole in a journal that opened never arrived.
/// * At or below it, a checkpoint may have compacted the header away. Those slots
///   go out blank and un-nacked, read as "no information" rather than licence to
///   truncate an op this replica considers committed.
///
/// Deriving the suffix on demand is also why it needs no durable record: the
/// merged log is in memory and bodies are fetched whole, so the WAL is the only
/// thing that ever backs a nack and recomputing after a restart gives the same
/// answer. A torn tail is the one exception, and it changes the answer correctly:
/// recovery truncates the incomplete append, which fsyncs before the ack, so no
/// replication quorum could have counted it.
fn build_metadata_dvc_suffix<J>(
    journal: Option<&J>,
    commit: u64,
    op: u64,
    view_headers: Option<&[PrepareHeader]>,
    unproven: std::ops::RangeInclusive<u64>,
) -> DvcSuffix
where
    J: JournalHandle,
    <J as JournalHandle>::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    let Some(journal) = journal else {
        return DvcSuffix::empty();
    };
    let handle = journal.handle();
    build_dvc_suffix(
        commit,
        op,
        |entry_op| {
            usize::try_from(entry_op)
                .ok()
                .and_then(|slot| handle.header(slot))
                .map(|header| *header)
        },
        view_headers,
        unproven,
    )
}

/// Plane-independent core of the suffix read. `header_at` answers "do I hold
/// this op, and what is its header".
fn build_dvc_suffix(
    commit: u64,
    op: u64,
    header_at: impl Fn(u64) -> Option<PrepareHeader>,
    view_headers: Option<&[PrepareHeader]>,
    unproven: std::ops::RangeInclusive<u64>,
) -> DvcSuffix {
    // Stitch the adopted view's headers over the journal, high-to-low.
    //
    // Reading the journal alone is only correct for a replica whose journal IS its
    // log. A backup that adopted a `StartView` is header-poor by design: the suffix
    // went to `pending_view_log` and the bodies are still being repaired, so the
    // journal holds nothing at those ops and would report blanks. The stitched
    // headers name the ops the view decided instead, and at the commit point a
    // blank would leave the op undecidable unless another sender describes it.
    // Above the commit point a stitched header without its body counts as a nack
    // (`tally_op`). A blank with a different resident prepare would abstain instead;
    // the adopted header proves which entry this replica cannot serve.
    //
    // The head rises to the view's head too, so a later view change cannot let the
    // op backtrack below what this replica already acknowledged.
    let view_head = view_headers
        .and_then(<[PrepareHeader]>::first)
        .map_or(0, |header| header.op);
    let op = op.max(view_head);
    if op == 0 {
        return DvcSuffix::empty();
    }
    // Window runs from the commit point up, floored at 1 because ops are 1-based.
    // That floor is a scan bound only: the lines below can raise it above the
    // commit point, so no reader may read it back as one. See `merge_commit_max`.
    let mut low = commit.max(1);
    if low > op {
        return DvcSuffix::empty();
    }
    if op - low + 1 > DVC_HEADERS_MAX as u64 {
        // Defensive: every plane's `prepare_queue_depth` is capped below
        // `DVC_HEADERS_MAX` so `op - commit` cannot reach this. If it does, the
        // clamped-away ops go out described by nobody. That is no abstention:
        // from a canonical sender it lets every lower-`log_view` copy of such an
        // op count as a nack. Keep the highest entries, whose fate the view
        // change decides, and log it rather than shipping a different window.
        let clamped = op - DVC_HEADERS_MAX as u64 + 1;
        tracing::warn!(
            commit,
            op,
            window_from = clamped,
            "uncommitted suffix wider than {DVC_HEADERS_MAX} entries; truncating the DVC window \
             from below. Ops {}..={} are now undecidable and will stall the view change",
            commit + 1,
            clamped - 1
        );
        low = clamped;
    }

    let len = usize::try_from(op - low + 1).unwrap_or(DVC_HEADERS_MAX);
    let mut headers = Vec::with_capacity(len);
    let mut nack_bitset = 0u128;
    let mut present_bitset = 0u128;
    for (index, entry_op) in (low..=op).rev().enumerate() {
        let resident = header_at(entry_op);
        if let Some(header) = resident.filter(|_| !unproven.contains(&entry_op)) {
            headers.push(header);
            // A header in the index means the entry is in the WAL at a known
            // offset, the same condition `on_request_prepares` serves from.
            present_bitset |= 1u128 << index;
        } else if let Some(header) = view_headers
            .and_then(|headers| view_header_at(headers, entry_op))
            .filter(|view| {
                // Above the commit point the merge reads a header without a body
                // as "never journaled", a nack. Over this replica's own unproven
                // copy of the view's entry that is false: it may have acked the
                // copy before a restart or a state transfer left it unproven. A
                // different prepare held here is no copy of the view's entry.
                entry_op <= commit
                    || !resident.is_some_and(|held| header_is_view_entry(&held, view))
            })
        {
            // No present bit: this replica cannot serve the body yet.
            headers.push(*header);
        } else {
            headers.push(dvc_blank(entry_op));
            if entry_op > commit && resident.is_none() {
                nack_bitset |= 1u128 << index;
            } else if entry_op <= commit {
                // The commit point, the one slot that goes out blank AND
                // un-nacked. The merge scans it and may not discard it, so a
                // sender is asking the new primary to take the header from
                // someone else; if every sender in the quorum does that, the
                // op is undecidable and the view never starts.
                //
                // Every compaction path is supposed to leave this header behind
                // (the metadata checkpoint drain stops one op short, a
                // partition serves it from the evicted ring), so reaching here
                // means a replica whose log genuinely starts above its own
                // commit point: a state-transfer receiver that jumped its
                // commit floor to a snapshot whose prepares it never held.
                tracing::warn!(
                    op = entry_op,
                    commit,
                    "no header at this replica's commit point; the DVC reports it blank and \
                     cannot nack it, so the view change stalls unless a peer supplies it"
                );
            }
        }
    }
    DvcSuffix::new(headers, nack_bitset, present_bitset)
}

/// An applied op whose history the view contradicts, or `None` when the view
/// extends what this replica applied.
///
/// Truncation stops above the applied floor, so a contradiction at or below it
/// is not repairable in place: the WAL refuses the view's next prepare because it
/// names a parent this replica never wrote, and that refusal used to stop the
/// node. Checked two ways, because the journal evicts applied headers:
/// * a canonical header at or below the floor against the local checksum there,
/// * the canonical header just above the floor, whose `parent` names the
///   checksum the view holds at the floor.
///
/// An unsealed checksum on either side is not evidence, as in
/// [`header_is_view_entry`].
fn applied_history_divergence(
    pending: &MergedLog,
    applied_floor: u64,
    checksum_at: impl Fn(u64) -> Option<u128>,
) -> Option<u64> {
    let contradicts = |op: u64, canonical: u128| {
        checksum_at(op).is_some_and(|local| {
            local != CHECKSUM_UNSEALED && canonical != CHECKSUM_UNSEALED && local != canonical
        })
    };
    pending.headers.iter().find_map(|canonical| {
        if canonical.op <= applied_floor {
            contradicts(canonical.op, canonical.checksum).then_some(canonical.op)
        } else if applied_floor > 0 && applied_floor.checked_add(1) == Some(canonical.op) {
            contradicts(applied_floor, canonical.parent).then_some(applied_floor)
        } else {
            None
        }
    })
}

/// The highest applied op below `diverged_op` whose history the view confirms,
/// or `None` when no header the view carries vouches for one.
///
/// Checksums chain through `parent`, so a match at an op confirms every op
/// below it. The pending headers run newest first and the divergence check
/// reports the newest contradiction, which can sit above older ones this
/// replica also applied, so ops at and above it never count. Checked the same
/// two ways as [`applied_history_divergence`].
fn applied_history_agreement(
    pending: &MergedLog,
    applied_floor: u64,
    diverged_op: u64,
    checksum_at: impl Fn(u64) -> Option<u128>,
) -> Option<u64> {
    let confirms = |op: u64, canonical: u128| {
        op <= applied_floor
            && op < diverged_op
            && canonical != CHECKSUM_UNSEALED
            && checksum_at(op).is_some_and(|local| local == canonical)
    };
    pending
        .headers
        .iter()
        .filter_map(|canonical| {
            let direct = confirms(canonical.op, canonical.checksum).then_some(canonical.op);
            let via_parent = canonical
                .op
                .checked_sub(1)
                .filter(|&parent_op| confirms(parent_op, canonical.parent));
            direct.max(via_parent)
        })
        .max()
}

/// [`applied_history_divergence`] against this partition's journal and WAL,
/// paired with the [`applied_history_agreement`] below it.
fn partition_applied_divergence<B, SB>(
    partition: &IggyPartition<B, SB>,
    pending: &MergedLog,
) -> Option<(u64, Option<u64>)>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    let applied_floor = partition.consensus().commit_min();
    // The repair ring too: a `StartView` the consensus refused on an evicted
    // header must still find the op it contradicted here.
    let checksum_at = |op| {
        partition
            .applied_header(op)
            .map(|header| header.checksum)
            .or_else(|| partition.wal_checksum(op))
    };
    let diverged_op = applied_history_divergence(pending, applied_floor, checksum_at)?;
    let agreed_op = applied_history_agreement(pending, applied_floor, diverged_op, checksum_at);
    Some((diverged_op, agreed_op))
}

/// Where [`reconcile_metadata_view_log`] stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataViewLogStep {
    /// Proven, fenced on a missing op, or stopped by a failed rewrite.
    Done,
    /// The next rewrite removes the entry the durable certificate is bound to.
    /// Persist the lowered binding, then reconcile again.
    PersistFirst,
}

#[allow(clippy::future_not_send)]
async fn reconcile_metadata_view_log<B, J>(
    shard: u16,
    consensus: &VsrConsensus<B>,
    journal: &J,
) -> MetadataViewLogStep
where
    B: MessageBus,
    J: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    if !consensus.view_log_unproven() {
        return MetadataViewLogStep::Done;
    }
    let pending = consensus.pending_view_log();
    let op_head = pending.as_ref().map_or_else(
        || consensus.sequencer().current_sequence(),
        |pending| pending.op_head,
    );
    let above_head = op_head.max(consensus.commit_min()).saturating_add(1);
    if consensus.view_log_repair_op().is_none()
        && journal.last_op().is_some_and(|last| last >= above_head)
    {
        if lower_metadata_certificate_below(consensus, journal, above_head) {
            return MetadataViewLogStep::PersistFirst;
        }
        if let Err(error) = journal.truncate_from(above_head).await {
            tracing::error!(shard, above_head, %error, "could not truncate superseded metadata suffix");
            return MetadataViewLogStep::Done;
        }
        consensus.note_journal_mutation();
    }
    loop {
        match consensus::view_log_proof(
            consensus.commit_min(),
            pending.as_ref().map_or(&[][..], |pending| &pending.headers),
            op_head,
            |op| {
                usize::try_from(op)
                    .ok()
                    .and_then(|slot| journal.header(slot).map(|header| *header))
            },
        ) {
            consensus::ViewLogProof::Proven => {
                consensus.settle_view_log_proof();
                // A primary-elect still holds its old log_view here; it certifies
                // when the view starts.
                if consensus.is_normal() {
                    let (op, checksum) = metadata_wal_head(journal, u64::MAX);
                    consensus.certify_view_log(op, checksum);
                }
                return MetadataViewLogStep::Done;
            }
            consensus::ViewLogProof::Missing {
                op,
                unproven_through,
            } => {
                consensus.fence_view_log(op, unproven_through);
                return MetadataViewLogStep::Done;
            }
            consensus::ViewLogProof::Divergent { first, last } => {
                consensus.fence_view_log(last, last);
                if lower_metadata_certificate_below(consensus, journal, first) {
                    return MetadataViewLogStep::PersistFirst;
                }
                // The canonical tail may be this replica's only copy. Remove
                // the proven run without cutting entries above a gap.
                if let Err(error) = journal.remove_range(first..=last).await {
                    tracing::error!(shard, first, last, %error, "could not remove divergent metadata prepares");
                    return MetadataViewLogStep::Done;
                }
                consensus.note_journal_mutation();
            }
        }
    }
}

/// Move the history certificate off the entries a rewrite removes from `from` up.
/// `true` when the lowered binding is not durable yet and must be before removal:
/// a restart would otherwise find the binding gone and certify nothing.
fn lower_metadata_certificate_below<B, J>(
    consensus: &VsrConsensus<B>,
    journal: &J,
    from: u64,
) -> bool
where
    B: MessageBus,
    J: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
{
    let (op, checksum) = metadata_wal_head(journal, from);
    consensus.lower_history_certificate(from, op, checksum);
    consensus.history_certificate_needs_persist()
}

/// Only the suffix above the adopted head is safe to truncate as a whole.
fn partition_view_divergence_from<B, SB>(
    partition: &IggyPartition<B, SB>,
    pending: Option<&MergedLog>,
) -> Option<u64>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    if !partition.consensus().view_log_unproven()
        || partition.consensus().view_log_repair_op().is_some()
    {
        return None;
    }
    let applied_floor = partition
        .consensus()
        .commit_min()
        .max(partition.log.journal().inner.evicted_through());
    let op_head = pending.map_or_else(
        || partition.consensus().sequencer().current_sequence(),
        |pending| pending.op_head,
    );
    let above_head = op_head.max(applied_floor).checked_add(1)?;
    partition
        .log
        .journal()
        .inner
        .last_op()
        .filter(|last| *last >= above_head)
        .map(|_| above_head)
}

#[allow(clippy::future_not_send)]
async fn reconcile_partition_view_divergence<B, SB>(
    shard: u16,
    partition: &mut IggyPartition<B, SB>,
    pending: Option<&MergedLog>,
) where
    B: MessageBus,
    SB: journal::superblock::SuperblockStore,
{
    if !partition.consensus().view_log_unproven() || partition.view_truncation_pending() {
        return;
    }
    if let Some(from_op) = partition_view_divergence_from(partition, pending)
        && let Err(error) = partition.truncate_uncommitted_from(from_op).await
    {
        tracing::error!(shard, from_op, %error, "could not truncate superseded partition suffix");
        return;
    }
    let op_head = pending.map_or_else(
        || partition.consensus().sequencer().current_sequence(),
        |pending| pending.op_head,
    );
    loop {
        let floor = partition
            .consensus()
            .commit_min()
            .max(partition.log.journal().inner.evicted_through());
        match consensus::view_log_proof(
            floor,
            pending.map_or(&[][..], |pending| &pending.headers),
            op_head,
            |op| partition.log.journal().inner.header_by_op(op),
        ) {
            consensus::ViewLogProof::Proven => {
                let consensus = partition.consensus();
                consensus.settle_view_log_proof();
                if consensus.view_log_merged() && !partition.wal_keeps_log_view() {
                    consensus.note_uncertified_merge();
                }
                // An adoption moves the sequencer, not the chain anchor. If the
                // journal already held the adopted head, no repair frame moves the
                // anchor, which then names a dropped op the WAL cannot certify.
                // A durable commit point survives only in the repair ring.
                if let Some(head) = partition
                    .log
                    .journal()
                    .inner
                    .repair_header(consensus.sequencer().current_sequence())
                {
                    consensus.set_last_prepare_checksum(head.checksum);
                }
                return;
            }
            consensus::ViewLogProof::Missing {
                op,
                unproven_through,
            } => {
                partition.consensus().fence_view_log(op, unproven_through);
                return;
            }
            consensus::ViewLogProof::Divergent { first, last } => {
                partition.consensus().fence_view_log(last, last);
                if partition.has_io_dispatcher() {
                    let consensus = partition.consensus();
                    let peer = (!consensus.is_primary())
                        .then(|| consensus.primary_index(consensus.view()));
                    partition.defer_view_truncation(first..=last, Vec::new(), peer);
                    return;
                }
                if let Err(error) = partition.remove_divergent_prepares(first..=last).await {
                    tracing::error!(shard, first, last, %error, "could not remove divergent partition prepares");
                    return;
                }
            }
        }
    }
}

/// Whether a locally journaled header IS the entry the view's log names at that op.
///
/// Identity, not presence: otherwise a stale prepare at the right op reads as
/// coverage everywhere: the repair ingest skips it as already held,
/// `RebuildPipeline` seeds the pipeline from it and self-acks, `CommitJournal`
/// applies it. `identity_checksum` excludes `view`, so a restamp still compares equal.
///
/// An unsealed checksum on either side is not evidence (a pre-seal WAL entry),
/// so it counts as agreement, as in `dvc_suffix_decode`.
const fn header_is_view_entry(local: &PrepareHeader, canonical: &PrepareHeader) -> bool {
    local.checksum == CHECKSUM_UNSEALED
        || canonical.checksum == CHECKSUM_UNSEALED
        || local.checksum == canonical.checksum
}

/// The lowest op in the merged log this replica cannot serve, or `None` when the
/// view can start.
///
/// Coverage is identity, not presence (see [`header_is_view_entry`]): starting a view
/// over a differing entry commits this replica's own operation where the view says
/// another belongs.
///
/// Covers every op the merged log names, including headers inherited from senders
/// behind the canonical `log_view`, which sit below the canonical window where header
/// repair cannot walk back to them. `repair_floor` drops the ops whose journal entry
/// is legitimately gone AND whose identity is already settled: on the metadata plane
/// ops compacted under a snapshot, on the partition plane ops at or below the local
/// commit point. Neither can diverge from the merged log (a committed or compacted
/// op is the quorum's op), and no repair puts the journal entry back, so demanding
/// one parks the view change forever.
///
/// Not a residency bound. `evict_prefix` clears the header vec up to `commit_max`
/// (the cluster frontier), so ops above `repair_floor` can be non-resident and
/// still serveable, from the evicted ring or the flushed segments. Callers pass a
/// `header_at` that reads both.
///
/// Opens at [`merged_log_scan_floor`]: the merged commit point alone would declare
/// the log serveable over a local gap, promoting a replica whose `CommitJournal`
/// gap-stops below where `RebuildPipeline` seeds.
///
/// Below the merged commit point, identity rests on the fault model rather than on
/// this scan. The merged log names headers only from the DVC suffixes, which span
/// `commit..=op` per sender, so an op the widened floor admits under
/// `pending.commit_max` usually has no canonical header and `held` degrades to
/// bare residency. Sound under crash-stop, where a committed op is the quorum's
/// op. Not a Byzantine or bit-rot guard: corruption is `verify_prepare_integrity`'s
/// job on the ingest side.
fn first_op_not_covered(
    pending: &MergedLog,
    floor: ScanFloor,
    header_at: impl Fn(u64) -> Option<PrepareHeader>,
) -> Option<u64> {
    // Indexed once: a lagging replica probes up to a journal of ops, and walking
    // every merged header per probe cost window x headers. The sort is stable, so
    // the dedup keeps the header a front-to-back search would find first.
    let mut canonical: Vec<(u64, &PrepareHeader)> = pending
        .headers
        .iter()
        .chain(pending.committed_elsewhere.iter())
        .map(|header| (header.op, header))
        .collect();
    canonical.sort_by_key(|(op, _)| *op);
    canonical.dedup_by_key(|(op, _)| *op);
    let held = |op: u64| {
        let Some(local) = header_at(op) else {
            return false;
        };
        canonical
            .binary_search_by_key(&op, |(op, _)| *op)
            .ok()
            .and_then(|index| canonical.get(index))
            .is_none_or(|(_, view_entry)| header_is_view_entry(&local, view_entry))
    };
    (floor.opens_at(pending)..=pending.op_head)
        .find(|op| !held(*op))
        .or_else(|| {
            // NOT raised to `opens_at`: these ops sit outside the merged window
            // by construction, and dropping the ones below it would start the
            // view over a committed op this replica cannot serve. The repair
            // window is floored to match instead, via
            // `MetadataRepairSession::from_op`.
            pending
                .committed_elsewhere
                .iter()
                .map(|header| header.op)
                .filter(|op| *op > floor.repair_floor)
                .find(|op| !held(*op))
        })
}

/// The adopted view's header at `op`, or `None` when the view says nothing about
/// it.
///
/// Headers run high-to-low from the view's head, so the slot is arithmetic. The
/// op is re-checked rather than assumed: a mismatch means the range is not the
/// contiguous run this indexing needs, and inventing a header for the wrong op
/// is worse than reporting none.
fn view_header_at(view_headers: &[PrepareHeader], op: u64) -> Option<&PrepareHeader> {
    let head = view_headers.first()?.op;
    let index = usize::try_from(head.checked_sub(op)?).ok()?;
    let header = view_headers.get(index)?;
    if header.op != op || matches!(dvc_header_kind(header), DvcHeaderKind::Blank) {
        return None;
    }
    Some(header)
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
    <J as JournalHandle>::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
            consensus.group(),
            consensus.view(),
        );
    }

    for action in actions {
        match action {
            VsrAction::SendStartViewChange { view, group } => {
                let msg = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                    .transmute_header(|_, h: &mut StartViewChangeHeader| {
                        h.command = Command::StartViewChange;
                        h.partition_incarnation = consensus.partition_incarnation();
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.group = *group;
                        h.size = size_of::<StartViewChangeHeader>() as u32;
                        h.seal();
                    });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendDoViewChange {
                view,
                target,
                log_view,
                op,
                commit,
                group,
                suffix,
                untrusted_log,
            } => {
                let header_size = size_of::<DoViewChangeHeader>();
                let total_size = header_size + suffix.encoded_len();
                let mut msg = Message::<DoViewChangeHeader>::new(total_size);
                // Body first: `transmute_header` zeroes only the header region, so
                // anything past it survives. Same order as the manifest build.
                suffix.encode_into(&mut msg.as_mut_slice()[header_size..total_size]);
                let body_checksum = control_body_checksum(&msg.as_slice()[header_size..total_size]);
                let nack_bitset = suffix.nack_bitset();
                let present_bitset = suffix.present_bitset();
                let msg = msg.transmute_header(|_, h: &mut DoViewChangeHeader| {
                    h.command = Command::DoViewChange;
                    h.partition_incarnation = consensus.partition_incarnation();
                    h.cluster = cluster;
                    h.replica = self_id;
                    h.view = *view;
                    h.log_view = *log_view;
                    h.op = *op;
                    h.commit = *commit;
                    h.group = *group;
                    if *untrusted_log {
                        h.flags |= DoViewChangeHeader::FLAG_UNTRUSTED_LOG;
                    }
                    h.nack_bitset = nack_bitset;
                    h.present_bitset = present_bitset;
                    h.checksum_body = body_checksum;
                    h.size = total_size as u32;
                    // Last: covers the bitsets a new primary truncates on.
                    h.seal();
                });
                // Broadcast, not unicast to `target`: a backup seeing a DVC for a
                // newer view adopts it instead of waiting out its heartbeat
                // timeout, which converges the view change in one round.
                let _ = target;
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendRequestStartView { view, group } => {
                // Stamp this replica's incarnation so the answering StartView can
                // echo it, proving to us the reply post-dates our restart.
                let incarnation = consensus.incarnation();
                let msg =
                    Message::<RequestStartViewHeader>::new(size_of::<RequestStartViewHeader>())
                        .transmute_header(|_, h: &mut RequestStartViewHeader| {
                            h.command = Command::RequestStartView;
                            h.partition_incarnation = consensus.partition_incarnation();
                            h.cluster = cluster;
                            h.replica = self_id;
                            h.view = *view;
                            h.incarnation = incarnation;
                            h.group = *group;
                            h.size = size_of::<RequestStartViewHeader>() as u32;
                            h.seal();
                        });
                broadcast(msg.into_generic().into_frozen()).await;
            }
            VsrAction::SendStartView {
                view,
                op,
                commit,
                incarnation,
                target,
                group,
                suffix,
            } => {
                let header_size = size_of::<StartViewHeader>();
                let total_size = header_size + suffix.len() * size_of::<PrepareHeader>();
                let mut msg = Message::<StartViewHeader>::new(total_size);
                // Body first: `transmute_header` zeroes only the header region.
                encode_prepare_headers(suffix, &mut msg.as_mut_slice()[header_size..total_size]);
                let body_checksum = control_body_checksum(&msg.as_slice()[header_size..total_size]);
                let msg = msg.transmute_header(|_, h: &mut StartViewHeader| {
                    h.checksum_body = body_checksum;
                    h.command = Command::StartView;
                    h.partition_incarnation = consensus.partition_incarnation();
                    h.cluster = cluster;
                    h.replica = self_id;
                    h.view = *view;
                    h.op = *op;
                    h.commit = *commit;
                    h.incarnation = *incarnation;
                    h.group = *group;
                    h.size = total_size as u32;
                    h.seal();
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
                group,
            } => {
                if consensus.view_log_unproven() || !consensus.history_certified() {
                    continue;
                }
                let Some(journal) = journal else {
                    continue;
                };
                for op in *from_op..=*to_op {
                    if !consensus.view_log_entry_proven(op) {
                        continue;
                    }
                    let Some(prepare_header) = journal.handle().header(op as usize) else {
                        continue;
                    };
                    let prepare_header = *prepare_header;
                    let msg = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
                        .transmute_header(|_, h: &mut PrepareOkHeader| {
                            h.command = Command::PrepareOk;
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
                            h.group = *group;
                            h.size = size_of::<PrepareOkHeader>() as u32;
                            h.seal();
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
                    let Some(frozen) =
                        restamp_prepare_view(prepare.into_generic().into_frozen(), current_view)
                    else {
                        continue;
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
                rebuild_pipeline_entries(
                    consensus,
                    self_id,
                    *from_op,
                    *to_op,
                    |op| {
                        usize::try_from(op)
                            .ok()
                            .and_then(|slot| journal.handle().header(slot))
                            .map(|header| *header)
                    },
                    |_| true,
                );
            }
            // Handled by the caller (shard view change handlers) since it
            // requires access to the plane's commit_journal method.
            VsrAction::CommitJournal => {}
            VsrAction::SendCommit {
                view,
                commit,
                group,
                timestamp_monotonic,
                target,
            } => {
                let msg = Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
                    |_, h: &mut CommitHeader| {
                        h.command = Command::Commit;
                        h.partition_incarnation = consensus.partition_incarnation();
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.view = *view;
                        h.commit = *commit;
                        h.group = *group;
                        h.timestamp_monotonic = *timestamp_monotonic;
                        h.size = size_of::<CommitHeader>() as u32;
                        h.seal();
                    },
                );
                let frozen = msg.into_generic().into_frozen();
                match target {
                    Some(replica) => send(*replica, frozen).await,
                    None => broadcast(frozen).await,
                }
            }
        }
    }
}

#[allow(clippy::future_not_send)]
async fn dispatch_partition_wire_actions<B, P, J, SB>(
    consensus: &VsrConsensus<B, P>,
    partition: &IggyPartition<B, SB>,
    mut actions: Vec<VsrAction>,
) where
    B: MessageBus,
    P: Pipeline<Entry = consensus::PipelineEntry>,
    J: JournalHandle,
    J::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    SB: SuperblockStore,
{
    if !partition.persist_superblock_if_needed().await {
        partition.defer_wire_actions(actions);
        return;
    }
    if partition.requires_state_transfer() {
        actions.retain(sendable_while_awaiting_transfer);
    }
    dispatch_vsr_actions::<B, P, J>(consensus, None, &actions).await;
    dispatch_partition_journal_actions(consensus, partition, &actions).await;
}

/// Wire actions a partition awaiting a state transfer may still send: its
/// probe, and the empty vote that vouches for no history but lets a group
/// whose every log is untrusted start a view.
fn sendable_while_awaiting_transfer(action: &VsrAction) -> bool {
    matches!(action, VsrAction::SendRequestStartView { .. }) || action.is_empty_vote()
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
    SB: SuperblockStore,
{
    let bus = consensus.message_bus();
    let self_id = consensus.replica();
    let journal = &partition.log.journal().inner;

    let send = |target: u8, msg: Frozen<MESSAGE_ALIGN>| async move {
        if let Err(e) = bus.send_to_replica(target, msg).await {
            tracing::debug!(replica = self_id, target, "bus send failed: {e}");
        }
    };

    // Same durable-before-send tripwire as `dispatch_vsr_actions`: this
    // dispatcher emits view-scoped `SendPrepareOk` too, and all callers are
    // persist-gated today -- assert it so a future bypass cannot slip
    // through the partition plane's own dispatcher silently.
    #[cfg(debug_assertions)]
    for action in actions {
        debug_assert!(
            !matches!(action, VsrAction::SendPrepareOk { .. })
                || !consensus.needs_superblock_persist(),
            "durable-before-send violated: dispatching a view-scoped action for \
             namespace {} while the superblock is behind the in-memory view {}",
            consensus.group(),
            consensus.view(),
        );
    }

    for action in actions {
        match action {
            VsrAction::SendPrepareOk {
                view,
                from_op,
                to_op,
                ..
            } => {
                if *view != consensus.view() {
                    continue;
                }
                for op in *from_op..=*to_op {
                    partition.acknowledge_prepare(op).await;
                }
            }
            VsrAction::RetransmitPrepares { targets } => {
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
                    let Some(prepare) = restamp_prepare_view(prepare, current_view) else {
                        continue;
                    };
                    for replica in replicas {
                        send(*replica, prepare.clone()).await;
                    }
                }
            }
            VsrAction::RebuildPipeline { from_op, to_op } => {
                rebuild_pipeline_entries(
                    consensus,
                    self_id,
                    *from_op,
                    *to_op,
                    |op| journal.header_by_op(op),
                    |header| partition.register_rebuilt_ack(header),
                );
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod warn_pace_tests {
    use super::*;

    #[test]
    fn given_a_repeating_warning_when_paced_should_print_once_per_window() {
        let mut pace = WarnPace::default();
        assert!(pace.admit(0), "the first event prints");
        assert!(!pace.admit(WARN_PACE_TICKS - 1), "the window holds it back");
        assert!(pace.admit(WARN_PACE_TICKS), "the next window prints again");
        assert!(!pace.admit(WARN_PACE_TICKS), "and opens a new quiet window");
    }
}

#[cfg(test)]
mod persist_gate_tests {
    use super::*;

    fn rebuild() -> VsrAction {
        VsrAction::RebuildPipeline {
            from_op: 3,
            to_op: 9,
        }
    }

    #[test]
    fn given_view_change_actions_when_split_should_keep_locals_out_of_the_gate() {
        // The exact action shape `complete_view_change_as_primary` emits
        // after it already flipped status/log_view and cleared its pipeline.
        // The regression: a failed superblock persist used to drop the whole
        // vec, and losing `RebuildPipeline` leaves a primary that discards
        // every backup PrepareOk for the orphaned window as UnknownPrepare.
        let actions = vec![
            VsrAction::SendStartView {
                view: 4,
                op: 9,
                commit: 3,
                incarnation: 0,
                target: None,
                group: 7,
                suffix: Vec::new(),
            },
            VsrAction::CommitJournal,
            rebuild(),
        ];
        let (local, wire) = split_local_actions(actions);
        assert!(
            local.iter().all(|action| matches!(
                action,
                VsrAction::CommitJournal | VsrAction::RebuildPipeline { .. }
            )),
            "locals must hold exactly the act-side actions"
        );
        assert_eq!(local.len(), 2, "both act-side actions survive the gate");
        assert_eq!(wire.len(), 1, "only the send is fenced by the persist");
        assert!(matches!(wire[0], VsrAction::SendStartView { .. }));
    }

    #[test]
    fn given_send_only_actions_when_split_should_leave_locals_empty() {
        let actions = vec![VsrAction::SendStartViewChange { view: 2, group: 7 }];
        let (local, wire) = split_local_actions(actions);
        assert!(local.is_empty());
        assert_eq!(wire.len(), 1);
    }
}

#[cfg(test)]
mod repair_scope_tests {
    //! Who parked the log decides what it means.

    use consensus::{LocalPipeline, Sequencer, VsrConsensus};
    use iggy_binary_protocol::{Command, PrepareHeader, RequestPreparesHeader};
    use message_bus::IggyMessageBus;
    use server_common::Message;

    use super::{
        MergedLog, adopted_suffix_head, pending_covers_suffix, repair_op_in_scope,
        repair_serve_ceiling,
    };

    fn header(op: u64) -> PrepareHeader {
        PrepareHeader {
            command: Command::Prepare,
            op,
            ..Default::default()
        }
    }

    /// A view that started at op 100 with commit 98.
    fn parked() -> MergedLog {
        MergedLog {
            op_head: 100,
            commit_max: 98,
            headers: (98..=100).rev().map(header).collect(),
            committed_elsewhere: Vec::new(),
        }
    }

    #[test]
    fn given_a_backup_with_a_parked_log_when_repairing_above_the_view_head_should_accept() {
        // A backup keeps its parked `StartView` suffix for the whole view, so at
        // op 200 the parked head is 100 ops stale. Reading it as a repair scope
        // silently discards the served op: the retry loops, the commit walk
        // freezes, checkpointing stops, and the backup stops acking.
        assert!(
            repair_op_in_scope(Some(&parked()), false, 149, 150),
            "a backup repairs for the whole view, not just the view-start range"
        );
    }

    #[test]
    fn given_a_backup_with_a_parked_log_when_repairing_below_commit_min_should_reject() {
        // A backup's parked log grants no licence to re-ingest committed ops.
        assert!(!repair_op_in_scope(Some(&parked()), false, 149, 149));
    }

    #[test]
    fn given_a_primary_elect_when_repairing_toward_its_merged_log_should_use_it_as_the_scope() {
        let pending = parked();
        // Inside the merged range, including inherited headers below `commit_min`.
        assert!(repair_op_in_scope(Some(&pending), true, 99, 98));
        assert!(repair_op_in_scope(Some(&pending), true, 99, 100));
        // Outside it: the primary-elect is not repairing toward these.
        assert!(!repair_op_in_scope(Some(&pending), true, 99, 101));
        assert!(!repair_op_in_scope(Some(&pending), true, 99, 97));
        // With nothing parked, the ordinary commit-point rule applies.
        assert!(!repair_op_in_scope(None, false, 149, 149));
        assert!(repair_op_in_scope(None, false, 149, 150));
    }

    #[test]
    fn given_a_primary_elect_when_an_op_is_committed_elsewhere_should_accept_it() {
        let mut pending = parked();
        pending.committed_elsewhere.push(header(42));
        assert!(repair_op_in_scope(Some(&pending), true, 99, 42));
    }

    #[test]
    fn given_a_repair_request_when_serving_should_clamp_to_the_frontier_but_not_below_it() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, IggyMessageBus::new(0), LocalPipeline::new());
        consensus.init();
        consensus.advance_commit_max(40);
        consensus.sequencer().set_sequence(90);
        let ceiling = |to_op| {
            let request = Message::<RequestPreparesHeader>::new(std::mem::size_of::<
                RequestPreparesHeader,
            >())
            .transmute_header(|_, header: &mut RequestPreparesHeader| {
                header.command = Command::RequestPrepares;
                header.cluster = consensus.cluster();
                header.size = u32::try_from(std::mem::size_of::<RequestPreparesHeader>()).unwrap();
                header.view = consensus.view();
                header.from_op = 1;
                header.to_op = to_op;
            });
            repair_serve_ceiling(request.header(), &consensus)
        };
        // `validate` accepts any `to_op >= from_op` and the serve path walks op by
        // op with no `.await`, so an unclamped ceiling hangs the whole shard.
        assert_eq!(ceiling(u64::MAX), 90);
        assert_eq!(ceiling(50), 50);
        // The suffix a new primary repairs toward sits above every commit point,
        // so clamping to `commit_max` alone deadlocks the view change.
        assert_eq!(ceiling(90), 90);
        // `commit_max` above the local head still counts: heartbeats outrun prepares.
        consensus.advance_commit_max(120);
        assert_eq!(ceiling(u64::MAX), 120);
    }

    #[test]
    fn given_an_adopted_suffix_when_live_head_advances_should_preserve_its_repair_boundary() {
        let pending = parked();
        assert_eq!(adopted_suffix_head(&pending, 98, 100), Some(100));
        assert_eq!(adopted_suffix_head(&pending, 98, 101), Some(100));
        assert_eq!(adopted_suffix_head(&pending, 99, 101), Some(100));
        assert_eq!(adopted_suffix_head(&pending, 100, 101), None);
        // A parked head ABOVE the local head is a different shape -- ops this
        // replica has not sequenced at all -- and stays out of scope.
        assert_eq!(adopted_suffix_head(&pending, 98, 99), None);
        let suffix = adopted_suffix_head(&pending, 98, 101);
        assert_eq!(
            super::partition_repair_fetch_to_op(0, 98, suffix),
            Some(100)
        );
        assert_eq!(
            super::partition_repair_fetch_to_op(98, 98, suffix),
            Some(100)
        );
        assert_eq!(super::partition_repair_fetch_to_op(0, 98, None), Some(98));
        assert_eq!(super::partition_repair_fetch_to_op(98, 98, None), None);
        let mut missing = pending;
        missing.headers.retain(|header| header.op != 99);
        assert_eq!(adopted_suffix_head(&missing, 98, 101), None);
    }

    #[test]
    fn given_a_walk_at_the_fetch_ceiling_when_repair_done_lands_should_not_request_a_chunk() {
        assert_eq!(super::partition_repair_next_chunk(4, 7, 8), Some(8));
        // No progress leaves the remainder to the stall retry.
        assert_eq!(super::partition_repair_next_chunk(7, 7, 8), None);
        // Progress that reached the ceiling has nothing left to ask for: the
        // sweep closes the session, and `9..=8` is not a range.
        assert_eq!(super::partition_repair_next_chunk(7, 8, 8), None);
        assert_eq!(super::partition_repair_next_chunk(7, 9, 8), None);
    }

    #[test]
    fn given_a_parked_view_when_fetching_above_commit_should_require_dense_canonical_suffix() {
        let pending = parked();
        assert!(pending_covers_suffix(&pending, 98, 100));

        let mut missing = pending.clone();
        missing.headers.retain(|header| header.op != 99);
        assert!(!pending_covers_suffix(&missing, 98, 100));

        let mut wrong_frontier = pending;
        wrong_frontier.commit_max = 99;
        assert!(!pending_covers_suffix(&wrong_frontier, 98, 100));
    }
}

#[cfg(test)]
mod view_coverage_tests {
    //! Holding an op is not holding the view's op.

    use super::{
        MergedLog, ScanFloor, applied_history_agreement, applied_history_divergence,
        first_op_not_covered,
    };
    use iggy_binary_protocol::{Command, Operation, PrepareHeader};

    /// What a caught-up replica passes: nothing compacted, nothing lagging.
    fn caught_up(pending: &MergedLog) -> ScanFloor {
        ScanFloor {
            repair_floor: 0,
            commit_min: pending.commit_max,
        }
    }

    fn sealed(op: u64, request: u64) -> PrepareHeader {
        let mut header = PrepareHeader {
            command: Command::Prepare,
            operation: Operation::CreateStream,
            op,
            request,
            ..Default::default()
        };
        header.checksum = header.identity_checksum();
        header
    }

    /// A view whose log runs `from..=to` over `request`, hash-chained.
    fn view_log(from: u64, to: u64, request: u64) -> MergedLog {
        let mut parent = sealed(from - 1, request).checksum;
        let mut headers = Vec::new();
        for op in from..=to {
            let mut header = sealed(op, request);
            header.parent = parent;
            header.checksum = header.identity_checksum();
            parent = header.checksum;
            headers.push(header);
        }
        headers.reverse();
        MergedLog {
            op_head: to,
            commit_max: from,
            headers,
            committed_elsewhere: Vec::new(),
        }
    }

    #[test]
    fn given_view_contradicting_an_applied_op_when_checked_should_report_it() {
        // Applied through 5 from one history; the view's op 5 is another. Only a
        // state transfer can replace an applied op. The view stops at 5, so no
        // header above the floor can give it away through its parent.
        let pending = view_log(4, 5, 1);
        let applied = sealed(5, 9).checksum;
        assert_eq!(
            applied_history_divergence(&pending, 5, |op| (op == 5).then_some(applied)),
            Some(5)
        );
    }

    #[test]
    fn given_evicted_applied_head_when_view_names_another_parent_should_report_it() {
        // The journal evicted everything applied, so the view's first op above the
        // floor is the only witness: its parent is not the checksum this replica
        // wrote at op 4, so the WAL would refuse it.
        let pending = view_log(5, 7, 1);
        let applied_four = sealed(4, 9).checksum;
        assert_eq!(
            applied_history_divergence(&pending, 4, |op| (op == 4).then_some(applied_four)),
            Some(4)
        );
    }

    #[test]
    fn given_applied_floor_zero_when_view_names_another_parent_should_report_nothing() {
        // Nothing is applied, so op 0 is no history this replica could lose.
        let pending = view_log(1, 3, 1);
        let local_zero = sealed(0, 9).checksum;
        assert_eq!(
            applied_history_divergence(&pending, 0, |op| (op == 0).then_some(local_zero)),
            None
        );
    }

    #[test]
    fn given_divergent_applied_suffix_when_measuring_agreement_should_stop_below_it() {
        // Ops 1..=3 match the view, ops 4 and 5 were minted by this replica alone.
        // The divergence check reports op 5 first, so anything at or above op 4
        // counted as agreed would rebuild with a frontier the group never reached.
        let view = view_log(1, 5, 1);
        let local_suffix = view_log(4, 5, 9);
        let local: Vec<(u64, u128)> = view
            .headers
            .iter()
            .filter(|header| header.op <= 3)
            .chain(&local_suffix.headers)
            .map(|header| (header.op, header.checksum))
            .collect();
        let checksum_at = |op: u64| {
            local
                .iter()
                .find(|(local_op, _)| *local_op == op)
                .map(|(_, checksum)| *checksum)
        };
        let window = |ops: &[u64]| MergedLog {
            headers: view
                .headers
                .iter()
                .filter(|header| ops.contains(&header.op))
                .copied()
                .collect(),
            ..view.clone()
        };

        let with_agreed_op = window(&[5, 4, 3]);
        assert_eq!(
            applied_history_divergence(&with_agreed_op, 5, checksum_at),
            Some(5)
        );
        assert_eq!(
            applied_history_agreement(&with_agreed_op, 5, 5, checksum_at),
            Some(3)
        );

        let via_parent = window(&[5, 4]);
        assert_eq!(
            applied_history_agreement(&via_parent, 5, 5, checksum_at),
            Some(3)
        );

        let no_witness = window(&[5]);
        assert_eq!(
            applied_history_agreement(&no_witness, 5, 5, checksum_at),
            None
        );
    }

    #[test]
    fn given_view_extending_applied_history_when_checked_should_report_nothing() {
        let pending = view_log(4, 7, 1);
        let applied: Vec<_> = pending
            .headers
            .iter()
            .map(|header| (header.op, header.checksum))
            .collect();
        let checksum_at = |op: u64| {
            applied
                .iter()
                .find(|(applied_op, _)| *applied_op == op)
                .map(|(_, checksum)| *checksum)
        };
        assert_eq!(applied_history_divergence(&pending, 4, checksum_at), None);
        assert_eq!(applied_history_divergence(&pending, 5, checksum_at), None);
        assert_eq!(
            applied_history_divergence(&pending, 4, |_| None),
            None,
            "an unknown local checksum is not evidence"
        );
        assert_eq!(
            applied_history_divergence(&pending, 4, |_| Some(super::CHECKSUM_UNSEALED)),
            None,
            "an unsealed local checksum is not evidence"
        );
        let mut unsealed = pending;
        for header in &mut unsealed.headers {
            header.checksum = super::CHECKSUM_UNSEALED;
        }
        assert_eq!(
            applied_history_divergence(&unsealed, 5, checksum_at),
            None,
            "an unsealed canonical checksum is not evidence"
        );
    }

    #[test]
    fn given_a_diverging_entry_when_scanning_should_report_it_like_a_hole() {
        // Op 99 is present and is not the view's op 99. Reading presence as coverage
        // starts the view over an operation the view says is something else, which
        // `CommitJournal` then applies at or below the commit point unchecked.
        let pending = MergedLog {
            op_head: 100,
            commit_max: 98,
            headers: (98..=100).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: Vec::new(),
        };
        let held = [sealed(100, 1), sealed(99, 7), sealed(98, 1)];
        let missing = first_op_not_covered(&pending, caught_up(&pending), |op| {
            held.iter().find(|header| header.op == op).copied()
        });
        assert_eq!(missing, Some(99));
    }

    #[test]
    fn given_an_evicted_committed_window_when_floored_should_start_the_view() {
        // The wedge behind the partition_state_transfer regressions: a survivor
        // that flushes on every commit holds NO resident journal header (the
        // flush evicts them), so a merged window opening on its own committed op
        // reads as a hole nothing can fill -- no repair re-journals a committed
        // op. The floor (local commit point) must count it as covered, or the
        // primary-elect parks in `ViewChange` forever and the rotation hands
        // primaryship to an empty rejoiner that then cannot be served the state
        // transfer it needs.
        let pending = MergedLog {
            op_head: 256,
            commit_max: 256,
            headers: vec![sealed(256, 1)],
            committed_elsewhere: Vec::new(),
        };
        let nothing_resident = |_: u64| None;
        assert_eq!(
            first_op_not_covered(&pending, caught_up(&pending), nothing_resident),
            Some(256),
            "unfloored, the evicted committed op reads as an unfillable hole"
        );
        assert_eq!(
            first_op_not_covered(
                &pending,
                ScanFloor {
                    repair_floor: 256,
                    commit_min: 256,
                },
                nothing_resident
            ),
            None,
            "floored at the local commit point, the view starts"
        );
    }

    #[test]
    fn given_a_hole_below_the_merged_commit_point_when_scanning_should_report_it() {
        // Missed op 7 and kept taking prepares above it: the cluster committed
        // through 10 while this state machine stopped at 6. From the merged commit
        // point the view would start over the gap and the first quorum ack would
        // apply an op with 7..=10 never executed locally.
        let pending = MergedLog {
            op_head: 12,
            commit_max: 10,
            headers: (7..=12).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: Vec::new(),
        };
        let held: Vec<_> = (8..=12).map(|op| sealed(op, 1)).collect();
        let missing = first_op_not_covered(
            &pending,
            ScanFloor {
                repair_floor: 0,
                commit_min: 6,
            },
            |op| held.iter().find(|header| header.op == op).copied(),
        );
        assert_eq!(
            missing,
            Some(7),
            "a hole below the merged commit point must park the view change"
        );
    }

    #[test]
    fn given_a_contiguous_prefix_when_scanning_should_open_at_the_merged_commit_point() {
        // Nothing missing below, so both bounds coincide. Op 9 is held but is not
        // the view's op 9, so the commit point itself is still identity-checked.
        let pending = MergedLog {
            op_head: 12,
            commit_max: 9,
            headers: (9..=12).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: Vec::new(),
        };
        let held: Vec<_> = (9..=12)
            .map(|op| sealed(op, if op == 9 { 7 } else { 1 }))
            .collect();
        let missing = first_op_not_covered(&pending, caught_up(&pending), |op| {
            held.iter().find(|header| header.op == op).copied()
        });
        assert_eq!(
            missing,
            Some(9),
            "the merged commit point stays in scope when the prefix is contiguous"
        );
    }

    #[test]
    fn given_a_held_run_below_the_hole_when_scanning_should_walk_to_the_hole() {
        // The span the widened floor buys, and the one that costs: the open sits
        // well below the merged commit point, the ops between are all held, and the
        // scan must walk them to reach 15. A scan that stopped on its first probe
        // would find op 4 covered and never look further.
        let pending = MergedLog {
            op_head: 22,
            commit_max: 20,
            headers: (4..=22).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: Vec::new(),
        };
        let held: Vec<_> = (4..=22)
            .filter(|op| *op != 15)
            .map(|op| sealed(op, 1))
            .collect();
        let missing = first_op_not_covered(
            &pending,
            ScanFloor {
                repair_floor: 0,
                commit_min: 3,
            },
            |op| held.iter().find(|header| header.op == op).copied(),
        );
        assert_eq!(
            missing,
            Some(15),
            "the scan must walk the held run below the merged commit point, not stop at its \
             first covered probe"
        );
    }

    #[test]
    fn given_a_committed_elsewhere_op_below_the_open_when_scanning_should_still_report_it() {
        // `committed_elsewhere` sits outside the merged window, so the fallback is
        // floored at `repair_floor` and not at the scan's open. Which is why the
        // repair window floors at the op reported here: a retry reopening at
        // `opens_at` would skip op 5 forever.
        let pending = MergedLog {
            op_head: 12,
            commit_max: 10,
            headers: (7..=12).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: vec![sealed(5, 1)],
        };
        let held: Vec<_> = (7..=12).map(|op| sealed(op, 1)).collect();
        let floor = ScanFloor {
            repair_floor: 0,
            commit_min: 6,
        };
        assert_eq!(floor.opens_at(&pending), 7);
        let missing = first_op_not_covered(&pending, floor, |op| {
            held.iter().find(|header| header.op == op).copied()
        });
        assert_eq!(
            missing,
            Some(5),
            "an op committed elsewhere and below the open is still uncovered"
        );
    }

    #[test]
    fn given_a_diverging_entry_committed_elsewhere_when_scanning_should_report_it() {
        // Identity binds below the canonical window too: a held op 8 that is not the
        // op 8 the other senders committed must park the view like a hole.
        let pending = MergedLog {
            op_head: 12,
            commit_max: 10,
            headers: (10..=12).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: (7..=9).rev().map(|op| sealed(op, 1)).collect(),
        };
        let held: Vec<_> = (7..=12)
            .map(|op| sealed(op, if op == 8 { 7 } else { 1 }))
            .collect();
        let missing = first_op_not_covered(
            &pending,
            ScanFloor {
                repair_floor: 0,
                commit_min: 6,
            },
            |op| held.iter().find(|header| header.op == op).copied(),
        );
        assert_eq!(
            missing,
            Some(8),
            "a held op that differs from the one committed elsewhere is not covered"
        );
    }

    #[test]
    fn given_a_hole_when_scoping_repair_should_admit_the_missing_op() {
        // Coverage and scope must agree: the scan parks on op 7, so op 7's repaired
        // prepare must be ingested. From the merged commit point it would be
        // requested and then refused.
        let pending = MergedLog {
            op_head: 12,
            commit_max: 10,
            headers: (7..=12).rev().map(|op| sealed(op, 1)).collect(),
            committed_elsewhere: Vec::new(),
        };
        assert!(
            super::repair_op_in_scope(Some(&pending), true, 6, 7),
            "the op the coverage scan parked on must be in repair scope"
        );
    }

    #[test]
    fn given_a_source_list_when_rotating_should_walk_it_as_a_ring() {
        use super::next_view_repair_peer;

        let sources = [1u8, 2, 3];
        assert_eq!(next_view_repair_peer(&sources, None), Some(1));
        assert_eq!(next_view_repair_peer(&sources, Some(1)), Some(2));
        assert_eq!(
            next_view_repair_peer(&sources, Some(2)),
            Some(3),
            "a filter answers 1 here and never reaches the third sender"
        );
        assert_eq!(
            next_view_repair_peer(&sources, Some(3)),
            Some(1),
            "the walk wraps"
        );
    }

    #[test]
    fn given_a_sole_or_absent_source_when_rotating_should_report_nobody_left() {
        use super::next_view_repair_peer;

        assert_eq!(next_view_repair_peer(&[], None), None);
        assert_eq!(next_view_repair_peer(&[], Some(1)), None);
        assert_eq!(
            next_view_repair_peer(&[1], Some(1)),
            None,
            "the only sender is the one that went quiet"
        );
        assert_eq!(
            next_view_repair_peer(&[2, 3], Some(9)),
            Some(2),
            "a peer no longer in the list means nothing here has been tried yet"
        );
    }
}

#[cfg(test)]
mod dvc_suffix_window_tests {
    //! The suffix window's floor is a scan bound, not a commit point.
    //!
    //! Reading the lowest suffix op back as a proven commit point assumes suffix
    //! generation stops at the sender's commit. These pin the two paths that break
    //! that premise, so it cannot be quietly reintroduced.

    use super::{DVC_HEADERS_MAX, build_dvc_suffix};
    use iggy_binary_protocol::{Command, Operation, PrepareHeader};

    /// A real prepare at `op`. The operation must not be `Reserved`: that is
    /// exactly `dvc_blank`, and `dvc_header_kind` classifies by equality with it.
    fn held(op: u64) -> PrepareHeader {
        PrepareHeader {
            command: Command::Prepare,
            operation: Operation::CreateStream,
            op,
            ..Default::default()
        }
    }

    /// The lowest op the built window describes.
    fn floor(suffix: &consensus::DvcSuffix) -> Option<u64> {
        suffix.headers().last().map(|header| header.op)
    }

    /// A view's headers for `low..=high`, high-to-low as the suffix carries them.
    fn view_headers(low: u64, high: u64) -> Vec<PrepareHeader> {
        (low..=high).rev().map(held).collect()
    }

    #[test]
    fn given_an_adopted_view_when_the_journal_is_empty_should_report_its_headers_unnacked() {
        // A backup that adopted a `StartView` put the suffix in `pending_view_log`
        // and is still repairing bodies, so its journal holds nothing at those ops.
        // It reports the view's headers there instead of blanks. The merge still
        // counts each one above the commit point as a nack, since no body backs it.
        let view = view_headers(3, 5);
        let suffix = build_dvc_suffix(2, 0, |_| None, Some(&view), 0..=0);

        assert_eq!(
            suffix.len(),
            4,
            "the window rises to the view's head even with an empty journal"
        );
        assert_eq!(
            floor(&suffix),
            Some(2),
            "the floor is still the commit point"
        );
        assert_eq!(
            suffix.nack_bitset(),
            0,
            "a header held from the adopted view carries no nack bit"
        );
        assert_eq!(
            suffix.present_bitset(),
            0,
            "and its body is not servable, so no present bit either"
        );
    }

    #[test]
    fn given_no_adopted_view_when_the_journal_is_empty_should_nack() {
        // The contrast: without an adopted view the same holes really are proof.
        let suffix = build_dvc_suffix(2, 5, |_| None, None, 0..=0);
        assert_eq!(
            suffix.nack_bitset(),
            0b0111,
            "ops 5, 4 and 3 nack; op 2 is the commit point"
        );
    }

    #[test]
    fn given_an_unproven_prefix_when_building_dvc_should_neither_advertise_nor_nack_held_entries() {
        let view = view_headers(5, 6);
        let suffix = build_dvc_suffix(2, 6, |op| Some(held(op)), Some(&view), 3..=4);
        assert_eq!(
            suffix.present_bitset(),
            0b10011,
            "only ops 2, 5 and 6 are proven"
        );
        assert_eq!(
            suffix.nack_bitset(),
            0,
            "unknown held entries cannot be nacked"
        );
        assert_eq!(suffix.headers()[2], consensus::dvc_blank(4));
        assert_eq!(suffix.headers()[3], consensus::dvc_blank(3));
    }

    #[test]
    fn given_an_unproven_copy_of_the_view_entry_should_not_report_it_header_only() {
        // Above the commit point the merge reads a header with no body as a nack,
        // and a replica that holds the view's entry may have acked it.
        let sealed = |op: u64, checksum: u128| PrepareHeader {
            checksum,
            ..held(op)
        };
        let view: Vec<_> = (3..=5).rev().map(|op| sealed(op, op.into())).collect();
        let different = u128::MAX;
        let journal = |op: u64| Some(sealed(op, if op == 4 { different } else { op.into() }));
        let suffix = build_dvc_suffix(2, 5, journal, Some(&view), 3..=u64::MAX);

        assert_eq!(suffix.nack_bitset(), 0);
        assert_eq!(
            suffix.present_bitset(),
            0b1000,
            "only the commit point is proven"
        );
        assert_eq!(suffix.headers()[0], consensus::dvc_blank(5));
        assert_eq!(suffix.headers()[2], consensus::dvc_blank(3));
        assert_eq!(
            suffix.headers()[1],
            view[1],
            "a different prepare at op 4 is no copy of the view's, so the view's header \
             still goes out"
        );
    }

    #[test]
    fn given_an_adopted_view_when_the_journal_covers_part_should_prefer_the_journal() {
        // Journal first, so an op whose body this replica can serve keeps its
        // present bit; the view fills only what the journal is missing.
        let view = view_headers(3, 5);
        let suffix = build_dvc_suffix(2, 5, |op| (op == 5).then(|| held(op)), Some(&view), 0..=0);

        assert_eq!(suffix.len(), 4);
        assert_eq!(suffix.present_bitset(), 0b0001, "only op 5 is servable");
        assert_eq!(suffix.nack_bitset(), 0, "the view covers ops 4 and 3");

        // The head is the max of the two, never the view's alone.
        let short_view = view_headers(3, 4);
        let deeper = build_dvc_suffix(2, 6, |op| Some(held(op)), Some(&short_view), 0..=0);
        assert_eq!(deeper.headers().first().map(|header| header.op), Some(6));
        assert_eq!(deeper.present_bitset(), 0b1_1111, "ops 6 down to 2");
    }

    #[test]
    fn given_a_blank_view_entry_should_not_report_it_as_held() {
        // A blank is the view saying "no header here", not one this replica holds.
        let mut view = view_headers(3, 5);
        view[1] = consensus::dvc_blank(4);
        let suffix = build_dvc_suffix(2, 0, |_| None, Some(&view), 0..=0);

        assert_eq!(suffix.nack_bitset(), 0b010, "only the blank op nacks");
    }

    #[test]
    fn given_no_header_at_the_commit_point_should_report_it_blank_and_undecidable() {
        // The window's floor is the commit point, and a blank there is the one
        // entry that goes out with neither a header nor a nack. The merge scans
        // that op and may not discard it, so a quorum of these deadlocks the view
        // change. Pinned here because both compaction paths are meant to keep the
        // header alive precisely so this shape never leaves a healthy replica.
        let suffix = build_dvc_suffix(5, 5, |_| None, None, 0..=0);

        assert_eq!(suffix.len(), 1);
        assert_eq!(floor(&suffix), Some(5));
        assert_eq!(
            suffix.nack_bitset(),
            0,
            "the commit point is never nacked, whatever the journal says"
        );
        assert_eq!(suffix.present_bitset(), 0);
    }

    #[test]
    fn given_a_window_at_the_depth_ceiling_when_building_should_floor_at_the_commit() {
        // At the deepest legal prepare-queue depth the window still starts exactly
        // at the commit point, so nothing is clamped and no op goes undescribed.
        // Config ceilings and `LocalPipeline::with_capacities` enforce the depth.
        let depth = DVC_HEADERS_MAX as u64 - 1;
        let commit = 500;
        let op = commit + depth;
        let suffix = build_dvc_suffix(commit, op, |op| Some(held(op)), None, 0..=0);

        assert_eq!(suffix.len(), DVC_HEADERS_MAX, "the widest window that fits");
        assert_eq!(
            floor(&suffix),
            Some(commit),
            "at the ceiling the floor is still the commit point"
        );
    }

    #[test]
    fn given_a_window_past_the_depth_ceiling_when_building_should_clamp_above_the_commit() {
        // One op deeper and the window clamps: the floor sits 501 ops above the
        // sender's commit, with no marker on the frame saying so.
        let commit = 500;
        let op = commit + DVC_HEADERS_MAX as u64;
        let suffix = build_dvc_suffix(commit, op, |op| Some(held(op)), None, 0..=0);

        assert_eq!(suffix.len(), DVC_HEADERS_MAX);
        assert_eq!(
            floor(&suffix),
            Some(op - DVC_HEADERS_MAX as u64 + 1),
            "the clamped floor sits above the commit point"
        );
        assert!(floor(&suffix) > Some(commit));

        // Second path, at any depth: ops are 1-based, so commit 0 floors at op 1.
        let from_zero = build_dvc_suffix(0, 3, |op| Some(held(op)), None, 0..=0);
        assert_eq!(floor(&from_zero), Some(1));
    }

    #[test]
    fn given_a_compacted_log_when_building_should_still_describe_the_commit_point() {
        // The commit point goes out blank AND un-nacked, so the merge can neither
        // adopt nor discard it: a quorum that all compacted to the same op deadlocks
        // and no further message fixes it. Both planes must keep that header
        // reachable (metadata's drain stops one op short, a partition serves it from
        // the evicted ring); nothing in `build_dvc_suffix` enforces it.
        let commit = 500;
        let compacted = |op: u64| (op >= commit).then(|| held(op));
        let suffix = build_dvc_suffix(commit, commit + 3, compacted, None, 0..=0);

        let commit_index = suffix.index_of(commit + 3, commit).expect("in window");
        assert!(
            suffix.valid_header_at(commit_index).is_some(),
            "a blank at the commit point is undecidable for the merge"
        );
        assert!(
            suffix.offers_body(commit_index),
            "the commit point must be servable, or the merge stalls waiting for a peer"
        );
        assert!(
            !suffix.nacks(commit_index),
            "the commit point can never be nacked"
        );
    }
}

#[cfg(test)]
mod adopted_view_retention_tests {
    //! The view 12 stall of workload-fuzz seed 890 at 45a4ea297. Replica 0
    //! adopted the view-11 `StartView` (head 26, commit 26), repaired ops 24 and
    //! 25, never received op 26, and was primary-elect of view 12. The view change
    //! dropped the parked headers, so its `DoViewChange` reported op 26, its own
    //! commit point, as a blank, and a merge with no other report of op 26 could
    //! neither keep nor discard it.
    //!
    //! The retained headers must not stand in for a prepare the replica holds:
    //! above its commit point the merge reads a header without a body as a nack.

    use super::{adopted_view_headers, build_dvc_suffix, refresh_metadata_dvc_suffix};
    use consensus::{
        CLIENTS_TABLE_MAX, ClientTable, Consensus, JoinMode, LocalPipeline, PlaneKind, Project,
        Sequencer, StateTransferStage, ViewLogProof, ViewRestore, VsrAction, VsrConsensus,
        encode_prepare_headers, send_prepare_ok, view_log_proof,
    };
    use iggy_binary_protocol::{
        Command, DoViewChangeHeader, Operation, PrepareHeader, PrepareOkHeader,
        RoutedRequestHeader, StartViewChangeHeader, StartViewHeader, WireEncode, WireName,
        WireOptions,
    };
    use iggy_common::variadic;
    use journal::prepare_journal::PrepareJournal;
    use journal::{Journal, JournalHandle};
    use message_bus::IggyMessageBus;
    use metadata::impls::metadata::IggySnapshot;
    use metadata::stm::snapshot::{FillSnapshot, MetadataSnapshot};
    use metadata::stm::stream::Streams;
    use metadata::stm::user::Users;
    use metadata::{IggyMetadata, MuxStateMachine};
    use server_common::Message;
    use server_common::sharding::METADATA_GROUP;

    const CLUSTER: u128 = 1;
    const REPLICA_COUNT: u8 = 3;
    const BACKUP: u8 = 0;
    const OLD_VIEW: u32 = 10;
    /// Led by replica 2.
    const ADOPTED_VIEW: u32 = 11;
    /// Led by `BACKUP`.
    const NEXT_VIEW: u32 = 12;
    const HEAD: u64 = 26;
    /// The backup's journal before the adoption.
    const JOURNAL_HEAD: u64 = 23;
    /// What repair brought before the view change.
    const REPAIRED_HEAD: u64 = 25;

    /// Hash-chained prepare headers for ops `1..=HEAD`, indexed by op, with the
    /// rising timestamps a suffix of more than one entry needs to decode.
    fn history() -> Vec<PrepareHeader> {
        let mut headers = vec![PrepareHeader::default()];
        for op in 1..=HEAD {
            let mut header = PrepareHeader {
                command: Command::Prepare,
                cluster: CLUSTER,
                group: METADATA_GROUP,
                operation: Operation::CreateStream,
                size: u32::try_from(size_of::<PrepareHeader>()).unwrap(),
                op,
                commit: op - 1,
                timestamp: op,
                parent: headers[usize::try_from(op - 1).unwrap()].checksum,
                view: OLD_VIEW,
                ..PrepareHeader::default()
            };
            header.checksum = header.identity_checksum();
            headers.push(header);
        }
        headers
    }

    fn at(history: &[PrepareHeader], op: u64) -> PrepareHeader {
        history[usize::try_from(op).unwrap()]
    }

    /// Keep the adopted identity at this backup's commit point. A proven peer
    /// can offer its journaled copy, but an unproven peer holding the same entry
    /// may abstain above its own commit point, so this report remains necessary.
    #[test]
    fn given_an_unrepaired_start_view_head_when_a_view_change_follows_should_keep_reporting_its_header()
     {
        let history = history();
        let mut backup = VsrConsensus::new(
            1,
            BACKUP,
            REPLICA_COUNT,
            METADATA_GROUP,
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        backup.restore_view(ViewRestore {
            durable_view: Some((OLD_VIEW, OLD_VIEW)),
            view_fallback: None,
            seed_view: None,
        });
        backup.init_as_backup();
        backup.sequencer().set_sequence(JOURNAL_HEAD);
        backup.restore_commit_state(JOURNAL_HEAD - 1, JOURNAL_HEAD - 1);

        let start_view = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
            .transmute_header(|_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = 1;
                header.view = ADOPTED_VIEW;
                header.op = HEAD;
                header.commit = HEAD;
                header.replica = backup.primary_index(ADOPTED_VIEW);
                header.group = METADATA_GROUP;
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            });
        let head = at(&history, HEAD);
        let suffix_body = Message::<PrepareHeader>::new(size_of::<PrepareHeader>())
            .transmute_header(|_, header: &mut PrepareHeader| *header = head);
        backup.handle_start_view(
            PlaneKind::Metadata,
            start_view.header(),
            suffix_body.as_slice(),
        );
        backup.fence_view_log(HEAD, 0);

        let start_view_change =
            Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                .transmute_header(|_, header: &mut StartViewChangeHeader| {
                    header.command = Command::StartViewChange;
                    header.cluster = 1;
                    header.view = NEXT_VIEW;
                    header.replica = backup.primary_index(ADOPTED_VIEW);
                    header.group = METADATA_GROUP;
                    header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                });
        backup.handle_start_view_change(PlaneKind::Metadata, start_view_change.header());
        assert_eq!(backup.view(), NEXT_VIEW);
        assert!(backup.is_primary_for_view(NEXT_VIEW));

        // The snapshot the shard takes for this replica's `DoViewChange`.
        let op = backup.sequencer().current_sequence();
        let adopted = adopted_view_headers(&backup);
        let suffix = build_dvc_suffix(
            backup.commit_max().min(op),
            op,
            |entry_op| (entry_op <= REPAIRED_HEAD).then(|| at(&history, entry_op)),
            adopted.as_ref().map(|log| log.headers.as_slice()),
            backup.view_log_unproven_range(),
        );
        let index = suffix
            .index_of(op, HEAD)
            .expect("the window covers the commit point");
        assert_eq!(
            suffix.valid_header_at(index),
            Some(&head),
            "the view change dropped the adopted header of op {HEAD}, so the primary-elect \
             reports its own commit point blank"
        );
        assert!(!suffix.nacks(index));
    }

    /// Replica 1 led `OLD_VIEW` and acked `HEAD`, and so did replica 2, so `HEAD`
    /// committed. Replica 0 never received it. Replica 2 led `ADOPTED_VIEW`, whose
    /// merge kept `HEAD`, and is lost before committing it again. Replica 1 restarted
    /// meanwhile: it adopts that view while awaiting a state transfer, which skips
    /// the reconcile that proves its journal, and votes with `HEAD` unproven.
    #[test]
    fn given_a_transferring_replica_that_acked_an_op_when_a_view_change_follows_should_not_nack_it()
    {
        const RESTARTED: u8 = 1;
        /// Leads `NEXT_VIEW`.
        const LAGGING: u8 = 0;
        let history = history();
        let head = at(&history, HEAD);
        let lagging_head = at(&history, HEAD - 1);
        let journal = |op: u64| (op <= HEAD).then(|| at(&history, op));
        let lagging_journal = |op: u64| (op <= lagging_head.op).then(|| at(&history, op));

        let before_restart = replica(RESTARTED);
        before_restart.init();
        before_restart.sequencer().set_sequence(HEAD);
        assert!(futures::executor::block_on(send_prepare_ok(
            &before_restart,
            &head,
            true
        )));

        let restarted = replica(RESTARTED);
        restarted.join(JoinMode::ProbeAsBackup {
            await_state_transfer: true,
        });
        restarted.sequencer().set_sequence(HEAD);
        // Recovery takes the commit point from the WAL's embedded watermark.
        restarted.restore_commit_state(head.commit, head.commit);

        let lagging = replica(LAGGING);
        lagging.init();
        lagging.sequencer().set_sequence(lagging_head.op);
        lagging.restore_commit_state(lagging_head.commit, lagging_head.commit);

        // The `ADOPTED_VIEW` primary never learned that `HEAD` committed.
        let announce = start_view(
            restarted.primary_index(ADOPTED_VIEW),
            head.commit,
            &[head, lagging_head],
        );
        let body = &announce.as_slice()[size_of::<StartViewHeader>()..];
        restarted.handle_start_view(PlaneKind::Metadata, announce.header(), body);
        lagging.handle_start_view(PlaneKind::Metadata, announce.header(), body);
        assert_eq!(
            restarted.state_transfer_stage(),
            StateTransferStage::AwaitingTarget
        );
        assert!(restarted.view_log_unproven());
        // The reconcile the shard runs on adoption when not transferring.
        let pending = lagging
            .pending_view_log()
            .expect("the StartView carried headers");
        let ViewLogProof::Missing {
            op,
            unproven_through,
        } = view_log_proof(
            lagging.commit_min(),
            &pending.headers,
            pending.op_head,
            lagging_journal,
        )
        else {
            panic!("the lagging replica misses op {HEAD} only");
        };
        lagging.fence_view_log(op, unproven_through);

        refresh_suffix(&restarted, journal);
        refresh_suffix(&lagging, lagging_journal);
        let vote = restarted
            .handle_start_view_change(PlaneKind::Metadata, start_view_change(LAGGING).header())
            .into_iter()
            .find(|action| matches!(action, VsrAction::SendDoViewChange { .. }))
            .expect("the restarted replica votes");
        lagging
            .handle_start_view_change(PlaneKind::Metadata, start_view_change(RESTARTED).header());
        let message = do_view_change(RESTARTED, &vote);
        lagging.handle_do_view_change(
            PlaneKind::Metadata,
            message.header(),
            &message.as_slice()[size_of::<DoViewChangeHeader>()..],
        );

        assert_eq!(
            lagging.pending_view_body_sources(lagging_head.op),
            [RESTARTED],
            "the restarted replica's vote is in the quorum"
        );
        let merged_head = lagging.pending_view_log().map(|log| log.op_head);
        assert!(
            merged_head.is_none_or(|op_head| op_head >= HEAD),
            "a nack quorum truncated committed op {HEAD}: merged head {merged_head:?}"
        );
        let VsrAction::SendDoViewChange { op, suffix, .. } = &vote else {
            unreachable!("matched above");
        };
        let index = suffix.index_of(*op, HEAD).expect("the window covers HEAD");
        assert!(!suffix.nacks(index));
        assert!(
            suffix.valid_header_at(index).is_none() || suffix.offers_body(index),
            "op {HEAD} goes out header-only above the commit point, which the merge counts \
             as a nack"
        );
    }

    /// Replica 1 led `OLD_VIEW` and pipelined ops 21..=26 under commit 20, the WAL
    /// watermark it reboots with. Replica 2 acked all six. Replica 1 committed
    /// 21..=23 and announced commit 23, then committed 24..=26 with replica 2's acks
    /// and crashed before announcing them. Replica 0 never received 21..=26.
    ///
    /// Replica 2 led `ADOPTED_VIEW` knowing commit 23. Replica 1 restarted and
    /// adopted that view's `StartView` while awaiting a state transfer. Replica 2
    /// then checkpointed at 23 while appending its first new prepare, so the
    /// transfer replica 1 installs drops its own copies of 24..=26. Replica 2 is
    /// lost before replica 0 repairs them.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_transfer_below_acked_ops_when_a_view_change_follows_should_not_nack_them() {
        const RESTARTED: u8 = 1;
        /// Leads `NEXT_VIEW`.
        const LAGGING: u8 = 0;
        /// The commit stamped on replica 1's pipelined prepares.
        const WATERMARK: u64 = 20;
        /// The commit replica 1 announced, and replica 2's checkpoint.
        const CHECKPOINT: u64 = 23;
        type Metadata = IggyMetadata<
            VsrConsensus,
            PrepareJournal,
            IggySnapshot,
            MuxStateMachine<variadic!(Users, Streams)>,
        >;

        let mut history = vec![PrepareHeader::default()];
        for op in 1..=HEAD {
            let mut header = PrepareHeader {
                command: Command::Prepare,
                cluster: CLUSTER,
                group: METADATA_GROUP,
                operation: Operation::CreateStream,
                size: u32::try_from(size_of::<PrepareHeader>()).unwrap(),
                op,
                commit: if op <= WATERMARK { op - 1 } else { WATERMARK },
                timestamp: op,
                parent: history[usize::try_from(op - 1).unwrap()].checksum,
                view: OLD_VIEW,
                ..PrepareHeader::default()
            };
            header.checksum = header.identity_checksum();
            history.push(header);
        }
        let lagging_journal = |op: u64| (op <= WATERMARK).then(|| at(&history, op));
        let announced: Vec<PrepareHeader> = (CHECKPOINT..=HEAD)
            .rev()
            .map(|op| at(&history, op))
            .collect();

        let before_restart = replica(RESTARTED);
        before_restart.init();
        before_restart.sequencer().set_sequence(HEAD);
        for op in CHECKPOINT + 1..=HEAD {
            assert!(futures::executor::block_on(send_prepare_ok(
                &before_restart,
                &at(&history, op),
                true
            )));
        }

        // Only the WAL and the install run on the runtime, each future boxed: held
        // inline, they overflow the test thread's stack in a debug build.
        let runtime = server_common::executor::create_shard_executor().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let wal_path = directory.path().join("journal.wal");
        let journal = runtime
            .block_on(Box::pin(PrepareJournal::open(&wal_path, 0)))
            .unwrap();
        let restarted = replica(RESTARTED);
        restarted.join(JoinMode::ProbeAsBackup {
            await_state_transfer: true,
        });
        restarted.sequencer().set_sequence(HEAD);
        // Recovery takes the commit point from the WAL's embedded watermark.
        restarted.restore_commit_state(WATERMARK, WATERMARK);
        let metadata: Metadata = IggyMetadata::new(
            Some(restarted),
            Some(journal),
            None,
            None,
            MuxStateMachine::default(),
            None,
        );
        let restarted = metadata.consensus.as_ref().unwrap();
        let wal = metadata.journal.as_ref().unwrap().handle();
        runtime.block_on(Box::pin(async {
            for op in 1..=HEAD {
                let header = at(&history, op);
                wal.append(
                    Message::<PrepareHeader>::new(size_of::<PrepareHeader>())
                        .transmute_header(|_, slot: &mut PrepareHeader| *slot = header),
                )
                .await
                .unwrap();
            }
        }));

        let lagging = replica(LAGGING);
        lagging.init();
        lagging.sequencer().set_sequence(WATERMARK);
        lagging.restore_commit_state(WATERMARK, WATERMARK);

        let announce = start_view(
            restarted.primary_index(ADOPTED_VIEW),
            CHECKPOINT,
            &announced,
        );
        let body = &announce.as_slice()[size_of::<StartViewHeader>()..];
        restarted.handle_start_view(PlaneKind::Metadata, announce.header(), body);
        lagging.handle_start_view(PlaneKind::Metadata, announce.header(), body);
        assert_eq!(
            restarted.state_transfer_stage(),
            StateTransferStage::AwaitingTarget
        );

        let mut checkpoint = MetadataSnapshot::new(CHECKPOINT);
        MuxStateMachine::<variadic!(Users, Streams)>::default()
            .fill_snapshot(&mut checkpoint)
            .unwrap();
        let table = ClientTable::new(CLIENTS_TABLE_MAX);
        checkpoint.client_table = Some(table.to_snapshot());
        let encoded = checkpoint.encode().unwrap();
        restarted.set_state_transfer_stage(StateTransferStage::Fetching);
        restarted.set_state_transfer_stage(StateTransferStage::Installing);
        runtime
            .block_on(Box::pin(
                metadata.install_state_transfer(&encoded, table, CHECKPOINT, CHECKPOINT),
            ))
            .unwrap();
        restarted.set_state_transfer_stage(StateTransferStage::Idle);

        reconcile(restarted, |op| {
            usize::try_from(op)
                .ok()
                .and_then(|slot| wal.header(slot).map(|header| *header))
        });
        reconcile(&lagging, lagging_journal);

        refresh_metadata_dvc_suffix(restarted, metadata.journal.as_ref());
        refresh_suffix(&lagging, lagging_journal);
        let vote = restarted
            .handle_start_view_change(PlaneKind::Metadata, start_view_change(LAGGING).header())
            .into_iter()
            .find(|action| matches!(action, VsrAction::SendDoViewChange { .. }))
            .expect("the restarted replica votes");
        lagging
            .handle_start_view_change(PlaneKind::Metadata, start_view_change(RESTARTED).header());
        let message = do_view_change(RESTARTED, &vote);
        lagging.handle_do_view_change(
            PlaneKind::Metadata,
            message.header(),
            &message.as_slice()[size_of::<DoViewChangeHeader>()..],
        );

        assert_eq!(
            lagging.pending_view_body_sources(CHECKPOINT),
            [RESTARTED],
            "the restarted replica's vote is in the quorum"
        );
        let merged_head = lagging.pending_view_log().map(|log| log.op_head);
        assert!(
            merged_head.is_none_or(|op_head| op_head >= HEAD),
            "a nack quorum truncated ops {}..={HEAD}, committed in view {OLD_VIEW}: merged \
             head {merged_head:?}",
            CHECKPOINT + 1
        );
        let VsrAction::SendDoViewChange { op, suffix, .. } = &vote else {
            unreachable!("matched above");
        };
        for acked in CHECKPOINT + 1..=HEAD {
            let index = suffix
                .index_of(*op, acked)
                .expect("the window covers the op");
            assert!(
                !suffix.nacks(index)
                    && (suffix.valid_header_at(index).is_none() || suffix.offers_body(index)),
                "the restarted replica nacks op {acked}, which it acked in view {OLD_VIEW}"
            );
        }
    }

    /// Both survivors retain the same certified view, but the lagging survivor
    /// also carries an uncommitted header adopted from the permanently lost peer.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_stitched_unproven_headers_when_their_primary_is_lost_should_elect_with_two_survivors()
    {
        const COMMON: u64 = HEAD - 2;
        const COMMITTED: u64 = HEAD - 1;
        const LATER_VIEW: u32 = ADOPTED_VIEW + REPLICA_COUNT as u32;
        for target_view in [
            LATER_VIEW + 1,
            LATER_VIEW + 2,
            LATER_VIEW + 4,
            LATER_VIEW + 5,
        ] {
            for restart in [false, true] {
                let lagging = replica(BACKUP);
                let old_primary = replica(1);
                let next_primary = replica(2);
                let mut original = history();
                original.truncate(usize::try_from(COMMON + 1).unwrap());
                for replica in [&lagging, &old_primary, &next_primary] {
                    replica.init();
                    replica.sequencer().set_sequence(COMMON);
                    replica.restore_commit_state(COMMON, COMMON);
                    replica.set_last_prepare_checksum(at(&original, COMMON).checksum);
                    replica.observe_prepare_timestamp(at(&original, COMMON).timestamp);
                }
                let lagging_journal = original.clone();
                let committed = projected_stream(&old_primary, "committed");
                old_primary.pipeline_message(PlaneKind::Metadata, &committed);
                original.push(*committed.header());
                next_primary.sequencer().set_sequence(COMMITTED);
                next_primary.set_last_prepare_checksum(committed.header().checksum);
                next_primary.extend_history_certificate(COMMITTED, committed.header().checksum);
                for replica in [&old_primary, &next_primary] {
                    assert!(replica.is_normal());
                    assert!(replica.history_certified());
                    assert!(!replica.view_log_unproven());
                    assert!(!replica.is_transferring());
                    assert_eq!(replica.sequencer().current_sequence(), COMMITTED);
                    let ack: Message<PrepareOkHeader> = committed.clone().project(replica);
                    assert!(matches!(
                        old_primary.handle_prepare_ok(PlaneKind::Metadata, ack.header()),
                        consensus::PrepareOkOutcome::Accepted { quorum_reached, .. }
                            if quorum_reached == (replica.replica() == next_primary.replica())
                    ));
                }
                old_primary.advance_commit_max(COMMITTED);
                assert_eq!(
                    old_primary.pop_committed_prepare().unwrap().header.op,
                    COMMITTED
                );
                old_primary.advance_commit_min(COMMITTED);
                let heartbeat = Message::<iggy_binary_protocol::CommitHeader>::new(size_of::<
                    iggy_binary_protocol::CommitHeader,
                >(
                ))
                .transmute_header(
                    |_, header: &mut iggy_binary_protocol::CommitHeader| {
                        header.command = Command::Commit;
                        header.size =
                            u32::try_from(size_of::<iggy_binary_protocol::CommitHeader>()).unwrap();
                        header.view = OLD_VIEW;
                        header.replica = old_primary.replica();
                        header.group = METADATA_GROUP;
                        header.commit = COMMITTED;
                    },
                );
                next_primary.handle_commit(heartbeat.header());
                next_primary.advance_commit_min(COMMITTED);
                let mut replacement = original.clone();
                let abandoned = projected_stream(&old_primary, "abandoned");
                old_primary.pipeline_message(PlaneKind::Metadata, &abandoned);
                original.push(*abandoned.header());
                old_primary.extend_history_certificate(HEAD, abandoned.header().checksum);

                // Replica 1 is isolated. The two remaining replicas decide view 11
                // through timer-produced SVCs and the DVCs their handlers emit.
                elect_from_handlers(
                    &next_primary,
                    &lagging,
                    &replacement,
                    &lagging_journal,
                    ADOPTED_VIEW,
                );
                announce_from_handler(&next_primary, &lagging, &replacement, &lagging_journal);
                assert_eq!(next_primary.log_view_claim(), ADOPTED_VIEW);
                assert_eq!(lagging.log_view_claim(), ADOPTED_VIEW);
                assert!(lagging.view_log_unproven());
                let uncommitted = projected_stream(&next_primary, "replacement");
                next_primary.pipeline_message(PlaneKind::Metadata, &uncommitted);
                replacement.push(*uncommitted.header());
                next_primary.extend_history_certificate(HEAD, uncommitted.header().checksum);
                assert_ne!(abandoned.header().checksum, uncommitted.header().checksum);

                // Drop all repair traffic to replica 0 and the intermediate elections.
                // Its timer eventually nominates replica 2 again. One body plus one nack
                // retains the uncommitted replacement, which that primary announces.
                elect_from_handlers(
                    &next_primary,
                    &lagging,
                    &replacement,
                    &lagging_journal,
                    LATER_VIEW,
                );
                assert_eq!(next_primary.pending_view_log().unwrap().op_head, HEAD);
                announce_from_handler(&next_primary, &lagging, &replacement, &lagging_journal);
                assert_eq!(lagging.log_view_claim(), LATER_VIEW);
                assert_eq!(lagging.commit_min(), COMMON);
                assert_eq!(
                    lagging.pending_view_log().unwrap().headers[0].checksum,
                    uncommitted.header().checksum
                );

                let lagging = if restart {
                    let record = lagging.vsr_state(COMMON, 0);
                    let record =
                        consensus::VsrState::try_from(record.to_bytes().as_slice()).unwrap();
                    let mut recovered = replica(BACKUP);
                    recovered.set_view(record.view);
                    recovered.sequencer().set_sequence(COMMON);
                    recovered.restore_commit_state(COMMON, COMMON);
                    recovered.set_last_prepare_checksum(at(&lagging_journal, COMMON).checksum);
                    let history = consensus::HistoryRecovery::new(Some(&record), false, |op| {
                        lagging_journal
                            .get(usize::try_from(op).unwrap())
                            .map(|header| header.checksum)
                    });
                    recovered.restore_history(
                        history,
                        (COMMON, at(&lagging_journal, COMMON).checksum),
                        Some(&record),
                    );
                    recovered.restore_adopted_view(record.adopted_view.as_ref());
                    recovered.resume_adopted_view();
                    recovered.init_as_backup();
                    recovered.begin_view_probe();
                    assert_eq!(recovered.adopted_view_head(), Some((LATER_VIEW, HEAD)));
                    assert!(!recovered.history_certified());
                    recovered
                } else {
                    lagging
                };

                // Replica 2 is now permanently lost and replica 1 reconnects. Both
                // survivors retain their own bodies while the adopted identity decides the tail.
                {
                    let view = target_view;
                    let (candidate, peer, candidate_journal, peer_journal) =
                        if lagging.primary_index(view) == lagging.replica() {
                            (&lagging, &old_primary, &lagging_journal, &original)
                        } else {
                            (&old_primary, &lagging, &original, &lagging_journal)
                        };
                    elect_from_handlers(candidate, peer, candidate_journal, peer_journal, view);
                    assert_eq!(lagging.log_view_claim(), LATER_VIEW);
                    assert_eq!(old_primary.log_view_claim(), OLD_VIEW);
                    assert!(
                        candidate.pending_view_log().is_some(),
                        "view {view}: two live survivors cannot decide op {HEAD}; the missing third replica's adopted header conflicts with the other survivor's uncommitted header"
                    );
                    let merged = candidate.pending_view_log().unwrap();
                    assert_eq!(merged.op_head, COMMITTED);
                    assert_eq!(merged.headers[0].checksum, committed.header().checksum);
                    if candidate.replica() == old_primary.replica() {
                        announce_from_handler(candidate, peer, candidate_journal, peer_journal);
                        assert!(candidate.is_normal());
                        assert!(candidate.history_certified());
                        assert_eq!(candidate.log_view_claim(), view);
                        assert_eq!(candidate.sequencer().current_sequence(), COMMITTED);
                        assert_eq!(peer.adopted_view_head(), Some((view, COMMITTED)));
                        let next = projected_stream(candidate, "after-recovery");
                        assert_eq!(next.header().op, HEAD);
                        assert_eq!(next.header().parent, committed.header().checksum);
                    }
                }
            }
        }
    }

    fn projected_stream(consensus: &VsrConsensus, name: &str) -> Message<PrepareHeader> {
        let body = iggy_binary_protocol::requests::streams::CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut request = Message::<RoutedRequestHeader>::new(total);
        request.as_mut_slice()[header_size..total].copy_from_slice(&body);
        request
            .transmute_header(|_, header: &mut RoutedRequestHeader| {
                header.command = Command::Request;
                header.operation = Operation::CreateStream;
                header.size = u32::try_from(total).unwrap();
                header.client = 1;
                header.session = 1;
                header.request = consensus.sequencer().current_sequence() + 1;
            })
            .project(consensus)
    }

    fn elect_from_handlers(
        candidate: &VsrConsensus,
        peer: &VsrConsensus,
        candidate_journal: &[PrepareHeader],
        peer_journal: &[PrepareHeader],
        view: u32,
    ) {
        const TICK_BUDGET: usize = 100_000;
        let held = |journal: &[PrepareHeader], op: u64| {
            usize::try_from(op)
                .ok()
                .and_then(|slot| journal.get(slot).copied())
        };
        refresh_suffix(peer, |op| held(peer_journal, op));
        refresh_suffix(candidate, |op| held(candidate_journal, op));
        let (initiator, responder) = if candidate.is_primary() {
            (peer, candidate)
        } else {
            (candidate, peer)
        };
        let svc = (0..TICK_BUDGET)
            .find_map(|_| {
                initiator
                    .tick(PlaneKind::Metadata)
                    .into_iter()
                    .find_map(|action| {
                        if let VsrAction::SendStartViewChange {
                            view: announced,
                            group,
                        } = action
                            && announced == view
                        {
                            let frame = Message::<StartViewChangeHeader>::new(size_of::<
                                StartViewChangeHeader,
                            >(
                            ))
                            .transmute_header(
                                |_, header: &mut StartViewChangeHeader| {
                                    header.command = Command::StartViewChange;
                                    header.cluster = CLUSTER;
                                    header.view = view;
                                    header.replica = initiator.replica();
                                    header.group = group;
                                    header.size =
                                        u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                                },
                            );
                            Some(*frame.header())
                        } else {
                            None
                        }
                    })
            })
            .expect("the isolated peer's timer reaches the election view");
        let response = responder.handle_start_view_change(PlaneKind::Metadata, &svc);
        assert!(response.iter().any(|action| matches!(action, VsrAction::SendStartViewChange { view: announced, .. } if *announced == view)));
        let reply = StartViewChangeHeader {
            replica: responder.replica(),
            ..svc
        };
        let initiated = initiator.handle_start_view_change(PlaneKind::Metadata, &reply);
        let actions = if peer.replica() == responder.replica() {
            response
        } else {
            initiated
        };
        let vote = actions
            .into_iter()
            .find(|action| matches!(action, VsrAction::SendDoViewChange { .. }))
            .expect("the peer responds with its real DVC");
        let frame = do_view_change(peer.replica(), &vote);
        candidate.handle_do_view_change(
            PlaneKind::Metadata,
            frame.header(),
            &frame.as_slice()[size_of::<DoViewChangeHeader>()..],
        );
    }

    fn announce_from_handler(
        primary: &VsrConsensus,
        backup: &VsrConsensus,
        primary_journal: &[PrepareHeader],
        backup_journal: &[PrepareHeader],
    ) {
        let held = |journal: &[PrepareHeader], op: u64| {
            usize::try_from(op)
                .ok()
                .and_then(|slot| journal.get(slot).copied())
        };
        reconcile(primary, |op| held(primary_journal, op));
        assert!(
            !primary.view_log_unproven(),
            "only the proven survivor can announce"
        );
        let announced_head = primary.pending_view_log().unwrap().op_head;
        // The plane resets this marker when its proved merge truncates the tail.
        primary.set_last_prepare_checksum(held(primary_journal, announced_head).unwrap().checksum);
        let action = primary
            .start_pending_view(PlaneKind::Metadata)
            .into_iter()
            .find(|action| matches!(action, VsrAction::SendStartView { .. }))
            .expect("the proven primary starts the merged view");
        let VsrAction::SendStartView {
            view,
            op,
            commit,
            suffix,
            ..
        } = action
        else {
            unreachable!()
        };
        let header_size = size_of::<StartViewHeader>();
        let total = header_size + size_of_val(suffix.as_slice());
        let mut frame = Message::<StartViewHeader>::new(total);
        encode_prepare_headers(&suffix, &mut frame.as_mut_slice()[header_size..total]);
        let frame = frame.transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = CLUSTER;
            header.view = view;
            header.op = op;
            header.commit = commit;
            header.replica = primary.replica();
            header.group = METADATA_GROUP;
            header.size = u32::try_from(total).unwrap();
        });
        backup
            .handle_start_view_guarded(
                PlaneKind::Metadata,
                frame.header(),
                &frame.as_slice()[header_size..],
                |op| held(backup_journal, op),
            )
            .expect("the emitted StartView does not contradict applied history");
        reconcile(backup, |op| held(backup_journal, op));
    }

    /// A replica restored at `OLD_VIEW`, as every boot path starts one.
    fn replica(id: u8) -> VsrConsensus {
        let mut consensus = VsrConsensus::new(
            CLUSTER,
            id,
            REPLICA_COUNT,
            METADATA_GROUP,
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.restore_view(ViewRestore {
            durable_view: Some((OLD_VIEW, OLD_VIEW)),
            view_fallback: None,
            seed_view: None,
        });
        consensus.restore_history(
            consensus::HistoryRecovery::Certified {
                view: OLD_VIEW,
                truncate_from: None,
            },
            (0, 0),
            None,
        );
        consensus
    }

    /// The suffix snapshot `refresh_metadata_dvc_suffix` takes before a view change
    /// handler, over a journal answering `held`.
    fn refresh_suffix(consensus: &VsrConsensus, held: impl Fn(u64) -> Option<PrepareHeader>) {
        let op = consensus.sequencer().current_sequence();
        let adopted = adopted_view_headers(consensus);
        consensus.set_local_dvc_suffix(build_dvc_suffix(
            consensus.commit_max().min(op),
            op,
            held,
            adopted.as_ref().map(|log| log.headers.as_slice()),
            consensus.view_log_unproven_range(),
        ));
    }

    /// The proof `reconcile_metadata_view_divergence` takes of an adopted view
    /// once the replica is not transferring, over a journal answering `held`.
    fn reconcile(consensus: &VsrConsensus, held: impl Fn(u64) -> Option<PrepareHeader>) {
        let pending = consensus
            .pending_view_log()
            .expect("the StartView carried headers");
        match view_log_proof(
            consensus.commit_min(),
            &pending.headers,
            pending.op_head,
            held,
        ) {
            ViewLogProof::Proven => consensus.settle_view_log_proof(),
            ViewLogProof::Missing {
                op,
                unproven_through,
            } => consensus.fence_view_log(op, unproven_through),
            ViewLogProof::Divergent { first, last } => {
                panic!("ops {first}..={last} diverge from the adopted view");
            }
        }
    }

    /// The `ADOPTED_VIEW` `StartView` carrying `headers`, high-to-low.
    fn start_view(primary: u8, commit: u64, headers: &[PrepareHeader]) -> Message<StartViewHeader> {
        let header_size = size_of::<StartViewHeader>();
        let total = header_size + size_of_val(headers);
        let mut message = Message::<StartViewHeader>::new(total);
        encode_prepare_headers(headers, &mut message.as_mut_slice()[header_size..total]);
        message.transmute_header(|_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.cluster = CLUSTER;
            header.view = ADOPTED_VIEW;
            header.op = HEAD;
            header.commit = commit;
            header.replica = primary;
            header.group = METADATA_GROUP;
            header.size = u32::try_from(total).unwrap();
        })
    }

    fn start_view_change(replica: u8) -> Message<StartViewChangeHeader> {
        Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>()).transmute_header(
            |_, header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.cluster = CLUSTER;
                header.view = NEXT_VIEW;
                header.replica = replica;
                header.group = METADATA_GROUP;
                header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
            },
        )
    }

    /// The frame the shard sends for `vote`, as `dispatch_vsr_actions` builds it.
    fn do_view_change(replica: u8, vote: &VsrAction) -> Message<DoViewChangeHeader> {
        let VsrAction::SendDoViewChange {
            view,
            log_view,
            op,
            commit,
            group,
            suffix,
            ..
        } = vote
        else {
            panic!("not a DoViewChange: {vote:?}");
        };
        let header_size = size_of::<DoViewChangeHeader>();
        let total = header_size + suffix.encoded_len();
        let mut message = Message::<DoViewChangeHeader>::new(total);
        suffix.encode_into(&mut message.as_mut_slice()[header_size..total]);
        message.transmute_header(|_, header: &mut DoViewChangeHeader| {
            header.command = Command::DoViewChange;
            header.cluster = CLUSTER;
            header.replica = replica;
            header.view = *view;
            header.log_view = *log_view;
            header.op = *op;
            header.commit = *commit;
            header.group = *group;
            header.nack_bitset = suffix.nack_bitset();
            header.present_bitset = suffix.present_bitset();
            header.size = u32::try_from(total).unwrap();
        })
    }
}

#[cfg(test)]
mod control_frame_tests {
    //! A control frame's body must be verified on a rule corruption cannot switch
    //! off. Keying on `checksum_body` looking sealed is bypassable by zeroing it.

    use super::{control_body_checksum, control_suffix_body_verified};
    use iggy_binary_protocol::{Command, DoViewChangeHeader, PrepareHeader};
    use server_common::Message;
    use std::mem::size_of;

    /// A `DoViewChange` frame carrying `entries` blank suffix headers.
    fn frame(entries: usize, checksum_body: u128) -> Message<DoViewChangeHeader> {
        let header_size = size_of::<DoViewChangeHeader>();
        let total = header_size + entries * size_of::<PrepareHeader>();
        let mut msg = Message::<DoViewChangeHeader>::new(total);
        for (index, byte) in msg.as_mut_slice()[header_size..total]
            .iter_mut()
            .enumerate()
        {
            *byte = u8::try_from(index % 251).expect("modulus fits u8");
        }
        msg.transmute_header(|_, header: &mut DoViewChangeHeader| {
            header.command = Command::DoViewChange;
            header.checksum_body = checksum_body;
            header.size = u32::try_from(total).expect("frame fits u32");
        })
    }

    #[test]
    fn given_a_sealed_body_when_verifying_should_accept() {
        let header_size = size_of::<DoViewChangeHeader>();
        let unsealed = frame(2, 0);
        let sealed_value = control_body_checksum(
            &unsealed.as_slice()[header_size..unsealed.header().size as usize],
        );
        let msg = frame(2, sealed_value);

        assert!(
            control_suffix_body_verified(&msg, msg.header().checksum_body).is_some(),
            "a correctly sealed body must be accepted"
        );
    }

    #[test]
    fn given_a_body_with_a_zeroed_checksum_when_verifying_should_reject() {
        // A non-empty body always came from a sender that seals it, so a zero here is
        // corruption. Treating it as "unsealed, skip" disables the layer by clearing
        // the one field that decides whether anything is checked.
        let msg = frame(2, 0);
        assert!(
            control_suffix_body_verified(&msg, msg.header().checksum_body).is_none(),
            "a non-empty body with a zeroed checksum must be rejected, not waved through"
        );
    }

    #[test]
    fn given_a_corrupted_body_when_verifying_should_reject() {
        let header_size = size_of::<DoViewChangeHeader>();
        let unsealed = frame(2, 0);
        let sealed_value = control_body_checksum(
            &unsealed.as_slice()[header_size..unsealed.header().size as usize],
        );
        let mut msg = frame(2, sealed_value);
        msg.as_mut_slice()[header_size] ^= 0xFF;

        assert!(
            control_suffix_body_verified(&msg, msg.header().checksum_body).is_none(),
            "a body that does not match its checksum must be rejected"
        );
    }

    #[test]
    fn given_a_header_only_frame_when_verifying_should_accept() {
        // A sender with nothing uncommitted contributes numbers only, no body.
        let msg = frame(0, 0);
        let body = control_suffix_body_verified(&msg, msg.header().checksum_body)
            .expect("a header-only frame has nothing to verify");
        assert_eq!(body, b"");
    }
}

#[cfg(test)]
mod superblock_fail_stop_tests {
    //! The bound must stay disabled at 0: the simulator asserts a wedged
    //! replica survives fenced in-process, and only the server arms it.

    use super::superblock_wedged;

    #[test]
    fn zero_bound_never_fires() {
        assert!(!superblock_wedged(u64::MAX, 0));
    }

    #[test]
    fn bound_fires_at_and_past_the_threshold() {
        assert!(!superblock_wedged(119, 120));
        assert!(superblock_wedged(120, 120));
        assert!(superblock_wedged(121, 120));
    }
}

#[cfg(test)]
mod sweep_scheduler_tests {
    //! Fairness of the partition sweep's per-tick caps.
    //!
    //! The caps exist so a node-wide rejoin cannot put every group's walk (each
    //! reaching a segment flush) into one tick body. They are only ACCEPTABLE
    //! because the sweep resumes where the WALK budget ran out: the snapshot is
    //! in ascending namespace order, so a fixed start would spend every pass on
    //! the same leading groups and leave the tail holding committed ops it can
    //! never walk to.
    //!
    //! Both budgets are modelled, because they share the sweep: the arm cap
    //! runs first and can turn groups away ahead of the walks, and the walk
    //! fairness bound has to survive that. What keeps them independent is that
    //! the arm cap moves no cursor, which is the property these runs pin.

    use super::{
        IggyNamespace, PARTITION_REPAIR_ARMS_PER_TICK_MAX, PARTITION_REPAIRS_INFLIGHT_MAX,
        PARTITION_WALKS_PER_TICK_MAX, rotate_sweep_to_cursor,
    };

    /// Comfortably past `PARTITION_WALKS_PER_TICK_MAX`, and deliberately not a
    /// multiple of it, so the wrap lands mid-snapshot on most passes.
    const GROUPS: usize = 40;

    /// Every third group is gap-stopped rather than walk-stalled. The two
    /// predicates are disjoint below the commit frontier, so a group is in one
    /// set or the other, and this spreads the arm-capped ones through the
    /// snapshot instead of parking them in one block.
    const fn is_gap_stopped(partition: usize) -> bool {
        partition.is_multiple_of(3)
    }

    fn namespaces() -> Vec<IggyNamespace> {
        (0..GROUPS)
            .map(|partition| IggyNamespace::new(1, 1, partition))
            .collect()
    }

    /// Per-group tallies, indexed by partition id. No `Default`: empty vecs
    /// next to a `new` that sizes them by `GROUPS` would panic on first index.
    struct Served {
        walks: Vec<u32>,
        arms: Vec<u32>,
    }

    impl Served {
        fn new() -> Self {
            Self {
                walks: vec![0; GROUPS],
                arms: vec![0; GROUPS],
            }
        }
    }

    /// Sweeps a session stays open for before it completes. Long enough that
    /// the live-session ceiling actually binds (it is reached on the third
    /// sweep at three arms a pass), so the model spends passes waiting on
    /// capacity the way a real rejoin does.
    const SESSION_SWEEPS: u32 = 6;

    /// Repair state per group, standing in for `partition.repair` (which fences
    /// a group out of the gap-stopped set while it stands) and for the hole
    /// itself (which a completed session closes, so the group stops being
    /// eligible rather than arming again).
    struct Repairs {
        live: Vec<Option<u32>>,
        done: Vec<bool>,
    }

    impl Repairs {
        fn new() -> Self {
            Self {
                live: vec![None; GROUPS],
                done: vec![false; GROUPS],
            }
        }

        /// Age every open session by one sweep, closing the gaps that finish.
        fn retire(&mut self) {
            for (partition, session) in self.live.iter_mut().enumerate() {
                let Some(remaining) = session else {
                    continue;
                };
                *remaining -= 1;
                if *remaining == 0 {
                    *session = None;
                    self.done[partition] = true;
                }
            }
        }

        fn live_count(&self) -> usize {
            self.live.iter().filter(|session| session.is_some()).count()
        }

        fn is_due(&self, partition: usize) -> bool {
            is_gap_stopped(partition) && !self.done[partition] && self.live[partition].is_none()
        }
    }

    /// One sweep of `tick_partitions`' scheduling: rotate to the carried walk
    /// cursor, retire whatever finished, then spend the rate cap, the live
    /// ceiling and the walk budget in snapshot order. Answers with the cursor
    /// this sweep leaves behind.
    fn sweep(
        cursor: Option<IggyNamespace>,
        served: &mut Served,
        repairs: &mut Repairs,
    ) -> Option<IggyNamespace> {
        repairs.retire();
        let mut snapshot = namespaces();
        rotate_sweep_to_cursor(&mut snapshot, cursor);
        let mut walks = 0;
        let mut arms = 0;
        let mut walk_deferred = None;
        for namespace in snapshot {
            let partition = namespace.partition_id();
            if is_gap_stopped(partition) {
                // Both ceilings, in the order the sweep applies them: the rate
                // cap it counts itself, then the live-session count the arm fn
                // refuses on.
                if !repairs.is_due(partition)
                    || arms >= PARTITION_REPAIR_ARMS_PER_TICK_MAX
                    || repairs.live_count() >= PARTITION_REPAIRS_INFLIGHT_MAX
                {
                    continue;
                }
                served.arms[partition] += 1;
                repairs.live[partition] = Some(SESSION_SWEEPS);
                arms += 1;
                continue;
            }
            if walks < PARTITION_WALKS_PER_TICK_MAX {
                served.walks[partition] += 1;
                walks += 1;
            } else {
                walk_deferred.get_or_insert(namespace);
            }
        }
        walk_deferred
    }

    #[test]
    fn given_more_eligible_groups_than_the_walk_budget_when_swept_should_reach_every_one() {
        let mut served = Served::new();
        let mut repairs = Repairs::new();
        let mut cursor = None;
        let walk_eligible = (0..GROUPS).filter(|p| !is_gap_stopped(*p)).count();
        let passes = walk_eligible.div_ceil(PARTITION_WALKS_PER_TICK_MAX);
        for _ in 0..passes {
            cursor = sweep(cursor, &mut served, &mut repairs);
        }
        let unserved: Vec<_> = (0..GROUPS)
            .filter(|partition| !is_gap_stopped(*partition) && served.walks[*partition] == 0)
            .collect();
        assert!(
            unserved.is_empty(),
            "{} of {walk_eligible} walk-eligible groups never had their walk run in \
             {passes} sweeps (partitions {unserved:?}); the budget is being spent on \
             the same leading groups every pass",
            unserved.len()
        );
    }

    #[test]
    fn given_a_sustained_backlog_when_swept_should_keep_every_group_within_one_walk() {
        // Sustained, because the starvation this guards against only shows over
        // many passes: one sweep serves the head no matter how the cursor moves.
        // Walk-eligible groups stay eligible throughout (a walked group is
        // walk-stalled again on the next produce), so a fair scheduler owes them
        // walks in round-robin and none may drift a full round behind.
        let mut served = Served::new();
        let mut repairs = Repairs::new();
        let mut cursor = None;
        for _ in 0..10 * GROUPS {
            cursor = sweep(cursor, &mut served, &mut repairs);
        }
        let walks: Vec<u32> = (0..GROUPS)
            .filter(|partition| !is_gap_stopped(*partition))
            .map(|partition| served.walks[partition])
            .collect();
        let most = walks.iter().max().copied().unwrap_or_default();
        let fewest = walks.iter().min().copied().unwrap_or_default();
        assert!(
            most - fewest <= 1,
            "walks are not spread evenly: one group got {most}, another {fewest}"
        );
    }

    #[test]
    fn given_a_capped_arm_backlog_when_swept_should_arm_every_gap_stopped_group() {
        // The arm caps carry no cursor because arming REMOVES a group from the
        // eligible set: the front of the queue drains, so the tail is reached
        // without one. If that ever stops holding, this run wedges.
        //
        // Both ceilings are modelled, so the run also pins that the live-session
        // cap only DELAYS: a group refused for capacity keeps its debounce and
        // arms once a session retires. Bounded by the slower of the two, plus a
        // session's life for the last batch to have somewhere to go.
        let mut served = Served::new();
        let mut repairs = Repairs::new();
        let mut cursor = None;
        let gap_stopped = (0..GROUPS).filter(|p| is_gap_stopped(*p)).count();
        let by_rate = gap_stopped.div_ceil(PARTITION_REPAIR_ARMS_PER_TICK_MAX);
        let by_capacity =
            gap_stopped.div_ceil(PARTITION_REPAIRS_INFLIGHT_MAX) * SESSION_SWEEPS as usize;
        for _ in 0..by_rate.max(by_capacity) + SESSION_SWEEPS as usize {
            cursor = sweep(cursor, &mut served, &mut repairs);
        }
        let unarmed: Vec<_> = (0..GROUPS)
            .filter(|partition| is_gap_stopped(*partition) && served.arms[*partition] == 0)
            .collect();
        assert!(
            unarmed.is_empty(),
            "gap-stopped groups {unarmed:?} never armed; the arm caps are queueing \
             behind the same prefix and need a cursor after all"
        );
        assert!(
            served.arms.iter().all(|arms| *arms <= 1),
            "a group armed twice while its first session was still open"
        );
    }

    #[test]
    fn given_the_live_session_ceiling_when_swept_should_never_exceed_it() {
        // The ceiling exists because each session is a window the SERVING peer
        // walks on its own pump; the rate cap alone would let a rejoin put every
        // group's stream in flight within `groups / 3` passes.
        let mut served = Served::new();
        let mut repairs = Repairs::new();
        let mut cursor = None;
        for _ in 0..10 * GROUPS {
            cursor = sweep(cursor, &mut served, &mut repairs);
            let live = repairs.live_count();
            assert!(
                live <= PARTITION_REPAIRS_INFLIGHT_MAX,
                "{live} sessions live at once, past the ceiling of \
                 {PARTITION_REPAIRS_INFLIGHT_MAX}"
            );
        }
    }

    #[test]
    fn given_a_cursor_naming_a_removed_namespace_when_rotating_should_resume_at_its_successor() {
        // The group the cap turned away can be deleted before the next sweep;
        // the resume point is then the next namespace above it, not the front.
        let mut snapshot = namespaces();
        let removed = snapshot.remove(20);
        rotate_sweep_to_cursor(&mut snapshot, Some(removed));
        assert_eq!(
            snapshot.first().copied(),
            Some(IggyNamespace::new(1, 1, 21))
        );
    }

    #[test]
    fn given_a_cursor_past_every_namespace_when_rotating_should_wrap_to_the_front() {
        let mut snapshot = namespaces();
        rotate_sweep_to_cursor(&mut snapshot, Some(IggyNamespace::new(1, 1, GROUPS)));
        assert_eq!(snapshot, namespaces());
    }

    #[test]
    fn given_no_deferral_last_pass_when_rotating_should_start_at_the_front() {
        let mut snapshot = namespaces();
        rotate_sweep_to_cursor(&mut snapshot, None);
        assert_eq!(snapshot, namespaces());
    }
}

#[cfg(test)]
mod gap_detector_tests {
    //! The level-triggered repair arm the partition and metadata tick drivers
    //! share.
    //!
    //! Its whole reason to exist is that the edge-triggered arming sites are
    //! starvable, so the guards it shares with them and the debounce that keeps
    //! it off healthy traffic are the parts worth pinning. Probes are built
    //! here by hand: what the two planes read off their own state is
    //! `partition_gap_probe`'s and `metadata_gap_probe`'s business, and the
    //! simulator's driver suites cover those end to end.

    use super::{
        GapArm, GapProbe, REPAIR_GAP_DEBOUNCE_TICKS_MIN, drive_group_gap_debounce,
        group_is_gap_stopped, group_is_walk_stalled,
    };

    const DEBOUNCE: u32 = 100;

    /// A gap-stopped follower: committed through op 10, walkable only to 5,
    /// because op 6 is not in its journal.
    const fn gap_stopped() -> GapProbe {
        GapProbe {
            normal: true,
            transferring: false,
            recovery_owned: false,
            commit_min: 5,
            commit_max: 10,
            next_op_resident: false,
            missing_suffix: false,
        }
    }

    /// A walk-stalled follower: the same lag, but op 6 IS in its journal, so
    /// nothing needs fetching and the walk just has to run.
    fn walk_stalled() -> GapProbe {
        GapProbe {
            next_op_resident: true,
            ..gap_stopped()
        }
    }

    /// A follower level with its commit frontier that holds adopted suffix
    /// headers whose bodies never arrived.
    fn missing_suffix() -> GapProbe {
        GapProbe {
            commit_min: 10,
            missing_suffix: true,
            ..gap_stopped()
        }
    }

    #[test]
    fn given_a_lagging_follower_with_the_next_op_resident_when_probed_should_not_be_gap_stopped() {
        // The half that keeps the predicate honest. A follower advances
        // commit_max from every prepare header in preflight, so commit_min <
        // commit_max is transiently true on any pipelined tick; without the
        // journal-hole test the driver would request repair against ordinary
        // produce, on every partition, forever.
        assert!(!group_is_gap_stopped(&walk_stalled()));
        assert!(group_is_gap_stopped(&gap_stopped()));
    }

    #[test]
    fn given_a_caught_up_follower_when_probed_should_not_be_gap_stopped() {
        let caught_up = GapProbe {
            commit_min: 10,
            ..gap_stopped()
        };
        assert!(!group_is_gap_stopped(&caught_up));
    }

    #[test]
    fn given_adopted_suffix_headers_without_bodies_when_probed_should_be_gap_stopped() {
        // The shape a commit-frontier test alone misses: both marks sit below
        // the head, so there is no lag to see, and the only other site that
        // notices is the single `on_start_view` edge that adopted the headers.
        // Left out, that class hangs until an unrelated view change.
        assert!(group_is_gap_stopped(&missing_suffix()));
        assert!(
            !group_is_gap_stopped(&GapProbe {
                missing_suffix: false,
                ..missing_suffix()
            }),
            "a caught-up replica with a complete suffix has nothing to repair"
        );
    }

    #[test]
    fn given_a_replica_outside_normal_status_when_probed_should_not_be_gap_stopped() {
        // A view change owns the log while it runs, and `maybe_request_partition_repair`
        // refuses outside Normal anyway; arming here would only burn a nonce.
        for probe in [gap_stopped(), missing_suffix()] {
            assert!(!group_is_gap_stopped(&GapProbe {
                normal: false,
                ..probe
            }));
            assert!(!group_is_gap_stopped(&GapProbe {
                transferring: true,
                ..probe
            }));
        }
    }

    #[test]
    fn given_recovery_already_owned_when_probed_should_not_be_gap_stopped() {
        // A session, a transfer, or a scheduled transfer re-arm all own the
        // recovery; a second one would race it or defeat the re-arm's backoff.
        for probe in [gap_stopped(), missing_suffix()] {
            assert!(!group_is_gap_stopped(&GapProbe {
                recovery_owned: true,
                ..probe
            }));
        }
    }

    #[test]
    fn given_a_gap_stopped_follower_when_debouncing_should_arm_only_at_the_threshold() {
        let probe = gap_stopped();
        let mut gap_ticks = 0;
        for tick in 1..DEBOUNCE {
            assert_eq!(
                drive_group_gap_debounce(&probe, &mut gap_ticks, DEBOUNCE, true),
                GapArm::NotDue,
                "armed at tick {tick}, before the debounce elapsed"
            );
        }
        assert_eq!(
            drive_group_gap_debounce(&probe, &mut gap_ticks, DEBOUNCE, true),
            GapArm::Arm
        );
    }

    #[test]
    fn given_a_debounce_in_progress_when_the_gap_closes_should_reset_the_counter() {
        let stopped = gap_stopped();
        let walkable = GapProbe {
            next_op_resident: true,
            ..stopped
        };
        let mut gap_ticks = 0;
        for _ in 0..DEBOUNCE - 1 {
            drive_group_gap_debounce(&stopped, &mut gap_ticks, DEBOUNCE, true);
        }
        assert_eq!(gap_ticks, DEBOUNCE - 1);

        assert_eq!(
            drive_group_gap_debounce(&walkable, &mut gap_ticks, DEBOUNCE, true),
            GapArm::NotDue
        );
        assert_eq!(gap_ticks, 0, "progress must restart the debounce");
        assert_eq!(
            drive_group_gap_debounce(&stopped, &mut gap_ticks, DEBOUNCE, true),
            GapArm::NotDue,
            "a fresh gap must serve its own debounce, not inherit the old count"
        );
    }

    #[test]
    fn given_a_follower_with_resident_committed_ops_when_probed_should_be_walk_stalled() {
        assert!(group_is_walk_stalled(&walk_stalled()));
        assert!(
            !group_is_walk_stalled(&gap_stopped()),
            "a missing next op is repair's job; a walk over it would stop dead"
        );
    }

    #[test]
    fn given_a_caught_up_follower_when_probed_should_not_be_walk_stalled() {
        let caught_up = GapProbe {
            commit_min: 10,
            ..walk_stalled()
        };
        assert!(!group_is_walk_stalled(&caught_up));
    }

    #[test]
    fn given_a_replica_outside_normal_status_when_probed_should_not_be_walk_stalled() {
        let electing = GapProbe {
            normal: false,
            ..walk_stalled()
        };
        assert!(!group_is_walk_stalled(&electing));

        // Same gate as the on-commit arm: a walk during a transfer can advance
        // commit_min past the incoming frontier.
        let installing = GapProbe {
            transferring: true,
            ..walk_stalled()
        };
        assert!(!group_is_walk_stalled(&installing));
    }

    #[test]
    fn given_recovery_already_owned_when_the_next_op_is_resident_should_still_be_walk_stalled() {
        // Deliberate: `apply_repaired_prepare` journals without walking, so a
        // gated walk would sit parked for the whole session while the resident
        // prefix is already applicable.
        assert!(group_is_walk_stalled(&GapProbe {
            recovery_owned: true,
            ..walk_stalled()
        }));
    }

    #[test]
    fn given_any_probe_when_evaluated_should_never_be_both_gap_stopped_and_walk_stalled() {
        // If they ever overlap, one tick both arms repair and walks the window
        // it is fetching, and the arm refetches a resident committed prefix.
        for normal in [false, true] {
            for transferring in [false, true] {
                for recovery_owned in [false, true] {
                    for (commit_min, commit_max) in [(5, 10), (10, 10)] {
                        for next_op_resident in [false, true] {
                            for missing_suffix in [false, true] {
                                let probe = GapProbe {
                                    normal,
                                    transferring,
                                    recovery_owned,
                                    commit_min,
                                    commit_max,
                                    next_op_resident,
                                    missing_suffix,
                                };
                                assert!(
                                    !(group_is_gap_stopped(&probe)
                                        && group_is_walk_stalled(&probe)),
                                    "both predicates claim {probe:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn given_a_missing_suffix_over_a_walkable_lag_when_evaluated_should_prefer_the_walk() {
        // Arming here would request `(commit_min, head]`: the committed prefix
        // this replica already holds resident, refetched, plus the suffix. The
        // walk closes the lag first -- `commit_max` cannot move while the suffix
        // is short of quorum -- and the suffix arms on the pass after.
        let probe = GapProbe {
            missing_suffix: true,
            ..walk_stalled()
        };
        assert!(group_is_walk_stalled(&probe));
        assert!(
            !group_is_gap_stopped(&probe),
            "a walkable lag must win the tick; the suffix arm waits for it to close"
        );
        assert!(
            group_is_gap_stopped(&GapProbe {
                commit_min: probe.commit_max,
                ..probe
            }),
            "and the same replica arms once the lag is gone"
        );
    }

    #[test]
    fn given_the_arm_budget_spent_when_debouncing_should_defer_without_losing_the_debounce() {
        let probe = gap_stopped();
        let mut gap_ticks = DEBOUNCE;
        assert_eq!(
            drive_group_gap_debounce(&probe, &mut gap_ticks, DEBOUNCE, false),
            GapArm::Deferred,
            "a spent budget must refuse the arm"
        );
        assert!(
            gap_ticks > DEBOUNCE,
            "a refused group stays due; restarting its debounce would push the \
             arm a whole interval out per contended tick"
        );
        assert_eq!(
            drive_group_gap_debounce(&probe, &mut gap_ticks, DEBOUNCE, true),
            GapArm::Arm,
            "the same group arms on the next pass with a slot free"
        );
    }

    #[test]
    fn given_a_debounce_shorter_than_the_floor_when_driven_should_hold_until_the_floor() {
        // `repair_retry_interval` is an operator knob whose primary meaning is
        // the stalled-stream retry, and `duration_to_ticks` floors it at one
        // tick. One tick of lag is ordinary pipelining, so without a floor of
        // its own a shortened retry interval would arm repair against a single
        // reordered prepare.
        let probe = gap_stopped();
        let mut gap_ticks = 0;
        for tick in 1..REPAIR_GAP_DEBOUNCE_TICKS_MIN {
            assert_eq!(
                drive_group_gap_debounce(&probe, &mut gap_ticks, 1, true),
                GapArm::NotDue,
                "a 1-tick debounce armed at tick {tick}, under the floor"
            );
        }
        assert_eq!(
            drive_group_gap_debounce(&probe, &mut gap_ticks, 1, true),
            GapArm::Arm
        );
    }
}

#[cfg(test)]
mod metadata_repair_session_tests {
    //! The three rules a standing metadata repair session lives by: who it is
    //! opened against, when it is closed, and when a walked terminator pulls
    //! the next chunk of its window.
    //!
    //! All three wedge the plane rather than failing loudly. A session fences
    //! every other arming site and holds the gap debounce at zero while it
    //! stands, so one opened against nobody, or kept past the view that decided
    //! its window, or that stops pulling chunks, pins the commit frontier with
    //! nothing else able to arm.

    use super::{
        MetadataRepairSession, gap_repair_peer, metadata_repair_superseded, next_repair_peer,
        next_transfer_peer, repair_chunk_walked, stalled_repair_from_op,
    };

    /// Armed at view 3, against the window `11..=20`.
    const fn session() -> MetadataRepairSession {
        MetadataRepairSession {
            nonce: 7,
            from_op: 11,
            to_op: 20,
            view: 3,
            peer: 0,
            idle_ticks: 0,
            transfer_refused: false,
        }
    }

    /// A merged-log session must re-ask for the op the coverage scan reported, even
    /// once the walk has passed it. Coverage is about the journal ENTRY; an op can
    /// be applied and still have no entry to serve, which is exactly what
    /// `committed_elsewhere` reports.
    #[test]
    fn given_a_dropped_response_below_commit_min_when_retrying_should_still_ask_for_it() {
        assert_eq!(
            stalled_repair_from_op(5, 6, true),
            5,
            "clamping to commit_min + 1 would retry from 7 and skip the reported hole"
        );
    }

    /// The tail-repair session is the other way round: its window is the commit gap,
    /// so ops the walk consumed must not be re-requested.
    #[test]
    fn given_a_walked_window_when_retrying_a_tail_session_should_open_above_it() {
        assert_eq!(stalled_repair_from_op(5, 6, false), 7);
        assert_eq!(
            stalled_repair_from_op(11, 3, false),
            11,
            "the arm floor still holds, so no retry asks for compacted ops"
        );
    }

    #[test]
    fn given_a_gap_stopped_backup_when_picking_a_peer_should_ask_the_primary() {
        assert_eq!(gap_repair_peer(2, 3, 0), Some(0));
        assert_eq!(gap_repair_peer(1, 5, 3), Some(3));
    }

    #[test]
    fn given_a_gap_stopped_primary_when_picking_a_peer_should_never_ask_itself() {
        // The case `maybe_request_metadata_repair`'s self-guard exists for: no
        // other site re-drives a settled primary's own hole, and a
        // self-addressed request fails to send AFTER the session is recorded.
        for replica_count in 2..=7u8 {
            for primary in 0..replica_count {
                let peer = gap_repair_peer(primary, replica_count, primary);
                assert_ne!(peer, Some(primary), "count {replica_count}");
                assert!(peer.is_some(), "count {replica_count}");
            }
        }
    }

    #[test]
    fn given_a_solo_group_when_picking_a_peer_should_answer_nobody() {
        assert_eq!(gap_repair_peer(0, 1, 0), None);
    }

    #[test]
    fn given_a_silent_peer_when_the_stall_budget_is_spent_should_rotate_off_it() {
        // Re-arming against the peer that just went quiet spends another whole
        // budget on it, and the ring is the only thing that names anyone else.
        for replica_count in 3..=7u8 {
            for primary in 0..replica_count {
                let self_id = (primary + 1) % replica_count;
                let failed = gap_repair_peer(self_id, replica_count, primary).expect("a peer");
                let next = next_transfer_peer(self_id, failed, replica_count, primary);
                assert_ne!(next, failed, "count {replica_count}, primary {primary}");
                assert_ne!(next, self_id, "count {replica_count}, primary {primary}");
            }
        }
    }

    #[test]
    fn given_two_replicas_when_the_stall_budget_is_spent_should_name_the_same_peer_back() {
        // Which is how the caller reads "nobody else to ask" and drops the
        // session instead of re-arming it.
        assert_eq!(next_transfer_peer(1, 0, 2, 0), 0);
        assert_eq!(next_repair_peer(1, 0, 2), 0);
    }

    #[test]
    fn given_a_five_replica_ring_when_repair_keeps_rotating_should_ask_every_other_replica() {
        // Going back to the primary after each backup alternates between two
        // peers, and in a group of five the other two would never be asked.
        const REPLICAS: u8 = 5;
        for self_id in 0..REPLICAS {
            for primary in (0..REPLICAS).filter(|&primary| primary != self_id) {
                let others: Vec<u8> = (0..REPLICAS).filter(|&peer| peer != self_id).collect();
                let mut asked = vec![primary];
                while asked.len() < others.len() {
                    let failed = asked[asked.len() - 1];
                    asked.push(next_repair_peer(self_id, failed, REPLICAS));
                }
                let lap_end = asked[asked.len() - 1];
                asked.sort_unstable();
                assert_eq!(asked, others, "self {self_id}, primary {primary}");
                assert_eq!(
                    next_repair_peer(self_id, lap_end, REPLICAS),
                    primary,
                    "self {self_id}: a full lap must come back to the primary"
                );
            }
        }
    }

    #[test]
    fn given_a_session_whose_window_is_walked_when_checked_should_be_superseded() {
        let session = session();
        assert!(metadata_repair_superseded(
            &session,
            session.to_op,
            session.view,
            true,
            false
        ));
        assert!(!metadata_repair_superseded(
            &session,
            session.to_op - 1,
            session.view,
            true,
            false
        ));
    }

    #[test]
    fn given_a_session_armed_in_an_earlier_view_when_checked_should_be_superseded() {
        let session = session();
        assert!(metadata_repair_superseded(
            &session,
            0,
            session.view + 1,
            true,
            false
        ));
    }

    #[test]
    fn given_a_replica_that_left_normal_when_checked_should_be_superseded() {
        let session = session();
        assert!(metadata_repair_superseded(
            &session,
            0,
            session.view,
            false,
            false
        ));
    }

    #[test]
    fn given_a_primary_elect_repairing_its_merged_log_when_checked_should_stand() {
        // The one session that runs outside `Normal`;
        // `advance_pending_metadata_view` cannot start the view until its
        // window fills.
        let session = session();
        assert!(!metadata_repair_superseded(
            &session,
            0,
            session.view,
            false,
            true
        ));
        assert!(
            metadata_repair_superseded(&session, 0, session.view + 1, false, true),
            "not even the primary-elect's session survives the next view"
        );
    }

    #[test]
    fn given_a_chunk_this_walk_moved_when_checked_should_pull_the_next_chunk() {
        assert!(repair_chunk_walked(5, 8, 12));
    }

    #[test]
    fn given_a_chunk_the_tick_already_walked_when_checked_should_pull_the_next_chunk() {
        // `tick_metadata` walks the same journal, so the terminator can arrive
        // with nothing left for its own walk to move.
        assert!(repair_chunk_walked(12, 12, 12));
    }

    #[test]
    fn given_a_window_still_holed_below_the_terminator_when_checked_should_wait_for_the_retry() {
        // A frame was lost inside the served chunk: re-requesting now would
        // race the retry timer for the same window.
        assert!(!repair_chunk_walked(5, 5, 12));
    }
}

#[cfg(test)]
mod partition_placement_tests {
    use super::*;
    use consensus::{CLIENTS_TABLE_MAX, LocalPipeline, build_reply_message_with};
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{
        ReplyHeader, WireDecode, WireEncode, WireIdentifier, WireName, WireOptions,
    };
    use iggy_common::{IggyByteSize, PartitionStats};
    use journal::prepare_journal::PrepareJournal;
    use message_bus::IggyMessageBus;
    use metadata::MuxStateMachine;
    use metadata::impls::metadata::IggySnapshot;
    use metadata::impls::recovery::{ReplicaIdentity, recover};
    use metadata::stm::stream::Streams;
    use metadata::stm::user::Users;
    use partitions::{PartitionPathLayout, PartitionsConfig};
    use server_common::sharding::METADATA_GROUP;
    use shards_table::PapayaShardsTable;
    use std::collections::BTreeSet;
    use std::mem::size_of;
    use std::sync::Arc;

    type PlacementShard = IggyShard<
        Rc<IggyMessageBus>,
        PrepareJournal,
        IggySnapshot,
        MuxStateMachine<variadic!(Users, Streams)>,
        PapayaShardsTable,
    >;

    pub fn owner(shard_id: u16) -> (PlacementShard, Receiver<ShardFrame>) {
        let bus = Rc::new(IggyMessageBus::new(shard_id));
        let segment_size = IggyByteSize::from(1_048_576_u64);
        let partitions = IggyPartitions::new(
            ShardId::new(shard_id),
            PartitionsConfig {
                messages_required_to_save: 100,
                size_of_messages_required_to_save: segment_size,
                validate_checksum: true,
                segment_size,
                preallocate_segments: false,
                key_context: Arc::default(),
                path_layout: PartitionPathLayout::default(),
            },
        );
        let metadata = IggyMetadata::new(
            None,
            None,
            None,
            None,
            MuxStateMachine::<variadic!(Users, Streams)>::default(),
            None,
        );
        let mut shard = PlacementShard::without_inbox(
            ShardIdentity::new(shard_id, "partition-placement-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus),
        );
        let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(shard_id + 1, 1, 1);
        let target = inboxes[0].take().expect("shard zero inbox");
        shard.attach_senders(senders);
        (shard, target)
    }

    fn insert(shard: &PlacementShard, id: usize, revision: u64, view: u32) -> IggyNamespace {
        let namespace = IggyNamespace::new(0, 0, id);
        let mut consensus = VsrConsensus::new(
            1,
            0,
            3,
            namespace.inner(),
            shard.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.set_view(view);
        consensus.init();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            shard.plane.partitions().config().segment_size,
        );
        partition.set_created_revision(revision);
        shard.plane.partitions().insert(namespace, partition);
        namespace
    }

    fn publish(shard: &PlacementShard) {
        let namespaces: Vec<_> = shard.plane.partitions().namespaces().copied().collect();
        shard.publish_partition_views(&namespaces);
    }

    fn receive(inbox: &Receiver<ShardFrame>) -> Vec<PartitionPlacementView> {
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionPlacementViews(reports)) =
            inbox.try_recv().expect("placement report queued")
        else {
            panic!("expected partition placement observations");
        };
        assert!(inbox.try_recv().is_err(), "at most one batch per sweep");
        reports
    }

    #[compio::test]
    async fn given_unchanged_placement_when_ticks_advance_should_report_at_probe_intervals() {
        let (shard, inbox) = owner(1);
        insert(&shard, 0, 7, 0);
        shard.ticks.set(1);
        publish(&shard);
        assert_eq!(receive(&inbox).len(), 1);
        let interval = u64::try_from(PARTITION_PLACEMENT_REPORT_INTERVAL_TICKS).unwrap();
        for tick in 2..=interval {
            shard.ticks.set(tick);
            publish(&shard);
            assert!(
                inbox.try_recv().is_err(),
                "placement report repeated at tick {tick}"
            );
        }
        shard.ticks.set(interval + 1);
        publish(&shard);
        assert_eq!(receive(&inbox).len(), 1);
    }

    #[compio::test]
    async fn given_more_groups_than_one_batch_when_an_interval_starts_should_report_each_group_within_it()
     {
        let (shard, inbox) = owner(1);
        let count = PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN * 2 + 3;
        for id in 0..count {
            insert(&shard, id, 7, 0);
        }
        let batches =
            u64::try_from(count.div_ceil(PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN)).unwrap();
        let mut observed = BTreeSet::new();
        for tick in 1..=batches {
            shard.ticks.set(tick);
            publish(&shard);
            observed.extend(
                receive(&inbox)
                    .into_iter()
                    .map(|report| report.namespace.partition_id()),
            );
        }
        assert_eq!(observed, (0..count).collect());
        shard.ticks.set(batches + 1);
        publish(&shard);
        assert!(
            inbox.try_recv().is_err(),
            "a finished pass waits for the next interval"
        );
    }

    #[compio::test]
    async fn given_more_groups_than_an_interval_of_batches_when_an_interval_runs_should_report_each_group_within_it()
     {
        let (shard, inbox) = owner(1);
        let interval = u64::try_from(PARTITION_PLACEMENT_REPORT_INTERVAL_TICKS).unwrap();
        let count = PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN * usize::try_from(interval).unwrap()
            + PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN
            + 3;
        for id in 0..count {
            insert(&shard, id, 7, 0);
        }
        let mut observed = BTreeSet::new();
        for tick in 1..=interval {
            shard.ticks.set(tick);
            publish(&shard);
            if !inbox.is_empty() {
                observed.extend(
                    receive(&inbox)
                        .into_iter()
                        .map(|report| report.namespace.partition_id()),
                );
            }
        }
        assert_eq!(
            observed.len(),
            count,
            "every group reports within one interval"
        );

        shard.ticks.set(interval + 1);
        publish(&shard);
        assert_eq!(
            receive(&inbox)[0].namespace,
            IggyNamespace::new(0, 0, 0),
            "the next interval starts a new pass at the front"
        );
    }

    #[compio::test]
    async fn given_owned_groups_when_ticking_should_report_only_normal_serving_views_to_shard_zero()
    {
        for shard_id in [0, 1] {
            let (shard, inbox) = owner(shard_id);
            for id in 0..6 {
                let view = u32::from(id == 1);
                insert(&shard, id, 7, view);
            }
            let partitions = shard.plane.partitions();
            partitions
                .get_by_ns(&IggyNamespace::new(0, 0, 2))
                .unwrap()
                .consensus()
                .begin_view_probe();
            partitions
                .get_by_ns(&IggyNamespace::new(0, 0, 3))
                .unwrap()
                .consensus()
                .begin_state_transfer_await();
            partitions
                .get_by_ns(&IggyNamespace::new(0, 0, 4))
                .unwrap()
                .consensus()
                .init_as_backup();
            partitions.tombstone(IggyNamespace::new(0, 0, 5));

            assert!(shard.tick_partitions(&mut Vec::new()).await.is_none());

            let reports = receive(&inbox);
            assert_eq!(reports.len(), 2);
            assert_eq!(reports[0].namespace, IggyNamespace::new(0, 0, 0));
            assert_eq!(reports[0].view, 0, "a normal primary contributes");
            assert_eq!(reports[1].namespace, IggyNamespace::new(0, 0, 1));
            assert_eq!(
                reports[1].view, 1,
                "a normal backup contributes its known view"
            );
            assert!(reports.iter().all(|report| report.created_revision == 7));
        }
    }

    #[compio::test]
    async fn given_more_groups_than_one_batch_when_publishing_should_bound_batches_and_cover_the_tail()
     {
        let (shard, inbox) = owner(1);
        let count = PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN * 2 + 3;
        for id in 0..count {
            insert(&shard, id, 7, 0);
        }
        let mut observed = BTreeSet::new();
        for _ in 0..count.div_ceil(PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN) {
            publish(&shard);
            let reports = receive(&inbox);
            assert_eq!(reports.len(), PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN);
            observed.extend(
                reports
                    .into_iter()
                    .map(|report| report.namespace.partition_id()),
            );
        }
        assert_eq!(observed, (0..count).collect());

        let removed = shard.partition_placement_cursor.get().unwrap();
        shard.plane.partitions().remove(&removed);
        publish(&shard);
        let reports = receive(&inbox);
        assert_eq!(
            reports[0].namespace.partition_id(),
            removed.partition_id() + 1
        );
        assert!(reports.iter().all(|report| report.namespace != removed));
    }

    #[compio::test]
    async fn given_a_full_inbox_when_retried_should_publish_current_incarnation_without_skipping_groups()
     {
        let (shard, inbox) = owner(1);
        let namespace = insert(&shard, 0, 7, 0);
        insert(&shard, 1, 7, 0);
        assert!(
            shard.senders[0]
                .try_send(ShardFrame::lifecycle(LifecycleFrame::MetadataCommitTick))
                .is_ok()
        );

        publish(&shard);
        assert_eq!(shard.partition_placement_cursor.get(), None);
        assert_eq!(
            inbox.len(),
            1,
            "no deferred report queue grows behind the full inbox"
        );
        assert!(matches!(
            inbox.try_recv(),
            Ok(ShardFrame::Lifecycle(LifecycleFrame::MetadataCommitTick))
        ));
        shard.plane.partitions().remove(&namespace);
        insert(&shard, 0, 11, 9);

        publish(&shard);
        let reports = receive(&inbox);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].namespace, namespace);
        assert_eq!(reports[0].created_revision, 11);
        assert_eq!(
            reports[0].view, 9,
            "retry rereads rather than retaining the old observation"
        );
        assert_eq!(reports[1].namespace, IggyNamespace::new(0, 0, 1));
    }

    #[compio::test]
    async fn given_an_ineligible_prefix_when_publishing_should_bound_visits_and_reach_live_groups()
    {
        let (shard, inbox) = owner(1);
        for id in 0..=PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN {
            let namespace = insert(&shard, id, 7, 0);
            if id < PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN {
                shard
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .unwrap()
                    .consensus()
                    .begin_view_probe();
            }
        }
        publish(&shard);
        assert!(
            inbox.try_recv().is_err(),
            "one sweep visits at most its bounded range"
        );

        publish(&shard);
        let reports = receive(&inbox);
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].namespace.partition_id(),
            PARTITION_PLACEMENT_REPORTS_PER_TICK_MIN
        );
    }

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_reopened_pending_create_when_rebuilding_pipeline_should_restore_placement_reservations()
     {
        type PlacementMetadata = IggyMetadata<
            VsrConsensus<Rc<IggyMessageBus>>,
            PrepareJournal,
            IggySnapshot,
            MuxStateMachine<variadic!(Users, Streams)>,
        >;
        const JOURNAL_SLOTS: usize = 128;
        const FIRST_CLIENT: u128 = 7;
        const NEXT_CLIENT: u128 = 8;
        const ADMISSION_BUDGET: Duration = Duration::from_secs(5);
        let register = |metadata: &PlacementMetadata, client| {
            let reply = build_reply_message_with(
                &PrepareHeader {
                    client,
                    op: 1,
                    operation: Operation::Register,
                    ..Default::default()
                },
                0,
                |_| {},
            );
            metadata
                .client_table
                .borrow_mut()
                .commit_register(client, 0, [0x5a; 32], reply)
                .unwrap();
        };
        let request = |client, name: &str| {
            let body = CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 2,
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            }
            .to_bytes();
            let header_size = size_of::<RoutedRequestHeader>();
            let mut message = Message::<RoutedRequestHeader>::new(header_size + body.len());
            message.as_mut_slice()[header_size..].copy_from_slice(&body);
            message.transmute_header(|_, header| {
                *header = RoutedRequestHeader {
                    command: Command::Request,
                    operation: Operation::CreateTopic,
                    size: u32::try_from(header_size + body.len()).unwrap(),
                    client,
                    session: 1,
                    request: 1,
                    group: METADATA_GROUP,
                    ..Default::default()
                };
            })
        };
        let capture = async |metadata: &PlacementMetadata, request, op| {
            let mut submit = Box::pin(metadata.submit_request_in_process(request));
            compio::time::timeout(ADMISSION_BUDGET, async {
                loop {
                    assert!(
                        submit.as_mut().now_or_never().is_none(),
                        "three replicas need another acknowledgment before commit"
                    );
                    let journal = metadata.journal.as_ref().unwrap();
                    let header = journal.handle().header(op).map(|header| *header);
                    if let Some(header) = header {
                        break journal.handle().entry(&header).await.unwrap();
                    }
                    compio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("admission must append within its budget")
        };

        let directory = tempfile::tempdir().unwrap();
        let metadata_directory = directory.path().join(metadata::impls::METADATA_DIR);
        std::fs::create_dir(&metadata_directory).unwrap();
        let inherited = {
            let journal = PrepareJournal::open_with_slots(
                &metadata_directory.join("journal.wal"),
                0,
                JOURNAL_SLOTS,
            )
            .await
            .unwrap();
            let consensus = VsrConsensus::new(
                1,
                0,
                3,
                METADATA_GROUP,
                Rc::new(IggyMessageBus::new(0)),
                LocalPipeline::new(),
            );
            consensus.init();
            let metadata = PlacementMetadata::new(
                Some(consensus),
                Some(journal),
                None,
                None,
                MuxStateMachine::default(),
                None,
            );
            register(&metadata, FIRST_CLIENT);
            let prepare = capture(&metadata, request(FIRST_CLIENT, "inherited"), 1).await;
            assert_eq!(metadata.consensus.as_ref().unwrap().commit_min(), 0);
            prepare
        };
        let original = CreateTopicWithAssignmentsRequest::decode_from(inherited.body())
            .unwrap()
            .partitions;
        assert_eq!(
            original
                .iter()
                .map(|partition| (partition.consensus_group_id, partition.initial_view))
                .collect::<Vec<_>>(),
            [(1, 0), (2, 1)]
        );

        let recovered = recover::<MuxStateMachine<variadic!(Users, Streams)>>(
            directory.path(),
            ReplicaIdentity {
                cluster: 1,
                replica_id: 0,
                replica_count: 3,
            },
            JOURNAL_SLOTS,
            CLIENTS_TABLE_MAX,
            |_| {},
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(recovered.last_applied_op, None);
        assert_eq!(recovered.last_journaled_op, Some(inherited.header().op));
        assert_eq!(recovered.chain_break_op, None);
        let reopened = recovered
            .journal
            .handle()
            .entry(inherited.header())
            .await
            .unwrap();
        assert_eq!(reopened.as_slice(), inherited.as_slice());

        // Enter the recovered-primary seam; the network election itself is covered separately.
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            METADATA_GROUP,
            Rc::new(IggyMessageBus::new(0)),
            LocalPipeline::new(),
        );
        consensus.init();
        consensus
            .sequencer()
            .set_sequence(recovered.last_journaled_op.unwrap());
        consensus.set_last_prepare_checksum(inherited.header().checksum);
        let metadata = PlacementMetadata::new(
            Some(consensus),
            Some(recovered.journal),
            recovered.snapshot,
            Some(Rc::new(recovered.superblock)),
            recovered.mux_stm,
            None,
        );
        register(&metadata, NEXT_CLIENT);
        let consensus = metadata.consensus.as_ref().unwrap();
        assert!(consensus.pipeline_is_empty());
        dispatch_vsr_actions(
            consensus,
            metadata.journal.as_ref(),
            &[VsrAction::RebuildPipeline {
                from_op: recovered.last_applied_op.unwrap_or(0) + 1,
                to_op: recovered.last_journaled_op.unwrap(),
            }],
        )
        .await;
        assert!(consensus.pipeline_holds_entry(inherited.header().op, inherited.header().checksum));
        let refused = metadata
            .submit_request_in_process(request(NEXT_CLIENT, "next"))
            .await
            .unwrap()
            .try_into_typed::<ReplyHeader>()
            .unwrap();
        assert_eq!(
            iggy_binary_protocol::result_code(refused.body()),
            Some(IggyError::TransientNotAccepted.as_code())
        );
        assert!(!refused.header().has_committed_result());
        assert_eq!(metadata.allocator.highest(), 0);
        metadata.refresh_placement().await;
        assert_eq!(metadata.allocator.highest(), 2);
        let next = capture(&metadata, request(NEXT_CLIENT, "next"), 2).await;
        let assignments = CreateTopicWithAssignmentsRequest::decode_from(next.body())
            .unwrap()
            .partitions;
        assert_eq!(
            assignments
                .iter()
                .map(|partition| (partition.consensus_group_id, partition.initial_view))
                .collect::<Vec<_>>(),
            [(3, 2), (4, 0)]
        );
        assert_eq!(next.header().parent, inherited.header().checksum);
        assert!(consensus.pipeline_holds_entry(inherited.header().op, inherited.header().checksum));
        let retained = metadata
            .journal
            .as_ref()
            .unwrap()
            .handle()
            .entry(inherited.header())
            .await
            .unwrap();
        assert_eq!(
            retained.as_slice(),
            inherited.as_slice(),
            "reconstruction must preserve the original assignment bytes"
        );
    }
}

#[cfg(test)]
mod partition_ack_durability_tests {
    use super::*;
    use consensus::LocalPipeline;
    use iggy_common::PartitionStats;
    use iggy_common::{Durability, IggyByteSize, TopicRuntimeOptions};
    use journal::prepare_journal::PrepareJournal;
    use message_bus::IggyMessageBus;
    use server_common::iobuf::Owned;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn ordinary_start_view_replies_do_not_turn_missing_bodies_into_canonical_headers() {
        let bus = IggyMessageBus::new(0);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = sent.clone();
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        for replica in 1..3 {
            assert!(bus.owner_table().try_claim(replica, 1));
        }
        let consensus = VsrConsensus::new(1, 0, 3, 42, bus, LocalPipeline::new());
        consensus.init();
        let partition: Box<IggyPartition<IggyMessageBus>> =
            Box::new(IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(1024 * 1024),
            ));
        let mut headers: Vec<PrepareHeader> = Vec::new();
        for op in 1..=2 {
            let prepare = Message::<PrepareHeader>::new(size_of::<PrepareHeader>())
                .transmute_header(|_, header: &mut PrepareHeader| {
                    header.command = Command::Prepare;
                    header.operation = Operation::StoreConsumerOffset;
                    header.cluster = 1;
                    header.group = 42;
                    header.op = op;
                    header.parent = headers.last().map_or(0, |previous| previous.checksum);
                    header.timestamp = op;
                    header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
                    header.checksum = header.identity_checksum();
                });
            headers.push(*prepare.header());
            partition
                .log
                .journal()
                .inner
                .append(prepare.into_frozen())
                .await
                .unwrap();
        }
        let consensus = partition.consensus();
        consensus.sequencer().set_sequence(2);
        consensus.advance_commit_max(1);
        let probe = Message::<RequestStartViewHeader>::new(size_of::<RequestStartViewHeader>())
            .transmute_header(|_, header: &mut RequestStartViewHeader| {
                header.command = Command::RequestStartView;
                header.cluster = 1;
                header.replica = 1;
                header.group = 42;
                header.size = u32::try_from(size_of::<RequestStartViewHeader>()).unwrap();
                header.seal();
            });
        let actions = consensus.handle_request_start_view(PlaneKind::Partitions, probe.header());
        dispatch_partition_wire_actions::<_, _, PrepareJournal, _>(consensus, &partition, actions)
            .await;
        assert_eq!(
            sent.borrow().len(),
            1,
            "probe reply is addressed to its requester"
        );
        respond_start_view::<_, _, PrepareJournal>(consensus).await;
        assert_eq!(
            sent.borrow().len(),
            3,
            "stale-view correction reaches both backups"
        );
        let backup = Box::new(VsrConsensus::new(
            1,
            1,
            3,
            42,
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        ));
        backup.init();
        for frame in sent.borrow().iter() {
            let header_size = size_of::<StartViewHeader>();
            assert_eq!(frame.len(), header_size);
            let message =
                Message::<StartViewHeader>::try_from(Owned::copy_from_slice(frame.as_slice()))
                    .unwrap();
            backup.handle_start_view(
                PlaneKind::Partitions,
                message.header(),
                &message.as_slice()[header_size..],
            );
            assert!(!backup.view_log_is_pending());
            let suffix = build_dvc_suffix(
                backup.commit_max(),
                backup.sequencer().current_sequence(),
                |_| None,
                None,
                0..=0,
            );
            assert_eq!(
                suffix.nack_bitset(),
                1,
                "the uncommitted body is still missing"
            );
        }
        sent.borrow_mut().clear();
        headers.reverse();
        // A merge concludes a view change, so its StartView opens a newer view. View 3
        // is led by replica 0 again; another view-0 StartView at the backup's head is
        // a duplicate the backup ignores.
        dispatch_partition_wire_actions::<_, _, PrepareJournal, _>(
            consensus,
            &partition,
            vec![VsrAction::SendStartView {
                view: 3,
                op: 2,
                commit: 1,
                incarnation: 0,
                target: Some(1),
                group: 42,
                suffix: headers,
            }],
        )
        .await;
        let frames = sent.borrow();
        assert_eq!(frames.len(), 1);
        let header_size = size_of::<StartViewHeader>();
        let message =
            Message::<StartViewHeader>::try_from(Owned::copy_from_slice(frames[0].as_slice()))
                .unwrap();
        consensus::dvc_suffix_decode(&message.as_slice()[header_size..], 2, 0, 0).unwrap();
        backup.handle_start_view(
            PlaneKind::Partitions,
            message.header(),
            &message.as_slice()[header_size..],
        );
        assert!(
            backup.view_log_is_pending(),
            "a merge-concluding suffix still reaches the backup"
        );
    }

    #[compio::test]
    async fn start_view_ack_waits_for_partition_wal_completion() {
        for durability in [Durability::Replicated, Durability::Persisted] {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = std::env::temp_dir().join(format!(
                "iggy-start-view-wal-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir(&directory).unwrap();
            let consensus =
                VsrConsensus::new(1, 0, 3, 42, IggyMessageBus::new(0), LocalPipeline::new());
            consensus.init();
            consensus.mark_superblock_durable(0, 0);
            let mut partition: IggyPartition<IggyMessageBus> =
                IggyPartition::with_in_memory_storage(
                    Arc::new(PartitionStats::default()),
                    consensus,
                    IggyByteSize::from(1024 * 1024),
                );
            partition.set_partition_dir(directory.to_string_lossy().into_owned());
            partition.set_runtime_options(TopicRuntimeOptions {
                consumer_offset_durability: durability,
                ..TopicRuntimeOptions::default()
            });
            partition.open_persistence().await.unwrap();
            let prepare = Message::<PrepareHeader>::new(size_of::<PrepareHeader>())
                .transmute_header(|_, header: &mut PrepareHeader| {
                    header.command = Command::Prepare;
                    header.operation = Operation::StoreConsumerOffset;
                    header.cluster = 1;
                    header.group = 42;
                    header.op = 1;
                    header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
                    header.checksum = header.identity_checksum();
                });
            partition
                .log
                .journal()
                .inner
                .append(prepare.into_frozen())
                .await
                .unwrap();
            partition.consensus().sequencer().set_sequence(1);
            dispatch_partition_journal_actions(
                partition.consensus(),
                &partition,
                &[VsrAction::SendPrepareOk {
                    view: 0,
                    from_op: 1,
                    to_op: 1,
                    target: 0,
                    group: 42,
                }],
            )
            .await;
            let mut acknowledgments = Vec::new();
            partition
                .consensus()
                .drain_loopback_into(&mut acknowledgments);
            assert_eq!(
                acknowledgments.len(),
                usize::from(durability == Durability::Replicated),
                "replicated acks require WAL admission; persisted acks also wait for durability"
            );
            for _ in 0..100 {
                compio::runtime::time::sleep(Duration::from_millis(10)).await;
                partition.drive_persistence().await;
                partition
                    .consensus()
                    .drain_loopback_into(&mut acknowledgments);
                if !acknowledgments.is_empty() {
                    break;
                }
            }
            assert_eq!(acknowledgments.len(), 1);
            drop(partition);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}

#[cfg(test)]
mod untrusted_do_view_change_tests {
    use super::*;
    use consensus::{DvcSuffix, LocalPipeline};
    use iggy_common::{IggyByteSize, PartitionStats};
    use journal::prepare_journal::PrepareJournal;
    use message_bus::IggyMessageBus;
    use server_common::iobuf::Owned;
    use std::sync::Arc;

    const GROUP: u64 = 42;

    fn do_view_change(op: u64, untrusted_log: bool) -> VsrAction {
        VsrAction::SendDoViewChange {
            view: 1,
            target: 1,
            log_view: 0,
            op,
            commit: 0,
            group: GROUP,
            suffix: DvcSuffix::empty(),
            untrusted_log,
        }
    }

    #[compio::test]
    async fn given_untrusted_log_when_dispatching_do_view_change_should_stamp_the_flag() {
        let bus = IggyMessageBus::new(0);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = sent.clone();
        bus.set_replica_forward_fn(Box::new(move |_, _, frame| {
            captured.borrow_mut().push(frame);
            Ok(())
        }));
        for replica in 1..3 {
            assert!(bus.owner_table().try_claim(replica, 1));
        }
        let consensus = VsrConsensus::new(1, 0, 3, GROUP, bus, LocalPipeline::new());
        consensus.init();
        let partition: Box<IggyPartition<IggyMessageBus>> =
            Box::new(IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(1024 * 1024),
            ));

        for untrusted_log in [true, false] {
            sent.borrow_mut().clear();
            dispatch_partition_wire_actions::<_, _, PrepareJournal, _>(
                partition.consensus(),
                &partition,
                vec![do_view_change(0, untrusted_log)],
            )
            .await;
            let frames = sent.borrow();
            assert_eq!(frames.len(), 2, "a DoViewChange is broadcast");
            for frame in frames.iter() {
                let message = Message::<DoViewChangeHeader>::try_from(Owned::copy_from_slice(
                    frame.as_slice(),
                ))
                .expect("a dispatched DoViewChange decodes");
                assert_eq!(message.header().untrusted_log(), untrusted_log);
                assert_eq!(message.header().verify_frame(), Ok(()));
            }
        }
    }

    #[test]
    fn given_partition_awaiting_transfer_should_send_only_its_probe_and_empty_vote() {
        assert!(sendable_while_awaiting_transfer(
            &VsrAction::SendRequestStartView {
                view: 1,
                group: GROUP,
            }
        ));
        assert!(sendable_while_awaiting_transfer(&do_view_change(0, true)));
        assert!(
            !sendable_while_awaiting_transfer(&do_view_change(0, false)),
            "a trusted vote vouches for a log the partition does not have"
        );
        let flagged = |log_view: u32, commit: u64, suffix: DvcSuffix| VsrAction::SendDoViewChange {
            view: 1,
            target: 1,
            log_view,
            op: 0,
            commit,
            group: GROUP,
            suffix,
            untrusted_log: true,
        };
        let one_header = DvcSuffix::new(vec![PrepareHeader::default()], 0, 0);
        for (vote, vouches_for) in [
            (do_view_change(5, true), "a head"),
            (flagged(1, 0, DvcSuffix::empty()), "a log_view"),
            (flagged(0, 1, DvcSuffix::empty()), "a commit point"),
            (flagged(0, 0, one_header), "suffix headers"),
        ] {
            assert!(
                !sendable_while_awaiting_transfer(&vote),
                "a vote carrying {vouches_for} vouches for history"
            );
        }
        assert!(!sendable_while_awaiting_transfer(
            &VsrAction::SendStartViewChange {
                view: 1,
                group: GROUP,
            }
        ));
    }
}
