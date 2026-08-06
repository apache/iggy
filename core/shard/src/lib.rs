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
    ChunkProgress, CommitOutcome, Consensus, ConsensusClock, MetadataHandle, MuxPlane,
    PartitionsHandle, Pipeline, Plane, PlaneKind, STATE_TRANSFER_MAX_DECODE_RETRIES,
    STATE_TRANSFER_MAX_STALL_RETRIES, Sequencer, VsrAction, VsrConsensus,
    build_deny_reply_from_request_header,
};
#[cfg(any(test, feature = "simulator"))]
use crossfire::AsyncRxTrait;
use crossfire::TrySendError;
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
use partitions::state_transfer::TransferArtifact;
use partitions::{IggyPartition, IggyPartitions, PollFragments, PollingArgs, PollingConsumer};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
use server_common::{MESSAGE_ALIGN, Message, MessageBag, iobuf::Frozen};
use shards_table::ShardsTable;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
/// Derived from the BUS frame cap, not `MAX_PAYLOAD_SIZE`: server-ng never
/// enforces the latter (its only enforcement sites are the legacy server and the
/// SDK batch types), so the largest appendable batch is whatever the message bus
/// will frame. This tracks the shipped `message_bus.max_message_size` default; an
/// operator raising that is caught by the config validator, which requires
/// `partition.transfer_artifact_bytes_max` to cover `system.segment.size` plus
/// the configured bus cap.
const SEGMENT_SIZE_OVERSHOOT_BYTES: u64 = 64 * 1024 * 1024;

/// Default alloc ceiling for ONE received state-transfer artifact.
///
/// Mirrors `[partition] transfer_artifact_bytes_max`. Free const so the config
/// crate's copy can be pinned to it by a `const _: () = assert!(..)` at the
/// server-ng build edge, the way every other runtime default is.
pub const PARTITION_ARTIFACT_LEN_DEFAULT: u64 =
    SEGMENT_SIZE_CEILING_BYTES + SEGMENT_SIZE_OVERSHOOT_BYTES;

/// Default per-shard resident budget for served segment payloads
/// (`[partition] transfer_served_cache_bytes_max`). Pinned like
/// [`PARTITION_ARTIFACT_LEN_DEFAULT`].
pub const SERVED_SEGMENT_CACHE_BYTES_DEFAULT: u64 =
    SEGMENT_SIZE_CEILING_BYTES * CONCURRENT_SERVED_SEGMENTS;

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
        let floor = self.sweeps.saturating_sub(idle_sweeps_max);
        let stale: Vec<(u64, u64)> = self
            .entries
            .iter()
            .filter(|(_, cached)| cached.last_use_sweep <= floor)
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
    /// A purge unlinks the segments these payloads copy, and the cache key is
    /// the manifest checksum over the PRE-purge bytes, so nothing about a hit
    /// can notice: the serve path answers from the resident copy without
    /// touching disk, and every served chunk resets the expiry clock, so an
    /// active puller keeps purged data alive indefinitely.
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

    /// Serving-side cache of state-transfer offers, both planes, keyed by
    /// `(namespace, requester replica id)`. Bounded by the replica count times
    /// the groups this shard serves; replaced per fresh nonce.
    state_transfer_offers: RefCell<HashMap<(u64, u8), ServedStateTransfer>>,

    /// See [`ServedSegmentCache`].
    served_segment_cache: RefCell<ServedSegmentCache>,

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

    /// Namespaces holding frames [`Self::redispatch_parked_frames`] could not
    /// re-queue, for the pump to retry.
    ///
    /// Without it a re-parked frame has no exit: its namespace is materialised
    /// by then, so the sweep skips it and `reconcile_additions` stages no second
    /// `InsertOwned`. Only a topic delete would reach it. The pump drains the
    /// inbox, so a refusal usually clears on its next iteration.
    ///
    /// [`BTreeSet`] for the reason [`Self::pending_partition_frames`] is a
    /// [`BTreeMap`]: fixed-seed simulator replay needs iteration order to be a
    /// function of the namespaces alone.
    reparked_partition_namespaces: RefCell<BTreeSet<IggyNamespace>>,

    /// Set while the shard-wide budget is shedding for namespaces holding no
    /// park entry of their own, which have no [`ParkEntry::shed`] to warn once
    /// from. Cleared when the park map empties, so one episode warns once.
    shard_park_shedding: Cell<bool>,

    /// Live ceiling on prepares served per `RequestPrepares` round. Defaults
    /// to [`REPAIR_CHUNK_MAX`]; server-ng overrides it from
    /// `[cluster] repair_chunk_max` at bootstrap.
    repair_chunk_max: Cell<u64>,

    /// Live stalled-repair retry threshold in consensus ticks. Defaults to
    /// [`partitions::REPAIR_RETRY_TICKS`]; server-ng overrides it from
    /// `[cluster] repair_retry_interval` at bootstrap.
    repair_retry_ticks: Cell<u32>,

    /// Live `[partition] transfer_served_cache_bytes_max`: the byte budget for
    /// segment payloads this shard keeps resident to serve chunk requests.
    /// Defaults to [`SERVED_SEGMENT_CACHE_BYTES_DEFAULT`]; server-ng
    /// overrides it at bootstrap.
    served_segment_cache_bytes_max: Cell<u64>,

    /// Live `[partition] transfer_artifact_bytes_max`: the alloc ceiling for one
    /// RECEIVED artifact. Defaults to [`PARTITION_ARTIFACT_LEN_DEFAULT`];
    /// server-ng overrides it at bootstrap.
    partition_artifact_len_max: Cell<u64>,

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
            pending_partition_frames: RefCell::new(BTreeMap::new()),
            parked_partition_bytes: Cell::new(0),
            reparked_partition_namespaces: RefCell::new(BTreeSet::new()),
            shard_park_shedding: Cell::new(false),
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            state_transfer_offers: RefCell::new(HashMap::new()),
            served_segment_cache: RefCell::new(ServedSegmentCache::default()),
            served_segment_cache_bytes_max: Cell::new(SERVED_SEGMENT_CACHE_BYTES_DEFAULT),
            partition_artifact_len_max: Cell::new(PARTITION_ARTIFACT_LEN_DEFAULT),
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
            pending_partition_frames: RefCell::new(BTreeMap::new()),
            parked_partition_bytes: Cell::new(0),
            reparked_partition_namespaces: RefCell::new(BTreeSet::new()),
            shard_park_shedding: Cell::new(false),
            metadata_repair: RefCell::new(None),
            metadata_transfer: RefCell::new(None),
            state_transfer_offers: RefCell::new(HashMap::new()),
            served_segment_cache: RefCell::new(ServedSegmentCache::default()),
            served_segment_cache_bytes_max: Cell::new(SERVED_SEGMENT_CACHE_BYTES_DEFAULT),
            partition_artifact_len_max: Cell::new(PARTITION_ARTIFACT_LEN_DEFAULT),
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

    /// Re-drive the re-dispatch for namespaces whose frames the inbox refused.
    ///
    /// Runs on the pump, wherever [`Self::apply_reconcile_ops`] does, so it
    /// fires right after a frame was consumed and a slot freed. Only another
    /// refusal puts a namespace back, so the set empties itself.
    ///
    /// Epoch comes from the routing row, which `InsertOwned` writes alongside
    /// the partition. Skipped when the row is gone or the namespace is fenced;
    /// teardown does both, and the reconciler sweep retires the frames.
    fn retry_reparked_frames(&self) {
        let pending: Vec<IggyNamespace> = {
            let mut reparked = self.reparked_partition_namespaces.borrow_mut();
            if reparked.is_empty() {
                return;
            }
            std::mem::take(&mut *reparked).into_iter().collect()
        };
        let partitions = self.plane.partitions();
        for namespace in pending {
            if partitions.is_tombstoned(&namespace) {
                continue;
            }
            let Some(epoch) = self.shards_table.epoch_for(namespace) else {
                continue;
            };
            self.redispatch_parked_frames(namespace, epoch);
        }
    }

    /// Drain and apply staged [`ReconcileOp`]s on the pump task.
    /// Synchronous: every arm is in-memory only. `ConfirmRemove`'s fsync +
    /// blocking close is offloaded to a detached task so the pump doesn't
    /// stall on bulk teardown.
    pub fn apply_reconcile_ops(&self)
    where
        B: MessageBus + 'static,
    {
        // Ahead of the staged ops and outside their empty-queue early return: a
        // re-parked frame waits on inbox capacity, not on a reconcile op.
        self.retry_reparked_frames();
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

/// The serving replica's `(view, commit_max)` for a descriptor.
///
/// Sampled per branch, always AFTER any offer build: the build force-flushes and
/// hashes every un-memoized segment (seconds on a first multi-GiB serve) while
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
/// side's caught-up-primary gate, so rotating by ring index alone can spend a
/// full backoff round on a backup that must refuse -- and, worse, can land on a
/// phantom view-0 primary of an empty group. Falls back to walking the ring past
/// the failed peer, skipping this replica; a cluster of two has no alternative
/// and retries the same peer.
///
/// `failed_peer` and `primary` are both bounded by `replica_count` at their
/// ingress, which is what keeps the `+ 1` here from wrapping a peer id of 255
/// onto replica 0.
const fn next_transfer_peer(self_id: u8, failed_peer: u8, replica_count: u8, primary: u8) -> u8 {
    if replica_count <= 1 {
        return failed_peer;
    }
    if primary != self_id && primary != failed_peer {
        return primary;
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
    /// read-timeout. Replicated traffic is dropped, recovered by retransmit.
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
    /// Never expires a replicated prepare: no client to answer, and
    /// `consensus::retransmit_targets` skips an op that already reached quorum,
    /// so expiry is silent permanent loss. Byte budgets bound those instead.
    ///
    /// Bounds RESIDENCY, not staleness. The SDK replays the identical request
    /// for the rest of its response timeout, so an absolute-offset
    /// `StoreConsumerOffset` rewinds the group on the replay anyway. What it
    /// buys: a buffer that cannot grow without limit, and a client that learns
    /// the outcome from a reply rather than a timeout.
    passes: u32,
    message: Message<GenericHeader>,
}

impl ParkedFrame {
    fn footprint(&self) -> usize {
        parked_footprint(self.message.as_slice().len())
    }

    /// No client on this node: nothing to answer, nothing recovers it.
    fn is_replicated(&self) -> bool {
        self.message.header().command != Command2::Request
    }
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
/// empty entry a larger frame could never park at all, and for a prepare that is
/// unrecoverable loss: `consensus::retransmit_targets` skips an op that already
/// reached quorum. Shipped `message_bus.max_message_size` is 64 MiB, so an
/// ordinary batched append exceeds this. Cost of the waiver is one convergence
/// window of shard budget; cost of the loss is the replica.
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
        B: MessageBus + 'static,
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
                    ParkOutcome::Tombstoned(request) | ParkOutcome::Overflow(request) => {
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

    /// Discard every frame parked under a namespace this shard can never serve:
    /// gone from committed metadata, mid-teardown, or not hashing here. Client
    /// requests get a transient deny, not silence; transports decode replies in
    /// lockstep, so silence wedges the connection until the SDK read-timeout.
    ///
    /// The one retirement path a prepare still travels. It is retained
    /// everywhere else (see [`ParkedFrame::passes`]); here the namespace itself
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

    /// Remove a namespace's entry, debiting [`Self::parked_partition_bytes`] and
    /// disarming the pump retry. Single place an entry leaves the map, so
    /// neither can drift out of step with it.
    fn take_parked_partition_frames(&self, namespace: IggyNamespace) -> Option<Vec<ParkedFrame>> {
        self.reparked_partition_namespaces
            .borrow_mut()
            .remove(&namespace);
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
            if self.deny_parked_client_request(frame) {
                answered += 1;
            } else {
                dropped += 1;
                self.metrics.record_frame_drop(
                    crate::metrics::frame_drop_variant::PARTITION,
                    crate::metrics::frame_drop_reason::PARK_DROPPED,
                );
            }
        }
        (answered, dropped)
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

    /// Re-queue the frames parked for `namespace` now that its partition exists
    /// at `epoch`, onto this shard's own inbox so the pump serves them after the
    /// current drain.
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
    /// it would be dropped with no recovery until an unrelated view change.
    /// The residual is unchanged from before the stamp existed -- a frame parked
    /// while the namespace was absent, then recreated under a new incarnation,
    /// is served against the replacement -- and closing it needs a wire-level
    /// discriminator (see the `TODO(krishna)` in
    /// `partition_reconciler`'s module docs), not a `None`-means-stale rule.
    ///
    /// A frame the inbox refuses is re-parked: retained, so not counted as a
    /// drop. Re-queuing appends, so a pass materialising many namespaces can
    /// overrun the inbox; staging a deny is futile because it rides the same
    /// sender with no await in between. One namespace alone can now do it, since
    /// [`MAX_PARKED_BYTES_PER_NAMESPACE`] admits 1024 header-only prepares
    /// against a default `inbox_capacity` of 1024. That costs other namespaces a
    /// later convergence, not a frame. The first `Full` ends the loop, since
    /// the sole consumer of `senders[self.id]` is the pump task running this
    /// call and no later frame can find a slot the first one could not.
    ///
    /// [`MAX_PARKED_PASSES`] does not bound a re-parked frame: the sweep ages a
    /// namespace only while un-materialised, and by here it is materialised.
    /// [`Self::repark_partition_frames`] arms the pump retry instead;
    /// `partition_reconciler::reconcile_parked_frames` is the backstop for an
    /// inbox that never drains.
    fn redispatch_parked_frames(&self, namespace: IggyNamespace, epoch: u64)
    where
        B: MessageBus + 'static,
    {
        let Some(frames) = self.take_parked_partition_frames(namespace) else {
            return;
        };
        tracing::debug!(
            shard = self.id,
            namespace_raw = namespace.inner(),
            count = frames.len(),
            epoch,
            "re-dispatching parked partition frames after materialisation"
        );
        // Incarnation filter first, independent of the sender: a prior
        // incarnation is rejected whether or not this shard can re-queue.
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
        let Some(sender) = self.senders.get(self.id as usize) else {
            self.retire_parked_frames(servable);
            return;
        };
        let mut refused_frames: Vec<ParkedFrame> = Vec::new();
        let mut remaining = servable.into_iter();
        while let Some(frame) = remaining.next() {
            let passes = frame.passes;
            let parked_epoch = frame.epoch;
            let Err(error) = sender.try_send(ShardFrame::consensus(self.id, frame.message)) else {
                continue;
            };
            let (refused, disconnected) = match error {
                TrySendError::Full(frame) => (frame, false),
                TrySendError::Disconnected(frame) => (frame, true),
            };
            let ShardFrame::Consensus { message, .. } = refused else {
                unreachable!("try_send returns the frame it was handed");
            };
            let refused_frame = ParkedFrame {
                epoch: parked_epoch,
                passes,
                message,
            };
            if disconnected {
                // Pump gone: re-parking holds the frame until process exit, and
                // every later send hits the same dead channel.
                self.metrics.record_frame_drop(
                    crate::metrics::frame_drop_variant::PARTITION,
                    crate::metrics::frame_drop_reason::DISCONNECTED,
                );
                tracing::warn!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    "re-dispatch of parked partition frames refused: inbox disconnected"
                );
                let mut stranded = vec![refused_frame];
                stranded.extend(remaining);
                self.retire_parked_frames(stranded);
                return;
            }
            refused_frames.push(refused_frame);
            refused_frames.extend(remaining);
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                count = refused_frames.len(),
                passes,
                "re-parking parked partition frames: inbox full"
            );
            break;
        }
        if !refused_frames.is_empty() {
            self.repark_partition_frames(namespace, refused_frames);
        }
    }

    /// Put frames back under `namespace` after a refused re-dispatch, keeping
    /// [`Self::parked_partition_bytes`] in step and arming the pump-side retry.
    ///
    /// Deliberately not budget-checked: these bytes were already counted while
    /// parked, so re-admitting them cannot grow the total past what it held a
    /// moment ago, and shedding here would answer a frame the inbox merely
    /// deferred.
    ///
    /// Arming [`Self::reparked_partition_namespaces`] is what makes it a
    /// deferral. Every other exit is closed once materialised: the sweep only
    /// ages a namespace it has not built, and `reconcile_additions` stages no
    /// second `InsertOwned` for one already in `IggyPartitions`.
    fn repark_partition_frames(&self, namespace: IggyNamespace, frames: Vec<ParkedFrame>) {
        let restored: usize = frames.iter().map(ParkedFrame::footprint).sum();
        let mut pending = self.pending_partition_frames.borrow_mut();
        let entry = pending.entry(namespace).or_default();
        for frame in frames {
            entry.push(frame);
        }
        drop(pending);
        self.parked_partition_bytes
            .set(self.parked_partition_bytes.get().saturating_add(restored));
        self.reparked_partition_namespaces
            .borrow_mut()
            .insert(namespace);
    }

    /// Age every frame under `namespace` by one pass, answering CLIENT REQUESTS
    /// past [`MAX_PARKED_PASSES`]. Returns the number answered.
    ///
    /// Prepares age but never expire. Expiry destroys a committed op with
    /// nothing to recover it (see [`ParkedFrame::passes`]), and passes are
    /// commit-driven: a non-empty buffer defeats the reconciler fast-skip, so a
    /// create burst elapses four in milliseconds, across every parked namespace
    /// rather than the one it concerns. Byte budgets bound them instead. Only
    /// [`Self::discard_parked_partition_frames`] still destroys a prepare.
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
                // Through the shared remover so the pump-retry set is disarmed
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

    /// Retire a frame that will never be served: a client request gets a
    /// transient deny, replicated traffic is destroyed. Returns `true` only when
    /// a reply reached the pump.
    ///
    /// Callers are synchronous (`apply_reconcile_ops`, the reconciler sweep), so
    /// the deny rides the pump's lifecycle path, not an inline bus send. A shard
    /// with no sender stages nothing, hence forwarding
    /// [`Self::stage_transient_deny`]'s verdict rather than assuming success.
    ///
    /// No reply must not mean no record: the primary retransmits only what has
    /// not reached quorum, so a destroyed prepare is invisible loss. The `false`
    /// return is what makes callers bump
    /// `frame_drops_total{variant=partition,reason=park_dropped}`.
    fn deny_parked_client_request(&self, frame: ParkedFrame) -> bool {
        if frame.message.header().command == Command2::Request
            && let Ok(request) = frame.message.try_into_typed::<RequestHeader>()
        {
            return self.stage_transient_deny(request.header());
        }
        false
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
        self.deny_parked_client_request(frame);
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
    /// `ReconcileOp::InsertOwned` lands, and only if the epoch stamped here
    /// still matches (see [`ParkedFrame`]); a full buffer reports
    /// [`ParkOutcome::Overflow`] so the caller can answer rather than shed
    /// silently.
    fn park_if_unmaterialised<H>(
        &self,
        message: Message<H>,
        operation: Operation,
        namespace_raw: u64,
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
        // can tell it apart from a same-key replacement.
        let epoch = self
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace);
        let frame_cost = parked_footprint(message.as_slice().len());
        let replicated = message.header().command() != Command2::Request;
        let mut pending = self.pending_partition_frames.borrow_mut();
        let parked_bytes = self.parked_partition_bytes.get();
        // Read the entry without `entry().or_default()`: inserting first would
        // leave an empty entry behind on the overflow path below, which reads as
        // a parked namespace to the reconciler sweep and its fast-skip guard.
        let existing = pending.get_mut(&namespace);
        let parked_len = existing.as_ref().map_or(0, |entry| entry.frames.len());
        let namespace_bytes = existing.as_ref().map_or(0, |entry| entry.bytes);
        // A prepare is never shed on a byte budget. No client to answer, and no
        // recovery: `consensus::retransmit_targets` skips an op that already
        // reached quorum and the plane opens a repair session only in
        // `on_start_view`, so shedding one is permanent loss where shedding a
        // request costs a retry. A request is refused the moment admitting it
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
            passes: 0,
            message: message.into_generic(),
        });
        drop(pending);
        self.parked_partition_bytes
            .set(parked_bytes.saturating_add(frame_cost));
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
            tracing::warn!(
                shard = self.id,
                client = request_header.client,
                operation = ?request_header.operation,
                error = %error,
                "failed to send transient deny for partition request"
            );
            return;
        }
        self.metrics.record_partition_request_denied_transient();
    }

    /// [`Self::deny_partition_request_transient`] for synchronous callers:
    /// hand the deny to this shard's own pump as a
    /// [`LifecycleFrame::ForwardClientSend`], whose handler performs the bus
    /// send (same funnel the parked-frame re-dispatch uses).
    ///
    /// Returns whether the pump took it. A shard with no sender stages nothing,
    /// so assuming success logs an answer for a request destroyed unanswered.
    fn stage_transient_deny(&self, request_header: &RequestHeader) -> bool {
        let reply = build_deny_reply_from_request_header(
            request_header,
            IggyError::TransientNotAccepted.as_code(),
        );
        let frame = ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
            client_id: request_header.client,
            msg: reply.into_generic().into_frozen(),
        });
        let Some(sender) = self.senders.get(self.id as usize) else {
            return false;
        };
        // Count only what was actually handed to the pump: crediting before the
        // send reports an answer to a client that never received one, which is
        // the opposite of what this counter is read for.
        if let Err(error) = sender.try_send(frame) {
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
    /// `superblock` is this group's durable `(view, log_view)` store. Passing
    /// `None` keeps the storeless branch, where the persist gate marks every view
    /// durable without writing anything -- fine for specs that never restart a
    /// replica, but it means the gate itself, its write-failure fence, and view
    /// recovery are all unexercised. A caller that hands one in (the simulator,
    /// which retains the store across a replica rebuild) gets the production
    /// contract: a recorded view is restored before the group joins, and a failed
    /// write withholds every view-scoped send.
    ///
    /// `recovered_state` is that store's last record, read by the caller (the
    /// store's read is async and this is not), mirroring how `new_shard` takes the
    /// metadata plane's.
    #[cfg(any(test, feature = "simulator"))]
    pub fn init_partition(
        &self,
        namespace: IggyNamespace,
        superblock: Option<Rc<SB>>,
        recovered_state: Option<consensus::VsrState>,
    ) where
        B: MessageBus + Clone,
    {
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
        // Recorded view first, exactly as the two boot paths order it: restoring
        // after `init` would advertise a view older than the recorded one.
        if let Some(state) = recovered_state {
            consensus.set_view(state.view);
            consensus.set_log_view(state.log_view);
            consensus.mark_superblock_durable(state.view, state.log_view);
        }
        consensus.init();

        let stats = Arc::new(PartitionStats::default());
        let mut partition = IggyPartition::with_in_memory_storage(
            stats,
            consensus,
            partitions.config().segment_size,
            partitions.config().enforce_fsync,
        );
        if let Some(superblock) = superblock {
            partition.set_superblock(superblock);
        }
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
            let (local_actions, wire_actions) = split_local_actions(actions);
            dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &local_actions).await;
            if planes.0.persist_superblock_if_needed(consensus).await {
                dispatch_vsr_actions(consensus, planes.0.journal.as_ref(), &wire_actions).await;
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
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &wire_actions).await;
            dispatch_partition_journal_actions(consensus, partition, &wire_actions).await;
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
            header.namespace,
            header.view,
            header.replica,
            "DoViewChange",
        ) else {
            return;
        };
        let consensus = partition.consensus();
        let actions = consensus.handle_do_view_change(PlaneKind::Partitions, &header);
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &wire_actions).await;
            dispatch_partition_journal_actions(consensus, partition, &wire_actions).await;
        }
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

    #[allow(clippy::future_not_send)]
    #[allow(clippy::too_many_lines)]
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

        let config = planes.1.0.config();
        // Counted BEFORE the `&mut partition` below exists: the scan takes
        // shared borrows of every partition (see `arm_partition_transfer`).
        // Gated on the arm actually being possible, so a stale or misdirected
        // frame -- and every StartView for a group that is not awaiting a
        // transfer, which is all of them during an ordinary view change -- does
        // not pay a node-wide scan. (A shard-level counter would remove the scan
        // entirely, but `IggyPartition::transfer` is `pub` and cleared inside the
        // partitions crate, so an externally maintained count would drift; that
        // refactor is a prerequisite, not a detail.)
        let transfers_inflight = if Self::may_arm_partition_transfer(&planes.1.0, header.namespace)
        {
            self.partition_transfers_inflight()
        } else {
            0
        };
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
        let adopted = !actions.is_empty();
        let (local_actions, wire_actions) = split_local_actions(actions);
        // Locals go to the partition dispatcher ONLY: `RebuildPipeline`
        // executes there (`dispatch_vsr_actions` bails on `journal: None`)
        // and `CommitJournal` is a no-op in both.
        dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &wire_actions).await;
            dispatch_partition_journal_actions(consensus, partition, &wire_actions).await;
        }
        // Gate on actual adoption: a rejected StartView returns no actions,
        // and re-arming on one would re-mint the nonce and drop an in-flight
        // descriptor.
        if adopted
            && partition.consensus().state_transfer_stage()
                == consensus::StateTransferStage::AwaitingTarget
        {
            tracing::info!(
                shard = self.id,
                namespace_raw = header.namespace,
                peer = header.replica,
                "adopted a live view while awaiting transfer; requesting partition state transfer"
            );
            // The announcing replica becomes `session.peer`, which the re-arm
            // path feeds to `next_transfer_peer`'s ring arithmetic, so an id
            // outside the cluster must not get that far.
            if self.peer_is_known(header.replica, "StartView") {
                let _ = self
                    .arm_partition_transfer(partition, header.replica, transfers_inflight)
                    .await;
            }
            return;
        }
        // A commit walk during Fetching can advance commit_min past the
        // incoming frontier (or trip the install's anti-rewind refusal), so
        // gate on the whole transfer, not one stage.
        if partition.consensus().is_transferring() {
            return;
        }
        // Outside the gate: the persist fences the SEND, not the local commit
        // walk or the repair fetch below (a fetch asks to LEARN, it does not
        // advertise this replica's view).
        if local_actions
            .iter()
            .any(|action| matches!(action, VsrAction::CommitJournal))
        {
            partition.commit_journal(config).await;
        }
        // Same gap-fill as the metadata arm: a journal-less rejoiner that
        // adopted the new view still lacks the window's entries; repair from
        // the announcing primary, floor settled by its RangeEvicted. The shared
        // helper carries one guard more than this site needs (`is_transferring`,
        // already covered by the early return above) and logs the arm.
        self.maybe_request_partition_repair(partition, header.replica)
            .await;
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
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
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
        if partition.persist_superblock_if_needed().await {
            dispatch_vsr_actions::<B, _, MJ>(consensus, None, &wire_actions).await;
            dispatch_partition_journal_actions(consensus, partition, &wire_actions).await;
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
        // `None` means the journal holds NOTHING, not "nothing was evicted":
        // the partition journal is memory-only and `clear_all` wipes the
        // evicted ring with it, so a freshly installed or freshly restarted
        // peer answers `None` for every op it once had. Reading that as "no
        // eviction" served a bare `RepairDone`, left the requester's floor at
        // `None`, and `FloorRefused` -- the ONLY route that arms a partition
        // state transfer -- never fired: a lagging replica on an idle
        // partition spun repair forever against a peer-sticky retry. An empty
        // journal instead reports eviction from the commit frontier, which
        // refuses the floor into a transfer (the empty window passes the
        // completeness check) and heals in one round.
        let retained_from = partition
            .log
            .journal()
            .inner
            .repair_retained_from()
            .unwrap_or_else(|| partition.consensus().commit_min().saturating_add(1));
        let mut from_op = header.from_op;
        if retained_from > from_op {
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
        // Legacy acceptance: pre-upgrade metadata WAL entries were journaled
        // before prepares stamped `consensus.namespace()`, and repair ships
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
        let legacy_metadata_claim = header.namespace == 0 && metadata_plane_op;
        if let Some(ref consensus) = planes.0.consensus
            && (consensus.namespace() == header.namespace || legacy_metadata_claim)
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
        // A metadata-plane op that did not match above (no metadata consensus on
        // this shard, or a namespace neither plane claims) is DROPPED, never
        // offered to the partition arm. Falling through let a metadata prepare
        // reach `apply_repaired_prepare`: it journals nothing, but it resets the
        // partition repair session's idle ticks (masking a genuine stall) and
        // carries the metadata prepare's checksum into the partition consensus
        // via `set_last_prepare_checksum` -- inert only while prepare checksums
        // are structurally zero, and a cross-plane parent stamp the moment the
        // checksum chain is activated (see the note in `consensus::impls`).
        if metadata_plane_op {
            tracing::debug!(
                shard = self.id,
                op = header.op,
                operation = ?header.operation,
                namespace_raw = header.namespace,
                "dropping a metadata-plane repair prepare this shard cannot journal"
            );
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
        // Counted BEFORE the `&mut partition` below exists, and only when an arm
        // is possible at all: see the StartView site for why the scan is gated
        // rather than replaced with a counter.
        let transfers_inflight = if Self::may_arm_partition_transfer(&planes.1.0, header.namespace)
        {
            self.partition_transfers_inflight()
        } else {
            0
        };
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
                            namespace_raw = header.namespace,
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
                            namespace_raw = header.namespace,
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
            h.command = Command2::StateTransferTarget;
            h.cluster = cluster;
            h.replica = self_id;
            h.nonce = nonce;
            h.namespace = namespace;
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
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
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
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.namespace() == header.namespace);
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
            .get_mut(&(header.namespace, header.replica))
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
                header.namespace,
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
                    TransferDescriptor::available(
                        &offer.manifest(),
                        offer.commit_op,
                        consensus.view(),
                        consensus.commit_max(),
                    ),
                )
                .await;
                self.state_transfer_offers.borrow_mut().insert(
                    (header.namespace, header.replica),
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
                    "cannot serve metadata state transfer; requester falls back"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.namespace,
                    TransferDescriptor::unavailable(
                        false,
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
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.namespace() == header.namespace);
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
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.namespace() == header.namespace);
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
                .get_mut(&(header.namespace, header.replica))
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
                    header.namespace,
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
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: RestorableMetadataStm,
        T: ShardsTable,
    {
        let header = *msg.header();
        let planes = self.plane.inner();
        let metadata_frame = planes
            .0
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.namespace() == header.namespace);
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
    #[allow(clippy::too_many_lines)]
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

        // Pre-pass: issue every group's pending superblock persist
        // CONCURRENTLY. A cluster-wide view change makes every group on
        // this shard need one in the same tick, and each `atomic_replace`
        // is a create + write + 2 fsyncs; run serially, a few hundred
        // groups on ordinary storage exceed the 5s view-change escalation
        // and loop elections. The persists are independent (each group owns
        // its store, lock, and failure bookkeeping, all behind `&self`),
        // and the per-group loop below re-checks the gate on its lock-free
        // fast path, so gating semantics are unchanged.
        let pending_persists: Vec<_> = namespaces
            .iter()
            .copied()
            .filter(|namespace| {
                partitions
                    .get_by_ns(namespace)
                    .is_some_and(|partition| partition.consensus().needs_superblock_persist())
            })
            .map(|namespace| async move {
                if let Some(partition) = partitions.get_by_ns(&namespace) {
                    let _ = partition.persist_superblock_if_needed().await;
                }
            })
            .collect();
        // Capped fan-out: each persist is a create + write + 2 fsyncs, and a
        // node-wide view change over many partitions must not dump an
        // unbounded fd/fsync burst onto the reactor in one tick.
        let mut pending_persists = pending_persists.into_iter();
        loop {
            let chunk: Vec<_> = pending_persists.by_ref().take(16).collect();
            if chunk.is_empty() {
                break;
            }
            futures::future::join_all(chunk).await;
        }

        // Counted at most ONCE per sweep and only if a re-arm actually fires,
        // then tracked locally as arms land. Counting per namespace is a full
        // scan per partition, so with per-partition groups the sweep would be
        // O(P^2) exactly when every group is re-arming at once (node-wide view
        // change or rejoin) -- and counting eagerly every tick pays that scan on
        // every quiet tick too, since the re-arm branch is rare. A slot freed
        // mid-sweep is seen on the next tick, the same latency a capped arm
        // already accepts.
        let mut transfers_inflight: Option<usize> = None;

        for namespace in namespaces {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                continue;
            };

            let consensus = partition.consensus();
            let actions = consensus.tick(PlaneKind::Partitions);
            // The tick emits view-scoped sends (heartbeats, view-change
            // retransmits), so it persists first like every dispatch site;
            // it is also what retries a persist an earlier site withheld on.
            let (local_actions, wire_actions) = split_local_actions(actions);
            // Locals to the partition dispatcher only; see the view-change
            // sites for the rationale.
            dispatch_partition_journal_actions(consensus, partition, &local_actions).await;
            if partition.persist_superblock_if_needed().await {
                dispatch_vsr_actions::<B, _, MJ>(consensus, None, &wire_actions).await;
                dispatch_partition_journal_actions(consensus, partition, &wire_actions).await;
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

    /// Whether this shard may build a partition offer for `namespace` without
    /// pushing the served-payload working set past its byte budget.
    ///
    /// Counts DISTINCT groups rather than requesters: the payload cache is
    /// content-addressed, so every requester pulling one group's offer shares
    /// one resident copy, and it is the group count that decides how many
    /// segments must be resident at once. A group already being served always
    /// passes, so admission cannot revoke a transfer midway.
    ///
    /// The cap tracks the configured budget rather than a constant, so an
    /// operator who raises `transfer_served_cache_bytes_max` gets the
    /// concurrency it pays for; at least one is always admitted, since refusing
    /// every rejoin is worse than re-reading for a single one.
    fn partition_transfer_admission_cap(&self) -> usize {
        let slots = self.served_segment_cache_bytes_max.get() / SEGMENT_SIZE_CEILING_BYTES;
        usize::try_from(slots).unwrap_or(usize::MAX).max(1)
    }

    fn may_serve_another_partition_transfer(&self, namespace: u64) -> bool {
        let offers = self.state_transfer_offers.borrow();
        let mut served: Vec<u64> = offers
            .iter()
            .filter(|(_, served)| matches!(served.offer, ServedOffer::Partition(_)))
            .map(|((offer_namespace, _), _)| *offer_namespace)
            .collect();
        if served.contains(&namespace) {
            return true;
        }
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
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
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
            .get_mut(&(header.namespace, header.replica))
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
                    namespace_raw = header.namespace,
                    requester = header.replica,
                    "re-answering a partition state transfer request from the offer \
                     already served"
                );
                Some(offer)
            }
            None if !self.may_serve_another_partition_transfer(header.namespace) => {
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
                    namespace_raw = header.namespace,
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
                    header.namespace,
                    TransferDescriptor::unavailable(true, view, commit_max),
                )
                .await;
                return;
            }
            None => match partition.state_transfer_offer(&config).await {
                Ok(offer) => {
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = header.namespace,
                        requester = header.replica,
                        commit_op = offer.commit_op,
                        artifacts = offer.artifact_count(),
                        total_len = offer.total_len(),
                        "serving partition state transfer"
                    );
                    self.state_transfer_offers.borrow_mut().insert(
                        (header.namespace, header.replica),
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
                    let transient = reason.transient();
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = header.namespace,
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
                        header.namespace,
                        TransferDescriptor::unavailable(transient, view, commit_max),
                    )
                    .await;
                    return;
                }
            },
        };
        let Some(offer) = offer else {
            return;
        };
        // Sampled AFTER any build: that build force-flushes and hashes every
        // un-memoized segment (seconds on a first multi-GiB serve) while reading
        // `commit_op` post-flush, so a pre-build sample could advertise a
        // `commit_max` below the descriptor's own `commit_op` -- which only
        // makes the receiver's gate refuse, and refusals feed a backoff.
        let (view, commit_max) = serving_progress(partition);
        self.send_state_transfer_target(
            cluster,
            self_id,
            header.replica,
            header.nonce,
            header.namespace,
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
        let Some(partition) = planes
            .1
            .0
            .get_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        let cluster = partition.consensus().cluster();
        let self_id = partition.consensus().replica();
        let serving_view = partition.consensus().view();
        let serving_commit_max = partition.consensus().commit_max();
        let chunk_len_max = self.state_chunk_len_max();
        let reply = loop {
            let attempt = 'attempt: {
                let mut offers = self.state_transfer_offers.borrow_mut();
                let served = offers
                    .get_mut(&(header.namespace, header.replica))
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
                            .get(header.namespace, source.entry.checksum)
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
                        namespace_raw = header.namespace,
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
                        h.command = Command2::StateChunk;
                        h.cluster = cluster;
                        h.replica = self_id;
                        h.nonce = header.nonce;
                        h.namespace = header.namespace;
                        h.artifact = header.artifact;
                        h.offset = header.offset;
                        h.size = total_size as u32;
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
                                header.namespace,
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
                        namespace_raw = header.namespace,
                        artifact = header.artifact,
                        path = %log_path,
                        transient,
                        %reason,
                        "cannot serve the requested segment; evicting the offer"
                    );
                    self.state_transfer_offers
                        .borrow_mut()
                        .remove(&(header.namespace, header.replica));
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
                    namespace_raw = header.namespace,
                    requester = header.replica,
                    transient,
                    "partition chunk request for an unknown offer; telling requester to restart"
                );
                self.send_state_transfer_target(
                    cluster,
                    self_id,
                    header.replica,
                    header.nonce,
                    header.namespace,
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
                    namespace_raw = header.namespace,
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

    /// Alloc cap for the `CONSUMER_OFFSETS` artifact, which accumulates whole
    /// in `ArtifactProgress::buf` before decode can reject it. Its decoder
    /// ceilings imply ~24 MiB (two sections of 2^20 12-byte entries); this
    /// leaves headroom without letting a hostile manifest stage gigabytes.
    const CONSUMER_OFFSETS_ARTIFACT_LEN_MAX: u64 = 32 << 20;

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
    /// describes -- retention cleaning, a committed truncate, a purge. None of
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

    /// Fence one partition for rebuild: quarantine its segment files, drop it
    /// from routing, and queue the retirement the reconciler re-materialises
    /// from committed metadata.
    ///
    /// Used wherever a partition is left without a serviceable segment chain (a
    /// failed state-transfer install whose convergence also failed, a purge that
    /// could not plant its replacement segment): the next append or poll would
    /// panic on `active_segment()`'s expect.
    ///
    /// `IggyPartitions` mandates external removals go through `ConfirmRemove` --
    /// a direct `remove()` would invalidate the `&mut` the caller still holds --
    /// but the tombstone and the routing row drop SYNCHRONOUSLY here, because
    /// the tombstone is the only gate in `get_mut_by_ns` and the queue does not
    /// drain until the end of the pump iteration.
    #[allow(clippy::future_not_send)]
    async fn fence_partition_for_rebuild(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) where
        B: MessageBus + 'static,
        T: ShardsTable,
    {
        // BEFORE the quarantine: it moves away the segments that are this
        // partition's only other witness to the offset frontier, and the
        // rebuild's sole anchor is then the durable record. Advancing form --
        // the live counter is what the rebuild must not fall below.
        partition.persist_offset_frontier().await;
        match partition.quarantine_partition_dir().await {
            Ok(Some(fenced_dir)) => tracing::error!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                fenced_dir,
                "quarantined the fenced partition's segment files; they are kept for \
                 inspection and never read again"
            ),
            Ok(None) => {}
            Err(error) => {
                // NO rebuild: `build_partition_fresh` plants segment 0 with
                // `file_exists = false`, truncating whatever the failed
                // quarantine left, so a rebuild here eats the chain one segment
                // per attempt. Tombstone and stop -- the bytes stay for an
                // operator, and the boot path makes the same call. The
                // partition stays unreachable until it is dealt with; that is
                // the intended fence, not a wait.
                tracing::error!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    %error,
                    "failed to quarantine the fenced partition's segment files; leaving it \
                     tombstoned rather than rebuilding over them"
                );
                self.plane.partitions().tombstone(namespace);
                self.shards_table.remove(&namespace);
                return;
            }
        }
        self.plane.partitions().tombstone(namespace);
        self.shards_table.remove(&namespace);
        self.enqueue_reconcile_op(ReconcileOp::ConfirmRemove { namespace });
        self.signal_reconcile_wake();
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
        if partition.transfer.is_none()
            && transfers_inflight >= Self::PARTITION_TRANSFERS_INFLIGHT_MAX
        {
            tracing::info!(
                shard = self.id,
                namespace_raw = partition.consensus().namespace(),
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
    #[allow(clippy::future_not_send)]
    async fn maybe_request_partition_repair(&self, partition: &mut IggyPartition<B, SB>, peer: u8)
    where
        B: MessageBus,
    {
        let consensus = partition.consensus();
        if !consensus.is_normal()
            || consensus.is_transferring()
            || consensus.commit_min() >= consensus.commit_max()
            || partition.repair.is_some()
        {
            return;
        }
        let nonce = iggy_common::random_id::get_uuid();
        let from_op = consensus.commit_min() + 1;
        let to_op = consensus.commit_max();
        let cluster = consensus.cluster();
        let self_id = consensus.replica();
        let namespace = consensus.namespace();
        partition.repair = Some(partitions::RepairSession {
            nonce,
            to_op,
            floor: None,
            peer,
            first_batch_offset: None,
            idle_ticks: 0,
        });
        tracing::info!(
            shard = self.id,
            namespace_raw = namespace,
            from_op,
            to_op,
            "partition behind the group frontier; requesting repair"
        );
        self.send_request_prepares(cluster, self_id, peer, nonce, from_op, to_op, namespace)
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
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        let session_matches = partition
            .transfer
            .as_ref()
            .is_some_and(|session| session.nonce == header.nonce);
        if !session_matches {
            return;
        }
        if header.available == 0 {
            // A refusal the peer marked transient (it is momentarily not the
            // caught-up primary, which `is_caught_up_primary` makes frequent
            // under produce load) must not charge the consecutive-failure count:
            // that count is reset only by a completed install, so ten routine
            // refusals pin the re-arm backoff at its 1024x ceiling while nothing
            // else recovers the partition -- repair keeps hitting the refused
            // floor and will not arm while a re-arm is pending. A hard refusal
            // (unreadable segment, failed flush) still charges.
            let transient = header.unavailable_transient == 1;
            tracing::info!(
                shard = self.id,
                namespace_raw = header.namespace,
                peer = header.replica,
                transient,
                "partition transfer peer cannot serve; backing off before re-arming"
            );
            if transient {
                self.rearm_partition_transfer_after_refusal(partition, header.replica)
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
        if header.view < local_view || header.commit_max < local_commit_max {
            tracing::warn!(
                shard = self.id,
                namespace_raw = header.namespace,
                peer = header.replica,
                serving_view = header.view,
                serving_commit_max = header.commit_max,
                local_view,
                local_commit_max,
                "refusing a partition transfer offer from a replica behind this one"
            );
            self.rearm_partition_transfer_after_refusal(partition, header.replica)
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
                    namespace_raw = header.namespace,
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
                namespace_raw = header.namespace,
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
                namespace_raw = header.namespace,
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
        self.on_partition_transfer_progress(header.namespace).await;
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
            .get_mut_by_ns(&IggyNamespace::from_raw(header.namespace))
        else {
            return;
        };
        {
            let Some(session) = partition.transfer.as_mut() else {
                return;
            };
            if session.nonce != header.nonce || !session.target_accepted {
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
        self.on_partition_transfer_progress(header.namespace).await;
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

        // Everything present: verify + decode the offsets artifact, install.
        let Some(session) = partition.transfer.take() else {
            return;
        };
        // `commit_op`, NOT a "generation": in this file that word means the
        // committed PURGE generation, and the callee's parameter is `commit_op`.
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
        // `applied_frontier`, while nothing else ever reads that frontier back.
        if let Some(frontier) = offsets_frontier
            && frontier != commit_op
        {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace,
                commit_op,
                offsets_frontier = frontier,
                "descriptor commit_op disagrees with its offsets artifact frontier;                  refusing the install"
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
        // A peer that has NOT yet applied a committed purge offers pre-purge
        // segments under the stale generation. The install's own generation
        // handling only widens permission (`max`), so it would resurrect the
        // purged data durably: the local applied value stays at the newer
        // generation, and the reconciler's `committed > applied` gate never
        // re-fires. Compared against the METADATA plane's committed value, not
        // this partition's applied one -- the latter is memory-only and reads 0
        // after every restart. Routed through the ordinary failure arm, which
        // rotates the peer; worst case is one wasted pull.
        let committed_purge_generation = self
            .plane
            .metadata()
            .mux_stm
            .streams()
            .partition_purge_generation(
                target_namespace.stream_id(),
                target_namespace.topic_id(),
                target_namespace.partition_id(),
            );
        let offered_purge_generation =
            partitions::state_transfer::offered_purge_generation(&offsets_bytes);
        if offered_purge_generation < committed_purge_generation {
            tracing::warn!(
                shard = self.id,
                namespace_raw = namespace,
                peer,
                offered_purge_generation,
                committed_purge_generation,
                "refusing a partition transfer offer built before a committed purge;                  installing it would resurrect purged data"
            );
            self.abandon_or_rearm_partition_transfer(partition, peer)
                .await;
            return;
        }
        partition
            .consensus()
            .set_state_transfer_stage(consensus::StateTransferStage::Installing);
        let outcome = partition
            .install_state_transfer(&config, commit_op, staged, &offsets_bytes)
            .await;
        partition
            .consensus()
            .set_state_transfer_stage(consensus::StateTransferStage::Idle);
        match outcome {
            Ok(outcome) => {
                partition.note_transfer_progress();
                partition.note_transfer_installed();
                partition.transfer_rearm = None;
                if outcome.offsets_written {
                    tracing::info!(
                        shard = self.id,
                        namespace_raw = namespace,
                        applied_frontier = outcome.applied_frontier,
                        "partition state transfer installed; handing tail to journal repair"
                    );
                } else {
                    // Deliberately NOT prefixed with the success line's text:
                    // specs match log substrings, and a shared prefix would
                    // let them pass on the degraded path.
                    tracing::warn!(
                        shard = self.id,
                        namespace_raw = namespace,
                        applied_frontier = outcome.applied_frontier,
                        "partition state transfer landed WITHOUT fully written consumer \
                         offsets; the next offset commit rewrites the files"
                    );
                }
                partition.commit_journal(&config).await;
                self.maybe_request_partition_repair(partition, peer).await;
            }
            Err(
                error @ partitions::state_transfer::PartitionInstallError::ConvergeFailed { .. },
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
                self.fence_partition_for_rebuild(IggyNamespace::from_raw(namespace), partition)
                    .await;
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
        B: MessageBus,
    {
        let failures = partition.record_transfer_failure();
        let after_ticks = transfer_rearm_backoff(self.repair_retry_ticks.get(), failures);
        self.schedule_partition_transfer_rearm(partition, peer, failures, after_ticks)
            .await;
    }

    /// Re-arm after a refusal the serving peer marked TRANSIENT: schedule the
    /// next attempt on a flat interval and charge nothing.
    ///
    /// "The peer is momentarily not the caught-up primary" is the common case
    /// under produce load, and `transfer_failures` is reset only by a completed
    /// install, so charging it turns a transient into a stall measured in re-arm
    /// ceilings: nothing else recovers the partition meanwhile, since repair
    /// keeps hitting the refused floor and will not arm while a re-arm is
    /// pending.
    #[allow(clippy::future_not_send)]
    async fn rearm_partition_transfer_after_refusal(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
    ) where
        B: MessageBus,
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
            tracing::error!(
                shard = self.id,
                namespace_raw = partition.consensus().namespace(),
                peer,
                refusals,
                "partition state transfer has been refused {refusals} times in a row;                  this partition is not rejoining"
            );
        }
        self.schedule_partition_transfer_rearm(partition, peer, 0, after_ticks)
            .await;
    }

    /// Drop the session, rotate the peer, and schedule the re-arm; shared by the
    /// charged and uncharged paths.
    #[allow(clippy::future_not_send)]
    async fn schedule_partition_transfer_rearm(
        &self,
        partition: &mut IggyPartition<B, SB>,
        peer: u8,
        failures: u32,
        after_ticks: u32,
    ) where
        B: MessageBus,
    {
        partition.transfer = None;
        let consensus = partition.consensus();
        if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
            consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
        }
        let next_peer = next_transfer_peer(
            consensus.replica(),
            peer,
            consensus.replica_count(),
            consensus.primary_index(consensus.view()),
        );
        tracing::info!(
            shard = self.id,
            namespace_raw = partition.consensus().namespace(),
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
                .any(|(namespace, _)| *namespace == consensus.namespace())
        });
        if !metadata_served {
            metadata.clear_state_transfer_offer_cache();
        }
        // Partition offer caches: release each namespace whose LAST offer
        // just aged out, so a served-once partition does not pin its offer
        // (manifest + offsets table) for the process lifetime.
        let mut vanished = namespaces_before;
        vanished.retain(|namespace| !offers.keys().any(|(live, _)| live == namespace));
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

        let (local_actions, wire_actions) = split_local_actions(actions);
        dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &local_actions).await;
        if metadata.persist_superblock_if_needed(consensus).await {
            dispatch_vsr_actions(consensus, metadata.journal.as_ref(), &wire_actions).await;
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
            consensus.namespace(),
            consensus.view(),
        );
    }

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
                namespace: 7,
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
        let actions = vec![VsrAction::SendStartViewChange {
            view: 2,
            namespace: 7,
        }];
        let (local, wire) = split_local_actions(actions);
        assert!(local.is_empty());
        assert_eq!(wire.len(), 1);
    }
}
