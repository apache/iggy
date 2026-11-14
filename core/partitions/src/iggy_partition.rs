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

use crate::consumer_offset_capacity::{
    ConsumerOffsetCapacity, ConsumerOffsetCapacityError, DurableConsumerOffsets,
    DurableOffsetState, ExplicitOffsetStores,
};
use crate::journal::{
    BodyPosition, MessageLookup, PartitionJournal, PartitionJournalMemStorage, RetainedBatchMeta,
};
use crate::log::JournalInfo;
use crate::log::SegmentedLog;
use crate::object_delete::{RetiredSegment, RetiredSegments};
use crate::persistence::{
    FileSyncBarrier, PartitionPersistence, PersistenceCompletion, PersistenceNotifier,
};
use crate::poll_plan::{
    DiskReadPlan, DiskSegment, PartitionDirResolution, PollContext, PollPlan, PollReadResult,
    PollTier, ResidentTailSnapshot, SegmentSource,
};
use crate::producer_state::{ProducerAdmission, ProducerEntry, ProducerTable, StampedBatch};
use crate::remote_segments::{IndexFetches, RemoteRange};
use crate::segment::{Segment, SegmentLocation};
use crate::state_machine::StateMachines;
use crate::state_transfer::{
    PartitionTransferSession, PartitionTransferUnavailable, PendingTransferRearm, TransferPlanSlot,
};
use crate::types::{
    COMMIT_WALK_OPS_MAX, FatalCommit, RepairConclusion, RepairIngest, RepairSession,
};
use crate::view_fence::{ViewFence, ViewWindowVerdict, view_window_verdict};
use crate::{
    AppendResult, Partition, PartitionOffsets, PartitionsConfig, PollFragments, PollQueryResult,
    PollingArgs, PollingConsumer,
};
use consensus::Pipeline;
use consensus::client_table::{CachedReply, ClientTableWireError};
use consensus::{
    AutoCommitRequestContext, ClientTable, ClientTableMode, CommitLogEvent, Consensus,
    ContradictionSource, DvcHeaderKind, LocalRequestOrder, PartitionDiagEvent, PipelineEntry,
    PlaneKind, Project, RepairEvidence, ReplicaLogContext, RequestLogEvent, Sequencer,
    SimEventKind, VsrConsensus, ack_preflight, ack_quorum_reached, build_deny_reply_from_request,
    build_reply_from_request, build_reply_message, dvc_header_kind, emit_namespace_progress_event,
    emit_partition_diag, emit_sim_event, fence_old_prepare_by_commit, repair_session_live,
    repaired_frontier_update, repaired_prepare_evidence, replicate_frozen_prepare,
    replicate_preflight, restamp_prepare_view, send_prepare_ok as send_prepare_ok_common,
    verify_prepare_integrity,
};
use iggy_binary_protocol::batch::BatchProducer;
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest, split_prepared_offset,
};
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, RetireConsumerGroupOwnersRequest,
    TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::requests::system::{RetireSessionsRequest, SessionIdentity};
use iggy_binary_protocol::responses::messages::{
    SendMessagesConfirmationResponse, SendMessagesResponse,
};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareHeader, WireDecode, WireEncode, WireIdentifier,
};
use iggy_binary_protocol::{PrepareOkHeader, ReplyHeader, RoutedRequestHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    EncryptorKind, IggyByteSize, IggyError, IggyExpiry, IggyTimestamp, PartitionStats, PollingKind,
    TopicRuntimeOptions,
};
use iggy_object_store::{SegmentKey, SegmentObject};
use journal::Journal as _;
use journal::local_gate::LocalGate;
use journal::superblock::{
    PingPongSuperblock, SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS, SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS,
    SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT, SuperblockStore,
};
use message_bus::{AUTO_COMMIT_CLIENT_ID, IggyMessageBus, MessageBus, is_auto_commit_client};
use server_common::poll::{AutoCommitReservation, PollHistoryId};
use server_common::{
    Message, SegmentStorage,
    iobuf::Frozen,
    send_messages::{
        BatchHeader, ChecksumMode, convert_request_message, decode_prepare_slice,
        decode_prepare_slice_trusted, stamp_prepare_for_persistence,
    },
    sharding::IggyNamespace,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::Hash;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error, warn};

const PREPARE_GAP_LOG_INTERVAL_MICROS: u64 = 1_000_000;

/// Which of a partition's offset counters are live.
///
/// Two bits, not one: a reservation-seeded boot makes the append counter live
/// with nothing committed behind it, and folding them together reports
/// `offset_frontier() == 1` for a partition holding nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct OffsetSpace {
    /// The APPEND counter is live, so the next mint continues it.
    pub append_live: bool,
    /// The COMMITTED counter names data.
    pub committed_seeded: bool,
}

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
#[allow(clippy::struct_excessive_bools)]
pub struct IggyPartition<B = IggyMessageBus, SB = PingPongSuperblock>
where
    B: MessageBus,
{
    consensus: VsrConsensus<B>,
    /// This group's slice of the VSR client table, run in
    /// [`ClientTableMode::PartitionSlice`]: per-client request watermarks
    /// folded in at commit. Replica-local and memory-only: boot lifts the commit
    /// frontier without re-applying the log, so a restarted replica comes back
    /// with an empty slice while its peers keep theirs, and only commits folded
    /// in after boot, or a state-transfer install, rebuild it. The mode turns
    /// off what this plane cannot use -- no reply ring (`SendMessages` has no
    /// result section, so a duplicate is answered by synthesizing the empty
    /// success its original earned), no epoch fence (a partition group never
    /// observes a `Register`), and no preallocated slot array (one table per
    /// group, where preallocating the cap would reserve hundreds of KiB per
    /// partition before a client connects).
    dedup: ClientTable,
    /// Committed state of idempotent producers. Every replica records a
    /// committed batch in op order. The primary checks a producer's batch
    /// against it and the uncommitted prepares before the batch gets offsets.
    producers: ProducerTable,
    pub log: SegmentedLog<PartitionJournal<PartitionJournalMemStorage>>,
    /// Highest durably persisted offset.
    pub offset: Arc<AtomicU64>,
    /// Highest offset assigned to prepares that may still only live in the in-memory journal.
    pub dirty_offset: AtomicU64,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    /// Highest offset this partition has served (polled) to each consumer group.
    /// The cooperative-rebalance reconciler completes a pending revocation once
    /// the source group has committed up to what it was polled
    /// (`committed >= last_polled`), i.e. nothing is in flight. Ephemeral (not
    /// persisted): recovery or a view change leaves prior progress unknown.
    pub last_polled_offsets: Arc<ConsumerGroupOffsets>,
    pub(crate) consumer_group_owners: BTreeMap<u64, InstalledConsumerGroupOwner>,
    // The flag proves that the owner installed in this view has served nothing.
    pub(crate) consumer_group_polls: BTreeMap<u64, Arc<AtomicBool>>,
    pub(crate) owner_retirement: Option<RetireConsumerGroupOwnersRequest>,
    pub(crate) pending_owner_install: Cell<Option<InstallConsumerGroupOwnerRequest>>,
    /// Op of the last unapplied install the latest log scan found. A view
    /// change can leave several, and the latch holds only the last one, so
    /// group reads, offset writes and retirement catalogs wait until every one
    /// of them has applied.
    pub(crate) owner_install_barrier: u64,
    pending_history_transition: Cell<Option<TransitionPartitionHistoryRequest>>,
    pub(crate) history_transition: Option<InstalledPartitionHistory>,
    /// The partition's state machines, applied at commit in op order.
    pub(crate) state_machines: StateMachines,
    /// The chain of a tiered partition, `None` until it opens one.
    pub(crate) segment_chain: Option<Box<crate::segment_chain::SegmentChain>>,
    /// Where polls read segments whose `.log` left the disk.
    remote_segments: Option<Rc<crate::RemoteSegments>>,
    /// The floor advances this replica applied that its chain does not record
    /// yet, oldest first. Recorded ones leave from the front.
    pub(crate) pending_advances: VecDeque<crate::tiered_floor::AdvanceTieredFloor>,
    /// How much of the partition stays on local disk, as the segment cleaner
    /// last resolved it from the topic. `None` until it has.
    local_envelope: Option<LocalEnvelope>,
    /// The upload in flight: its attempt, and the scope whose end stops the
    /// job.
    segment_upload: Option<(crate::SegmentUploadAttempt, iggy_object_store::TierScope)>,
    /// Attempts started so far, so that no two share a number.
    segment_upload_attempts: u64,
    /// The offset after the last segment this primary stored, when uploads
    /// have run oldest first up to it. Only where the search for the next
    /// upload starts: see [`Self::slot_after_uploaded`].
    upload_cursor: Option<u64>,
    /// Set when a sealed segment became uploadable, for the shard to turn into
    /// an upload pass: the partition cannot send itself a frame.
    upload_wanted: Cell<bool>,
    /// The highest committed truncation watermark a timed pass read from
    /// metadata for this partition. The watermark only rises, so the one
    /// metadata holds now is never below it.
    committed_truncation: u64,
    /// What this replica deletes from the object store while it is the
    /// primary.
    object_deletes: ObjectDeletes,
    /// Number of the last object-delete job this partition started.
    object_delete_jobs: u64,
    /// The fetches of remote segments' indexes that polls have in flight.
    index_fetches: Rc<IndexFetches>,
    pub stats: Arc<PartitionStats>,
    /// Widest batch committed here, the floor for a disk poll's first read.
    /// See [`Self::widest_committed_batch`].
    widest_batch_bytes: Cell<u64>,
    /// See [`PollVisibility::committed_bytes`].
    committed_bytes: u64,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub(crate) offset_space: OffsetSpace,
    pub write_lock: Arc<TokioMutex<()>>,
    /// The offset directory of each kind, by [`ConsumerKind::index`], once
    /// storage is configured.
    pub(crate) consumer_offset_dirs: [Option<String>; ConsumerKind::COUNT],
    /// Canonical on-disk partition directory, set at construction by the
    /// server builder. Disk polls must not derive this from live writers:
    /// sealed segments drop their writer at rotation, so a writer-derived
    /// path transiently disappears and silently hides the disk tier.
    /// `None` only for in-memory (simulated) partitions.
    pub(crate) partition_dir: Option<String>,
    segment_names_dirty: Cell<bool>,
    /// This topic's runtime knobs, resolved at topic admission and carried
    /// here by the builder. Every `None` field falls back to the shard-wide
    /// `PartitionsConfig` value (simulator and tests build partitions with
    /// no resolved options at all).
    pub(crate) runtime_options: TopicRuntimeOptions,
    /// The at-rest encryptor of this partition's stream incarnation, `None`
    /// for a plaintext stream. Set before the partition becomes servable and
    /// never swapped, so a poll answered after the stream id is reused still
    /// decrypts with the key its bytes were written under.
    encryptor: Option<Arc<EncryptorKind>>,
    pub(crate) persistence: Option<Rc<PartitionPersistence>>,
    pending_retry_checkpoint: Option<u64>,
    pub(crate) materialization_missing: bool,
    recovered_log_view: Option<u32>,
    pending_persisted_acks: RefCell<BTreeMap<u64, PrepareHeader>>,
    /// Without a WAL, an install must still preserve ops admitted to an ack.
    pub(crate) acknowledged_op: Cell<u64>,
    /// In-flight journal repair:
    /// set when the recovery handshake finds this replica behind the group's
    /// commit frontier, cleared when `RepairDone` completes the walk.
    pub repair: Option<RepairSession>,
    /// Consecutive shard-sweep ticks this partition has been seen gap-stopped
    /// (committed ops it cannot walk to, because the op at its commit frontier
    /// plus one is missing). Debounces the sweep's level-triggered repair arm,
    /// and is spent by whichever site opens the repair session.
    ///
    /// `Cell` for the same reason as `prepare_gap_drops`: the sweep
    /// drives it from the shared borrow it probes the partition through, so the
    /// in-flight scan the arm budget needs can run without a `&mut` outstanding.
    pub gap_ticks: Cell<u32>,
    /// Prepares the backup gap check destroyed since the shard last drained
    /// the count, folded into `partition_prepare_gap_drops_total` there.
    /// Replicated traffic has no client to answer and retransmit skips ops that
    /// already reached quorum, so nothing else records that the frame existed.
    /// It counts what the ordering check destroyed, which is neither the holes
    /// nor only them: a gap opened by the last prepare of a burst leaves it at
    /// zero, and a duplicate delivery of an op this replica already sequenced
    /// bumps it without any hole existing. Zero proves nothing, and nonzero is
    /// a reason to look at the `sequence` field on the drop log, which is what
    /// separates the two shapes.
    ///
    /// `Cell`: the shard drains it once per sweep, off the same shared borrow
    /// the rest of the tick reads the partition through, so a `&mut` here would
    /// buy a second lookup of the same group per tick.
    prepare_gap_drops: Cell<u64>,
    prepare_gap_log_after: Cell<u64>,
    /// Fault injection for the harness, armed by
    /// [`Self::inject_commit_failure`]. Behind a feature only the simulator's
    /// DEV-dependencies turn on, so no build that ships this crate compiles the
    /// branch it gates.
    #[cfg(any(test, feature = "fault-injection"))]
    injected_commit_failure: bool,
    /// Highest message offset recovered from segments at boot (`None` when
    /// the partition booted empty). Repaired batches at or below this line
    /// are already persisted and counted; the flush and commit paths skip
    /// re-persisting / re-counting them. Immutable after boot, so live
    /// traffic (always above it) is never affected.
    pub recovered_durable_offset: Option<u64>,
    /// Where the group's offset space STARTS on this replica: everything
    /// below it is represented by a completed state-transfer install (or by
    /// the empty segment such an install planted at the frontier). Consulted
    /// only by the repair floor-connect check, so an install with zero
    /// staged segments does not force one wasted transfer round per rejoin.
    /// Deliberately separate from [`Self::recovered_durable_offset`], which
    /// also gates repaired-batch persistence -- overstating THAT field would
    /// silently drop the `(commit_op, commit_max]` replay window.
    pub installed_frontier: Option<u64>,
    /// Set once a local commit fails for an op the cluster already committed.
    /// Fences the partition: every path that would advance or serve it turns
    /// into a no-op, so the shard's tick can observe the fault and shut the
    /// server down without the partition moving again in the meantime.
    pub(crate) fatal: Option<FatalCommit>,
    pending_commit: Option<PendingCommit>,
    local_request_order: Cell<u64>,
    no_ack: VecDeque<PendingNoAck>,
    materialization: Option<PendingMaterialization>,
    maintenance: PendingPartitionMaintenance,
    io_notifier: RefCell<Option<crate::PartitionIoNotifier>>,
    io_bytes_max: Cell<usize>,
    io_sequence: Cell<u64>,
    pub(crate) io_active: Rc<crate::io::PartitionIoQuiescence>,
    pub(crate) transition: Option<PendingPartitionTransition>,
    deferred_views: Vec<server_common::MessageBag>,
    pub(crate) pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
    /// Entries journaled before this replica adopted the current view, held back
    /// from commit and acknowledgement until proven. See [`Self::fence_view_window`].
    view_fence: Option<ViewFence>,
    /// Set at boot when the WAL replayed history: the first view adoption after a
    /// restart fences it even though `log_view` came back unchanged.
    recovered_unproven: bool,
    /// Commit point the recovered WAL names. Entries at or below it are
    /// committed history, so a fence neither holds them back nor proves them.
    recovered_commit: u64,
    /// Commit point the WAL names on a replica that booted awaiting a state
    /// transfer. Boot skipped the replay, so the journal is empty and
    /// `commit_min` reads 0 while the WAL still holds those ops. Above 0 it
    /// proves that the group committed an op, which an offer at op 0 would
    /// erase.
    pub(crate) unreplayed_commit: u64,
    /// First op of a broken fenced window the I/O lane is truncating. The fence's
    /// follow-up runs when that truncation lands.
    unproven_drop_from: Option<u64>,
    /// `(commit_min, bytes)`: the bytes that repair journaled above that
    /// `commit_min` while the view log was unproven. See
    /// [`Self::proof_walk_over_budget`].
    proof_walk: (u64, u64),
    pub(crate) proof_walk_memory: Option<crate::repair_memory::RepairMemoryReservation>,
    pub(crate) served_repair_tails: RefCell<HashMap<u8, crate::state_transfer::ServedRepairTail>>,
    /// The hole of the proof walk and the highest op held below it when the
    /// last repair request went out. See [`Self::next_proof_walk_pull`].
    proof_walk_requested: Cell<(u64, Option<u64>)>,
    /// Identity shared with pending polls and replaced when their history retires.
    poll_history: PollHistoryId,
    incarnation: crate::PartitionIncarnation,
    workerless_offset_files: Rc<crate::offset_storage::RetainedOffsetFiles<compio::fs::File>>,
    poll_notifier: Option<Rc<dyn Fn()>>,
    /// The node's reserve on the data disk. `None` never refuses.
    free_space_reserve: Option<Arc<crate::FreeSpaceReserve>>,
    /// Bytes of the send the reserve admitted and this partition is turning
    /// into a prepare. The reserve counts them as pending until the WAL does.
    admitted_send_bytes: Cell<u64>,
    /// Committed consumer-offset membership and values. This is deliberately
    /// separate from the eager poll maps because follower-local and uncommitted
    /// auto-commit progress must never consume a durable slot or enter a state
    /// transfer artifact. It is the only store of external group offsets: that
    /// kind is never polled, so it has no eager progress to keep apart.
    pub(crate) durable_consumer_offsets: DurableConsumerOffsets,
    pub(crate) explicit_offset_stores: ExplicitOffsetStores,
    /// Admission capacity of each kind, by [`ConsumerKind::index`].
    pub(crate) consumer_offset_capacities: [ConsumerOffsetCapacity; ConsumerKind::COUNT],
    pub(crate) observed_view: u32,
    offset_reservations_need_resync: Cell<bool>,
    /// Set by a truncation and taken by the next log scan. The truncation can
    /// drop the install the latch names, and a scan in an unchanged view keeps
    /// the latch, so this flag makes that scan rebuild it.
    truncated_since_scan: Cell<bool>,
    offset_reservations_scan_state: Option<(u64, u64, u64, Option<u64>)>,
    consumer_group_offsets_dirty: Rc<RefCell<BTreeSet<IggyNamespace>>>,
    /// Shared by the partitions of a shard. Set when a fence publishes, so the
    /// pump wakes the reconciler that reports it to metadata.
    fence_published: Rc<Cell<bool>>,
    consumer_offset_dirs_dirty: [Cell<bool>; ConsumerKind::COUNT],
    /// Latest operation of each kind in the current commit walk. Even a covered
    /// store depends on an earlier unsynced directory entry of its kind.
    consumer_offset_dirs_touched: [Cell<Option<(u64, Operation)>>; ConsumerKind::COUNT],
    #[cfg(test)]
    consumer_offset_dir_sync_fault: Cell<Option<usize>>,
    #[cfg(test)]
    offset_dir_sync_count: Cell<usize>,
    pub(crate) required_metadata_frontier: u64,
    /// Newest metadata `FinalizeSession` op whose retirement debt a committed
    /// payment covers. Replicated with the receipts it protects.
    pub(crate) retirement_paid_through: u64,
    /// Newest retirement debt op this replica learned from local metadata.
    /// While it exceeds `retirement_paid_through`, session requests wait.
    retirement_debt: Cell<u64>,
    /// Durable superblock for this partition's consensus group, recording
    /// `(view, log_view)` across a crash so this replica can never
    /// re-participate in a view older than one it advertised. `None` for
    /// in-memory / simulated partitions, where the persist gate is a no-op
    /// and views stay process-lifetime only. Behind `Rc` because the boot
    /// path opens the store once and hands the same instance here:
    /// re-opening would fork the ping-pong sequence counter.
    superblock: Option<Rc<SB>>,
    /// Serializes this partition's superblock writes so at most one is in
    /// flight: `PingPongSuperblock::write` picks its slot before it awaits,
    /// so two overlapping writers would target the same slot and could tear
    /// it while both report success. Per partition, not per shard -- every
    /// group owns its own two-file store, so writes to different partitions
    /// never contend.
    superblock_lock: Rc<LocalGate>,
    superblock_intent: Cell<Option<SuperblockIntent>>,
    superblock_running: Cell<Option<(SuperblockIntent, bool)>>,
    superblock_completed: Cell<Option<(SuperblockIntent, bool)>>,
    pending_wire_actions: RefCell<Option<PendingWireActions>>,
    /// Consecutive failed superblock writes, and the clock reading after which
    /// the next attempt may run. A persistent `ENOSPC` / `EIO` would otherwise
    /// re-run a full `atomic_replace` on every 10 ms consensus tick. Reset on
    /// the first success. See [`Self::persist_superblock_if_needed`] for the
    /// terminal policy.
    superblock_write_failures: Cell<u64>,
    superblock_retry_after_micros: Cell<u64>,
    /// The `offset_frontier` the last successful superblock write recorded,
    /// seeded at boot from the record that write left behind.
    ///
    /// The advance direction maxes against THIS as well as the live counter,
    /// because the two diverge: a failed install leaves the counter at its
    /// pre-install value while the record already names the incoming frontier,
    /// and the fence that follows then persists the counter. Maxing against
    /// the counter alone writes 0 over a recorded N and quarantines the
    /// segments that were the only other witness, after which the rebuild
    /// re-mints offsets the group already handed out.
    durable_offset_frontier: Cell<u64>,
    /// The `offset_reserved` ceiling the last successful superblock write
    /// recorded, seeded at boot from the record that write left behind.
    ///
    /// A `Cell` rather than a re-read of the record because every append reads
    /// it, and the steady state ("the block still covers this batch") has to
    /// cost nothing. Kept apart from [`Self::durable_offset_frontier`]: see
    /// `consensus::VsrState::offset_reserved`.
    durable_offset_reserved: Cell<u64>,
    /// Offsets the append fence claims per superblock write; installed by boot
    /// from `PartitionsConfig`.
    offset_reservation_lease: u64,
    /// In-flight state transfer for this group (rejoin whose repair floor was
    /// refused); tail repair takes over at install. See
    /// [`PartitionTransferSession`].
    pub transfer: Option<PartitionTransferSession>,
    /// Consecutive transfer stall rounds WITHIN one recovery attempt. NOT in the
    /// session: three of four metadata arming sites re-minted their session, so
    /// a per-session counter bounded nothing and a permanent failure cycled
    /// abandon -> repair -> refusal -> re-arm at zero forever. Reset by
    /// [`Self::note_transfer_progress`] and by
    /// [`Self::note_transfer_rearm_scheduled`]; livelock across attempts is
    /// bounded by [`Self::transfer_failures`] and its exponential backoff.
    transfer_attempts: u32,
    /// Repair stall rounds without progress in [`Self::repair_charged_view`],
    /// against [`crate::types::REPAIR_MAX_STALL_RETRIES`]. Restarts once a
    /// session spends it, so each peer that the shard rotates to gets the
    /// whole budget; cleared by real progress.
    repair_attempts: u32,
    /// Rotations since the last progress in [`Self::repair_charged_view`]
    /// that opened a session against another peer. Survives the sessions, so
    /// the shard can give up on the ring; cleared by real progress.
    repair_rotations: u32,
    /// `commit_min` at the last repair stall round. A walk past it is real
    /// progress, see [`Self::burn_repair_attempt`].
    repair_walked_to: u64,
    /// The view of the last repair stall round. A round in another view
    /// starts a fresh count.
    repair_charged_view: u32,
    /// CONSECUTIVE transfer failures of any class (decode, spill, install,
    /// peer-unavailable, stall exhaustion). Deliberately NOT keyed on the
    /// offered generation: a committing primary advances its generation
    /// every round, and a generation-keyed count reset to 1 forever, so a
    /// deterministic local failure (ENOSPC, an undecodable artifact) looped
    /// at network round-trip rate. Reset only by
    /// [`Self::note_transfer_installed`]; drives the re-arm backoff.
    transfer_failures: u32,
    /// CONSECUTIVE transient refusals (a peer that cannot serve right now).
    /// Drives log escalation only -- never the backoff. See
    /// [`Self::record_transfer_refusal`].
    transfer_refusals: u32,
    /// A scheduled transfer re-arm: try `peer` again once `after_ticks`
    /// consensus ticks elapse. Owned by the shard tick sweep; while one is
    /// pending, the repair-refusal trigger must not arm concurrently.
    pub transfer_rearm: Option<PendingTransferRearm>,
    /// Memoized segment-payload checksum state, keyed by segment base offset.
    /// Sealed segments are immutable, so their stamp never changes; the active
    /// segment extends its own hasher over the bytes it gained. Without this,
    /// EVERY offer build re-reads and re-hashes all retained bytes on the pump
    /// -- and a committing primary advances `commit_op` each round, so the offer
    /// cache alone never saves the pass. Retention drops the entry of each
    /// segment it retires. An install replaces the map with the stamps of the
    /// segments that it installs, and a converge clears the map.
    pub(crate) segment_checksum_cache:
        RefCell<std::collections::HashMap<u64, crate::state_transfer::SegmentChecksumMemo>>,
    /// Verified staging files retained across partial scans and advancing offers.
    /// See [`crate::state_transfer::ReuseScanMemo`].
    pub(crate) reuse_scan_memo: RefCell<Option<crate::state_transfer::ReuseScanMemo>>,
    /// Resolves when the last staging writer ends. Reuse scans and spills wait
    /// for it so abandoned tasks cannot race a new session on the same paths.
    pub(crate) staging_work_ended: Option<futures::channel::oneshot::Receiver<()>>,
    /// The checksum state of the last segment that a transfer spilled. The
    /// install keeps it as the memo of the segment it opens as active.
    pub(crate) spilled_hasher: RefCell<Option<crate::state_transfer::SpilledHasher>>,
    /// Serving-side offer cache, keyed by the `commit_op` it was built at, so
    /// simultaneous rejoiners share one manifest instead of re-reading every
    /// segment per requester. Invalidated by segment removal (same commit
    /// frontier, different bytes) and released by the shard's offer-expiry
    /// sweep.
    pub(crate) transfer_offer_cache:
        RefCell<Option<Rc<crate::state_transfer::PartitionStateTransferOffer>>>,
    /// Serving-side plan the rounds of one offer build share. Released when
    /// the build ends, with the offer cache, and with the history it
    /// describes.
    pub(crate) transfer_plan: RefCell<Option<TransferPlanSlot>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstalledConsumerGroupOwner {
    pub installation: InstallConsumerGroupOwnerRequest,
    pub partition_op: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstalledPartitionHistory {
    pub transition: TransitionPartitionHistoryRequest,
    pub partition_op: u64,
}

impl<B, SB> fmt::Debug for IggyPartition<B, SB>
where
    B: MessageBus,
{
    // Hand-written because `SB` carries no `Debug` bound; the fields listed
    // are the ones diagnostics actually key on.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IggyPartition")
            .field("namespace", &self.consensus.group())
            .field("offset", &self.offset)
            .field("dirty_offset", &self.dirty_offset)
            .field("offset_space", &self.offset_space)
            .field("partition_dir", &self.partition_dir)
            .field("repair", &self.repair)
            .field("recovered_durable_offset", &self.recovered_durable_offset)
            .field("observed_view", &self.observed_view)
            .finish_non_exhaustive()
    }
}

/// Read accepted by the owner, with any local progress updates already applied.
/// Acceptance does not imply that its automatic offset commit is durable.
#[derive(Debug)]
pub struct PollCompletion {
    pub context: iggy_binary_protocol::primitives::partition_history::PartitionContext,
    /// Selected message bytes that the owner has authorized for the reply.
    pub fragments: PollFragments,
    /// Partition message frontier from planning, which may now lag new commits.
    pub current_offset: u64,
    /// Assigned prepare to replicate after releasing the reply. `None` can also
    /// mean the automatic commit is queued and has no prepare slot yet.
    pub replication: Option<PollReplication>,
    /// The key the fragments were written under, `None` for a plaintext
    /// stream or an empty reply. Captured with the fragments, so a reply that
    /// outlives the stream never decrypts with a later stream's key.
    pub encryptor: Option<Arc<EncryptorKind>>,
}

/// A prepare assigned by completion, with capacity held through journal staging.
#[derive(Debug)]
pub struct PollReplication {
    /// Automatic offset commit with an operation number already assigned.
    prepare: Message<PrepareHeader>,
    /// Keeps the consumer key reserved until replication stages the prepare.
    reservation: AutoCommitReservation,
}

/// What a parked poll compares between two reads of a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollVisibility {
    /// The committed offset, `None` until the offset space is seeded.
    pub offset: Option<u64>,
    /// Encoded bytes, batch framing included, of the batches that commits
    /// made visible since the partition was opened. Only its growth between
    /// two reads has a meaning.
    pub committed_bytes: u64,
}

/// Post-preflight dispatch in `on_request`: replicate via VSR or take the
/// `NoAck` leader-local fast path. `RoutedRequestHeader` is boxed to avoid the
/// 277-byte inline variant tripping clippy's `large_enum_variant`.
enum Disposition {
    Replicate(Message<PrepareHeader>),
    NoAck {
        request_header: Box<RoutedRequestHeader>,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PendingConsumerOffsetCommit {
    kind: ConsumerKind,
    consumer_id: u32,
    mutation: PendingConsumerOffsetMutation,
    /// A server auto-commit (a poll's `auto_commit`, replicated via the reserved
    /// `AUTO_COMMIT_CLIENT_ID`): the commit-apply must be monotone so it cannot
    /// rewind the eager in-memory offset a newer poll already advanced. Explicit
    /// client stores leave this `false` (a store may legitimately rewind).
    auto_commit: bool,
    owner_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PendingConsumerOffsetMutation {
    Upsert(u64),
    Delete,
}

#[derive(Clone, Copy)]
struct ConsumerOffsetEffect {
    pending: PendingConsumerOffsetCommit,
    state: Option<DurableOffsetState>,
}

struct PendingNoAck {
    request_header: Box<RoutedRequestHeader>,
    pending: PendingConsumerOffsetCommit,
    reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    local_order: LocalRequestOrder,
    reservation: AutoCommitReservation,
    phase: NoAckPhase,
}

enum NoAckPhase {
    Validate,
    OffsetReady(ConsumerOffsetPlan),
    OffsetSubmitted(ConsumerOffsetPlan),
    Directory,
    DirectorySubmitted,
    Reply(Result<(), IggyError>),
}

enum NoAckStep {
    Progress,
    Pending,
    Ready,
    Reply,
}

pub enum PendingPartitionTransition {
    View {
        drain: Option<crate::PersistenceDrain>,
    },
    PrimaryTransfer {
        request: crate::PreparedPrimaryTransfer,
        drain: Option<crate::PersistenceDrain>,
    },
    Truncate {
        ops: std::ops::RangeInclusive<u64>,
        drain: Option<crate::PersistenceDrain>,
        actions: Vec<consensus::VsrAction>,
        peer: Option<u8>,
    },
    Install(Box<crate::state_transfer::PendingInstall>),
    Quarantine {
        frontier: u64,
        drain: Option<crate::PersistenceDrain>,
        phase: QuarantinePhase,
    },
}

pub enum QuarantinePhase {
    Drain,
    Frontier,
    Files,
    Submitted,
    Done(std::io::Result<Option<String>>),
}

#[derive(Clone, Copy)]
struct ConsumerOffsetPlan {
    pending: PendingConsumerOffsetCommit,
    tracked: Option<DurableOffsetState>,
    persisted: bool,
    action: ConsumerOffsetAction,
    /// The emptied-at op this effect leaves when it changes it, `0` for none.
    emptied_at: Option<u64>,
}

#[derive(Clone, Copy)]
enum ConsumerOffsetAction {
    Ready(ConsumerOffsetEffect),
    Write { offset: u64, fold_max: bool },
    Delete,
}

/// Selected headers and replies remain in their original owners until all
/// physical phases settle. Journal eviction is deferred for the same lifetime.
struct PendingCommit {
    headers: Vec<PrepareHeader>,
    journal_headers: Vec<Option<PrepareHeader>>,
    batch_stats: Vec<Option<CommittedBatchStats>>,
    retirements: Vec<(u64, RetireSessionsRequest)>,
    owner_install: Option<InstallConsumerGroupOwnerRequest>,
    offset_effects: Vec<ConsumerOffsetEffect>,
    offset_batch: Vec<(usize, ConsumerOffsetPlan)>,
    /// The offset table's entry count and emptied-at op once the accepted
    /// effects publish.
    offset_count: usize,
    initial_emptied_at: u64,
    emptied_at: Option<u64>,
    incarnation: crate::PartitionIncarnation,
    poll_history: PollHistoryId,
    applied_floor: u64,
    from_pipeline: bool,
    send_client_replies: bool,
    materialize: bool,
    index_span_limit: u64,
    phase: CommitPhase,
    next_entry: usize,
    evictable: usize,
}

impl PendingCommit {
    fn has_sends(&self) -> bool {
        self.headers
            .iter()
            .any(|header| header.operation == Operation::SendMessages)
    }
}

#[derive(Clone, Copy)]
enum CommitPhase {
    Start,
    Materialize,
    Offsets,
    OffsetReady(ConsumerOffsetPlan),
    OffsetSubmitted(ConsumerOffsetPlan),
    SegmentDirectory,
    SegmentDirectorySubmitted,
    OffsetDirectories,
    OffsetDirectoriesSubmitted,
    Publish,
}

enum CommitStep {
    Progress,
    Pending,
    Ready,
    Publish,
}

/// A partition fence body. The journal and the commit walk decode it alike.
enum PartitionLifecycle {
    Owner(InstallConsumerGroupOwnerRequest),
    History(TransitionPartitionHistoryRequest),
    OwnerRetirement(RetireConsumerGroupOwnersRequest),
}

impl PartitionLifecycle {
    fn decode(operation: Operation, body: &[u8]) -> Result<Self, IggyError> {
        match operation {
            Operation::InstallConsumerGroupOwner => {
                InstallConsumerGroupOwnerRequest::decode_from(body).map(Self::Owner)
            }
            Operation::TransitionPartitionHistory => {
                TransitionPartitionHistoryRequest::decode_from(body).map(Self::History)
            }
            Operation::RetireConsumerGroupOwners => {
                RetireConsumerGroupOwnersRequest::decode_from(body).map(Self::OwnerRetirement)
            }
            _ => return Err(IggyError::InvalidCommand),
        }
        .map_err(|_| IggyError::InvalidCommand)
    }
}

struct PendingMaterialization {
    index_span_limit: u64,
    incarnation: crate::PartitionIncarnation,
    poll_history: PollHistoryId,
    through_op: u64,
    prefix_len: usize,
    next_entry: usize,
    durable_offset: Option<u64>,
    phase: MaterializationPhase,
    save: Option<PendingMaterializationSave>,
    checksum: Option<Box<consensus::StateArtifactHasher>>,
    digest: Option<Box<crate::segment_chain::SegmentDigest>>,
}

#[derive(Clone, Copy)]
struct PendingMaterializationSave {
    chunk: MaterializationChunk,
    written: u64,
}

#[derive(Default)]
struct PendingPartitionMaintenance {
    flush_through: Option<u64>,
    flush_drain: Option<crate::PersistenceDrain>,
    flush_completed_through: Option<u64>,
    transfer_flush: bool,
    shutdown: Option<PartitionShutdownPhase>,
    shutdown_drain: Option<crate::PersistenceDrain>,
    checkpoint: Option<PendingCheckpoint>,
    retention: Option<u64>,
    removing: Option<Segment>,
    /// The trim point the removal appends before it unlinks.
    removal_trim: Option<u64>,
    eviction: Option<u64>,
    tiering: Option<PendingTiering>,
}

/// A request to complete the partition's chain, which may still lack the
/// trim point or the record of a sealed segment, and how far it got.
struct PendingTiering {
    prefix: iggy_object_store::ObjectPrefix,
    /// Every sealed segment that starts below this offset has its record or
    /// needs none.
    scan_from: u64,
    /// The hash of a sealed segment between its steps.
    digest: Option<Box<crate::segment_chain::SegmentDigest>>,
    /// Bytes the next job reads into one buffer.
    read_len: usize,
    /// A job just ran, so work that waited behind it goes first once.
    yielding: bool,
    failures: u32,
    retry_after_micros: u64,
}

impl PendingTiering {
    const fn new(prefix: iggy_object_store::ObjectPrefix) -> Self {
        Self {
            prefix,
            scan_from: 0,
            digest: None,
            read_len: 0,
            yielding: false,
            failures: 0,
            retry_after_micros: 0,
        }
    }
}

const TIERING_RETRY_BACKOFF_BASE_MICROS: u64 = 1_000_000;
const TIERING_RETRY_BACKOFF_MAX_SHIFT: u32 = 6;

struct PendingWireActions {
    view: u32,
    log_view: u32,
    actions: Vec<consensus::VsrAction>,
}

enum PendingCheckpoint {
    /// The receipt table at the checkpoint op. Commits keep advancing while the
    /// checkpoint runs, so it is captured before any other phase.
    Receipts(u64, Vec<u8>),
    ReceiptsWriting(u64),
    Flush(u64),
    Index(u64),
    Syncing(u64),
    /// Carries the index file already synced through its original writer.
    Enqueue(u64, Option<String>),
    Wal(u64),
    /// Receipt files older than the completed WAL checkpoint.
    Reclaim(u64),
    Reclaiming,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionShutdownPhase {
    DrainAccepted,
    Flush,
    DrainWriters,
    Collapse,
    Complete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuperblockIntent {
    /// Preserve an incoming frontier before a destructive swap; never rewind.
    Advance { frontier: u64, reserved: u64 },
    /// Drop a local reservation to the installed group's committed frontier.
    Install(u64),
    /// Bypass retry backoff before moving the frontier's corroborating files.
    Quarantine(u64),
    /// After a successful shutdown flush, retain only the consumed reservation.
    Collapse { frontier: u64, reserved: u64 },
}

impl SuperblockIntent {
    const fn final_write(self) -> bool {
        matches!(self, Self::Quarantine(_) | Self::Collapse { .. })
    }
}

#[derive(Clone, Copy)]
enum MaterializationPhase {
    Inspect,
    Save(MaterializationChunk),
    Rotate(u64),
    Saving(MaterializationChunk),
    Rotating,
}

#[derive(Clone, Copy)]
struct MaterializationChunk {
    first_entry: usize,
    end_entry: usize,
    persisted_end: Option<u64>,
    buffers: usize,
    allocation_charge: usize,
    index: Option<crate::iggy_index::IggyIndex>,
    info: JournalInfo,
}

enum MaterializationStep {
    Progress,
    Pending,
    Ready,
    Complete,
}

impl PendingConsumerOffsetCommit {
    const fn upsert(kind: ConsumerKind, consumer_id: u32, offset: u64) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Upsert(offset),
            auto_commit: false,
            owner_generation: 0,
        }
    }

    /// Monotone-apply variant for a server auto-commit op. See `auto_commit`.
    const fn upsert_auto_commit(kind: ConsumerKind, consumer_id: u32, offset: u64) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Upsert(offset),
            auto_commit: true,
            owner_generation: 0,
        }
    }

    const fn delete(kind: ConsumerKind, consumer_id: u32) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Delete,
            auto_commit: false,
            owner_generation: 0,
        }
    }

    /// An explicit client mutation: a store for `Some(offset)`, a delete for
    /// `None`.
    const fn explicit(kind: ConsumerKind, consumer_id: u32, offset: Option<u64>) -> Self {
        match offset {
            Some(offset) => Self::upsert(kind, consumer_id, offset),
            None => Self::delete(kind, consumer_id),
        }
    }

    fn try_from_polling_consumer(
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<Self, IggyError> {
        let (kind, consumer_id) = match consumer {
            PollingConsumer::Consumer(id, _) => (
                ConsumerKind::Consumer,
                u32::try_from(id).map_err(|_| IggyError::InvalidCommand)?,
            ),
            PollingConsumer::ConsumerGroup(group_id, _) => (
                ConsumerKind::ConsumerGroup,
                u32::try_from(group_id).map_err(|_| IggyError::InvalidCommand)?,
            ),
        };
        Ok(Self::upsert(kind, consumer_id, offset))
    }
}

impl<B: MessageBus, SB> Drop for IggyPartition<B, SB> {
    fn drop(&mut self) {
        if let Some(persistence) = &self.persistence {
            persistence.retire();
        }
        if let Some(reserve) = &self.free_space_reserve {
            reserve.release_pending(self.admitted_send_bytes.take());
        }
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    #[allow(clippy::too_many_lines)]
    pub fn new(stats: Arc<PartitionStats>, consensus: VsrConsensus<B>) -> Self {
        let observed_view = consensus.view();
        let single_replica = consensus.replica_count() == 1;
        let io_active = Rc::<crate::io::PartitionIoQuiescence>::default();
        let poll_history = PollHistoryId::default();
        let partition = Self {
            consensus,
            dedup: ClientTable::with_mode(
                consensus::PARTITION_DEDUP_CLIENTS_MAX,
                ClientTableMode::PartitionSlice,
            ),
            producers: ProducerTable::default(),
            log: SegmentedLog::default(),
            offset: Arc::new(AtomicU64::new(0)),
            dirty_offset: AtomicU64::new(0),
            consumer_offsets: Arc::new(ConsumerOffsets::with_capacity(1)),
            consumer_group_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(1)),
            last_polled_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(1)),
            consumer_group_owners: BTreeMap::new(),
            consumer_group_polls: BTreeMap::new(),
            owner_retirement: None,
            pending_owner_install: Cell::new(None),
            owner_install_barrier: 0,
            pending_history_transition: Cell::new(None),
            history_transition: None,
            state_machines: StateMachines::default(),
            segment_chain: None,
            remote_segments: None,
            pending_advances: VecDeque::new(),
            local_envelope: None,
            segment_upload: None,
            segment_upload_attempts: 0,
            upload_cursor: None,
            upload_wanted: Cell::new(false),
            committed_truncation: 0,
            object_deletes: ObjectDeletes::default(),
            object_delete_jobs: 0,
            index_fetches: Rc::new(IndexFetches::new(Rc::clone(&io_active), poll_history)),
            stats,
            widest_batch_bytes: Cell::new(0),
            committed_bytes: 0,
            created_at: IggyTimestamp::now(),
            revision_id: 0,
            offset_space: OffsetSpace::default(),
            write_lock: Arc::new(TokioMutex::new(())),
            consumer_offset_dirs: Default::default(),
            partition_dir: None,
            segment_names_dirty: Cell::new(true),
            runtime_options: TopicRuntimeOptions::default(),
            encryptor: None,
            persistence: None,
            pending_retry_checkpoint: None,
            materialization_missing: false,
            recovered_log_view: None,
            pending_persisted_acks: RefCell::new(BTreeMap::new()),
            acknowledged_op: Cell::new(0),
            repair: None,
            gap_ticks: Cell::new(0),
            prepare_gap_drops: Cell::new(0),
            prepare_gap_log_after: Cell::new(0),
            #[cfg(any(test, feature = "fault-injection"))]
            injected_commit_failure: false,
            recovered_durable_offset: None,
            installed_frontier: None,
            fatal: None,
            pending_consumer_offset_commits: HashMap::new(),
            view_fence: None,
            recovered_unproven: false,
            recovered_commit: 0,
            unreplayed_commit: 0,
            unproven_drop_from: None,
            proof_walk: (0, 0),
            proof_walk_memory: None,
            served_repair_tails: RefCell::new(HashMap::new()),
            proof_walk_requested: Cell::new((0, None)),
            pending_commit: None,
            local_request_order: Cell::new(0),
            no_ack: VecDeque::new(),
            materialization: None,
            maintenance: PendingPartitionMaintenance::default(),
            io_notifier: RefCell::new(None),
            io_bytes_max: Cell::new(isize::MAX as usize),
            io_sequence: Cell::new(0),
            io_active,
            transition: None,
            deferred_views: Vec::new(),
            poll_history,
            incarnation: crate::PartitionIncarnation::default(),
            workerless_offset_files: Rc::default(),
            poll_notifier: None,
            free_space_reserve: None,
            admitted_send_bytes: Cell::new(0),
            durable_consumer_offsets: DurableConsumerOffsets::default(),
            explicit_offset_stores: ExplicitOffsetStores::default(),
            consumer_offset_capacities: ConsumerKind::ALL
                .map(|kind| ConsumerOffsetCapacity::new(kind, crate::DEFAULT_CONSUMER_OFFSETS_MAX)),
            observed_view,
            offset_reservations_need_resync: Cell::new(false),
            truncated_since_scan: Cell::new(false),
            offset_reservations_scan_state: None,
            consumer_group_offsets_dirty: Rc::default(),
            fence_published: Rc::default(),
            consumer_offset_dirs_dirty: std::array::from_fn(|_| Cell::new(false)),
            consumer_offset_dirs_touched: std::array::from_fn(|_| Cell::new(None)),
            #[cfg(test)]
            consumer_offset_dir_sync_fault: Cell::new(None),
            #[cfg(test)]
            offset_dir_sync_count: Cell::new(0),
            required_metadata_frontier: 0,
            retirement_paid_through: 0,
            retirement_debt: Cell::new(0),
            superblock: None,
            superblock_lock: Rc::new(LocalGate::new()),
            superblock_intent: Cell::new(None),
            superblock_running: Cell::new(None),
            superblock_completed: Cell::new(None),
            pending_wire_actions: RefCell::new(None),
            superblock_write_failures: Cell::new(0),
            superblock_retry_after_micros: Cell::new(0),
            durable_offset_frontier: Cell::new(0),
            durable_offset_reserved: Cell::new(0),
            offset_reservation_lease: u64::from(crate::DEFAULT_OFFSET_RESERVATION_LEASE),
            transfer: None,
            transfer_attempts: 0,
            repair_attempts: 0,
            repair_rotations: 0,
            repair_walked_to: 0,
            repair_charged_view: 0,
            transfer_failures: 0,
            transfer_refusals: 0,
            transfer_rearm: None,
            segment_checksum_cache: RefCell::new(std::collections::HashMap::new()),
            reuse_scan_memo: RefCell::new(None),
            staging_work_ended: None,
            spilled_hasher: RefCell::new(None),
            transfer_offer_cache: RefCell::new(None),
            transfer_plan: RefCell::new(None),
        };
        if single_replica {
            partition.log.journal().inner.set_repair_retention(false);
        }
        partition.refresh_recovered_log_complete();
        partition
    }

    /// Record the metadata incarnation this partition was built for.
    pub fn set_created_revision(&mut self, created_revision: u64) {
        if created_revision != self.created_revision() {
            self.retirement_debt.set(0);
        }
        self.consensus.set_partition_incarnation(created_revision);
    }

    /// Metadata creation revision that isolates this incarnation's on-disk
    /// history and consensus frames. Zero only without a metadata row.
    #[must_use]
    pub const fn created_revision(&self) -> u64 {
        self.consensus.partition_incarnation()
    }

    /// A report covers an applied retirement barrier or terminal history fence
    /// on a serving replica. Replay or a checkpoint must restore that proof.
    /// Retirement barriers require persistence at every durability level.
    #[must_use]
    pub fn session_retired(&self, identity: SessionIdentity) -> bool {
        self.fatal.is_none()
            && !self.requires_state_transfer()
            && self.consensus.is_normal()
            && !self.consensus.is_transferring()
            && self.consensus.pending_request(identity.client_id).is_none()
            && (self
                .history_transition
                .is_some_and(|history| history.partition_op <= self.consensus.commit_min())
                || (self.required_metadata_frontier >= identity.metadata_watermark
                    && self.dedup.get_epoch(identity.client_id) != Some(identity.session)))
    }

    /// Arms the session gate for `debt_op`. Only a normal primary with an
    /// empty pipeline lists its retry slice: the gate then keeps every session
    /// request out until the payment built from that list commits.
    #[must_use]
    pub fn observe_retirement_debt(&self, debt_op: u64) -> RetirementDebtStatus {
        self.retirement_debt
            .set(self.retirement_debt.get().max(debt_op));
        if self.retirement_paid_through >= debt_op {
            return RetirementDebtStatus::Paid;
        }
        let consensus = &self.consensus;
        let can_pay = self.fatal.is_none()
            && !self.requires_state_transfer()
            && consensus.is_primary()
            && consensus.is_normal()
            && !consensus.is_transferring()
            && consensus.commit_min() == consensus.commit_max()
            && consensus.pipeline_is_empty();
        RetirementDebtStatus::Unpaid(can_pay.then(|| {
            self.dedup
                .active_sessions()
                .map(|(client_id, _, session)| (client_id, session))
                .collect()
        }))
    }

    /// Whether session requests must wait for a retirement debt payment.
    #[must_use]
    pub const fn owes_session_retirement(&self) -> bool {
        self.retirement_debt.get() > self.retirement_paid_through
    }

    fn pending_session_retirement(&self, client_id: u128, session: u64) -> bool {
        let matches_identity = |body: &[u8]| {
            RetireSessionsRequest::decode_from(body).is_ok_and(|request| {
                request
                    .identities
                    .iter()
                    .any(|identity| identity.client_id == client_id && identity.session == session)
            })
        };
        self.consensus.with_pipeline(|pipeline| {
            pipeline.pending_requests().any(|entry| {
                entry.message.header().operation == Operation::RetireSession
                    && matches_identity(entry.message.body())
            }) || pipeline.prepare_head().is_some_and(|head| {
                let tail = pipeline.prepare_tail().expect("pipeline head implies tail");
                (head.header.op..=tail.header.op).any(|op| {
                    pipeline.message_by_op(op).is_some_and(|entry| {
                        entry.header.operation == Operation::RetireSession
                            && self
                                .log
                                .journal()
                                .inner
                                .repair_entry(op)
                                .is_none_or(|prepare| {
                                    matches_identity(
                                        &prepare.as_slice()[iggy_binary_protocol::HEADER_SIZE..],
                                    )
                                })
                    })
                })
            })
        })
    }

    /// Apply writes the row before its walk publishes, so the row proves the
    /// install only once `commit_min` reaches its op.
    #[must_use]
    pub fn installed_consumer_group_owner(
        &self,
        installation: &InstallConsumerGroupOwnerRequest,
    ) -> Option<u64> {
        if self.fatal.is_some()
            || self.requires_state_transfer()
            || !self.consensus.is_normal()
            || self.consensus.is_transferring()
        {
            return None;
        }
        self.consumer_group_owners
            .get(&installation.group_id)
            .filter(|installed| {
                installed.installation == *installation
                    && installed.partition_op <= self.consensus.commit_min()
            })
            .map(|installed| installed.partition_op)
    }

    /// Pending, applied, or superseded in this incarnation, so no prepare is needed.
    #[must_use]
    pub fn consumer_group_owner_admitted(
        &self,
        installation: &InstallConsumerGroupOwnerRequest,
    ) -> bool {
        self.pending_owner_install.get() == Some(*installation)
            || self
                .consumer_group_owners
                .get(&installation.group_id)
                .is_some_and(|installed| {
                    let installed = installed.installation;
                    installed.incarnation == installation.incarnation
                        && (installed == *installation
                            || installed.owner.generation > installation.owner.generation)
                })
    }

    #[must_use]
    pub fn consumer_group_owner(&self, group_id: u64) -> Option<ConsumerGroupOwner> {
        self.consumer_group_owners
            .get(&group_id)
            .filter(|installed| installed.installation.incarnation == self.created_revision())
            .map(|installed| installed.installation.owner)
    }

    #[must_use]
    pub fn consumer_group_retired(&self, group_id: u64) -> bool {
        self.owner_retirement
            .as_ref()
            .is_some_and(|catalog| catalog.retires(group_id))
    }

    /// The installed generation that a group offset write with owner 0 binds
    /// to under rule H1: the installed owner is the writer's client and
    /// session, and it was decided no later than the metadata op the writer
    /// routed by. A later decision can have moved the partition away and back,
    /// so the writer's offset can predate it, and `None` leaves the write to
    /// the owner match, which refuses it.
    fn captured_offset_owner(&self, group_id: u64, header: &RoutedRequestHeader) -> Option<u64> {
        let installation = self.consumer_group_owners.get(&group_id)?.installation;
        (installation.incarnation == self.created_revision()
            && !installation.owner.is_unassigned()
            && installation.owner.client_id == header.client
            && installation.owner.session == header.session
            && installation.metadata_op <= header.minimum_metadata_op)
            .then_some(installation.owner.generation)
    }

    fn matches_consumer_owner(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        client_id: u128,
        session: u64,
        owner_generation: u64,
        operation: Operation,
    ) -> bool {
        if kind != ConsumerKind::ConsumerGroup {
            return owner_generation == 0;
        }
        let group_id = u64::from(consumer_id);
        let Some(owner) = self.consumer_group_owner(group_id) else {
            // Group ids are never reused, so no owner follows a retired group.
            // A replica can still hold its offset when the primary that retired
            // it had none, and a delete admitted under the fence can commit
            // after the catalog, so this cleanup passes at any generation.
            return operation == Operation::DeleteConsumerOffset
                && is_auto_commit_client(client_id)
                && self.consumer_group_retired(group_id);
        };
        owner.generation == owner_generation
            && (!owner.is_unassigned()
                || (operation == Operation::DeleteConsumerOffset
                    && is_auto_commit_client(client_id)))
            && (is_auto_commit_client(client_id)
                || (owner.client_id == client_id && owner.session == session))
    }

    /// Judges the installed owner, not the one metadata names: a successor
    /// can commit its install here before a later rebalance supersedes it,
    /// and metadata then still names its predecessor.
    #[must_use]
    pub fn consumer_group_drained(&self, group_id: u64) -> bool {
        if self.observed_view != self.consensus.view()
            || self.consumer_group_owner(group_id).is_none()
            || self
                .consumer_group_polls
                .get(&group_id)
                .is_some_and(|polls| Arc::strong_count(polls) > 1)
        {
            return false;
        }
        if self
            .consumer_group_polls
            .get(&group_id)
            .is_some_and(|polls| polls.load(Ordering::Relaxed))
        {
            return true;
        }
        let Ok(id) = u32::try_from(group_id) else {
            return false;
        };
        self.last_polled_offsets
            .pin()
            .get(&ConsumerGroupId(id as usize))
            .is_some_and(|polled| {
                self.durable_consumer_offsets
                    .get(ConsumerKind::ConsumerGroup, id)
                    .is_some_and(|committed| {
                        committed.committed_offset >= polled.offset.load(Ordering::Acquire)
                    })
            })
    }

    pub fn installed_history_transition(
        &self,
        transition: &TransitionPartitionHistoryRequest,
    ) -> Option<u64> {
        let installed = self.history_transition?;
        (installed.transition == *transition
            && transition.incarnation == self.created_revision()
            && installed.partition_op <= self.consensus.commit_min()
            && self.fatal.is_none()
            && !self.materialization_missing
            && !self.consensus.is_transferring())
        .then_some(installed.partition_op)
    }

    /// A committed delete fence closes this incarnation for good.
    pub const fn history_deleted(&self) -> bool {
        self.history_transition.is_some()
    }

    /// Admission drains the partition before the fence and refuses a second
    /// one, so only a replay of the installed fence can arrive after it.
    fn apply_history_transition(
        &mut self,
        transition: TransitionPartitionHistoryRequest,
        partition_op: u64,
    ) -> Result<(), IggyError> {
        if transition.incarnation != self.created_revision() {
            return Err(IggyError::HistoryUnavailable);
        }
        if let Some(installed) = self.history_transition {
            return if installed.transition == transition {
                Ok(())
            } else {
                Err(IggyError::HistoryUnavailable)
            };
        }
        self.history_transition = Some(InstalledPartitionHistory {
            transition,
            partition_op,
        });
        self.required_metadata_frontier =
            self.required_metadata_frontier.max(transition.metadata_op);
        self.pending_history_transition.set(None);
        self.fence_published.set(true);
        Ok(())
    }

    fn apply_consumer_group_owner(
        &mut self,
        installation: InstallConsumerGroupOwnerRequest,
        partition_op: u64,
    ) -> Result<(), IggyError> {
        if installation.incarnation != self.created_revision() {
            return Ok(());
        }
        if self
            .owner_retirement
            .as_ref()
            .is_some_and(|catalog| catalog.retires(installation.group_id))
        {
            return Ok(());
        }
        if self.history_deleted() {
            return Err(IggyError::HistoryUnavailable);
        }
        if let Some(installed) = self.consumer_group_owners.get(&installation.group_id) {
            if self.conflicts_with_installed_owner(&installation) {
                return Err(IggyError::InvalidCommand);
            }
            if installation.owner.generation <= installed.installation.owner.generation {
                return Ok(());
            }
        } else if self.consumer_group_owners.len()
            >= crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX as usize
        {
            return Err(IggyError::InvalidCommand);
        }
        let group_id =
            u32::try_from(installation.group_id).map_err(|_| IggyError::InvalidCommand)?;
        self.required_metadata_frontier = self
            .required_metadata_frontier
            .max(installation.metadata_op);
        self.consumer_group_owners.insert(
            installation.group_id,
            InstalledConsumerGroupOwner {
                installation,
                partition_op,
            },
        );
        self.consumer_group_polls
            .insert(installation.group_id, Arc::new(AtomicBool::new(true)));
        self.last_polled_offsets
            .pin()
            .remove(&ConsumerGroupId(group_id as usize));
        self.mark_consumer_group_offsets_dirty();
        self.fence_published.set(true);
        self.publish_poll_visibility();
        Ok(())
    }

    /// Another owner at the installed generation, or a higher generation
    /// decided no later than the installed owner. The apply refuses both, and
    /// admission refuses them before they are prepared.
    fn conflicts_with_installed_owner(
        &self,
        installation: &InstallConsumerGroupOwnerRequest,
    ) -> bool {
        self.consumer_group_owners
            .get(&installation.group_id)
            .is_some_and(|installed| {
                let installed = installed.installation;
                if installation.owner.generation == installed.owner.generation {
                    *installation != installed
                } else {
                    installation.owner.generation > installed.owner.generation
                        && installation.metadata_op <= installed.metadata_op
                }
            })
    }

    /// Retirement follows committed metadata deletion and ordered offset cleanup.
    /// Older owner fences cannot resurrect IDs absent from the retained catalog.
    ///
    /// Returns the deleted groups that a catalog keeps beside the live ones, or
    /// `None` while it would retire no row or must wait. A deleted group whose
    /// durable offset is stranded is kept: a catalog waits for a durable offset,
    /// and a stranded key gets no delete while the mark holds. A stranded key
    /// with no durable entry does not keep its group: the cleanup of a retired
    /// group deletes the offset once a primary that holds it leads. Until that
    /// delete, a file that every boot fails to read, or fails to remove when
    /// torn or corrupt, stays on disk with no row.
    #[must_use]
    pub fn retirement_kept_groups(&self, is_live: impl Fn(u64) -> bool) -> Option<Vec<u64>> {
        let capacity = self.consumer_offset_capacity_for(ConsumerKind::ConsumerGroup);
        let (kept, retired): (Vec<u64>, Vec<u64>) = self
            .consumer_group_owners
            .keys()
            .copied()
            .filter(|&group_id| !is_live(group_id))
            .partition(|&group_id| {
                u32::try_from(group_id).is_ok_and(|id| {
                    capacity.is_stranded(id)
                        && self
                            .durable_consumer_offsets
                            .contains(ConsumerKind::ConsumerGroup, id)
                })
            });
        (!retired.is_empty()
            && !self.retirement_waits(|group_id| retired.binary_search(&group_id).is_ok()))
        .then_some(kept)
    }

    /// A catalog waits until each group it retires is unassigned and has no
    /// durable offset, so its apply never depends on local offset files.
    fn retirement_waits(&self, retires: impl Fn(u64) -> bool) -> bool {
        self.consumer_group_owners
            .iter()
            .any(|(&group_id, installed)| {
                retires(group_id)
                    && (!installed.installation.owner.is_unassigned()
                        || u32::try_from(group_id).is_ok_and(|id| {
                            self.durable_consumer_offsets
                                .contains(ConsumerKind::ConsumerGroup, id)
                        }))
            })
    }

    fn apply_consumer_group_owner_retirement(
        &mut self,
        mut catalog: RetireConsumerGroupOwnersRequest,
    ) -> Result<(), IggyError> {
        if catalog.incarnation != self.created_revision() {
            return Ok(());
        }
        if let Some(installed) = &self.owner_retirement {
            if catalog.metadata_op < installed.metadata_op {
                return Ok(());
            }
            if catalog.next_group_id < installed.next_group_id
                || (catalog.metadata_op == installed.metadata_op
                    && catalog.next_group_id != installed.next_group_id)
            {
                return Err(IggyError::InvalidCommand);
            }
            // Each primary keeps the deleted groups it saw with a durable
            // stranded offset, so a catalog can keep a group that an earlier
            // one retired.
            catalog
                .live_group_ids
                .retain(|&group_id| !installed.retires(group_id));
        }
        if self.consumer_group_owners.iter().any(|(&id, installed)| {
            catalog.retires(id) && !installed.installation.owner.is_unassigned()
        }) {
            return Err(IggyError::InvalidCommand);
        }
        let mut removed = Vec::new();
        self.consumer_group_owners.retain(|&group_id, _| {
            let prune = catalog.retires(group_id);
            if prune {
                removed.push(group_id);
            }
            !prune
        });
        for group_id in removed {
            self.consumer_group_polls.remove(&group_id);
            // Last-polled keys come from u32 group ids only.
            if let Ok(id) = u32::try_from(group_id) {
                self.last_polled_offsets
                    .pin()
                    .remove(&ConsumerGroupId(id as usize));
            }
        }
        self.required_metadata_frontier = self.required_metadata_frontier.max(catalog.metadata_op);
        self.owner_retirement = Some(catalog);
        self.publish_poll_visibility();
        Ok(())
    }

    #[must_use]
    pub const fn consensus(&self) -> &VsrConsensus<B> {
        &self.consensus
    }

    /// The committed producer table, ascending by producer id, as a
    /// checkpoint or a transfer offer carries it.
    pub(crate) fn producer_entries(&self) -> Vec<(u64, ProducerEntry)> {
        self.producers
            .entries()
            .map(|(producer_id, entry)| (producer_id, *entry))
            .collect()
    }

    /// Replace the committed producer table with the one a checkpoint or a
    /// transfer carried.
    pub(crate) fn restore_producers(&mut self, entries: Vec<(u64, ProducerEntry)>) {
        self.producers = ProducerTable::from_entries(entries);
    }

    /// This group's dedup slice. Read at admission to classify a request,
    /// written only from the commit path.
    #[must_use]
    pub(crate) const fn dedup(&self) -> &ClientTable {
        &self.dedup
    }

    /// Mutable slice, for the commit path and state-transfer install.
    pub(crate) const fn dedup_mut(&mut self) -> &mut ClientTable {
        &mut self.dedup
    }

    /// Size the dedup slice to `[partition] dedup_clients_max`. Boot-only:
    /// `set_capacity` replaces the table rather than evicting into the new
    /// bound, and panics if the slice already holds an entry. Config
    /// validation rejects a zero cap before it can reach here.
    pub fn set_dedup_clients_max(&mut self, clients_max: usize) {
        self.dedup.set_capacity(clients_max);
    }

    /// Set the per-kind durable consumer-offset limit for this partition.
    pub fn set_consumer_offsets_max(&mut self, offsets_max: usize) {
        for capacity in &self.consumer_offset_capacities {
            capacity.set_limit(offsets_max);
        }
    }

    #[must_use]
    pub fn with_in_memory_storage(
        stats: Arc<PartitionStats>,
        consensus: VsrConsensus<B>,
        segment_size: IggyByteSize,
    ) -> Self {
        let mut partition = Self::new(stats, consensus);
        let start_offset = 0;
        let segment = Segment::new(start_offset, segment_size);
        let storage = SegmentStorage::default();
        partition
            .log
            .add_persisted_segment(segment, storage, None, None);
        partition.offset.store(start_offset, Ordering::Release);
        partition
            .dirty_offset
            .store(start_offset, Ordering::Relaxed);
        partition.set_offset_space_used(false);
        partition.stats.increment_segments_count(1);
        partition
    }

    pub fn set_persistence_notifier(&self, notifier: PersistenceNotifier) {
        if let Some(persistence) = &self.persistence {
            persistence.set_notifier(notifier);
        }
    }

    pub fn capture_teardown(&self) -> crate::PartitionTeardown {
        let teardown = self.teardown_handle();
        teardown.delete();
        teardown
    }

    pub(crate) fn teardown_handle(&self) -> crate::PartitionTeardown {
        crate::PartitionTeardown {
            io: Rc::clone(&self.io_active),
            persistence: self.persistence.clone(),
            offset_files: Rc::clone(&self.workerless_offset_files),
        }
    }

    pub fn begin_quarantine(&mut self, frontier: u64) {
        self.cancel_primary_transfer();
        if self
            .superblock_intent
            .get()
            .is_some_and(|intent| !intent.final_write())
        {
            self.superblock_intent.set(None);
        }
        if !matches!(
            self.transition,
            Some(PendingPartitionTransition::Quarantine { .. })
        ) {
            self.transition = Some(PendingPartitionTransition::Quarantine {
                frontier,
                drain: None,
                phase: QuarantinePhase::Drain,
            });
        }
        self.io_active.retire();
        self.notify_io();
    }

    async fn drive_quarantine_io(&mut self) -> crate::PartitionIoStep {
        let Some(PendingPartitionTransition::Quarantine {
            frontier,
            mut drain,
            mut phase,
        }) = self.transition.take()
        else {
            return crate::PartitionIoStep::Pending;
        };
        let mut step = crate::PartitionIoStep::Progress;
        match phase {
            QuarantinePhase::Drain => {
                self.pending_commit = None;
                self.materialization = None;
                self.no_ack.clear();
                self.workerless_offset_files.clear();
                if let Some(persistence) = &self.persistence {
                    if drain.is_none() {
                        persistence.retire();
                        drain = Some(persistence.begin_drain());
                    }
                    match persistence
                        .observe_drain(drain.as_ref().expect("quarantine drain exists"))
                    {
                        Ok(true) => phase = QuarantinePhase::Frontier,
                        Ok(false) => step = crate::PartitionIoStep::Pending,
                        Err(error) => phase = QuarantinePhase::Done(Err(error)),
                    }
                } else {
                    phase = QuarantinePhase::Frontier;
                }
            }
            QuarantinePhase::Frontier => match self
                .transition_superblock(SuperblockIntent::Quarantine(frontier))
                .await
            {
                crate::PartitionIoVerdict::Pending => step = crate::PartitionIoStep::Pending,
                verdict => {
                    if verdict == crate::PartitionIoVerdict::Failed {
                        error!(
                            namespace_raw = self.namespace().inner(),
                            "quarantine frontier write failed; retaining the last durable record"
                        );
                    }
                    phase = QuarantinePhase::Files;
                }
            },
            QuarantinePhase::Files => {
                if self.partition_dir.is_none() {
                    phase = QuarantinePhase::Done(Ok(None));
                } else {
                    match self.io_plan(crate::PartitionIoContinuation::Quarantine) {
                        Ok(ready) => step = ready,
                        Err(error) => {
                            phase = QuarantinePhase::Done(Err(std::io::Error::other(error)));
                        }
                    }
                }
            }
            QuarantinePhase::Submitted => step = crate::PartitionIoStep::Pending,
            QuarantinePhase::Done(outcome) => {
                self.maintenance.shutdown = Some(PartitionShutdownPhase::Complete);
                return crate::PartitionIoStep::QuarantineFinished(outcome);
            }
        }
        self.transition = Some(PendingPartitionTransition::Quarantine {
            frontier,
            drain,
            phase,
        });
        step
    }

    /// # Errors
    /// Returns an error if durable prepare history cannot be opened or replayed.
    pub async fn open_persistence(&mut self) -> Result<(), IggyError> {
        self.open_persistence_with_capacity(
            journal::partition_journal::PARTITION_WAL_BYTES_MAX,
            std::time::Duration::ZERO,
        )
        .await
    }

    /// # Errors
    /// Returns an error if durable prepare history cannot be opened or replayed.
    pub async fn open_persistence_with_capacity(
        &mut self,
        capacity: u64,
        group_commit_delay: std::time::Duration,
    ) -> Result<(), IggyError> {
        self.open_persistence_with_recovered(capacity, group_commit_delay, None)
            .await
    }

    /// Open prepare history for every disk partition, including replicated topics.
    ///
    /// `prepares-{created_revision}/` holds `prepares-{generation}.wal`, the
    /// published `frontier`, and `segment-{generation}-{start_offset}.log` hard
    /// links retaining message bodies. Replicated sends still incur buffered WAL
    /// writes and link bookkeeping, but do not require an fsync before acknowledgement.
    /// Persisted messages, persisted consumer offsets and session retirement request
    /// sync barriers; checkpoints also sync before reclaiming history.
    ///
    /// A supplied WAL has already had its receipt checkpoint restored before
    /// segment recovery; opening it again must not repeat that disk scan.
    ///
    /// # Errors
    /// Returns an error if durable history cannot be opened, migrated, or replayed.
    #[allow(clippy::too_many_lines)]
    pub async fn open_persistence_with_recovered(
        &mut self,
        capacity: u64,
        group_commit_delay: std::time::Duration,
        recovered: Option<(Rc<PartitionPersistence>, Vec<Message<PrepareHeader>>)>,
    ) -> Result<(), IggyError> {
        let partition_directory = self
            .partition_dir
            .as_ref()
            .ok_or(IggyError::CannotReadFile)?;
        let directory = std::path::Path::new(partition_directory)
            .join(format!("prepares-{}", self.created_revision()));
        if recovered.is_none()
            && self.consensus.replica_count() > 1
            && !self.consensus.recovery_election_allowed()
        {
            PartitionPersistence::fence_missing_history(&directory, self.created_revision())
                .await
                .map_err(|error| {
                    warn!(%error, "cannot fence missing partition prepare history");
                    IggyError::CannotSyncFile
                })?;
        }
        self.materialization_missing = crate::state_transfer::materialization_is_missing(
            partition_directory,
            self.created_revision(),
        )
        .await
        .map_err(|_| IggyError::CannotReadFile)?;
        self.ensure_materialization_recovery();
        let restore_receipts = recovered.is_none();
        let (persistence, prepares) = if let Some(recovered) = recovered {
            recovered
        } else {
            PartitionPersistence::open_with_capacity(
                &directory,
                self.namespace().inner(),
                self.created_revision(),
                crate::WalStorage::default(),
                capacity,
                self.runtime_options
                    .preallocate_segments
                    .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS),
            )
            .await
            .map_err(|error| {
                warn!(%error, "cannot open partition prepare WAL");
                IggyError::CannotReadFile
            })?
        };
        persistence.set_group_commit_delay(group_commit_delay);
        if restore_receipts {
            self.restore_retry_checkpoint(persistence.checkpoint_op())
                .await
                .map_err(|error| {
                    warn!(%error, checkpoint_op = persistence.checkpoint_op(), "cannot restore partition retry protection");
                    IggyError::StateFileCorrupted
                })?;
        }
        if !self.materialization_missing {
            let segment = self.log.active_segment();
            let length = segment.size.as_bytes_u64();
            let initial = journal::partition_journal::SegmentPosition {
                start_offset: segment.start_offset,
                length,
                next_offset: if length == 0 {
                    segment.start_offset
                } else {
                    segment
                        .end_offset
                        .checked_add(1)
                        .ok_or(IggyError::CannotReadFile)?
                },
                first_timestamp: segment.start_timestamp,
            };
            persistence.enable_segment_storage(
                initial,
                segment.max_size.as_bytes_u64(),
                self.effective_segment_max_age(),
            );
            if persistence.start() {
                self.consensus
                    .message_bus()
                    .spawn(Rc::clone(&persistence).run());
            }
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|error| {
                    warn!(%error, "cannot enable partition segment persistence");
                    IggyError::CannotSyncFile
                })?;
        }
        self.restore_certified_log_view(&persistence).await?;
        if !self.materialization_missing && persistence.recovered_frontier() {
            self.consensus.set_recovery_election_allowed(true);
        }
        self.workerless_offset_files.clear();
        let checkpoint = persistence.checkpoint_op();
        let head = persistence.head();
        let mut commit = prepares
            .iter()
            .filter(|message| message.header().op != checkpoint)
            .fold(checkpoint, |commit, message| {
                commit.max(message.header().commit.min(head))
            });
        if self.materialization_missing {
            self.unreplayed_commit = commit;
            self.persistence = Some(persistence);
            self.report_wal_backlog_to_reserve();
            self.consensus.resume_adopted_view();
            return Ok(());
        }
        for message in prepares {
            let header = *message.header();
            if header.op == checkpoint {
                self.log
                    .journal()
                    .inner
                    .restore_checkpoint_prepare(checkpoint, message.into_frozen());
                continue;
            }
            if header.operation == Operation::SendMessages {
                self.append_repaired_send_messages(message).await?;
            } else {
                self.apply_replicated_operation(message).await?;
            }
        }
        if persistence.holds_commands() {
            self.log.journal().inner.retain_commands_from(checkpoint);
        }
        if head > 0 {
            if self.consensus.replica_count() == 1 {
                commit = head;
            }
            self.consensus.sequencer().set_sequence(head);
            if let Some(checksum) = persistence.checksum(head) {
                self.consensus.set_last_prepare_checksum(checksum);
            }
            self.consensus.restore_commit_state(checkpoint, commit);
            self.recovered_commit = commit;
        }
        // Recovery can reuse completed writes whose last barrier was interrupted.
        // Include them once before reclaiming any recovered WAL history.
        for segment in self.log.segments() {
            persistence.mark_segment_dirty(segment.start_offset);
        }
        for kind in ConsumerKind::ALL {
            self.durable_consumer_offsets.with_entries(kind, |entries| {
                for consumer_id in entries.keys() {
                    persistence.mark_offset_dirty(kind.index(), *consumer_id, true);
                }
            });
        }
        self.persistence = Some(persistence);
        self.report_wal_backlog_to_reserve();
        self.consensus.resume_adopted_view();
        Ok(())
    }

    async fn restore_certified_log_view(
        &mut self,
        persistence: &Rc<PartitionPersistence>,
    ) -> Result<(), IggyError> {
        // New empty WALs carry view zero; positive certificates belong to recovered history.
        if !self.materialization_missing
            && self.recovered_log_view.is_none()
            && persistence.head() == 0
            && matches!(persistence.certified_log_view(), None | Some(0))
        {
            persistence.certify_log_view(self.consensus.log_view(), 0, 0);
            if persistence.start() {
                self.consensus
                    .message_bus()
                    .spawn(Rc::clone(persistence).run());
            }
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|_| IggyError::CannotSyncFile)?;
        }
        self.restore_recovered_log_view(persistence, !self.materialization_missing);
        Ok(())
    }

    fn restore_recovered_log_view(
        &mut self,
        persistence: &PartitionPersistence,
        restore_certificate: bool,
    ) {
        if restore_certificate {
            match persistence.certified_log_view() {
                Some(view) if view >= self.consensus.log_view() => {
                    if view > self.consensus.view() {
                        self.consensus.set_view(view);
                    }
                    self.consensus.set_log_view(view);
                }
                _ => {
                    self.materialization_missing = true;
                    self.ensure_materialization_recovery();
                }
            }
        }
        self.recovered_unproven = persistence.head() > 0;
    }

    pub const fn requires_state_transfer(&self) -> bool {
        self.materialization_missing
    }

    pub fn ensure_materialization_recovery(&self) {
        self.refresh_recovered_log_complete();
        if self.materialization_missing
            && (self.consensus.state_transfer_stage() == consensus::StateTransferStage::Idle
                || self.consensus.status() == consensus::Status::ViewChange)
        {
            // Without a certified log this replica has nothing to elect on, and
            // the shard drops its votes, so an election after unanswered probes
            // only lifts its view above the primary that must answer them.
            self.consensus.set_recovery_election_allowed(false);
            self.consensus.begin_view_probe();
            if self.consensus.state_transfer_stage() == consensus::StateTransferStage::Idle {
                self.consensus.begin_state_transfer_await();
            }
        }
    }

    pub async fn on_persistence_completed(
        &mut self,
        completion: PersistenceCompletion,
        config: &PartitionsConfig,
    ) {
        if !self
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.accepts_completion(completion))
        {
            return;
        }
        self.drive_persistence().await;
        if self.fatal.is_none()
            && let Some(through_op) = self.pending_retry_checkpoint
            && let Some(persistence) = &self.persistence
            && persistence.checkpoint_op() >= through_op
        {
            let checkpoint_op = persistence.checkpoint_op();
            self.pending_retry_checkpoint = None;
            if let Err(error) = self.reclaim_retry_checkpoints(checkpoint_op).await {
                warn!(%error, namespace_raw = self.namespace().inner(), "cannot reclaim obsolete receipt checkpoints");
            }
        }
        if self.consensus.is_normal()
            && !self.consensus.is_transferring()
            && self.consensus.commit_min() < self.consensus.commit_max()
        {
            self.commit_journal(config).await;
        }
        self.notify_io();
    }

    pub fn needs_persistence_checkpoint(&self) -> bool {
        self.persistence_checkpoint_through().is_some()
    }

    /// The op a checkpoint started now would cover. The shard tick starts one
    /// whenever this holds, so a checkpoint that declined where this says yes
    /// would spend every tick on a no-op.
    ///
    /// Receipts are captured at `commit_min`, so the checkpoint covers exactly
    /// that op, and only once the WAL holds it.
    fn persistence_checkpoint_through(&self) -> Option<u64> {
        let persistence = self.persistence.as_ref()?;
        let through_op = self.consensus.commit_min();
        (self.fatal.is_none()
            && self.maintenance.checkpoint.is_none()
            && !self.checkpoint_waits_for_commit()
            && !self.checkpoint_waits_for_advances()
            && persistence.needs_checkpoint()
            && through_op > persistence.checkpoint_op()
            && through_op <= persistence.head())
        .then_some(through_op)
    }

    pub async fn checkpoint_persistence(&mut self, config: &PartitionsConfig) {
        let (Some(through_op), Some(persistence)) = (
            self.persistence_checkpoint_through(),
            self.persistence.clone(),
        ) else {
            return;
        };
        if self.has_io_dispatcher() {
            match self.encode_retry_checkpoint(through_op) {
                Ok(receipts) => {
                    self.maintenance.checkpoint =
                        Some(PendingCheckpoint::Receipts(through_op, receipts));
                    self.notify_io();
                }
                Err(error) => {
                    error!(%error, namespace_raw = self.namespace().inner(), "partition receipt checkpoint failed");
                    self.fatal = Some(FatalCommit {
                        namespace_raw: self.namespace().inner(),
                        op: through_op,
                        operation: None,
                    });
                }
            }
            return;
        }
        match self.commit_messages_inner(config, true, through_op).await {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                error!(%error, namespace_raw = self.namespace().inner(), "partition checkpoint failed");
                self.fatal = Some(FatalCommit {
                    namespace_raw: self.namespace().inner(),
                    op: through_op,
                    operation: None,
                });
                return;
            }
        }
        let mut barriers = Vec::new();
        if let Some(writer) = self.log.index_writers().last().and_then(Option::as_ref) {
            if let Err(error) = writer.fsync().await {
                error!(%error, namespace_raw = self.namespace().inner(), "partition checkpoint index sync failed");
                self.fatal = Some(FatalCommit {
                    namespace_raw: self.namespace().inner(),
                    op: through_op,
                    operation: None,
                });
                return;
            }
            // [`IggyIndexWriter::fsync`] used the original descriptor above.
            // The marker tells [`PartitionPersistence::checkpoint_files`] not
            // to replace that proof with a fresh-handle sync.
            barriers.push(FileSyncBarrier::already_synced(writer.path()));
        }
        if let Err(error) = self.persist_retry_checkpoint(through_op).await {
            error!(%error, namespace_raw = self.namespace().inner(), "partition receipt checkpoint failed");
            self.fatal = Some(FatalCommit {
                namespace_raw: self.namespace().inner(),
                op: through_op,
                operation: None,
            });
            return;
        }
        // Receipt publication already syncs the file and its directory.
        let (files, directories) = self.persistence_checkpoint_files(config);
        persistence.checkpoint_files(through_op, files, directories, barriers);
        self.pending_retry_checkpoint = Some(through_op);
        self.start_persistence();
    }

    /// Queue original-writer syncs before the install backup is published.
    /// This runs even when the checkpoint frontier cannot advance.
    pub(crate) fn barrier_install_files_locked(&self) -> BTreeSet<PathBuf> {
        let Some(persistence) = &self.persistence else {
            return BTreeSet::new();
        };
        let mut barriers = Vec::with_capacity(2);
        if let Some(writer) = self.log.messages_writers().last().and_then(Option::as_ref) {
            let path = writer.path();
            let writer = Rc::clone(writer);
            barriers.push(FileSyncBarrier::from_future(path, async move {
                writer.fsync().await.map_err(std::io::Error::other)
            }));
        }
        if let Some(writer) = self.log.index_writers().last().and_then(Option::as_ref) {
            let path = writer.path().to_owned();
            let writer = Rc::clone(writer);
            barriers.push(FileSyncBarrier::from_future(path, async move {
                writer.fsync().await.map_err(std::io::Error::other)
            }));
        }
        persistence.barrier_files(barriers)
    }

    fn persistence_checkpoint_files(
        &self,
        config: &PartitionsConfig,
    ) -> (Vec<std::path::PathBuf>, Vec<std::path::PathBuf>) {
        let namespace = self.namespace();
        let Some(persistence) = &self.persistence else {
            return (Vec::new(), Vec::new());
        };
        let (segments, offsets, dirty_directories) = persistence.take_dirty_files();
        let mut paths = Vec::with_capacity(
            segments.len() * 2
                + offsets
                    .iter()
                    .map(std::collections::BTreeSet::len)
                    .sum::<usize>(),
        );
        for start_offset in segments {
            // Retention may already have removed a dirty sealed segment, and
            // an evicted one has no `.log` left to sync: boot marks every
            // recovered segment dirty, remote ones included.
            let local = self
                .log
                .segments()
                .binary_search_by_key(&start_offset, |segment| segment.start_offset)
                .is_ok_and(|slot| self.log.segments()[slot].location != SegmentLocation::Remote);
            if !local {
                continue;
            }
            paths.push(config.get_messages_path(
                namespace.stream_id(),
                namespace.topic_id(),
                namespace.partition_id(),
                start_offset,
            ));
            paths.push(config.get_index_path(
                namespace.stream_id(),
                namespace.topic_id(),
                namespace.partition_id(),
                start_offset,
            ));
        }
        for (kind, consumers) in ConsumerKind::ALL.into_iter().zip(offsets) {
            for consumer_id in consumers {
                if self.durable_consumer_offsets.contains(kind, consumer_id)
                    && let Some(path) = self.persisted_offset_path(kind, consumer_id)
                {
                    paths.push(path);
                }
            }
        }
        let mut directories = Vec::with_capacity(ConsumerKind::COUNT + 2);
        for (directory, dirty) in self.consumer_offset_dirs.iter().zip(dirty_directories) {
            let Some(directory) = directory.as_ref().filter(|_| dirty) else {
                continue;
            };
            let path = std::path::PathBuf::from(directory);
            directories.push(path.clone());
            if let Some(parent) = path.parent()
                && !directories.iter().any(|existing| existing == parent)
            {
                directories.push(parent.to_path_buf());
            }
        }
        if let Some(directory) = &self.partition_dir {
            directories.push(std::path::PathBuf::from(directory));
        }
        (
            paths.into_iter().map(std::path::PathBuf::from).collect(),
            directories,
        )
    }

    fn drive_checkpoint_io(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Option<crate::PartitionIoStep>, IggyError> {
        let Some(checkpoint) = &self.maintenance.checkpoint else {
            return Ok(None);
        };
        match *checkpoint {
            PendingCheckpoint::Receipts(..) | PendingCheckpoint::Reclaim(_) => self
                .io_plan(crate::PartitionIoContinuation::Checkpoint)
                .map(Some),
            PendingCheckpoint::ReceiptsWriting(_)
            | PendingCheckpoint::Syncing(_)
            | PendingCheckpoint::Reclaiming => Ok(Some(crate::PartitionIoStep::Pending)),
            PendingCheckpoint::Flush(through) => {
                if !self.begin_materialization(
                    through,
                    self.effective_size_of_messages_required_to_save(config),
                )? {
                    return Ok(Some(crate::PartitionIoStep::Pending));
                }
                if self.materialization.is_none() {
                    self.maintenance.checkpoint = Some(PendingCheckpoint::Index(through));
                }
                Ok(Some(crate::PartitionIoStep::Progress))
            }
            PendingCheckpoint::Index(through) => {
                if self
                    .log
                    .index_writers()
                    .last()
                    .and_then(Option::as_ref)
                    .is_some()
                {
                    self.io_plan(crate::PartitionIoContinuation::Checkpoint)
                        .map(Some)
                } else {
                    self.maintenance.checkpoint = Some(PendingCheckpoint::Enqueue(through, None));
                    Ok(Some(crate::PartitionIoStep::Progress))
                }
            }
            PendingCheckpoint::Enqueue(through, ref synced_index) => {
                let barriers = synced_index
                    .iter()
                    .map(FileSyncBarrier::already_synced)
                    .collect();
                let (files, directories) = self.persistence_checkpoint_files(config);
                if let Some(persistence) = &self.persistence {
                    persistence.checkpoint_files(through, files, directories, barriers);
                    self.start_persistence();
                }
                self.maintenance.checkpoint = Some(PendingCheckpoint::Wal(through));
                Ok(Some(crate::PartitionIoStep::Progress))
            }
            PendingCheckpoint::Wal(through) => {
                let Some(persistence) = &self.persistence else {
                    self.maintenance.checkpoint = None;
                    return Ok(None);
                };
                let checkpoint_op = persistence.checkpoint_op();
                if checkpoint_op < through {
                    return Ok(None);
                }
                self.maintenance.checkpoint = Some(PendingCheckpoint::Reclaim(checkpoint_op));
                Ok(Some(crate::PartitionIoStep::Progress))
            }
        }
    }

    pub(crate) fn cancel_history_maintenance(&mut self) {
        self.maintenance.checkpoint = None;
        self.maintenance.retention = None;
        self.maintenance.eviction = None;
    }

    fn capture_checkpoint_phase(
        &mut self,
    ) -> Result<Option<crate::io::PartitionIoJob<SB>>, IggyError> {
        let job = match &mut self.maintenance.checkpoint {
            Some(PendingCheckpoint::Receipts(through, _)) => {
                let through = *through;
                let path = self
                    .retry_checkpoint_path(through)
                    .map_err(|_| IggyError::CannotWriteToFile)?;
                let Some(PendingCheckpoint::Receipts(_, bytes)) = self
                    .maintenance
                    .checkpoint
                    .replace(PendingCheckpoint::ReceiptsWriting(through))
                else {
                    unreachable!("checkpoint phase is unchanged during capture");
                };
                crate::PartitionIoJob::RetryCheckpoint { path, bytes }
            }
            Some(PendingCheckpoint::Index(through)) => {
                let through = *through;
                let writer = self
                    .log
                    .index_writers()
                    .last()
                    .and_then(Option::as_ref)
                    .ok_or(IggyError::CannotSyncFile)?;
                let job = crate::PartitionIoJob::IndexSync(Rc::clone(writer));
                self.maintenance.checkpoint = Some(PendingCheckpoint::Syncing(through));
                job
            }
            Some(PendingCheckpoint::Reclaim(through)) => {
                let through = *through;
                let current = self
                    .retry_checkpoint_path(through)
                    .map_err(|_| IggyError::CannotDeleteFile)?;
                self.maintenance.checkpoint = Some(PendingCheckpoint::Reclaiming);
                crate::PartitionIoJob::ReclaimRetryCheckpoints {
                    current,
                    keep_current: through > 0,
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(job))
    }

    fn accept_checkpoint_io(&mut self, result: crate::PartitionIoResult) -> Result<(), IggyError> {
        match (self.maintenance.checkpoint.take(), result) {
            (
                Some(PendingCheckpoint::ReceiptsWriting(through)),
                crate::PartitionIoResult::RetryCheckpoint(outcome),
            ) => {
                if let Err(error) = outcome {
                    error!(%error, namespace_raw = self.namespace().inner(), "partition receipt checkpoint failed");
                    return Err(IggyError::CannotSyncFile);
                }
                self.maintenance.checkpoint = Some(PendingCheckpoint::Flush(through));
                Ok(())
            }
            (
                Some(PendingCheckpoint::Syncing(through)),
                crate::PartitionIoResult::IndexSync { writer, outcome },
            ) => {
                outcome?;
                if !self
                    .log
                    .index_writers()
                    .last()
                    .and_then(Option::as_ref)
                    .is_some_and(|installed| Rc::ptr_eq(installed, &writer))
                {
                    return Err(IggyError::CannotSyncFile);
                }
                self.maintenance.checkpoint = Some(PendingCheckpoint::Enqueue(
                    through,
                    Some(writer.path().to_owned()),
                ));
                Ok(())
            }
            (
                Some(PendingCheckpoint::Reclaiming),
                crate::PartitionIoResult::ReclaimRetryCheckpoints(outcome),
            ) => {
                if let Err(error) = outcome {
                    warn!(%error, namespace_raw = self.namespace().inner(), "cannot reclaim obsolete receipt checkpoints");
                }
                Ok(())
            }
            (checkpoint, _) => {
                self.maintenance.checkpoint = checkpoint;
                Err(IggyError::CannotSyncFile)
            }
        }
    }

    pub fn take_persistence_metrics(&self) -> Option<crate::persistence::PersistenceMetrics> {
        self.persistence
            .as_ref()
            .map(|persistence| persistence.take_metrics())
    }

    /// Entries and bytes this partition's repair ring pins right now.
    pub fn repair_ring_occupancy(&self) -> (usize, u64) {
        self.log.journal().inner.evicted_ring_occupancy()
    }

    fn persistence_checkpoint_pending(&self) -> bool {
        self.persistence
            .as_ref()
            .is_some_and(|persistence| persistence.checkpoint_pending())
    }

    /// Persisted commits keep their earned quorum and reply slots in the
    /// pipeline while the checkpoint worker synchronizes this partition's
    /// materialized files. Replicated commits continue.
    fn commit_waits_for_checkpoint(&self) -> bool {
        self.durability().is_persisted() && self.persistence_checkpoint_pending()
    }

    /// A lifecycle fence applies its owner and history state before its commit
    /// publishes `commit_min`. Receipts captured in between would hold an op
    /// past their own frontier, which recovery refuses.
    fn checkpoint_waits_for_commit(&self) -> bool {
        self.pending_commit.as_ref().is_some_and(|pending| {
            pending
                .headers
                .iter()
                .any(|header| header.operation.is_partition_lifecycle())
        })
    }

    /// A checkpoint may still need to open the deleted path. In-place writes
    /// preserve that path and remain covered by the uncheckpointed WAL tail.
    fn offset_effect_waits_for_checkpoint(&self, plan: &ConsumerOffsetPlan) -> bool {
        matches!(plan.action, ConsumerOffsetAction::Delete) && self.persistence_checkpoint_pending()
    }

    pub async fn drive_persistence(&mut self) {
        self.refresh_proof_walk_memory();
        if self.fatal.is_some() {
            return;
        }
        let Some(persistence) = self.persistence.as_ref() else {
            return;
        };
        if let Some(error) = persistence.failure() {
            error!(%error, namespace_raw = self.namespace().inner(), "partition prepare persistence failed");
            if self.fatal.is_none() {
                self.fatal = Some(FatalCommit {
                    namespace_raw: self.namespace().inner(),
                    op: persistence.head(),
                    operation: Some(persistence.failure_operation()),
                });
            }
            return;
        }
        if self.materialization_missing || !self.ensure_wal_view() {
            return;
        }
        // The file lane must persist the superblock before WAL acks can advance.
        if self.superblock.is_some() && self.consensus.needs_superblock_persist() {
            return;
        }
        loop {
            let pending = self
                .pending_persisted_acks
                .borrow()
                .first_key_value()
                .map(|(_, header)| *header);
            let Some(header) = pending else {
                break;
            };
            if header.op > persistence.head() {
                break;
            }
            if persistence.checksum(header.op) == Some(header.checksum)
                && !self.send_prepare_ok(&header).await
            {
                break;
            }
            self.pending_persisted_acks.borrow_mut().remove(&header.op);
        }
    }

    /// `VsrAction::RetryLocalWrite`: re-submit the journaled WAL tail, then
    /// re-arm the self-ack for `op` unless it is already armed.
    pub async fn retry_local_write(&self, op: u64) {
        // Only a primary in normal status owns a self-ack to re-drive.
        if !self.consensus.is_primary() || !self.consensus.is_normal() {
            return;
        }
        self.persist_repaired_prefix();
        if self.pending_persisted_acks.borrow().contains_key(&op) {
            return;
        }
        self.acknowledge_prepare(op).await;
    }

    pub async fn acknowledge_prepare(&self, op: u64) {
        let Some(prepare) = self.log.journal().inner.repair_entry(op) else {
            return;
        };
        let Ok(header) = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            &prepare.as_slice()[..size_of::<PrepareHeader>()],
        ) else {
            return;
        };
        let header = *header;
        self.persist_repaired_prefix();
        self.send_prepare_ok(&header).await;
    }

    const fn requires_persistence(&self, operation: Operation) -> bool {
        match operation {
            Operation::RetireSession | Operation::StateMachineCommand => true,
            Operation::SendMessages => self.durability().is_persisted(),
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                self.consumer_offset_durability().is_persisted()
            }
            _ => operation.is_partition_lifecycle(),
        }
    }

    /// Whether the WAL still certifies `log_view` over this journal. A truncation
    /// clears the certificate at once. An entry of a later view clears it when
    /// the WAL writes that entry, and before that the journal already holds a log
    /// that `log_view` does not name.
    #[must_use]
    pub fn wal_keeps_log_view(&self) -> bool {
        let Some(persistence) = &self.persistence else {
            return true;
        };
        let log_view = self.consensus.log_view();
        let journal = &self.log.journal().inner;
        let floor = self.consensus.commit_min().max(journal.evicted_through());
        persistence.certified_log_view() == Some(log_view)
            && journal.last_op().is_none_or(|last| {
                (floor.saturating_add(1)..=last)
                    .filter_map(|op| journal.header_by_op(op))
                    .all(|header| header.view <= log_view)
            })
    }

    fn ensure_wal_view(&self) -> bool {
        let Some(persistence) = &self.persistence else {
            return true;
        };
        if persistence.certified_log_view() == Some(self.consensus.log_view()) {
            return true;
        }
        // Outside `Normal` the journal stays the log of `log_view` once that log is
        // proven, unless it is a primary-elect's merge, which `log_view` names only
        // once the view starts.
        if !self.consensus.is_normal()
            && (self.consensus.view_log_unproven() || self.consensus.view_log_merged())
        {
            return false;
        }
        // An install certifies the WAL it reset. Until it publishes its consensus,
        // the sequencer still names the history that the install replaces.
        if self.materialization_missing
            || matches!(
                self.transition,
                Some(PendingPartitionTransition::Install(_))
            )
        {
            return false;
        }
        self.persist_repaired_prefix();
        let (op, checksum) = if let Some(fence) = self.view_fence.filter(|_| self.view_fenced()) {
            // The certificate covers the whole WAL, so while fenced it may only
            // vouch for a WAL holding nothing above `commit_min`, and only when
            // the window sits below the suffix a `DoViewChange` reports.
            let floor = self.consensus.commit_min();
            if persistence.head() != floor || fence.through >= self.consensus.commit_max() {
                return false;
            }
            let checksum = if floor == 0 {
                0
            } else {
                let Some(checksum) = persistence.checksum(floor) else {
                    return false;
                };
                checksum
            };
            (floor, checksum)
        } else {
            (
                self.consensus.sequencer().current_sequence(),
                self.consensus.last_prepare_checksum(),
            )
        };
        let ready = persistence.certify_log_view(self.consensus.log_view(), op, checksum);
        self.start_persistence();
        ready
    }

    pub fn register_rebuilt_ack(&self, header: &PrepareHeader) -> bool {
        let ready = self.prepare_ack_ready(header);
        if ready {
            self.record_acknowledged_op(header.op);
        } else {
            self.pending_persisted_acks
                .borrow_mut()
                .insert(header.op, *header);
        }
        ready
    }

    fn prepare_ack_ready(&self, header: &PrepareHeader) -> bool {
        // State-transfer admission uses the accepted WAL head to protect every
        // acknowledged op, including replicated prepares adopted in a new view.
        let accepted = self
            .persistence
            .as_ref()
            .is_none_or(|persistence| header.op <= persistence.head());
        accepted
            && (!self.requires_persistence(header.operation)
                || self.persistence.as_ref().is_some_and(|persistence| {
                    persistence.is_durable(header)
                        && persistence.certified_log_view() == Some(self.consensus.log_view())
                }))
    }

    fn record_acknowledged_op(&self, op: u64) {
        self.acknowledged_op.set(self.acknowledged_op.get().max(op));
    }

    fn submit_prepare_persistence(&self, prepare: Frozen<4096>, operation: Operation) -> bool {
        let Some(persistence) = &self.persistence else {
            return self.consensus.replica_count() == 1 || !self.requires_persistence(operation);
        };
        // A previous admission may have stopped at capacity. Preserve the
        // ordered prefix before submitting a newer forwarded prepare.
        let op = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            &prepare.as_slice()[..size_of::<PrepareHeader>()],
        )
        .map_or(u64::MAX, |header| header.op);
        // Same cap as `persist_repaired_prefix_through`: a held entry can sit at
        // the WAL head, and a prepare chaining to the view's entry instead would
        // fail the WAL's parent check. The proof's release writes the journal's
        // copy once the window is proven.
        if self.view_fenced() && op > self.consensus.commit_min() {
            return false;
        }
        if op > persistence.head().saturating_add(1) {
            self.persist_repaired_prefix_through(op.saturating_sub(1));
            if op > persistence.head().saturating_add(1) {
                return false;
            }
        }
        if let Err(error) = persistence.append(prepare, self.requires_persistence(operation)) {
            warn!(%error, namespace_raw = self.namespace().inner(), "partition WAL refused prepare");
            if error.kind() != std::io::ErrorKind::WouldBlock {
                persistence.fail(error);
            }
            return false;
        }
        self.start_persistence();
        true
    }

    fn persist_repaired_prefix(&self) {
        self.persist_repaired_prefix_through(u64::MAX);
    }

    fn persist_repaired_prefix_through(&self, through: u64) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        // A held entry can sit at the WAL head, where a repaired successor that
        // chains to the view's entry instead would fail the WAL's parent check.
        let through = if self.view_fenced() {
            through.min(self.consensus.commit_min())
        } else {
            through
        };
        while let Some(op) = persistence.head().checked_add(1) {
            if op > through
                || op > self.consensus.sequencer().current_sequence()
                || !self.consensus.view_log_entry_proven(op)
            {
                break;
            }
            let Some(prepare) = self.log.journal().inner.repair_entry(op) else {
                break;
            };
            if let Err(error) = journal::partition_journal::record_length(prepare.len()) {
                persistence.fail(error);
                break;
            }
            if !persistence.has_capacity(prepare.len()) {
                break;
            }
            // A completed repair may immediately vote in a new view.
            if let Err(error) = persistence.append(prepare, true) {
                warn!(%error, op, "cannot persist repaired partition history");
                if error.kind() != std::io::ErrorKind::WouldBlock {
                    persistence.fail(error);
                }
                break;
            }
        }
        self.start_persistence();
    }

    pub(crate) fn start_persistence(&self) {
        if let Some(persistence) = &self.persistence
            && persistence.start()
        {
            self.consensus
                .message_bus()
                .spawn(Rc::clone(persistence).run());
        }
    }

    pub fn set_partition_dir(&mut self, partition_dir: String) {
        self.partition_dir = Some(partition_dir);
        self.segment_names_dirty.set(true);
    }

    /// Attach the durable superblock store the boot path opened for this
    /// partition's group, along with the record it read back. Boot seeds
    /// consensus with the recovered `(view, log_view)` and marks them durable
    /// before attaching; from then on [`Self::persist_superblock_if_needed`]
    /// keeps the record current.
    ///
    /// The record is a PARAMETER rather than a follow-up seeding call because
    /// the advance direction maxes against its frontier: an attach that left
    /// that at zero against a record naming N would let the first write after a
    /// fence lower it, which is the whole defect the field exists to prevent.
    /// As a separate call it was silently optional, and one of the three attach
    /// sites dropped it.
    pub fn set_superblock(&mut self, superblock: Rc<SB>, recovered: Option<&consensus::VsrState>) {
        self.recovered_log_view = recovered.map(|state| state.log_view);
        self.consensus
            .restore_adopted_view(recovered.and_then(|state| state.adopted_view.as_ref()));
        self.superblock = Some(superblock);
        self.durable_offset_frontier
            .set(recovered.map_or(0, |state| state.offset_frontier));
        self.durable_offset_reserved
            .set(recovered.map_or(0, |state| state.offset_reserved));
    }

    /// Persist this group's VSR state to its superblock when the view changed
    /// since the last write. The split-brain gate, partition edition: callers
    /// MUST invoke this before dispatching any view-scoped VSR message for
    /// this partition, so a replica that acted in a view can never recover an
    /// older one after a crash.
    ///
    /// It fences the SEND, not the ACT. By the time a caller reaches here the
    /// handler has already moved `view`, `log_view`, `status`, the sequencer
    /// and the pipeline, and the commit walk runs outside the gate, so a
    /// failed persist still applies committed ops locally. That is the VSR
    /// fence and it is sufficient: local state a crash forgets is state no
    /// peer ever saw, whereas an externalized view must be recoverable.
    ///
    /// `true` when the send may proceed, either because the state is now
    /// durable or because there was nothing to persist (no store attached --
    /// in-memory / simulated partitions -- or an unchanged view). `false`
    /// while a write is queued, in progress, or failed. Callers retain wire
    /// actions with [`Self::defer_wire_actions`] until persistence completes.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn persist_superblock_if_needed(&self) -> bool {
        if !self.materialization_missing
            && !self.consensus.view_log_unproven()
            && !self.ensure_wal_view()
        {
            return false;
        }
        if self.superblock.is_none() {
            let state = self.consensus.vsr_state(0, 0);
            self.consensus
                .mark_superblock_durable(state.view, state.log_view);
            self.consensus
                .mark_adopted_view_durable(state.adopted_view.as_ref());
            return true;
        }
        if !self.consensus.needs_superblock_persist() {
            return true;
        }
        self.complete_superblock_intent(SuperblockIntent::Advance {
            frontier: self.offset_frontier(),
            reserved: 0,
        })
        .await
    }

    pub fn defer_wire_actions(&self, actions: Vec<consensus::VsrAction>) {
        if actions.is_empty() || !self.has_io_dispatcher() {
            return;
        }
        let mut pending = self.pending_wire_actions.borrow_mut();
        let view = self.consensus.view();
        let log_view = self.consensus.log_view();
        if pending
            .as_ref()
            .is_some_and(|pending| pending.view != view || pending.log_view != log_view)
        {
            *pending = None;
        }
        let pending = pending.get_or_insert_with(|| PendingWireActions {
            view,
            log_view,
            actions: Vec::new(),
        });
        for action in actions {
            // Timers keep firing during a held write. Retain only their latest snapshot.
            let previous = match action {
                // A `Commit` to one backup must not displace the heartbeat to all.
                consensus::VsrAction::SendCommit { target, .. } => {
                    pending.actions.iter_mut().find(|pending| {
                        matches!(
                            pending,
                            consensus::VsrAction::SendCommit { target: held, .. } if *held == target
                        )
                    })
                }
                consensus::VsrAction::SendStartViewChange { .. }
                | consensus::VsrAction::SendDoViewChange { .. }
                | consensus::VsrAction::SendRequestStartView { .. }
                | consensus::VsrAction::RetransmitPrepares { .. } => {
                    pending.actions.iter_mut().find(|pending| {
                        std::mem::discriminant(*pending) == std::mem::discriminant(&action)
                    })
                }
                _ => None,
            };
            if let Some(previous) = previous {
                *previous = action;
            } else {
                pending.actions.push(action);
            }
        }
        self.notify_io();
    }

    /// The advance rule for the frontier, shared by every writer that claims
    /// one: never below what this replica holds, never below what the record
    /// already says. Held messages, NOT [`Self::mint_frontier`], which stands a
    /// lease block above them after a reservation-seeded boot and names none of
    /// them.
    fn advanced_frontier(&self, claim: u64) -> u64 {
        claim
            .max(self.held_offset_frontier())
            .max(self.durable_offset_frontier.get())
    }

    const fn offset_range_is_reserved(&self, end_offset: u64) -> bool {
        self.consensus.replica_count() > 1
            || self.superblock.is_none()
            || self.durable_offset_reserved.get() > end_offset
    }

    // A failed install can leave the local chain behind the durable frontier.
    // Advances also retain reservations witnessed only by the durable record.
    fn superblock_state(&self, intent: SuperblockIntent) -> consensus::VsrState {
        let mut state = self.consensus.vsr_state(0, 0);
        let (frontier, reserved) = match intent {
            SuperblockIntent::Advance { frontier, reserved } => (
                self.advanced_frontier(frontier),
                reserved.max(self.durable_offset_reserved.get()),
            ),
            SuperblockIntent::Install(frontier) => (self.advanced_frontier(frontier), frontier),
            SuperblockIntent::Quarantine(frontier) => {
                (frontier, self.durable_offset_reserved.get())
            }
            SuperblockIntent::Collapse { frontier, reserved } => (frontier, reserved),
        };
        state.offset_frontier = frontier;
        state.offset_reserved = reserved.max(frontier);
        state
    }

    fn request_superblock_intent(&self, intent: SuperblockIntent) -> crate::PartitionIoVerdict {
        if self.superblock.is_none() {
            return crate::PartitionIoVerdict::Ready;
        }
        if let SuperblockIntent::Advance { frontier, reserved } = intent {
            if !self.consensus.needs_superblock_persist()
                && self.durable_offset_frontier.get() >= frontier
                && self.durable_offset_reserved.get() >= reserved
            {
                return crate::PartitionIoVerdict::Ready;
            }
        } else if let Some((completed, succeeded)) = self.superblock_completed.get()
            && completed == intent
        {
            self.superblock_completed.set(None);
            return if succeeded {
                crate::PartitionIoVerdict::Ready
            } else {
                crate::PartitionIoVerdict::Failed
            };
        }
        if self.superblock_write_is_backed_off() && !intent.final_write() {
            return crate::PartitionIoVerdict::Failed;
        }
        if self
            .superblock_running
            .get()
            .is_some_and(|(running, _)| running == intent)
        {
            return crate::PartitionIoVerdict::Pending;
        }
        let pending = match (self.superblock_intent.get(), intent) {
            (
                Some(SuperblockIntent::Advance {
                    frontier: old_frontier,
                    reserved: old_reserved,
                }),
                SuperblockIntent::Advance { frontier, reserved },
            ) => SuperblockIntent::Advance {
                frontier: old_frontier.max(frontier),
                reserved: old_reserved.max(reserved),
            },
            (Some(pending), _)
                if pending != intent && (pending.final_write() || !intent.final_write()) =>
            {
                return crate::PartitionIoVerdict::Pending;
            }
            _ => intent,
        };
        self.superblock_intent.set(Some(pending));
        self.notify_io();
        crate::PartitionIoVerdict::Pending
    }

    async fn complete_superblock_intent(&self, intent: SuperblockIntent) -> bool {
        if self.io_notifier.borrow().is_some() {
            return self.request_superblock_intent(intent) == crate::PartitionIoVerdict::Ready;
        }
        let Some(superblock) = self.superblock.as_ref() else {
            return true;
        };
        if self.superblock_write_is_backed_off() && !intent.final_write() {
            return false;
        }
        self.write_superblock_locked(superblock, intent).await
    }

    /// Write `intent` under `superblock_lock`. The caller owns the backoff gate.
    #[allow(clippy::future_not_send)]
    async fn write_superblock_locked(&self, superblock: &Rc<SB>, intent: SuperblockIntent) -> bool {
        let _guard = self.superblock_lock.acquire().await;
        let state = self.superblock_state(intent);
        let inside_backoff_window = self.superblock_write_is_backed_off();
        let result = crate::io::SuperblockIoJob {
            superblock: Rc::clone(superblock),
            state,
        }
        .execute()
        .await;
        self.accept_superblock(result, inside_backoff_window)
    }

    fn accept_superblock(
        &self,
        result: crate::io::SuperblockIoResult,
        inside_backoff_window: bool,
    ) -> bool {
        let state = result.state;
        match result.outcome {
            Ok(()) => {
                self.consensus
                    .mark_superblock_durable(state.view, state.log_view);
                self.consensus
                    .mark_adopted_view_durable(state.adopted_view.as_ref());
                self.durable_offset_frontier.set(state.offset_frontier);
                self.durable_offset_reserved.set(state.offset_reserved);
                self.superblock_write_failures.set(0);
                self.superblock_retry_after_micros.set(0);
                true
            }
            Err(error) => {
                // One failure per backoff step. `superblock_wedged` reads the
                // count as elapsed time (the window is capped at 1 s, the
                // default threshold is 2 m), which a per-attempt count would
                // turn into seconds under a handful of retrying producers.
                if inside_backoff_window {
                    return false;
                }
                let failures = self.superblock_write_failures.get() + 1;
                self.superblock_write_failures.set(failures);
                let backoff = SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS
                    .saturating_mul(1 << failures.min(SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT))
                    .min(SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS);
                self.superblock_retry_after_micros
                    .set(self.consensus.clock_realtime_micros() + backoff);
                // Rate-limited to the backoff steps: the tick would otherwise
                // emit this every 10 ms for as long as the disk stays broken.
                if failures.is_power_of_two() {
                    tracing::error!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        replica_id = self.consensus.replica(),
                        namespace_raw = self.consensus.group(),
                        view = state.view,
                        log_view = state.log_view,
                        superblock_write_failures = failures,
                        retry_in_micros = backoff,
                        %error,
                        "partition superblock persist failed; withholding every view-scoped \
                         send for this group until it succeeds, so this replica stays \
                         quorum-invisible there"
                    );
                }
                false
            }
        }
    }

    /// Re-seed the offset counter from a recovered superblock record, taking
    /// the MAX of what the record holds and what the recovered segments already
    /// proved.
    ///
    /// The record is a lower bound, never a completeness claim: it exists
    /// because four paths leave a replica whose counter would otherwise
    /// restart below where the group already is (a transfer install of an
    /// all-GC'd origin, a crash inside the install's swap window, the
    /// fence-and-rebuild path, which needs no crash at all, and a crash while
    /// acked messages were still resident in the journal). A solo group that
    /// restarts its counter mints those offsets a second time, and a rebuilt
    /// replica of a persisted group has nothing else to anchor its chain to.
    ///
    /// A multi-replica group that acks before it persists ignores the record.
    /// A crash can take from every member a tail that the record names, and
    /// the group then reuses those offsets. A replica that recovered a log
    /// resumes where that log ends: its WAL only extends its own segment
    /// tail, and a backup refuses a batch whose `base_offset` does not
    /// continue its own counter. A rebuilt replica recovered no log and
    /// awaits a state transfer. A counter at the record makes that replica
    /// refuse every offer of a group that resumed below the record, and a
    /// group of two then never commits again, because a waiting replica
    /// acknowledges nothing. A counter below the group is a lag there: repair
    /// journals the missing ops at the offsets the group stamped and raises
    /// the counter with them, and an install takes the frontier of its offer.
    ///
    /// A persisted group keeps the raise. Its record never names an offset
    /// that the group lost: a replica acknowledges a persisted send only once
    /// its own WAL holds it durably, and the record takes the committed
    /// counter or the frontier of an installed offer, never an uncommitted
    /// counter. A record writer that breaks this, or a quorum that loses
    /// durable data, leaves the counter above the group, and the group stalls
    /// on the batch check above instead of handing those offsets out twice.
    ///
    /// The two counters are seeded separately, because the record carries two
    /// bounds. `offset_frontier` is what messages reached, so it seeds the
    /// COMMITTED counter; `offset_reserved` is what may have been handed to a
    /// client, so it seeds the APPEND counter and nothing else. Folding the
    /// reservation into the committed one would publish a `current_offset` over
    /// a lease-block hole, and `store_consumer_offset` would then admit offsets
    /// inside it.
    ///
    /// The reservation is SOLO ONLY and applies to weaker durability. Persisted
    /// topics recover exact append positions from their prepare WAL. Advancing
    /// them to a reservation before replay would strand the retained segment tail.
    /// A backup mints nothing: it journals what
    /// the primary stamped and rejects anything that does not continue its own
    /// counter, so an append point a lease block above its group would have every
    /// peer refuse the batch. A replicated group is also less exposed, since an
    /// ack there means a quorum journaled the batch and the hole needs a
    /// FULL-cluster crash.
    ///
    /// Lives HERE rather than in the server crate so the boot paths and the
    /// simulator share one implementation. A copy in the harness was a copy of
    /// the max rule that had lost the max, in the one place built to catch
    /// violations of it.
    pub fn restore_offset_frontier(&mut self, recovered: Option<&consensus::VsrState>) {
        let Some(state) = recovered else {
            return;
        };
        if self.consensus.replica_count() > 1 && !self.durability().is_persisted() {
            return;
        }
        let frontier = state.offset_frontier;
        let reserved = if self.consensus.replica_count() == 1 && !self.durability().is_persisted() {
            state.offset_reserved
        } else {
            0
        };
        // NOT `frontier > 0`: the shape a crash before the first flush leaves is
        // a zero frontier and a nonzero reservation, because the append fence
        // runs before the journal append, so the first record a partition ever
        // writes names no data at all.
        let append_point = frontier.max(reserved);
        if append_point == 0 {
            return;
        }
        let seeded = self.offset_space.append_live;
        // Each counter takes its own max, since the record's two bounds move
        // independently: a graceful stop collapses the reservation onto the
        // append point while the frontier stays where the data ended.
        let committed_restored = frontier.checked_sub(1).is_some_and(|committed_end| {
            let raise = !seeded || self.offset.load(Ordering::Acquire) < committed_end;
            if raise {
                self.offset.store(committed_end, Ordering::Release);
            }
            raise
        });
        let append_end = append_point - 1;
        let append_restored = !seeded || self.dirty_offset.load(Ordering::Relaxed) < append_end;
        if append_restored {
            self.dirty_offset.store(append_end, Ordering::Relaxed);
        }
        if !committed_restored && !append_restored {
            return;
        }
        tracing::debug!(
            namespace_raw = self.consensus().group(),
            offset_frontier = frontier,
            offset_reserved = reserved,
            append_point,
            "restored partition offset counters from its superblock"
        );
        self.offset_space.append_live = true;
        // Only the frontier names data. A record carrying a reservation alone is
        // the pre-first-flush shape, where the committed counter still seeds
        // nothing.
        self.offset_space.committed_seeded |= frontier > 0;
        self.publish_poll_visibility();
    }

    /// Path of the anchor whose lifecycle is this segment's.
    ///
    /// Anchors are unlinked with the segment they sit beside, so a stale record
    /// never `covers` the bounds a later plant reuses.
    pub(crate) fn anchor_cleanup_path(&self, start_offset: u64) -> Option<String> {
        self.partition_dir
            .as_deref()
            .map(|dir| crate::segment_anchor::anchor_path(dir, start_offset))
    }

    /// Seed or clear BOTH offset-space bits.
    ///
    /// For the callers that genuinely move both: a fresh partition (neither
    /// counter names anything) and a boot off segments (both do,
    /// because a segment on disk holds committed messages only). Everything on
    /// the live path moves ONE bit -- see [`Self::note_append_live`] and
    /// [`Self::note_committed_seeded`].
    pub fn set_offset_space_used(&mut self, used: bool) {
        self.offset_space = OffsetSpace {
            append_live: used,
            committed_seeded: used,
        };
        self.publish_poll_visibility();
    }

    /// The append counter is live: an offset has been journaled, so the next
    /// mint continues from it.
    ///
    /// APPEND only. A journaled offset is not a committed one: `build_poll_plan`
    /// gates on `OffsetSpace::committed_seeded`, and seeding that here would
    /// serve a resident offset 0 to a consumer before the first commit and let
    /// the frontier persist name data no quorum agreed on. A view change may
    /// still truncate this offset away.
    pub const fn note_append_live(&mut self) {
        self.offset_space.append_live = true;
    }

    /// The committed counter names data: an offset has passed commit, so it is
    /// pollable and the frontier may record it.
    ///
    /// Implies the append counter is live too -- nothing commits that was not
    /// journaled first -- but the reverse does not hold, which is the whole
    /// reason the two bits are separate.
    pub fn note_committed_seeded(&mut self) {
        self.offset_space.append_live = true;
        self.offset_space.committed_seeded = true;
        self.publish_poll_visibility();
    }

    /// Whether this partition ever stamped an offset, i.e. whether its offset
    /// counters describe a real offset space rather than an untouched zero. The
    /// one bit separating a partition holding one message at offset 0 from one
    /// that never took a write: both report `(0, 0)`.
    #[cfg(any(test, feature = "simulator"))]
    pub const fn offset_space_used(&self) -> bool {
        self.offset_space.append_live
    }

    /// The frontier a pending quarantine records for the rebuild, if one is
    /// pending.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub const fn quarantine_frontier(&self) -> Option<u64> {
        match &self.transition {
            Some(PendingPartitionTransition::Quarantine { frontier, .. }) => Some(*frontier),
            _ => None,
        }
    }

    /// Mark this partition's files lost, as boot does when it finds the
    /// on-disk `materialization_missing` record, which in-memory storage has no
    /// directory to hold.
    #[cfg(feature = "fault-injection")]
    pub fn force_requires_state_transfer_for_test(&mut self) {
        self.materialization_missing = true;
        self.ensure_materialization_recovery();
    }

    /// Capture the ideal-storage simulator's durable state at process loss.
    #[cfg(any(test, feature = "simulator"))]
    pub fn take_retained_state(&mut self) -> crate::RetainedPartitionState {
        crate::RetainedPartitionState {
            head_op: self.consensus().sequencer().current_sequence(),
            applied_op: self.consensus().commit_min(),
            prepare_checksum: self.consensus().last_prepare_checksum(),
            retry_capacity: self
                .dedup
                .capacity_committed()
                .then_some(self.dedup.capacity()),
            retry_protection: self.dedup.watermarks_sorted(),
            required_metadata_frontier: self.required_metadata_frontier,
            retirement_paid_through: self.retirement_paid_through,
            consumer_offsets: self.retained_consumer_offsets(ConsumerKind::Consumer),
            consumer_group_offsets: self.retained_consumer_offsets(ConsumerKind::ConsumerGroup),
            external_group_offsets: self.retained_consumer_offsets(ConsumerKind::ExternalGroup),
            emptied_at: self.durable_consumer_offsets.emptied_at(),
            consumer_group_owners: std::mem::take(&mut self.consumer_group_owners),
            history_transition: self.history_transition,
            owner_retirement: self.owner_retirement.take(),
            producers: std::mem::take(&mut self.producers),
            machines: self
                .state_machines
                .holds_state()
                .then(|| self.state_machines.capture(self.consensus().commit_min())),
            durable_offset: self.offset.load(Ordering::Acquire),
            write_offset: self.dirty_offset.load(Ordering::Relaxed),
            offset_space: self.offset_space,
            log: std::mem::take(&mut self.log),
        }
    }

    /// Restore an ideal-storage image of the previous partition incarnation.
    /// Retained logs and receipts model consensus recovery, not filesystem barriers.
    ///
    /// `durable_offset` and `write_offset` are what the caller recovered, as
    /// `segment_recovery` derives them from segments. Applied as a MAX against
    /// whatever the superblock frontier already proved, for the same reason
    /// [`Self::restore_offset_frontier`] maxes: a recovered value behind the
    /// frontier must not lower it.
    ///
    /// # Panics
    /// If the retained image contains inconsistent retry protection.
    #[cfg(any(test, feature = "simulator"))]
    pub fn adopt_retained_log(&mut self, state: crate::RetainedPartitionState) {
        self.invalidate_poll_history();
        let crate::RetainedPartitionState {
            log,
            head_op: _,
            applied_op: _,
            prepare_checksum: _,
            retry_capacity,
            retry_protection,
            required_metadata_frontier,
            retirement_paid_through,
            durable_offset,
            write_offset,
            offset_space,
            consumer_offsets,
            consumer_group_offsets,
            external_group_offsets,
            emptied_at,
            consumer_group_owners,
            history_transition,
            owner_retirement,
            producers,
            machines,
        } = state;
        // A restart that registers other machines than the process before it
        // is refused the way a boot refuses a checkpoint of them.
        if let Some(machines) = &machines
            && let Err(error) = self.restore_machines(Some(machines), machines.op())
        {
            error!(
                namespace_raw = self.namespace().inner(),
                op = machines.op(),
                %error,
                "cannot restore the retained state machines"
            );
            self.fatal = Some(FatalCommit {
                namespace_raw: self.namespace().inner(),
                op: machines.op(),
                operation: None,
            });
        }
        self.log = log;
        self.consumer_group_owners = consumer_group_owners;
        self.history_transition = history_transition;
        self.owner_retirement = owner_retirement;
        self.producers = producers;
        if let Some(capacity) = retry_capacity {
            self.dedup
                .install_watermarks(capacity, retry_protection)
                .expect("retained simulator protection is valid");
        } else {
            assert!(
                retry_protection.is_empty(),
                "uncommitted capacity cannot protect requests"
            );
        }
        self.required_metadata_frontier = required_metadata_frontier;
        self.retirement_paid_through = retirement_paid_through;
        for (consumer_id, offset) in consumer_offsets {
            self.consumer_offsets.pin().insert(
                consumer_id as usize,
                ConsumerOffset::new(ConsumerKind::Consumer, consumer_id, offset, String::new()),
            );
            self.durable_consumer_offsets.record_explicit(
                ConsumerKind::Consumer,
                consumer_id,
                offset,
                offset,
            );
        }
        for (group_id, offset) in consumer_group_offsets {
            self.consumer_group_offsets.pin().insert(
                ConsumerGroupId(group_id as usize),
                ConsumerOffset::new(ConsumerKind::ConsumerGroup, group_id, offset, String::new()),
            );
            self.durable_consumer_offsets.record_explicit(
                ConsumerKind::ConsumerGroup,
                group_id,
                offset,
                offset,
            );
        }
        for (group_id, offset) in external_group_offsets {
            self.durable_consumer_offsets.record_explicit(
                ConsumerKind::ExternalGroup,
                group_id,
                offset,
                offset,
            );
        }
        self.durable_consumer_offsets.set_emptied_at(emptied_at);
        // Zero counters can describe an empty partition or its first batch.
        // Preserve both bits so an uncommitted first batch does not become a
        // committed offset that view truncation is forbidden to discard.
        if !offset_space.append_live {
            return;
        }
        let durable = durable_offset.max(self.offset.load(Ordering::Acquire));
        let dirty = write_offset
            .max(durable)
            .max(self.dirty_offset.load(Ordering::Relaxed));
        self.offset.store(durable, Ordering::Release);
        self.publish_poll_visibility();
        self.dirty_offset.store(dirty, Ordering::Relaxed);
        self.offset_space = offset_space;
        self.recovered_durable_offset = offset_space.committed_seeded.then_some(durable);
    }

    /// Attach the WAL the simulator reopened from its filesystem model, as
    /// [`Self::open_persistence_with_recovered`] attaches one after segment
    /// recovery. The adopted log stands in for the replayed prepares, so it must
    /// hold exactly what the WAL holds above the applied prefix.
    #[cfg(feature = "simulator")]
    pub fn attach_simulated_persistence(&mut self, persistence: Rc<PartitionPersistence>) {
        // New empty WALs carry view zero; positive certificates belong to recovered history.
        let empty_wal = self.recovered_log_view.is_none()
            && persistence.head() == 0
            && matches!(persistence.certified_log_view(), None | Some(0));
        if empty_wal {
            persistence.certify_log_view(self.consensus.log_view(), 0, 0);
        }
        self.restore_recovered_log_view(&persistence, !empty_wal);
        if !self.materialization_missing && persistence.recovered_frontier() {
            self.consensus.set_recovery_election_allowed(true);
        }
        self.persistence = Some(persistence);
        self.report_wal_backlog_to_reserve();
        self.start_persistence();
    }

    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn retained_consumer_offsets(&self, kind: ConsumerKind) -> Vec<(u32, u64)> {
        self.durable_consumer_offsets.committed_entries(kind)
    }

    /// The in-memory half of [`Self::reanchor_to_offset_frontier`], for a
    /// simulator partition rebuilt over a restored offset counter.
    ///
    /// The production re-anchor cannot be reused here: it creates and unlinks
    /// real segment files, and an in-memory partition carries no
    /// `partition_dir`, so it would write a chain to whatever path the config
    /// resolves. What it does to the chain's SHAPE is the part the simulator
    /// needs, and this makes exactly the same two decisions on the same two
    /// conditions.
    ///
    /// Without it a restored replica keeps a single segment named at 0 while the
    /// counter resumes a lease block above it, and the next mint lands INSIDE
    /// that segment. Production can never reach that shape -- boot plants at the
    /// append point -- so the harness both diverges from what it is modelling and
    /// cannot expose the chain refusal a real node would hit on the boot after.
    #[cfg(any(test, feature = "simulator"))]
    pub fn reanchor_in_memory_to_mint_frontier(&mut self, segment_size: IggyByteSize) {
        let frontier = self.mint_frontier();
        if frontier == 0 {
            return;
        }
        // Empty tails named below the append point claim a range they do not
        // hold. No files to unlink, so the retire is the whole job.
        while let Some(segment) = self.log.segments().last() {
            if segment.size.as_bytes_u64() > 0 || segment.start_offset >= frontier {
                break;
            }
            if self.log.retire_back().is_none() {
                break;
            }
            self.stats.decrement_segments_count(1);
        }
        let tail = self
            .log
            .segments()
            .last()
            .map(|segment| (segment.end_offset, segment.size.as_bytes_u64()));
        let plant = match tail {
            // An emptied chain: plant at the append point, the same as boot's
            // `None` arm. Nothing precedes it, so there is no gap to record.
            None => true,
            // A SIZED tail below the append point gets sealed and planted past.
            // An empty one either just went or is already named at the frontier
            // and can take the appends as it stands.
            Some((sealed_end, size)) if size > 0 && sealed_end.saturating_add(1) < frontier => {
                self.log.active_segment_mut().sealed = true;
                true
            }
            Some(_) => false,
        };
        if plant {
            self.log.add_persisted_segment(
                crate::Segment::new(frontier, segment_size),
                server_common::SegmentStorage::default(),
                None,
                None,
            );
            self.stats.increment_segments_count(1);
        }
    }

    /// Copy this incarnation's offset counter into the shared
    /// [`PartitionStats`], making it the value readers (offset validation,
    /// `get_topic`, `get_stats`) see.
    ///
    /// Called from [`IggyPartitions::insert`](crate::IggyPartitions::insert)
    /// only: when the instance BECOMES the addressable one, never while
    /// building it. The stats registry keys on the namespace, not the
    /// incarnation, so every build of a namespace holds the same `Arc` as
    /// whatever is already serving it -- and a build is not guaranteed to be
    /// adopted. Seeding from the build instead leaves a zeroed `current_offset`
    /// on the live incarnation, which then rejects every
    /// `store_consumer_offset` above 0 with `InvalidOffset` until the next send
    /// re-seeds it.
    pub(crate) fn publish_current_offset(&self) {
        self.stats
            .set_current_offset(self.offset.load(Ordering::Acquire));
    }

    /// One past the highest COMMITTED offset, `0` while the offset space is
    /// still empty. The value stamped into the durable record, and what a
    /// transfer offer advertises.
    ///
    /// Not [`Self::mint_frontier`]: after a reservation-seeded boot the append
    /// point stands a lease block above this, and neither the record nor an
    /// offer may claim offsets no message reached.
    #[must_use]
    pub fn offset_frontier(&self) -> u64 {
        if self.offset_space.committed_seeded {
            self.offset.load(Ordering::Acquire).saturating_add(1)
        } else {
            0
        }
    }

    /// [`Self::offset_frontier`] as it stood once every op below `op` was
    /// applied, read off the stamped entries this replica still holds.
    ///
    /// `None` when an applied op from `op` up to the first one that minted
    /// offsets is no longer retained, so the frontier there cannot be known.
    #[must_use]
    pub fn offset_frontier_before(&self, op: u64) -> Option<u64> {
        let journal = &self.log.journal().inner;
        for applied in op..=self.consensus.commit_min() {
            let entry = journal.repair_entry(applied)?;
            if let Some(batch) = materialization_batch_meta(&entry, None).ok()? {
                return Some(batch.base_offset);
            }
        }
        Some(self.offset_frontier())
    }

    /// The offset the next mint will take, which is where the segment chain has
    /// to be anchored for the appends that follow to land contiguously.
    ///
    /// Above [`Self::offset_frontier`] by exactly the offsets this replica has
    /// journaled but not committed, plus -- on the first boot after a crash that
    /// took acked-but-unflushed messages with it -- the lease block the durable
    /// reservation claimed. That gap is the whole point: the reservation is the
    /// only surviving witness that those offsets were handed to a client, so the
    /// counter resumes above them instead of re-minting them.
    #[must_use]
    pub fn mint_frontier(&self) -> u64 {
        if self.offset_space.append_live {
            self.dirty_offset.load(Ordering::Relaxed).saturating_add(1)
        } else {
            0
        }
    }

    /// One past the highest offset this replica holds and may not lose: named by
    /// a sized segment, or committed and still resident in the journal. `0` when
    /// it holds nothing.
    ///
    /// The committed arm is not redundant with the disk arm, since the
    /// threshold-gated flush routinely leaves committed messages unnamed by any
    /// segment. It reads the COMMITTED counter and not `journal.info`, whose
    /// `current_offset` is the DIRTY tail: a view change truncates that tail
    /// (`truncate_uncommitted_from`) while the durable frontier only advances, so
    /// recording it would leave every later boot seeding the counter above the
    /// group, where each replicated prepare fails the
    /// `base_offset == dirty_offset + 1` check until a state transfer -- again on
    /// the boot after that one.
    #[must_use]
    pub fn held_offset_frontier(&self) -> u64 {
        // Reverse search, not a scan-and-max: the chain is ordered and the
        // contiguity guard keeps end offsets ascending, so the LAST sized segment
        // is the highest one. Only the trailing empties are walked.
        let on_disk = self
            .log
            .segments()
            .iter()
            .rev()
            .find(|segment| segment.size.as_bytes_u64() > 0)
            .map_or(0, |segment| segment.end_offset.saturating_add(1));
        on_disk.max(self.offset_frontier())
    }

    /// Whether a recent write failure's backoff window is still open.
    ///
    /// The same gate [`Self::persist_superblock_if_needed`] applies before its
    /// own write, extended to the spelled-value writers because their callers
    /// retry: without this each retry re-runs a full `atomic_replace` against
    /// a disk that just refused one, as fast as `ENOSPC` returns.
    fn superblock_write_is_backed_off(&self) -> bool {
        self.consensus.clock_realtime_micros() < self.superblock_retry_after_micros.get()
    }

    /// Collapse only after a successful flush. A reservation consumed on boot
    /// remains the mint frontier even when no new messages have been appended.
    fn collapse_superblock_intent(&self) -> Option<SuperblockIntent> {
        let reserved = self.mint_frontier();
        (self.superblock.is_some() && self.durable_offset_reserved.get() > reserved).then(|| {
            SuperblockIntent::Collapse {
                frontier: self.advanced_frontier(0),
                reserved,
            }
        })
    }

    /// Whether the offset reservation is close enough to being consumed that it
    /// should be extended NOW, off the append path.
    ///
    /// The append fence is correct but badly placed: it writes the superblock
    /// inline in the shard's frame pump, where the consensus tick is a sibling
    /// arm, so its two fsyncs delay heartbeats for every group on the core. The
    /// fix is to make the fence's fast path
    /// (`durable_offset_reserved > end_offset`) the only path it ever takes under
    /// load, by extending from the tick instead.
    ///
    /// HALF a block of headroom, which is a wide margin on purpose: over-claiming
    /// costs nothing but offset space, while arriving late puts the write back on
    /// the append path. Floored at 1, since validation admits a lease of 1 and
    /// `1 / 2` would never trigger, leaving every append to pay the inline claim.
    /// A partition that has never minted is skipped: extending every idle
    /// partition at boot would write a superblock per partition for nothing, and
    /// the first block is already claimed where the partition is created, off
    /// the append path.
    #[must_use]
    pub fn needs_offset_reservation_extension(&self) -> bool {
        // The shutdown collapse must stay the last superblock write.
        if self.consensus.replica_count() > 1
            || self.superblock.is_none()
            || self.maintenance.shutdown.is_some()
        {
            return false;
        }
        if !self.offset_space.append_live {
            return false;
        }
        let headroom = self
            .durable_offset_reserved
            .get()
            .saturating_sub(self.mint_frontier());
        headroom < (self.offset_reservation_lease / 2).max(1)
    }

    /// Extend the reservation a full block past the CEILING already on disk.
    ///
    /// Pairs with [`Self::needs_offset_reservation_extension`]; the caller is the
    /// shard tick, so this write is off the append path. A failure needs no
    /// handling beyond the writer's own logging and backoff: admission reserves
    /// each request's exact mint range before it can be sequenced.
    ///
    /// From the ceiling, NOT [`Self::mint_frontier`]. The trigger fires while the
    /// append point still sits under the ceiling -- that being the point of
    /// extending early -- so claiming a lease past the append point buys back only
    /// the headroom the trigger had left, about half a lease, and doubles the
    /// write rate the default lease is sized for. Maxed against the append point
    /// so a ceiling that somehow fell behind still comes forward.
    #[allow(clippy::future_not_send)]
    pub async fn extend_offset_reservation(&self) -> bool {
        let ceiling = self.durable_offset_reserved.get().max(self.mint_frontier());
        self.complete_superblock_intent(SuperblockIntent::Advance {
            frontier: 0,
            reserved: ceiling.saturating_add(self.offset_reservation_lease),
        })
        .await
    }

    /// Last offset a `SendMessages` request will mint when sequenced now.
    /// Admission checks this exact range before sequencing or queue promotion;
    /// a queued request must recompute it when preceding requests have advanced
    /// the mint frontier.
    ///
    /// `None` above one replica, where nothing is reserved, and when the body is
    /// not one canonical batch, which `convert_request_message` has already
    /// rejected by the time this runs.
    ///
    /// Header decode, NOT `decode_batch_slice`: the verifying decode fails on
    /// every ordinary send, because `convert_request_message` runs at
    /// [`ChecksumMode::Skip`] and leaves `batch_checksum` zeroed, which would
    /// silently skip reservation during admission. It also re-hashes every body
    /// `admit_wire_request` already hashed.
    fn request_mint_ceiling(&self, message: &Message<RoutedRequestHeader>) -> Option<u64> {
        if message.header().operation != Operation::SendMessages
            || self.consensus.replica_count() > 1
        {
            return None;
        }
        let body = message
            .as_slice()
            .get(std::mem::size_of::<RoutedRequestHeader>()..message.header().size as usize)?;
        let count = BatchHeader::decode(body).ok()?.message_count;
        // The batch's LAST offset, not one past it: the claim adds the exclusive
        // successor and the lease itself, so a ceiling one too high wastes an
        // offset on every claim and overstates what a crash can lose.
        //
        // Saturating rather than `None` on overflow. `None` means "no fence
        // applies here" and would send an exhausted offset space on to the mint,
        // where the refusal fences the partition and takes the node down; a
        // saturated ceiling reaches the fence instead and comes back as a
        // retryable transient.
        let last = u64::from(count).saturating_sub(1);
        Some(self.mint_frontier().saturating_add(last))
    }

    /// Reserve a single-replica offset range before CREATE publishes the partition.
    /// Admission uses `reserve_offsets_through_retryable` for the exact range a
    /// request will mint. Minting only verifies that range; it never extends it.
    /// Repair reuses offsets already fenced by their original replica.
    ///
    /// This path bypasses retry backoff because a failed CREATE can be retried
    /// before the namespace is published. Mounted partitions defer the write to
    /// their I/O lane. Replicated groups do not use the reservation ceiling.
    ///
    /// The frontier is exclusive: `u64::MAX` cannot be covered. Saturating its
    /// successor would permit an offset that recovery could mint again.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the fence verdict; dropping it lets the append escape unreserved"]
    pub async fn reserve_offsets_through(&self, end_offset: u64) -> bool {
        if self.offset_range_is_reserved(end_offset) {
            return true;
        }
        let Some(exclusive) = end_offset.checked_add(1) else {
            return false;
        };
        let intent = SuperblockIntent::Advance {
            frontier: 0,
            reserved: exclusive.saturating_add(self.offset_reservation_lease),
        };
        if self.io_notifier.borrow().is_none() {
            let Some(superblock) = self.superblock.as_ref() else {
                return true;
            };
            return self.write_superblock_locked(superblock, intent).await;
        }
        self.complete_superblock_intent(intent).await
    }

    /// [`Self::reserve_offsets_through`] for the ADMITTED path, where a refusal
    /// costs the client one retry rather than the process its life.
    ///
    /// Identical except that it honours the superblock retry backoff, AFTER the
    /// coverage fast path: a batch the record already permits owes the disk
    /// nothing and must not be refused by a window some other writer opened.
    ///
    /// Minting performs no I/O and requires this exact range to be covered.
    /// Backoff keeps client retries from repeating atomic replacements on a
    /// failing disk while already-covered requests can continue.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the fence verdict; dropping it lets the append escape unreserved"]
    async fn reserve_offsets_through_retryable(
        &self,
        end_offset: u64,
    ) -> crate::PartitionIoVerdict {
        if self.offset_range_is_reserved(end_offset) {
            return crate::PartitionIoVerdict::Ready;
        }
        let Some(exclusive) = end_offset.checked_add(1) else {
            return crate::PartitionIoVerdict::Failed;
        };
        let intent = SuperblockIntent::Advance {
            frontier: 0,
            reserved: exclusive.saturating_add(self.offset_reservation_lease),
        };
        if self.io_notifier.borrow().is_some() {
            return self.request_superblock_intent(intent);
        }
        if self.complete_superblock_intent(intent).await {
            crate::PartitionIoVerdict::Ready
        } else {
            crate::PartitionIoVerdict::Failed
        }
    }

    /// Check reservation coverage before direct sequencing or queue promotion.
    /// Pending retains the original request and reply in the bounded request queue.
    #[allow(clippy::future_not_send)]
    async fn admit_reserved_send(
        &self,
        message: &Message<RoutedRequestHeader>,
    ) -> crate::PartitionIoVerdict {
        match self.request_mint_ceiling(message) {
            Some(ceiling) => self.reserve_offsets_through_retryable(ceiling).await,
            None => crate::PartitionIoVerdict::Ready,
        }
    }

    /// Answer a send the reservation would not cover with a retryable transient,
    /// and say why.
    ///
    /// `TransientNotAccepted`, per its contract: nothing was admitted, so the
    /// client may re-issue anywhere without double-apply risk. It does make the
    /// SDK recheck the leader and walk the roster, which finds no better node
    /// when the fault is this one's disk -- wasteful, but the weaker code would
    /// claim an unknown outcome for a request that provably has none.
    #[allow(clippy::future_not_send)]
    async fn deny_unreserved_send(
        &self,
        header: &RoutedRequestHeader,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let consensus = self.consensus();
        emit_partition_diag(
            tracing::Level::WARN,
            &PartitionDiagEvent::new(
                ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                "refusing a send: the offset reservation could not be extended",
            )
            .with_operation(Operation::SendMessages),
        );
        Self::send_partition_deny_or_log(
            consensus,
            header,
            IggyError::TransientNotAccepted.as_code(),
            "unreserved send transient reply send failed",
            waiter,
        )
        .await;
    }

    /// Take and clear the gap-drop count (`prepare_gap_drops`).
    #[must_use = "dropping the count loses the only record those prepares existed"]
    pub const fn take_prepare_gap_drops(&self) -> u64 {
        self.prepare_gap_drops.replace(0)
    }

    /// Read the gap-drop count without clearing it, so a test can prove the
    /// count was still buffered on the partition at the moment it was removed.
    #[cfg(any(test, feature = "fault-injection"))]
    #[must_use]
    pub const fn prepare_gap_drops(&self) -> u64 {
        self.prepare_gap_drops.get()
    }

    /// Fail the next local commit of a committed `SendMessages` op the way a
    /// full disk fails it, fencing the partition.
    ///
    /// The commit path's own fence is covered by a `/dev/full` writer in this
    /// crate's tests, but the shard sweep that must OBSERVE the fence runs over
    /// in-memory partitions in the simulator, where no device can be made to
    /// fail.
    #[cfg(any(test, feature = "fault-injection"))]
    pub const fn inject_commit_failure(&mut self) {
        self.injected_commit_failure = true;
    }

    /// Burn one repair stall round; `true` once the session spent the budget.
    /// The count then restarts, so the session that the shard opens in its
    /// place gets the whole budget.
    ///
    /// On the PARTITION, not the session, for the same reason the transfer
    /// budget is: the rotation mints a new session, so only a count outside
    /// it can add up the rotations ([`Self::note_repair_rotation`]) until the
    /// shard gives up on the ring.
    ///
    /// A walk since the last round restarts both counts like a new frame does:
    /// while the WAL drains, the peer re-serves frames this journal holds. A
    /// new view restarts them too, as on the metadata plane: the rounds of an
    /// earlier view say nothing about the ring of this one.
    #[must_use = "the bool is the rotate verdict; dropping it disables the stall budget"]
    pub const fn burn_repair_attempt(&mut self) -> bool {
        let commit_min = self.consensus.commit_min();
        if commit_min > self.repair_walked_to {
            self.repair_walked_to = commit_min;
            self.restart_repair_budget();
        }
        let view = self.consensus.view();
        if view != self.repair_charged_view {
            self.repair_charged_view = view;
            self.restart_repair_budget();
        }
        self.repair_attempts += 1;
        let spent = self.repair_attempts > crate::types::REPAIR_MAX_STALL_RETRIES;
        if spent {
            self.repair_attempts = 0;
        }
        spent
    }

    /// Count a rotation that opened a repair session against another peer.
    /// The shard gives up on the ring once these rotations, with the session
    /// that just spent its budget, reach the other replicas.
    pub const fn note_repair_rotation(&mut self) {
        self.repair_rotations = self.repair_rotations.saturating_add(1);
    }

    /// Rotations since the last progress, for the shard's ring check and its
    /// log.
    #[must_use]
    pub const fn repair_rotations(&self) -> u32 {
        self.repair_rotations
    }

    /// Restart both stall counts when progress or a transfer supersedes repair.
    pub const fn restart_repair_budget(&mut self) {
        self.repair_attempts = 0;
        self.repair_rotations = 0;
    }

    /// Burn one transfer stall round; `true` once the budget is exhausted.
    /// Lives on the partition, not the session, so a re-minted session
    /// cannot reset it (see [`Self::transfer_attempts`]).
    #[must_use = "the bool is the abandon verdict; dropping it disables the stall budget"]
    pub const fn burn_transfer_attempt(&mut self) -> bool {
        self.transfer_attempts += 1;
        self.transfer_attempts > consensus::STATE_TRANSFER_MAX_STALL_RETRIES
    }

    /// Real transfer progress: reset the stall budget. The budget bounds
    /// CONSECUTIVE stalls, not lifetime ones; without this a handful of
    /// stalls scattered across a large transfer would abandon one that was
    /// nearly done, throwing away every byte already pulled.
    pub const fn note_transfer_progress(&mut self) {
        self.transfer_attempts = 0;
    }

    /// Charge one transfer failure (any class) and return the consecutive
    /// count; the shard scales its re-arm backoff by it. Never resets on a
    /// new generation or on received chunks -- a deterministic failure
    /// re-pulls successfully every round and still must back off -- only
    /// [`Self::note_transfer_installed`] clears it.
    pub const fn record_transfer_failure(&mut self) -> u32 {
        self.transfer_failures = self.transfer_failures.saturating_add(1);
        self.transfer_failures
    }

    /// A completed install: the one signal that genuinely proves the
    /// transfer pipeline works end to end, so it alone resets the
    /// consecutive-failure count.
    pub const fn note_transfer_installed(&mut self) {
        self.transfer_failures = 0;
        self.transfer_refusals = 0;
    }

    /// Charge one TRANSIENT refusal and return the consecutive count.
    ///
    /// Separate from [`Self::record_transfer_failure`] on purpose: a transient
    /// refusal must not touch the exponential backoff (the flat retry interval
    /// is the point), but a partition refused for hours still has to be
    /// visible, so the count exists only to escalate logging and feed a metric.
    /// Reset by [`Self::note_transfer_installed`] alongside the failure count.
    pub const fn record_transfer_refusal(&mut self) -> u32 {
        self.transfer_refusals = self.transfer_refusals.saturating_add(1);
        self.transfer_refusals
    }

    /// A fresh re-arm is scheduled: the stall budget starts over for it.
    ///
    /// Carrying an exhausted budget into the next attempt left every later
    /// session with a single retry-interval window to land its first response --
    /// against a re-arm backoff climbing to 1024x, and a serving side that
    /// hashes retained bytes before it can answer, so a slow first response is
    /// ordinary rather than a stall. The budget bounds consecutive stalls within
    /// one attempt; `transfer_failures` and its backoff are what bound livelock
    /// across attempts.
    pub const fn note_transfer_rearm_scheduled(&mut self) {
        self.transfer_attempts = 0;
    }

    /// Read-only view of the stall budget, for diagnostics. The counters
    /// themselves are private: they are the anti-livelock argument, and the
    /// docs promise exactly one resetter each -- a `pub` field would let any
    /// future call site break that silently.
    #[must_use]
    pub const fn transfer_attempts(&self) -> u32 {
        self.transfer_attempts
    }

    /// Install this topic's runtime knobs, as resolved at topic admission.
    /// Unset fields keep the shard-wide configured values.
    pub fn set_runtime_options(&mut self, runtime_options: TopicRuntimeOptions) {
        self.runtime_options = runtime_options;
        self.refresh_recovered_log_complete();
    }

    /// A replica whose materialization is missing lost the prepares it
    /// acknowledged, whatever the durability, so it answers view changes with
    /// the empty vote rather than staying silent until a transfer.
    pub(crate) fn refresh_recovered_log_complete(&self) {
        self.consensus.set_recovered_log_complete(
            self.acknowledges_only_written_prepares() && !self.materialization_missing,
        );
    }

    /// Whether every prepare this replica acknowledges is on stable storage
    /// first, so a restart recovers all of them. `replicated` acknowledges once
    /// the write is queued, and either policy covers some of the log.
    const fn acknowledges_only_written_prepares(&self) -> bool {
        self.durability().is_persisted() && self.consumer_offset_durability().is_persisted()
    }

    #[must_use]
    pub const fn runtime_options(&self) -> TopicRuntimeOptions {
        self.runtime_options
    }

    pub fn set_encryptor(&mut self, encryptor: Option<Arc<EncryptorKind>>) {
        self.encryptor = encryptor;
    }

    #[must_use]
    pub const fn encryptor(&self) -> Option<&Arc<EncryptorKind>> {
        self.encryptor.as_ref()
    }

    /// Segment size used consistently by live writes, recovery, and transfer.
    #[must_use]
    pub fn effective_segment_size(&self) -> IggyByteSize {
        self.runtime_options.effective_segment_size()
    }

    /// Microseconds past a segment's first batch at which a batch starts the
    /// next one, shared by materialization and the WAL. `None` rolls by size.
    #[must_use]
    pub fn effective_segment_max_age(&self) -> Option<u64> {
        self.runtime_options.effective_segment_max_age()
    }

    /// The stable-storage requirement for message acknowledgment.
    #[must_use]
    pub const fn durability(&self) -> iggy_common::Durability {
        self.runtime_options.durability
    }

    #[must_use]
    pub const fn consumer_offset_durability(&self) -> iggy_common::Durability {
        self.runtime_options.consumer_offset_durability
    }

    /// Message-count threshold that flushes this partition's journal.
    #[must_use]
    pub fn effective_messages_required_to_save(&self, config: &PartitionsConfig) -> u32 {
        self.runtime_options
            .messages_required_to_save
            .unwrap_or(config.messages_required_to_save)
    }

    /// Install the offset-reservation block size resolved from this node's
    /// `PartitionsConfig`.
    ///
    /// Carried on the partition because the fence runs inside `on_request` /
    /// `on_replicate`, which take no config. `NonZeroU32` because a zero block
    /// reserves nothing and would write the superblock before every append:
    /// coercing it here instead would contradict the configuration validator and
    /// hide a wiring error that handed this a zero.
    pub const fn set_offset_reservation_lease(&mut self, lease: NonZeroU32) {
        self.offset_reservation_lease = lease.get() as u64;
    }

    /// Whether this partition's segments reserve their bytes on open.
    #[must_use]
    pub fn effective_preallocate_segments(&self, config: &PartitionsConfig) -> bool {
        self.runtime_options
            .preallocate_segments
            .unwrap_or(config.preallocate_segments)
    }

    /// Byte threshold that flushes this partition's journal.
    #[must_use]
    pub fn effective_size_of_messages_required_to_save(&self, config: &PartitionsConfig) -> u64 {
        self.runtime_options
            .size_of_messages_required_to_save
            .unwrap_or(config.size_of_messages_required_to_save)
            .as_bytes_u64()
    }

    /// `dirs` holds one offset directory per kind, in [`ConsumerKind::index`] order.
    pub fn configure_consumer_offset_storage(
        &mut self,
        dirs: [String; ConsumerKind::COUNT],
        consumer_offsets: ConsumerOffsets,
        consumer_group_offsets: ConsumerGroupOffsets,
    ) {
        self.invalidate_poll_history();
        self.consumer_offsets = Arc::new(consumer_offsets);
        self.consumer_group_offsets = Arc::new(consumer_group_offsets);
        self.consumer_offset_dirs = dirs.map(Some);
    }

    /// Seed committed membership from one recovered offset file.
    pub fn seed_recovered_consumer_offset(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        committed_offset: u64,
        persisted_high_water: u64,
    ) {
        self.durable_consumer_offsets.record_explicit(
            kind,
            consumer_id,
            committed_offset,
            persisted_high_water,
        );
    }

    /// Drop a recovered offset whose file boot removed, as if it had never been loaded.
    pub(crate) fn forget_recovered_consumer_offset(&self, kind: ConsumerKind, consumer_id: u32) {
        match kind {
            ConsumerKind::Consumer => {
                self.consumer_offsets.pin().remove(&(consumer_id as usize));
            }
            ConsumerKind::ConsumerGroup => {
                self.consumer_group_offsets
                    .pin()
                    .remove(&ConsumerGroupId(consumer_id as usize));
            }
            // The durable table below is the only store of an external offset.
            ConsumerKind::ExternalGroup => {}
        }
        self.durable_consumer_offsets.remove(kind, consumer_id);
    }

    pub fn seed_stranded_consumer_offset(&self, kind: ConsumerKind, consumer_id: u32) -> bool {
        if !self.durable_consumer_offsets.contains(kind, consumer_id) {
            self.consumer_offset_capacity_for(kind)
                .record_stranded(consumer_id);
            return true;
        }
        false
    }

    /// Restore the op of the committed delete that emptied the recovered table.
    pub(crate) fn seed_offsets_emptied_at(&self, op: u64) {
        self.durable_consumer_offsets.set_emptied_at(op);
    }

    #[must_use]
    pub fn durable_consumer_offset_count(&self, kind: ConsumerKind) -> usize {
        self.durable_consumer_offsets.count(kind)
    }

    #[must_use]
    pub fn occupied_consumer_offset_count(&self, kind: ConsumerKind) -> usize {
        self.consumer_offset_capacity_for(kind)
            .occupied(&self.durable_consumer_offsets)
    }

    #[must_use]
    pub fn consumer_offset_map_count(&self, kind: ConsumerKind) -> usize {
        match kind {
            ConsumerKind::Consumer => self.consumer_offsets.len(),
            ConsumerKind::ConsumerGroup => self.consumer_group_offsets.len(),
            ConsumerKind::ExternalGroup => self.durable_consumer_offsets.count(kind),
        }
    }

    /// Stage an already journaled mutation without touching its offset file.
    /// The commit driver persists it before publication, so discarding an
    /// uncommitted prepare does not require rolling back a file write.
    pub(crate) fn stage_consumer_offset_upsert(
        &mut self,
        op: u64,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
        auto_commit: bool,
    ) {
        let pending = if auto_commit {
            PendingConsumerOffsetCommit::upsert_auto_commit(kind, consumer_id, offset)
        } else {
            PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset)
        };
        let replaced = self.pending_consumer_offset_commits.insert(op, pending);
        if let Some(replaced) = replaced {
            self.refresh_consumer_offset_reservation(replaced.kind, replaced.consumer_id);
        }
        self.refresh_consumer_offset_reservation(kind, consumer_id);
    }

    /// Stage a consumer offset delete for the replicated op. See
    /// [`Self::stage_consumer_offset_upsert`] for the ordering contract.
    ///
    /// Deliberately infallible: this runs on the replicated-apply path (every
    /// replica), where the offset may legitimately be absent (e.g. a backup
    /// that never observed the primary-only `NoAck` store). The client-facing
    /// "offset must exist" precondition is enforced once at primary admission
    /// (`ensure_consumer_offset_exists` in `on_request`); re-checking here would
    /// fail the replicated apply on such a replica and wedge the group.
    pub(crate) fn stage_consumer_offset_delete(
        &mut self,
        op: u64,
        kind: ConsumerKind,
        consumer_id: u32,
    ) {
        let pending = PendingConsumerOffsetCommit::delete(kind, consumer_id);
        if let Some(replaced) = self.pending_consumer_offset_commits.insert(op, pending) {
            self.refresh_consumer_offset_reservation(replaced.kind, replaced.consumer_id);
        }
    }

    #[cfg(test)]
    pub(crate) async fn apply_staged_consumer_offset_commit(
        &mut self,
        op: u64,
    ) -> Result<(), IggyError> {
        // Keep the staged cursor until materialization succeeds. The commit
        // walk fences a failed apply, which must not look like a completed op.
        let pending = match self.pending_consumer_offset_commits.get(&op) {
            Some(pending) => *pending,
            // A view change clears the staged table (uncommitted ops may be
            // superseded by the new view's log), and suffixes adopted via
            // DoViewChange/StartView or journal repair never pass the live
            // staging path at all. The journal entry IS the new view's
            // authoritative content for this op, so re-derive the commit
            // from it instead of wedging the commit walk.
            None => self.restage_consumer_offset_from_journal(op)?,
        };
        // Persist to the on-disk offset table first so a crash after the
        // in-memory apply cannot observe a readable offset that was not
        // durably stored; the in-memory update is idempotent on replay
        // because we look up by (kind, id).
        self.persist_consumer_offset_commit(pending).await?;
        if let Some(persistence) = &self.persistence {
            persistence.mark_offset_dirty(
                pending.kind.index(),
                pending.consumer_id,
                matches!(pending.mutation, PendingConsumerOffsetMutation::Upsert(_)),
            );
        }
        let operation = match pending.mutation {
            PendingConsumerOffsetMutation::Upsert(_) => Operation::StoreConsumerOffset,
            PendingConsumerOffsetMutation::Delete => Operation::DeleteConsumerOffset,
        };
        // A covered store also depends on an earlier dirty directory entry.
        self.consumer_offset_dirs_touched[pending.kind.index()].set(Some((op, operation)));
        self.apply_consumer_offset_commit(pending);
        self.pending_consumer_offset_commits.remove(&op);
        self.refresh_consumer_offset_reservation(pending.kind, pending.consumer_id);
        Ok(())
    }

    #[cfg(test)]
    async fn write_consumer_offset(
        &self,
        path: &str,
        offset: u64,
        persisted: bool,
    ) -> Result<(), IggyError> {
        let job = self.prepare_consumer_offset_write(path, offset, persisted, false);
        let result = job.execute().await;
        self.accept_consumer_offset_write(result).map(|_| ())
    }

    #[cfg(test)]
    async fn write_cold_consumer_offset(
        &self,
        path: &str,
        offset: u64,
        persisted: bool,
    ) -> Result<(u64, bool), IggyError> {
        let job = self.prepare_consumer_offset_write(path, offset, persisted, true);
        let result = job.execute().await;
        self.accept_consumer_offset_write(result)
            .map(|value| (value.offset, value.written))
    }

    fn prepare_consumer_offset_write(
        &self,
        path: &str,
        offset: u64,
        persisted: bool,
        fold_max: bool,
    ) -> crate::io::OffsetIoJob {
        let (owner, checked_out) = match (&self.persistence, persisted) {
            (Some(persistence), _) => (
                crate::io::OffsetFileOwner::Wal,
                persistence.checkout_offset_file(path),
            ),
            (None, false) => self.workerless_offset_files.checkout(path).map_or(
                (crate::io::OffsetFileOwner::Uncached, None),
                |checked_out| (crate::io::OffsetFileOwner::Workerless, Some(checked_out)),
            ),
            (None, true) => {
                self.workerless_offset_files.retire(path);
                self.workerless_offset_files.discard_retired();
                (crate::io::OffsetFileOwner::Uncached, None)
            }
        };
        let (file, permit) =
            checked_out.map_or((None, None), |(file, permit)| (file, Some(permit)));
        crate::io::OffsetIoJob {
            path: path.to_owned(),
            offset,
            fold_max,
            persisted,
            owner,
            file,
            permit,
            retired_marker: None,
        }
    }

    fn accept_consumer_offset_write(
        &self,
        result: crate::io::OffsetIoResult,
    ) -> Result<crate::offset_storage::PersistedOffset, IggyError> {
        let crate::io::OffsetIoResult {
            path,
            owner,
            file,
            permit,
            outcome,
            write_error,
        } = result;
        match owner {
            crate::io::OffsetFileOwner::Wal => {
                let persistence = self
                    .persistence
                    .as_ref()
                    .ok_or(IggyError::CannotWriteToFile)?;
                if let (Some(file), Some(permit)) = (file, permit) {
                    persistence.return_offset_file(&path, file, permit);
                } else {
                    persistence.retire_offset_file(&path);
                }
                if let Some(error) = write_error {
                    persistence.fail_operation(error, Operation::StoreConsumerOffset);
                }
            }
            crate::io::OffsetFileOwner::Workerless => {
                if write_error.is_some() || file.is_none() {
                    self.workerless_offset_files.retire(&path);
                }
                if let (Some(file), Some(permit)) = (file, permit) {
                    self.workerless_offset_files.put(&path, file, permit);
                }
                self.workerless_offset_files.discard_retired();
            }
            crate::io::OffsetFileOwner::Uncached => {}
        }
        outcome
    }

    pub(crate) fn retire_consumer_offset_file(&self, path: &str) {
        if let Some(persistence) = &self.persistence {
            persistence.retire_offset_file(path);
        } else {
            self.workerless_offset_files.retire(path);
            self.workerless_offset_files.discard_retired();
        }
    }

    fn prepare_consumer_offset_delete(&self, path: &str) -> crate::io::OffsetDeleteIoJob {
        self.retire_consumer_offset_file(path);
        crate::io::OffsetDeleteIoJob {
            path: path.to_owned(),
            emptied_marker: None,
        }
    }

    #[cfg(test)]
    async fn persist_consumer_offset_commit(
        &self,
        pending: PendingConsumerOffsetCommit,
    ) -> Result<(), IggyError> {
        let tracked = self
            .durable_consumer_offsets
            .get(pending.kind, pending.consumer_id);
        let effect = self
            .persist_consumer_offset_effect(pending, tracked)
            .await?;
        self.publish_consumer_offset_effect(effect);
        Ok(())
    }

    #[cfg(test)]
    async fn persist_consumer_offset_effect(
        &self,
        pending: PendingConsumerOffsetCommit,
        tracked: Option<DurableOffsetState>,
    ) -> Result<ConsumerOffsetEffect, IggyError> {
        let plan = self.plan_consumer_offset_effect(
            pending,
            tracked,
            self.durable_consumer_offsets.emptied_at(),
        );
        if let ConsumerOffsetAction::Ready(effect) = plan.action {
            return Ok(effect);
        }
        let job = self.capture_consumer_offset_effect(&plan)?;
        self.accept_consumer_offset_effect(plan, job.execute().await)
    }

    /// `table_emptied_at` is the emptied-at op of the table this effect lands in.
    fn plan_consumer_offset_effect(
        &self,
        pending: PendingConsumerOffsetCommit,
        tracked: Option<DurableOffsetState>,
        table_emptied_at: u64,
    ) -> ConsumerOffsetPlan {
        let has_path = self.consumer_offsets_dir(pending.kind).is_some();
        let action = match pending.mutation {
            PendingConsumerOffsetMutation::Upsert(offset) => {
                if !has_path {
                    ConsumerOffsetAction::Ready(consumer_offset_effect(pending, tracked, offset))
                } else if pending.auto_commit
                    && let Some(state) =
                        tracked.filter(|state| offset <= state.persisted_high_water)
                {
                    ConsumerOffsetAction::Ready(consumer_offset_effect(
                        pending,
                        tracked,
                        state.persisted_high_water,
                    ))
                } else {
                    ConsumerOffsetAction::Write {
                        offset: if pending.auto_commit {
                            tracked.map_or(offset, |state| state.committed_offset.max(offset))
                        } else {
                            offset
                        },
                        fold_max: pending.auto_commit && tracked.is_none(),
                    }
                }
            }
            PendingConsumerOffsetMutation::Delete => {
                if has_path {
                    ConsumerOffsetAction::Delete
                } else {
                    ConsumerOffsetAction::Ready(ConsumerOffsetEffect {
                        pending,
                        state: None,
                    })
                }
            }
        };
        ConsumerOffsetPlan {
            pending,
            tracked,
            persisted: self.consumer_offset_durability().is_persisted()
                && self.persistence.is_none(),
            action,
            emptied_at: (matches!(pending.mutation, PendingConsumerOffsetMutation::Upsert(_))
                && table_emptied_at != 0)
                .then_some(0),
        }
    }

    fn capture_consumer_offset_effect(
        &self,
        plan: &ConsumerOffsetPlan,
    ) -> Result<crate::io::PartitionIoJob<SB>, IggyError> {
        let path = self
            .persisted_offset_path(plan.pending.kind, plan.pending.consumer_id)
            .ok_or(IggyError::CannotWriteToFile)?;
        let marker = plan
            .emptied_at
            .and_then(|emptied_at| Some((self.emptied_marker_path()?, emptied_at)));
        match plan.action {
            ConsumerOffsetAction::Write { offset, fold_max } => {
                let mut job =
                    self.prepare_consumer_offset_write(&path, offset, plan.persisted, fold_max);
                job.retired_marker = marker.map(|(marker, _)| marker);
                Ok(crate::io::PartitionIoJob::OffsetWrite(job))
            }
            ConsumerOffsetAction::Delete => {
                let mut job = self.prepare_consumer_offset_delete(&path);
                job.emptied_marker = marker;
                Ok(crate::io::PartitionIoJob::OffsetDelete(job))
            }
            ConsumerOffsetAction::Ready(_) => Err(IggyError::CannotWriteToFile),
        }
    }

    fn accept_consumer_offset_effect(
        &self,
        plan: ConsumerOffsetPlan,
        result: crate::io::PartitionIoResult,
    ) -> Result<ConsumerOffsetEffect, IggyError> {
        let pending = plan.pending;
        match (plan.action, result) {
            (
                ConsumerOffsetAction::Write { .. },
                crate::io::PartitionIoResult::OffsetWrite(result),
            ) => {
                let value = self.accept_consumer_offset_write(result)?;
                if value.written && plan.persisted {
                    self.mark_consumer_offset_dir_dirty(pending.kind);
                }
                Ok(consumer_offset_effect(pending, plan.tracked, value.offset))
            }
            (ConsumerOffsetAction::Delete, crate::io::PartitionIoResult::OffsetDelete(outcome)) => {
                match outcome {
                    Ok(removed) => {
                        if removed && plan.persisted {
                            self.mark_consumer_offset_dir_dirty(pending.kind);
                        }
                        Ok(ConsumerOffsetEffect {
                            pending,
                            state: None,
                        })
                    }
                    // Only a failed unlink reports this error. The marker write
                    // after a successful one leaves no file behind to strand.
                    Err(error @ IggyError::CannotDeleteConsumerOffsetFile(_)) => {
                        self.consumer_offset_capacity_for(pending.kind)
                            .record_stranded(pending.consumer_id);
                        warn!(
                            namespace_raw = self.namespace().inner(),
                            kind = ?pending.kind, consumer_id = pending.consumer_id, %error,
                            "committed consumer offset delete could not remove its file"
                        );
                        Err(error)
                    }
                    Err(error) => Err(error),
                }
            }
            _ => Err(IggyError::CannotWriteToFile),
        }
    }

    fn publish_consumer_offset_effect(&self, effect: ConsumerOffsetEffect) {
        let ConsumerOffsetEffect { pending, state } = effect;
        let capacity = self.consumer_offset_capacity_for(pending.kind);
        if let Some(state) = state {
            let created = !self
                .durable_consumer_offsets
                .contains(pending.kind, pending.consumer_id);
            if pending.auto_commit {
                self.durable_consumer_offsets.record_auto_commit(
                    pending.kind,
                    pending.consumer_id,
                    state.committed_offset,
                    state.persisted_high_water,
                );
            } else {
                self.durable_consumer_offsets.record_explicit(
                    pending.kind,
                    pending.consumer_id,
                    state.committed_offset,
                    state.persisted_high_water,
                );
            }
            if keyed_by_group(pending.kind) && created {
                self.mark_consumer_group_offsets_dirty();
            }
        } else {
            self.durable_consumer_offsets
                .remove(pending.kind, pending.consumer_id);
            capacity.forget_inactive_provisional(pending.consumer_id);
            capacity.rearm_if_below_limit(&self.durable_consumer_offsets);
        }
        capacity.clear_stranded(pending.consumer_id);
    }

    /// Accept a read only while its message history still belongs to this owner.
    /// Validation, admission, and progress updates must stay in one synchronous
    /// owner turn so a history change or recovery cannot interleave between them.
    /// Nonempty group reads update `last_polled` even without automatic commits.
    /// Rejection leaves this read's progress unapplied.
    pub(crate) fn complete_poll(
        &mut self,
        result: PollReadResult,
    ) -> Result<PollCompletion, IggyError> {
        // Recovery can become necessary while disk I/O is pending, even if
        // the history identity has not changed.
        if result.context.history != self.poll_history
            || self.fatal.is_some()
            || self.materialization_missing
        {
            return Err(IggyError::TransientNotAccepted);
        }
        // The delete fence leaves the history identity unchanged, and this
        // node's metadata can still list the partition after the fence.
        if self.history_deleted() {
            return Err(IggyError::HistoryUnavailable);
        }
        // A view change rebuilds the install latch and barrier from the log,
        // so the owner checks below must run after it.
        self.resynchronize_consumer_offset_reservations();
        if let PollingConsumer::ConsumerGroup(group_id, _) = result.context.consumer {
            // A store by an unassigned owner fails at apply on every replica.
            // The read carries no requester, only the owner it captured.
            if let Some(owner) = result.context.owner.filter(|owner| owner.is_unassigned()) {
                return Err(IggyError::ConsumerGroupPartitionNotOwned(
                    owner.client_id,
                    u32::try_from(self.namespace().partition_id()).unwrap_or(u32::MAX),
                ));
            }
            if result.context.owner.is_none()
                || result.context.owner != self.consumer_group_owner(group_id as u64)
                || self
                    .pending_owner_install
                    .get()
                    .is_some_and(|pending| pending.group_id == group_id as u64)
                || self.consensus.commit_min() < self.owner_install_barrier
            {
                return Err(IggyError::TransientNotAccepted);
            }
        }
        // Only now: the store failing a read of a history or an owner that
        // was replaced meanwhile says nothing about the ones served now.
        if let Some(error) = result.tiered_read_failure() {
            return Err(error);
        }
        let mut replication = None;
        if let Some(offset) = result
            .last_matching_offset
            .filter(|_| !result.fragments.is_empty())
        {
            if result.context.auto_commit {
                let pending = PendingConsumerOffsetCommit::try_from_polling_consumer(
                    result.context.consumer,
                    offset,
                )?;
                let kind = pending.kind;
                let consumer_id = pending.consumer_id;
                // Admit before changing either progress map, or a rejected
                // read could make the next poll skip its messages.
                replication = self.admit_poll_auto_commit(kind, consumer_id, offset)?;
                self.apply_local_poll_offset(kind, consumer_id, offset);
            }
            if let PollingConsumer::ConsumerGroup(group_id, _) = result.context.consumer {
                if let Some(polls) = self.consumer_group_polls.get(&(group_id as u64)) {
                    polls.store(false, Ordering::Relaxed);
                }
                upsert_offset_max(
                    &self.last_polled_offsets,
                    ConsumerGroupId(group_id),
                    offset,
                    || {
                        ConsumerOffset::new(
                            ConsumerKind::ConsumerGroup,
                            u32::try_from(group_id).unwrap_or(u32::MAX),
                            offset,
                            String::new(),
                        )
                    },
                );
            }
        }
        Ok(PollCompletion {
            context: iggy_binary_protocol::primitives::partition_history::PartitionContext {
                incarnation: self.created_revision(),
                owner_generation: result.context.owner.map_or(0, |owner| owner.generation),
                metadata_op: self.required_metadata_frontier,
            },
            // An empty reply has nothing to decrypt, so it skips the cross-core
            // refcount bump of the owner's encryptor.
            encryptor: if result.fragments.is_empty() {
                None
            } else {
                self.encryptor.clone()
            },
            fragments: result.fragments,
            current_offset: result.commit_offset,
            replication,
        })
    }

    /// Stage an assigned prepare and release its provisional capacity guard.
    /// The owner releases the poll reply first because replica sends may wait.
    pub(crate) async fn replicate_poll_completion(&mut self, replication: PollReplication) {
        let PollReplication {
            prepare,
            reservation,
        } = replication;
        self.on_replicate(prepare).await;
        drop(reservation);
    }

    /// Invalidate after preflight, before replacing data or progress.
    /// Preserve the identity when preflight leaves served state unchanged.
    /// Queued automatic commits also belong to the old history, even when
    /// their reads have already completed. So does a pinned transfer plan.
    pub(crate) fn invalidate_poll_history(&mut self) {
        self.workerless_offset_files.clear();
        self.poll_history = PollHistoryId::default();
        self.index_fetches.replace_history(self.poll_history);
        self.publish_poll_visibility();
        self.discard_queued_auto_commits();
        self.transfer_plan.borrow_mut().take();
    }

    /// The served history and what commits made visible in it. A change of
    /// either can make a parked poll ready.
    pub fn poll_visibility(&self) -> (PollHistoryId, PollVisibility) {
        (
            self.poll_history,
            PollVisibility {
                offset: self
                    .offset_space
                    .committed_seeded
                    .then(|| self.offset.load(Ordering::Acquire)),
                committed_bytes: self.committed_bytes,
            },
        )
    }

    /// The notifier runs inside partition mutations whenever [`Self::poll_visibility`]
    /// or an installed group owner can change. It must only record the change,
    /// and it must not re-enter the partition.
    pub fn set_poll_notifier(&mut self, notifier: Option<Rc<dyn Fn()>>) {
        self.poll_notifier = notifier;
    }

    /// While the reserve is short, this partition refuses new messages as a
    /// primary. A backup keeps journalling, because a replica that refuses
    /// prepares stalls the commit of every group that needs its ack.
    pub fn set_free_space_reserve(&mut self, reserve: Option<Arc<crate::FreeSpaceReserve>>) {
        self.release_send_space();
        self.free_space_reserve = reserve;
        self.report_wal_backlog_to_reserve();
    }

    /// The WAL counts what it accepted and has not written as pending bytes
    /// of the reserve. Call after either of the two is installed.
    fn report_wal_backlog_to_reserve(&self) {
        if let Some(persistence) = &self.persistence {
            persistence.report_pending_bytes_to(self.free_space_reserve.as_ref());
        }
    }

    /// Whether the reserve leaves room for this send. Its bytes stay pending
    /// until [`Self::release_send_space`], so no other partition of the node
    /// is given the same room while this one awaits.
    ///
    /// A batch the partition already holds passes a short disk: its sender
    /// is owed the outcome, and answering it takes no new space.
    fn reserve_send_space(&self, message: &Message<RoutedRequestHeader>) -> bool {
        let Some(reserve) = &self.free_space_reserve else {
            return true;
        };
        self.release_send_space();
        let frame_bytes = Self::prepared_request_size(message) as u64;
        if reserve.admit(frame_bytes) {
            self.admitted_send_bytes.set(frame_bytes);
            return true;
        }
        if matches!(
            self.check_producer_batch(message),
            Ok(ProducerAdmission::Duplicate { .. } | ProducerAdmission::DuplicateInFlight)
        ) {
            return true;
        }
        reserve.count_refusal();
        false
    }

    /// Call once the admitted send is a prepare in the WAL queue, which
    /// counts it from then on, or was refused or queued on the way there.
    fn release_send_space(&self) {
        let admitted = self.admitted_send_bytes.take();
        if admitted > 0
            && let Some(reserve) = &self.free_space_reserve
        {
            reserve.release_pending(admitted);
        }
    }

    pub(crate) fn publish_poll_visibility(&self) {
        if let Some(notify) = &self.poll_notifier {
            notify();
        }
    }

    fn discard_queued_auto_commits(&self) {
        self.consensus.with_pipeline_mut(|pipeline| {
            pipeline.retain_requests(|request| request.auto_commit().is_none());
        });
    }

    /// Admit an automatic commit without advancing this read's progress.
    /// `Some` carries an assigned prepare. `None` means the request is queued
    /// or the durable offset already covers it, both of which leave the
    /// caller free to apply local progress. Errors release any provisional
    /// guard.
    fn admit_poll_auto_commit(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
    ) -> Result<Option<PollReplication>, IggyError> {
        if !self.auto_commit_admission_ready(kind, consumer_id) {
            return Err(IggyError::TransientNotAccepted);
        }
        self.check_local_poll_key(kind, consumer_id)
            .map_err(|error| self.consumer_offset_capacity_error(error))?;
        // Already durable: the commit this poll would make has happened and
        // replicated, so applying it locally syncs this replica to something
        // the group agreed. Safe wherever the read was served, and checked
        // before the role so a caught-up backup is not refused for a
        // commit nobody needs.
        if !self.consumer_offset_is_blocked(kind, consumer_id, None, true)
            && self
                .durable_consumer_offsets
                .covers(kind, consumer_id, offset)
        {
            return Ok(None);
        }
        let consensus = self.consensus();
        // A replica that cannot originate the prepare cannot record this
        // progress anywhere a peer will ever see. `Ok(None)` would leave
        // `complete_poll` applying the offset to local state alone, so the
        // poll would report progress the group never agreed, and a later
        // read on the primary would hand the same messages out again.
        // Refusing keeps the outcome retriable on a replica that can commit.
        if consensus.is_normal_backup() {
            return Err(IggyError::NotPrimary);
        }
        if !consensus.is_primary()
            || !consensus.is_normal()
            || consensus.is_transferring()
            || self.refuses_prepares()
        {
            return Err(IggyError::TransientNotAccepted);
        }
        // A payment starts only from an empty pipeline. A consumer committing
        // on every read would otherwise keep a prepare queued at each pay read
        // and hold the partition's sessions off until it drained its backlog.
        if self.owes_session_retirement() {
            return Err(IggyError::TransientNotAccepted);
        }
        if self.pending_owner_install.get().is_some_and(|pending| {
            kind == ConsumerKind::ConsumerGroup && pending.group_id == u64::from(consumer_id)
        }) || self.pending_history_transition.get().is_some()
        {
            return Err(IggyError::TransientNotAccepted);
        }

        let reservation = self
            .consumer_offset_capacity_for(kind)
            .reserve_provisional(consumer_id, &self.durable_consumer_offsets)
            .map_err(|error| self.consumer_offset_capacity_error(error))?;
        let request = self.build_poll_auto_commit_request(kind, consumer_id, offset)?;
        // Automatic commits bypass the request handler, so check journal
        // capacity here before queueing or assigning an operation.
        if self.persistence.as_ref().is_some_and(|persistence| {
            !persistence.has_capacity(Self::prepared_request_size(&request))
        }) {
            return Err(IggyError::TransientNotAccepted);
        }
        let order = self.next_local_request_order()?;
        if self.local_request_must_queue()
            || self.consumer_offset_is_blocked(kind, consumer_id, Some(offset), true)
        {
            let context = AutoCommitRequestContext { reservation };
            self.queue_local_request(
                consensus::RequestEntry::with_auto_commit(request, context),
                order,
            )
            .map_err(|_| IggyError::TransientNotAccepted)?;
            Ok(None)
        } else {
            self.reserve_consumer_offset(kind, consumer_id)
                .map_err(|error| self.consumer_offset_capacity_error(error))?;
            let prepare = self.pipeline_local_request(request, None, Some(order));
            Ok(Some(PollReplication {
                prepare,
                reservation,
            }))
        }
    }

    fn next_local_request_order(&self) -> Result<LocalRequestOrder, IggyError> {
        self.local_request_order
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(LocalRequestOrder)
            .ok_or(IggyError::TransientNotAccepted)
    }

    fn consumer_offset_is_blocked(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
        include_queued: bool,
    ) -> bool {
        // Admission reserves a store's key before its prepare is journaled.
        // This also covers a poll completion awaiting on_replicate.
        if self
            .consumer_offset_capacity_for(kind)
            .has_pending(consumer_id)
            || self
                .pending_consumer_offset_commits
                .values()
                .any(|pending| pending.kind == kind && pending.consumer_id == consumer_id)
            || (include_queued
                && self.no_ack.iter().any(|pending| {
                    pending.pending.kind == kind && pending.pending.consumer_id == consumer_id
                }))
        {
            return true;
        }
        let needs_messages =
            offset.is_some_and(|offset| self.store_offset_range_error(kind, offset).is_some());
        if needs_messages
            && self
                .pending_commit
                .as_ref()
                .is_some_and(PendingCommit::has_sends)
        {
            return true;
        }
        self.consensus.with_pipeline(|pipeline| {
            (include_queued
                && pipeline.pending_requests().any(|entry| {
                    (needs_messages && entry.message.header().operation == Operation::SendMessages)
                        || Self::parse_consumer_offset_request(
                            entry.message.header().operation,
                            &entry.message,
                        )
                        .is_ok_and(|(pending_kind, pending_id, _, _)| {
                            pending_kind == kind && pending_id == consumer_id
                        })
                }))
                || (needs_messages
                    && pipeline.prepare_head().is_some_and(|head| {
                        let tail = pipeline.prepare_tail().expect("pipeline head implies tail");
                        (head.header.op..=tail.header.op).any(|op| {
                            pipeline.message_by_op(op).is_some_and(|entry| {
                                entry.header.operation == Operation::SendMessages
                            })
                        })
                    }))
        })
    }

    /// `on_replicate` journals nothing during a transition, retirement or
    /// shutdown, so a primary must not originate a prepare while this holds.
    fn refuses_prepares(&self) -> bool {
        self.transition.is_some()
            || self.io_active.is_retiring()
            || self.maintenance.shutdown.is_some()
    }

    fn pipeline_local_request(
        &self,
        message: Message<RoutedRequestHeader>,
        sender: Option<consensus::Sender<Message<ReplyHeader>>>,
        order: Option<LocalRequestOrder>,
    ) -> Message<PrepareHeader> {
        debug_assert!(
            !self.refuses_prepares(),
            "on_replicate would drop a prepare that the pipeline already holds"
        );
        let prepare = consensus::seal_prepare_capacity(
            message.project(&self.consensus),
            self.consensus.retry_capacity(&self.dedup),
        );
        self.consensus.verify_pipeline();
        self.consensus
            .pipeline_message_with_order(PlaneKind::Partitions, &prepare, sender, order);
        if let Some(order) = order {
            self.local_request_order
                .set(self.local_request_order.get().max(order.0.get()));
        }
        prepare
    }

    /// Checks the batch of an idempotent producer right before the request
    /// becomes a prepare and its batch gets offsets. Answers a batch that the
    /// partition does not write, and returns whether the request goes on.
    async fn admit_producer_batch(
        &self,
        message: &Message<RoutedRequestHeader>,
        reply: &mut Option<consensus::Sender<Message<ReplyHeader>>>,
    ) -> bool {
        let header = message.header();
        let refusal = match self.check_producer_batch(message) {
            Ok(ProducerAdmission::Append) => return true,
            Ok(ProducerAdmission::Duplicate { base_offset }) => {
                // The batch committed, so the answer is a committed result,
                // which also settles a send the client holds as uncertain.
                let duplicate = build_reply_from_request(
                    &self.consensus,
                    header,
                    send_confirmation_body(header.group, base_offset),
                )
                .transmute_header(|mut duplicate, output: &mut ReplyHeader| {
                    duplicate.reserved[0] = ReplyHeader::COMMITTED_RESULT;
                    duplicate.partition_incarnation = header.partition_incarnation;
                    *output = duplicate;
                });
                Self::deliver_reply_or_log(
                    &self.consensus,
                    header,
                    CachedReply::from(duplicate),
                    reply.take(),
                    "producer duplicate reply send failed",
                )
                .await;
                return false;
            }
            // The first copy can still commit, so the outcome of this send
            // stays unknown, as for a request that is still pending.
            Ok(ProducerAdmission::DuplicateInFlight) => IggyError::TransientNotCommitted,
            Ok(ProducerAdmission::OutOfOrder) => IggyError::ProducerSequenceOutOfOrder,
            Ok(ProducerAdmission::Unknown) => IggyError::ProducerUnknown,
            Ok(ProducerAdmission::StaleEpoch) => IggyError::ProducerEpochFenced,
            Err(error) => error,
        };
        Self::send_partition_deny_or_log(
            &self.consensus,
            header,
            refusal.as_code(),
            "producer batch refusal reply failed",
            reply.take(),
        )
        .await;
        false
    }

    fn check_producer_batch(
        &self,
        message: &Message<RoutedRequestHeader>,
    ) -> Result<ProducerAdmission, IggyError> {
        if message.header().operation != Operation::SendMessages {
            return Ok(ProducerAdmission::Append);
        }
        let batch = message
            .as_slice()
            .get(std::mem::size_of::<RoutedRequestHeader>()..)
            .and_then(|body| BatchHeader::decode(body).ok())
            .ok_or(IggyError::InvalidCommand)?;
        let Some(producer) = batch.producer else {
            return Ok(ProducerAdmission::Append);
        };
        let in_flight = self.in_flight_producer_batches(producer.producer_id())?;
        Ok(self
            .producers
            .admit(producer, batch.message_count, &in_flight))
    }

    /// The batches of `producer_id` in the uncommitted prepares, in op order.
    /// A prepare this replica cannot read leaves the answer unknown, so the
    /// sender is told to retry.
    fn in_flight_producer_batches(&self, producer_id: u64) -> Result<Vec<StampedBatch>, IggyError> {
        let head = self.consensus.sequencer().current_sequence();
        let mut batches = Vec::new();
        for op in self.consensus.commit_min().saturating_add(1)..=head {
            let entry = self
                .log
                .journal()
                .inner
                .repair_entry(op)
                .filter(|entry| !entry.is_empty())
                .ok_or(IggyError::TransientNotAccepted)?;
            if peek_operation(&entry) != Operation::SendMessages {
                continue;
            }
            let batch = entry
                .as_slice()
                .get(std::mem::size_of::<PrepareHeader>()..)
                .and_then(|body| BatchHeader::decode(body).ok())
                .ok_or(IggyError::TransientNotAccepted)?;
            if let Some(producer) = batch
                .producer
                .filter(|producer| producer.producer_id() == producer_id)
            {
                batches.push(StampedBatch {
                    producer,
                    message_count: batch.message_count,
                    base_offset: batch.base_offset,
                    timestamp: batch.base_timestamp,
                });
            }
        }
        Ok(batches)
    }

    pub(crate) fn local_request_must_queue(&self) -> bool {
        self.consensus.pipeline_is_full()
            || self
                .consensus
                .with_pipeline(|pipeline| !pipeline.request_queue_is_empty())
            || !self.no_ack.is_empty()
    }

    fn queue_local_request(
        &self,
        mut request: consensus::RequestEntry,
        order: LocalRequestOrder,
    ) -> Result<(), consensus::RequestEntry> {
        request.local_order = Some(order);
        self.consensus.push_queued_request(request)?;
        self.local_request_order.set(order.0.get());
        self.notify_io();
        Ok(())
    }

    fn auto_commit_admission_ready(&self, kind: ConsumerKind, consumer_id: u32) -> bool {
        self.observed_view == self.consensus.view()
            && !self.offset_reservations_need_resync.get()
            && (!self.consumer_offset_capacity_for(kind).is_uncertain()
                || self.durable_consumer_offsets.contains(kind, consumer_id))
    }

    fn check_local_poll_key(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        if self.offset_key_exists(kind, consumer_id) {
            return Ok(());
        }
        let capacity = self.consumer_offset_capacity_for(kind);
        let count = self.consumer_offset_map_count(kind);
        if count >= capacity.limit() {
            self.reclaim_phantom_offsets(kind, count);
        }
        capacity.admit_local_map_key(
            self.consumer_offset_map_count(kind),
            self.durable_consumer_offsets.count(kind) >= capacity.limit(),
        )
    }

    /// Advance automatic progress without letting a slower read move it back.
    /// Explicit offset stores retain their separate semantics and may rewind.
    fn apply_local_poll_offset(&self, kind: ConsumerKind, consumer_id: u32, offset: u64) {
        let existed = self.offset_key_exists(kind, consumer_id);
        let create = || {
            ConsumerOffset::new(
                kind,
                consumer_id,
                offset,
                self.persisted_offset_path(kind, consumer_id)
                    .unwrap_or_default(),
            )
        };
        match kind {
            ConsumerKind::Consumer => {
                upsert_offset_max(&self.consumer_offsets, consumer_id as usize, offset, create);
            }
            ConsumerKind::ConsumerGroup => upsert_offset_max(
                &self.consumer_group_offsets,
                ConsumerGroupId(consumer_id as usize),
                offset,
                create,
            ),
            // Never polled, so it has no local progress.
            ConsumerKind::ExternalGroup => {}
        }
        if !existed {
            self.consumer_offset_capacity_for(kind)
                .note_local_key_change();
        }
    }

    fn consumer_offset_capacity_error(&self, error: ConsumerOffsetCapacityError) -> IggyError {
        if error.first_in_episode {
            warn!(namespace_raw = self.namespace().inner(), kind = ?error.kind,
                occupied = error.occupied, limit = error.limit, uncertain = error.uncertain,
                "consumer offset admission refused");
        }
        error.into()
    }

    fn build_poll_auto_commit_request(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
    ) -> Result<Message<RoutedRequestHeader>, IggyError> {
        let namespace = self.namespace();
        let request = StoreConsumerOffsetRequest {
            consumer: WireConsumer {
                kind: kind.as_code(),
                id: WireIdentifier::Numeric(consumer_id),
            },
            stream_id: WireIdentifier::Numeric(
                u32::try_from(namespace.stream_id())
                    .map_err(|_| IggyError::InvalidConfiguration)?,
            ),
            topic_id: WireIdentifier::Numeric(
                u32::try_from(namespace.topic_id()).map_err(|_| IggyError::InvalidConfiguration)?,
            ),
            partition_id: Some(
                u32::try_from(namespace.partition_id())
                    .map_err(|_| IggyError::InvalidConfiguration)?,
            ),
            offset,
            ack: AckLevel::Quorum,
        };
        let body = request.to_bytes();
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total_size = header_size + body.len();
        let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
        // Reserve the owner trailer without exposing it to request decoders before projection.
        let mut message = Message::<RoutedRequestHeader>::new(
            total_size
                + iggy_binary_protocol::requests::consumer_offsets::PREPARED_OWNER_GENERATION_SIZE,
        );
        message.as_mut_slice()[header_size..total_size].copy_from_slice(&body);
        Ok(
            message.transmute_header(|_, header: &mut RoutedRequestHeader| {
                *header = RoutedRequestHeader {
                    command: Command::Request,
                    operation: Operation::StoreConsumerOffset,
                    size,
                    client: AUTO_COMMIT_CLIENT_ID,
                    session: 1,
                    request: 1,
                    group: namespace.inner(),
                    partition_incarnation: self.created_revision(),
                    owner_generation: if kind == ConsumerKind::ConsumerGroup {
                        self.consumer_group_owner(u64::from(consumer_id))
                            .map_or(0, |owner| owner.generation)
                    } else {
                        0
                    },
                    ..Default::default()
                };
            }),
        )
    }

    fn apply_consumer_offset_commit(&self, pending: PendingConsumerOffsetCommit) {
        if keyed_by_group(pending.kind)
            && matches!(pending.mutation, PendingConsumerOffsetMutation::Delete)
        {
            self.mark_consumer_group_offsets_dirty();
        }
        let id = pending.consumer_id;
        if !pending.auto_commit {
            let now = self.consensus.message_bus().monotonic_micros();
            match pending.mutation {
                PendingConsumerOffsetMutation::Upsert(_) => {
                    self.explicit_offset_stores
                        .record_store(pending.kind, id, now);
                }
                PendingConsumerOffsetMutation::Delete => {
                    self.explicit_offset_stores
                        .record_delete(pending.kind, id, now);
                }
            }
        }
        let key = usize::try_from(id).expect("u32 consumer id must fit usize");
        match (pending.kind, pending.mutation) {
            (ConsumerKind::Consumer, PendingConsumerOffsetMutation::Upsert(offset)) => {
                let create = || {
                    self.consumer_offsets_dir(ConsumerKind::Consumer)
                        .map_or_else(
                            || ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
                            |path| ConsumerOffset::default_for_consumer(id, path),
                        )
                };
                upsert_committed_offset(
                    &self.consumer_offsets,
                    key,
                    offset,
                    pending.auto_commit,
                    create,
                );
            }
            (ConsumerKind::ConsumerGroup, PendingConsumerOffsetMutation::Upsert(offset)) => {
                let key = ConsumerGroupId(key);
                let create = || {
                    self.consumer_offsets_dir(ConsumerKind::ConsumerGroup)
                        .map_or_else(
                            || {
                                ConsumerOffset::new(
                                    ConsumerKind::ConsumerGroup,
                                    id,
                                    0,
                                    String::new(),
                                )
                            },
                            |path| ConsumerOffset::default_for_consumer_group(key, path),
                        )
                };
                upsert_committed_offset(
                    &self.consumer_group_offsets,
                    key,
                    offset,
                    pending.auto_commit,
                    create,
                );
            }
            // Two independently admitted deletes can commit after both saw
            // the key present. Deletion is idempotent on every replica, while
            // admission still rejects a request for an already absent key.
            (ConsumerKind::Consumer, PendingConsumerOffsetMutation::Delete) => {
                self.consumer_offsets.pin().remove(&key);
            }
            (ConsumerKind::ConsumerGroup, PendingConsumerOffsetMutation::Delete) => {
                self.consumer_group_offsets
                    .pin()
                    .remove(&ConsumerGroupId(key));
            }
            // `persist_consumer_offset_commit` already updated the durable
            // table, the only store of an external offset.
            (ConsumerKind::ExternalGroup, _) => {}
        }
    }

    /// Whether a client stored or deleted the offset of `consumer` at or after
    /// `since` on the bus clock, also with the value it held, or has such a
    /// store staged that is not applied yet. A parked committing read is
    /// fenced by any such store.
    #[must_use]
    pub fn consumer_offset_stored_since(&self, consumer: PollingConsumer, since: u64) -> bool {
        let (kind, id) = match consumer {
            PollingConsumer::Consumer(id, _) => (ConsumerKind::Consumer, id),
            PollingConsumer::ConsumerGroup(id, _) => (ConsumerKind::ConsumerGroup, id),
        };
        let Ok(id) = u32::try_from(id) else {
            return false;
        };
        // A staged store applies before the commit of a read that finishes
        // now, which would then pass over it.
        self.explicit_offset_stores.since(kind, id, since)
            || self
                .pending_consumer_offset_commits
                .values()
                .any(|pending| {
                    !pending.auto_commit && pending.kind == kind && pending.consumer_id == id
                })
    }

    /// Group ids that currently have a stored offset on this partition. Used by
    /// the reconciler to find offsets belonging to deleted consumer groups.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn consumer_group_offset_ids(&self) -> Vec<u64> {
        self.consumer_group_offsets
            .pin()
            .keys()
            .map(|key| key.0 as u64)
            .collect()
    }

    /// Snapshot the committed offsets of every kind keyed by consumer group
    /// whose group `is_live` rejects, for deletion through the partition's VSR
    /// log. A local unlink could free the primary's quota while backups
    /// retained every older generation, so reclamation uses the same ordered
    /// delete as an explicit consumer-offset request.
    ///
    /// Ordered by group id, with both kinds of one group side by side: the
    /// reconciler deletes a bounded number of keys per pass, so a backlog of
    /// one kind must not hold the other back by whole passes.
    #[must_use]
    pub fn dead_group_offset_keys(
        &self,
        is_live: impl Fn(u64) -> bool,
    ) -> Vec<(ConsumerKind, u32)> {
        if !self.consensus.is_primary() || !self.consensus.is_normal() {
            return Vec::new();
        }
        let mut dead = Vec::new();
        for kind in [ConsumerKind::ConsumerGroup, ConsumerKind::ExternalGroup] {
            // A stranded file already failed normal loading or unlink. Reissuing
            // replicated deletes every reconciliation pass cannot make its
            // filesystem writable and would create a permanent commit loop.
            // A single-replica explicit deletion can retry after repair. On a
            // replicated partition the file must be repaired or removed locally,
            // because older peers do not recognize a delete for a map-missing key.
            let capacity = self.consumer_offset_capacity_for(kind);
            let ids: Vec<u32> = self.durable_consumer_offsets.with_entries(kind, |entries| {
                entries
                    .keys()
                    .copied()
                    .filter(|&id| !is_live(u64::from(id)) && !capacity.is_stranded(id))
                    .collect()
            });
            dead.extend(ids.into_iter().map(|id| (kind, id)));
        }
        dead.sort_unstable_by_key(|&(kind, id)| (id, kind.index()));
        dead
    }

    pub(crate) fn set_consumer_group_offsets_dirty(
        &mut self,
        dirty: Rc<RefCell<BTreeSet<IggyNamespace>>>,
    ) {
        self.consumer_group_offsets_dirty = dirty;
        self.mark_consumer_group_offsets_dirty();
    }

    pub(crate) fn set_fence_published_flag(&mut self, flag: Rc<Cell<bool>>) {
        self.fence_published = flag;
    }

    fn mark_consumer_group_offsets_dirty(&self) {
        self.consumer_group_offsets_dirty
            .borrow_mut()
            .insert(self.namespace());
    }

    /// The stored offset of external group `group_id`, if any.
    #[must_use]
    pub fn external_group_offset(&self, group_id: u32) -> Option<u64> {
        self.durable_consumer_offsets
            .get(ConsumerKind::ExternalGroup, group_id)
            .map(|state| state.committed_offset)
    }

    /// Cooperative-rebalance classification: a group's `(last_polled, committed)`
    /// offsets on this partition, so the join enrichment can tell an in-flight
    /// partition (committed < last-polled) from a never-polled/drained one.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn group_offset_state(&self, group_id: u64) -> (Option<u64>, Option<u64>) {
        let key = ConsumerGroupId(group_id as usize);
        let load = |offset: &ConsumerOffset| offset.offset.load(Ordering::Relaxed);
        let last_polled = self.last_polled_offsets.pin().get(&key).map(load);
        let committed = self.consumer_group_offsets.pin().get(&key).map(load);
        (last_polled, committed)
    }

    /// The FIFO owns capacity and replies until the local mutation's barrier settles.
    #[allow(clippy::future_not_send)]
    async fn apply_consumer_offset_no_ack(
        &mut self,
        request_header: Box<RoutedRequestHeader>,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let admission = self.reserve_no_ack(kind, consumer_id);
        let (order, reservation) = match admission {
            Ok(admission) => admission,
            Err(error) => {
                Self::send_partition_deny_or_log(
                    &self.consensus,
                    &request_header,
                    error.as_code(),
                    "no_ack admission reply failed",
                    waiter,
                )
                .await;
                return;
            }
        };
        let pending = PendingConsumerOffsetCommit::explicit(kind, consumer_id, offset);
        self.no_ack.push_back(PendingNoAck {
            request_header,
            pending,
            reply: waiter,
            local_order: order,
            reservation,
            phase: NoAckPhase::Validate,
        });
        self.local_request_order.set(order.0.get());
        self.drain_no_ack().await;
    }

    fn reserve_no_ack(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(LocalRequestOrder, AutoCommitReservation), IggyError> {
        if self.no_ack.len()
            >= self
                .consensus
                .with_pipeline(consensus::LocalPipeline::request_queue_capacity)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        let order = self.next_local_request_order()?;
        let reservation = self
            .consumer_offset_capacity_for(kind)
            .reserve_provisional(consumer_id, &self.durable_consumer_offsets)
            .map_err(|error| self.consumer_offset_capacity_error(error))?;
        Ok((order, reservation))
    }

    async fn drain_no_ack(&mut self) {
        if self.has_io_dispatcher() {
            self.notify_io();
            return;
        }
        loop {
            match self.drive_no_ack() {
                NoAckStep::Progress => {}
                NoAckStep::Pending => return,
                NoAckStep::Ready => match self.capture_no_ack_phase() {
                    Ok(job) => {
                        let result = job.execute().await;
                        self.accept_no_ack_phase(result);
                    }
                    Err(error) => self.fail_no_ack(error, false),
                },
                NoAckStep::Reply => self.deliver_no_ack_reply().await,
            }
        }
    }

    fn drive_no_ack(&mut self) -> NoAckStep {
        let Some(head) = self.no_ack.front() else {
            return NoAckStep::Pending;
        };
        match &head.phase {
            NoAckPhase::Validate => {
                let pending = head.pending;
                if let Some(error) = self.no_ack_refusal(head) {
                    self.fail_no_ack(error, false);
                    return NoAckStep::Reply;
                }
                if self.pending_commit.is_some() {
                    return NoAckStep::Pending;
                }
                if self
                    .consensus
                    .with_pipeline(|pipeline| pipeline.has_request_before(head.local_order))
                {
                    if self.maintenance.shutdown.is_some() {
                        self.fail_no_ack(IggyError::TransientNotAccepted, false);
                        return NoAckStep::Reply;
                    }
                    return NoAckStep::Pending;
                }
                let validation = match pending.mutation {
                    PendingConsumerOffsetMutation::Upsert(offset) => self
                        .store_offset_range_error(pending.kind, offset)
                        .map_or(Ok(()), Err),
                    PendingConsumerOffsetMutation::Delete => {
                        self.ensure_consumer_offset_exists(pending.kind, pending.consumer_id)
                    }
                };
                if let Err(error) = validation {
                    self.fail_no_ack(error, false);
                    return NoAckStep::Reply;
                }
                let tracked = self
                    .durable_consumer_offsets
                    .get(pending.kind, pending.consumer_id);
                let plan = self.plan_consumer_offset_effect(
                    pending,
                    tracked,
                    self.durable_consumer_offsets.emptied_at(),
                );
                if let ConsumerOffsetAction::Ready(effect) = plan.action {
                    self.publish_no_ack_effect(effect);
                } else {
                    self.no_ack.front_mut().expect("no_ack head exists").phase =
                        NoAckPhase::OffsetReady(plan);
                }
                NoAckStep::Progress
            }
            NoAckPhase::OffsetReady(plan) if self.offset_effect_waits_for_checkpoint(plan) => {
                NoAckStep::Pending
            }
            NoAckPhase::OffsetReady(_) => NoAckStep::Ready,
            NoAckPhase::Directory => {
                if self.consumer_offset_dirs_dirty[head.pending.kind.index()].get() {
                    NoAckStep::Ready
                } else {
                    self.no_ack.front_mut().expect("no_ack head exists").phase =
                        NoAckPhase::Reply(Ok(()));
                    NoAckStep::Reply
                }
            }
            NoAckPhase::OffsetSubmitted(_) | NoAckPhase::DirectorySubmitted => NoAckStep::Pending,
            NoAckPhase::Reply(_) => NoAckStep::Reply,
        }
    }

    fn capture_no_ack_phase(&mut self) -> Result<crate::io::PartitionIoJob<SB>, IggyError> {
        // TODO: Batch independent NoAck offsets while preserving key and session order.
        let head = self.no_ack.front().ok_or(IggyError::CannotWriteToFile)?;
        let (job, submitted) = match &head.phase {
            NoAckPhase::OffsetReady(plan) => (
                self.capture_consumer_offset_effect(plan)?,
                NoAckPhase::OffsetSubmitted(*plan),
            ),
            NoAckPhase::Directory => {
                let mut kinds = [false; ConsumerKind::COUNT];
                kinds[head.pending.kind.index()] = true;
                (
                    crate::io::PartitionIoJob::OffsetDirectories(
                        self.prepare_consumer_offset_directories(kinds),
                    ),
                    NoAckPhase::DirectorySubmitted,
                )
            }
            _ => return Err(IggyError::CannotWriteToFile),
        };
        self.no_ack.front_mut().expect("no_ack head exists").phase = submitted;
        Ok(job)
    }

    fn accept_no_ack_phase(&mut self, result: crate::io::PartitionIoResult) {
        let Some(head) = self.no_ack.front() else {
            return;
        };
        match (&head.phase, result) {
            (NoAckPhase::OffsetSubmitted(plan), result) => {
                match self.accept_consumer_offset_effect(*plan, result) {
                    Ok(effect) => self.publish_no_ack_effect(effect),
                    Err(error) => self.fail_no_ack(error, true),
                }
            }
            (
                NoAckPhase::DirectorySubmitted,
                crate::io::PartitionIoResult::OffsetDirectories(result),
            ) => {
                self.accept_consumer_offset_directories(&result);
                if result.failed.iter().any(|failed| *failed) {
                    self.fail_no_ack(IggyError::CannotSyncFile, true);
                } else {
                    self.no_ack.front_mut().expect("no_ack head exists").phase =
                        NoAckPhase::Reply(Ok(()));
                }
            }
            _ => self.fail_no_ack(IggyError::CannotWriteToFile, true),
        }
    }

    fn publish_no_ack_effect(&mut self, effect: ConsumerOffsetEffect) {
        self.publish_consumer_offset_effect(effect);
        self.apply_consumer_offset_commit(effect.pending);
        self.no_ack.front_mut().expect("no_ack head exists").phase = NoAckPhase::Directory;
    }

    fn fail_no_ack(&mut self, error: IggyError, file_attempted: bool) {
        let Some(head) = self.no_ack.front() else {
            return;
        };
        if file_attempted {
            self.consumer_offset_capacity_for(head.pending.kind)
                .record_stranded(head.pending.consumer_id);
        }
        warn!(
            namespace_raw = self.namespace().inner(),
            operation = ?head.request_header.operation, %error,
            "local consumer offset mutation failed"
        );
        self.no_ack.front_mut().expect("no_ack head exists").phase = NoAckPhase::Reply(Err(error));
    }

    async fn deliver_no_ack_reply(&mut self) {
        let Some(head) = self.no_ack.pop_front() else {
            return;
        };
        let NoAckPhase::Reply(outcome) = head.phase else {
            self.no_ack.push_front(head);
            return;
        };
        let reply = match outcome {
            Ok(()) => build_reply_from_request(
                &self.consensus,
                &head.request_header,
                committed_reply_body(head.request_header.operation),
            ),
            Err(error) => build_deny_reply_from_request(
                &self.consensus,
                &head.request_header,
                error.as_code(),
            ),
        };
        Self::deliver_reply_or_log(
            &self.consensus,
            &head.request_header,
            reply.into(),
            head.reply,
            "no_ack reply send failed",
        )
        .await;
        drop(head.reservation);
        self.refresh_consumer_offset_reservation(head.pending.kind, head.pending.consumer_id);
    }

    pub(crate) fn persisted_offset_path(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Option<String> {
        self.consumer_offsets_dir(kind)
            .map(|path| format!("{path}/{consumer_id}"))
    }

    /// The offset directory of `kind`, once storage is configured.
    pub(crate) fn consumer_offsets_dir(&self, kind: ConsumerKind) -> Option<&str> {
        self.consumer_offset_dirs[kind.index()].as_deref()
    }

    /// The marker beside the offset directories that names the committed
    /// delete which emptied every kind. `None` unless every kind has a
    /// directory under one parent, which is where boot reads it.
    pub(crate) fn emptied_marker_path(&self) -> Option<String> {
        let mut parents = ConsumerKind::ALL.into_iter().map(|kind| {
            self.consumer_offsets_dir(kind)
                .and_then(|dir| std::path::Path::new(dir).parent())
        });
        let parent = parents.next().flatten()?;
        parents.all(|other| other == Some(parent)).then(|| {
            parent
                .join(crate::offset_storage::EMPTIED_MARKER_FILE)
                .to_string_lossy()
                .into_owned()
        })
    }

    pub(crate) const fn consumer_offset_capacity_for(
        &self,
        kind: ConsumerKind,
    ) -> &ConsumerOffsetCapacity {
        &self.consumer_offset_capacities[kind.index()]
    }

    /// Whether `kind` holds `consumer_id`: the live map of a polled kind, or the
    /// durable table of an external group.
    fn offset_key_exists(&self, kind: ConsumerKind, consumer_id: u32) -> bool {
        let key = || usize::try_from(consumer_id).expect("u32 consumer id must fit usize");
        match kind {
            ConsumerKind::Consumer => self.consumer_offsets.pin().contains_key(&key()),
            ConsumerKind::ConsumerGroup => self
                .consumer_group_offsets
                .pin()
                .contains_key(&ConsumerGroupId(key())),
            ConsumerKind::ExternalGroup => {
                self.durable_consumer_offsets.contains(kind, consumer_id)
            }
        }
    }

    fn reserve_consumer_offset(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), ConsumerOffsetCapacityError> {
        self.consumer_offset_capacity_for(kind)
            .try_reserve(consumer_id, &self.durable_consumer_offsets)
    }

    fn refresh_consumer_offset_reservation(&self, kind: ConsumerKind, consumer_id: u32) {
        let count = self
            .pending_consumer_offset_commits
            .values()
            .filter(|pending| {
                pending.kind == kind
                    && pending.consumer_id == consumer_id
                    && matches!(pending.mutation, PendingConsumerOffsetMutation::Upsert(_))
            })
            .count();
        let capacity = self.consumer_offset_capacity_for(kind);
        capacity.set_pending_count(consumer_id, count);
        capacity.rearm_if_below_limit(&self.durable_consumer_offsets);
    }

    async fn admit_consumer_offset_key(
        &self,
        header: &RoutedRequestHeader,
        kind: ConsumerKind,
        consumer_id: u32,
        waiter: &mut Option<consensus::Sender<Message<ReplyHeader>>>,
    ) -> bool {
        if self.consumer_offset_capacity_for(kind).is_uncertain()
            && !self.durable_consumer_offsets.contains(kind, consumer_id)
        {
            Self::send_partition_deny_or_log(
                &self.consensus,
                header,
                IggyError::TransientNotAccepted.as_code(),
                "consumer offset accounting unavailable reply send failed",
                waiter.take(),
            )
            .await;
            return false;
        }
        let Err(capacity_error) = self.reserve_consumer_offset(kind, consumer_id) else {
            return true;
        };
        if capacity_error.first_in_episode {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = self.consensus.replica(),
                namespace_raw = self.namespace().inner(),
                ?kind,
                occupied = capacity_error.occupied,
                limit = capacity_error.limit,
                config = "[partition] consumer_offsets_max",
                "consumer offset admission limit reached"
            );
        }
        Self::send_partition_deny_or_log(
            &self.consensus,
            header,
            IggyError::TooManyConsumerOffsets.as_code(),
            "consumer offset capacity deny reply send failed",
            waiter.take(),
        )
        .await;
        false
    }

    fn ensure_consumer_offset_exists(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), IggyError> {
        if self.durable_consumer_offsets.contains(kind, consumer_id) {
            return Ok(());
        }
        // A local-only cursor is not a replicated key. Older replicas reject
        // missing-key deletes when replaying as primary during a rolling upgrade.
        if self.consensus.replica_count() > 1 {
            return Err(IggyError::ConsumerOffsetNotFound(consumer_id as usize));
        }
        let found = self.offset_key_exists(kind, consumer_id);

        let local_stranded = self
            .consumer_offset_capacity_for(kind)
            .is_stranded(consumer_id);
        if found || local_stranded {
            Ok(())
        } else {
            Err(IggyError::ConsumerOffsetNotFound(
                usize::try_from(consumer_id).expect("u32 consumer id must fit usize"),
            ))
        }
    }

    #[must_use]
    fn diag_ctx(&self) -> ReplicaLogContext {
        ReplicaLogContext::from_consensus(self.consensus(), PlaneKind::Partitions)
    }

    /// `None` for an external group: its value belongs to the external system.
    fn store_offset_range_error(&self, kind: ConsumerKind, offset: u64) -> Option<IggyError> {
        if kind == ConsumerKind::ExternalGroup {
            return None;
        }
        let current = self.stats.current_offset();
        (offset > current || (current == 0 && self.stats.messages_count_inconsistent() == 0))
            .then_some(IggyError::InvalidOffset(offset))
    }

    fn resynchronize_consumer_offset_reservations(&mut self) {
        self.resynchronize_consumer_offset_reservations_inner(false);
    }

    /// Retry incomplete accounting at most once per shard tick, after progress.
    pub fn retry_consumer_offset_reservations(&mut self) {
        if self.any_consumer_offset_capacity_uncertain() {
            self.resynchronize_consumer_offset_reservations_inner(true);
        }
    }

    fn any_consumer_offset_capacity_uncertain(&self) -> bool {
        self.consumer_offset_capacities
            .iter()
            .any(ConsumerOffsetCapacity::is_uncertain)
    }

    #[allow(clippy::too_many_lines)]
    fn resynchronize_consumer_offset_reservations_inner(&mut self, from_tick: bool) {
        let current_view = self.consensus.view();
        let scan_state = (
            self.consensus.commit_min(),
            self.consensus.commit_max(),
            self.consensus.sequencer().current_sequence(),
            self.log.journal().inner.last_op(),
        );
        let uncertain = self.any_consumer_offset_capacity_uncertain();
        // After a truncation the latch rebuild below runs before the next
        // admission reads the latch, even while uncertainty holds rescans.
        let truncated = self.truncated_since_scan.take();
        if current_view == self.observed_view && !truncated {
            if uncertain && !from_tick {
                return;
            }
            let retry_requested = self.offset_reservations_need_resync.get()
                || (uncertain && self.offset_reservations_scan_state != Some(scan_state));
            if !retry_requested {
                return;
            }
        }

        if current_view != self.observed_view {
            self.last_polled_offsets.pin().clear();
            self.consumer_group_polls.clear();
            self.pending_owner_install.set(None);
            self.pending_history_transition.set(None);
            self.discard_queued_auto_commits();
            self.mark_consumer_group_offsets_dirty();
        } else if truncated {
            // A dropped install never publishes, so a latch on it refuses every
            // later install. A truncation follows a view change, which empties
            // the request queue, so the retained log alone re-arms the latch.
            self.pending_owner_install.set(None);
        }

        let from_op = self.consensus.commit_min().saturating_add(1);
        let commit_max = self.consensus.commit_max();
        let to_op = self
            .consensus
            .sequencer()
            .current_sequence()
            .min(self.log.journal().inner.last_op().unwrap_or(commit_max));
        // Committed offset prepares still need local apply, even if a message
        // flush already evicted their journal bytes. Never drop their staging.
        // Within one view an op is assigned once, so uncommitted staging is
        // kept too; only a view change can replace what sits at those ops, and
        // only then is the tail decoded again.
        let same_view = current_view == self.observed_view;
        let mut rebuilt: HashMap<_, _> = self
            .pending_consumer_offset_commits
            .iter()
            .filter(|(op, _)| {
                **op >= from_op && (**op <= commit_max || (same_view && **op <= to_op))
            })
            .map(|(op, pending)| (*op, *pending))
            .collect();
        let headers = self.log.journal().inner.repair_headers_in(from_op..=to_op);
        self.owner_install_barrier = headers
            .iter()
            .rev()
            .find(|(_, header)| header.operation == Operation::InstallConsumerGroupOwner)
            .map_or(0, |(op, _)| *op);
        let uncommitted_from = from_op.max(commit_max.saturating_add(1));
        let expected = to_op
            .checked_sub(uncommitted_from)
            .map_or(0, |span| span.saturating_add(1));
        let mut decode_failed =
            headers.keys().filter(|op| **op >= uncommitted_from).count() as u64 != expected;
        for (op, header) in headers {
            if header.operation == Operation::TransitionPartitionHistory
                && let Some(prepare) = self.log.journal().inner.repair_entry(op)
                && let Some(body) = prepare.as_slice().get(iggy_binary_protocol::HEADER_SIZE..)
                && let Ok(transition) = TransitionPartitionHistoryRequest::decode_from(body)
            {
                self.pending_history_transition.set(Some(transition));
            }
            if header.operation == Operation::InstallConsumerGroupOwner
                && let Some(prepare) = self.log.journal().inner.repair_entry(op)
                && let Some(body) = prepare.as_slice().get(iggy_binary_protocol::HEADER_SIZE..)
                && let Ok(installation) = InstallConsumerGroupOwnerRequest::decode_from(body)
            {
                self.pending_owner_install.set(Some(installation));
            }
            if !matches!(
                header.operation,
                Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
            ) {
                continue;
            }
            if rebuilt.contains_key(&op) {
                continue;
            }
            match self.restage_consumer_offset_from_journal(op) {
                Ok(pending) => {
                    rebuilt.insert(op, pending);
                }
                Err(error) => {
                    error!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        replica_id = self.consensus.replica(),
                        namespace_raw = self.namespace().inner(),
                        op,
                        %error,
                        "failed to rebuild consumer offset reservations after view change"
                    );
                    decode_failed = true;
                    break;
                }
            }
        }
        self.pending_consumer_offset_commits = rebuilt;
        if decode_failed {
            for capacity in &self.consumer_offset_capacities {
                capacity.mark_uncertain();
            }
        } else {
            for kind in ConsumerKind::ALL {
                let ids = self
                    .pending_consumer_offset_commits
                    .values()
                    .filter(|pending| {
                        pending.kind == kind
                            && matches!(pending.mutation, PendingConsumerOffsetMutation::Upsert(_))
                    })
                    .map(|pending| pending.consumer_id);
                self.consumer_offset_capacity_for(kind)
                    .rebuild(&self.durable_consumer_offsets, ids);
            }
        }
        self.observed_view = current_view;
        self.offset_reservations_scan_state = Some(scan_state);
        // The shard tick retries uncertainty after journal or frontier progress.
        self.offset_reservations_need_resync.set(false);
    }

    fn reclaim_phantom_offsets(&self, kind: ConsumerKind, map_count: usize) {
        let capacity = self.consumer_offset_capacity_for(kind);
        if self.durable_consumer_offsets.count(kind) >= capacity.limit()
            || !capacity.should_reclaim(&self.durable_consumer_offsets)
        {
            return;
        }
        let needed = map_count.saturating_sub(capacity.limit()).saturating_add(1);
        match kind {
            ConsumerKind::Consumer => {
                self.reclaim_phantom_offset_keys(&self.consumer_offsets, kind, needed, |key| {
                    u32::try_from(*key).ok()
                });
            }
            ConsumerKind::ConsumerGroup => self.reclaim_phantom_offset_keys(
                &self.consumer_group_offsets,
                kind,
                needed,
                |key| u32::try_from(key.0).ok(),
            ),
            // Never polled, so every key it holds is durable.
            ConsumerKind::ExternalGroup => {}
        }
    }

    fn reclaim_phantom_offset_keys<K: Hash + Eq>(
        &self,
        offsets: &papaya::HashMap<K, ConsumerOffset>,
        kind: ConsumerKind,
        mut remaining: usize,
        consumer_id: impl Fn(&K) -> Option<u32>,
    ) {
        let capacity = self.consumer_offset_capacity_for(kind);
        let map = offsets.pin();
        for (key, _) in &map {
            if let Some(id) = consumer_id(key)
                && !capacity.holds(id, &self.durable_consumer_offsets)
                && map.remove(key).is_some()
            {
                capacity.forget_inactive_provisional(id);
                // This cursor was never durable, so the consumer's next `Next`
                // poll restarts from offset 0 and redelivers. At-least-once
                // permits it; an operator should still see it happen.
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    ?kind,
                    consumer_id = id,
                    "reclaimed a local consumer offset cursor with no durable backing; its next \
                     poll restarts from offset 0"
                );
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
    }

    /// Snapshot read resources synchronously on the partition owner.
    /// Only the owned snapshot crosses a suspension during disk I/O.
    #[cfg(test)]
    pub(crate) fn build_poll_plan(
        &mut self,
        consumer: PollingConsumer,
        args: &PollingArgs,
        validate_checksum: bool,
    ) -> PollPlan {
        self.build_poll_plan_within(consumer, args, validate_checksum, usize::MAX)
    }

    /// Hand polls the store that holds this partition's remote segments.
    pub fn set_remote_segments(&mut self, remote: Rc<crate::RemoteSegments>) {
        self.remote_segments = Some(remote);
    }

    /// [`Self::build_poll_plan`] for a poll whose reply holds at most
    /// `byte_budget` bytes of messages.
    ///
    /// The snapshot ends at the segment that covers the poll's count or its
    /// budget, whichever comes first. Remote segments make the list as long
    /// as total retention, so a snapshot that ran to the tail would cost a
    /// poll near the front of a long topic a clone per segment.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn build_poll_plan_within(
        &mut self,
        consumer: PollingConsumer,
        args: &PollingArgs,
        validate_checksum: bool,
        byte_budget: usize,
    ) -> PollPlan {
        // Reads the durable commit frontier (`self.offset`, stored only on
        // commit). Also used below as the poll's high-water bound: this function
        // is fully synchronous, so the single load cannot drift mid-plan.
        let commit_offset = self.offsets().commit_offset;
        let owner = match consumer {
            PollingConsumer::ConsumerGroup(group_id, _) => {
                self.consumer_group_owner(group_id as u64)
            }
            PollingConsumer::Consumer(..) => None,
        };
        let group_poll = match consumer {
            PollingConsumer::ConsumerGroup(group_id, _) if owner.is_some() => Some(Arc::clone(
                self.consumer_group_polls
                    .entry(group_id as u64)
                    .or_default(),
            )),
            _ => None,
        };
        let context = PollContext {
            history: self.poll_history,
            consumer,
            auto_commit: args.auto_commit,
            owner,
            _group_poll: group_poll,
        };
        if !self.offset_space.committed_seeded || args.count == 0 {
            return PollPlan {
                commit_offset,
                context,
                tier: PollTier::Empty,
            };
        }

        let query = match args.kind {
            PollingKind::Timestamp => MessageLookup::Timestamp {
                timestamp: args.value,
                count: args.count,
                ceiling: commit_offset,
            },
            kind => {
                let start_offset = match kind {
                    PollingKind::Offset => args.value,
                    PollingKind::First => 0,
                    PollingKind::Last => commit_offset.saturating_sub(u64::from(args.count) - 1),
                    PollingKind::Next => self
                        .get_consumer_offset(consumer)
                        .map_or(0, |offset| offset + 1),
                    PollingKind::Timestamp => unreachable!(),
                };
                if start_offset > commit_offset {
                    return PollPlan {
                        commit_offset,
                        context,
                        tier: PollTier::Empty,
                    };
                }
                MessageLookup::Offset {
                    offset: start_offset,
                    count: args.count,
                    ceiling: commit_offset,
                }
            }
        };

        if args.auto_commit
            && let Ok(pending) = PendingConsumerOffsetCommit::try_from_polling_consumer(consumer, 0)
        {
            let capacity = self.consumer_offset_capacity_for(pending.kind);
            if !capacity.is_uncertain()
                && !self.offset_key_exists(pending.kind, pending.consumer_id)
            {
                let map_count = self.consumer_offset_map_count(pending.kind);
                if map_count >= capacity.limit() {
                    self.reclaim_phantom_offsets(pending.kind, map_count);
                }
            }
        }

        let serve_journal_first = match query {
            MessageLookup::Offset { offset, .. } => self
                .log
                .journal()
                .inner
                .oldest_resident_offset()
                .is_some_and(|oldest| offset >= oldest),
            MessageLookup::Timestamp { .. } => !self.has_persisted_segment_bytes(),
        };

        if serve_journal_first {
            let tier = match self.journal_get_sync(&query) {
                Some((fragments, last_matching_offset, message_count)) => PollTier::Resident {
                    fragments,
                    last_matching_offset,
                    message_count,
                },
                None => PollTier::Empty,
            };
            return PollPlan {
                commit_offset,
                context,
                tier,
            };
        }

        let (start_segment, start_position, start_index_offset) = self.disk_poll_start(&query);
        // Cap resident sealed read handles: touch this poll's start segment so
        // the LRU keeps the hot set and drops the least-recently-used fd +
        // index (a no-op for the active segment, whose slot is bounded by
        // rotation instead).
        self.log.touch_sealed_read_state(start_segment);
        // Snapshot only the segments the disk walk visits (`start_segment..`),
        // so `start_position` applies to the first snapshotted segment. Every
        // segment carries its shared read-state handle so the off-borrow read
        // reuses (or fills) the cached fd; only a sealed one also resolves its
        // start byte from the shared sparse index.
        let object_prefix = self.object_prefix();
        let walked = walked_segments(
            &self.log.segments()[start_segment..],
            args.count,
            byte_budget,
        );
        let cut = start_segment + walked < self.log.segments().len();
        let segments = self.log.segments()[start_segment..start_segment + walked]
            .iter()
            .zip(self.log.sealed_read_state()[start_segment..].iter())
            .map(|(segment, read_state)| DiskSegment {
                start_offset: segment.start_offset,
                persisted: segment.size.as_bytes_u64(),
                read_state: Rc::clone(read_state),
                sealed: segment.sealed,
                source: match object_prefix {
                    Some(prefix) if segment.location == SegmentLocation::Remote => {
                        SegmentSource::Remote(RemoteRange {
                            key: SegmentKey {
                                prefix,
                                start_offset: segment.start_offset,
                                object: SegmentObject::Log,
                            },
                            range: 0..segment.size.as_bytes_u64(),
                        })
                    }
                    _ => SegmentSource::Local,
                },
            })
            .collect();
        let disk = DiskReadPlan {
            partition_dir: self.partition_dir_resolution(),
            remote: self.remote_segments.clone(),
            object_prefix,
            index_fetches: self.index_fetches.fence(),
            segments,
            cut,
            deadline: None,
            start_position,
            start_index_offset,
            namespace_raw: self.namespace().inner(),
            validate_checksum,
            bytes_per_message: self.mean_encoded_message_size(),
            widest_batch_bytes: self.widest_committed_batch(),
        };
        // Snapshot the resident journal tail now (on the pump, under the
        // borrow) so the straddle splice runs off-task on owned data with no
        // partition reference. Point-in-time, so immune to a concurrent commit
        // evicting the run just past the disk match.
        let resident_tail = self.resident_tail_snapshot();
        PollPlan {
            commit_offset,
            context,
            tier: PollTier::Disk {
                disk,
                query,
                resident_tail,
            },
        }
    }

    /// Synchronous in-memory journal poll, for the resident tier. Never awaits
    /// (see [`PartitionJournal::get_sync`]), so it is safe under a partition
    /// borrow.
    pub(crate) fn journal_get_sync(&self, query: &MessageLookup) -> Option<PollQueryResult<4096>> {
        self.log.journal().inner.get_sync(query)
    }

    /// Snapshot the resident journal tail (oldest resident offset + op-ascending
    /// message entry clones) for the disk-tier straddle continuation. Taken
    /// synchronously under the partition borrow so the splice runs off-task on
    /// owned data; see [`ResidentTailSnapshot`].
    fn resident_tail_snapshot(&self) -> ResidentTailSnapshot {
        let journal = &self.log.journal().inner;
        ResidentTailSnapshot {
            oldest_resident: journal.oldest_resident_offset(),
            entries: journal.resident_message_entries(),
        }
    }
}

impl<B, SB> Partition for IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    async fn append_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<AppendResult, IggyError> {
        self.stamp_and_append_messages(message)
            .await
            .map(|journaled| journaled.result)
    }

    fn get_consumer_offset(&self, consumer: PollingConsumer) -> Option<u64> {
        match consumer {
            PollingConsumer::Consumer(id, _) => self
                .consumer_offsets
                .pin()
                .get(&id)
                .map(|co| co.offset.load(Ordering::Relaxed)),
            PollingConsumer::ConsumerGroup(group_id, _) => self
                .consumer_group_offsets
                .pin()
                .get(&ConsumerGroupId(group_id))
                .map(|co| co.offset.load(Ordering::Relaxed)),
        }
    }

    fn offsets(&self) -> PartitionOffsets {
        PartitionOffsets::new(
            self.offset.load(Ordering::Acquire),
            self.dirty_offset.load(Ordering::Relaxed),
        )
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    async fn stamp_and_append_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<JournaledMessages, IggyError> {
        let header = *message.header();
        if header.operation != Operation::SendMessages {
            return Err(IggyError::CannotAppendMessage);
        }

        // Only here: this is the only path that mints. A backup re-stamps what
        // the primary sends (`append_received_send_messages_to_journal`) and
        // must follow it exactly, so raising ITS counter would fork the group.
        let dirty_offset = if self.offset_space.append_live {
            self.dirty_offset
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or(IggyError::CannotAppendMessage)?
        } else {
            0
        };

        // Reuse the prepare's monotonic timestamp, assigned once by the primary
        // in `project()` (`next_monotonic_timestamp`) and replicated verbatim to
        // every backup. Sourcing it here instead of a fresh local `now()` makes
        // the persisted `base_timestamp` (and the `batch_checksum` derived from
        // it) byte-identical across replicas. A local `now()` diverges per node.
        let batch_timestamp = header.timestamp;
        let (message, batch, batch_messages_count) =
            stamp_prepare_for_persistence(message, dirty_offset, batch_timestamp)
                .map_err(|_| IggyError::CannotAppendMessage)?;

        debug_assert_eq!(batch.message_count, batch_messages_count);
        self.append_stamped_messages(message, batch).await
    }
    #[must_use]
    pub(crate) fn namespace(&self) -> IggyNamespace {
        IggyNamespace::from_raw(self.consensus.group())
    }

    #[must_use]
    pub const fn incarnation(&self) -> crate::PartitionIncarnation {
        self.incarnation
    }

    pub fn set_io_notifier(&self, notifier: crate::PartitionIoNotifier, bytes_max: usize) {
        self.io_bytes_max.set(bytes_max);
        *self.io_notifier.borrow_mut() = Some(notifier);
        self.notify_io();
    }

    /// Wake this incarnation's dispatcher, if one is installed.
    pub fn notify_io(&self) {
        let notifier = self.io_notifier.borrow().clone();
        if let Some(notifier) = notifier {
            notifier(self.namespace(), self.incarnation);
        }
    }

    /// [`Self::notify_io`] for a detached task that outlives this borrow.
    pub(crate) fn io_wake(&self) -> impl Fn() + 'static {
        let notifier = self.io_notifier.borrow().clone();
        let (namespace, incarnation) = (self.namespace(), self.incarnation);
        move || {
            if let Some(notifier) = &notifier {
                notifier(namespace, incarnation);
            }
        }
    }

    #[must_use]
    pub fn needs_io_retry(&self) -> bool {
        if self.io_active.is_interrupted() {
            // File resources stay pinned while the WAL drain still needs deadline probes.
            return self
                .maintenance
                .shutdown
                .is_some_and(|phase| phase != PartitionShutdownPhase::Complete);
        }
        self.io_active.get().is_none()
            && (self.pending_commit.is_some()
                || self.materialization.is_some()
                || self.transition.is_some()
                || !self.deferred_views.is_empty()
                || self.superblock_intent.get().is_some()
                || self.pending_wire_actions.borrow().is_some()
                || !self.pending_persisted_acks.borrow().is_empty()
                || !self.no_ack.is_empty()
                || self.maintenance.flush_through.is_some()
                || self.maintenance.checkpoint.is_some()
                || self.maintenance.retention.is_some()
                || self.maintenance.eviction.is_some()
                || self.maintenance.tiering.is_some()
                || self
                    .maintenance
                    .shutdown
                    .is_some_and(|phase| phase != PartitionShutdownPhase::Complete)
                || self.queued_requests_ready())
    }

    #[must_use]
    pub const fn has_pending_commit(&self) -> bool {
        self.pending_commit.is_some()
    }

    #[must_use]
    pub fn read_history_is_changing(&self) -> bool {
        self.io_active.is_retiring()
            || !self.deferred_views.is_empty()
            || matches!(
                self.transition,
                Some(
                    PendingPartitionTransition::Install(_)
                        | PendingPartitionTransition::Quarantine { .. }
                )
            )
    }

    /// Whether history work or an accepted `NoAck` request must settle first.
    pub fn history_is_busy(&self) -> bool {
        self.history_writes_pending() || !self.no_ack.is_empty()
    }

    /// Whether a writer or transition still owns the current history.
    pub fn history_writes_pending(&self) -> bool {
        self.io_active.get().is_some()
            || self.io_active.is_retiring()
            || self.pending_commit.is_some()
            || self.materialization.is_some()
            || self.maintenance.flush_through.is_some()
            || self.no_ack.front().is_some_and(|head| {
                !matches!(
                    head.phase,
                    NoAckPhase::Validate | NoAckPhase::OffsetReady(_)
                )
            })
            || self.transition.is_some()
    }

    /// Fence new admission while accepted history work drains for this grant.
    /// Refusal leaves the current history owner unchanged.
    pub fn defer_primary_transfer(&mut self, request: crate::PreparedPrimaryTransfer) -> bool {
        if self.transition.is_some()
            || !self.deferred_views.is_empty()
            || !self.primary_transfer_is_current(&request)
        {
            return false;
        }
        self.transition = Some(PendingPartitionTransition::PrimaryTransfer {
            request,
            drain: None,
        });
        self.notify_io();
        true
    }

    fn primary_transfer_is_current(&self, request: &crate::PreparedPrimaryTransfer) -> bool {
        request.is_active_at(self.consensus.message_bus().monotonic_now())
            && request.created_revision == self.created_revision()
            && request.expected_view == self.consensus.view()
            // A fenced backup refuses prepares while the primary keeps
            // committing, so its drain stalls until the grant expires.
            && self.consensus.is_primary()
            && self
                .consensus
                .members()
                .member(request.target)
                .is_some_and(|target| target != self.consensus.primary_index(self.consensus.view()))
            && self.consensus.is_normal()
            && !self.consensus.has_ceded_primaryship()
            && !self.consensus.is_transferring()
            && !self.consensus.view_log_unproven()
            && self.fatal.is_none()
            && !self.materialization_missing
            && !self.io_active.is_retiring()
            && !self.is_shutting_down()
    }

    pub(crate) fn cancel_primary_transfer(&mut self) {
        if !matches!(
            self.transition,
            Some(PendingPartitionTransition::PrimaryTransfer { .. })
        ) {
            return;
        }
        if let Some(PendingPartitionTransition::PrimaryTransfer { drain, .. }) =
            self.transition.take()
            && let (Some(persistence), Some(drain)) = (&self.persistence, drain)
        {
            persistence.finish_drain(&drain);
        }
        self.notify_io();
    }

    /// Keep the newest frame per phase and sender until history writes drain.
    pub fn defer_view_transition(&mut self, message: server_common::MessageBag) {
        let Some((view, phase, sender)) = deferred_view_order(&message) else {
            return;
        };
        if sender >= self.consensus.replica_count() {
            return;
        }
        self.cancel_primary_transfer();
        if let Some(pending) = self.deferred_views.iter_mut().find(|pending| {
            deferred_view_order(pending).is_some_and(|(_, pending_phase, pending_sender)| {
                pending_phase == phase && pending_sender == sender
            })
        }) {
            if deferred_view_order(pending).is_some_and(|(pending_view, _, _)| view >= pending_view)
            {
                *pending = message;
            }
        } else {
            self.deferred_views.push(message);
        }
        if self.transition.is_none() {
            self.transition = Some(PendingPartitionTransition::View { drain: None });
        }
        self.notify_io();
    }

    /// Whether file jobs are submitted through the shard's bounded I/O lane.
    pub fn has_io_dispatcher(&self) -> bool {
        self.io_notifier.borrow().is_some()
    }

    pub fn defer_view_truncation(
        &mut self,
        ops: std::ops::RangeInclusive<u64>,
        actions: Vec<consensus::VsrAction>,
        peer: Option<u8>,
    ) {
        self.cancel_primary_transfer();
        self.transition = Some(PendingPartitionTransition::Truncate {
            ops,
            drain: None,
            actions,
            peer,
        });
        self.notify_io();
    }

    /// Run a view adoption's `actions` once the pending truncation lands, so the
    /// pipeline rebuild reads the journal after the cut.
    pub fn defer_view_adoption(&mut self, adoption: Vec<consensus::VsrAction>, from: u8) {
        if let Some(PendingPartitionTransition::Truncate { actions, peer, .. }) =
            &mut self.transition
        {
            actions.extend(adoption);
            *peer = Some(from);
        }
    }

    #[must_use]
    pub const fn view_truncation_pending(&self) -> bool {
        matches!(
            self.transition,
            Some(PendingPartitionTransition::Truncate { .. })
        )
    }

    #[must_use]
    pub fn retains_io_reservation(&self, identity: crate::PartitionIoIdentity) -> bool {
        identity.incarnation == self.incarnation
            && !self.io_active.is_retiring()
            && identity.continuation == crate::PartitionIoContinuation::NoAck
            && self
                .no_ack
                .front()
                .is_some_and(|head| Some(head.local_order) == identity.local_order)
    }

    /// One complete owner mutation per call. File execution is always separate.
    pub async fn resume_io(&mut self, config: &PartitionsConfig) -> crate::PartitionIoStep {
        match self.resume_io_inner(config).await {
            Ok(step) => step,
            Err(error) => {
                self.cancel_primary_transfer();
                self.fail_io_work(&error, false);
                if self.maintenance.shutdown.is_some() {
                    self.maintenance.shutdown = Some(PartitionShutdownPhase::Complete);
                }
                crate::PartitionIoStep::Pending
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn resume_io_inner(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<crate::PartitionIoStep, IggyError> {
        if let Some(PendingPartitionTransition::PrimaryTransfer { request, .. }) = &self.transition
            && !self.primary_transfer_is_current(request)
        {
            self.cancel_primary_transfer();
        }
        if self.transition.is_none() && !self.deferred_views.is_empty() {
            self.transition = Some(PendingPartitionTransition::View { drain: None });
        }
        if self.io_active.get().is_some() {
            if self.io_active.is_interrupted() {
                self.finish_shutdown_without_flush();
            }
            return Ok(crate::PartitionIoStep::Pending);
        }
        if self.io_active.is_retiring() {
            if !self.io_active.is_deleting()
                && matches!(
                    self.transition,
                    Some(PendingPartitionTransition::Quarantine { .. })
                )
            {
                if self.superblock_intent.get().is_some() {
                    return self.io_plan(crate::PartitionIoContinuation::Superblock);
                }
                return Ok(self.drive_quarantine_io().await);
            }
            return Ok(crate::PartitionIoStep::Pending);
        }
        if self.transition.is_some()
            && let Some(head) = self.no_ack.front()
            && matches!(
                head.phase,
                NoAckPhase::Validate | NoAckPhase::OffsetReady(_)
            )
        {
            // Admission stops while a transition is pending, so the head
            // predates the transition.
            self.fail_no_ack(IggyError::TransientNotAccepted, false);
        }
        if self.no_ack_ready() {
            match self.drive_no_ack() {
                NoAckStep::Progress => return Ok(crate::PartitionIoStep::Progress),
                NoAckStep::Ready => {
                    return match self.io_plan(crate::PartitionIoContinuation::NoAck) {
                        Ok(step) => Ok(step),
                        Err(error) => {
                            self.fail_no_ack(error, false);
                            Ok(crate::PartitionIoStep::Progress)
                        }
                    };
                }
                NoAckStep::Reply => {
                    self.deliver_no_ack_reply().await;
                    return Ok(crate::PartitionIoStep::Progress);
                }
                NoAckStep::Pending => {}
            }
        }
        if self.superblock_intent.get().is_some()
            && (!self.superblock_write_is_backed_off()
                || self
                    .superblock_intent
                    .get()
                    .is_some_and(SuperblockIntent::final_write))
        {
            return self.io_plan(crate::PartitionIoContinuation::Superblock);
        }
        if self.fatal.is_some()
            && !matches!(
                self.transition,
                Some(PendingPartitionTransition::Install(_))
            )
        {
            self.finish_shutdown_without_flush();
            return Ok(crate::PartitionIoStep::Pending);
        }
        let has_pending_wire_actions = self.pending_wire_actions.borrow().is_some();
        if has_pending_wire_actions && self.persist_superblock_if_needed().await {
            let pending = self.pending_wire_actions.borrow_mut().take();
            if let Some(pending) = pending
                && pending.view == self.consensus.view()
                && pending.log_view == self.consensus.log_view()
            {
                return Ok(crate::PartitionIoStep::WireActions(pending.actions));
            }
            return Ok(crate::PartitionIoStep::Progress);
        }
        if self.resume_pending_prepare_ack().await {
            return Ok(crate::PartitionIoStep::Progress);
        }
        if self.pending_commit.is_some() {
            return match self.drive_commit()? {
                CommitStep::Progress => Ok(crate::PartitionIoStep::Progress),
                CommitStep::Pending => Ok(crate::PartitionIoStep::Pending),
                CommitStep::Ready => self.io_plan(crate::PartitionIoContinuation::Commit),
                CommitStep::Publish => {
                    self.finish_pending_commit().await;
                    Ok(crate::PartitionIoStep::Progress)
                }
            };
        }
        if self.materialization.is_some() {
            return match self.drive_materialization()? {
                MaterializationStep::Progress | MaterializationStep::Complete => {
                    Ok(crate::PartitionIoStep::Progress)
                }
                MaterializationStep::Pending => Ok(crate::PartitionIoStep::Pending),
                MaterializationStep::Ready => {
                    self.io_plan(crate::PartitionIoContinuation::Materialization)
                }
            };
        }
        if let Some(through) = self.maintenance.flush_through {
            if let Some(drain) = self.maintenance.flush_drain.as_ref()
                && let Some(persistence) = self.persistence.as_ref()
                && !persistence
                    .observe_drain(drain)
                    .map_err(|_| IggyError::CannotSyncFile)?
            {
                return Ok(crate::PartitionIoStep::Pending);
            }
            let ready_through = self.flushable_through(through);
            let prefix = self.materializable_prefix_len(ready_through);
            let retained = self
                .log
                .journal()
                .inner
                .with_entries(|entries| entries.len().saturating_sub(prefix));
            // A partial flush waits until it frees at least as many entries as
            // it keeps, so a slow writer gets fewer and larger flushes.
            if (ready_through == through || prefix >= retained)
                && self.begin_materialization(
                    ready_through,
                    self.effective_size_of_messages_required_to_save(config),
                )?
                && (self.materialization.is_some() || ready_through == through)
            {
                if self.materialization.is_none() {
                    self.finish_flush();
                }
                return Ok(crate::PartitionIoStep::Progress);
            }
        }
        if matches!(
            self.transition,
            Some(PendingPartitionTransition::PrimaryTransfer { .. })
        ) && self.consensus.request_queue_len() > 0
        {
            self.deny_queued_requests("primary transfer queued request reply failed")
                .await;
            return Ok(crate::PartitionIoStep::Progress);
        }
        if matches!(
            self.transition,
            Some(PendingPartitionTransition::PrimaryTransfer { .. })
        ) && (self.consensus.commit_min() < self.consensus.commit_max()
            || !self.consensus.pipeline_is_empty())
        {
            // Commit selection refuses history transitions. This synchronous
            // window keeps admission fenced between pump events while draining
            // the already committed prefix before handing leadership away.
            let transition = self.transition.take();
            let mut headers = self.select_persistable_commits(config);
            let reply = !headers.is_empty();
            if headers.is_empty() {
                headers = self.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, config);
            }
            let selected = self.select_commit(headers, config, reply);
            self.transition = transition;
            return Ok(if selected {
                crate::PartitionIoStep::Progress
            } else {
                crate::PartitionIoStep::Pending
            });
        }
        if let Some(
            PendingPartitionTransition::View { drain }
            | PendingPartitionTransition::PrimaryTransfer { drain, .. },
        ) = &mut self.transition
        {
            if let Some(persistence) = &self.persistence {
                let drain = drain.get_or_insert_with(|| persistence.begin_drain());
                if !persistence
                    .observe_drain(drain)
                    .map_err(|_| IggyError::CannotSyncFile)?
                {
                    return Ok(crate::PartitionIoStep::Pending);
                }
                persistence.finish_drain(drain);
            }
            if let Some(PendingPartitionTransition::PrimaryTransfer { request, .. }) =
                self.transition.take()
            {
                return Ok(crate::PartitionIoStep::PrimaryTransfer(request));
            }
            let next = self
                .deferred_views
                .iter()
                .enumerate()
                .min_by_key(|(_, message)| deferred_view_order(message))
                .map(|(index, _)| index)
                .expect("a view transition owns a deferred message");
            let message = self.deferred_views.swap_remove(next);
            return Ok(crate::PartitionIoStep::Transition(message));
        }
        if let Some(PendingPartitionTransition::Truncate { ops, drain, .. }) = &mut self.transition
        {
            if let Some(persistence) = &self.persistence {
                if drain.is_none() {
                    persistence.truncate_from(*ops.start());
                    if persistence.start() {
                        self.consensus
                            .message_bus()
                            .spawn(Rc::clone(persistence).run());
                    }
                    *drain = Some(persistence.begin_drain());
                }
                let drain = drain.as_ref().expect("truncate drain exists");
                if !persistence
                    .observe_drain(drain)
                    .map_err(|_| IggyError::CannotSyncFile)?
                {
                    return Ok(crate::PartitionIoStep::Pending);
                }
                persistence.finish_drain(drain);
            }
            let Some(PendingPartitionTransition::Truncate {
                ops, actions, peer, ..
            }) = self.transition.take()
            else {
                unreachable!("truncate transition exists");
            };
            let from_op = *ops.start();
            // Another cut that lands first leaves the marked entries resident, so the
            // marker stays for the proof that finds them again.
            let unproven_drop = self
                .unproven_drop_from
                .is_some_and(|marked| from_op <= marked);
            if unproven_drop {
                self.unproven_drop_from = None;
            }
            let removed = self.remove_resident_range(ops).await?;
            if unproven_drop {
                self.note_unproven_dropped(from_op, removed);
            }
            return Ok(crate::PartitionIoStep::ViewApplied { actions, peer });
        }
        if matches!(
            self.transition,
            Some(PendingPartitionTransition::Install(_))
        ) {
            return Ok(self.drive_install_io().await);
        }
        if !self.is_shutting_down()
            && self.transfer.as_ref().is_some_and(|session| {
                session.target_accepted
                    && (session
                        .scan
                        .as_ref()
                        .is_some_and(crate::state_transfer::LocalWork::landed)
                        || session
                            .seed
                            .as_ref()
                            .is_some_and(crate::state_transfer::PrefixSeed::landed)
                        || session
                            .artifacts
                            .iter()
                            .all(consensus::ChunkProgress::complete))
            })
            && self.consensus.state_transfer_stage() == consensus::StateTransferStage::Fetching
        {
            return Ok(crate::PartitionIoStep::TransferReady);
        }
        if self.materialization_missing {
            if self.maintenance.shutdown == Some(PartitionShutdownPhase::DrainAccepted) {
                self.maintenance.shutdown = Some(PartitionShutdownPhase::DrainWriters);
            }
            if self.maintenance.shutdown.is_some() {
                return Ok(self.drive_shutdown_io());
            }
            return Ok(crate::PartitionIoStep::Pending);
        }
        if self.maintenance.shutdown.is_none() && self.drive_transfer_plan() {
            return Ok(crate::PartitionIoStep::Progress);
        }
        match self.drive_checkpoint_io(config) {
            Ok(Some(step)) => return Ok(step),
            Ok(None) => {}
            Err(error) => {
                self.fail_io_work(&error, true);
                return Ok(crate::PartitionIoStep::Pending);
            }
        }
        if let Some(through) = self.maintenance.retention
            && self.retention_is_ready(through)
        {
            return self.io_plan(crate::PartitionIoContinuation::Retention);
        }
        if let Some(through) = self.maintenance.eviction
            && self.eviction_is_ready(through)
        {
            return self.io_plan(crate::PartitionIoContinuation::Eviction);
        }
        let tiering = self.tiering_is_ready();
        if tiering && !self.tiering_yields() {
            return self.io_plan(crate::PartitionIoContinuation::Tiering);
        }
        if self.maintenance.shutdown.is_some() && self.consensus.request_queue_len() > 0 {
            self.deny_queued_requests("shutdown queued request reply failed")
                .await;
            return Ok(crate::PartitionIoStep::Progress);
        }
        if self.queued_requests_ready() {
            let queued = self.consensus.request_queue_len();
            self.drain_request_queue_into_prepares(consensus::PIPELINE_PREPARE_QUEUE_MAX)
                .await;
            if self.consensus.request_queue_len() < queued {
                return Ok(crate::PartitionIoStep::Progress);
            }
        }
        let mut headers = self.select_persistable_commits(config);
        let reply = !headers.is_empty() && self.consensus.is_primary();
        if headers.is_empty() {
            headers = self.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, config);
        }
        if self.select_commit(headers, config, reply) {
            return Ok(crate::PartitionIoStep::Progress);
        }
        if tiering {
            return self.io_plan(crate::PartitionIoContinuation::Tiering);
        }
        Ok(self.drive_shutdown_io())
    }

    pub(crate) fn plan_install_io(&mut self) -> crate::PartitionIoStep {
        match self.io_plan(crate::PartitionIoContinuation::Install) {
            Ok(step) => step,
            Err(error) => {
                self.fail_install_capture(error);
                crate::PartitionIoStep::Progress
            }
        }
    }

    fn io_plan(
        &self,
        continuation: crate::PartitionIoContinuation,
    ) -> Result<crate::PartitionIoStep, IggyError> {
        let materialization = continuation == crate::PartitionIoContinuation::Materialization
            || (continuation == crate::PartitionIoContinuation::Commit
                && self
                    .pending_commit
                    .as_ref()
                    .is_some_and(|pending| matches!(pending.phase, CommitPhase::Materialize)));
        let mut allocation_charge = if continuation == crate::PartitionIoContinuation::Install {
            self.install_io_charge()
                .ok_or(IggyError::CannotWriteToFile)?
        } else if materialization
            && let Some(MaterializationPhase::Save(chunk)) =
                self.materialization.as_ref().map(|pending| pending.phase)
        {
            chunk.allocation_charge
        } else {
            crate::io::file_phase_charge::<SB>(
                std::iter::once(self.partition_dir.as_deref())
                    .chain(ConsumerKind::ALL.map(|kind| self.consumer_offsets_dir(kind)))
                    .flatten(),
            )
            .ok_or(IggyError::CannotWriteToFile)?
        };
        if continuation == crate::PartitionIoContinuation::Commit
            && let Some(pending) = &self.pending_commit
            && matches!(pending.phase, CommitPhase::OffsetReady(_))
        {
            allocation_charge = allocation_charge
                .checked_mul(pending.offset_batch.len() + 1)
                .ok_or(IggyError::CannotWriteToFile)?;
        }
        if continuation == crate::PartitionIoContinuation::Tiering
            && let Some(pending) = &self.maintenance.tiering
        {
            allocation_charge = allocation_charge
                .checked_add(pending.read_len)
                .ok_or(IggyError::CannotWriteToFile)?;
        }
        if continuation == crate::PartitionIoContinuation::Checkpoint
            && let Some(PendingCheckpoint::Receipts(_, receipts)) = &self.maintenance.checkpoint
        {
            allocation_charge = allocation_charge
                .checked_add(receipts.capacity())
                .ok_or(IggyError::CannotWriteToFile)?;
        }
        if allocation_charge > self.io_bytes_max.get() {
            return Err(IggyError::CannotWriteToFile);
        }
        Ok(crate::PartitionIoStep::Ready(crate::PartitionIoPlan {
            continuation,
            allocation_charge,
        }))
    }

    /// Caller must reserve the plan's slot and byte charge before capture.
    ///
    /// # Errors
    /// Rejects exhausted identity space or a phase whose captured resources changed.
    pub fn capture_io(
        &mut self,
        plan: crate::PartitionIoPlan,
        config: &PartitionsConfig,
    ) -> Result<Option<crate::CapturedPartitionIo<SB>>, IggyError> {
        if self.io_active.get().is_some()
            || self.io_active.is_deleting()
            || (self.io_active.is_retiring()
                && !matches!(
                    self.transition,
                    Some(PendingPartitionTransition::Quarantine { .. })
                ))
        {
            return Ok(None);
        }
        let sequence = self
            .io_sequence
            .get()
            .checked_add(1)
            .ok_or(IggyError::CannotWriteToFile)?;
        let mut gate = None;
        let job = match plan.continuation {
            crate::PartitionIoContinuation::Commit => self.capture_commit_phase(config)?,
            crate::PartitionIoContinuation::NoAck => Some(self.capture_no_ack_phase()?),
            crate::PartitionIoContinuation::Materialization => {
                self.capture_materialization(config)?
            }
            crate::PartitionIoContinuation::Install => Some(self.capture_install_io(config)?),
            crate::PartitionIoContinuation::Quarantine => {
                let Some(PendingPartitionTransition::Quarantine { phase, .. }) =
                    self.transition.as_mut()
                else {
                    return Ok(None);
                };
                *phase = QuarantinePhase::Submitted;
                Some(crate::PartitionIoJob::Quarantine {
                    directory: self
                        .partition_dir
                        .clone()
                        .ok_or(IggyError::CannotDeleteFile)?,
                    revision: self.created_revision(),
                    replicated: self.consensus.replica_count() > 1,
                })
            }
            crate::PartitionIoContinuation::Retention => self.capture_segment_removal(),
            crate::PartitionIoContinuation::Eviction => self.capture_segment_eviction(),
            crate::PartitionIoContinuation::Tiering => self
                .next_tiering_job()
                .map(|job| crate::PartitionIoJob::Tiering(Box::new(job))),
            crate::PartitionIoContinuation::Checkpoint => self.capture_checkpoint_phase()?,
            crate::PartitionIoContinuation::Superblock => {
                let Some(guard) = self.superblock_lock.try_acquire_owned() else {
                    return Ok(None);
                };
                let Some(intent) = self.superblock_intent.take() else {
                    guard.release();
                    return Ok(None);
                };
                let Some(superblock) = self.superblock.as_ref() else {
                    guard.release();
                    return Ok(None);
                };
                let state = self.superblock_state(intent);
                self.superblock_running
                    .set(Some((intent, self.superblock_write_is_backed_off())));
                gate = Some(guard);
                Some(crate::PartitionIoJob::Superblock(
                    crate::io::SuperblockIoJob {
                        superblock: Rc::clone(superblock),
                        state,
                    },
                ))
            }
        };
        let Some(job) = job else {
            return Ok(None);
        };
        let identity = crate::PartitionIoIdentity {
            namespace: self.namespace(),
            incarnation: self.incarnation,
            history: self.poll_history,
            sequence,
            continuation: plan.continuation,
            local_order: (plan.continuation == crate::PartitionIoContinuation::NoAck)
                .then(|| self.no_ack.front().map(|head| head.local_order))
                .flatten(),
        };
        self.io_sequence.set(sequence);
        self.io_active.set(Some(identity));
        Ok(Some(crate::CapturedPartitionIo {
            identity,
            job,
            gate,
            quiescence: Rc::clone(&self.io_active),
        }))
    }

    /// # Errors
    /// Rejects obsolete identities and propagates the phase's storage failure.
    pub fn accept_io(
        &mut self,
        identity: crate::PartitionIoIdentity,
        result: crate::PartitionIoResult,
    ) -> Result<(), IggyError> {
        if self.io_active.get() != Some(identity) || identity.incarnation != self.incarnation {
            return Err(IggyError::CannotWriteToFile);
        }
        self.io_active.set(None);
        if self.io_active.is_retiring()
            && !matches!(
                identity.continuation,
                crate::PartitionIoContinuation::Superblock
                    | crate::PartitionIoContinuation::Quarantine
            )
        {
            drop(result);
            self.notify_io();
            return Ok(());
        }
        if identity.history != self.poll_history {
            self.fence_flush_failure();
            return Err(IggyError::CannotWriteToFile);
        }
        let outcome = match identity.continuation {
            crate::PartitionIoContinuation::Commit => self.accept_commit_phase(result),
            crate::PartitionIoContinuation::Materialization => {
                self.accept_materialization_phase(result)
            }
            crate::PartitionIoContinuation::NoAck => {
                self.accept_no_ack_phase(result);
                Ok(())
            }
            crate::PartitionIoContinuation::Install => {
                self.accept_install_io(result);
                Ok(())
            }
            crate::PartitionIoContinuation::Quarantine => {
                let Some(PendingPartitionTransition::Quarantine { phase, .. }) =
                    self.transition.as_mut()
                else {
                    return Err(IggyError::CannotDeleteFile);
                };
                let crate::PartitionIoResult::Quarantine(outcome) = result else {
                    return Err(IggyError::CannotDeleteFile);
                };
                *phase = QuarantinePhase::Done(outcome.map(Some));
                Ok(())
            }
            crate::PartitionIoContinuation::Retention => self
                .accept_segment_removal(&result)
                .map(|_| ())
                .ok_or(IggyError::CannotDeleteFile),
            crate::PartitionIoContinuation::Eviction => {
                self.accept_segment_eviction(result).map(|_| ())
            }
            crate::PartitionIoContinuation::Tiering => self.accept_tiering(result),
            crate::PartitionIoContinuation::Checkpoint => self.accept_checkpoint_io(result),
            crate::PartitionIoContinuation::Superblock => {
                let Some((intent, backoff)) = self.superblock_running.take() else {
                    return Err(IggyError::CannotWriteToFile);
                };
                let crate::PartitionIoResult::Superblock(result) = result else {
                    return Err(IggyError::CannotWriteToFile);
                };
                let succeeded = self.accept_superblock(result, backoff);
                self.superblock_completed.set(Some((intent, succeeded)));
                if !succeeded
                    && matches!(intent, SuperblockIntent::Advance { .. })
                    && !self.io_active.is_retiring()
                    && self.superblock_intent.get().is_none()
                {
                    self.superblock_intent.set(Some(intent));
                }
                Ok(())
            }
        };
        if let Err(error) = &outcome {
            self.fail_io_work(
                error,
                identity.continuation == crate::PartitionIoContinuation::Checkpoint,
            );
        }
        self.notify_io();
        outcome
    }

    pub fn reject_io_capture(
        &mut self,
        continuation: crate::PartitionIoContinuation,
        error: IggyError,
    ) {
        match continuation {
            crate::PartitionIoContinuation::NoAck => {
                let file_attempted = self.no_ack.front().is_some_and(|head| {
                    matches!(
                        head.phase,
                        NoAckPhase::Directory | NoAckPhase::DirectorySubmitted
                    )
                });
                self.fail_no_ack(error, file_attempted);
            }
            crate::PartitionIoContinuation::Install => self.fail_install_capture(error),
            crate::PartitionIoContinuation::Quarantine => {
                if let Some(PendingPartitionTransition::Quarantine { phase, .. }) =
                    &mut self.transition
                {
                    *phase = QuarantinePhase::Done(Err(std::io::Error::other(error)));
                }
            }
            crate::PartitionIoContinuation::Retention => self.maintenance.retention = None,
            crate::PartitionIoContinuation::Eviction => self.maintenance.eviction = None,
            crate::PartitionIoContinuation::Tiering => self.maintenance.tiering = None,
            _ => self.fail_io_work(
                &error,
                continuation == crate::PartitionIoContinuation::Checkpoint,
            ),
        }
        self.notify_io();
    }

    pub fn interrupt_io(&mut self, identity: crate::PartitionIoIdentity) {
        if self.io_active.get() == Some(identity) {
            self.fence_flush_failure();
            self.notify_io();
        }
    }

    pub fn begin_shutdown_io(&mut self) {
        if self.maintenance.shutdown.is_none() {
            self.maintenance.shutdown = Some(PartitionShutdownPhase::DrainAccepted);
        }
        self.maintenance.transfer_flush = false;
        self.notify_io();
    }

    /// Shutdown closes admission while previously accepted work drains.
    #[must_use]
    pub const fn is_shutting_down(&self) -> bool {
        self.maintenance.shutdown.is_some()
    }

    #[must_use]
    pub fn shutdown_io_complete(&self) -> bool {
        (self.io_active.get().is_none() || self.io_active.is_interrupted())
            && ((self.maintenance.shutdown == Some(PartitionShutdownPhase::Complete)
                && (self.fatal.is_some()
                    || self.materialization_missing
                    || self.io_active.is_retiring()
                    || self
                        .maintenance
                        .flush_completed_through
                        .is_some_and(|through| through >= self.shutdown_flush_target())))
                || (self.io_active.is_deleting()
                    && self
                        .persistence
                        .as_ref()
                        .is_none_or(|persistence| persistence.is_quiescent())))
    }

    fn fail_io_work(&mut self, error: &IggyError, checkpoint_failure: bool) {
        if self.pending_commit.is_none()
            && self.maintenance.transfer_flush
            && !checkpoint_failure
            && !matches!(
                self.maintenance.checkpoint,
                Some(PendingCheckpoint::Flush(_))
            )
        {
            self.materialization = None;
            self.finish_flush();
            self.maintenance.flush_completed_through = None;
            let mut plan = self.transfer_plan.borrow_mut();
            if matches!(*plan, Some(TransferPlanSlot::Wanted)) {
                *plan = Some(TransferPlanSlot::Failed(
                    PartitionTransferUnavailable::FlushFailed(error.clone()),
                ));
            }
        } else {
            self.fail_pending_commit(error);
            self.fence_flush_failure();
        }
    }

    fn defer_flush(&mut self, through: u64) {
        self.maintenance.flush_through = Some(
            self.maintenance
                .flush_through
                .map_or(through, |pending| pending.max(through)),
        );
    }

    /// A Replicated commit runs ahead of the WAL writer, so its flush takes the
    /// written prefix and leaves the rest for a later writer completion.
    fn flushable_through(&self, through: u64) -> u64 {
        self.persistence
            .as_ref()
            .filter(|persistence| {
                persistence.segment_checkpoint().is_some() && !self.durability().is_persisted()
            })
            .map_or(through, |persistence| through.min(persistence.written_op()))
    }

    fn request_flush(&mut self, through: u64) {
        self.defer_flush(through);
        if self.maintenance.flush_drain.is_none()
            && let Some(persistence) = self
                .persistence
                .as_ref()
                .filter(|persistence| persistence.segment_checkpoint().is_some())
        {
            self.start_persistence();
            self.maintenance.flush_drain = Some(persistence.begin_drain());
        }
        self.notify_io();
    }

    fn finish_flush(&mut self) {
        if let Some(through) = self.maintenance.flush_through.take() {
            self.maintenance.flush_completed_through = Some(through);
        }
        self.maintenance.transfer_flush = false;
        if let Some(drain) = self.maintenance.flush_drain.take()
            && let Some(persistence) = self.persistence.as_ref()
        {
            persistence.finish_drain(&drain);
        }
    }

    /// Whether the next offer round hashes against a pinned plan, instead of
    /// returning at once to wait for the pin.
    #[must_use]
    pub fn transfer_plan_pinned(&self) -> bool {
        matches!(
            *self.transfer_plan.borrow(),
            Some(TransferPlanSlot::Pinned(_))
        )
    }

    /// The plan an offer round hashes against, or `None` while the flush that
    /// has to come before it runs. A mounted owner pins the plan in the I/O
    /// turn that finds every applied op in its segment, so commits landing
    /// between rounds cannot starve it. One without an owner flushes and
    /// captures inline.
    pub(crate) async fn request_transfer_plan(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Option<Rc<crate::state_transfer::TransferPlan>>, PartitionTransferUnavailable> {
        if !self.has_io_dispatcher() {
            if !self
                .flush_committed_messages(config)
                .await
                .map_err(PartitionTransferUnavailable::FlushFailed)?
            {
                return Ok(None);
            }
            return self.capture_transfer_plan().map(|plan| Some(Rc::new(plan)));
        }
        if self.fatal.is_some() {
            return Err(PartitionTransferUnavailable::FlushFailed(
                IggyError::CannotSyncFile,
            ));
        }
        // An install or a quarantine rewrites the files a plan names.
        if self.transition.is_some() || self.io_active.is_retiring() {
            return Ok(None);
        }
        let slot = self.transfer_plan.borrow_mut().take();
        match slot {
            Some(TransferPlanSlot::Pinned(plan)) => {
                *self.transfer_plan.borrow_mut() = Some(TransferPlanSlot::Pinned(Rc::clone(&plan)));
                Ok(Some(plan))
            }
            Some(TransferPlanSlot::Failed(error)) => Err(error),
            Some(TransferPlanSlot::Wanted) | None => {
                *self.transfer_plan.borrow_mut() = Some(TransferPlanSlot::Wanted);
                self.notify_io();
                Ok(None)
            }
        }
    }

    /// Pins the plan a transfer request waits for in the turn that finds every
    /// applied op in its segment, flushing the applied prefix first while any
    /// of it is still resident.
    fn drive_transfer_plan(&mut self) -> bool {
        if !matches!(*self.transfer_plan.borrow(), Some(TransferPlanSlot::Wanted))
            || self.history_writes_pending()
        {
            return false;
        }
        let commit_min = self.consensus.commit_min();
        if self.materializable_prefix_len(commit_min) > 0 {
            self.maintenance.transfer_flush = true;
            self.defer_flush(commit_min);
            self.notify_io();
            return true;
        }
        // A flush through a committed op the walk has not applied yet leaves
        // its bytes in the segments, past the op the plan would name.
        if self.consensus.commit_max() > commit_min
            && !commit_min
                .checked_add(1)
                .is_some_and(|next| self.log.journal().inner.holds_op(next))
        {
            return false;
        }
        let slot = match self.capture_transfer_plan() {
            Ok(plan) => TransferPlanSlot::Pinned(Rc::new(plan)),
            Err(error) => TransferPlanSlot::Failed(error),
        };
        *self.transfer_plan.borrow_mut() = Some(slot);
        true
    }

    fn drive_shutdown_io(&mut self) -> crate::PartitionIoStep {
        if !self.materialization_missing
            && matches!(
                self.maintenance.shutdown,
                Some(
                    PartitionShutdownPhase::DrainWriters
                        | PartitionShutdownPhase::Collapse
                        | PartitionShutdownPhase::Complete
                )
            )
            && self
                .maintenance
                .flush_completed_through
                .is_none_or(|through| through < self.shutdown_flush_target())
        {
            self.maintenance.shutdown_drain = None;
            self.maintenance.shutdown = Some(PartitionShutdownPhase::DrainAccepted);
        }
        match self.maintenance.shutdown {
            Some(PartitionShutdownPhase::DrainAccepted) => {
                if !self.no_ack.is_empty() || self.consensus.request_queue_len() > 0 {
                    return crate::PartitionIoStep::Pending;
                }
                // A stopping backup may never ack the uncommitted pipeline tail.
                self.request_flush(self.shutdown_flush_target());
                self.maintenance.shutdown = Some(PartitionShutdownPhase::Flush);
                crate::PartitionIoStep::Progress
            }
            Some(PartitionShutdownPhase::Flush)
                if self.maintenance.flush_through.is_none() && self.materialization.is_none() =>
            {
                self.maintenance.shutdown = Some(PartitionShutdownPhase::DrainWriters);
                crate::PartitionIoStep::Progress
            }
            Some(PartitionShutdownPhase::DrainWriters) => {
                match self.drain_wal_for_shutdown() {
                    None => return crate::PartitionIoStep::Pending,
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        warn!(%error, "partition shutdown could not drain its WAL");
                        self.fence_flush_failure();
                        self.maintenance.shutdown = Some(PartitionShutdownPhase::Complete);
                        return crate::PartitionIoStep::Progress;
                    }
                }
                self.maintenance.shutdown = Some(if self.materialization_missing {
                    PartitionShutdownPhase::Complete
                } else {
                    PartitionShutdownPhase::Collapse
                });
                crate::PartitionIoStep::Progress
            }
            Some(PartitionShutdownPhase::Collapse) => {
                let verdict = self
                    .collapse_superblock_intent()
                    .map_or(crate::PartitionIoVerdict::Ready, |intent| {
                        self.request_superblock_intent(intent)
                    });
                if verdict == crate::PartitionIoVerdict::Pending {
                    return crate::PartitionIoStep::Pending;
                }
                if verdict == crate::PartitionIoVerdict::Failed {
                    warn!(
                        namespace_raw = self.namespace().inner(),
                        "could not collapse the offset reservation on shutdown; the restart will retain its gap"
                    );
                }
                self.maintenance.shutdown = Some(PartitionShutdownPhase::Complete);
                crate::PartitionIoStep::Progress
            }
            _ => crate::PartitionIoStep::Pending,
        }
    }

    fn finish_shutdown_without_flush(&mut self) {
        if !self
            .maintenance
            .shutdown
            .is_some_and(|phase| phase != PartitionShutdownPhase::Complete)
        {
            return;
        }
        // Interrupted file jobs keep their resources, but accepted WAL writes
        // still need to reach the recovery frontier before shutdown completes.
        match self.drain_wal_for_shutdown() {
            None => return,
            Some(Err(error)) => {
                warn!(%error, "fatal partition could not drain its WAL on shutdown");
            }
            Some(Ok(())) => {}
        }
        self.maintenance.shutdown = Some(PartitionShutdownPhase::Complete);
    }

    /// Publishes what the WAL accepted, because recovery keeps only the segment
    /// bytes that the durable WAL frontier covers. `None` while the drain runs.
    fn drain_wal_for_shutdown(&mut self) -> Option<std::io::Result<()>> {
        let Some(persistence) = &self.persistence else {
            return Some(Ok(()));
        };
        if self.maintenance.shutdown_drain.is_none() && persistence.segment_checkpoint().is_some() {
            persistence.sync();
        }
        self.start_persistence();
        let drain = self
            .maintenance
            .shutdown_drain
            .get_or_insert_with(|| persistence.begin_drain());
        match persistence.observe_drain(drain) {
            Ok(false) => None,
            Ok(true) => Some(Ok(())),
            Err(error) => Some(Err(error)),
        }
    }

    // Completion relies on shutdown rejecting new and repaired journal entries.
    fn shutdown_flush_target(&self) -> u64 {
        let committed = self
            .consensus
            .commit_max()
            .min(self.log.journal().inner.last_op().unwrap_or(0));
        self.persistence
            .as_ref()
            .map_or(committed, |persistence| committed.min(persistence.head()))
    }

    async fn resume_pending_prepare_ack(&self) -> bool {
        if self.consensus.needs_superblock_persist() {
            return false;
        }
        let header = self
            .pending_persisted_acks
            .borrow()
            .first_key_value()
            .map(|(_, header)| *header);
        let Some(header) = header else {
            return false;
        };
        if self
            .log
            .journal()
            .inner
            .repair_header(header.op)
            .is_none_or(|stored| stored.checksum != header.checksum)
        {
            self.pending_persisted_acks.borrow_mut().remove(&header.op);
            return true;
        }
        if !self.send_prepare_ok(&header).await {
            return false;
        }
        self.pending_persisted_acks.borrow_mut().remove(&header.op);
        true
    }

    async fn finish_pending_commit(&mut self) {
        if let Some(committed) = self.publish_pending_commit() {
            let committed_count = committed.len();
            self.deliver_commit_replies(committed).await;
            self.drain_no_ack().await;
            self.drain_request_queue_into_prepares(committed_count)
                .await;
        }
    }

    pub(crate) fn invalidate_workerless_offset_files(&self) {
        self.workerless_offset_files.clear();
    }

    /// Checksum of the prepare accepted into this replica's WAL at `op`, readable after
    /// the journal has evicted the header and until a checkpoint reclaims it.
    #[must_use]
    pub fn wal_checksum(&self, op: u64) -> Option<u128> {
        self.persistence
            .as_ref()
            .and_then(|persistence| persistence.checksum(op))
    }

    /// The commit fault that fenced this partition, if one has.
    #[must_use]
    pub const fn fatal(&self) -> Option<&FatalCommit> {
        self.fatal.as_ref()
    }

    /// Consecutive superblock write failures for this group, for the shard's
    /// wedge fail-stop. A partition that cannot record its state withholds every
    /// view-scoped send and refuses every append, so past some window it is
    /// serving nothing and a supervisor should be handling it instead.
    #[must_use]
    pub const fn superblock_write_failures(&self) -> u64 {
        self.superblock_write_failures.get()
    }

    /// Fence this partition after the shutdown flush failed to persist its
    /// committed journal prefix: that data is cluster-committed and now lives
    /// only in this process's memory, so the shard must not report a clean
    /// exit over it. A fault the commit path already recorded is kept, since
    /// it names the op that first diverged; `commit_min` here only bounds
    /// where the unpersisted prefix ends.
    pub fn fence_flush_failure(&mut self) {
        if self.fatal.is_none() {
            self.fatal = Some(FatalCommit {
                namespace_raw: self.namespace().inner(),
                op: self.consensus.commit_min(),
                operation: Some(Operation::SendMessages),
            });
        }
    }

    pub(crate) const fn fence_install_failure(&mut self, op: u64) {
        if self.fatal.is_none() {
            self.fatal = Some(FatalCommit {
                namespace_raw: self.consensus.group(),
                op,
                operation: Some(Operation::SendMessages),
            });
        }
    }

    fn partition_dir(&self) -> Option<String> {
        if self.partition_dir.is_some() {
            return self.partition_dir.clone();
        }
        // Writer-derived fallback for partitions built without
        // `set_partition_dir`. Unreliable mid-rotation: sealed segments
        // drop their writer, so prefer the stored path above.
        self.log
            .messages_writers()
            .iter()
            .rev()
            .flatten()
            .next()
            .and_then(|writer| {
                std::path::Path::new(&writer.path())
                    .parent()
                    .map(|dir| dir.to_string_lossy().into_owned())
            })
    }

    /// [`Self::partition_dir`] upgraded with the reason a dir is absent, so a
    /// disk poll can tell file-less (simulated) storage from a live partition
    /// whose dir is transiently unresolvable mid-rotation. Storage readers,
    /// unlike writers, survive segment sealing, so any present reader or
    /// writer proves file-backed data exists behind the missing dir.
    fn partition_dir_resolution(&self) -> PartitionDirResolution {
        if let Some(dir) = self.partition_dir() {
            return PartitionDirResolution::Resolved(dir);
        }
        let file_backed =
            self.log.storages().iter().any(|storage| {
                storage.messages_reader.is_some() || storage.messages_size.is_some()
            });
        if file_backed {
            PartitionDirResolution::Unresolvable
        } else {
            PartitionDirResolution::NoFiles
        }
    }

    fn has_persisted_segment_bytes(&self) -> bool {
        self.log
            .segments()
            .iter()
            .any(|segment| segment.size.as_bytes_u64() > 0)
    }

    /// Mean encoded bytes per committed message, including its share of the
    /// batch headers, or `None` while the partition has committed nothing.
    ///
    /// Both counters are relaxed loads that retention also decrements, so this
    /// is a hint and nothing reads it as a bound. Its one consumer sizes the
    /// first read of a disk poll, where being wrong costs an extra read.
    fn mean_encoded_message_size(&self) -> Option<u32> {
        let messages = self.stats.messages_count_inconsistent();
        let bytes = self.stats.size_bytes_inconsistent();
        (messages > 0).then(|| u32::try_from(bytes / messages).unwrap_or(u32::MAX))
    }

    /// Widest observed committed batch, for the disk walk's chunk floor.
    /// Recovered history starts unknown; each walk learns its own batch floor
    /// when an incomplete batch requires an exact reread.
    ///
    /// A high-water, never lowered: retention cannot make an older batch
    /// narrower, and the read path clamps it to the chunk ceiling anyway, so
    /// the worst a stale value costs is the fixed-size read polls did before
    /// they were sized at all.
    const fn widest_committed_batch(&self) -> u64 {
        self.widest_batch_bytes.get()
    }

    /// Starting `(segment index, byte position)` for a disk poll, resolved
    /// via each segment's sparse index cache. An index miss starts at the
    /// segment's first byte (the walk filters precisely).
    fn disk_poll_start(&self, query: &MessageLookup) -> (usize, u64, Option<u64>) {
        let segments = self.log.segments();
        match query {
            MessageLookup::Offset { offset, .. } => {
                let segment_index = segment_holding(segments, *offset);
                let entry = self
                    .log
                    .segment_indexes(segment_index)
                    .and_then(|cache| cache.offset_lower_bound(*offset));
                let position = entry.map_or(0, |index| index.position);
                let entry_offset = entry.map(|index| index.offset).or_else(|| {
                    segments
                        .get(segment_index)
                        .map(|segment| segment.start_offset)
                });
                (segment_index, position, entry_offset)
            }
            MessageLookup::Timestamp { timestamp, .. } => {
                // Resolve the starting SEGMENT from segment metadata, not from
                // the per-segment index caches: sealed segments drop their
                // cache at rotation, and a cache miss must not read as "the
                // timestamp is not in this segment" (skipping a sealed segment
                // loses its messages). Timestamps are monotone across
                // segments, so the first segment whose max timestamp reaches
                // the query is the correct start; the walk filters precisely,
                // so an early start is safe.
                let segment_index = segments
                    .iter()
                    .position(|segment| segment.max_timestamp >= *timestamp)
                    .unwrap_or_else(|| segments.len().saturating_sub(1));
                let position = self
                    .log
                    .segment_indexes(segment_index)
                    .and_then(|cache| cache.timestamp_lower_bound(*timestamp))
                    .map_or(0, |index| index.position);
                (segment_index, position, None)
            }
        }
    }

    /// Project a client request into a prepare.
    ///
    /// A replay of a committed `(client, request)` is absorbed by this group's
    /// dedup slice; anything above the watermark projects into a prepare.
    /// Session lifecycle + eviction live on the metadata plane.
    ///
    /// `reply` is the in-process channel a `PartitionSubmit` carried in. When
    /// present the committed reply fires on it instead of going to the bus:
    /// the connection-owning shard writes it to the socket it holds, because
    /// `header.client` is the VSR consensus id and carries no home-shard
    /// routing. `None` keeps the bus path (auto-commit ops, tests).
    ///
    /// # Panics
    /// Panics if called when this partition's consensus instance is not the
    /// primary, is not in normal status, or is currently syncing.
    #[allow(clippy::future_not_send)]
    pub async fn on_request(
        &mut self,
        message: Message<RoutedRequestHeader>,
        reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        self.admit_request(message, reply).await;
        self.release_send_space();
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn admit_request(
        &mut self,
        message: Message<RoutedRequestHeader>,
        reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        // Taken by whichever arm answers: the deny paths, the NoAck fast path,
        // or the pipeline entry that fires it at commit. Exactly one runs.
        let mut reply = reply;
        self.resynchronize_consumer_offset_reservations();
        let namespace = IggyNamespace::from_raw(message.header().group);
        let client_id = message.header().client;
        let request = message.header().request;

        let disposition = {
            let consensus = self.consensus();
            emit_sim_event(
                SimEventKind::ClientRequestReceived,
                &RequestLogEvent {
                    replica: ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                    client_id,
                    request_id: request,
                    operation: message.header().operation,
                },
            );

            let message = if message.header().operation == Operation::SendMessages {
                // Skip the batch-checksum pass: on the partition ingest path
                // nothing reads it before `stamp_prepare_for_persistence`
                // recomputes it over the stamped header. An already-canonical
                // batch (the plane's pre-encrypt convert output) returns early
                // inside the convert, so Skip only affects the wire-form
                // admission, whose output goes straight to project/stamp.
                match convert_request_message(namespace, message, ChecksumMode::Skip) {
                    Ok(message) => message,
                    Err(error) => {
                        emit_partition_diag(
                            tracing::Level::WARN,
                            &PartitionDiagEvent::new(
                                ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                                "failed to convert send_messages request",
                            )
                            .with_operation(Operation::SendMessages)
                            .with_error(error.to_string()),
                        );
                        return;
                    }
                }
            } else {
                message
            };

            // Recovered commits need receipt restoration; live pipeline entries
            // already carry the pending identity and immutable capacity.
            let committed_history_pending = consensus.commit_min() < consensus.commit_max()
                && consensus.with_pipeline(|pipeline| {
                    pipeline.entry_by_op(consensus.commit_min() + 1).is_none()
                        || pipeline.entry_by_op(consensus.commit_max()).is_none()
                });
            // State-dependent admission belongs to the primary. A backup can
            // lag the committed offset table or message frontier and would
            // otherwise turn a routing artifact into a terminal 404 or 400.
            // Reject it first with a response that proves the request was
            // never admitted, so the caller may safely retry elsewhere. A
            // normal backup names the cause, so the caller goes to the
            // primary at once instead of backing off.
            if consensus.is_normal_backup() {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::NotPrimary.as_code(),
                    "not-primary reply send failed",
                    reply.take(),
                )
                .await;
                return;
            }
            if self.fatal.is_some()
                || self.maintenance.shutdown.is_some()
                || self.io_active.is_retiring()
                || self.transition.is_some()
                || self.materialization_missing
                || consensus.is_follower()
                || !consensus.is_normal()
                || consensus.is_transferring()
                || committed_history_pending
            {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        if committed_history_pending {
                            "rejecting client request while committed history is pending"
                        } else {
                            "rejecting client request on unavailable partition replica"
                        },
                    )
                    .with_operation(message.header().operation),
                );
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "non-primary transient reply send failed",
                    reply.take(),
                )
                .await;
                return;
            }

            let frame_bytes = Self::prepared_request_size(&message);
            // A caller context from a node ahead of the dispatching one can name
            // an incarnation this replica has not materialized yet, so only a
            // dispatch that applied the caller's metadata may refuse its history.
            if message.header().minimum_metadata_op > message.header().metadata_watermark {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "request requires newer metadata",
                    reply.take(),
                )
                .await;
                return;
            }
            if !self.request_history_available(message.header()) {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::HistoryUnavailable.as_code(),
                    "request history is no longer available",
                    reply.take(),
                )
                .await;
                return;
            }
            if message.header().operation == Operation::RetireConsumerGroupOwners {
                let Some(catalog) = RetireConsumerGroupOwnersRequest::decode_from(message.body())
                    .ok()
                    .filter(|catalog| {
                        catalog.incarnation == self.created_revision()
                            && catalog.metadata_op <= message.header().metadata_watermark
                            && !self.history_deleted()
                    })
                else {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::InvalidCommand.as_code(),
                        "invalid owner retirement catalog",
                        reply.take(),
                    )
                    .await;
                    return;
                };
                // An install admitted ahead of the catalog commits first and can
                // assign a retired row again, which the catalog apply refuses.
                if self.retirement_waits(|group_id| catalog.retires(group_id))
                    || self
                        .pending_owner_install
                        .get()
                        .is_some_and(|pending| catalog.retires(pending.group_id))
                    || consensus.commit_min() < self.owner_install_barrier
                {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::TransientNotAccepted.as_code(),
                        "owner retirement awaits offset cleanup or an owner install",
                        reply.take(),
                    )
                    .await;
                    return;
                }
            }
            if message.header().operation == Operation::TransitionPartitionHistory {
                let transition =
                    match TransitionPartitionHistoryRequest::decode_from(message.body()) {
                        Ok(transition)
                            if transition.incarnation == self.created_revision()
                                && !self.history_deleted() =>
                        {
                            transition
                        }
                        _ => {
                            Self::send_partition_deny_or_log(
                                consensus,
                                message.header(),
                                IggyError::HistoryUnavailable.as_code(),
                                "invalid history transition",
                                reply.take(),
                            )
                            .await;
                            return;
                        }
                    };
                if self
                    .pending_history_transition
                    .get()
                    .is_some_and(|pending| pending != transition)
                {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::TransientNotAccepted.as_code(),
                        "conflicting history transition",
                        reply.take(),
                    )
                    .await;
                    return;
                }
                self.pending_history_transition.set(Some(transition));
                if !consensus.pipeline_is_empty()
                    || consensus.request_queue_len() != 0
                    || consensus.commit_min() != consensus.sequencer().current_sequence()
                {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::TransientNotAccepted.as_code(),
                        "history transition awaits admitted operations",
                        reply.take(),
                    )
                    .await;
                    return;
                }
                self.pending_owner_install.set(None);
            } else if self.pending_history_transition.get().is_some() {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "partition is draining for history transition",
                    reply.take(),
                )
                .await;
                return;
            }
            // Latched only once the request is queued or pipelined, so a refusal
            // below never leaves an install the partition will not apply.
            let owner_install =
                if message.header().operation == Operation::InstallConsumerGroupOwner {
                    let installation =
                        match InstallConsumerGroupOwnerRequest::decode_from(message.body()) {
                            Ok(installation)
                                if installation.incarnation == self.created_revision()
                                    && !self.history_deleted() =>
                            {
                                installation
                            }
                            _ => {
                                Self::send_partition_deny_or_log(
                                    consensus,
                                    message.header(),
                                    IggyError::InvalidCommand.as_code(),
                                    "invalid ownership installation",
                                    reply.take(),
                                )
                                .await;
                                return;
                            }
                        };
                    let retired = self
                        .owner_retirement
                        .as_ref()
                        .is_some_and(|catalog| catalog.retires(installation.group_id));
                    let reserved_rows = consensus
                        .sequencer()
                        .current_sequence()
                        .saturating_sub(consensus.commit_min())
                        .saturating_add(consensus.request_queue_len() as u64);
                    if installation.group_id > u64::from(u32::MAX) || retired {
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            IggyError::HistoryUnavailable.as_code(),
                            "consumer group is retired or unavailable",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                    if !self
                        .consumer_group_owners
                        .contains_key(&installation.group_id)
                        && (self.consumer_group_owners.len() as u64).saturating_add(reserved_rows)
                            >= u64::from(crate::state_transfer::CONSUMER_OFFSETS_ENTRIES_MAX)
                    {
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            IggyError::TransientNotAccepted.as_code(),
                            "consumer group owner capacity is reserved",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                    if let Some(pending) = self.pending_owner_install.get() {
                        // A second copy of the pending install would only add a
                        // prepare, and a different one must wait until the pending
                        // one publishes.
                        let reason = if pending == installation {
                            "ownership installation is already pending"
                        } else {
                            "superseded ownership installation"
                        };
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            IggyError::TransientNotAccepted.as_code(),
                            reason,
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                    if self.conflicts_with_installed_owner(&installation) {
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            IggyError::InvalidCommand.as_code(),
                            "conflicting ownership installation",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                    if self.consumer_group_owner_admitted(&installation) {
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            IggyError::TransientNotAccepted.as_code(),
                            "ownership installation is already applied or superseded",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                    Some(installation)
                } else {
                    None
                };
            if message.header().operation == Operation::StateMachineCommand
                && let Some((error, reason)) =
                    self.command_refusal(message.header(), message.body())
            {
                // The others are transient. This one is a caller's bug, and a
                // server-originated command has no client to report it to.
                if matches!(error, IggyError::InvalidCommand) {
                    warn!(
                        namespace_raw = message.header().group,
                        client = message.header().client,
                        reason,
                        "refusing a state-machine command"
                    );
                }
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    error.as_code(),
                    reason,
                    reply.take(),
                )
                .await;
                return;
            }
            // Checked before dedup: a finalized session's cached receipt must
            // not be served either until the debt is paid.
            if !is_auto_commit_client(client_id) && self.owes_session_retirement() {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "partition owes session retirement",
                    reply.take(),
                )
                .await;
                return;
            }
            if !is_auto_commit_client(client_id)
                && message.header().metadata_watermark < self.required_metadata_frontier
            {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "partition requires newer metadata",
                    reply.take(),
                )
                .await;
                return;
            }
            if self.persistence.is_some()
                && frame_bytes > journal::partition_journal::PREPARE_BYTES_MAX
            {
                let error = IggyError::InvalidMessagesSize(
                    u32::try_from(frame_bytes).unwrap_or(u32::MAX),
                    u32::try_from(journal::partition_journal::PREPARE_BYTES_MAX)
                        .expect("prepare limit fits wire size"),
                );
                warn!(%error, namespace_raw = self.namespace().inner(), "persisted topic rejected oversized prepare");
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    error.as_code(),
                    "oversized prepare rejection failed",
                    reply.take(),
                )
                .await;
                return;
            }

            // Retirement and the retiring identity must never occupy the same
            // pipeline. Otherwise a later commit could recreate its receipt.
            let retirement_conflict = if message.header().operation == Operation::RetireSession {
                let Ok(retirement) = RetireSessionsRequest::decode_from(message.body()) else {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::InvalidCommand.as_code(),
                        "invalid retirement reply failed",
                        reply.take(),
                    )
                    .await;
                    return;
                };
                retirement.identities.iter().any(|identity| {
                    consensus.pending_request(identity.client_id).is_some()
                        || self.pending_session_retirement(identity.client_id, identity.session)
                })
            } else {
                !is_auto_commit_client(client_id)
                    && self.pending_session_retirement(client_id, message.header().session)
            };
            if retirement_conflict {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "session retirement admission reply failed",
                    reply.take(),
                )
                .await;
                return;
            }

            // Parse once for both the delete-existence check and AckLevel dispatch.
            let consumer_offset = match message.header().operation {
                Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                    match Self::parse_consumer_offset_request(message.header().operation, &message)
                    {
                        Ok(parsed) => Some(parsed),
                        Err(error) => {
                            emit_partition_diag(
                                tracing::Level::WARN,
                                &PartitionDiagEvent::new(
                                    ReplicaLogContext::from_consensus(
                                        consensus,
                                        PlaneKind::Partitions,
                                    ),
                                    "failed to parse consumer offset request",
                                )
                                .with_operation(message.header().operation)
                                .with_error(error.to_string()),
                            );
                            return;
                        }
                    }
                }
                _ => None,
            };

            // Dedup BEFORE the admission checks below: a replay of an
            // already-committed delete must answer the success its original
            // earned, not the typed 404 the existence check would raise now
            // that the offset is gone.
            //
            // One unresolved mutation per session/group protects the original
            // receipt until the caller resolves it. An exact pending retry is
            // still uncertain; a different request waits for that original
            // instead of displacing its receipt.
            //
            // HTTP handlers and pipelining SDKs can deliver a client's ids out of order:
            // a write refused transiently here is replayed after its
            // successors committed. The slice therefore keeps a committed-id
            // window under the watermark (`consensus::COMMITTED_WINDOW_BITS`)
            // and admits an unmarked id inside it instead of absorbing it.
            if !is_auto_commit_client(client_id) {
                if let Some((pending_session, pending_request, pending_operation)) =
                    consensus.pending_request(client_id)
                {
                    let error = if pending_request != request {
                        IggyError::TransientNotAccepted
                    } else if pending_session != message.header().session
                        || pending_operation != message.header().operation
                    {
                        IggyError::InvalidCommand
                    } else {
                        IggyError::TransientNotCommitted
                    };
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        error.as_code(),
                        "in-flight dedup transient reply send failed",
                        reply.take(),
                    )
                    .await;
                    return;
                }
                match self.dedup.check_partition_request(
                    client_id,
                    message.header().user_id,
                    message.header().session,
                    request,
                    message.header().operation,
                ) {
                    consensus::client_table::RequestStatus::New => {}
                    consensus::client_table::RequestStatus::Duplicate(cached) => {
                        if cached.header().partition_incarnation != self.created_revision() {
                            Self::send_partition_deny_or_log(
                                consensus,
                                message.header(),
                                IggyError::HistoryUnavailable.as_code(),
                                "retained receipt belongs to destroyed history",
                                reply.take(),
                            )
                            .await;
                            return;
                        }
                        Self::deliver_reply_or_log(
                            &self.consensus,
                            message.header(),
                            cached,
                            reply.take(),
                            "duplicate reply send failed",
                        )
                        .await;
                        return;
                    }
                    status => {
                        let error = match status {
                            consensus::client_table::RequestStatus::AlreadyApplied { .. } => {
                                emit_partition_diag(
                                    tracing::Level::DEBUG,
                                    &PartitionDiagEvent::new(
                                        ReplicaLogContext::from_consensus(
                                            consensus,
                                            PlaneKind::Partitions,
                                        ),
                                        "rejecting client request aged out of the dedup window",
                                    )
                                    .with_operation(message.header().operation)
                                    .with_client(client_id)
                                    .with_request(request),
                                );
                                IggyError::RequestTooOld
                            }
                            consensus::client_table::RequestStatus::OperationMismatch {
                                ..
                            } => IggyError::InvalidCommand,
                            _ => IggyError::Unauthenticated,
                        };
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            error.as_code(),
                            "dedup rejection reply failed",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                }
                if !consensus.has_retry_capacity(&self.dedup, client_id) {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::TransientNotAccepted.as_code(),
                        "retry protection capacity reached",
                        reply.take(),
                    )
                    .await;
                    return;
                }
            }

            // Only messages grow the disk without bound. Offsets, segment
            // deletion and every other operation stay admitted, so a consumer
            // and retention can still make room. Below the dedup lookup: a
            // retry is owed the outcome of its original, which this refusal
            // would replace with a claim that nothing was admitted.
            if message.header().operation == Operation::SendMessages
                && !self.reserve_send_space(&message)
            {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "data disk is below its free-space reserve",
                    reply.take(),
                )
                .await;
                return;
            }

            // A group's offsets wait for its installation, or they would land
            // behind the install under the old owner token. The latch names only
            // the last install, so after a view change every group waits for the
            // barrier.
            if let Some((ConsumerKind::ConsumerGroup, consumer_id, _, _)) = consumer_offset
                && (self
                    .pending_owner_install
                    .get()
                    .is_some_and(|pending| pending.group_id == u64::from(consumer_id))
                    || consensus.commit_min() < self.owner_install_barrier)
            {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "consumer group is draining for ownership installation",
                    reply.take(),
                )
                .await;
                return;
            }
            // A group offset write without a position carries owner 0. It
            // takes the installed generation before the owner match, so
            // promotion, the prepare and apply all see the bound owner.
            let message = match consumer_offset {
                Some((ConsumerKind::ConsumerGroup, consumer_id, _, _))
                    if message.header().owner_generation == 0
                        && !is_auto_commit_client(client_id) =>
                {
                    match self.captured_offset_owner(u64::from(consumer_id), message.header()) {
                        Some(generation) => message.transmute_header(
                            |mut header, captured: &mut RoutedRequestHeader| {
                                header.owner_generation = generation;
                                *captured = header;
                            },
                        ),
                        None => message,
                    }
                }
                _ => message,
            };
            if let Some((kind, consumer_id, _, _)) = consumer_offset
                && !self.matches_consumer_owner(
                    kind,
                    consumer_id,
                    message.header().client,
                    message.header().session,
                    message.header().owner_generation,
                    message.header().operation,
                )
            {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::ConsumerGroupPartitionNotOwned(
                        message.header().client,
                        u32::try_from(namespace.partition_id()).unwrap_or(u32::MAX),
                    )
                    .as_code(),
                    "consumer offset owner is not installed",
                    reply.take(),
                )
                .await;
                return;
            }
            if self
                .persistence
                .as_ref()
                .is_some_and(|persistence| !persistence.has_capacity(frame_bytes))
            {
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    IggyError::TransientNotAccepted.as_code(),
                    "partition WAL backpressure reply failed",
                    reply.take(),
                )
                .await;
                return;
            }

            let offset_blocked = consumer_offset.is_some_and(|(kind, id, offset, _)| {
                self.consumer_offset_is_blocked(kind, id, offset, true)
            });
            if matches!(message.header().operation, Operation::DeleteConsumerOffset)
                && !offset_blocked
                && let Some((kind, consumer_id, _, _)) = consumer_offset
                && let Err(error) = self.ensure_consumer_offset_exists(kind, consumer_id)
            {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "rejecting delete_consumer_offset for missing offset",
                    )
                    .with_operation(message.header().operation)
                    .with_error(error.to_string()),
                );
                // Deny on the primary before the op enters the pipeline: nothing
                // replicates, so backups never see the rejected delete, and the
                // client gets a typed failure instead of waiting out its reply
                // timeout. The code rides `ReplyHeader.status` (not the result
                // body): the HTTP listener's `classify_partition_reply` reads the
                // status field to render the typed 404.
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    error.as_code(),
                    "delete_consumer_offset deny reply send failed",
                    reply.take(),
                )
                .await;
                return;
            }

            // Reject an out-of-range consumer-offset store at admission,
            // mirroring the legacy `validate_partition_offset`: an empty
            // partition accepts no offset, and a stored offset may not run ahead
            // of the committed offset. Done here so the doomed op is never
            // replicated. Like the delete-offset deny above, the typed
            // `InvalidOffset` rides `ReplyHeader.status` (op=0, empty body): the
            // status-only `classify_partition_reply` would misread a result-body
            // code on this committed-shaped frame (op=commit_max) as success.
            if matches!(message.header().operation, Operation::StoreConsumerOffset)
                && !offset_blocked
                && let Some((kind, _, Some(requested_offset), _)) = consumer_offset
                && let Some(error) = self.store_offset_range_error(kind, requested_offset)
            {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "rejecting store_consumer_offset for out-of-range offset",
                    )
                    .with_operation(message.header().operation)
                    .with_error(error.to_string()),
                );
                Self::send_partition_deny_or_log(
                    consensus,
                    message.header(),
                    error.as_code(),
                    "store_consumer_offset deny reply send failed",
                    reply.take(),
                )
                .await;
                return;
            }

            // The node-local fast path is safe only for a single-replica
            // partition. Every mutation in a replicated partition enters VSR
            // regardless of the acknowledgement byte.
            if let Some((kind, consumer_id, offset, AckLevel::NoAck)) = consumer_offset
                && consensus.replica_count() == 1
                && matches!(
                    message.header().operation,
                    Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset,
                )
            {
                Disposition::NoAck {
                    request_header: Box::new(*message.header()),
                    kind,
                    consumer_id,
                    offset,
                }
            } else {
                let order = match self.next_local_request_order() {
                    Ok(order) => order,
                    Err(error) => {
                        Self::send_partition_deny_or_log(
                            consensus,
                            message.header(),
                            error.as_code(),
                            "local request order exhausted reply failed",
                            reply.take(),
                        )
                        .await;
                        return;
                    }
                };
                // Two-queue: prepare slot -> project+replicate; prepare full +
                // request room -> buffer; both full -> drop+warn (client retries
                // via read-timeout).
                let queued = self.local_request_must_queue() || offset_blocked;
                let reservation = if queued {
                    crate::PartitionIoVerdict::Pending
                } else {
                    self.admit_reserved_send(&message).await
                };
                if reservation == crate::PartitionIoVerdict::Pending {
                    let entry = consensus::RequestEntry::with_sender(message, reply.take());
                    let push_result = self.queue_local_request(entry, order);
                    if let Err(mut refused) = push_result {
                        emit_partition_diag(
                            tracing::Level::WARN,
                            &PartitionDiagEvent::new(
                                ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                                "on_request: prepare and request queues both full, dropping",
                            ),
                        );
                        // The request provably never entered either queue, so a
                        // waiter can be told so instead of waiting out its
                        // timeout.
                        let waiter = refused.take_reply_sender();
                        Self::send_partition_deny_or_log(
                            consensus,
                            refused.message.header(),
                            IggyError::TransientNotAccepted.as_code(),
                            "queues-full transient reply send failed",
                            waiter,
                        )
                        .await;
                    } else if owner_install.is_some() {
                        self.pending_owner_install.set(owner_install);
                    }
                    return;
                }

                if reservation == crate::PartitionIoVerdict::Failed {
                    self.deny_unreserved_send(message.header(), reply.take())
                        .await;
                    return;
                }
                if !self.admit_producer_batch(&message, &mut reply).await {
                    return;
                }
                if let Some((kind, consumer_id, Some(_), _)) = consumer_offset
                    && !self
                        .admit_consumer_offset_key(message.header(), kind, consumer_id, &mut reply)
                        .await
                {
                    return;
                }

                if owner_install.is_some() {
                    self.pending_owner_install.set(owner_install);
                }
                let prepare = self.pipeline_local_request(message, reply.take(), Some(order));
                Disposition::Replicate(prepare)
            }
        };

        match disposition {
            Disposition::Replicate(prepare) => self.on_replicate(prepare).await,
            Disposition::NoAck {
                request_header,
                kind,
                consumer_id,
                offset,
            } => {
                self.apply_consumer_offset_no_ack(
                    request_header,
                    kind,
                    consumer_id,
                    offset,
                    reply.take(),
                )
                .await;
            }
        }
    }

    /// Promote requests in local admission order after their predecessors settle.
    /// A reservation wait retains the original queue head and its reply sender.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn drain_request_queue_into_prepares(&mut self, slots_freed: usize) {
        if self.refuses_prepares() {
            self.notify_io();
            return;
        }

        self.resynchronize_consumer_offset_reservations();
        let mut promoted = 0;
        let mut consecutive_denials = 0usize;
        while promoted < slots_freed && !self.consensus.pipeline_is_full() {
            if !self.consensus.is_primary()
                || !self.consensus.is_normal()
                || self.consensus.is_transferring()
                || self.fatal.is_some()
            {
                break;
            }
            let head = self.consensus.with_pipeline(|pipeline| {
                pipeline.request_head().map(|request| {
                    (
                        *request.message.header(),
                        request.local_order,
                        self.request_mint_ceiling(&request.message),
                        self.queued_request_is_blocked(&request.message, request.local_order),
                    )
                })
            });
            let Some((header, order, ceiling, blocked)) = head else {
                break;
            };
            if blocked {
                break;
            }
            let reservation_ready = if let Some(ceiling) = ceiling {
                self.reserve_offsets_through_retryable(ceiling).await
            } else {
                crate::PartitionIoVerdict::Ready
            };
            if reservation_ready == crate::PartitionIoVerdict::Pending {
                break;
            }
            let unchanged = self.consensus.with_pipeline(|pipeline| {
                pipeline.request_head().is_some_and(|request| {
                    request.local_order == order
                        && bytemuck::bytes_of(request.message.header())
                            == bytemuck::bytes_of(&header)
                })
            });
            if !unchanged {
                break;
            }
            let Some(mut request) = self.consensus.pop_queued_request() else {
                break;
            };
            // A refusal below drops the queued install, so its latch goes with
            // it; promotion re-arms the latch.
            let queued_install = self.release_queued_install(&request);
            let context = request.take_auto_commit();
            if context.as_ref().is_some_and(|context| {
                !self
                    .consumer_offset_capacity_for(context.reservation.kind())
                    .owns(&context.reservation)
            }) {
                if Self::record_promotion_denial(&mut consecutive_denials) {
                    break;
                }
                continue;
            }

            // Taken before the preflight so a refusal answers the parked waiter
            // instead of waking it with `Canceled`.
            let mut reply_sender = request.take_reply_sender();
            if !self.request_history_available(&header) {
                if self
                    .deny_queued_request(
                        &header,
                        IggyError::HistoryUnavailable.as_code(),
                        "queued request belongs to destroyed history",
                        reply_sender.take(),
                        &mut consecutive_denials,
                    )
                    .await
                {
                    break;
                }
                continue;
            }
            let offset_request = matches!(
                header.operation,
                Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
            )
            .then(|| Self::parse_consumer_offset_request(header.operation, &request.message));
            if let Some(Ok((kind, consumer_id, _, _))) = offset_request
                && !self.matches_consumer_owner(
                    kind,
                    consumer_id,
                    header.client,
                    header.session,
                    header.owner_generation,
                    header.operation,
                )
            {
                if self
                    .deny_queued_request(
                        &header,
                        IggyError::ConsumerGroupPartitionNotOwned(
                            header.client,
                            u32::try_from(self.namespace().partition_id()).unwrap_or(u32::MAX),
                        )
                        .as_code(),
                        "queued offset owner is no longer installed",
                        reply_sender.take(),
                        &mut consecutive_denials,
                    )
                    .await
                {
                    break;
                }
                continue;
            }
            if !is_auto_commit_client(header.client)
                && header.metadata_watermark < self.required_metadata_frontier
            {
                if self
                    .deny_queued_request(
                        &header,
                        IggyError::TransientNotAccepted.as_code(),
                        "queued request metadata frontier is behind retirement",
                        reply_sender.take(),
                        &mut consecutive_denials,
                    )
                    .await
                {
                    break;
                }
                continue;
            }
            if reservation_ready == crate::PartitionIoVerdict::Failed {
                self.deny_unreserved_send(&header, reply_sender.take())
                    .await;
                break;
            }
            if self.persistence.as_ref().is_some_and(|persistence| {
                !persistence.has_capacity(Self::prepared_request_size(&request.message))
            }) {
                if self
                    .deny_queued_request(
                        &header,
                        IggyError::TransientNotAccepted.as_code(),
                        "queued request exceeds partition WAL capacity",
                        reply_sender.take(),
                        &mut consecutive_denials,
                    )
                    .await
                {
                    break;
                }
                continue;
            }
            if let Some(parsed) = offset_request {
                let (kind, consumer_id, offset, _) = match parsed {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        if self
                            .deny_queued_request(
                                &header,
                                error.as_code(),
                                "queued consumer offset parse reply failed",
                                reply_sender.take(),
                                &mut consecutive_denials,
                            )
                            .await
                        {
                            break;
                        }
                        continue;
                    }
                };
                if is_auto_commit_client(header.client)
                    && let Some(offset) = offset
                    && self
                        .durable_consumer_offsets
                        .covers(kind, consumer_id, offset)
                {
                    if let Some(sender) = reply_sender.take() {
                        let reply = build_reply_from_request(
                            &self.consensus,
                            &header,
                            committed_reply_body(header.operation),
                        )
                        .transmute_header(
                            |mut reply, output: &mut ReplyHeader| {
                                reply.reserved[0] = ReplyHeader::COMMITTED_RESULT;
                                reply.partition_incarnation = header.partition_incarnation;
                                *output = reply;
                            },
                        );
                        let _ = sender.send(reply);
                    }
                    continue;
                }
                let validation = offset.map_or_else(
                    || self.ensure_consumer_offset_exists(kind, consumer_id),
                    |offset| {
                        self.store_offset_range_error(kind, offset)
                            .map_or(Ok(()), Err)
                    },
                );
                if let Err(error) = validation {
                    if self
                        .deny_queued_request(
                            &header,
                            error.as_code(),
                            "queued consumer offset validation reply failed",
                            reply_sender.take(),
                            &mut consecutive_denials,
                        )
                        .await
                    {
                        break;
                    }
                    continue;
                }
                if offset.is_some()
                    && !self
                        .admit_consumer_offset_key(&header, kind, consumer_id, &mut reply_sender)
                        .await
                {
                    if Self::record_promotion_denial(&mut consecutive_denials) {
                        break;
                    }
                    continue;
                }
            }
            if !self
                .admit_producer_batch(&request.message, &mut reply_sender)
                .await
            {
                if Self::record_promotion_denial(&mut consecutive_denials) {
                    break;
                }
                continue;
            }
            // The disk can fall short while a send waits in the queue. It
            // holds no op yet, so its client can safely retry it.
            if header.operation == Operation::SendMessages
                && !self.reserve_send_space(&request.message)
            {
                if self
                    .deny_queued_request(
                        &header,
                        IggyError::TransientNotAccepted.as_code(),
                        "data disk fell below its free-space reserve",
                        reply_sender.take(),
                        &mut consecutive_denials,
                    )
                    .await
                {
                    break;
                }
                continue;
            }
            consecutive_denials = 0;
            if queued_install.is_some() {
                self.pending_owner_install.set(queued_install);
            }
            let prepare = self.pipeline_local_request(request.message, reply_sender, order);
            promoted += 1;
            self.on_replicate(prepare).await;
            self.release_send_space();
            drop(context);
        }
        self.drain_no_ack().await;
    }

    #[must_use]
    pub fn queued_requests_ready(&self) -> bool {
        self.no_ack_ready()
            || (self.fatal.is_none()
                && self.consensus.is_primary()
                && self.consensus.is_normal()
                && !self.consensus.is_transferring()
                && !self.consensus.pipeline_is_full()
                && self.consensus.with_pipeline(|pipeline| {
                    pipeline.request_head().is_some_and(|request| {
                        !self.queued_request_is_blocked(&request.message, request.local_order)
                    })
                }))
    }

    fn queued_request_is_blocked(
        &self,
        message: &Message<RoutedRequestHeader>,
        order: Option<LocalRequestOrder>,
    ) -> bool {
        self.no_ack
            .front()
            .is_some_and(|head| order.is_some_and(|order| order > head.local_order))
            || (matches!(
                message.header().operation,
                Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
            ) && Self::parse_consumer_offset_request(message.header().operation, message)
                .is_ok_and(|(kind, id, offset, _)| {
                    self.consumer_offset_is_blocked(kind, id, offset, false)
                }))
    }

    fn no_ack_ready(&self) -> bool {
        self.no_ack.front().is_some_and(|head| match head.phase {
            NoAckPhase::Validate => {
                self.no_ack_refusal(head).is_some()
                    || (self.pending_commit.is_none()
                        && (self.maintenance.shutdown.is_some()
                            || !self.consensus.with_pipeline(|pipeline| {
                                pipeline.has_request_before(head.local_order)
                            })))
            }
            NoAckPhase::OffsetReady(_) | NoAckPhase::Directory | NoAckPhase::Reply(_) => true,
            NoAckPhase::OffsetSubmitted(_) | NoAckPhase::DirectorySubmitted => false,
        })
    }

    fn no_ack_refusal(&self, head: &PendingNoAck) -> Option<IggyError> {
        (self.fatal.is_some()
            || !self.consensus.is_primary()
            || !self.consensus.is_normal()
            || self.consensus.is_transferring()
            || !self
                .consumer_offset_capacity_for(head.pending.kind)
                .owns(&head.reservation))
        .then_some(IggyError::TransientNotAccepted)
    }

    /// Nothing promotes a queued request during shutdown or a primary transfer,
    /// so answer every one now. None holds an op yet, so its client can safely
    /// retry it.
    async fn deny_queued_requests(&self, send_fail_label: &'static str) {
        while let Some(mut request) = self.consensus.pop_queued_request() {
            self.release_queued_install(&request);
            let waiter = request.take_reply_sender();
            Self::send_partition_deny_or_log(
                &self.consensus,
                request.message.header(),
                IggyError::TransientNotAccepted.as_code(),
                send_fail_label,
                waiter,
            )
            .await;
        }
    }

    /// A queued install leaves the queue with its latch. A refused install never
    /// publishes, so its latch would refuse every later copy as already pending.
    fn release_queued_install(
        &self,
        request: &consensus::RequestEntry,
    ) -> Option<InstallConsumerGroupOwnerRequest> {
        let install = (request.message.header().operation == Operation::InstallConsumerGroupOwner)
            .then(|| InstallConsumerGroupOwnerRequest::decode_from(request.message.body()).ok())
            .flatten();
        if install.is_some() && install == self.pending_owner_install.get() {
            self.pending_owner_install.set(None);
        }
        install
    }

    /// Resume a bounded promotion turn without waiting for another commit.
    pub async fn resume_queued_requests(&mut self) {
        if self.queued_requests_ready() {
            self.drain_request_queue_into_prepares(1).await;
        }
    }

    /// Bytes of the prepare that `project` builds. An offset prepare is sized
    /// from `header.size`, because an auto-commit buffer already reserves the
    /// owner trailer past it.
    fn prepared_request_size(message: &Message<RoutedRequestHeader>) -> usize {
        if matches!(
            message.header().operation,
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
        ) {
            message.header().size as usize
                + iggy_binary_protocol::requests::consumer_offsets::PREPARED_OWNER_GENERATION_SIZE
        } else {
            message.as_slice().len()
        }
    }

    async fn deny_queued_request(
        &self,
        header: &RoutedRequestHeader,
        status: u32,
        send_fail_label: &'static str,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
        consecutive_denials: &mut usize,
    ) -> bool {
        Self::send_partition_deny_or_log(self.consensus(), header, status, send_fail_label, waiter)
            .await;
        Self::record_promotion_denial(consecutive_denials)
    }

    const fn record_promotion_denial(consecutive_denials: &mut usize) -> bool {
        *consecutive_denials += 1;
        *consecutive_denials >= PROMOTION_DENIALS_MAX
    }

    /// # Panics
    /// Panics on a primary when a prepare's op is ahead of the local
    /// sequencer: journaling it would make the next op assignment collide,
    /// which is unrecoverable in place.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn on_replicate(&mut self, message: Message<PrepareHeader>) {
        if self.refuses_prepares() {
            self.notify_io();
            return;
        }

        self.resynchronize_consumer_offset_reservations();
        let header = *message.header();
        // Same reason as the metadata plane: `checksum` is compared as an opaque token
        // downstream, so a corrupted frame passes whenever its flipped value satisfies
        // those comparisons.
        if let Err(reason) = verify_prepare_integrity(&header, message.as_slice()) {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    ReplicaLogContext::from_consensus(self.consensus(), PlaneKind::Partitions),
                    "discarding prepare that failed its own integrity check",
                )
                .with_operation(header.operation)
                .with_op(header.op)
                .with_reason(reason),
            );
            return;
        }
        if !self.prepare_history_available(&header) {
            warn!(
                op = header.op,
                namespace_raw = header.group,
                "discarding prepare from unavailable partition history"
            );
            return;
        }
        let current_op = {
            let consensus = self.consensus();
            match replicate_preflight(consensus, &header) {
                Ok(current_op) => current_op,
                Err(reason) => {
                    emit_partition_diag(
                        tracing::Level::WARN,
                        &PartitionDiagEvent::new(
                            ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                            "ignoring prepare during replicate preflight",
                        )
                        .with_operation(header.operation)
                        .with_op(header.op)
                        .with_reason(reason.as_str()),
                    );
                    return;
                }
            }
        };
        #[allow(clippy::cast_possible_truncation)]
        let fenced_by_commit = fence_old_prepare_by_commit(self.consensus(), &header);
        if fenced_by_commit {
            if self.consensus().contradicts_applied_log(
                PlaneKind::Partitions,
                ContradictionSource::Prepare,
                header.view,
                [&header],
                |op| self.applied_header(op),
            ) {
                return;
            }
            emit_partition_diag(
                tracing::Level::DEBUG,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "received committed prepare retransmission",
                )
                .with_operation(header.operation)
                .with_op(header.op),
            );
            // A new primary may still need quorum for an op committed here.
            // Retained bytes prove its identity; the ack gate proves durability.
            if let Some(journaled) = self.log.journal().inner.repair_entry(header.op)
                && journaled_prepare_matches_retransmit(&journaled, &message)
            {
                self.send_prepare_ok(&header).await;
            }
            return;
        }
        // A Break drops the window from the broken op, inline or on the I/O lane
        // once a mounted partition's other history work settles. Either way this
        // prepare would sit above the cut on a head that does not chain to it.
        // The fence keeps its anchor. The primary resends only ops still short
        // of quorum, so gap repair, not a retransmit, may be what refills it.
        if self.consensus().is_follower() && self.offer_view_anchor(&header) {
            let verdict = self.prove_view_window().await;
            if matches!(verdict, Some(ViewWindowVerdict::Break { .. })) || self.refuses_prepares() {
                self.notify_io();
                return;
            }
        }

        let journal_holds_op = self.log.journal().inner.holds_op(header.op);
        if journal_holds_op {
            // Re-ack retransmissions so a lost acknowledgement cannot stall commit.
            emit_partition_diag(
                tracing::Level::DEBUG,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "journal already holds prepare, re-acking",
                )
                .with_operation(header.operation)
                .with_op(header.op),
            );
            let Some(journaled) = self.log.journal().inner.repair_entry(header.op) else {
                emit_partition_diag(
                    tracing::Level::ERROR,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "journal header exists without matching prepare bytes",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op),
                );
                return;
            };
            if !journaled_prepare_matches_retransmit(&journaled, &message) {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "rejecting retransmitted prepare that differs from the journaled entry",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op),
                );
                return;
            }
            let Some(frozen_for_forward) = restamp_prepare_view(journaled, header.view) else {
                emit_partition_diag(
                    tracing::Level::ERROR,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to restamp journaled prepare for retransmission",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op),
                );
                return;
            };
            let prepare_to_persist = self
                .persistence
                .as_ref()
                .map(|_| frozen_for_forward.clone());
            let consensus = self.consensus();
            if consensus.is_follower() && header.op > consensus.sequencer().current_sequence() {
                // Repair can hold this op above a hole, so the head moves only
                // along the contiguous run.
                self.advance_head_over_journaled_run();
                consensus.observe_prepare_timestamp(header.timestamp);
            }
            if let Err(error) = replicate_frozen_prepare(consensus, frozen_for_forward).await {
                let is_transport_error = error.is_transport();
                emit_partition_diag(
                    if is_transport_error {
                        tracing::Level::WARN
                    } else {
                        tracing::Level::ERROR
                    },
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to enqueue retransmitted prepare for replication",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
                if !is_transport_error {
                    return;
                }
            }
            if let Some(prepare) = prepare_to_persist
                && !self.submit_prepare_persistence(prepare, header.operation)
            {
                return;
            }
            self.send_prepare_ok(&header).await;
            return;
        }

        // A header-only StartView can announce bodies this backup still lacks.
        // Live same-view retransmits may fill its next verified slot; repair
        // frames above commit still require the elected canonical headers.
        let is_backup = self.consensus().is_follower();
        if is_backup {
            let fills_announced_gap = header.op <= current_op
                && !self.consensus().view_log_is_pending()
                && self
                    .log
                    .journal()
                    .inner
                    .repaired_window_shape(self.consensus().commit_min(), header.op - 1)
                    .complete
                && self.chains_to_journaled_predecessor(&header);
            if header.op != current_op + 1 && !fills_announced_gap {
                // `sequence` is what separates the two shapes this line covers:
                // a forward gap (op above the sequencer, the hole the repair
                // driver closes) and a retransmit of an op this replica already
                // sequenced. Without it they read identically.
                self.record_prepare_gap(&header, current_op, "dropping out-of-order prepare (gap)");
                // An op at or below the head is a retransmit, which the primary
                // sends straight to every backup that has not acknowledged it.
                return;
            }
            // While a transfer re-arm waits, an op above a hole only grows a
            // journal that the install wipes. A `StartView` adopted in the wait
            // lifts the head above the hole, so the next live op passes the gap
            // check. The lowest missing op still lands, so the live stream can
            // fill the hole.
            if self.transfer_rearm.is_some() && header.op > self.held_through() + 1 {
                self.record_prepare_gap(
                    &header,
                    current_op,
                    "dropping prepare above a hole while a transfer re-arm waits",
                );
                return;
            }
        } else {
            // Primary: `push_prepare_entry` pre-advanced the sequencer, so a
            // locally-originated prepare always satisfies
            // `header.op == current_op`. The two violation directions carry
            // very different risk:
            // - below the sequencer: a duplicate delivery (parked-frame
            //   redispatch, retransmit echo) of an op this primary already
            //   sequenced. Proceeding is safe only because the two gates above
            //   already returned for every copy this replica can still see:
            //   `fence_old_prepare_by_commit` drops the executed ops and
            //   `journal_holds_op` the resident ones, so reaching here means
            //   the journal lacks this op and has to be given it. Apply is not
            //   idempotent on its own for a produce: `append_messages`
            //   re-stamps from the local dirty counter and the journal's op
            //   index is last-write-wins, so appending an op the journal
            //   already holds would mint a second copy at fresh offsets and
            //   orphan the first. Log loudly for diagnosis.
            // - above the sequencer: journaling an op the sequencer has not
            //   assigned yet means the next local assignment would collide
            //   with it. Unreachable today (view fences run first, one
            //   primary per view, fanout targets only backups), so
            //   trip the invariant in debug; in release log loudly and drop
            //   rather than crash a library or corrupt op assignment.
            if header.op > current_op {
                debug_assert!(
                    header.op <= current_op,
                    "primary: prepare op {} ahead of sequencer {}; next op assignment would collide",
                    header.op,
                    current_op
                );
                emit_partition_diag(
                    tracing::Level::ERROR,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "primary prepare ahead of sequencer; dropping to avoid op-assignment collision",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op),
                );
                return;
            }
            if header.op < current_op {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "primary received prepare below sequencer; applying idempotently",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_reason("duplicate delivery"),
                );
            }
        }
        // The journal and primary fanout share the frozen allocation.
        let replicated_result = if is_backup && header.operation == Operation::SendMessages {
            self.append_received_send_messages_to_journal(message).await
        } else {
            self.apply_replicated_operation(message).await
        };
        let frozen_for_forward = match replicated_result {
            Ok(frozen) => frozen,
            Err(error) => {
                // A BACKUP refusing here is the design, not a fault: it rejects
                // any prepare whose `base_offset` does not continue its own
                // counter, which is exactly what a dropped or out-of-order
                // prepare leaves, and withholding `PrepareOk` is the fail-closed
                // answer. The primary retransmits, journal repair fills the gap,
                // and the group elects around the replica if it cannot catch up.
                // Nothing here is owed an ack this replica already skipped.
                //
                // On the PRIMARY the same return is unrecoverable. The op is
                // already in the pipeline with the sequencer advanced past it
                // (`push_prepare_entry`), and this sits ahead of
                // `send_prepare_ok`, so it never gets its ack and `commit_max`
                // can never pass it: every later op journals fine and queues
                // behind it forever. Nothing lifts that: the op never reached
                // the journal, so `retry_local_write` has nothing to re-submit,
                // and a solo group never starts a view change. Clients get no
                // reply at all, since replies are generated on commit, so they
                // wait out their read timeout, and once the queues fill so does
                // every send after.
                //
                // So fence there, the way a failed local commit of a
                // cluster-committed op does: the shard picks `fatal` up on its
                // next tick and takes the node down. A one-second superblock
                // backoff must not cost a partition the rest of the process's
                // life in the dark.
                emit_partition_diag(
                    if is_backup {
                        tracing::Level::WARN
                    } else {
                        tracing::Level::ERROR
                    },
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        if is_backup {
                            "failed to apply replicated partition operation"
                        } else {
                            "failed to apply an operation this replica sequenced; \
                             fencing the partition and shutting down"
                        },
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
                if !is_backup && self.fatal.is_none() {
                    self.fatal = Some(FatalCommit {
                        namespace_raw: self.namespace().inner(),
                        op: header.op,
                        operation: Some(header.operation),
                    });
                }
                return;
            }
        };

        let prepare_to_persist = self
            .persistence
            .as_ref()
            .map(|_| frozen_for_forward.clone());
        let consensus = self.consensus();
        // Backup only: advance sequencer + checksum after journal append.
        // Pre-advance on failing apply would leave consensus claiming op N
        // while the journal has nothing. Retransmit of N would silently drop
        // as is_old_prepare (header.op <= current_sequence). The primary does
        // not re-set here because push_prepare_entry already advanced it. A
        // sibling request pipelined during the apply await would otherwise be
        // rewound to a stale op + parent, projecting a duplicate next.
        if is_backup {
            // Filling a lower slot must not rewind the announced head or parent.
            if header.op >= current_op {
                consensus.sequencer().set_sequence(header.op);
                consensus.set_last_prepare_checksum(header.checksum);
                // A filled hole joins the run that repair journaled above it.
                if header
                    .op
                    .checked_add(1)
                    .is_some_and(|next| self.log.journal().inner.holds_op(next))
                {
                    self.advance_head_over_journaled_run();
                }
            }
            consensus.observe_prepare_timestamp(header.timestamp);
        }
        if let Err(error) = replicate_frozen_prepare(consensus, frozen_for_forward).await {
            let is_transport_error = error.is_transport();
            emit_partition_diag(
                if is_transport_error {
                    tracing::Level::WARN
                } else {
                    tracing::Level::ERROR
                },
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "failed to enqueue prepare for replication",
                )
                .with_operation(header.operation)
                .with_op(header.op)
                .with_error(error.to_string()),
            );
            if !is_transport_error {
                return;
            }
        }

        {
            let consensus = self.consensus();
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                header.op,
                consensus.pipeline_len(),
            );
        }

        if let Some(prepare) = prepare_to_persist
            && !self.submit_prepare_persistence(prepare, header.operation)
        {
            return;
        }
        self.send_prepare_ok(&header).await;
    }

    /// Whether `header` chains onto the entry this replica holds one op below
    /// it, resident or retained for repair.
    fn chains_to_journaled_predecessor(&self, header: &PrepareHeader) -> bool {
        if header.op == 1 {
            header.parent == 0
        } else {
            self.log
                .journal()
                .inner
                .repair_header(header.op - 1)
                .is_some_and(|previous| previous.checksum == header.parent)
        }
    }

    fn record_prepare_gap(&self, header: &PrepareHeader, sequence: u64, message: &'static str) {
        self.prepare_gap_drops
            .set(self.prepare_gap_drops.get().saturating_add(1));
        let now = self.consensus().message_bus().monotonic_micros();
        if now < self.prepare_gap_log_after.get() {
            return;
        }
        self.prepare_gap_log_after
            .set(now.saturating_add(PREPARE_GAP_LOG_INTERVAL_MICROS));
        emit_partition_diag(
            tracing::Level::WARN,
            &PartitionDiagEvent::new(self.diag_ctx(), message)
                .with_operation(header.operation)
                .with_op(header.op)
                .with_sequence(sequence),
        );
    }

    /// Move the head along the contiguous run the journal holds above it.
    ///
    /// DVC advertises the head and elections pick the highest, so a head above
    /// a hole can win a view it cannot walk. A hole stops the head, and the op
    /// that fills it, from repair or from the live stream, moves the head over
    /// every op journaled above it meanwhile. The checksum comes from the
    /// journaled header at the new head, so a lower backfill never rewinds the
    /// parent the next prepare chains onto. No append observes the timestamps
    /// of the passed ops: repair does not, and their live copies arrive below
    /// the head. The head's timestamp therefore joins the floor here.
    fn advance_head_over_journaled_run(&self) {
        let consensus = self.consensus();
        let journal = &self.log.journal().inner;
        let Some((head, checksum)) =
            repaired_frontier_update(consensus.sequencer().current_sequence(), |op| {
                journal.header_by_op(op)
            })
        else {
            return;
        };
        consensus.sequencer().set_sequence(head);
        consensus.set_last_prepare_checksum(checksum);
        if let Some(header) = journal.header_by_op(head) {
            consensus.observe_prepare_timestamp(header.timestamp);
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&mut self, message: Message<PrepareOkHeader>, config: &PartitionsConfig) {
        if self.fatal.is_some() {
            return;
        }
        self.resynchronize_consumer_offset_reservations();
        let header = *message.header();
        {
            let consensus = self.consensus();
            if let Err(reason) = ack_preflight(consensus) {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "ignoring ack during preflight",
                    )
                    .with_op(header.op)
                    .with_reason(reason.as_str()),
                );
                return;
            }

            if !consensus.pipeline_holds_entry(header.op, header.prepare_checksum) {
                consensus.handle_late_prepare_ok(&header);
                emit_partition_diag(
                    tracing::Level::DEBUG,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "ack target prepare not in pipeline",
                    )
                    .with_op(header.op)
                    .with_prepare_checksum(header.prepare_checksum),
                );
                return;
            }
        }

        if !ack_quorum_reached(self.consensus(), PlaneKind::Partitions, &header) {
            return;
        }
        if self.commit_waits_for_checkpoint() {
            return;
        }

        if self.pending_commit.is_some() {
            self.handle_committed_entries(Vec::new(), config, true)
                .await;
            return;
        }

        let drained = self.select_persistable_commits(config);
        if drained.is_empty() {
            return;
        }

        self.handle_committed_entries(drained, config, true).await;
        {
            let consensus = self.consensus();
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                consensus.commit_min(),
                consensus.pipeline_len(),
            );
        }
    }

    /// Apply the committed prefix this replica can reach, from the pipeline if
    /// the primary populated one and from the journal otherwise.
    ///
    /// The journal half is bounded by [`COMMIT_WALK_OPS_MAX`], for EVERY caller
    /// and not just the tick sweep: `on_commit`, `StartView` adoption, the
    /// post-transfer tail and the post-repair walk all reach this with the same
    /// resident backlog behind them, and all four run on the shard pump. The
    /// pipeline half is left alone, being the primary's own in-flight window,
    /// which the pipeline depth already caps.
    ///
    /// Nothing is lost by stopping early: the run is re-derived from
    /// `commit_min` on the next call, and the sweep's walk predicate stays true
    /// until the group drains, so the next tick resumes exactly here. The one
    /// caller with no next tick is the shutdown drain, and it is covered twice
    /// over: [`Self::flush_committed_messages`] persists the committed prefix
    /// by bytes rather than by walk, and what stays un-applied sits above the
    /// `commit_min` the superblock records, which is what makes the restarted
    /// replica ask its peers for it.
    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&mut self, config: &PartitionsConfig) {
        self.refresh_proof_walk_memory();
        if self.transition.is_some() || self.io_active.is_retiring() {
            self.notify_io();
            return;
        }

        if self.fatal.is_some()
            || self.materialization_missing
            || self.commit_waits_for_checkpoint()
            || self.consensus.view_log_unproven()
        {
            return;
        }
        self.resynchronize_consumer_offset_reservations();

        if self.pending_commit.is_some() {
            self.handle_committed_entries(Vec::new(), config, false)
                .await;
            return;
        }
        // After the in-flight commit: a proof that breaks truncates the journal.
        if self.view_fenced() {
            if !self.view_proof_deferred() {
                self.prove_view_window().await;
            }
            if self.view_fenced() && self.recovered_commit <= self.consensus.commit_min() {
                return;
            }
        }

        // The primary commits inline via `on_ack` (it drains its own pipeline).
        // Backups never populate the pipeline - they journal replicated prepares
        // in `apply_replicated_operation` - so the pipeline drain is empty for
        // them. Fall back to the journal so backups durably persist committed
        // data. `commit_messages` then flushes only the committed prefix and
        // keeps the uncommitted tail journal-resident, so a later commit of that
        // tail still finds its headers here (no wedge). Pipeline-first keeps a
        // freshly promoted primary (rebuilt pipeline) draining there, avoiding a
        // double-count against `advance_commit_min`.
        // A fenced pipeline holds unproven entries: only the journal walk, capped
        // at the recovered commit point, may take ops.
        let mut drained = if self.view_fenced() {
            Vec::new()
        } else {
            self.select_persistable_commits(config)
        };
        let send_client_replies = !drained.is_empty() && self.consensus.is_primary();
        if drained.is_empty() {
            drained = self.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, config);
        }
        if drained.is_empty() {
            return;
        }

        self.handle_committed_entries(drained, config, send_client_replies)
            .await;
        {
            let consensus = self.consensus();
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                consensus.commit_min(),
                consensus.pipeline_len(),
            );
        }
    }

    fn select_persistable_commits(&self, config: &PartitionsConfig) -> Vec<PrepareHeader> {
        if self.transition.is_some() || self.io_active.is_retiring() {
            return Vec::new();
        }

        if self.pending_commit.is_some() {
            return Vec::new();
        }
        let Some(first) = consensus::peek_committable_head(self.consensus()) else {
            // A repairing backup has no pipeline, and its journal walk commits
            // only what the WAL accepted. Shutdown keeps repair history out.
            if self.maintenance.shutdown.is_none() {
                self.persist_repaired_prefix();
            }
            return Vec::new();
        };
        self.persist_repaired_prefix();
        let through = self.persistence.as_ref().map_or_else(
            || self.consensus.commit_max(),
            |persistence| self.consensus.commit_max().min(persistence.head()),
        );
        let materialize = self.should_persist_messages(config);
        self.consensus.with_pipeline(|pipeline| {
            let mut selected = Vec::new();
            for op in first.op..=through {
                let Some(entry) = pipeline.entry_by_op(op) else {
                    break;
                };
                if !self.consensus.view_log_entry_proven(entry.header.op)
                    || !self.prepare_body_is_ready(&entry.header, materialize)
                {
                    break;
                }
                if self.no_ack.front().is_some_and(|head| {
                    entry
                        .local_order
                        .is_some_and(|order| order > head.local_order)
                }) {
                    break;
                }
                selected.push(entry.header);
            }
            selected
        })
    }

    /// Under a dispatcher a Replicated commit runs ahead of the WAL writer while
    /// [`PartitionPersistence::can_run_ahead`] allows it, and its flush takes
    /// the written prefix later, see `flushable_through`. Without one the
    /// commit flushes inline, so a due flush waits for the written body.
    fn prepare_body_is_ready(&self, header: &PrepareHeader, materialize: bool) -> bool {
        let dispatched = self.has_io_dispatcher();
        header.operation != Operation::SendMessages
            || (!self.durability().is_persisted() && !materialize && !dispatched)
            || self
                .persistence
                .as_ref()
                .filter(|persistence| persistence.segment_checkpoint().is_some())
                .is_none_or(|persistence| {
                    if self.durability().is_persisted() {
                        persistence.is_durable(header)
                    } else {
                        persistence.is_written(header)
                            || (dispatched && persistence.can_run_ahead())
                    }
                })
    }

    /// Committable entries (ops `commit_min+1 ..= commit_max`) read from the
    /// journal, for a backup whose pipeline is empty. Stops at the first missing
    /// op: a replication gap must not be skipped, or `advance_commit_min`'s
    /// sequential contract breaks.
    ///
    /// Reads resident headers only. The commit walk caps flushing at its last
    /// drained op, keeping later headers resident for the next bounded walk.
    fn collect_committable_from_journal(
        &self,
        max_ops: usize,
        config: &PartitionsConfig,
    ) -> Vec<PrepareHeader> {
        if self.transition.is_some() || self.io_active.is_retiring() {
            return Vec::new();
        }

        let from_op = self.consensus.commit_min() + 1;
        if self.view_fenced() && self.recovered_commit < from_op {
            return Vec::new();
        }
        // Stop below the pipeline head. The drain above holds rather than pops
        // when the head is not the op owed next; walking that op out of the
        // journal instead advances `commit_min` past a still-resident entry, and
        // `on_ack` then finds the drain empty on every later ack -- no reply is
        // ever shipped and each stranded entry leaves its awaiter parked.
        //
        // Only a head at or above `from_op` lowers the ceiling: an absent head is
        // a backup's empty pipeline, and a lower head is already stranded and must
        // not freeze the walk on top of that.
        let commit_max = self.persistence.as_ref().map_or_else(
            || self.consensus.commit_max(),
            |persistence| self.consensus.commit_max().min(persistence.head()),
        );
        let commit_max = self
            .consensus
            .pipeline_head_header()
            .filter(|head| head.op >= from_op)
            .map_or(commit_max, |head| commit_max.min(head.op - 1));
        let commit_max = if self.view_fenced() {
            commit_max.min(self.recovered_commit)
        } else {
            commit_max
        };
        let materialize = self.should_persist_messages(config);
        self.log
            .journal()
            .inner
            .committed_headers_from(from_op, commit_max, max_ops)
            .into_iter()
            .take_while(|header| {
                self.consensus.view_log_entry_proven(header.op)
                    && self.prepare_body_is_ready(header, materialize)
            })
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    async fn apply_replicated_operation(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<Frozen<4096>, IggyError> {
        let header = *message.header();
        let replica_id = self.consensus.replica();
        let namespace_raw = self.consensus.group();

        match header.operation {
            operation if operation.is_partition_lifecycle() => {
                PartitionLifecycle::decode(operation, message.body())?;
                let frozen = message.into_frozen();
                self.journal_append(frozen.clone()).await?;
                Ok(frozen)
            }
            Operation::StateMachineCommand => {
                Self::check_replicated_command(message.body())?;
                let frozen = message.into_frozen();
                self.journal_append(frozen.clone()).await?;
                Ok(frozen)
            }
            Operation::RetireSession => {
                let retirement = RetireSessionsRequest::decode_from(message.body())
                    .map_err(|_| IggyError::InvalidCommand)?;
                if retirement
                    .identities
                    .iter()
                    .any(|identity| is_auto_commit_client(identity.client_id))
                {
                    return Err(IggyError::InvalidCommand);
                }
                let frozen = message.into_frozen();
                self.journal_append(frozen.clone()).await?;
                Ok(frozen)
            }
            Operation::SendMessages => {
                let frozen = self.append_send_messages_to_journal(message).await?;
                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = replica_id,
                    op = header.op,
                    namespace_raw,
                    operation = ?header.operation,
                    "replicated send_messages appended to partition journal"
                );
                Ok(frozen)
            }
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                // Replication semantics do not depend on the acknowledgement
                // byte. Multi-replica `NoAck` requests enter this same path.
                let (kind, consumer_id, offset, _ack, owner_generation) =
                    Self::parse_staged_consumer_offset_commit(header.operation, &message)?;
                let write_lock = self.write_lock.clone();
                let _guard = write_lock.lock().await;

                // Journal the prepare before staging so
                // `VsrAction::RetransmitPrepares` can read this op back
                // on a view change. Without the journal entry, the
                // `header_by_op` lookup in `on_replicate` would miss,
                // the gap check would drop the retransmit, and the
                // primary's pipeline would wedge indefinitely. Skip
                // the `journal.info` accounting: it counts SendMessages
                // batches for segment-commit thresholds, which do not
                // apply to offset ops.
                let frozen = message.into_frozen();
                self.journal_append(frozen.clone()).await?;

                match header.operation {
                    Operation::StoreConsumerOffset => {
                        self.stage_consumer_offset_upsert(
                            header.op,
                            kind,
                            consumer_id,
                            offset.expect("store_consumer_offset must include offset"),
                            is_auto_commit_client(header.client),
                        );
                    }
                    Operation::DeleteConsumerOffset => {
                        self.stage_consumer_offset_delete(header.op, kind, consumer_id);
                    }
                    _ => unreachable!(),
                }
                if let Some(pending) = self.pending_consumer_offset_commits.get_mut(&header.op) {
                    pending.owner_generation = owner_generation;
                }

                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = replica_id,
                    op = header.op,
                    namespace_raw,
                    operation = ?header.operation,
                    consumer_kind = ?kind,
                    consumer_id,
                    offset = ?offset,
                    "replicated consumer offset journaled and staged"
                );
                Ok(frozen)
            }
            _ => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = replica_id,
                    namespace_raw,
                    op = header.op,
                    operation = ?header.operation,
                    "unexpected replicated partition operation"
                );
                Err(IggyError::InvalidCommand)
            }
        }
    }

    /// Journal one prepare and record the mutation.
    ///
    /// Every partition-plane append goes through here. The DVC suffix snapshot is
    /// tagged by the head and the commit point, and a repair filling a body under
    /// a `StartView` this replica already adopted moves neither: the head is the
    /// announced one and the op is not committed yet. An append that skipped the
    /// counter would leave the header-only snapshot reading as current, and the
    /// merge takes a header no sender can serve as proof that sender never
    /// journaled the op.
    async fn journal_append(&self, entry: Frozen<4096>) -> Result<(), IggyError> {
        let appended = self.log.journal().inner.append(entry).await;
        self.consensus.note_journal_mutation();
        appended.map_err(|_| IggyError::CannotAppendMessage)
    }

    async fn append_send_messages_to_journal(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<Frozen<4096>, IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        self.stamp_and_append_messages(message)
            .await
            .map(|journaled| journaled.prepare)
    }

    async fn append_received_send_messages_to_journal(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<Frozen<4096>, IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        let header = *message.header();
        if header.operation != Operation::SendMessages {
            return Err(IggyError::CannotAppendMessage);
        }
        let validated = decode_prepare_slice(message.as_slice())?.header;
        if validated.message_count == 0 {
            return Err(IggyError::InvalidCommand);
        }
        let expected_offset = if self.offset_space.append_live {
            self.dirty_offset
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or(IggyError::CannotAppendMessage)?
        } else {
            0
        };
        if validated.base_timestamp != header.timestamp {
            return Err(IggyError::CannotAppendMessage);
        }
        if validated.base_offset == expected_offset {
            return self
                .append_stamped_messages(message, validated)
                .await
                .map(|journaled| journaled.prepare);
        }
        // Repair can journal ops above a hole and lift the dirty offset past
        // it, so the live prepare that fills the hole starts below that offset.
        // Chained to the entry below it, it carries the offsets the primary
        // stamped, which repair would journal for this op as well.
        let fills_hole = validated.base_offset < expected_offset
            && self
                .log
                .journal()
                .inner
                .last_op()
                .is_some_and(|last| last > header.op)
            && self.chains_to_journaled_predecessor(&header);
        if !fills_hole {
            return Err(IggyError::CannotAppendMessage);
        }
        self.append_canonical_send_messages(message, validated)
            .await
    }

    async fn append_stamped_messages(
        &mut self,
        message: Message<PrepareHeader>,
        batch: BatchHeader,
    ) -> Result<JournaledMessages, IggyError> {
        let batch_messages_count = batch.message_count;
        if batch_messages_count == 0 {
            return Err(IggyError::CannotAppendMessage);
        }

        let batch_messages_size =
            u64::try_from(batch.total_size()).map_err(|_| IggyError::CannotAppendMessage)?;
        let last_dirty_offset = batch
            .base_offset
            .checked_add(u64::from(batch_messages_count) - 1)
            .ok_or(IggyError::CannotAppendMessage)?;

        // Admission must have covered this exact mint frontier before sequencing.
        if !self.offset_range_is_reserved(last_dirty_offset) {
            return Err(IggyError::CannotAppendMessage);
        }

        let segment_index = self.log.segments().len() - 1;
        let current_position = self.log.segments()[segment_index].current_position;
        let next_position = current_position
            .checked_add(batch_messages_size)
            .ok_or(IggyError::CannotAppendMessage)?;

        let mut journal_info = self.log.journal().info;
        journal_info.messages_count = journal_info
            .messages_count
            .checked_add(batch_messages_count)
            .ok_or(IggyError::CannotAppendMessage)?;
        journal_info.size = IggyByteSize::from(
            journal_info
                .size
                .as_bytes_u64()
                .checked_add(batch_messages_size)
                .ok_or(IggyError::CannotAppendMessage)?,
        );
        journal_info.current_offset = last_dirty_offset;
        if journal_info.first_timestamp == 0 {
            journal_info.first_timestamp = batch.base_timestamp;
        }
        journal_info.end_timestamp = batch.base_timestamp;
        journal_info.max_timestamp = journal_info.max_timestamp.max(batch.base_timestamp);

        let frozen = message.into_frozen();
        self.journal_append(frozen.clone()).await?;

        self.note_append_live();
        self.dirty_offset
            .store(last_dirty_offset, Ordering::Relaxed);
        self.log.segments_mut()[segment_index].current_position = next_position;
        self.log.journal_mut().info = journal_info;

        Ok(JournaledMessages {
            result: AppendResult::new(batch.base_offset, last_dirty_offset, batch_messages_count),
            prepare: frozen,
        })
    }

    /// Hold `(fence_floor, through]` back from commit and acknowledgement, where
    /// `fence_floor` is the higher of `commit_min` and the recovered commit point, until
    /// [`Self::prove_view_window`] links it into `anchor`, the current view's header
    /// at `through + 1`. Without an anchor the first current-view prepare at that op
    /// supplies one (`offer_view_anchor`).
    ///
    /// For a replica that adopted a newer view: it can hold an entry from a log the
    /// view discarded under the view's commit point, which nothing else compares
    /// against the view's log.
    pub fn fence_view_window(&mut self, through: u64, anchor: Option<PrepareHeader>) {
        if through <= self.fence_floor() {
            self.view_fence = None;
            return;
        }
        let view = self.consensus.view();
        let previous = self
            .view_fence
            .filter(|fence| fence.view == view && fence.through == through);
        let anchor = anchor
            .filter(PrepareHeader::anchors_chain)
            .or_else(|| previous.and_then(|fence| fence.anchor));
        if previous.is_none() {
            debug!(
                namespace_raw = self.namespace().inner(),
                view,
                commit_min = self.consensus.commit_min(),
                floor = self.fence_floor(),
                recovered_commit = self.recovered_commit,
                through,
                anchored = anchor.is_some(),
                "holding partition entries journaled before this view until its log proves them"
            );
        }
        self.view_fence = Some(ViewFence {
            view,
            through,
            anchor,
            hole: previous.and_then(|fence| fence.hole),
            unanchored_ticks: previous.map_or(0, |fence| fence.unanchored_ticks),
        });
    }

    /// Highest op through which this replica holds every entry above its commit
    /// point. Repair progress, since a fenced commit point cannot move until the
    /// whole window is held.
    #[must_use]
    pub fn held_through(&self) -> u64 {
        self.log
            .journal()
            .inner
            .held_through(self.consensus.commit_min())
    }

    /// The held prefix finds replication gaps. The received prefix inside it
    /// follows the same per-operation durability contract as prepare acks.
    #[must_use]
    pub fn replication_progress(&self) -> Option<consensus::BackupPrefix> {
        if !self.consensus.is_normal()
            || self.consensus.is_transferring()
            || self.consensus.has_untrusted_log()
            || self.view_fenced()
            || self.read_history_is_changing()
        {
            return None;
        }
        let held_through = self.held_through();
        let mut held = self.consensus.commit_min();
        let mut received = None;
        while held < held_through {
            let next = held + 1;
            let Some(header) = self.log.journal().inner.repair_header(next) else {
                break;
            };
            if !self.consensus.view_log_entry_proven(next) {
                break;
            }
            if received.is_none() && !self.prepare_ack_ready(&header) {
                received = Some(held);
            }
            held = next;
        }
        Some(consensus::BackupPrefix {
            held_op: held,
            received_op: received.unwrap_or(held),
        })
    }

    /// Whether an anchored fence holds every entry of its window, so a proof
    /// now concludes instead of waiting on repair.
    #[must_use]
    pub fn view_fence_held(&self) -> bool {
        self.view_fence.as_ref().is_some_and(|fence| {
            fence.anchor.is_some()
                && fence.through > self.consensus.commit_min()
                && self
                    .log
                    .journal()
                    .inner
                    .held_through_until(self.consensus.commit_min(), fence.through)
                    >= fence.through
        })
    }

    /// Whether a commit walk would run the proof of a held fence now.
    #[must_use]
    pub fn view_fence_provable(&self) -> bool {
        self.fatal.is_none()
            && !self.materialization_missing
            && !self.persistence_checkpoint_pending()
            // The commit walk returns before the proof while the view's log is unproven.
            && !self.consensus.view_log_unproven()
            && !self.view_proof_deferred()
            && self.view_fence_held()
    }

    /// Whether a proof now would only find the same `Break` again: a Break found
    /// while history work runs or a transition is queued cuts nothing
    /// (`drop_unproven_from`), so walking the window rebuilds its header map for
    /// nothing.
    fn view_proof_deferred(&self) -> bool {
        self.history_is_busy() || self.transition.is_some()
    }

    /// An installed snapshot replaces the history that the WAL commit points named.
    pub(crate) const fn forget_recovered_commit(&mut self) {
        self.recovered_commit = 0;
        self.unreplayed_commit = 0;
    }

    /// Whether a fenced walk still has recovered committed ops to take.
    #[must_use]
    pub fn recovered_prefix_pending(&self) -> bool {
        self.view_fenced() && self.recovered_commit > self.consensus.commit_min()
    }

    /// Bottom of a fenced window: what this replica committed, or the recovered
    /// commit point when the walk has not reached it yet.
    #[must_use]
    pub fn fence_floor(&self) -> u64 {
        self.consensus.commit_min().max(self.recovered_commit)
    }

    /// Whether a fenced window is still unproven.
    #[must_use]
    pub fn view_fenced(&self) -> bool {
        self.view_fence
            .as_ref()
            .is_some_and(|fence| fence.through > self.consensus.commit_min())
    }

    /// Whether the fence is still waiting for its anchor.
    #[must_use]
    pub fn view_fence_unanchored(&self) -> bool {
        self.view_fenced()
            && self
                .view_fence
                .as_ref()
                .is_some_and(|fence| fence.anchor.is_none())
    }

    /// Count one tick without an anchor; true every `every` ticks, when the caller
    /// should ask the view's primary for its headers again.
    pub fn view_fence_probe_due(&mut self, every: u32) -> bool {
        if !self.view_fence_unanchored() {
            return false;
        }
        let Some(fence) = self.view_fence.as_mut() else {
            return false;
        };
        fence.unanchored_ticks = fence.unanchored_ticks.saturating_add(1);
        if fence.unanchored_ticks < every.max(1) {
            return false;
        }
        fence.unanchored_ticks = 0;
        true
    }

    /// Take `header` as the fence's anchor when it is the op above the window and
    /// was prepared in the fence's view. Only for a header from that view's primary
    /// (a prepare that passed `replicate_preflight`, or its `StartView`). Returns
    /// whether the anchor was taken.
    pub(crate) const fn offer_view_anchor(&mut self, header: &PrepareHeader) -> bool {
        if let Some(fence) = self.view_fence.as_mut()
            && fence.anchor.is_none()
            && fence.view == header.view
            && header.op == fence.through + 1
            && header.anchors_chain()
        {
            fence.anchor = Some(*header);
            fence.hole = None;
            return true;
        }
        false
    }

    /// Lowest op of the fenced window, at or under `commit_max`, with no entry:
    /// what repair has to fetch before the chain can reach below it.
    #[must_use]
    pub fn view_fence_hole(&self) -> Option<u64> {
        let fence = self.view_fence.as_ref()?;
        let floor = self.fence_floor();
        let top = fence.through.min(self.consensus.commit_max());
        let held = self.log.journal().inner.held_through_until(floor, top);
        (held < top).then_some(held + 1)
    }

    /// Residency of a window op. A forced flush evicts past `commit_min` only on a
    /// caught-up primary or at shutdown, and the flush is capped while fenced, so
    /// a window op is never ring-only and the index alone answers.
    fn holds_view_entry(&self, op: u64) -> bool {
        self.log.journal().inner.holds_op(op)
    }

    /// Check the fenced window against its anchor. `Proven` lifts the fence, a
    /// `Break` drops the entries from the broken op so repair refetches the view's,
    /// a `Hole` waits for repair. `None` while there is no fence or no anchor.
    ///
    /// A `Break` cuts the whole suffix from the broken op, entries the anchor
    /// already proved and current-view prepares included; none of them was acked.
    /// Without an I/O dispatcher the cut runs here. With one it is handed to the
    /// I/O lane, or left for the next proof while other history work holds the
    /// lane, and the entries stay resident until the lane runs it.
    pub async fn prove_view_window(&mut self) -> Option<ViewWindowVerdict> {
        let fence = self.view_fence?;
        let floor = self.fence_floor();
        if fence.through <= floor {
            self.view_fence = None;
            self.persist_repaired_prefix();
            return Some(ViewWindowVerdict::Proven);
        }
        let anchor = fence.anchor?;
        // A commit floor raised past the hole (a repair floor, an install) took
        // it out of the window.
        if let Some(hole) = fence.hole.filter(|hole| *hole > floor)
            && !self.holds_view_entry(hole)
        {
            return Some(ViewWindowVerdict::Hole { op: hole });
        }
        let journal = &self.log.journal().inner;
        let verdict = view_window_verdict(floor, fence.through, anchor.parent, |op| {
            journal.repair_header(op)
        });
        match verdict {
            ViewWindowVerdict::Proven => {
                // The WAL flush below reads the last append per op; a duplicate
                // whose last append is not the entry just proven breaks there.
                if let Some(op) = (floor + 1..=fence.through).find_map(|op| {
                    let header = journal.repair_header(op)?;
                    let flushed = journal.repair_entry(op)?;
                    let flushed = bytemuck::checked::try_from_bytes::<PrepareHeader>(
                        &flushed.as_slice()[..size_of::<PrepareHeader>()],
                    )
                    .ok()?;
                    (flushed.checksum != header.checksum).then_some(op)
                }) {
                    self.drop_unproven_from(op).await;
                    return Some(ViewWindowVerdict::Break { op });
                }
                debug!(
                    namespace_raw = self.namespace().inner(),
                    through = fence.through,
                    "the adopted view's log proves the held partition entries"
                );
                self.view_fence = None;
                // A Break earlier in this fence left it naming a dropped entry.
                let head = self.consensus.sequencer().current_sequence();
                if let Some(header) = self.log.journal().inner.repair_header(head) {
                    self.consensus.set_last_prepare_checksum(header.checksum);
                }
                self.persist_repaired_prefix();
                self.queue_acks_withheld_by_fence(fence.through, head);
            }
            ViewWindowVerdict::Hole { op } => {
                if fence.hole != Some(op) {
                    debug!(
                        namespace_raw = self.namespace().inner(),
                        op, "held partition entries wait on op {op} before they can be proven"
                    );
                }
                if let Some(fence) = self.view_fence.as_mut() {
                    fence.hole = Some(op);
                }
            }
            ViewWindowVerdict::Break { op } => self.drop_unproven_from(op).await,
        }
        Some(verdict)
    }

    async fn drop_unproven_from(&mut self, op: u64) {
        // The evicted ring holds only the applied prefix, so a broken entry above
        // `commit_min` is always resident.
        debug_assert!(self.log.journal().inner.holds_op(op));
        if !self.log.journal().inner.holds_op(op) {
            error!(
                namespace_raw = self.namespace().inner(),
                op,
                "partition entry at op {op} is not the adopted view's and is already \
                 flushed; marking the partition fatal and shutting down"
            );
            self.mark_view_fence_fatal(op);
            return;
        }
        if self.has_io_dispatcher() {
            // The I/O lane owns a mounted partition's WAL. While other history work
            // is in flight or a transition is queued the fence keeps holding, and
            // the next proof finds the same break.
            if !self.view_proof_deferred() {
                self.unproven_drop_from = Some(op);
                self.defer_view_truncation(op..=u64::MAX, Vec::new(), None);
            }
            return;
        }
        match self.truncate_uncommitted_from(op).await {
            Ok(removed) => self.note_unproven_dropped(op, removed),
            Err(error) => {
                error!(
                    namespace_raw = self.namespace().inner(),
                    op,
                    %error,
                    "cannot drop partition entries from op {op} the adopted view's log does not \
                     chain to; marking the partition fatal and shutting down"
                );
                self.mark_view_fence_fatal(op);
            }
        }
    }

    fn note_unproven_dropped(&mut self, op: u64, removed: usize) {
        warn!(
            namespace_raw = self.namespace().inner(),
            op,
            removed,
            view = self.consensus.view(),
            "dropped {removed} partition entries from op {op}: the adopted view's log does not \
             chain to them"
        );
        if let Some(fence) = self.view_fence.as_mut() {
            fence.hole = None;
        }
        self.persist_repaired_prefix();
    }

    /// Prepares this view's primary sent while the window was fenced were
    /// journaled but neither persisted nor acked. The primary resends one only
    /// while it lacks quorum, after its retransmit timeout. Queue their acks for
    /// the WAL to release as soon as the fence lifts.
    fn queue_acks_withheld_by_fence(&self, through: u64, head: u64) {
        let view = self.consensus.view();
        let mut pending = self.pending_persisted_acks.borrow_mut();
        for op in through + 1..=head {
            if let Some(header) = self.log.journal().inner.repair_header(op)
                && header.view == view
            {
                pending.insert(op, header);
            }
        }
        drop(pending);
        self.notify_io();
    }

    fn mark_view_fence_fatal(&mut self, op: u64) {
        self.fatal = Some(FatalCommit {
            namespace_raw: self.namespace().inner(),
            op,
            operation: None,
        });
    }

    /// Whether the WAL replayed history at boot, consumed by the first view
    /// adoption so it fences that history even with `log_view` unchanged.
    pub fn take_recovered_unproven(&mut self) -> bool {
        std::mem::take(&mut self.recovered_unproven)
    }

    /// Drop an uncommitted view-divergent suffix inline and restore append cursors
    /// from the retained prefix. Mounted partitions defer this work to the I/O lane.
    ///
    /// # Errors
    ///
    /// Returns an error if a retained batch is invalid, the restored segment
    /// position overflows, or the journal cannot truncate the suffix.
    pub async fn truncate_uncommitted_from(&mut self, from_op: u64) -> Result<usize, IggyError> {
        if let Some(persistence) = &self.persistence {
            persistence.truncate_from(from_op);
            self.start_persistence();
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|_| IggyError::CannotSyncFile)?;
        }
        self.truncate_resident_from(from_op).await
    }

    async fn truncate_resident_from(&mut self, from_op: u64) -> Result<usize, IggyError> {
        self.remove_resident_range(from_op..=u64::MAX).await
    }

    /// Replace a proven divergent run while retaining the canonical tail across gaps.
    ///
    /// # Errors
    /// Returns an error if the WAL or resident indexes cannot be rewritten.
    pub async fn remove_divergent_prepares(
        &mut self,
        ops: std::ops::RangeInclusive<u64>,
    ) -> Result<(), IggyError> {
        if *ops.start()
            <= self
                .consensus
                .commit_min()
                .max(self.log.journal().inner.evicted_through())
        {
            return Err(IggyError::InvalidCommand);
        }
        if let Some(persistence) = &self.persistence {
            // The durable WAL is contiguous. A canonical tail beyond a divergent
            // entry is repair-held and must be persisted again after the gap fills.
            // Truncation invalidates the head certificate. A crash before refill
            // requires state transfer; the old certificate cannot vouch for a prefix.
            persistence.truncate_from(*ops.start());
            self.start_persistence();
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|_| IggyError::CannotSyncFile)?;
        }
        self.remove_resident_range(ops).await?;
        Ok(())
    }

    async fn remove_resident_range(
        &mut self,
        ops: std::ops::RangeInclusive<u64>,
    ) -> Result<usize, IggyError> {
        let mut entries = self.log.journal().inner.resident_entries();
        entries.sort_unstable_by_key(peek_op);
        let mut retained_info = JournalInfo::default();
        let mut retained_next_offset = 0;
        let mut removed_messages = false;
        for entry in &entries {
            if peek_operation(entry) != Operation::SendMessages {
                continue;
            }
            let batch = decode_prepare_slice_trusted(entry.as_slice())
                .map_err(|_| IggyError::InvalidCommand)?;
            if batch.message_count() == 0 {
                continue;
            }
            if ops.contains(&peek_op(entry)) {
                removed_messages = true;
                continue;
            }
            accumulate_committed_info(
                &mut retained_info,
                batch.header.base_offset,
                batch.header.base_timestamp,
                batch.header.total_size() as u64,
                batch.message_count(),
            );
            retained_next_offset = retained_next_offset.max(
                batch
                    .header
                    .base_offset
                    .saturating_add(u64::from(batch.message_count())),
            );
        }

        let active = self.log.active_segment();
        let active_size = active.size.as_bytes_u64();
        let durable_next_offset = if active_size == 0 {
            active.start_offset
        } else {
            active.end_offset.saturating_add(1)
        };
        let minimum_next_offset = durable_next_offset
            .max(retained_next_offset)
            .max(
                self.recovered_durable_offset
                    .map_or(0, |offset| offset.saturating_add(1)),
            )
            .max(self.installed_frontier.unwrap_or(0));
        let restored_position = active_size
            .checked_add(retained_info.size.as_bytes_u64())
            .ok_or(IggyError::CannotAppendMessage)?;
        let removed = self
            .log
            .journal()
            .inner
            .remove_range(ops.clone())
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;

        self.log.journal_mut().info = retained_info;
        self.log.active_segment_mut().current_position = restored_position;
        if removed_messages {
            let next_offset = minimum_next_offset;
            self.dirty_offset
                .store(next_offset.saturating_sub(1), Ordering::Relaxed);
            // The APPEND bit follows the rewound counter. The committed bit does
            // not: this drops an uncommitted suffix, which by definition names
            // nothing that ever committed, so raising it here would make a
            // truncation the event that publishes offsets no quorum agreed on.
            // A rewind to zero is the exception -- nothing is left at all.
            self.offset_space.append_live = next_offset > 0;
            if next_offset == 0 {
                self.offset_space.committed_seeded = false;
            }
        }
        self.consensus.note_journal_mutation();
        if *ops.start() <= self.recovered_commit {
            let lowered = ops.start().saturating_sub(1);
            error!(
                namespace_raw = self.namespace().inner(),
                from_op = *ops.start(),
                recovered_commit = self.recovered_commit,
                lowered,
                "cut partition entries the WAL recovered as committed; lowering the recovered \
                 commit point to {lowered} so repair refetches them"
            );
            self.recovered_commit = lowered;
        }
        self.pending_consumer_offset_commits
            .retain(|op, _| !ops.contains(op));
        // Nothing is in flight for a cut range, so the next sweep re-requests it
        // instead of waiting out the retry timer.
        if removed > 0
            && let Some(session) = self.repair.as_mut()
        {
            session.requested_from = None;
        }
        self.offset_reservations_need_resync.set(true);
        self.truncated_since_scan.set(true);
        drop(entries);
        if self.log.journal().inner.holds_no_ops() {
            self.reset_proof_walk_memory();
        }
        Ok(removed)
    }

    #[cfg(test)]
    async fn commit_messages(
        &mut self,
        config: &PartitionsConfig,
        through_op: u64,
    ) -> Result<bool, IggyError> {
        if std::mem::take(&mut self.injected_commit_failure) {
            return Err(IggyError::CannotSaveMessagesToSegment);
        }
        self.commit_messages_inner(
            config,
            self.consensus.replica_count() == 1 && self.durability().is_persisted(),
            through_op,
        )
        .await
    }

    /// Flush the committed journal prefix to segment storage regardless of
    /// the `messages_required_to_save` thresholds.
    ///
    /// Shutdown-path counterpart of the commit-time persist gate: a graceful
    /// stop must not lose committed messages that were still resident in the
    /// in-memory journal (consumer offsets are persisted eagerly, so losing
    /// the messages would fail recovery with an offset ahead of the data).
    ///
    /// Returns `false` while the mounted owner's flush is pending. The caller
    /// retries through its existing protocol state after completion service.
    ///
    /// # Errors
    ///
    /// Returns [`IggyError`] when writing the committed batches or their
    /// index entries to segment storage fails.
    pub async fn flush_committed_messages(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<bool, IggyError> {
        if self.has_io_dispatcher() {
            if self.fatal.is_some() {
                return Err(IggyError::CannotSyncFile);
            }
            if self.history_is_busy() {
                return Ok(false);
            }
            let through = self.consensus.commit_max();
            if self
                .maintenance
                .flush_completed_through
                .is_some_and(|completed| completed >= through)
                && self.materializable_prefix_len(through) == 0
            {
                return Ok(true);
            }
            self.request_flush(through);
            return Ok(false);
        }
        if let Some(persistence) = self
            .persistence
            .as_ref()
            .filter(|persistence| persistence.segment_checkpoint().is_some())
        {
            // Recovery only keeps segment bytes covered by the published WAL frontier.
            persistence.sync();
            self.start_persistence();
            // Shutdown and transfer must finish the flush, but never drain while
            // holding the lock used to append new messages.
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|error| {
                    warn!(%error, "cannot flush committed partition messages");
                    IggyError::CannotSyncFile
                })?;
        }
        if self
            .commit_messages_inner(config, true, self.consensus.commit_max())
            .await?
        {
            Ok(true)
        } else {
            Err(IggyError::CannotSyncFile)
        }
    }

    fn should_persist_messages(&self, config: &PartitionsConfig) -> bool {
        let journal_info = self.log.journal().info;
        // The existing thresholds include both committed and uncommitted batches.
        let messages_due = journal_info.messages_count > 0
            && (self.log.active_segment().is_full()
                || journal_info.messages_count >= self.effective_messages_required_to_save(config)
                || journal_info.size.as_bytes_u64()
                    >= self.effective_size_of_messages_required_to_save(config));
        messages_due || self.control_ops_due(config)
    }

    /// Consumer offset ops count toward no message threshold, yet the flush
    /// is the journal's only eviction, so they get the message-count bound of
    /// their own: without it a consume-only partition keeps one auto-commit
    /// op per poll resident until a producer happens to trip a flush.
    fn control_ops_due(&self, config: &PartitionsConfig) -> bool {
        self.log.journal().inner.resident_control_ops()
            >= self.effective_messages_required_to_save(config) as usize
    }

    /// Materialize inline for partitions without a dispatcher. Returns false
    /// while the captured prefix is pending in the WAL. While a view fence is up
    /// it flushes no further than `commit_min`.
    async fn commit_messages_inner(
        &mut self,
        config: &PartitionsConfig,
        force: bool,
        through_op: u64,
    ) -> Result<bool, IggyError> {
        if self.materialization.is_none() {
            if self.log.journal().inner.is_empty()
                || (!force && !self.should_persist_messages(config))
            {
                return Ok(true);
            }
            if !self.begin_materialization(
                through_op,
                self.effective_size_of_messages_required_to_save(config),
            )? {
                return Ok(false);
            }
            if self.materialization.is_none() {
                return Ok(true);
            }
        }
        loop {
            match self.drive_materialization()? {
                MaterializationStep::Progress => {}
                MaterializationStep::Pending => return Ok(false),
                MaterializationStep::Ready => {
                    if let Some(job) = self.capture_materialization(config)? {
                        let result = job.execute().await;
                        self.accept_materialization_phase(result)?;
                    }
                }
                MaterializationStep::Complete => return Ok(true),
            }
        }
    }

    fn begin_materialization(
        &mut self,
        through_op: u64,
        index_span_limit: u64,
    ) -> Result<bool, IggyError> {
        if self.materialization.is_some() {
            return Ok(true);
        }
        let mut through_op = self.consensus.commit_max().min(through_op);
        // Forced flushes (shutdown, transfer offers) reach here without the walk:
        // nothing above an unproven window may land in a segment.
        if self.view_fenced() {
            through_op = through_op.min(self.consensus.commit_min());
        }
        let prefix_len = self.materializable_prefix_len(through_op);
        if prefix_len == 0 {
            return Ok(true);
        }
        if !self.materialization_prefix_is_ready(prefix_len, through_op)? {
            return Ok(false);
        }
        self.materialization = Some(PendingMaterialization {
            index_span_limit,
            incarnation: self.incarnation,
            poll_history: self.poll_history,
            through_op,
            prefix_len,
            next_entry: 0,
            durable_offset: None,
            phase: MaterializationPhase::Inspect,
            save: None,
            checksum: None,
            digest: None,
        });
        Ok(true)
    }

    /// Front journal entries a flush through `through` may materialize. Repair
    /// backfill and a live tail during rejoin append below resident ops, so op
    /// order is restored first unless a materialization addresses entries by
    /// position. A commit's deferred eviction count survives the sort: it counts
    /// a written run that started at the lowest resident op, and anything that
    /// sorts ahead of it was absent then and lies at or below the commit point
    /// or the evicted ops, so it is on disk too.
    fn materializable_prefix_len(&self, through: u64) -> usize {
        let journal = &self.log.journal().inner;
        if self.materialization.is_none() && journal.sort_by_op() {
            // Sorting moves slots under a suffix snapshot, as eviction does.
            self.consensus.note_journal_mutation();
        }
        journal.committed_prefix_len(self.consensus.commit_min(), through)
    }

    fn drive_materialization(&mut self) -> Result<MaterializationStep, IggyError> {
        let pending = self
            .materialization
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?;
        if pending.incarnation != self.incarnation || pending.poll_history != self.poll_history {
            return Err(IggyError::CannotWriteToFile);
        }
        match pending.phase {
            MaterializationPhase::Save(_) | MaterializationPhase::Rotate(_) => {
                return Ok(MaterializationStep::Ready);
            }
            MaterializationPhase::Saving(_) | MaterializationPhase::Rotating => {
                return Ok(MaterializationStep::Pending);
            }
            MaterializationPhase::Inspect => {}
        }
        if !self.materialization_prefix_is_ready(pending.prefix_len, pending.through_op)? {
            return Ok(MaterializationStep::Pending);
        }
        let segment = self.log.active_segment();
        if segment.size.as_bytes_u64() >= segment.max_size.as_bytes_u64()
            || self.next_batch_rolls_by_age(pending.next_entry, pending.prefix_len)?
        {
            let next_offset = segment
                .end_offset
                .checked_add(1)
                .ok_or(IggyError::CannotWriteToFile)?;
            self.materialization
                .as_mut()
                .expect("materialization exists")
                .phase = MaterializationPhase::Rotate(next_offset);
            return Ok(MaterializationStep::Ready);
        }
        if pending.next_entry == pending.prefix_len {
            let finished = self.materialization.take().expect("materialization exists");
            if self
                .maintenance
                .flush_through
                .is_some_and(|through| through <= finished.through_op)
            {
                self.finish_flush();
            }
            self.evict_committed_prefix(finished.prefix_len);
            if self.pending_commit.is_none()
                && let Some(offset) = finished.durable_offset
            {
                self.note_committed_seeded();
                // A Replicated flush runs behind commits that already
                // published a higher offset.
                let visible = self.offset.fetch_max(offset, Ordering::AcqRel).max(offset);
                self.stats.set_current_offset(visible);
            }
            return Ok(MaterializationStep::Complete);
        }
        let chunk =
            self.plan_materialization_chunk(pending.next_entry, pending.prefix_len, None)?;
        if chunk.index.is_some()
            && self.log.index_writers().last().is_none_or(Option::is_none)
            && self
                .log
                .messages_writers()
                .last()
                .is_none_or(Option::is_none)
            && self
                .persistence
                .as_ref()
                .is_none_or(|persistence| persistence.segment_checkpoint().is_none())
        {
            let segment = self.log.active_segment_mut();
            segment.size = IggyByteSize::from(
                segment
                    .size
                    .as_bytes_u64()
                    .checked_add(chunk.info.size.as_bytes_u64())
                    .ok_or(IggyError::CannotWriteToFile)?,
            );
            self.accept_materialization_chunk(chunk)?;
            return Ok(MaterializationStep::Progress);
        }
        let piece = if chunk.allocation_charge > self.io_bytes_max.get() {
            self.plan_materialization_chunk(
                chunk.first_entry,
                chunk.end_entry,
                Some(self.io_bytes_max.get()),
            )?
        } else {
            chunk
        };
        let pending = self
            .materialization
            .as_mut()
            .expect("materialization exists");
        if chunk.index.is_none() {
            pending.next_entry = chunk.end_entry;
            return Ok(MaterializationStep::Progress);
        }
        pending.save = Some(PendingMaterializationSave { chunk, written: 0 });
        pending.phase = MaterializationPhase::Save(piece);
        Ok(MaterializationStep::Ready)
    }

    fn materialization_prefix_is_ready(
        &self,
        prefix_len: usize,
        through_op: u64,
    ) -> Result<bool, IggyError> {
        if let Some(persistence) = self
            .persistence
            .as_ref()
            .filter(|persistence| persistence.segment_checkpoint().is_some())
        {
            if persistence.failure().is_some() {
                return Err(IggyError::CannotSyncFile);
            }
            let last_send = self.log.journal().inner.with_entries(|entries| {
                entries
                    .iter()
                    .take(prefix_len)
                    .rev()
                    .find(|entry| peek_operation(entry) == Operation::SendMessages)
                    .map(peek_op)
            });
            if last_send.is_some_and(|op| op > through_op) {
                return Err(IggyError::CannotWriteToFile);
            }
            if last_send.is_some_and(|op| {
                if self.durability().is_persisted() {
                    !persistence.is_durable_through(op)
                } else {
                    !persistence.is_written_through(op)
                }
            }) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// The last offset the active segment, or a recovered durable tail, already
    /// holds. Batches at or below it are on disk and never materialized again.
    fn materialization_persisted_end(&self) -> Option<u64> {
        let segment = self.log.active_segment();
        if segment.size.as_bytes_u64() == 0 {
            segment.start_offset.checked_sub(1)
        } else {
            Some(segment.end_offset)
        }
        .max(self.recovered_durable_offset)
    }

    /// Whether the next batch the prefix still has to materialize is stamped
    /// `segment_max_age` or more past the non-empty active segment's first
    /// batch, so it starts the next segment, as the WAL decides at prepare.
    fn next_batch_rolls_by_age(
        &self,
        first_entry: usize,
        prefix_len: usize,
    ) -> Result<bool, IggyError> {
        let max_age = self.effective_segment_max_age();
        let segment = self.log.active_segment();
        if max_age.is_none() || segment.size.as_bytes_u64() == 0 {
            return Ok(false);
        }
        let persisted_end = self.materialization_persisted_end();
        self.log.journal().inner.with_entries(|entries| {
            let entries = (first_entry <= prefix_len && prefix_len <= entries.len())
                .then(|| entries.range(first_entry..prefix_len))
                .ok_or(IggyError::CannotWriteToFile)?;
            for entry in entries {
                if let Some(meta) = materialization_batch_meta(entry, persisted_end)? {
                    return Ok(rolls_by_age(
                        max_age,
                        segment.start_timestamp,
                        meta.base_timestamp,
                    ));
                }
            }
            Ok(false)
        })
    }

    fn plan_materialization_chunk(
        &self,
        first_entry: usize,
        prefix_len: usize,
        bytes_max: Option<usize>,
    ) -> Result<MaterializationChunk, IggyError> {
        // TODO: Validate reference-WAL batches by borrowing, then reserve only
        // retained index state. Coalesce adjacent index spans within that budget.
        let segment = self.log.active_segment();
        let position = segment.size.as_bytes_u64();
        let persisted_end = self.materialization_persisted_end();
        let max_age = self.effective_segment_max_age();
        let mut chunk = MaterializationChunk {
            first_entry,
            end_entry: first_entry,
            persisted_end,
            buffers: 0,
            allocation_charge: 0,
            index: None,
            info: JournalInfo::default(),
        };
        // Reference-WAL bodies can run ahead while an index job waits. Bound
        // each index's span without writing those bodies a second time.
        let index_span_limit = self
            .persistence
            .as_ref()
            .filter(|persistence| persistence.segment_checkpoint().is_some())
            .and_then(|_| {
                self.materialization
                    .as_ref()
                    .map(|pending| pending.index_span_limit)
            });
        self.log.journal().inner.with_entries(|entries| {
            let entries = (first_entry <= prefix_len && prefix_len <= entries.len())
                .then(|| entries.range(first_entry..prefix_len))
                .ok_or(IggyError::CannotWriteToFile)?;
            let mut backing = 0_usize;
            let mut file_position = position;
            for entry in entries {
                let Some(meta) = materialization_batch_meta(entry, persisted_end)? else {
                    chunk.end_entry += 1;
                    continue;
                };
                // The batch that ages the segment out starts the next one, which
                // the drive rotates into before planning it.
                let segment_first_timestamp = if position > 0 {
                    Some(segment.start_timestamp)
                } else {
                    chunk.index.as_ref().map(|index| index.timestamp)
                };
                if segment_first_timestamp
                    .is_some_and(|first| rolls_by_age(max_age, first, meta.base_timestamp))
                {
                    break;
                }
                let next_backing = backing
                    .checked_add(
                        Frozen::<4096>::allocation_size(entry.backing_capacity())
                            .ok_or(IggyError::CannotWriteToFile)?,
                    )
                    .ok_or(IggyError::CannotWriteToFile)?;
                let next_charge = crate::io::materialization_charge(
                    next_backing,
                    chunk.buffers + 1,
                    crate::iggy_index::IGGY_INDEX_SIZE,
                )
                .ok_or(IggyError::CannotWriteToFile)?;
                if bytes_max.is_some_and(|maximum| next_charge > maximum) {
                    if chunk.buffers == 0 {
                        return Err(IggyError::CannotWriteToFile);
                    }
                    break;
                }
                chunk.end_entry += 1;
                if chunk.index.is_none() {
                    chunk.index = Some(crate::iggy_index::IggyIndex::new(
                        meta.base_offset,
                        meta.base_timestamp,
                        file_position,
                    ));
                }
                file_position = file_position
                    .checked_add(meta.total_size)
                    .ok_or(IggyError::CannotWriteToFile)?;
                chunk.buffers += 1;
                backing = next_backing;
                accumulate_committed_info(
                    &mut chunk.info,
                    meta.base_offset,
                    meta.base_timestamp,
                    meta.total_size,
                    meta.message_count,
                );
                if file_position >= segment.max_size.as_bytes_u64()
                    || index_span_limit
                        .is_some_and(|maximum| chunk.info.size.as_bytes_u64() >= maximum)
                {
                    break;
                }
            }
            chunk.allocation_charge = crate::io::materialization_charge(
                backing,
                chunk.buffers,
                crate::iggy_index::IGGY_INDEX_SIZE,
            )
            .ok_or(IggyError::CannotWriteToFile)?;
            Ok(chunk)
        })
    }

    fn capture_materialization(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Option<crate::io::PartitionIoJob<SB>>, IggyError> {
        let phase = self
            .materialization
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?
            .phase;
        match phase {
            MaterializationPhase::Save(chunk) => {
                let save = self
                    .materialization
                    .as_ref()
                    .and_then(|pending| pending.save)
                    .ok_or(IggyError::CannotWriteToFile)?;
                let completes = chunk.end_entry == save.chunk.end_entry;
                let batches = self.log.journal().inner.with_entries(|entries| {
                    let entries = (chunk.first_entry <= chunk.end_entry
                        && chunk.end_entry <= entries.len())
                    .then(|| entries.range(chunk.first_entry..chunk.end_entry))
                    .ok_or(IggyError::CannotWriteToFile)?;
                    let mut batches = Vec::with_capacity(chunk.buffers);
                    for entry in entries {
                        if materialization_batch_meta(entry, chunk.persisted_end)?.is_some() {
                            batches.push(entry.clone());
                        }
                    }
                    Ok::<_, IggyError>(batches)
                })?;
                let index = save.chunk.index.ok_or(IggyError::CannotWriteToFile)?;
                let index_bytes = if completes {
                    crate::iggy_index::IggyIndexCache::serialize(&index)
                } else {
                    Vec::new()
                };
                let job = self.prepare_materialization(
                    batches,
                    index_bytes,
                    chunk.info.messages_count,
                    save.written,
                    completes,
                )?;
                if let Some(job) = job {
                    if job.allocation_charge() > chunk.allocation_charge {
                        return Err(IggyError::CannotWriteToFile);
                    }
                    self.materialization
                        .as_mut()
                        .expect("materialization exists")
                        .phase = MaterializationPhase::Saving(chunk);
                    Ok(Some(crate::io::PartitionIoJob::Materialize(job)))
                } else {
                    self.accept_materialization_chunk(chunk)?;
                    Ok(None)
                }
            }
            MaterializationPhase::Rotate(start_offset) => {
                let (target, job) = self.prepare_rotation(config, start_offset);
                self.materialization
                    .as_mut()
                    .expect("materialization exists")
                    .phase = MaterializationPhase::Rotating;
                Ok(Some(crate::io::PartitionIoJob::Rotate { target, job }))
            }
            _ => Err(IggyError::CannotWriteToFile),
        }
    }

    fn accept_materialization_phase(
        &mut self,
        result: crate::io::PartitionIoResult,
    ) -> Result<(), IggyError> {
        let phase = self
            .materialization
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?
            .phase;
        match (phase, result) {
            (
                MaterializationPhase::Saving(chunk),
                crate::io::PartitionIoResult::Materialize(result),
            ) => {
                if let Err(error) = self.accept_materialization(result) {
                    self.materialization
                        .as_mut()
                        .expect("materialization exists")
                        .phase = MaterializationPhase::Save(chunk);
                    return Err(error);
                }
                let save = self
                    .materialization
                    .as_ref()
                    .and_then(|pending| pending.save)
                    .ok_or(IggyError::CannotWriteToFile)?;
                if chunk.end_entry < save.chunk.end_entry {
                    let written = save
                        .written
                        .checked_add(chunk.info.size.as_bytes_u64())
                        .ok_or(IggyError::CannotWriteToFile)?;
                    let next = self.plan_materialization_chunk(
                        chunk.end_entry,
                        save.chunk.end_entry,
                        Some(self.io_bytes_max.get()),
                    )?;
                    let pending = self
                        .materialization
                        .as_mut()
                        .expect("materialization exists");
                    pending.save = Some(PendingMaterializationSave {
                        chunk: save.chunk,
                        written,
                    });
                    pending.phase = MaterializationPhase::Save(next);
                    Ok(())
                } else {
                    self.materialization
                        .as_mut()
                        .expect("materialization exists")
                        .save = None;
                    self.accept_materialization_chunk(save.chunk)
                }
            }
            (
                MaterializationPhase::Rotating,
                crate::io::PartitionIoResult::Rotate { target, outcome },
            ) => {
                let start_offset = target
                    .segment_end
                    .checked_add(1)
                    .ok_or(IggyError::CannotWriteToFile)?;
                match outcome {
                    Ok(installed) => self.accept_rotation(&target, installed)?,
                    Err(error) => {
                        self.materialization
                            .as_mut()
                            .expect("materialization exists")
                            .phase = MaterializationPhase::Rotate(start_offset);
                        return Err(error);
                    }
                }
                self.materialization
                    .as_mut()
                    .expect("materialization exists")
                    .phase = MaterializationPhase::Inspect;
                Ok(())
            }
            _ => Err(IggyError::CannotWriteToFile),
        }
    }

    fn accept_materialization_chunk(
        &mut self,
        chunk: MaterializationChunk,
    ) -> Result<(), IggyError> {
        let index = chunk.index.ok_or(IggyError::CannotWriteToFile)?;
        if let Some(persistence) = &self.persistence {
            persistence.mark_segment_dirty(self.log.active_segment().start_offset);
        }
        self.log.ensure_indexes();
        self.log
            .active_indexes_mut()
            .ok_or(IggyError::CannotWriteToFile)?
            .insert(index.offset, index.timestamp, index.position);
        let segment = self.log.active_segment_mut();
        if segment.start_timestamp == 0 && chunk.info.first_timestamp != 0 {
            segment.start_timestamp = chunk.info.first_timestamp;
        }
        segment.end_timestamp = chunk.info.end_timestamp;
        segment.max_timestamp = segment.max_timestamp.max(chunk.info.max_timestamp);
        segment.end_offset = chunk.info.current_offset;
        let pending = self
            .materialization
            .as_mut()
            .ok_or(IggyError::CannotWriteToFile)?;
        pending.next_entry = chunk.end_entry;
        pending.durable_offset = Some(chunk.info.current_offset);
        pending.phase = MaterializationPhase::Inspect;
        Ok(())
    }

    /// Evict the committed prefix (the `count` front entries read by
    /// `committed_prefix_len`) and set the message count and size in
    /// `journal.info` to the batches left resident, so the next persist
    /// threshold counts that tail alone. Call once the prefix is durable, or
    /// when there is nothing to persist.
    fn evict_committed_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        if let Some(pending) = &mut self.pending_commit {
            pending.evictable = pending.evictable.max(count);
            return;
        }
        let persistence = self.persistence.as_deref();
        let journal = &self.log.journal().inner;
        journal.evict_prefix_located(count, |op| {
            persistence?.segment_reference(op).map(BodyPosition::from)
        });
        // Every retained entry moves `count` slots towards the front, so the
        // slots a suffix snapshot describes move under it without the head or
        // the commit point changing.
        self.consensus.note_journal_mutation();
        // Appends reject a batch that overflows the count, so it fits.
        let messages_count = u32::try_from(journal.resident_messages()).unwrap_or(u32::MAX);
        let size = IggyByteSize::from(journal.resident_message_size());
        let info = &mut self.log.journal_mut().info;
        info.messages_count = messages_count;
        info.size = size;
    }

    async fn handle_committed_entries(
        &mut self,
        headers: Vec<PrepareHeader>,
        config: &PartitionsConfig,
        send_client_replies: bool,
    ) {
        let mut just_selected = self.pending_commit.is_none();
        if just_selected && !self.select_commit(headers, config, send_client_replies) {
            return;
        }
        loop {
            let step = if std::mem::take(&mut just_selected) {
                self.drive_validated_commit()
            } else {
                self.drive_commit()
            };
            let step = match step {
                Ok(step) => step,
                Err(error) => {
                    self.fail_pending_commit(&error);
                    return;
                }
            };
            match step {
                CommitStep::Progress => {}
                CommitStep::Pending => return,
                CommitStep::Ready => {
                    if self.has_io_dispatcher() {
                        self.notify_io();
                        return;
                    }
                    let job = match self.capture_commit_phase(config) {
                        Ok(job) => job,
                        Err(error) => {
                            self.fail_pending_commit(&error);
                            return;
                        }
                    };
                    if let Some(job) = job
                        && let Err(error) = self.accept_commit_phase(job.execute().await)
                    {
                        self.fail_pending_commit(&error);
                        return;
                    }
                }
                CommitStep::Publish => {
                    self.finish_pending_commit().await;
                    return;
                }
            }
        }
    }

    fn drive_commit(&mut self) -> Result<CommitStep, IggyError> {
        if !self.pending_commit_is_current() {
            return Err(IggyError::CannotWriteToFile);
        }
        self.drive_validated_commit()
    }

    fn drive_validated_commit(&mut self) -> Result<CommitStep, IggyError> {
        // Selection bounds the commit run. Revalidate after yielding or I/O,
        // while CPU-only phases can share this validation in the same turn.
        loop {
            let pending = self
                .pending_commit
                .as_ref()
                .ok_or(IggyError::CannotWriteToFile)?;
            let step = match pending.phase {
                CommitPhase::Start => self.start_commit_materialization(),
                CommitPhase::Materialize => match self.drive_materialization()? {
                    MaterializationStep::Progress => Ok(CommitStep::Progress),
                    MaterializationStep::Pending => Ok(CommitStep::Pending),
                    MaterializationStep::Ready => Ok(CommitStep::Ready),
                    MaterializationStep::Complete => {
                        let pending = self
                            .pending_commit
                            .as_mut()
                            .expect("selected commit exists");

                        pending.phase = CommitPhase::Offsets;
                        Ok(CommitStep::Progress)
                    }
                },
                CommitPhase::Offsets => self.drive_committed_offsets(),
                CommitPhase::OffsetReady(plan)
                    if self.offset_effect_waits_for_checkpoint(&plan) =>
                {
                    Ok(CommitStep::Pending)
                }
                CommitPhase::OffsetReady(_) => Ok(CommitStep::Ready),
                CommitPhase::SegmentDirectory => {
                    if (pending.has_sends() || pending.materialize)
                        && self.consensus.replica_count() == 1
                        && self.durability().is_persisted()
                        && self.segment_names_dirty.get()
                        && self.partition_dir.is_some()
                    {
                        return Ok(CommitStep::Ready);
                    }
                    self.pending_commit
                        .as_mut()
                        .expect("selected commit exists")
                        .phase = CommitPhase::OffsetDirectories;
                    Ok(CommitStep::Progress)
                }
                CommitPhase::OffsetDirectories => {
                    let dirty = self
                        .consumer_offset_dirs_touched
                        .iter()
                        .zip(&self.consumer_offset_dirs_dirty)
                        .any(|(touched, dirty)| touched.get().is_some() && dirty.get());
                    if dirty {
                        return Ok(CommitStep::Ready);
                    }
                    self.pending_commit
                        .as_mut()
                        .expect("selected commit exists")
                        .phase = CommitPhase::Publish;
                    Ok(CommitStep::Publish)
                }
                CommitPhase::Publish => Ok(CommitStep::Publish),
                CommitPhase::OffsetSubmitted(_)
                | CommitPhase::SegmentDirectorySubmitted
                | CommitPhase::OffsetDirectoriesSubmitted => Ok(CommitStep::Pending),
            }?;
            if !matches!(step, CommitStep::Progress) {
                return Ok(step);
            }
        }
    }

    fn start_commit_materialization(&mut self) -> Result<CommitStep, IggyError> {
        let pending = self
            .pending_commit
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?;

        #[cfg(any(test, feature = "fault-injection"))]
        if (pending.has_sends() || pending.materialize)
            && std::mem::take(&mut self.injected_commit_failure)
        {
            return Err(IggyError::CannotSaveMessagesToSegment);
        }
        let has_sends = pending.has_sends();
        if pending.materialize {
            let through_op = pending
                .headers
                .last()
                .ok_or(IggyError::CannotWriteToFile)?
                .op;
            if !self.begin_materialization(through_op, pending.index_span_limit)? {
                if has_sends {
                    return Ok(CommitStep::Pending);
                }
                self.defer_flush(through_op);
            }
            if self.materialization.is_some() {
                self.pending_commit
                    .as_mut()
                    .expect("selected commit exists")
                    .phase = CommitPhase::Materialize;
                return Ok(CommitStep::Progress);
            }
        }
        let pending = self
            .pending_commit
            .as_mut()
            .expect("selected commit exists");

        pending.phase = CommitPhase::Offsets;
        Ok(CommitStep::Progress)
    }

    fn drive_committed_offsets(&mut self) -> Result<CommitStep, IggyError> {
        let pending = self
            .pending_commit
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?;
        let Some(header) = pending.headers.get(pending.next_entry).copied() else {
            self.pending_commit
                .as_mut()
                .expect("selected commit exists")
                .phase = CommitPhase::SegmentDirectory;
            return Ok(CommitStep::Progress);
        };
        if header.operation.is_partition_lifecycle() {
            self.apply_committed_lifecycle(header)?;
        }
        let pending = self
            .pending_commit
            .as_ref()
            .expect("selected commit exists");
        if !matches!(
            header.operation,
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
        ) {
            self.pending_commit
                .as_mut()
                .expect("selected commit exists")
                .next_entry += 1;
            return Ok(CommitStep::Progress);
        }
        let plan = self.plan_committed_offset(&header, pending)?;
        if let ConsumerOffsetAction::Ready(effect) = plan.action {
            self.accept_committed_offset_effect(&plan, effect)?;
        } else {
            let mut batch = Vec::new();
            // A batch writes concurrently, so a store that retires the marker
            // runs alone to keep the removal ahead of every write.
            if matches!(plan.action, ConsumerOffsetAction::Write { .. })
                && plan.emptied_at.is_none()
            {
                for (entry, header) in pending
                    .headers
                    .iter()
                    .enumerate()
                    .skip(pending.next_entry + 1)
                {
                    if batch.len() + 1 >= crate::state_transfer::OFFSET_PERSIST_CONCURRENCY {
                        break;
                    }
                    match header.operation {
                        Operation::StoreConsumerOffset => {}
                        Operation::DeleteConsumerOffset => break,
                        _ => continue,
                    }
                    let next = self.plan_committed_offset(header, pending)?;
                    if !matches!(next.action, ConsumerOffsetAction::Write { .. })
                        || std::iter::once(&plan)
                            .chain(batch.iter().map(|(_, plan)| plan))
                            .any(|earlier| {
                                earlier.pending.kind == next.pending.kind
                                    && earlier.pending.consumer_id == next.pending.consumer_id
                            })
                    {
                        break;
                    }
                    batch.push((entry, next));
                }
            }
            let pending = self
                .pending_commit
                .as_mut()
                .expect("selected commit exists");
            pending.offset_batch = batch;
            pending.phase = CommitPhase::OffsetReady(plan);
        }
        Ok(CommitStep::Progress)
    }

    fn apply_committed_lifecycle(&mut self, header: PrepareHeader) -> Result<(), IggyError> {
        let prepare = self
            .log
            .journal()
            .inner
            .repair_entry(header.op)
            .ok_or(IggyError::InvalidCommand)?;
        let body = prepare
            .as_slice()
            .get(iggy_binary_protocol::HEADER_SIZE..)
            .ok_or(IggyError::InvalidCommand)?;
        match PartitionLifecycle::decode(header.operation, body)? {
            PartitionLifecycle::Owner(installation) => {
                self.apply_consumer_group_owner(installation, header.op)?;
                if let Some(pending) = self.pending_commit.as_mut() {
                    pending.owner_install = Some(installation);
                }
            }
            PartitionLifecycle::History(transition) => {
                self.apply_history_transition(transition, header.op)?;
            }
            PartitionLifecycle::OwnerRetirement(catalog) => {
                self.apply_consumer_group_owner_retirement(catalog)?;
            }
        }
        Ok(())
    }

    fn plan_committed_offset(
        &self,
        header: &PrepareHeader,
        pending: &PendingCommit,
    ) -> Result<ConsumerOffsetPlan, IggyError> {
        let offset = self
            .pending_consumer_offset_commits
            .get(&header.op)
            .copied()
            .map_or_else(|| self.restage_consumer_offset_from_journal(header.op), Ok)?;
        if !self.matches_consumer_owner(
            offset.kind,
            offset.consumer_id,
            header.client,
            header.session,
            offset.owner_generation,
            header.operation,
        ) {
            return Err(IggyError::InvalidCommand);
        }
        let tracked = pending
            .offset_effects
            .iter()
            .rev()
            .find(|effect| {
                effect.pending.kind == offset.kind
                    && effect.pending.consumer_id == offset.consumer_id
            })
            .map_or_else(
                || {
                    self.durable_consumer_offsets
                        .get(offset.kind, offset.consumer_id)
                },
                |effect| effect.state,
            );
        let emptied_at = pending.emptied_at.unwrap_or(pending.initial_emptied_at);
        let mut plan = self.plan_consumer_offset_effect(offset, tracked, emptied_at);
        if matches!(offset.mutation, PendingConsumerOffsetMutation::Delete) {
            // Only a delete of the last key the table holds empties it. An
            // absent key may be one whose file boot lost, and a stranded file
            // may hold an offset the table never loaded.
            //
            // Accepted limit: a donor whose boot lost offsets (one file, every
            // file or a kind directory) still marks the table emptied when it
            // deletes the last key it holds, even one stored after the boot.
            // A receiver that missed that delete then drops the lost keys too.
            // A non-empty offer from that donor already drops them the same way.
            plan.emptied_at = (emptied_at == 0
                && tracked.is_some()
                && pending.offset_count == 1
                && ConsumerKind::ALL
                    .into_iter()
                    .all(|kind| self.consumer_offset_capacity_for(kind).stranded_count() == 0))
            .then_some(header.op);
        }
        Ok(plan)
    }

    fn capture_commit_phase(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<Option<crate::io::PartitionIoJob<SB>>, IggyError> {
        let phase = self
            .pending_commit
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?
            .phase;
        let (job, submitted) = match phase {
            CommitPhase::Materialize => return self.capture_materialization(config),
            CommitPhase::OffsetReady(plan) => {
                let first = self.capture_consumer_offset_effect(&plan)?;
                let batch = &self
                    .pending_commit
                    .as_ref()
                    .expect("selected commit exists")
                    .offset_batch;
                let job = if batch.is_empty() {
                    first
                } else {
                    let crate::io::PartitionIoJob::OffsetWrite(first) = first else {
                        return Err(IggyError::CannotWriteToFile);
                    };
                    let mut jobs = Vec::with_capacity(batch.len() + 1);
                    jobs.push(first);
                    for (_, plan) in batch {
                        let crate::io::PartitionIoJob::OffsetWrite(job) =
                            self.capture_consumer_offset_effect(plan)?
                        else {
                            return Err(IggyError::CannotWriteToFile);
                        };
                        jobs.push(job);
                    }
                    crate::io::PartitionIoJob::OffsetBatch(jobs)
                };
                (job, CommitPhase::OffsetSubmitted(plan))
            }
            CommitPhase::SegmentDirectory => (
                crate::io::PartitionIoJob::SegmentDirectory(
                    self.partition_dir
                        .clone()
                        .ok_or(IggyError::CannotSyncFile)?,
                ),
                CommitPhase::SegmentDirectorySubmitted,
            ),
            CommitPhase::OffsetDirectories => (
                crate::io::PartitionIoJob::OffsetDirectories(
                    self.prepare_consumer_offset_directories(
                        self.consumer_offset_dirs_touched
                            .each_ref()
                            .map(|touched| touched.get().is_some()),
                    ),
                ),
                CommitPhase::OffsetDirectoriesSubmitted,
            ),
            _ => return Err(IggyError::CannotWriteToFile),
        };
        self.pending_commit
            .as_mut()
            .expect("selected commit exists")
            .phase = submitted;
        Ok(Some(job))
    }

    fn accept_commit_phase(
        &mut self,
        result: crate::io::PartitionIoResult,
    ) -> Result<(), IggyError> {
        let phase = self
            .pending_commit
            .as_ref()
            .ok_or(IggyError::CannotWriteToFile)?
            .phase;
        match (phase, result) {
            (CommitPhase::Materialize, result) => self.accept_materialization_phase(result),
            (CommitPhase::OffsetSubmitted(plan), result) => {
                let pending = self
                    .pending_commit
                    .as_mut()
                    .expect("selected commit exists");
                pending.phase = CommitPhase::OffsetReady(plan);
                if let crate::io::PartitionIoResult::OffsetBatch(results) = result {
                    let batch = std::mem::take(&mut pending.offset_batch);
                    if results.len() != batch.len() + 1 {
                        return Err(IggyError::CannotWriteToFile);
                    }
                    let first_entry = pending.next_entry;
                    let mut first_error = None;
                    for ((entry, plan), result) in std::iter::once((first_entry, plan))
                        .chain(batch)
                        .zip(results)
                    {
                        self.pending_commit
                            .as_mut()
                            .expect("selected commit exists")
                            .next_entry = entry;
                        let outcome = self
                            .accept_consumer_offset_effect(
                                plan,
                                crate::io::PartitionIoResult::OffsetWrite(result),
                            )
                            .and_then(|effect| self.accept_committed_offset_effect(&plan, effect));
                        if let Err(error) = outcome {
                            first_error.get_or_insert((entry, error));
                        }
                    }
                    if let Some((entry, error)) = first_error {
                        self.pending_commit
                            .as_mut()
                            .expect("selected commit exists")
                            .next_entry = entry;
                        Err(error)
                    } else {
                        Ok(())
                    }
                } else {
                    let effect = self.accept_consumer_offset_effect(plan, result)?;
                    self.accept_committed_offset_effect(&plan, effect)
                }
            }
            (
                CommitPhase::SegmentDirectorySubmitted,
                crate::io::PartitionIoResult::SegmentDirectory(outcome),
            ) => {
                self.pending_commit
                    .as_mut()
                    .expect("selected commit exists")
                    .phase = CommitPhase::SegmentDirectory;
                outcome.map_err(|_| IggyError::CannotSyncFile)?;
                self.segment_names_dirty.set(false);
                self.pending_commit
                    .as_mut()
                    .expect("selected commit exists")
                    .phase = CommitPhase::OffsetDirectories;
                Ok(())
            }
            (
                CommitPhase::OffsetDirectoriesSubmitted,
                crate::io::PartitionIoResult::OffsetDirectories(result),
            ) => {
                self.pending_commit
                    .as_mut()
                    .expect("selected commit exists")
                    .phase = CommitPhase::OffsetDirectories;
                self.accept_consumer_offset_directories(&result);
                if let Some((op, operation)) = result
                    .failed
                    .iter()
                    .zip(&self.consumer_offset_dirs_touched)
                    .find_map(|(failed, touched)| if *failed { touched.get() } else { None })
                {
                    self.fatal = Some(FatalCommit {
                        namespace_raw: self.namespace().inner(),
                        op,
                        operation: Some(operation),
                    });
                    return Err(IggyError::CannotSyncFile);
                }
                self.pending_commit
                    .as_mut()
                    .expect("selected commit exists")
                    .phase = CommitPhase::Publish;
                Ok(())
            }
            _ => Err(IggyError::CannotWriteToFile),
        }
    }

    fn accept_committed_offset_effect(
        &mut self,
        plan: &ConsumerOffsetPlan,
        effect: ConsumerOffsetEffect,
    ) -> Result<(), IggyError> {
        let pending = self
            .pending_commit
            .as_mut()
            .ok_or(IggyError::CannotWriteToFile)?;
        let header = pending
            .headers
            .get(pending.next_entry)
            .ok_or(IggyError::CannotWriteToFile)?;
        if let Some(persistence) = &self.persistence {
            persistence.mark_offset_dirty(
                effect.pending.kind.index(),
                effect.pending.consumer_id,
                effect.state.is_some(),
            );
        }
        self.consumer_offset_dirs_touched[effect.pending.kind.index()]
            .set(Some((header.op, header.operation)));
        pending.offset_count = (pending.offset_count + usize::from(effect.state.is_some()))
            .saturating_sub(usize::from(plan.tracked.is_some()));
        if let Some(emptied_at) = plan.emptied_at {
            pending.emptied_at = Some(emptied_at);
        }
        pending.offset_effects.push(effect);
        pending.next_entry += 1;
        pending.phase = CommitPhase::Offsets;
        Ok(())
    }

    fn fail_pending_commit(&mut self, error: &IggyError) {
        warn!(namespace_raw = self.namespace().inner(), %error, "committed partition phase failed");
        if self.fatal.is_none()
            && let Some(pending) = &self.pending_commit
            && let Some(header) = pending
                .headers
                .get(pending.next_entry)
                .or_else(|| pending.headers.last())
        {
            self.fence_failed_commit(*header);
        }
    }

    fn select_commit(
        &mut self,
        mut headers: Vec<PrepareHeader>,
        config: &PartitionsConfig,
        send_client_replies: bool,
    ) -> bool {
        // Lifecycle fences apply between complete commit walks, so owner changes
        // cannot overtake offsets.
        if let Some(index) = headers
            .iter()
            .position(|header| header.operation.is_partition_lifecycle())
        {
            headers.truncate(index.max(1));
        }
        let applied_floor = self.consensus.commit_min();
        if self.transition.is_some()
            || self.commit_waits_for_checkpoint()
            || matches!(*self.transfer_plan.borrow(), Some(TransferPlanSlot::Wanted))
            || self.consensus.is_transferring()
            || self.io_active.is_retiring()
            || headers
                .first()
                .is_none_or(|first| Some(first.op) != applied_floor.checked_add(1))
            || headers
                .windows(2)
                .any(|pair| pair[0].op.checked_add(1) != Some(pair[1].op))
            || headers
                .iter()
                .any(|header| !self.consensus.view_log_entry_proven(header.op))
        {
            return false;
        }
        let from_pipeline = self.consensus.pipeline_head_header().as_ref() == headers.first();
        let journal_headers = self
            .log
            .journal()
            .inner
            .headers_for_commit(headers[0].op, headers.len());
        let batch_stats = self.resolve_committed_batch_stats(&headers);
        let Some(retirements) = self.validate_committed_entries(&headers, &batch_stats) else {
            return false;
        };
        let has_sends = headers
            .iter()
            .any(|header| header.operation == Operation::SendMessages);
        let flush_due = if has_sends {
            self.should_persist_messages(config)
                || (self.consensus.replica_count() == 1 && self.durability().is_persisted())
        } else {
            self.control_ops_due(config)
        };
        // Replicated sends defer message materialization to maintenance. A
        // commit run containing offset writes still needs the partition's slot
        // before it can publish its replies.
        let materialize =
            flush_due && (self.durability().is_persisted() || !self.has_io_dispatcher());
        if flush_due
            && !materialize
            && let Some(last) = headers.last()
        {
            self.defer_flush(last.op);
            self.notify_io();
        }
        self.pending_commit = Some(PendingCommit {
            headers,
            journal_headers,
            batch_stats,
            retirements,
            owner_install: None,
            offset_effects: Vec::new(),
            offset_batch: Vec::new(),
            offset_count: ConsumerKind::ALL
                .into_iter()
                .map(|kind| self.durable_consumer_offsets.count(kind))
                .sum(),
            initial_emptied_at: self.durable_consumer_offsets.emptied_at(),
            emptied_at: None,
            incarnation: self.incarnation,
            poll_history: self.poll_history,
            applied_floor,
            from_pipeline,
            send_client_replies,
            materialize,
            index_span_limit: self.effective_size_of_messages_required_to_save(config),
            phase: CommitPhase::Start,
            next_entry: 0,
            evictable: 0,
        });
        for touched in &self.consumer_offset_dirs_touched {
            touched.set(None);
        }
        true
    }

    fn pending_commit_is_current(&self) -> bool {
        let Some(pending) = &self.pending_commit else {
            return false;
        };
        if pending.incarnation != self.incarnation
            || pending.poll_history != self.poll_history
            || pending.applied_floor != self.consensus.commit_min()
        {
            return false;
        }
        if !self
            .log
            .journal()
            .inner
            .commit_headers_match(pending.headers[0].op, &pending.journal_headers)
        {
            return false;
        }
        !pending.from_pipeline
            || self.consensus.with_pipeline(|pipeline| {
                pending.headers.iter().all(|header| {
                    pipeline
                        .entry_by_op(header.op)
                        .is_some_and(|entry| entry.header == *header)
                })
            })
    }

    fn fence_failed_commit(&mut self, header: PrepareHeader) {
        error!(
            namespace_raw = self.namespace().inner(),
            op = header.op, operation = ?header.operation,
            "partition local commit failed for a cluster-committed operation"
        );
        self.fatal = Some(FatalCommit {
            namespace_raw: self.namespace().inner(),
            op: header.op,
            operation: Some(header.operation),
        });
    }

    #[allow(clippy::too_many_lines)]
    fn publish_pending_commit(&mut self) -> Option<Vec<(PipelineEntry, Option<CachedReply>)>> {
        let pending = self.pending_commit.take()?;
        let entries = if pending.from_pipeline {
            self.consensus.with_pipeline_mut(|pipeline| {
                let mut entries = Vec::with_capacity(pending.headers.len());
                for _ in &pending.headers {
                    entries.push(pipeline.pop()?);
                }
                Some(entries)
            })
        } else {
            Some(
                pending
                    .headers
                    .iter()
                    .copied()
                    .map(PipelineEntry::new)
                    .collect(),
            )
        };
        let Some(entries) = entries else {
            self.fence_failed_commit(*pending.headers.first()?);
            return None;
        };

        for effect in &pending.offset_effects {
            self.publish_consumer_offset_effect(*effect);
            self.apply_consumer_offset_commit(effect.pending);
        }
        if let Some(emptied_at) = pending.emptied_at {
            self.durable_consumer_offsets.set_emptied_at(emptied_at);
        }
        let mut retirements = pending.retirements.into_iter().peekable();
        let mut committed = Vec::with_capacity(entries.len());
        let mut offset_keys = HashSet::new();
        for ((entry, header), batch_stats) in entries
            .into_iter()
            .zip(&pending.headers)
            .zip(&pending.batch_stats)
        {
            if let Some(batch_stats) = *batch_stats {
                self.publish_committed_batch_stats(batch_stats);
            }
            if let Some(offset) = self.pending_consumer_offset_commits.remove(&header.op) {
                offset_keys.insert((offset.kind, offset.consumer_id));
            }
            if let Some((_, retirement)) = retirements.next_if(|(op, _)| *op == header.op) {
                for identity in retirement.identities {
                    self.dedup
                        .forget_session(identity.client_id, identity.session);
                    self.required_metadata_frontier = self
                        .required_metadata_frontier
                        .max(identity.metadata_watermark);
                }
                // A finalized session absent from the slice still must not
                // start writing here, so the frontier covers the whole debt.
                self.retirement_paid_through =
                    self.retirement_paid_through.max(retirement.paid_through);
                self.required_metadata_frontier =
                    self.required_metadata_frontier.max(retirement.paid_through);
            }
            let reply = match self.commit_receipt(
                &entry,
                header,
                *batch_stats,
                pending.send_client_replies,
            ) {
                Ok(reply) => reply,
                Err(error) => {
                    error!(
                        namespace_raw = self.namespace().inner(),
                        op = header.op,
                        %error,
                        "cannot retain committed partition receipt"
                    );
                    self.fence_failed_commit(*header);
                    return None;
                }
            };
            // After the receipt, so a commit that fences never leaves the
            // machines applied past `commit_min`.
            if header.operation == Operation::StateMachineCommand
                && !self.apply_committed_command(header)
            {
                return None;
            }
            self.consensus.advance_commit_min(header.op);
            // A lifecycle completion counts only once the reporter commits the
            // fence locally, so the backups must not wait for the heartbeat.
            // Data commits are too frequent to announce one by one.
            if header.operation.is_partition_lifecycle() {
                self.consensus.request_eager_commit();
            }
            let event = CommitLogEvent {
                replica: ReplicaLogContext::from_consensus(&self.consensus, PlaneKind::Partitions),
                op: header.op,
                client_id: header.client,
                request_id: header.request,
                operation: header.operation,
                pipeline_depth: self.consensus.pipeline_len(),
            };
            emit_sim_event(SimEventKind::OperationCommitted, &event);
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &event.replica,
                header.op,
                event.pipeline_depth,
            );
            committed.push((entry, reply));
        }
        for (kind, consumer_id) in offset_keys {
            self.refresh_consumer_offset_reservation(kind, consumer_id);
        }
        // Released at publish, not at apply: until commit_min passes the
        // install, a reservation rescan still finds it and re-arms the latch.
        if pending.owner_install.is_some()
            && pending.owner_install == self.pending_owner_install.get()
        {
            self.pending_owner_install.set(None);
        }
        self.consensus.sync_prepare_timeout();
        self.evict_committed_prefix(pending.evictable);
        self.release_checkpointed_commands();
        for touched in &self.consumer_offset_dirs_touched {
            touched.set(None);
        }
        Some(committed)
    }

    /// Commands leave memory only once a checkpoint covers them, so a repair
    /// floor they lift sits where the WALs dropped them too. Past the
    /// retention caps with nothing checkpointed to drop, ask for the
    /// checkpoint.
    fn release_checkpointed_commands(&self) {
        let commit_min = self.consensus.commit_min();
        let checkpoint_op = self
            .persistence
            .as_ref()
            .map_or(commit_min, |persistence| persistence.checkpoint_op())
            .min(commit_min);
        let over = self
            .log
            .journal()
            .inner
            .release_checkpointed_commands(checkpoint_op);
        if over && let Some(persistence) = &self.persistence {
            persistence.request_checkpoint();
        }
    }

    /// Every replica retains the committed result, so a promoted primary answers
    /// a retry with the original append. Returns the reply this replica sends.
    ///
    /// # Errors
    /// The receipt table refused the result, which fences the partition.
    fn commit_receipt(
        &mut self,
        entry: &PipelineEntry,
        header: &PrepareHeader,
        batch_stats: Option<CommittedBatchStats>,
        send_client_replies: bool,
    ) -> Result<Option<CachedReply>, ClientTableWireError> {
        let auto_commit = is_auto_commit_client(header.client);
        if auto_commit && !entry.has_reply_sender() {
            return Ok(None);
        }
        let body = match header.operation {
            Operation::SendMessages => send_messages_reply_body(header.group, batch_stats),
            operation => committed_reply_body(operation),
        };
        let reply = build_reply_message(header, &body);
        if auto_commit {
            return Ok(Some(CachedReply::from(reply)));
        }
        let cached = self
            .dedup
            .commit_partition_reply(header.user_id, header.session, reply)?;
        Ok(send_client_replies.then_some(cached))
    }

    /// Captures each retirement before the walk evicts the journal prefix.
    /// Missing retry protection or append results fence the partition before
    /// apply, because an incomplete receipt cannot safely answer a retry.
    fn validate_committed_entries(
        &mut self,
        headers: &[PrepareHeader],
        batch_stats: &[Option<CommittedBatchStats>],
    ) -> Option<Vec<(u64, RetireSessionsRequest)>> {
        let namespace_raw = self.namespace().inner();
        let mut retirements = Vec::new();
        for (header, batch_stats) in headers.iter().zip(batch_stats) {
            if matches!(
                header.operation,
                Operation::SendMessages
                    | Operation::StoreConsumerOffset
                    | Operation::DeleteConsumerOffset
                    | Operation::InstallConsumerGroupOwner
                    | Operation::RetireConsumerGroupOwners
                    | Operation::StateMachineCommand
            ) && (header.partition_incarnation != self.created_revision()
                || self.history_deleted())
            {
                self.fence_failed_commit(*header);
                return None;
            }
            let configured_capacity =
                (!self.dedup.capacity_committed()).then(|| self.dedup.capacity());
            let retirement = (header.operation == Operation::RetireSession)
                .then(|| {
                    let prepare = self.log.journal().inner.repair_entry(header.op)?;
                    RetireSessionsRequest::decode_from(
                        prepare
                            .as_slice()
                            .get(iggy_binary_protocol::HEADER_SIZE..)?,
                    )
                    .ok()
                })
                .flatten();
            if self
                .dedup
                .commit_capacity(header.retry_capacity as usize)
                .is_err()
                || (header.operation == Operation::SendMessages && batch_stats.is_none())
                || (header.operation == Operation::RetireSession && retirement.is_none())
                || (header.operation == Operation::StateMachineCommand
                    && !self.committed_command_is_held(header))
            {
                error!(
                    namespace_raw,
                    op = header.op,
                    "committed partition entry lacks valid retry protection or append result"
                );
                self.fatal = Some(FatalCommit {
                    namespace_raw,
                    op: header.op,
                    operation: Some(header.operation),
                });
                return None;
            }
            if let Some(configured_capacity) = configured_capacity
                && configured_capacity != self.dedup.capacity()
            {
                warn!(
                    namespace_raw,
                    configured_capacity,
                    committed_capacity = self.dedup.capacity(),
                    "partition retry capacity is fixed by committed state; ignoring local configuration"
                );
            }
            if let Some(identity) = retirement {
                retirements.push((header.op, identity));
            }
        }
        Some(retirements)
    }

    async fn deliver_commit_replies(
        &self,
        committed: impl IntoIterator<Item = (PipelineEntry, Option<CachedReply>)>,
    ) {
        for (mut entry, reply) in committed {
            let Some(reply) = reply else {
                continue;
            };
            let header = entry.header;
            if is_auto_commit_client(header.client) {
                if let Some(sender) = entry.take_reply_sender() {
                    let _ = sender.send(reply.into_message());
                }
                continue;
            }
            let event = CommitLogEvent {
                replica: ReplicaLogContext::from_consensus(&self.consensus, PlaneKind::Partitions),
                op: header.op,
                client_id: header.client,
                request_id: header.request,
                operation: header.operation,
                pipeline_depth: self.consensus.pipeline_len(),
            };
            emit_sim_event(SimEventKind::ClientReplyEmitted, &event);
            if let Some(sender) = entry.take_reply_sender() {
                let _ = sender.send(reply.into_message());
            } else if let Err(error) = self
                .consensus
                .message_bus()
                .send_to_client(header.client, reply.into_wire_bytes())
                .await
            {
                debug!(
                    namespace_raw = self.namespace().inner(),
                    client = header.client, op = header.op, %error,
                    "client reply has no route by client id"
                );
            }
        }
    }

    #[cfg(test)]
    async fn flush_consumer_offset_directories_for(
        &self,
        kinds: [bool; ConsumerKind::COUNT],
    ) -> [bool; ConsumerKind::COUNT] {
        let job = self.prepare_consumer_offset_directories(kinds);
        let result = job.execute().await;
        self.accept_consumer_offset_directories(&result);
        result.failed
    }

    /// Selects the dirty offset directories of `kinds`, indexed by
    /// [`ConsumerKind::index`].
    fn prepare_consumer_offset_directories(
        &self,
        kinds: [bool; ConsumerKind::COUNT],
    ) -> crate::io::OffsetDirectoriesIoJob {
        let attempted = std::array::from_fn(|index| {
            kinds[index] && self.consumer_offset_dirs_dirty[index].get()
        });
        let paths = std::array::from_fn(|index| {
            if attempted[index] {
                self.consumer_offsets_dir(ConsumerKind::ALL[index])
                    .map(str::to_owned)
            } else {
                None
            }
        });
        crate::io::OffsetDirectoriesIoJob {
            paths,
            attempted,
            #[cfg(test)]
            fault: self.consumer_offset_dir_sync_fault.get(),
        }
    }

    /// A failed directory stays dirty for the next attempt.
    fn accept_consumer_offset_directories(&self, result: &crate::io::OffsetDirectoriesIoResult) {
        for index in 0..result.attempted.len() {
            if result.synced[index] {
                self.finish_consumer_offset_dir_sync(index);
            } else if result.attempted[index] && !result.failed[index] {
                // A kind without a directory has no entry to make durable.
                self.consumer_offset_dirs_dirty[index].set(false);
            }
        }
    }

    fn finish_consumer_offset_dir_sync(&self, index: usize) {
        #[cfg(test)]
        self.offset_dir_sync_count
            .set(self.offset_dir_sync_count.get() + 1);
        self.consumer_offset_dirs_dirty[index].set(false);
    }

    fn mark_consumer_offset_dir_dirty(&self, kind: ConsumerKind) {
        self.consumer_offset_dirs_dirty[kind.index()].set(true);
    }

    /// Keys of `kind` whose offset file could not be loaded or unlinked.
    #[must_use]
    pub fn stranded_consumer_offset_count(&self, kind: ConsumerKind) -> usize {
        self.consumer_offset_capacity_for(kind).stranded_count()
    }

    /// Batch stats for each drained entry, positionally parallel to `drained`.
    /// Every entry contributes exactly one slot (`None` for the operations that
    /// carry no batch), which is what makes the pairing correct by
    /// construction; keying on `op` instead would let a lookup miss attribute
    /// one batch's offsets to another entry's reply.
    fn resolve_committed_batch_stats(
        &self,
        drained: &[PrepareHeader],
    ) -> Vec<Option<CommittedBatchStats>> {
        drained
            .iter()
            .map(|entry| {
                if entry.operation != Operation::SendMessages {
                    return None;
                }
                match self.committed_batch_stats_for_prepare(entry) {
                    Ok(batch_stats) => batch_stats,
                    Err(error) => {
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            replica_id = self.consensus.replica(),
                            namespace_raw = self.namespace().inner(),
                            op = entry.op,
                            operation = ?entry.operation,
                            %error,
                            "failed to resolve committed visible offset for partition entry"
                        );
                        None
                    }
                }
            })
            .collect()
    }

    fn publish_committed_batch_stats(&mut self, batch_stats: CommittedBatchStats) {
        if let Some(producer) = batch_stats.producer {
            self.producers.record(&StampedBatch {
                producer,
                message_count: batch_stats.message_count,
                base_offset: batch_stats.base_offset,
                timestamp: batch_stats.timestamp,
            });
        }
        self.widest_batch_bytes
            .set(self.widest_batch_bytes.get().max(batch_stats.size_bytes));
        let end_offset = batch_stats.end_offset();
        // The committed counter now names data, which is what makes
        // it pollable and persistable. Outside the recovered-offset
        // guard below: that guard only skips re-counting stats a
        // previous life already persisted, and those offsets are
        // committed either way.
        self.note_committed_seeded();
        // A repaired batch at or below the boot-time recovered
        // durable offset was already counted (and persisted)
        // before the restart; skip it. Live traffic always sits
        // above the (immutable) line.
        if self
            .recovered_durable_offset
            .is_none_or(|durable| end_offset > durable)
        {
            self.offset.store(end_offset, Ordering::Release);
            self.committed_bytes = self.committed_bytes.saturating_add(batch_stats.size_bytes);
            self.stats.set_current_offset(end_offset);
            // Advance the aggregate stats with the visible offset. Disk
            // persistence is threshold-gated in `commit_messages`, which
            // must not also touch these counters or committed messages
            // would be double-counted once they flush.
            self.stats
                .increment_messages_count(u64::from(batch_stats.message_count));
            self.stats.increment_size_bytes(batch_stats.size_bytes);
        }
    }

    /// Read the committed batch's own stamps back out of the journal.
    ///
    /// INVARIANT: two replicas can never report a different `base_offset` for
    /// the same batch. Backups do re-stamp from their own `dirty_offset` in
    /// `append_messages`, so the guarantee is not "the bytes are replicated";
    /// it rests on three mechanisms. The backup gap check drops any prepare
    /// that is not `current_op + 1`, so every replica stamps a partition's
    /// batches in the primary's order off the same counter.
    /// `append_repaired_send_messages` journals a repaired prepare with its
    /// embedded stamps instead of re-stamping, so filling a hole out of live
    /// order cannot re-mint offsets. And that same path advances the counter
    /// with `dirty.max(last_offset)`, so a repaired window below the recovered
    /// durable end cannot rewind it and hand the next live batch offsets that
    /// were already issued.
    ///
    /// `repair_entry` is deliberate: it never awaits, and it falls back to the
    /// evicted ring, which the resident-only lookup does not.
    fn committed_batch_stats_for_prepare(
        &self,
        prepare_header: &PrepareHeader,
    ) -> Result<Option<CommittedBatchStats>, IggyError> {
        let entry = self
            .log
            .journal()
            .inner
            .repair_entry(prepare_header.op)
            // A resident slot can read back empty, which the caller must treat
            // as a miss and not as a zero-message batch.
            .filter(|entry| !entry.is_empty())
            .ok_or(IggyError::InvalidCommand)?;
        // Trusted (no batch-hash): the entry was read back from this replica's
        // own journal, where it was stamped/validated at append; only header
        // stats are needed, so re-hashing the ~1 MiB blob is redundant.
        let batch = decode_prepare_slice_trusted(entry.as_slice())
            .map_err(|_| IggyError::InvalidCommand)?;
        let message_count = batch.message_count();
        if message_count == 0 {
            return Ok(None);
        }

        Ok(Some(CommittedBatchStats {
            base_offset: batch.header.base_offset,
            message_count,
            size_bytes: batch.header.total_size() as u64,
            producer: batch.header.producer,
            timestamp: batch.header.base_timestamp,
        }))
    }

    fn parse_consumer_offset_request(
        operation: Operation,
        message: &Message<RoutedRequestHeader>,
    ) -> Result<(ConsumerKind, u32, Option<u64>, AckLevel), IggyError> {
        let total_size =
            usize::try_from(message.header().size).map_err(|_| IggyError::InvalidCommand)?;
        let body = message
            .as_slice()
            .get(std::mem::size_of::<RoutedRequestHeader>()..total_size)
            .ok_or(IggyError::InvalidCommand)?;
        Self::parse_consumer_offset_payload(operation, body)
    }

    /// Send `header`'s deny reply with `status` on `ReplyHeader.status` (empty
    /// body, op=0), logging a WARN under `send_fail_label` if the reply send
    /// fails. Callers deny on the primary, before the op enters the pipeline,
    /// so nothing replicates. `waiter` is the submit's in-process channel,
    /// taken by the caller.
    async fn send_partition_deny_or_log(
        consensus: &VsrConsensus<B>,
        header: &RoutedRequestHeader,
        status: u32,
        send_fail_label: &'static str,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        if waiter.is_none() && is_auto_commit_client(header.client) {
            return;
        }
        let reply = build_deny_reply_from_request(consensus, header, status);
        if let Some(waiter) = waiter {
            let _ = waiter.send(reply);
            return;
        }
        Self::deliver_reply_or_log(consensus, header, reply.into(), None, send_fail_label).await;
    }

    /// Deliver an admission-time reply (a deny, or an absorbed duplicate's
    /// success). When `waiter` is present the reply goes there: `header.client`
    /// is then the VSR consensus id, which the bus cannot route. Otherwise the
    /// bus carries it, and a failed send logs a WARN under `send_fail_label`.
    async fn deliver_reply_or_log(
        consensus: &VsrConsensus<B>,
        header: &RoutedRequestHeader,
        reply: CachedReply,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
        send_fail_label: &'static str,
    ) {
        if let Some(waiter) = waiter {
            let _ = waiter.send(reply.into_message());
            return;
        }
        if let Err(send_error) = consensus
            .message_bus()
            .send_to_client(header.client, reply.into_wire_bytes())
            .await
        {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                    send_fail_label,
                )
                .with_operation(header.operation)
                .with_error(send_error.to_string()),
            );
        }
    }

    fn restage_consumer_offset_from_journal(
        &self,
        op: u64,
    ) -> Result<PendingConsumerOffsetCommit, IggyError> {
        let entry = self
            .log
            .journal()
            .inner
            .repair_entry(op)
            .ok_or(IggyError::InvalidCommand)?;
        let bytes = entry.as_slice();
        let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            bytes
                .get(..size_of::<PrepareHeader>())
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidCommand)?;
        let body = bytes
            .get(size_of::<PrepareHeader>()..header.size as usize)
            .ok_or(IggyError::InvalidCommand)?;
        let (payload, owner_generation) =
            split_prepared_offset(body).map_err(|_| IggyError::InvalidCommand)?;
        let (kind, consumer_id, offset, _) =
            Self::parse_consumer_offset_payload(header.operation, payload)?;
        let mut pending = match header.operation {
            Operation::StoreConsumerOffset => {
                let offset = offset.ok_or(IggyError::InvalidCommand)?;
                if is_auto_commit_client(header.client) {
                    PendingConsumerOffsetCommit::upsert_auto_commit(kind, consumer_id, offset)
                } else {
                    PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset)
                }
            }
            Operation::DeleteConsumerOffset => {
                PendingConsumerOffsetCommit::delete(kind, consumer_id)
            }
            _ => return Err(IggyError::InvalidCommand),
        };
        pending.owner_generation = owner_generation;
        Ok(pending)
    }

    fn parse_staged_consumer_offset_commit(
        operation: Operation,
        message: &Message<PrepareHeader>,
    ) -> Result<(ConsumerKind, u32, Option<u64>, AckLevel, u64), IggyError> {
        let (payload, owner_generation) =
            split_prepared_offset(message.body()).map_err(|_| IggyError::InvalidCommand)?;
        let (kind, consumer_id, offset, ack) =
            Self::parse_consumer_offset_payload(operation, payload)?;
        Ok((kind, consumer_id, offset, ack, owner_generation))
    }

    fn parse_consumer_offset_payload(
        operation: Operation,
        body: &[u8],
    ) -> Result<(ConsumerKind, u32, Option<u64>, AckLevel), IggyError> {
        // Decode through the typed wire requests: the consumer is a
        // `WireConsumer` (kind + variable-length identifier), not a fixed
        // `[kind, u32]` prefix, so hand-rolled offsets would key the
        // committed offset under a garbled consumer id and reads (which
        // decode properly) would never find it.
        let (consumer, offset, ack) = match operation {
            Operation::StoreConsumerOffset => {
                let request = StoreConsumerOffsetRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (request.consumer, Some(request.offset), request.ack)
            }
            Operation::DeleteConsumerOffset => {
                let request = DeleteConsumerOffsetRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (request.consumer, None, request.ack)
            }
            _ => return Err(IggyError::InvalidCommand),
        };
        let kind = ConsumerKind::from_code(consumer.kind)?;
        // Named consumers hash to a stable u32 (mirrors the legacy
        // `PollingConsumer::resolve_consumer_id`), so writes key the offset
        // table identically to the read path's resolution.
        let consumer_id = match &consumer.id {
            WireIdentifier::Numeric(id) => *id,
            WireIdentifier::String(name) => iggy_common::calculate_32(name.as_str().as_bytes()),
        };
        Ok((kind, consumer_id, offset, ack))
    }

    fn prepare_materialization(
        &mut self,
        frozen_batches: Vec<Frozen<4096>>,
        index_bytes: Vec<u8>,
        batch_count: u32,
        written: u64,
        completes: bool,
    ) -> Result<Option<crate::MaterializationIoJob>, IggyError> {
        if batch_count == 0 || !self.log.has_segments() {
            return Ok(None);
        }
        let bodies_written = self
            .persistence
            .as_ref()
            .filter(|persistence| persistence.segment_checkpoint().is_some())
            .map(|persistence| {
                let segment = self.log.active_segment();
                persistence
                    .validate_segment_prefix(
                        &frozen_batches,
                        segment.start_offset,
                        segment
                            .size
                            .as_bytes_u64()
                            .checked_add(written)
                            .ok_or(IggyError::CannotWriteToFile)?,
                        self.durability().is_persisted(),
                    )
                    .map_err(|error| {
                        warn!(%error, "cannot expose the segment prefix");
                        IggyError::CannotSyncFile
                    })
            })
            .transpose()?;
        let messages_writer = self
            .log
            .messages_writers()
            .last()
            .and_then(Option::as_ref)
            .cloned();
        let index_writer = self
            .log
            .index_writers()
            .last()
            .and_then(Option::as_ref)
            .cloned();
        if bodies_written.is_none() && (messages_writer.is_none() || index_writer.is_none()) {
            let saved = frozen_batches
                .iter()
                .try_fold(0_u64, |saved, batch| {
                    saved.checked_add((batch.len() - size_of::<PrepareHeader>()) as u64)
                })
                .ok_or(IggyError::CannotWriteToFile)?;
            let segment = self.log.active_segment_mut();
            segment.size = IggyByteSize::from(
                segment
                    .size
                    .as_bytes_u64()
                    .checked_add(saved)
                    .ok_or(IggyError::CannotWriteToFile)?,
            );
            return Ok(None);
        }
        let index_writer = index_writer.ok_or(IggyError::CannotWriteToFile)?;
        let segment = self.log.active_segment();
        let target = crate::io::MaterializationTarget {
            namespace: self.namespace(),
            incarnation: self.incarnation,
            poll_history: self.poll_history,
            segment_start: segment.start_offset,
            segment_size: segment.size.as_bytes_u64(),
            messages_position: messages_writer.as_ref().map(|writer| writer.position()),
            messages_writer,
            index_position: index_writer.position(),
            index_writer,
        };
        let checksum = if written == 0 {
            self.take_segment_hasher(target.segment_size)
        } else {
            self.materialization
                .as_mut()
                .and_then(|pending| pending.checksum.take())
        };
        let digest =
            self.take_materialization_digest(target.segment_start, target.segment_size, written);
        let frozen_batches = if bodies_written.is_some() && checksum.is_none() && digest.is_none() {
            Vec::new()
        } else {
            frozen_batches
        };
        let job = crate::MaterializationIoJob {
            allocation_charge: crate::io::materialization_allocation_charge(
                &frozen_batches,
                frozen_batches.capacity(),
                index_bytes.capacity(),
            )
            .ok_or(IggyError::CannotWriteToFile)?,
            target,
            batches: frozen_batches,
            indexes: index_bytes,
            bodies_written,
            written,
            completes,
            checksum,
            digest,
        };
        Ok(Some(job))
    }

    /// The digest a save `written` bytes into its chunk feeds: the chain's for
    /// the first piece, the one the previous piece handed back after it.
    fn take_materialization_digest(
        &mut self,
        segment_start: u64,
        segment_size: u64,
        written: u64,
    ) -> Option<Box<crate::segment_chain::SegmentDigest>> {
        if written == 0 {
            self.segment_chain
                .as_mut()
                .and_then(|chain| chain.take_digest(segment_start, segment_size))
        } else {
            self.materialization
                .as_mut()
                .and_then(|pending| pending.digest.take())
        }
    }

    fn accept_materialization(
        &mut self,
        result: crate::MaterializationIoResult,
    ) -> Result<(), IggyError> {
        let target = result.target;
        if target.incarnation != self.incarnation || target.poll_history != self.poll_history {
            return Err(IggyError::CannotWriteToFile);
        }
        let segment_index = self
            .log
            .segments()
            .iter()
            .rposition(|segment| segment.start_offset == target.segment_start)
            .ok_or(IggyError::CannotWriteToFile)?;
        let messages_writer = self
            .log
            .messages_writers()
            .get(segment_index)
            .and_then(Option::as_ref);
        let same_messages_writer = match (messages_writer, target.messages_writer.as_ref()) {
            (Some(installed), Some(captured)) => {
                Rc::ptr_eq(installed, captured)
                    && Some(installed.position()) == target.messages_position
            }
            (None, None) => true,
            _ => false,
        };
        if !same_messages_writer
            || !self
                .log
                .index_writers()
                .get(segment_index)
                .and_then(Option::as_ref)
                .is_some_and(|writer| {
                    Rc::ptr_eq(writer, &target.index_writer)
                        && writer.position() == target.index_position
                })
            || self.log.segments()[segment_index].size.as_bytes_u64() != target.segment_size
        {
            return Err(IggyError::CannotWriteToFile);
        }
        let (saved, saved_indexes) = result.outcome?;
        let size = target
            .segment_size
            .checked_add(saved)
            .ok_or(IggyError::CannotWriteToFile)?;
        target
            .index_position
            .checked_add(saved_indexes)
            .ok_or(IggyError::CannotWriteToFile)?;
        if let Some(position) = target.messages_position {
            position
                .checked_add(saved)
                .ok_or(IggyError::CannotWriteToFile)?;
        }
        if let Some(writer) = target.messages_writer {
            writer.advance(saved);
        }
        target.index_writer.advance(saved_indexes);
        self.log.segments_mut()[segment_index].size = IggyByteSize::from(size);
        if saved == 0 {
            if let Some(pending) = self.materialization.as_mut() {
                pending.checksum = result.checksum;
                pending.digest = result.digest;
            }
        } else {
            if let Some(hasher) = result.checksum {
                self.segment_checksum_cache.borrow_mut().insert(
                    target.segment_start,
                    crate::state_transfer::SegmentChecksumMemo::Hashing {
                        hashed_len: size,
                        hasher,
                    },
                );
            }
            if let Some(chain) = self.segment_chain.as_mut() {
                chain.restore_digest(result.digest);
            }
        }
        Ok(())
    }

    pub(crate) async fn rotate_segment(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<(), IggyError> {
        let start_offset = self.log.active_segment().end_offset + 1;
        self.rotate_segment_at(config, start_offset).await
    }

    /// Seal the active segment and plant a fresh empty one at `start_offset`.
    ///
    /// Shared by the size-driven roll, which plants at `end_offset + 1`, and the
    /// boot re-anchor, which plants at the append point the reservation moved the
    /// counter to. One seal path, and one order: the new segment's files are
    /// created BEFORE the sealed segment's writers are torn down, so a failed
    /// create leaves the chain serviceable.
    async fn rotate_segment_at(
        &mut self,
        config: &PartitionsConfig,
        start_offset: u64,
    ) -> Result<(), IggyError> {
        let sealed_end = self.log.active_segment().end_offset;
        debug_assert!(
            start_offset > sealed_end,
            "a plant at {start_offset} overlaps the sealed tail ending at {sealed_end}"
        );
        // A wider gap than the roll's own is legitimate only with the anchor
        // already durable, which is the caller's obligation
        // (`record_reanchor_gap`) and is otherwise readable nowhere in here.
        #[cfg(debug_assertions)]
        if start_offset > sealed_end.saturating_add(1)
            && let Some(partition_dir) = self.partition_dir()
        {
            debug_assert!(
                matches!(
                    crate::segment_anchor::read_anchor(&partition_dir, start_offset).await,
                    Ok(Some(anchor)) if anchor.covers(
                        start_offset,
                        self.log.active_segment().start_offset,
                        sealed_end,
                    )
                ),
                "a plant at {start_offset} leaves a gap past {sealed_end} with no anchor"
            );
        }
        let (target, job) = self.prepare_rotation(config, start_offset);
        let installed = job.execute().await?;
        self.accept_rotation(&target, installed)
    }

    fn prepare_rotation(
        &self,
        config: &PartitionsConfig,
        start_offset: u64,
    ) -> (crate::io::RotationTarget, crate::io::SegmentIoJob) {
        let segment = self.log.active_segment();
        let messages_writer = self
            .log
            .messages_writers()
            .last()
            .and_then(Option::as_ref)
            .cloned();
        let index_writer = self
            .log
            .index_writers()
            .last()
            .and_then(Option::as_ref)
            .cloned();
        let chain_append = self
            .segment_chain
            .as_ref()
            .and_then(|chain| chain.seal(segment, self.created_revision()));
        let target = crate::io::RotationTarget {
            chain_record: chain_append.as_ref().map(|append| append.record),
            incarnation: self.incarnation,
            poll_history: self.poll_history,
            segment_start: segment.start_offset,
            segment_size: segment.size.as_bytes_u64(),
            segment_end: segment.end_offset,
            messages_position: messages_writer.as_ref().map(|writer| writer.position()),
            index_position: index_writer.as_ref().map(|writer| writer.position()),
            messages_writer,
            index_writer,
        };
        let mut job = self.prepare_empty_segment(config, start_offset);
        if self.persistence.is_some() {
            job.old_index.clone_from(&target.index_writer);
        }
        job.chain_append = chain_append;
        (target, job)
    }

    fn accept_rotation(
        &mut self,
        target: &crate::io::RotationTarget,
        installed: crate::io::InstalledSegment,
    ) -> Result<(), IggyError> {
        let segment = self.log.active_segment();
        let messages_writer = self.log.messages_writers().last().and_then(Option::as_ref);
        let index_writer = self.log.index_writers().last().and_then(Option::as_ref);
        if target.incarnation != self.incarnation || target.poll_history != self.poll_history {
            return Err(IggyError::CannotWriteToFile);
        }
        if segment.start_offset != target.segment_start
            || segment.size.as_bytes_u64() != target.segment_size
            || segment.end_offset != target.segment_end
            || messages_writer.map(Rc::as_ptr) != target.messages_writer.as_ref().map(Rc::as_ptr)
            || index_writer.map(Rc::as_ptr) != target.index_writer.as_ref().map(Rc::as_ptr)
            || messages_writer.map(|writer| writer.position()) != target.messages_position
            || index_writer.map(|writer| writer.position()) != target.index_position
            || installed.segment.start_offset <= segment.end_offset
        {
            return Err(IggyError::CannotWriteToFile);
        }
        let sealed_index = self.log.segments().len() - 1;
        let start_offset = installed.segment.start_offset;
        self.log.active_segment_mut().sealed = true;
        self.accept_empty_segment(installed);
        self.stats.increment_segments_count(1);
        self.log.storages_mut()[sealed_index].seal();
        self.log.messages_writers_mut()[sealed_index] = None;
        self.log.index_writers_mut()[sealed_index] = None;
        self.log.indexes_mut()[sealed_index] = None;
        self.log.reset_read_state(sealed_index);
        self.stamp_sealed_segment(target.segment_start);
        if let Some(chain) = self.segment_chain.as_mut() {
            let diverged = chain.sealed(target.chain_record.as_ref(), start_offset);
            // A segment whose bytes did not all pass through the digest seals
            // without a record, so it gets one hashed from disk.
            if target.chain_record.is_none() {
                if let Some(prefix) = self.object_prefix() {
                    self.request_tiering(prefix);
                }
            } else {
                self.want_upload();
            }
            self.chain_settled(&diverged);
        }
        debug!(
            namespace_raw = self.namespace().inner(),
            sealed_end = target.segment_end,
            start_offset,
            "sealed the active segment and planted a fresh one"
        );
        Ok(())
    }

    /// Minimum committed offset across all consumers and consumer groups, with
    /// the holder's identity. `None` when nothing has been committed, in which
    /// case there is no deletion barrier. External groups never hold retention.
    fn min_committed_offset(&self) -> Option<(u64, ConsumerKind, u32)> {
        let consumer_guard = self.consumer_offsets.pin();
        let group_guard = self.consumer_group_offsets.pin();
        let consumers = consumer_guard.iter().map(|(_, offset)| {
            (
                offset.offset.load(Ordering::Relaxed),
                offset.kind,
                offset.consumer_id,
            )
        });
        let groups = group_guard.iter().map(|(_, offset)| {
            (
                offset.offset.load(Ordering::Relaxed),
                offset.kind,
                offset.consumer_id,
            )
        });
        consumers.chain(groups).min_by_key(|(offset, _, _)| *offset)
    }

    /// Time-expiry plus size-retention in one pass, for a partition that is not
    /// tiered: remove the leading sealed
    /// segments that have expired or that push the partition's SEALED bytes
    /// past `max_bytes`. Capped per call by
    /// `SEGMENT_REMOVAL_BUDGET_PER_PASS`; the returned
    /// [`SegmentRemoval::budget_spent`] tells the caller whether the rest is
    /// still waiting.
    pub async fn clean_expired_segments(
        &mut self,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) -> SegmentRemoval {
        // A tiered partition's retention is one decision for the local and
        // the remote segments together, which the primary commits as a
        // truncation. A replica that expired segments by itself could drop
        // one that was never uploaded.
        if self.is_tiered() {
            return SegmentRemoval::default();
        }
        let expired = leading_expired_end(self.log.segments(), now, message_expiry);
        let oversized =
            max_bytes.and_then(|max_bytes| leading_oversized_end(self.log.segments(), max_bytes));
        let Some(up_to) = expired.into_iter().chain(oversized).max() else {
            return SegmentRemoval::default();
        };
        self.remove_sealed_segments_up_to(up_to).await
    }

    /// What the retention policy of a tiered topic removes from this
    /// partition: the end offset of the last leading sealed segment that
    /// expired or that pushes the SEALED bytes past `max_bytes`, held at the
    /// consumer barrier as [`Self::remove_sealed_segments_up_to`] holds a
    /// removal. Segments whose `.log` already left the disk count like local
    /// ones, since the store still holds their bytes.
    ///
    /// Nothing is removed here. The caller commits the offset as a
    /// truncation, which is what removes the segments on every replica.
    ///
    /// `None` on a partition without a chain, and on every replica but a
    /// primary that applied the view it started: a backup's segment list can
    /// trail the primary's, and one decision per partition is what keeps the
    /// replicas and the store on the same front.
    #[must_use]
    pub fn total_retention_end(
        &self,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) -> Option<u64> {
        let consensus = &self.consensus;
        if !self.is_tiered()
            || !consensus.is_primary()
            || !consensus.is_normal()
            || consensus.is_transferring()
            || !consensus.applied_view_start()
        {
            return None;
        }
        retention_end(
            self.log.segments(),
            now,
            message_expiry,
            max_bytes,
            self.min_committed_offset().map(|(barrier, _, _)| barrier),
        )
    }

    /// Remove the oldest sealed segments whose `end_offset <= up_to_offset`,
    /// never the active segment and never past the consumer barrier (the
    /// minimum committed consumer/group offset). Unlinks the messages and
    /// index files and decrements partition stats. Idempotent: an offset below
    /// the oldest sealed segment removes nothing.
    ///
    /// NOT exhaustive: at most `SEGMENT_REMOVAL_BUDGET_PER_PASS` segments go
    /// per call, so a caller enforcing a retention decision has to re-issue it
    /// until the layout converges rather than assume one call finished the job.
    /// Both callers already do: the segment cleaner re-stages on
    /// [`SegmentRemoval::budget_spent`] and again on its
    /// `data_maintenance.messages.interval` tick, and the partition reconciler
    /// re-stages a committed delete watermark on every pass while the first
    /// local segment still starts below it.
    ///
    /// Mounted or busy partitions retain the watermark for the I/O lane and
    /// return an empty result. Only an idle partition without a dispatcher removes
    /// files inline and returns their counts.
    pub async fn remove_sealed_segments_up_to(&mut self, up_to_offset: u64) -> SegmentRemoval {
        self.maintenance.retention = Some(
            self.maintenance
                .retention
                .map_or(up_to_offset, |previous| previous.max(up_to_offset)),
        );
        if self.has_io_dispatcher() || self.history_is_busy() {
            self.notify_io();
            return SegmentRemoval::default();
        }
        let mut removal = SegmentRemoval::default();
        for _ in 0..SEGMENT_REMOVAL_BUDGET_PER_PASS {
            if !self.retention_is_ready(up_to_offset) {
                break;
            }
            let Some(job) = self.capture_segment_removal() else {
                break;
            };
            let result = job.execute().await;
            if let Some(messages) = self.accept_segment_removal(&result) {
                removal.segments += 1;
                removal.messages += messages;
            }
        }
        removal.budget_spent = self.retention_is_ready(up_to_offset);
        self.maintenance.retention = None;
        removal
    }

    fn retention_is_ready(&mut self, through: u64) -> bool {
        if self.log.segments().len() < 2 {
            self.maintenance.retention = None;
            return false;
        }
        let segment = &self.log.segments()[0];
        if !segment.sealed || segment.end_offset > through {
            self.maintenance.retention = None;
            return false;
        }
        if let Some((barrier, kind, consumer_id)) = self.min_committed_offset()
            && segment.end_offset > barrier
        {
            warn!(namespace_raw = self.namespace().inner(), start_offset = segment.start_offset, end_offset = segment.end_offset, barrier, %kind, consumer_id, "segment retained: blocked by committed consumer offset");
            self.maintenance.retention = None;
            return false;
        }
        if self.persistence_checkpoint_pending() {
            return false;
        }
        if let Some(persistence) = &self.persistence
            && let Some(checkpoint) = persistence.segment_checkpoint()
            && segment.end_offset >= checkpoint.next_offset
        {
            persistence.request_checkpoint();
            return false;
        }
        true
    }

    fn capture_segment_removal(&mut self) -> Option<crate::PartitionIoJob<SB>> {
        let (segment, storage) = self.log.retire_front()?;
        self.log
            .journal()
            .inner
            .forget_segment(segment.start_offset);
        let (messages, indexes) = storage.segment_and_index_paths();
        // A segment that came here remote has no `.index` in its storage, and
        // a poll may have fetched one since or be fetching it now.
        self.index_fetches.retire_through(segment.end_offset);
        let indexes = indexes.or_else(|| {
            let dir = self.partition_dir.as_deref()?;
            (segment.location == SegmentLocation::Remote)
                .then(|| format!("{dir}/{:0>20}.index", segment.start_offset))
        });
        let anchor = self.anchor_cleanup_path(segment.start_offset);
        self.segment_checksum_cache
            .borrow_mut()
            .remove(&segment.start_offset);
        // Boot refuses a front above the trim point, and keeps no record at or
        // below it, so the trim point must cover the segment before it goes.
        // It runs up to the next segment: the anchor that certifies a gap
        // between the two names the segment leaving now, so past this removal
        // only the trim point vouches for the new front.
        let trim_point = self
            .log
            .segments()
            .first()
            .map_or(segment.end_offset, |next| {
                next.start_offset.saturating_sub(1).max(segment.end_offset)
            });
        let trim = self
            .segment_chain
            .as_ref()
            .and_then(|chain| chain.trim(trim_point))
            .map(Box::new);
        self.maintenance.removal_trim = trim.as_ref().map(|_| trim_point);
        self.maintenance.removing = Some(segment);
        self.clear_state_transfer_offer_cache();
        Some(crate::PartitionIoJob::RemoveSegment {
            namespace: self.namespace(),
            paths: [messages, indexes, anchor],
            // TODO: Retain failed retention unlinks for retry and sync the
            // partition directory before reporting cleanup complete.
            strict: false,
            trim,
        })
    }

    fn accept_segment_removal(&mut self, result: &crate::PartitionIoResult) -> Option<u64> {
        let trim_point = self.maintenance.removal_trim.take();
        if !matches!(result, crate::PartitionIoResult::SegmentRemoved(Ok(()))) {
            return None;
        }
        let segment = self.maintenance.removing.take()?;
        if let (Some(trim_point), Some(chain)) = (trim_point, self.segment_chain.as_mut()) {
            chain.note_appended(&crate::segment_chain::ChainRecord::TrimPoint(trim_point));
        }
        let (messages, shortfall) = settle_cleaned_segment(&self.stats, &segment);
        shortfall.report(
            self.namespace(),
            Some((segment.start_offset, segment.end_offset)),
        );
        self.retire_segment_objects(
            segment.start_offset,
            segment.end_offset,
            IggyTimestamp::now(),
        );
        Some(messages)
    }

    /// Evict the leading uploaded segments that left the local envelope. Each
    /// one's remote record is made durable before its `.log` is unlinked, and
    /// the segment stays in the list as `Remote`, so offsets and timestamps
    /// resolve as before and the partition's sizes do not change.
    ///
    /// The run starts after the segments already remote and stops at the first
    /// of: the active segment, a segment the WAL checkpoint still reaches, one
    /// ending above `floor`, one whose hash no floor advance matched, or one the
    /// envelope keeps. A partition without a chain evicts nothing.
    ///
    /// Capped per call like [`Self::remove_sealed_segments_up_to`], with the
    /// same split between the I/O lane and an idle partition without a
    /// dispatcher.
    pub async fn evict_segments(
        &mut self,
        now: IggyTimestamp,
        floor: u64,
        envelope: LocalEnvelope,
    ) -> SegmentEviction {
        let Some(through) = self.eviction_end(now, floor, envelope) else {
            return SegmentEviction::default();
        };
        self.maintenance.eviction = Some(
            self.maintenance
                .eviction
                .map_or(through, |previous| previous.max(through)),
        );
        if self.has_io_dispatcher() || self.history_is_busy() {
            self.notify_io();
            return SegmentEviction::default();
        }
        let mut eviction = SegmentEviction::default();
        for _ in 0..SEGMENT_REMOVAL_BUDGET_PER_PASS {
            if !self.eviction_is_ready(through) {
                break;
            }
            let Some(job) = self.capture_segment_eviction() else {
                break;
            };
            if !matches!(self.accept_segment_eviction(job.execute().await), Ok(true)) {
                break;
            }
            eviction.segments += 1;
        }
        eviction.budget_spent = self.eviction_is_ready(through);
        self.maintenance.eviction = None;
        eviction
    }

    /// The end offset of the last segment the pass may evict, judged by the
    /// rules that do not move while it runs.
    fn eviction_end(&self, now: IggyTimestamp, floor: u64, envelope: LocalEnvelope) -> Option<u64> {
        let chain = self.segment_chain.as_ref()?;
        let sealed = self.log.segments().split_last()?.1;
        let mut local: u64 = sealed
            .iter()
            .filter(|segment| segment.location != SegmentLocation::Remote)
            .map(|segment| segment.size.as_bytes_u64())
            .sum();
        let mut through = None;
        for segment in sealed
            .iter()
            .filter(|segment| segment.location != SegmentLocation::Remote)
        {
            let outside = segment.is_expired(now, envelope.age)
                || envelope.size.is_some_and(|size| local > size);
            if !segment.sealed
                || segment.end_offset > floor
                || !chain.evictable(segment.start_offset, segment.end_offset)
                || !outside
            {
                break;
            }
            local -= segment.size.as_bytes_u64();
            through = Some(segment.end_offset);
        }
        through
    }

    /// Turn tiering on for this partition, or check that its chain is
    /// complete when it is on already.
    ///
    /// The I/O lane creates the chain holding `prefix` when the directory has
    /// none, which starts the hashing of the active segment on the commit
    /// path. It then records the front that retention left, and hashes every
    /// sealed segment without a record from its `.log`. Until the chain is
    /// adopted the partition is not tiered, so it evicts nothing.
    pub fn enable_tiering(&mut self, prefix: iggy_object_store::ObjectPrefix) {
        self.request_tiering(prefix);
        self.notify_io();
    }

    /// A request already under way keeps its progress and its retry delay.
    fn request_tiering(&mut self, prefix: iggy_object_store::ObjectPrefix) {
        if self
            .maintenance
            .tiering
            .as_ref()
            .is_none_or(|pending| pending.prefix != prefix)
        {
            self.maintenance.tiering = Some(PendingTiering::new(prefix));
        }
    }

    /// The prefix of an enable that the I/O lane has not finished yet.
    #[must_use]
    pub fn pending_tiering(&self) -> Option<iggy_object_store::ObjectPrefix> {
        self.maintenance
            .tiering
            .as_ref()
            .map(|pending| pending.prefix)
    }

    /// Whether this partition keeps a chain.
    #[must_use]
    pub const fn is_tiered(&self) -> bool {
        self.segment_chain.is_some()
    }

    /// The segments an install is replacing are not the ones the request
    /// scanned and hashed.
    pub(crate) fn restart_tiering(&mut self) {
        if let Some(pending) = &mut self.maintenance.tiering {
            *pending = PendingTiering::new(pending.prefix);
        }
    }

    fn tiering_is_ready(&mut self) -> bool {
        let Some(pending) = &self.maintenance.tiering else {
            return false;
        };
        let (failures, retry_after_micros) = (pending.failures, pending.retry_after_micros);
        // Before any job is chosen: the lane records only the front advance.
        self.drop_recorded_advances();
        // The next start asks again, and the shutdown flush does not wait for
        // the rest of the history to be hashed. The applied advances are still
        // recorded, each with one short append that goes before any hash: a
        // checkpoint during the shutdown would otherwise outrun them.
        if self.maintenance.shutdown.is_some()
            && (failures > 0 || self.segment_chain.is_none() || self.pending_advances.is_empty())
        {
            self.maintenance.tiering = None;
            return false;
        }
        if self.consensus.clock_realtime_micros() < retry_after_micros {
            return false;
        }
        let Some(read_len) = self.next_tiering_read_len() else {
            self.maintenance.tiering = None;
            // No chain can be opened here, so nothing ever records these.
            self.pending_advances.clear();
            return false;
        };
        if let Some(pending) = &mut self.maintenance.tiering {
            pending.read_len = read_len;
        }
        true
    }

    /// Take the committed truncation watermark that metadata holds for this
    /// partition. Uploads and floor advances are judged against the highest
    /// one taken, so a pass that cannot read metadata still judges against a
    /// watermark that was committed.
    pub fn note_committed_truncation(&mut self, committed_up_to: u64) {
        self.committed_truncation = self.committed_truncation.max(committed_up_to);
    }

    /// The trim point this primary has to commit as a truncation before it
    /// uploads: the last offset its own retention removed, when neither the
    /// tiered floor nor `committed_up_to`, the truncation watermark metadata
    /// holds, has reached it.
    ///
    /// The offsets up to it are gone here, but another replica can still
    /// hold them, and a floor advance over them would declare them stored.
    /// Only the recorded trim point is offered, never the front of the
    /// segment list by itself.
    ///
    /// `None` on a partition without a chain or a trim point, and on every
    /// replica but a primary that applied the view it started, as for
    /// [`Self::total_retention_end`].
    #[must_use]
    pub fn trim_point_to_commit(&self, committed_up_to: u64) -> Option<u64> {
        let consensus = &self.consensus;
        if !self.is_tiered()
            || !consensus.is_primary()
            || !consensus.is_normal()
            || consensus.is_transferring()
            || !consensus.applied_view_start()
        {
            return None;
        }
        let trim_point = self.chain_trim_point()?;
        let above_floor = self.tiered_floor().is_none_or(|floor| trim_point > floor);
        (above_floor && trim_point > committed_up_to).then_some(trim_point)
    }

    /// The segment the primary uploads next: the oldest sealed one that is
    /// still local and above the tiered floor.
    ///
    /// `None` when there is none, and also while that segment has no usable
    /// record: its hash from disk is still being taken, or an advance
    /// contradicted it. A younger segment never goes first, so a seal does not
    /// overtake a backlog. The caller checks that this replica is the primary.
    #[must_use]
    pub fn next_segment_upload(&self) -> Option<crate::SegmentUpload> {
        let slot = self.oldest_unuploaded_segment()?;
        let (segment, storage) = (&self.log.segments()[slot], &self.log.storages()[slot]);
        let record = self.undisputed_segment_record(segment.start_offset)?;
        let (log_path, index_path) = storage.segment_and_index_paths();
        Some(crate::SegmentUpload {
            prefix: self.object_prefix()?,
            record,
            log_path: log_path?,
            index_path: index_path?,
        })
    }

    /// Start the next upload, when this replica is the primary and none is in
    /// flight. The caller runs it under the returned scope and hands the
    /// result to [`Self::finish_segment_upload`] with the returned attempt.
    ///
    /// A replica that is no longer the primary cancels the upload it started,
    /// so a scan of a demoted replica is what stops its job.
    pub fn begin_segment_upload(&mut self) -> Option<crate::StartedSegmentUpload> {
        self.begin_segment_upload_if(|_| true)
    }

    /// [`Self::begin_segment_upload`], unless `admit` refuses the segment's
    /// size in bytes. `admit` is asked last, and only about an upload that
    /// would start, so what it charges is charged to a started upload. A
    /// refusal leaves the segment for a later call.
    pub fn begin_segment_upload_if(
        &mut self,
        admit: impl FnOnce(u64) -> bool,
    ) -> Option<crate::StartedSegmentUpload> {
        if !self.consensus.is_primary() || !self.consensus.is_normal() {
            self.segment_upload = None;
            self.upload_cursor = None;
            return None;
        }
        if self.segment_upload.is_some() {
            return None;
        }
        let upload = self.next_segment_upload()?;
        // A segment whose advance cannot be sent stays local: storing it
        // would spend the budget on bytes no replica may evict.
        self.floor_advance_over(&upload.record)?;
        if !admit(upload.record.size) {
            return None;
        }
        let scope = iggy_object_store::TierScope::new();
        let token = scope.token();
        self.segment_upload_attempts += 1;
        let attempt = crate::SegmentUploadAttempt(self.segment_upload_attempts);
        self.segment_upload = Some((attempt, scope));
        Some((upload, token, attempt))
    }

    /// The upload this partition waits for the result of.
    ///
    /// A result of any other attempt is stale: its upload was cancelled, and
    /// the segment may be on its way again under a later attempt.
    #[must_use]
    pub fn segment_upload_attempt(&self) -> Option<crate::SegmentUploadAttempt> {
        self.segment_upload
            .as_ref()
            .filter(|(_, scope)| !scope.is_cancelled())
            .map(|(attempt, _)| *attempt)
    }

    /// Take the result of upload `attempt`, of the segment in `record`.
    /// Returns the advance to commit when the segment is stored and this
    /// replica is still the primary that started the upload.
    ///
    /// The result of an attempt other than the one in flight changes nothing.
    /// A failure leaves the segment local, so a later scan offers it again. A
    /// failure that no retry changes is reported: the segment and everything
    /// after it stay out of the store until someone repairs it.
    ///
    /// An upload whose segment left the list while it ran yields no advance,
    /// and its objects are queued for their delete.
    pub fn finish_segment_upload(
        &mut self,
        attempt: crate::SegmentUploadAttempt,
        record: &crate::segment_chain::SegmentRecord,
        outcome: &crate::UploadOutcome,
    ) -> Option<crate::tiered_floor::AdvanceTieredFloor> {
        if self.segment_upload_attempt() != Some(attempt) {
            return None;
        }
        self.segment_upload = None;
        let listed = self
            .log
            .segments()
            .binary_search_by_key(&record.start_offset, |segment| segment.start_offset)
            .is_ok();
        if !listed {
            // Retention removed the segment while its upload ran. An advance
            // over it would tell every replica the store holds a segment that
            // is gone, and a failed upload can still have stored its `.log`.
            self.retire_segment_objects(
                record.start_offset,
                record.end_offset,
                IggyTimestamp::now(),
            );
            return None;
        }
        match outcome {
            Ok(Ok(())) if self.consensus.is_primary() && self.consensus.is_normal() => {
                self.note_segment_uploaded(record)
            }
            Ok(Ok(())) | Err(_) => None,
            Ok(Err(failure)) => {
                if failure.is_transient() {
                    warn!(
                        namespace_raw = self.namespace().inner(),
                        start_offset = record.start_offset,
                        %failure,
                        "segment upload failed and is tried again"
                    );
                } else {
                    error!(
                        namespace_raw = self.namespace().inner(),
                        start_offset = record.start_offset,
                        %failure,
                        "segment upload cannot succeed, and holds back every later segment"
                    );
                }
                None
            }
        }
    }

    /// Stop the upload in flight, as a partition does on its way out.
    pub fn cancel_segment_upload(&mut self) {
        self.segment_upload = None;
    }

    /// Whether a segment became uploadable since the last call. Only a
    /// primary asks for a pass, since only a primary uploads.
    pub fn take_upload_wanted(&self) -> bool {
        self.upload_wanted.take()
    }

    fn want_upload(&self) {
        if self.consensus.is_primary() {
            self.upload_wanted.set(true);
        }
    }

    /// The slot of the oldest sealed segment that is still local and above
    /// the tiered floor.
    fn oldest_unuploaded_segment(&self) -> Option<usize> {
        let from = self
            .first_slot_above_floor()
            .max(self.slot_after_uploaded());
        self.log.segments()[from..]
            .iter()
            .position(|segment| {
                segment.sealed
                    && segment.location == SegmentLocation::Local
                    && segment.size.as_bytes_u64() > 0
            })
            .map(|slot| from + slot)
    }

    /// The slot of the first segment that holds an offset above the tiered
    /// floor, found without walking the history below it.
    fn first_slot_above_floor(&self) -> usize {
        let Some(floor) = self.tiered_floor() else {
            return 0;
        };
        let segments = self.log.segments();
        let after = segments.partition_point(|segment| segment.start_offset <= floor);
        match after.checked_sub(1) {
            Some(last) if segments[last].end_offset > floor => last,
            _ => after,
        }
    }

    /// The slot after the segments [`Self::upload_cursor`] covers, or `0`
    /// when the cursor does not describe the segment list.
    ///
    /// The cursor is trusted only while the segment that ends right before it
    /// is still marked as stored: a segment list rebuilt from disk marks
    /// every segment local again, and those have to be offered again.
    fn slot_after_uploaded(&self) -> usize {
        let Some(cursor) = self.upload_cursor else {
            return 0;
        };
        let segments = self.log.segments();
        let slot = segments.partition_point(|segment| segment.start_offset < cursor);
        let stands = slot.checked_sub(1).is_some_and(|last| {
            segments[last].location != SegmentLocation::Local
                && segments[last].end_offset.checked_add(1) == Some(cursor)
        });
        if stands { slot } else { 0 }
    }

    /// How far this primary's uploads are behind at `now`: the age of the
    /// newest message in the oldest segment that waits for its upload.
    ///
    /// `None` on a replica that uploads nothing, and when no segment waits.
    /// A segment sealed without a known timestamp counts as no lag, as it
    /// counts as not expired.
    #[must_use]
    pub fn upload_lag(&self, now: IggyTimestamp) -> Option<Duration> {
        if !self.is_tiered() || !self.consensus.is_primary() {
            return None;
        }
        let segment = &self.log.segments()[self.oldest_unuploaded_segment()?];
        if segment.max_timestamp == 0 {
            return None;
        }
        Some(Duration::from_micros(
            now.as_micros().saturating_sub(segment.max_timestamp),
        ))
    }

    /// Whether this primary holds a sealed segment that the local envelope
    /// would evict at `now`, were it uploaded: the leading local segments are
    /// outside the envelope up to one that is above the tiered floor.
    ///
    /// Judged against the envelope the segment cleaner last resolved, so it
    /// is `false` until the first eviction pass.
    #[must_use]
    pub fn eviction_awaits_upload(&self, now: IggyTimestamp) -> bool {
        let Some(envelope) = self.local_envelope else {
            return false;
        };
        if !self.is_tiered() || !self.consensus.is_primary() {
            return false;
        }
        let Some((_, sealed)) = self.log.segments().split_last() else {
            return false;
        };
        let floor = self.tiered_floor();
        let local = || {
            sealed
                .iter()
                .filter(|segment| segment.location != SegmentLocation::Remote)
        };
        let mut local_bytes: u64 = local().map(|segment| segment.size.as_bytes_u64()).sum();
        for segment in local() {
            let outside = segment.is_expired(now, envelope.age)
                || envelope.size.is_some_and(|size| local_bytes > size);
            if !segment.sealed || !outside {
                return false;
            }
            if floor.is_none_or(|floor| segment.end_offset > floor) {
                return true;
            }
            local_bytes -= segment.size.as_bytes_u64();
        }
        false
    }

    /// Whether this replica is the one that deletes the partition's objects:
    /// the primary of a tiered partition that applied the view it started,
    /// as for [`Self::total_retention_end`].
    fn deletes_objects(&self) -> bool {
        let consensus = &self.consensus;
        self.is_tiered()
            && consensus.is_primary()
            && consensus.is_normal()
            && !consensus.is_transferring()
            && consensus.applied_view_start()
    }

    /// The delete state of the current view. What an earlier view left is
    /// dropped, and its job cancelled: a primary only acts on what it learned
    /// while it has been the primary without a break.
    fn object_deletes_of_this_view(&mut self) -> &mut ObjectDeletes {
        let view = self.consensus.view();
        if self.object_deletes.view != Some(view) {
            self.object_deletes = ObjectDeletes {
                view: Some(view),
                ..ObjectDeletes::default()
            };
        }
        &mut self.object_deletes
    }

    /// Queue the objects of the segment from `start_offset` to `end_offset`
    /// for their delete, when this replica is the one that deletes. Any
    /// other replica keeps nothing: the next primary finds the objects by
    /// listing them.
    fn retire_segment_objects(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        retired_at: IggyTimestamp,
    ) {
        if !self.deletes_objects() {
            return;
        }
        self.object_deletes_of_this_view()
            .retired
            .retire(start_offset, end_offset, retired_at);
    }

    /// The last offset a segment may hold for its objects to be deleted: the
    /// lower of the committed truncation watermark and this replica's own
    /// trim point. `None` while either is missing. A watermark of zero is
    /// the one metadata starts with, so it proves nothing.
    fn object_delete_bound(&self, committed_up_to: u64) -> Option<u64> {
        let trim_point = self.chain_trim_point()?;
        (committed_up_to > 0).then(|| committed_up_to.min(trim_point))
    }

    fn segment_start_offsets(&self) -> Vec<u64> {
        self.log
            .segments()
            .iter()
            .map(|segment| segment.start_offset)
            .collect()
    }

    /// Start the next object-store job of this partition's deletes, when
    /// this replica is the primary and none is in flight. The caller runs it
    /// under the returned scope and hands the result to
    /// [`Self::finish_object_delete`].
    ///
    /// The first job of a primary lists the incarnation's objects, since the
    /// segments an earlier primary queued are not known here, and reads
    /// where the ones that may be gone end. Every later
    /// job deletes the objects of the queued segments whose grace period
    /// ended by `now` and that end at or below both `committed_up_to`, the
    /// truncation watermark metadata holds, and this replica's trim point.
    ///
    /// A replica that is not the primary drops what it queued and cancels
    /// the job it started, so a pass over a demoted replica is what stops
    /// its job.
    pub fn begin_object_delete(
        &mut self,
        now: IggyTimestamp,
        committed_up_to: u64,
    ) -> Option<(crate::ObjectDeleteJob, iggy_object_store::TierScopeToken)> {
        if !self.deletes_objects() {
            self.object_deletes = ObjectDeletes::default();
            return None;
        }
        let prefix = self.object_prefix()?;
        let bound = self.object_delete_bound(committed_up_to)?;
        let deletes = self.object_deletes_of_this_view();
        if deletes.running.is_some() || (deletes.swept && deletes.retired.is_empty()) {
            return None;
        }
        let (work, running) = if self.object_deletes.swept {
            let segments = self.log.segments();
            let due = self.object_deletes.retired.due(now, bound, |start_offset| {
                segments
                    .binary_search_by_key(&start_offset, |segment| segment.start_offset)
                    .is_ok()
            });
            if due.is_empty() {
                return None;
            }
            let start_offsets = due.iter().map(|(start_offset, _)| *start_offset).collect();
            (
                crate::ObjectDeleteWork::Delete { start_offsets },
                RunningObjectDelete::Delete(due),
            )
        } else {
            (
                crate::ObjectDeleteWork::Sweep {
                    segment_starts: self.segment_start_offsets(),
                    bound,
                },
                RunningObjectDelete::Sweep { committed_up_to },
            )
        };
        let scope = iggy_object_store::TierScope::new();
        let token = scope.token();
        self.object_delete_jobs += 1;
        let id = self.object_delete_jobs;
        self.object_deletes.running = Some((id, running, scope));
        Some((crate::ObjectDeleteJob { id, prefix, work }, token))
    }

    /// Take the result of the job numbered `id`. A result of a job this
    /// partition does not wait for, or one that arrives after primacy moved,
    /// changes nothing.
    ///
    /// A sweep queues the segments that [`crate::SweptObjects::ended_by`]
    /// proves gone, each with the end its object records, to wait their
    /// grace period from `now`. A sweep that failed is repeated by a later
    /// pass. A delete forgets the segments it removed, and the ones it
    /// did not stay queued.
    pub fn finish_object_delete(
        &mut self,
        id: u64,
        outcome: crate::ObjectDeleteOutcome,
        now: IggyTimestamp,
    ) {
        let awaited = self
            .object_deletes
            .running
            .as_ref()
            .is_some_and(|(running, _, _)| *running == id);
        if !awaited {
            return;
        }
        if !self.deletes_objects() || self.object_deletes.view != Some(self.consensus.view()) {
            self.object_deletes = ObjectDeletes::default();
            return;
        }
        let Some((_, running, _scope)) = self.object_deletes.running.take() else {
            return;
        };
        let namespace_raw = self.namespace().inner();
        match (running, outcome) {
            (
                RunningObjectDelete::Sweep { committed_up_to },
                Ok(crate::ObjectDeleteDone::Swept(Ok(swept))),
            ) => {
                // The watermark the pass came with only trails the one
                // metadata holds now, so it never proves more than is true.
                let found = self
                    .object_delete_bound(committed_up_to)
                    .map_or_default(|bound| swept.ended_by(&self.segment_start_offsets(), bound));
                let deletes = &mut self.object_deletes;
                for (start_offset, end_offset) in &found {
                    deletes
                        .retired
                        .retire_swept(*start_offset, *end_offset, now);
                }
                deletes.swept = true;
                debug!(
                    namespace_raw,
                    listed = swept.listed.len(),
                    queued = found.len(),
                    "listed the objects of the partition for the segments that are gone"
                );
            }
            (RunningObjectDelete::Sweep { .. }, Ok(crate::ObjectDeleteDone::Swept(Err(error)))) => {
                warn!(
                    namespace_raw,
                    %error,
                    "sweeping the objects of the partition failed and is tried again"
                );
            }
            (
                RunningObjectDelete::Delete(sent),
                Ok(crate::ObjectDeleteDone::Deleted { failed }),
            ) => {
                self.object_deletes.retired.settle(&sent, &failed);
                if !failed.is_empty() {
                    warn!(
                        namespace_raw,
                        failed = failed.len(),
                        queued = self.object_deletes.retired.len(),
                        "some objects of removed segments were not deleted and are tried again"
                    );
                }
            }
            // A cancelled job, or a result of the other kind of work, settles
            // nothing: the queue is as the job found it, and a sweep that did
            // not finish runs again.
            (RunningObjectDelete::Sweep { .. } | RunningObjectDelete::Delete(_), _) => {}
        }
    }

    /// Account for a finished upload of the segment in `record`, and return
    /// the advance that tells every replica the store holds it.
    ///
    /// `None` when the advance would reach over offsets that this replica
    /// removed and nothing committed as deleted, as for
    /// `floor_advance_over`. The segment counts as uploaded either
    /// way, and [`Self::pending_floor_advance`] offers its advance once the
    /// watermark covers them.
    pub fn note_segment_uploaded(
        &mut self,
        record: &crate::segment_chain::SegmentRecord,
    ) -> Option<crate::tiered_floor::AdvanceTieredFloor> {
        let oldest = self.oldest_unuploaded_segment();
        let slot = self
            .log
            .segments()
            .binary_search_by_key(&record.start_offset, |segment| segment.start_offset)
            .ok()
            .filter(|slot| self.log.segments()[*slot].location == SegmentLocation::Local);
        if let Some(slot) = slot {
            self.log.segments_mut()[slot].location = SegmentLocation::Uploaded;
            // A segment stored out of turn leaves an older one local, which
            // the cursor must not step over.
            if oldest == Some(slot) {
                self.upload_cursor = record.end_offset.checked_add(1);
            }
        }
        self.floor_advance_over(record)
    }

    /// The advance of an upload that finished and whose segment the floor has
    /// not passed: its request was refused or lost, or has not committed yet.
    ///
    /// Only the oldest segment above the floor can owe one, since an advance
    /// starts right after the floor. `None` while that segment is still
    /// local, so no advance reaches over a segment the store does not hold,
    /// and while offsets this replica removed are not committed as deleted.
    #[must_use]
    pub fn pending_floor_advance(&self) -> Option<crate::tiered_floor::AdvanceTieredFloor> {
        let segment = self.log.segments()[self.first_slot_above_floor()..]
            .iter()
            .find(|segment| segment.sealed && segment.size.as_bytes_u64() > 0)?;
        if segment.location != SegmentLocation::Uploaded {
            return None;
        }
        let record = self.undisputed_segment_record(segment.start_offset)?;
        self.floor_advance_over(&record)
    }

    /// The chain's record of the sealed segment at `start_offset`, unless an
    /// advance contradicted it.
    fn undisputed_segment_record(
        &self,
        start_offset: u64,
    ) -> Option<crate::segment_chain::SegmentRecord> {
        let chain = self.segment_chain.as_ref()?;
        self.sealed_segment_record(start_offset).filter(|record| {
            // An advance that ends anywhere inside the segment and does
            // not match it means the store holds other bytes there.
            chain
                .diverged()
                .range(record.start_offset..=record.end_offset)
                .next()
                .is_none()
        })
    }

    /// The advance that tells every replica the store holds the segment in
    /// `record`. It starts right after the floor, which the floor machine
    /// requires, so it also covers every offset between the floor and the
    /// segment.
    ///
    /// The store holds the ones in this replica's earlier segments, since
    /// uploads go oldest first. It holds none below this replica's first
    /// segment, and only a committed truncation says no other replica holds
    /// them either: each replica expired its own segments before the topic
    /// was tiered. `None` while one of those is above the committed
    /// truncation watermark.
    fn floor_advance_over(
        &self,
        record: &crate::segment_chain::SegmentRecord,
    ) -> Option<crate::tiered_floor::AdvanceTieredFloor> {
        let next = self
            .tiered_floor()
            .map_or(0, |floor| floor.saturating_add(1));
        let front = self.log.segments().first()?.start_offset;
        if let Some(last_removed) = front.checked_sub(1)
            && last_removed >= next
            && last_removed > self.committed_truncation
        {
            return None;
        }
        Some(crate::tiered_floor::AdvanceTieredFloor {
            start: next.min(record.start_offset),
            up_to: record.end_offset,
            hash: record.hash,
        })
    }

    /// Drop the leading advances that the chain holds or a segment settled,
    /// so that choosing the next job looks at one advance however many wait.
    fn drop_recorded_advances(&mut self) {
        let Some(chain) = self.segment_chain.as_ref() else {
            return;
        };
        while self
            .pending_advances
            .front()
            .is_some_and(|advance| !chain.lacks_advance(advance))
        {
            self.pending_advances.pop_front();
        }
    }

    /// A checkpoint keeps the floor and lets the WAL drop the advances that
    /// moved it. One taken before the chain holds them would leave a restarted
    /// replica unable to evict their segments, or any segment after them. It
    /// waits only while the I/O lane is about to record them.
    ///
    /// A failed append is retried after a delay that keeps growing, and only
    /// checkpoints bound the WAL, so a checkpoint does not wait for a retry.
    /// Nor does it wait where the lane never reaches the tiering step.
    fn checkpoint_waits_for_advances(&self) -> bool {
        self.has_io_dispatcher()
            && !self.materialization_missing
            && self
                .maintenance
                .tiering
                .as_ref()
                .is_some_and(|pending| pending.failures == 0)
            && !self.pending_advances.is_empty()
    }

    /// Take a floor advance this replica applied. The store holds the
    /// segment, so this replica may evict its own copy, but only once it has
    /// recorded the advance and its own sealed record carries the same hash.
    pub(crate) fn note_tiered_advance(&mut self, advance: crate::tiered_floor::AdvanceTieredFloor) {
        self.pending_advances.push_back(advance);
        if let Some(prefix) = self.object_prefix() {
            self.request_tiering(prefix);
        }
        self.notify_io();
    }

    /// Act on what the chain learned from a new record: report the advances
    /// that this replica's segments contradict, and stage the eviction of the
    /// segments an advance confirmed.
    fn chain_settled(&mut self, diverged: &[u64]) {
        for up_to in diverged {
            error!(
                namespace_raw = self.namespace().inner(),
                up_to,
                "a tiered floor advance does not match this replica's segment, which stays on disk"
            );
        }
        self.stage_eviction(IggyTimestamp::now());
    }

    /// The last offsets of the floor advances that this replica's own
    /// segments contradict. Any entry means the replica and the store
    /// disagree about committed bytes.
    #[must_use]
    pub fn tiered_divergences(&self) -> Vec<u64> {
        self.segment_chain
            .as_ref()
            .map_or_default(|chain| chain.diverged().iter().copied().collect())
    }

    /// Keep what the topic says stays on local disk, for the evictions that a
    /// floor advance stages between two passes of the segment cleaner.
    pub const fn set_local_envelope(&mut self, envelope: Option<LocalEnvelope>) {
        self.local_envelope = envelope;
    }

    /// Hand the I/O lane the segments that may leave the disk now: sealed,
    /// below the tiered floor, confirmed by an advance and outside the local
    /// envelope.
    pub fn stage_eviction(&mut self, now: IggyTimestamp) {
        let (Some(envelope), Some(floor)) = (self.local_envelope, self.tiered_floor()) else {
            return;
        };
        let Some(through) = self.eviction_end(now, floor, envelope) else {
            return;
        };
        self.maintenance.eviction = Some(
            self.maintenance
                .eviction
                .map_or(through, |previous| previous.max(through)),
        );
        self.notify_io();
    }

    /// Whether the tiering job that is ready lets other work go first. It
    /// does so once after every job, so a long hash does not hold up commits.
    fn tiering_yields(&mut self) -> bool {
        self.maintenance
            .tiering
            .as_mut()
            .is_some_and(|pending| std::mem::take(&mut pending.yielding))
    }

    /// Bytes the next step toward a complete chain reads into one buffer,
    /// `None` when the chain is complete. Moves the scan to the sealed
    /// segment the step hashes.
    fn next_tiering_read_len(&mut self) -> Option<usize> {
        let pending = self.maintenance.tiering.as_mut()?;
        let Some(chain) = self.segment_chain.as_ref() else {
            self.partition_dir.as_ref()?;
            return Some(0);
        };
        let segments = self.log.segments();
        if let Some(removed) = segments.first()?.start_offset.checked_sub(1)
            && chain.trim(removed).is_some()
        {
            return Some(0);
        }
        if self
            .pending_advances
            .front()
            .is_some_and(|advance| chain.lacks_advance(advance))
        {
            return Some(0);
        }
        let scanned = segments.partition_point(|segment| segment.start_offset < pending.scan_from);
        let segment = segments[scanned..]
            .iter()
            .zip(&self.log.storages()[scanned..])
            .find_map(|(segment, storage)| {
                (segment.sealed
                    && segment.size.as_bytes_u64() > 0
                    && chain.lacks_record(segment)
                    && storage.segment_and_index_paths().0.is_some())
                .then_some(segment)
            })?;
        pending.scan_from = segment.start_offset;
        let size = segment.size.as_bytes_u64();
        if !pending
            .digest
            .as_ref()
            .is_some_and(|digest| digest.resumes(segment.start_offset, size))
        {
            pending.digest = Some(Box::new(crate::segment_chain::SegmentDigest::new(
                segment.start_offset,
            )));
        }
        pending.digest.as_ref().map(|digest| digest.step_len(size))
    }

    /// The next step toward a complete chain, `None` when it is complete.
    fn next_tiering_job(&mut self) -> Option<crate::segment_chain::TieringJob> {
        self.next_tiering_read_len()?;
        let pending = self.maintenance.tiering.as_ref()?;
        let Some(chain) = self.segment_chain.as_ref() else {
            return Some(crate::segment_chain::TieringJob::Open {
                directory: self.partition_dir.clone()?,
                prefix: pending.prefix,
            });
        };
        let segments = self.log.segments();
        // The trim point is the last offset retention removed, so a front at
        // zero needs none.
        if let Some(removed) = segments.first()?.start_offset.checked_sub(1)
            && let Some(append) = chain.trim(removed)
        {
            return Some(crate::segment_chain::TieringJob::Append(append));
        }
        // Advances apply in offset order, and the recorded ones were dropped
        // from the front.
        if let Some(append) = self
            .pending_advances
            .front()
            .and_then(|advance| chain.record_advance(advance))
        {
            return Some(crate::segment_chain::TieringJob::Append(append));
        }
        let digest = pending.digest.clone()?;
        let index = segments
            .binary_search_by_key(&pending.scan_from, |segment| segment.start_offset)
            .ok()?;
        Some(crate::segment_chain::TieringJob::Hash {
            log_path: self.log.storages()[index].segment_and_index_paths().0?,
            append: chain.hash_from_disk(&segments[index], self.created_revision())?,
            digest,
        })
    }

    /// A step that fails leaves the chain as it was and runs again after a
    /// delay that grows with every failure in a row.
    fn accept_tiering(&mut self, result: crate::PartitionIoResult) -> Result<(), IggyError> {
        let crate::PartitionIoResult::Tiering(result) = result else {
            return Err(IggyError::CannotWriteToFile);
        };
        let failure = match result {
            crate::segment_chain::TieringResult::Opened(Ok(chain)) => {
                self.adopt_segment_chain(&chain).err()
            }
            crate::segment_chain::TieringResult::Opened(Err(error)) => Some(error),
            crate::segment_chain::TieringResult::Appended(Ok(record)) => {
                let diverged = self
                    .segment_chain
                    .as_mut()
                    .map(|chain| chain.note_appended(&record));
                // A segment that sealed without a record is uploadable only
                // from here on.
                if matches!(record, crate::segment_chain::ChainRecord::Sealed(_)) {
                    self.want_upload();
                }
                self.chain_settled(&diverged.unwrap_or_default());
                None
            }
            crate::segment_chain::TieringResult::Appended(Err(error)) => Some(error.into()),
            crate::segment_chain::TieringResult::Hashed(digest) => {
                if let Some(pending) = &mut self.maintenance.tiering
                    && pending
                        .digest
                        .as_ref()
                        .is_some_and(|captured| digest.continues(captured))
                {
                    pending.digest = Some(digest);
                }
                None
            }
        };
        let Some(failure) = failure else {
            if let Some(pending) = &mut self.maintenance.tiering {
                pending.failures = 0;
                pending.yielding = true;
            }
            return Ok(());
        };
        let transient = matches!(failure, crate::segment_chain::ChainError::Io(_));
        let now = self.consensus.clock_realtime_micros();
        let retry_in_micros =
            self.maintenance
                .tiering
                .as_mut()
                .filter(|_| transient)
                .map(|pending| {
                    let backoff = TIERING_RETRY_BACKOFF_BASE_MICROS
                        << pending.failures.min(TIERING_RETRY_BACKOFF_MAX_SHIFT);
                    pending.failures = pending.failures.saturating_add(1);
                    pending.retry_after_micros = now.saturating_add(backoff);
                    backoff
                });
        if retry_in_micros.is_none() {
            self.maintenance.tiering = None;
        }
        error!(
            namespace_raw = self.namespace().inner(),
            %failure,
            retry_in_micros,
            "cannot complete the partition's segment chain"
        );
        Ok(())
    }

    fn eviction_is_ready(&mut self, through: u64) -> bool {
        let segments = self.log.segments();
        let slot = self.first_local_slot();
        let next = segments
            .get(slot)
            .filter(|_| slot + 1 < segments.len())
            .filter(|segment| {
                segment.sealed
                    && segment.end_offset <= through
                    && self.segment_chain.as_ref().is_some_and(|chain| {
                        chain.evictable(segment.start_offset, segment.end_offset)
                    })
            });
        let Some(segment) = next else {
            self.maintenance.eviction = None;
            return false;
        };
        let (start_offset, end_offset) = (segment.start_offset, segment.end_offset);
        if self.persistence_checkpoint_pending() {
            return false;
        }
        // The WAL links its tail and checkpoint-cursor segments and boot reads
        // the latter, so evicting either would wedge every replica's boot.
        if let Some(persistence) = &self.persistence
            && let Some(checkpoint) = persistence.segment_checkpoint()
            && (start_offset >= checkpoint.start_offset || end_offset >= checkpoint.next_offset)
        {
            persistence.request_checkpoint();
            return false;
        }
        true
    }

    /// The slot of the first segment still on local disk. Remote segments are
    /// always the front of the list, so a binary search finds where they end
    /// without walking a prefix that grows with every eviction.
    fn first_local_slot(&self) -> usize {
        self.log
            .segments()
            .partition_point(|segment| segment.location == SegmentLocation::Remote)
    }

    fn capture_segment_eviction(&self) -> Option<crate::PartitionIoJob<SB>> {
        let slot = self.first_local_slot();
        let start_offset = self.log.segments().get(slot)?.start_offset;
        let messages_path = self.log.storages()[slot].segment_and_index_paths().0;
        let job = self
            .segment_chain
            .as_ref()?
            .evict(start_offset, messages_path)?;
        self.clear_state_transfer_offer_cache();
        Some(crate::PartitionIoJob::EvictSegment(Box::new(job)))
    }

    /// Mark the evicted segment `Remote` and drop what still names its `.log`,
    /// as retention does after an unlink. Returns whether the segment left the
    /// disk.
    ///
    /// A record or an unlink that may not be durable fails like any other
    /// durability failure. A failed unlink only ends the pass: the record is
    /// durable, so the next pass retries the unlink alone.
    fn accept_segment_eviction(
        &mut self,
        result: crate::PartitionIoResult,
    ) -> Result<bool, IggyError> {
        let crate::PartitionIoResult::SegmentEvicted(evicted) = result else {
            return Err(IggyError::CannotDeleteFile);
        };
        let start_offset = evicted.start_offset;
        if evicted.appended
            && let Some(chain) = self.segment_chain.as_mut()
        {
            chain.note_evicted(start_offset);
        }
        match evicted.outcome {
            Ok(()) => {}
            Err(crate::segment_chain::EvictionFailure::Unlink) => {
                self.maintenance.eviction = None;
                return Ok(false);
            }
            Err(crate::segment_chain::EvictionFailure::Record) => {
                return Err(IggyError::CannotWriteToFile);
            }
            Err(crate::segment_chain::EvictionFailure::Sync) => {
                return Err(IggyError::CannotSyncFile);
            }
        }
        let slot = self
            .log
            .segments()
            .binary_search_by_key(&start_offset, |segment| segment.start_offset)
            .map_err(|_| IggyError::CannotDeleteFile)?;
        self.log.segments_mut()[slot].location = SegmentLocation::Remote;
        self.log.storages_mut()[slot].messages_reader = None;
        self.log.reset_read_state(slot);
        self.log.journal().inner.forget_segment(start_offset);
        self.segment_checksum_cache
            .borrow_mut()
            .remove(&start_offset);
        self.clear_state_transfer_offer_cache();
        Ok(true)
    }

    pub(crate) async fn install_empty_segment(
        &mut self,
        config: &PartitionsConfig,
        start_offset: u64,
    ) -> Result<(), IggyError> {
        if self.partition_dir.is_none() && self.persistence.is_none() {
            self.log.add_persisted_segment(
                Segment::new(start_offset, self.effective_segment_size()),
                SegmentStorage::default(),
                None,
                None,
            );
            return Ok(());
        }
        let job = self.prepare_empty_segment(config, start_offset);
        let segment = job.execute().await?;
        self.accept_empty_segment(segment);
        Ok(())
    }

    pub(crate) fn prepare_empty_segment(
        &self,
        config: &PartitionsConfig,
        start_offset: u64,
    ) -> crate::io::SegmentIoJob {
        let namespace = self.namespace();
        let (messages_path, index_path) = self.partition_dir().map_or_else(
            || {
                (
                    config.get_messages_path(
                        namespace.stream_id(),
                        namespace.topic_id(),
                        namespace.partition_id(),
                        start_offset,
                    ),
                    config.get_index_path(
                        namespace.stream_id(),
                        namespace.topic_id(),
                        namespace.partition_id(),
                        start_offset,
                    ),
                )
            },
            |directory| {
                (
                    format!("{directory}/{start_offset:0>20}.log"),
                    format!("{directory}/{start_offset:0>20}.index"),
                )
            },
        );
        crate::io::SegmentIoJob {
            messages_path,
            index_path,
            start_offset,
            segment_size: self.effective_segment_size(),
            persisted: self.durability().is_persisted(),
            preallocate: self.effective_preallocate_segments(config),
            segment_bodies: self.persistence.is_some(),
            old_index: None,
            chain_append: None,
        }
    }

    pub(crate) fn accept_empty_segment(&mut self, installed: crate::io::InstalledSegment) {
        self.segment_names_dirty.set(true);
        self.log.add_persisted_segment(
            installed.segment,
            installed.storage,
            installed.messages_writer,
            installed.index_writer,
        );
    }

    /// Re-anchor the append point after boot re-seeded the offset counter above
    /// what the recovered segment chain holds.
    ///
    /// A hole INSIDE a segment is not survivable: `recover_segment_bounds` walks
    /// a segment from its FILENAME with a running `expected_offset` and REFUSES
    /// at the first offset that does not continue it (`OffsetDiscontinuity`),
    /// which on a solo group tombstones the partition. A surviving index does not
    /// help: a first entry that is not the file-name offset makes recovery
    /// discard the index and walk from byte 0, reaching the same refusal. On a
    /// segment BOUNDARY every reader copes -- absolute offsets in the index,
    /// `disk_poll_start` walking on into later segments, and a chain guard that
    /// admits a forward gap the reservation covers.
    ///
    /// So an empty tail is unlinked (its name claims a range it does not hold), a
    /// sized tail (the only copy of its messages) is sealed with a fresh segment
    /// planted at the append point, and a chain the unlinks emptied is planted
    /// directly -- `ensure_initial_segment` names its segment for the COMMITTED
    /// frontier and would put the first mint inside it.
    ///
    /// # Errors
    /// [`IggyError`] when the fresh segment cannot be created, leaving the
    /// partition without a serviceable chain.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn reanchor_to_offset_frontier(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<(), IggyError> {
        if let Some(persistence) = self.persistence.as_ref().map(Rc::clone) {
            // Retained WAL bodies still belong to the old chain. Materialize
            // and checkpoint them before moving the append point past it.
            while self.consensus.commit_min() < self.consensus.commit_max() {
                let before = self.consensus.commit_min();
                self.commit_journal(config).await;
                if self.consensus.commit_min() == before {
                    return Err(IggyError::CannotSyncFile);
                }
            }
            if !self.flush_committed_messages(config).await? {
                return Err(IggyError::CannotSyncFile);
            }
            persistence.request_checkpoint();
            self.checkpoint_persistence(config).await;
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|_| IggyError::CannotSyncFile)?;
            if self.fatal.is_some() || persistence.checkpoint_op() != persistence.head() {
                return Err(IggyError::CannotSyncFile);
            }
        }
        // Where the next append will land: the counter, or an armed mint floor
        // above it. The floor is the whole reason a hole can appear, so
        // anchoring to the counter alone would leave the chain as unprepared.
        let frontier = self.mint_frontier();
        if frontier == 0 {
            return Ok(());
        }
        let namespace = self.namespace();
        let mut retired = 0usize;
        while let Some(segment) = self.log.segments().last() {
            if segment.size.as_bytes_u64() > 0 || segment.start_offset >= frontier {
                break;
            }
            let Some((segment, storage)) = self.log.retire_back() else {
                break;
            };
            let (messages_path, index_path) = storage.segment_and_index_paths();
            for path in messages_path
                .into_iter()
                .chain(index_path)
                .chain(self.anchor_cleanup_path(segment.start_offset))
            {
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        // Refused, not logged. The segment is already out of the
                        // in-memory chain, so a file left behind becomes a
                        // non-tail empty segment as soon as the plant lands --
                        // `[sized][stale empty][planted]` -- which the next boot
                        // refuses outright as `EmptyNonTailSegment`. Failing boot
                        // here says so while the directory is still readable.
                        error!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            namespace_raw = namespace.inner(),
                            path = %path,
                            %error,
                            "failed to unlink a stale empty segment during the boot \
                             re-anchor; refusing to plant beside it"
                        );
                        return Err(IggyError::CannotDeleteFile);
                    }
                }
            }
            tracing::info!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                start_offset = segment.start_offset,
                offset_frontier = frontier,
                "unlinked an empty segment named below the restored offset frontier"
            );
            // Boot DOES count the recovered chain -- `load_persisted_segments`
            // increments per segment before it looks at the size, so empty tails
            // are in the total -- and retention pairs its own retire with a
            // decrement. Without this the count stays one high on the wire for
            // the life of the process.
            self.stats.decrement_segments_count(1);
            retired += 1;
        }
        // Durable before anything is planted beside them: a crash in between
        // would boot the stale name back into the chain. Refused, not logged:
        // the emptied-chain arm plants through `install_empty_segment`, which
        // fsyncs no directory of its own, so a swallowed error here is the whole
        // promise gone.
        if retired > 0
            && let Some(partition_dir) = self.partition_dir.clone()
            && let Err(error) = crate::state_transfer::fsync_dir(&partition_dir).await
        {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                partition_dir,
                %error,
                "boot re-anchor could not fsync the partition dir after unlinking; \
                 refusing to plant beside a name that may come back"
            );
            return Err(IggyError::CannotSyncFile);
        }
        // Bounds copied out: the plant below takes `&mut self`, so the borrow on
        // the chain cannot still be live.
        let tail = self
            .log
            .segments()
            .last()
            .map(|segment| (segment.start_offset, segment.end_offset, segment.size));
        match tail {
            // An EMPTIED chain still needs the plant, and it cannot be left to
            // the caller's `ensure_initial_segment`, which names the segment for
            // the COMMITTED frontier and knows nothing of the append point. On
            // the shape a crash before the first flush leaves -- committed
            // frontier 0, append point a lease block up -- that plants
            // `0.log` and then mints inside it, the hole this function exists to
            // prevent. The index does not save it either: a first entry that is
            // not the file-name offset makes recovery discard the index and walk
            // from byte 0, where the discontinuity tombstones the partition.
            //
            // No anchor: with nothing before it the plant leaves no gap, so the
            // chain guard has no pair to judge.
            None => {
                self.install_empty_segment(config, frontier).await?;
                self.stats.increment_segments_count(1);
                tracing::info!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    offset_frontier = frontier,
                    "planted a fresh segment at the restored offset frontier over an \
                     empty recovered chain"
                );
            }
            // Only a SIZED tail: an empty one either just went, or is already
            // named at the frontier and can take the appends as it is.
            Some((sealed_start, sealed_end, size))
                if size.as_bytes_u64() > 0 && sealed_end.saturating_add(1) < frontier =>
            {
                self.record_reanchor_gap(frontier, sealed_start, sealed_end)
                    .await?;
                self.rotate_segment_at(config, frontier).await?;
                tracing::info!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    sealed_end,
                    offset_frontier = frontier,
                    "sealed the recovered tail and planted a fresh segment at the \
                     restored offset frontier"
                );
            }
            Some(_) => {}
        }
        if let Some(persistence) = self.persistence.as_ref().map(Rc::clone)
            && persistence.segment_checkpoint().is_some_and(|checkpoint| {
                checkpoint.next_offset < self.log.active_segment().start_offset
            })
        {
            persistence.reanchor_segments(self.log.active_segment().start_offset);
            self.start_persistence();
            persistence
                .drain_with_timeout(|timeout| self.consensus.message_bus().sleep(timeout))
                .await
                .map_err(|error| {
                    warn!(%error, "cannot reanchor the partition WAL at its reserved offset");
                    IggyError::CannotSyncFile
                })?;
        }
        Ok(())
    }

    /// Write the anchor that makes the gap a plant at `frontier` leaves
    /// legitimate, and make it durable before the segment exists.
    ///
    /// The chain guard admits a forward gap only when the far side carries an
    /// anchor naming exactly the near side, so this record is what separates the
    /// re-anchor's own gap from a lost segment. Ordering is load-bearing in one
    /// direction only: an anchor with no segment is swept at the next boot,
    /// while a segment with no anchor reads as damage.
    ///
    /// # Errors
    /// [`IggyError::CannotCreateSegmentLogFile`] naming the anchor path. The
    /// caller must not plant.
    #[allow(clippy::future_not_send)]
    async fn record_reanchor_gap(
        &self,
        frontier: u64,
        sealed_start: u64,
        sealed_end: u64,
    ) -> Result<(), IggyError> {
        // No directory means an in-memory partition, whose chain no boot reads.
        let Some(partition_dir) = self.partition_dir() else {
            return Ok(());
        };
        let anchor = crate::segment_anchor::SegmentAnchor {
            planted_start: frontier,
            sealed_start,
            sealed_end,
        };
        if let Err(error) = crate::segment_anchor::write_anchor(&partition_dir, anchor).await {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                offset_frontier = frontier,
                sealed_start,
                sealed_end,
                %error,
                "could not record the boot re-anchor's gap; refusing to plant a segment \
                 the next boot would read as a lost one"
            );
            return Err(IggyError::CannotCreateSegmentLogFile(
                crate::segment_anchor::anchor_path(&partition_dir, frontier),
            ));
        }
        Ok(())
    }

    pub(crate) async fn transition_superblock(
        &self,
        intent: SuperblockIntent,
    ) -> crate::PartitionIoVerdict {
        if self.has_io_dispatcher() {
            self.request_superblock_intent(intent)
        } else if self.complete_superblock_intent(intent).await {
            crate::PartitionIoVerdict::Ready
        } else {
            crate::PartitionIoVerdict::Failed
        }
    }

    pub(crate) fn offset_cleanup_charge(&self, kind: ConsumerKind) -> Option<usize> {
        // Bound hash-table load factor and bucket rounding. The file-phase
        // control reserve covers the trailing control group and alignment.
        const HASH_BUCKETS_PER_ID_MAX: usize = 4;
        self.offset_cleanup_known_count(kind)?
            .checked_mul(HASH_BUCKETS_PER_ID_MAX)?
            .checked_mul(size_of::<u32>() + size_of::<u8>())
    }

    pub(crate) fn capture_offset_cleanup(
        &self,
        kind: ConsumerKind,
        retained: Vec<u32>,
    ) -> Result<crate::PartitionIoJob<SB>, IggyError> {
        let mut known = HashSet::with_capacity(
            self.offset_cleanup_known_count(kind)
                .ok_or(IggyError::CannotDeleteFile)?,
        );
        self.consumer_offset_capacity_for(kind)
            .extend_stranded_ids(&mut known);
        // The durable table names every file a committed offset op wrote.
        self.durable_consumer_offsets
            .with_entries(kind, |entries| known.extend(entries.keys()));
        Ok(crate::PartitionIoJob::OffsetCleanup {
            directory: self
                .consumer_offsets_dir(kind)
                .ok_or(IggyError::CannotDeleteFile)?
                .to_owned(),
            known,
            retained,
        })
    }

    fn offset_cleanup_known_count(&self, kind: ConsumerKind) -> Option<usize> {
        self.consumer_offset_capacity_for(kind)
            .stranded_count()
            .checked_add(
                self.durable_consumer_offsets
                    .with_entries(kind, HashMap::len),
            )
    }

    /// `end_offset` of the `count`-th oldest sealed (non-active) segment, used
    /// to resolve a client `DeleteSegments` count into a concrete truncation
    /// offset on the owning shard. `None` when there are no deletable sealed
    /// segments; clamps to the last sealed segment when fewer than `count`
    /// exist.
    #[must_use]
    pub fn nth_oldest_sealed_end_offset(&self, count: u32) -> Option<u64> {
        nth_oldest_sealed_end(self.log.segments(), count)
    }

    /// Settle the session floor that a `RangeEvicted` reply reports.
    pub const fn note_repair_floor(&mut self, floor: u64) {
        if let Some(session) = self.repair.as_mut() {
            session.floor = Some(floor);
        }
    }

    /// Bytes that repair journaled above `commit_min` while the view log was
    /// unproven.
    const fn proof_walk_bytes(&self) -> u64 {
        let (commit_min, bytes) = self.proof_walk;
        if commit_min == self.consensus().commit_min() {
            bytes
        } else {
            0
        }
    }

    fn refresh_proof_walk_memory(&mut self) {
        if !self.consensus.view_log_unproven() || self.proof_walk.0 != self.consensus.commit_min() {
            self.reset_proof_walk_memory();
        }
    }

    pub(crate) fn reset_proof_walk_memory(&mut self) {
        if let Some(memory) = &mut self.proof_walk_memory {
            memory.clear();
        }
        self.proof_walk = (self.consensus.commit_min(), 0);
    }

    /// Whether an unproven repair exceeded its WAL budget or exhausted the
    /// shared allocation budget before accepting another frame.
    ///
    /// The walk fetches ops from the adopted head down, and nothing above
    /// `commit_min` commits before it reaches `commit_min`, so every body it
    /// fetches stays in memory until then. The WAL budget already caps the
    /// prepares that a partition holds above its WAL checkpoint, so the walk
    /// can hold as much, plus the one body that crosses it. Past that, state
    /// transfer takes the window. Bodies that a later truncation drops still
    /// count, so the bound errs toward transfer. The node's allocation budget
    /// also applies without a WAL and never admits the body that exceeds it.
    fn proof_walk_over_budget(&self) -> bool {
        self.consensus().view_log_unproven()
            && (self
                .proof_walk_memory
                .as_ref()
                .is_some_and(crate::repair_memory::RepairMemoryReservation::exhausted)
                || self
                    .persistence
                    .as_ref()
                    .is_some_and(|persistence| self.proof_walk_bytes() > persistence.capacity()))
    }

    /// Ingest one repaired prepare: journal + stage it exactly like a live
    /// replicated op, minus the view-number check, the gap check, and the ack (the
    /// op is already committed cluster-wide; there is nobody to ack to). The
    /// commit walk runs at `RepairDone`, after the floor is known.
    #[allow(clippy::too_many_lines)]
    pub async fn apply_repaired_prepare(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> RepairIngest {
        self.refresh_proof_walk_memory();
        let header = *message.header();
        if self.view_truncation_pending() || !self.prepare_history_available(&header) {
            return RepairIngest::Handled;
        }
        // The I/O lane already cut the WAL for a broken fenced window; an entry
        // journaled now would outlive it there. Refetched once the cut lands.
        if self.unproven_drop_from.is_some() {
            return RepairIngest::Handled;
        }
        let Some(session) = self.repair else {
            return RepairIngest::Handled;
        };
        let consensus = self.consensus();
        // NOT `is_normal` alone: a primary-elect repairing toward its parked
        // merged log runs this in `ViewChange`, and dropping the session on its
        // first inbound frame leaves the coverage scan re-arming every tick over
        // a stream it can never keep.
        if !repair_session_live(consensus) || consensus.view() != session.view {
            self.repair = None;
            return RepairIngest::Handled;
        }
        if header.op <= consensus.commit_min() || header.op > session.fetch_to_op {
            return RepairIngest::Handled;
        }
        // Bodies name no sender or request, so only this replica's view log can vouch.
        let journal = &self.log.journal().inner;
        let held_at = |op| {
            journal
                .header_by_op(op)
                .map(|held| (held, consensus.view_log_entry_proven(op)))
        };
        let (named, evidence) = consensus
            .with_pending_view_log(|pending| {
                (
                    pending.headers.iter().any(|expected| {
                        expected.op == header.op
                            && dvc_header_kind(expected) != DvcHeaderKind::Blank
                    }),
                    repaired_prepare_evidence(&header, consensus.view(), Some(pending), held_at),
                )
            })
            .unwrap_or_else(|| {
                (
                    false,
                    repaired_prepare_evidence(&header, consensus.view(), None, held_at),
                )
            });
        if header.op > session.commit_to_op && !named {
            return RepairIngest::Handled;
        }
        // An unproven frame may wait for `view_log_proof` only where the walk
        // still covers its op: nothing journaled there is committed, served or
        // reported before the walk proves it against the view's hash chain.
        if evidence == RepairEvidence::Contradicted
            || (evidence == RepairEvidence::Unproven && !consensus.view_log_proof_covers(header.op))
        {
            return RepairIngest::Unvouched;
        }
        // A held frame is no progress: a peer that re-serves the same chunk
        // every round must still spend the budget, or it cannot be rotated
        // away from.
        if journal.header_by_op(header.op).is_some() {
            self.consensus.note_repaired_view_header(header, evidence);
            return RepairIngest::Handled;
        }
        // Any other in-window frame proves the stream is alive; only silence
        // should age the stall counter.
        if let Some(session) = self.repair.as_mut() {
            session.idle_ticks = 0;
        }
        if self.materialization_missing
            || self.transition.is_some()
            || self.io_active.is_retiring()
            || self.consensus.is_transferring()
            || self.maintenance.flush_drain.is_some()
            || self.maintenance.shutdown.is_some()
        {
            // The pause keeps this frame out of the journal, not a fault in
            // the frame, so it still refunds the budget.
            self.restart_repair_budget();
            return RepairIngest::Handled;
        }
        // A walk past its budget journals nothing more, and `complete_repair`
        // hands the window to state transfer.
        if self.proof_walk_over_budget() {
            return RepairIngest::Handled;
        }
        let charge = if self.consensus.view_log_unproven() {
            message.retained_allocation_size().unwrap_or(usize::MAX)
        } else {
            0
        };
        if let Some(memory) = &mut self.proof_walk_memory
            && !memory.try_add(charge)
        {
            return RepairIngest::Handled;
        }
        let applied = if header.operation == Operation::SendMessages {
            self.append_repaired_send_messages(message).await
        } else {
            self.apply_replicated_operation(message).await.map(|_| ())
        };
        if let Err(error) = applied {
            if let Some(memory) = &mut self.proof_walk_memory {
                memory.release(charge);
            }
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                op = header.op,
                %error,
                "failed to journal repaired prepare"
            );
            return RepairIngest::Handled;
        }
        // A frame the journal refuses is no progress: it stays not held, so
        // its peer serves it again every round, and a refund per serve would
        // pin the session to that peer.
        self.restart_repair_budget();
        if self.consensus().view_log_unproven() {
            self.proof_walk = (
                self.consensus().commit_min(),
                self.proof_walk_bytes() + u64::from(header.size),
            );
        }
        self.consensus.note_repaired_view_header(header, evidence);
        self.refresh_proof_walk_memory();
        self.persist_repaired_prefix();
        self.advance_head_over_journaled_run();
        self.offset_reservations_need_resync.set(true);
        RepairIngest::Handled
    }

    /// Conclude a repair stream: settle the commit floor at the serving
    /// peer's eviction point (everything below it is represented by this
    /// replica's recovered segments + offset files) and walk the repaired
    /// window through the normal commit path.
    #[allow(clippy::too_many_lines)]
    pub async fn complete_repair(&mut self, config: &PartitionsConfig) -> RepairConclusion {
        if self.history_is_busy() {
            return RepairConclusion::InProgress;
        }
        let Some(mut session) = self.repair else {
            return RepairConclusion::Done;
        };
        if !consensus::repair_session_live(self.consensus())
            || self.consensus().view() != session.view
        {
            self.repair = None;
            return RepairConclusion::Done;
        }
        if self.proof_walk_over_budget() {
            let node_memory_exhausted = self
                .proof_walk_memory
                .as_ref()
                .is_some_and(crate::repair_memory::RepairMemoryReservation::exhausted);
            tracing::info!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                walk_bytes = self.proof_walk_bytes(),
                node_memory_exhausted,
                "unproven repair walk is past its memory budget, so state transfer takes the window"
            );
            self.repair = None;
            return RepairConclusion::FloorRefused {
                floor: self.consensus().commit_min(),
                to_op: session.commit_to_op,
            };
        }
        let journal = &self.log.journal().inner;
        // Apply a resident prefix normally instead of substituting the peer's
        // floor for operations recovered after boot.
        session.floor = session.floor.filter(|&floor| {
            !journal
                .repaired_window_shape(self.consensus().commit_min(), floor)
                .complete
        });
        self.persist_repaired_prefix();
        // With a prepare WAL every op in `(commit_min, head]` is resident, so a
        // floor the filter kept is above the head. The WAL grows only at
        // `head + 1` and the peer retains nothing at or below the floor, so
        // this floor can never become durable here and its refusal is final.
        if let Some(floor) = session.floor
            && self
                .persistence
                .as_ref()
                .is_some_and(|persistence| floor > persistence.head())
        {
            self.commit_journal(config).await;
            self.repair = None;
            return RepairConclusion::FloorRefused {
                floor,
                to_op: session.commit_to_op,
            };
        }
        // A floor at or below the live commit point is moot: it cannot move
        // `commit_min`, so nothing below it is skipped and the connection
        // check has no jump to guard. Verifying it could only refuse: below
        // `commit_to_op` the window never proved complete, because the ops
        // between the floor and `commit_min` are committed with their headers
        // evicted, and at `commit_to_op` the empty window refused outright.
        if let Some(floor) = session
            .floor
            .filter(|&floor| floor > self.consensus().commit_min())
        {
            // Offset continuity cannot certify that recovered bytes belong to
            // the adopted history. An evicted proof gap needs state transfer.
            // So does any jump past commands, which no machine would apply,
            // while a transfer carries the machines' snapshot.
            if self.consensus().view_log_unproven() || !self.state_machines.is_empty() {
                self.repair = None;
                return RepairConclusion::FloorRefused {
                    floor,
                    to_op: session.commit_to_op,
                };
            }
            // A peer may have evicted past this replica's commit frontier;
            // an unclamped floor would drive commit_min above commit_max and
            // panic the next advance.
            let floor = floor.min(self.consensus().commit_max());
            // The floor claims "recovered durable state stands in below me".
            // Verify it: the served window must connect to the recovered
            // segments. A window starting above the durable end means ops
            // below the floor are neither locally durable nor repaired --
            // that gap is state-transfer territory, and accepting the floor
            // would silently serve a holed log. Refuse and stay gap-stopped:
            // a visible stall beats invisible loss.
            let durable_end = self.recovered_durable_offset;
            // Recovered bytes and an installed frontier both "stand in"
            // below the floor; a window connecting to either is whole. `None`
            // orders below every `Some`, so the join covers all four
            // combinations.
            let stand_in = durable_end
                .map(|durable| durable.saturating_add(1))
                .max(self.installed_frontier);
            let committed_shape = journal.repaired_window_shape(floor, session.commit_to_op);
            let anchor = self.repair_window_anchor(floor, session.commit_to_op);
            let connected = match (anchor, stand_in) {
                (Some(anchor), Some(bound)) => anchor.base_offset <= bound,
                (Some(anchor), None) => anchor.base_offset == 0,
                // No batch above the floor, so nothing ties the floor to this
                // replica's offsets. The floor stands only while this journal
                // lacks an op up to the reported floor, and no shape of the
                // window above it shows that the ops the jump skips carried no
                // messages. Refused, the window goes to state transfer.
                (None, _) => false,
            };
            if connected {
                if floor > self.consensus().commit_min() {
                    self.consensus().set_commit_floor(floor);
                }
            } else if let Some(conclusion) = self
                .refuse_disconnected_floor(
                    config,
                    session,
                    floor,
                    durable_end,
                    anchor,
                    committed_shape.complete,
                )
                .await
            {
                return conclusion;
            }
        }
        // Apply verified prefixes before the whole window is durable so a
        // checkpoint can reclaim WAL capacity for the remaining repair.
        let before = self.consensus().commit_min();
        self.commit_journal(config).await;
        let commit_min = self.consensus().commit_min();
        // Completion is decided HERE, not by the peer's served-through
        // claim: repair frames ride a lossy best-effort bus, so a stream
        // the peer fully served can still arrive with holes. Only a walk
        // that reached the requested frontier closes the session; anything
        // less keeps it armed and the stall retry re-requests the remains
        // (`commit_min + 1..`), converging over rounds.
        // The residency check starts at the LIVE commit point, not the
        // session's snapshotted one: delivering the suffix bodies is what
        // lets the group commit past `commit_to_op`, and the `commit_journal`
        // above then evicts exactly those headers. Judged from
        // `commit_to_op`, a fully successful repair would report itself
        // incomplete forever, pin the session, and block every later re-arm
        // until a view change. Ops at or below `commit_min` are committed and
        // applied, a monotone fact the flush cannot erase, so they need no
        // resident header to count as fetched.
        let fetch_complete = session.fetch_to_op <= session.commit_to_op
            || self
                .log
                .journal()
                .inner
                .repaired_window_shape(session.commit_to_op.max(commit_min), session.fetch_to_op)
                .complete;
        // A completed repair may vote in the next view at once, so the whole
        // window must be durable, also the part a history fence held back.
        let window_durable = self
            .persistence
            .as_ref()
            .is_none_or(|persistence| persistence.is_durable_through(session.fetch_to_op));
        let done = commit_min >= session.commit_to_op && fetch_complete && window_durable;
        if done {
            self.repair = None;
            self.transfer_rearm = None;
        }
        tracing::info!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = self.namespace().inner(),
            commit_min_before = before,
            commit_min_after = commit_min,
            commit_max = self.consensus().commit_max(),
            commit_to_op = session.commit_to_op,
            fetch_to_op = session.fetch_to_op,
            fetch_complete,
            done,
            "repair window commit walk finished"
        );
        if done {
            RepairConclusion::Done
        } else {
            RepairConclusion::InProgress
        }
    }

    /// Judge a floor whose window may not connect to recovered durable state.
    ///
    /// `Some(FloorRefused)` once the refusal is final, `Some(InProgress)` while
    /// a frame still to come can land below the anchor and connect, or `None`
    /// when the walk below makes the floor moot and completion proceeds as
    /// without one.
    async fn refuse_disconnected_floor(
        &mut self,
        config: &PartitionsConfig,
        session: RepairSession,
        floor: u64,
        durable_end: Option<u64>,
        anchor: Option<RepairAnchor>,
        window_complete: bool,
    ) -> Option<RepairConclusion> {
        self.commit_journal(config).await;
        // Ops this replica already held can carry the walk past the floor.
        // Under lazy flush their headers stay resident, so the leading edge
        // below would read as resident and refuse a floor that no longer
        // moves `commit_min`, spending a state transfer on nothing. Judged by
        // the peer's unclamped floor: one clamped down to `commit_max` still
        // names ops the peer evicted.
        if session
            .floor
            .is_some_and(|raw_floor| self.consensus().commit_min() >= raw_floor)
        {
            return None;
        }
        let leading_edge_resident = self.repair_leading_edge_resident(floor, anchor);
        // Definitive once the window, or its leading edge `(floor, anchor.op)`,
        // is resident: until then a frame still to come can land below the
        // anchor and connect. The leading edge suffices because the primary
        // assigns offsets in op order.
        if window_complete || leading_edge_resident {
            let reason = if window_complete {
                "whole_window"
            } else {
                "leading_edge"
            };
            tracing::error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                floor,
                reported_floor = ?session.floor,
                commit_to_op = session.commit_to_op,
                anchor_op = ?anchor.map(|anchor| anchor.op),
                anchor_base_offset = ?anchor.map(|anchor| anchor.base_offset),
                reason,
                recovered_durable_offset = ?durable_end,
                "refusing commit floor: repaired window does not connect \
                 to recovered durable state (needs state transfer)"
            );
            self.repair = None;
            return Some(RepairConclusion::FloorRefused {
                floor,
                to_op: session.commit_to_op,
            });
        }
        tracing::warn!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = self.namespace().inner(),
            floor,
            commit_to_op = session.commit_to_op,
            anchor_op = ?anchor.map(|anchor| anchor.op),
            anchor_base_offset = ?anchor.map(|anchor| anchor.base_offset),
            recovered_durable_offset = ?durable_end,
            "commit floor does not connect yet; repairing below the anchor"
        );
        Some(RepairConclusion::InProgress)
    }

    /// Whether another bounded repair response can fit before checkpointing.
    /// Unproven histories and unresolved floors must fetch before they can
    /// apply a prefix, so WAL backpressure must not stop their proof walks.
    #[must_use]
    pub fn repair_fetch_ready(&self) -> bool {
        self.view_fenced()
            || self.consensus.has_untrusted_log()
            || self.consensus.view_log_unproven()
            || self.consensus.commit_min() >= self.consensus.commit_max()
            || self.repair.is_some_and(|session| {
                session
                    .floor
                    .is_some_and(|floor| floor > self.consensus.commit_min())
            })
            || self.persistence.as_ref().is_none_or(|persistence| {
                persistence.has_capacity(journal::partition_journal::PREPARE_BYTES_MAX)
            })
    }

    /// Where the next pull of the repair window starts: past the resident
    /// prefix above a floor that still holds the walk, else past the held run
    /// above `commit_min`. `None` without a session.
    ///
    /// A held walk never moves `commit_min`, so pulling from it re-serves the
    /// same first chunk forever while the rest of the window stays absent.
    #[must_use]
    pub fn repair_pull_from(&self) -> Option<u64> {
        let session = self.repair.as_ref()?;
        let commit_min = self.consensus().commit_min();
        let journal = &self.log.journal().inner;
        let Some(floor) = session.floor.filter(|&floor| floor > commit_min) else {
            return Some(journal.held_through(commit_min).saturating_add(1));
        };
        let mut op = floor.saturating_add(1);
        while op <= session.fetch_to_op && journal.holds_op(op) {
            op += 1;
        }
        Some(op)
    }

    /// The next chunk to pull right after a `RepairDone` while a floor holds
    /// the walk, or `None` when the resident prefix has not grown since the
    /// last such pull or reaches past `fetch_to_op`. A returned chunk is
    /// recorded in the session's `floor_pulled_from`, so the same start is
    /// never returned twice.
    #[must_use]
    pub fn next_floor_pull(&mut self) -> Option<u64> {
        let commit_min = self.consensus().commit_min();
        let from_op = self.repair_pull_from()?;
        let session = self.repair.as_mut()?;
        let floor_pending = session.floor.is_some_and(|floor| floor > commit_min);
        if !floor_pending || from_op > session.fetch_to_op || from_op <= session.floor_pulled_from {
            return None;
        }
        session.floor_pulled_from = from_op;
        Some(from_op)
    }

    /// Highest op held below the hole that the proof walk waits on, for the
    /// repair request about to go out, whose window starts above it. A donor
    /// serves a window from its bottom up to a disk byte budget, so a window
    /// that starts at held ops gets them again on every round and never
    /// reaches the hole. Records the hole with it for
    /// [`Self::next_proof_walk_pull`].
    pub fn note_proof_walk_request(&self) -> Option<u64> {
        let hole = self.consensus().view_log_repair_op()?;
        let held_below = self.log.journal().inner.held_below(hole);
        self.proof_walk_requested.set((hole, held_below));
        held_below
    }

    /// The unproven hole to pull again right after a `RepairDone` when the
    /// walk gained ops below it since the last request, or `None`. The next
    /// window then starts above them. A round that brought nothing leaves the
    /// hole to the retry timer.
    #[must_use]
    pub fn next_proof_walk_pull(&self) -> Option<u64> {
        let hole = self
            .consensus()
            .view_log_repair_op()
            .filter(|&hole| !self.consensus().view_log_entry_proven(hole))?;
        let walked = (hole, self.log.journal().inner.held_below(hole));
        (walked != self.proof_walk_requested.get()).then_some(hole)
    }

    /// The lowest resident `SendMessages` batch in `(floor, commit_to_op]`:
    /// where the repair window begins in offset space.
    ///
    /// Read from the journal rather than tracked per session. A batch at or
    /// below the floor is not what the floor claims recovered state stands in
    /// for, and a batch resident from an earlier session or the boot WAL
    /// counts as much as one repaired now.
    fn repair_window_anchor(&self, floor: u64, commit_to_op: u64) -> Option<RepairAnchor> {
        let from = floor.checked_add(1)?;
        let (op, entry) = self
            .log
            .journal()
            .inner
            .lowest_resident_send_in(from..=commit_to_op)?;
        // The batch checksum covers `base_offset` and hashes only each frame's
        // stored checksum, so a corrupt header still yields no anchor and the
        // floor is refused, without hashing every message body.
        let batch = decode_prepare_slice_trusted(entry.as_slice())
            .inspect_err(|error| {
                tracing::warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    op,
                    %error,
                    "resident repair batch does not decode; the window has no anchor"
                );
            })
            .ok()?;
        let batch_checksum = batch.header.checksum_for_blob(batch.blob());
        if batch_checksum != batch.header.batch_checksum {
            tracing::warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                op,
                stored = batch.header.batch_checksum,
                computed = batch_checksum,
                "resident repair batch fails its batch checksum; the window has no anchor"
            );
            return None;
        }
        Some(RepairAnchor {
            op,
            base_offset: batch.header.base_offset,
        })
    }

    /// Whether every op in `(floor, anchor.op)` is resident, so no frame still
    /// to come can land below the anchor.
    fn repair_leading_edge_resident(&self, floor: u64, anchor: Option<RepairAnchor>) -> bool {
        anchor.is_some_and(|anchor| {
            self.log
                .journal()
                .inner
                .repaired_window_shape(floor, anchor.op.saturating_sub(1))
                .complete
        })
    }

    /// Journal a repaired `SendMessages` prepare, preserving its embedded
    /// batch stamps. A stored prepare was stamped by `append_messages` on
    /// the serving replica BEFORE it was journaled, so its `base_offset` /
    /// `base_timestamp` / `batch_checksum` are the canonical values every
    /// replica agreed on. Re-stamping from this replica's dirty counter
    /// (what the live path does) mints a second copy of the window at
    /// fresh offsets whenever recovered segments already hold the
    /// originals: the counter sits at the recovered durable END, not at
    /// the op's position in history.
    async fn append_repaired_send_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<(), IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        if !self.prepare_history_available(message.header()) {
            return Err(IggyError::HistoryUnavailable);
        }
        let batch = decode_prepare_slice(message.as_slice())?.header;
        self.append_canonical_send_messages(message, batch).await?;
        Ok(())
    }

    /// Journal a `SendMessages` prepare at the offsets its batch carries: a
    /// repaired op, or a live op that fills a hole below ops repair journaled.
    /// The dirty offset only grows, so a fill below it leaves it in place. The
    /// caller holds the write lock.
    async fn append_canonical_send_messages(
        &mut self,
        message: Message<PrepareHeader>,
        batch: BatchHeader,
    ) -> Result<Frozen<4096>, IggyError> {
        let (base_offset, base_timestamp, total_size, message_count) = (
            batch.base_offset,
            batch.base_timestamp,
            batch.total_size() as u64,
            batch.message_count,
        );
        if message_count == 0 {
            return Err(IggyError::InvalidCommand);
        }

        let last_offset = base_offset
            .checked_add(u64::from(message_count) - 1)
            .ok_or(IggyError::CannotAppendMessage)?;
        let dirty = self.dirty_offset.load(Ordering::Relaxed);

        let segment_index = self.log.segments().len() - 1;
        let current_position = self.log.segments()[segment_index].current_position;
        let next_position = current_position
            .checked_add(total_size)
            .ok_or(IggyError::CannotAppendMessage)?;

        let mut journal_info = self.log.journal().info;
        journal_info.messages_count = journal_info
            .messages_count
            .checked_add(message_count)
            .ok_or(IggyError::CannotAppendMessage)?;
        journal_info.size = IggyByteSize::from(
            journal_info
                .size
                .as_bytes_u64()
                .checked_add(total_size)
                .ok_or(IggyError::CannotAppendMessage)?,
        );
        journal_info.current_offset = last_offset;
        if journal_info.first_timestamp == 0 {
            journal_info.first_timestamp = base_timestamp;
        }
        journal_info.end_timestamp = base_timestamp;
        journal_info.max_timestamp = journal_info.max_timestamp.max(base_timestamp);

        let frozen = message.into_frozen();
        self.journal_append(frozen.clone()).await?;

        self.note_append_live();
        self.dirty_offset
            .store(dirty.max(last_offset), Ordering::Relaxed);
        self.log.segments_mut()[segment_index].current_position = next_position;
        self.log.journal_mut().info = journal_info;
        Ok(frozen)
    }

    /// Retirement requests name sessions rather than histories; their prepares
    /// carry this partition's incarnation. A lifecycle fence names its
    /// incarnation in its body, which admission checks.
    fn request_history_available(&self, header: &RoutedRequestHeader) -> bool {
        header.operation == Operation::RetireSession
            || header.operation.is_partition_lifecycle()
            || (header.partition_incarnation == self.created_revision() && !self.history_deleted())
    }

    const fn prepare_history_available(&self, header: &PrepareHeader) -> bool {
        header.partition_incarnation == self.created_revision()
            && (matches!(header.operation, Operation::RetireSession) || !self.history_deleted())
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) -> bool {
        if self.fatal.is_some() || self.materialization_missing {
            return false;
        }
        // An ack above an unproven window can be wiped when the window turns out
        // to break; the primary's retransmit re-drives it once the fence lifts.
        if self.view_fenced() {
            return false;
        }
        // Durable-before-send: a PrepareOk implies this replica's
        // (view, log_view), so it must not leave until they are durable, or a
        // crash could recover an older view than the one this ack helped
        // commit in, losing a committed op. Mirrors the view-change dispatch
        // gate; withhold on persist failure and let the prepare retransmit
        // (backup) or `retry_local_write` (primary) re-drive the ack once a
        // later persist succeeds.
        if (self.consensus.replica_count() > 1 || self.persistence.is_some())
            && !self.register_rebuilt_ack(header)
        {
            self.ensure_wal_view();
            return false;
        }
        if !self.persist_superblock_if_needed().await {
            if self.persistence.is_some() || self.io_notifier.borrow().is_some() {
                self.pending_persisted_acks
                    .borrow_mut()
                    .insert(header.op, *header);
            }
            return false;
        }
        // `VsrAction::RetransmitPrepares` reads from `self.log.journal`.
        // Both `SendMessages` (via `append_send_messages_to_journal`) and
        // consumer-offset ops (via `apply_replicated_operation`) append
        // to that journal before `send_prepare_ok` fires, so every op
        // that reaches here is journal-backed and ACKs as durable.
        self.record_acknowledged_op(header.op);
        send_prepare_ok_common(self.consensus(), header, true).await
    }

    /// The header of the prepare this replica applied at `op`, while the journal
    /// still holds it: the `applied_at` of
    /// [`VsrConsensus::contradicts_applied_log`].
    ///
    /// This journal keeps no snapshot floor. At or below `commit_min` it holds
    /// what the walk applied, resident until a flush and then in the repair ring
    /// until retention drops it, and the checkpoint prepare a boot or a state
    /// transfer restored at its commit point (a transfer clears every older
    /// entry). A repair floor jump, possible only without a prepare WAL, leaves
    /// entries the walk skipped below the floor; the view log was proven before
    /// the jump, so they are the committed ones. Ingest refuses every op at or
    /// below `commit_min`, so nothing replaces an entry there.
    #[must_use]
    pub fn applied_header(&self, op: u64) -> Option<PrepareHeader> {
        self.log.journal().inner.repair_header(op)
    }
}

/// The batch a repair window's floor-connect check is judged by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RepairAnchor {
    op: u64,
    base_offset: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum DeferredViewPhase {
    StartViewChange,
    DoViewChange,
    StartView,
    RequestStartView,
    StateTransferTarget,
}

fn deferred_view_order(
    message: &server_common::MessageBag,
) -> Option<(u32, DeferredViewPhase, u8)> {
    match message {
        server_common::MessageBag::StartViewChange(message) => Some((
            message.header().view,
            DeferredViewPhase::StartViewChange,
            message.header().replica,
        )),
        server_common::MessageBag::DoViewChange(message) => Some((
            message.header().view,
            DeferredViewPhase::DoViewChange,
            message.header().replica,
        )),
        server_common::MessageBag::StartView(message) => Some((
            message.header().view,
            DeferredViewPhase::StartView,
            message.header().replica,
        )),
        server_common::MessageBag::RequestStartView(message) => Some((
            message.header().view,
            DeferredViewPhase::RequestStartView,
            message.header().replica,
        )),
        server_common::MessageBag::StateTransferTarget(message) => Some((
            message.header().view,
            DeferredViewPhase::StateTransferTarget,
            message.header().replica,
        )),
        _ => None,
    }
}

pub fn next_consumer_kind(kind: ConsumerKind) -> Option<ConsumerKind> {
    ConsumerKind::ALL.get(kind.index() + 1).copied()
}

/// Automatic commits remain monotone because an earlier poll can commit after
/// a later one. Explicit stores may intentionally rewind the cursor.
fn upsert_committed_offset<K>(
    map: &papaya::HashMap<K, ConsumerOffset>,
    key: K,
    offset: u64,
    auto_commit: bool,
    create_on_miss: impl FnOnce() -> ConsumerOffset,
) where
    K: Hash + Eq + Clone + Send + Sync,
{
    if auto_commit {
        upsert_offset_max(map, key, offset, create_on_miss);
    } else {
        upsert_offset(map, key, offset, create_on_miss);
    }
}

fn upsert_offset<K>(
    map: &papaya::HashMap<K, ConsumerOffset>,
    key: K,
    offset: u64,
    create_on_miss: impl FnOnce() -> ConsumerOffset,
) where
    K: Hash + Eq + Clone + Send + Sync,
{
    let guard = map.pin();
    if let Some(existing) = guard.get(&key) {
        existing.offset.store(offset, Ordering::Relaxed);
    } else {
        let created = create_on_miss();
        created.offset.store(offset, Ordering::Relaxed);
        guard.insert(key, created);
    }
}

/// Kinds keyed by consumer group, whose offsets go when the group is deleted.
const fn keyed_by_group(kind: ConsumerKind) -> bool {
    match kind {
        ConsumerKind::Consumer => false,
        ConsumerKind::ConsumerGroup | ConsumerKind::ExternalGroup => true,
    }
}

fn upsert_offset_max<K>(
    map: &papaya::HashMap<K, ConsumerOffset>,
    key: K,
    offset: u64,
    create_on_miss: impl FnOnce() -> ConsumerOffset,
) where
    K: Hash + Eq + Clone + Send + Sync,
{
    let guard = map.pin();
    if let Some(existing) = guard.get(&key) {
        existing.offset.fetch_max(offset, Ordering::Relaxed);
    } else {
        let created = create_on_miss();
        created.offset.store(offset, Ordering::Relaxed);
        guard.insert(key, created);
    }
}

/// The operation tag at the front of a journal entry. Every entry begins with a
/// `PrepareHeader`, so reading the tag is a cheap cast, not a full batch decode;
/// it tells a committed consumer-offset op (no segment bytes) apart from a
/// `SendMessages` batch without relying on a decode failure to do so.
pub fn peek_operation(entry: &Frozen<4096>) -> Operation {
    bytemuck::checked::try_from_bytes::<PrepareHeader>(
        &entry[..std::mem::size_of::<PrepareHeader>()],
    )
    .expect("journal entry must begin with a valid prepare header")
    .operation
}

/// The consensus op of a journal entry, same cheap header cast as
/// [`peek_operation`].
fn peek_op(entry: &Frozen<4096>) -> u64 {
    bytemuck::checked::try_from_bytes::<PrepareHeader>(
        &entry[..std::mem::size_of::<PrepareHeader>()],
    )
    .expect("journal entry must begin with a valid prepare header")
    .op
}

pub fn materialization_batch_meta(
    entry: &Frozen<4096>,
    persisted_end: Option<u64>,
) -> Result<Option<RetainedBatchMeta>, IggyError> {
    if peek_operation(entry) != Operation::SendMessages {
        return Ok(None);
    }
    let batch = decode_prepare_slice_trusted(entry.as_slice())
        .map_err(|_| IggyError::CannotSaveMessagesToSegment)?;
    let message_count = batch.message_count();
    if message_count == 0 {
        return Ok(None);
    }
    let batch_end = batch
        .header
        .base_offset
        .checked_add(u64::from(message_count) - 1)
        .ok_or(IggyError::CannotWriteToFile)?;
    if persisted_end.is_some_and(|offset| batch_end <= offset) {
        return Ok(None);
    }
    Ok(Some(RetainedBatchMeta {
        base_offset: batch.header.base_offset,
        base_timestamp: batch.header.base_timestamp,
        total_size: batch.header.total_size() as u64,
        message_count,
    }))
}

fn consumer_offset_effect(
    pending: PendingConsumerOffsetCommit,
    tracked: Option<DurableOffsetState>,
    persisted_high_water: u64,
) -> ConsumerOffsetEffect {
    let state = match pending.mutation {
        PendingConsumerOffsetMutation::Upsert(offset) if pending.auto_commit => {
            Some(DurableOffsetState {
                committed_offset: tracked.map_or(persisted_high_water, |state| {
                    state.committed_offset.max(offset)
                }),
                persisted_high_water: tracked.map_or(persisted_high_water, |state| {
                    state.persisted_high_water.max(persisted_high_water)
                }),
            })
        }
        PendingConsumerOffsetMutation::Upsert(offset) => Some(DurableOffsetState {
            committed_offset: offset,
            persisted_high_water,
        }),
        PendingConsumerOffsetMutation::Delete => None,
    };
    ConsumerOffsetEffect { pending, state }
}

/// Match a retransmit against immutable, validated journal bytes. Only the view
/// may change, so exact body equality replaces another checksum pass.
fn journaled_prepare_matches_retransmit(
    journaled: &Frozen<4096>,
    incoming: &Message<PrepareHeader>,
) -> bool {
    const VIEW_OFFSET: usize = std::mem::offset_of!(PrepareHeader, view);

    let stored = journaled.as_slice();
    let received = incoming.as_slice();
    let header_size = std::mem::size_of::<PrepareHeader>();
    if stored.len() != received.len() || stored.len() < header_size {
        return false;
    }

    let view_end = VIEW_OFFSET + std::mem::size_of::<u32>();
    if stored[..VIEW_OFFSET] != received[..VIEW_OFFSET]
        || stored[view_end..header_size] != received[view_end..header_size]
    {
        return false;
    }
    stored[header_size..] == received[header_size..]
}

/// Success reply body for a committed partition op other than `SendMessages`
/// (which confirms its offsets through [`send_messages_reply_body`]).
///
/// Result-framed ops (`Operation::is_result_framed`; on this plane the
/// consumer-offset ops, whose rejections ship typed errors) must carry an
/// explicit empty result section (`[count = 0]`) so the SDK's framed decode
/// does not misread the payload; every other partition op replies with an
/// empty body.
const fn committed_reply_body(operation: Operation) -> bytes::Bytes {
    if operation.is_result_framed() {
        bytes::Bytes::from_static(&[0, 0, 0, 0])
    } else {
        bytes::Bytes::new()
    }
}

// The confirmation payload below ships raw, with no result section ahead of it.
// If `SendMessages` ever became result-framed, a batch with confirmations would
// misdecode into a spurious typed error, which is loud; a batch without them
// would decode as a clean success, which is silent.
const _: () = assert!(!Operation::SendMessages.is_result_framed());

/// One confirmation for the committed batch, or `count = 0` when its offsets
/// could not be resolved (missing or undecodable journal entry, or an empty
/// batch).
///
/// `count = 0` is a first-class answer meaning "committed, no offsets to
/// report", not a decode problem: the SDK reads it as an empty list, exactly as
/// it reads the legacy server's empty body. That is also why absence must stay
/// absent - a placeholder entry would carry a valid stream/topic/partition/
/// offset tuple and be indistinguishable from a real commit at offset 0.
fn send_messages_reply_body(
    namespace: u64,
    batch_stats: Option<CommittedBatchStats>,
) -> bytes::Bytes {
    batch_stats.map_or_else(
        || bytes::Bytes::from_static(&[0, 0, 0, 0]),
        |stats| send_confirmation_body(namespace, stats.base_offset),
    )
}

#[allow(clippy::cast_possible_truncation)]
fn send_confirmation_body(namespace: u64, base_offset: u64) -> bytes::Bytes {
    let namespace = IggyNamespace::from_raw(namespace);
    SendMessagesResponse {
        confirmations: vec![SendMessagesConfirmationResponse {
            // Every field is narrower than a `u32` (widths compile-asserted in
            // `iggy_binary_protocol`), so each component fits by construction.
            stream_id: namespace.stream_id() as u32,
            topic_id: namespace.topic_id() as u32,
            partition_id: namespace.partition_id() as u32,
            base_offset,
        }],
    }
    .to_bytes()
}

/// Committed-batch accounting surfaced at commit time so the aggregate stats
/// (`messages_count`, `size_bytes`) advance with the visible offset rather than
/// waiting on the threshold-gated disk persist, and so the `SendMessages` reply
/// can confirm where the batch landed.
#[derive(Clone, Copy)]
struct CommittedBatchStats {
    base_offset: u64,
    message_count: u32,
    size_bytes: u64,
    producer: Option<BatchProducer>,
    timestamp: u64,
}

struct JournaledMessages {
    result: AppendResult,
    prepare: Frozen<4096>,
}

impl CommittedBatchStats {
    /// Offset of the batch's last message. The batch carries a contiguous
    /// offset run, and the sole constructor rejects an empty one, so the
    /// subtraction cannot underflow.
    fn end_offset(self) -> u64 {
        self.base_offset + u64::from(self.message_count) - 1
    }
}

/// Whether a batch stamped `timestamp` starts a new segment rather than join
/// one whose first batch was stamped `first`. The WAL applies the same rule at
/// prepare time, so both sites roll at the same batch. A stamp behind `first`
/// counts as no age.
const fn rolls_by_age(max_age: Option<u64>, first: u64, timestamp: u64) -> bool {
    match max_age {
        Some(max_age) => timestamp.saturating_sub(first) >= max_age,
        None => false,
    }
}

/// Fold one `SendMessages` batch's accounting into a running `JournalInfo`,
/// matching the field updates `append_messages` applies per append.
/// `current_offset` is the batch's last message offset; the batch carries a
/// contiguous offset run. Takes raw header fields so the persist-build path
/// (decoding the committed prefix) and the eviction path (folding the meta
/// `evict_prefix` surfaced) share one accumulator with no duplicate decode.
fn accumulate_committed_info(
    info: &mut JournalInfo,
    base_offset: u64,
    base_timestamp: u64,
    total_size: u64,
    count: u32,
) {
    info.messages_count += count;
    info.size += IggyByteSize::from(total_size);
    info.current_offset = base_offset + u64::from(count) - 1;
    if info.first_timestamp == 0 {
        info.first_timestamp = base_timestamp;
    }
    info.end_timestamp = base_timestamp;
    info.max_timestamp = info.max_timestamp.max(base_timestamp);
}

/// Sealed segments one call to [`IggyPartition::remove_sealed_segments_up_to`]
/// may unlink before it stops and leaves the rest to the next pass.
///
/// The removal loop runs inside ONE frame body on the shard pump, and this
/// shard's consensus ticks are a sibling select arm that stays unpolled while
/// that body awaits, so the budget is really a bound on how long every OTHER
/// group on this core goes without a heartbeat. Uncapped it is a bound on the
/// backlog instead: the first pass after the cleaner is switched on walks
/// however many segments retention accumulated, which on a large log silences
/// those groups long enough to lose them to a view change.
///
/// A SEGMENT budget standing in for a time bound, so the margin is filesystem
/// specific: 16 segments is at most 32 unlinks (log plus index), a few hundred
/// milliseconds on commodity `NVMe` against the shipped 5 s
/// `cluster.heartbeat_timeout`, and still under 2 s if each unlink costs a
/// pathological 50 ms on a contended journal. Large enough that steady-state
/// retention, which reclaims a handful of segments per interval, never reaches
/// it -- only a backlog does, and that one drains over several passes.
const SEGMENT_REMOVAL_BUDGET_PER_PASS: usize = 16;

/// Consecutive denials that end one promotion drain. Denials free no slot, so
/// without this bound a single commit turn could answer every queued request,
/// each with an awaited reply send.
const PROMOTION_DENIALS_MAX: usize = 4;

/// Answer of [`IggyPartition::observe_retirement_debt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetirementDebtStatus {
    /// A committed payment covers the debt.
    Paid,
    /// The retry slice as `(client_id, session)` when this replica can pay now.
    Unpaid(Option<Vec<(u128, u64)>>),
}

/// What a replica deletes from the object store while it is the primary of
/// a tiered partition. Nothing here is persisted.
#[derive(Default)]
struct ObjectDeletes {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// Whether the objects of the incarnation were listed in this view.
    swept: bool,
    /// The segments whose objects wait for their delete.
    retired: RetiredSegments,
    /// The job in flight: its number, what it does, and the scope whose end
    /// stops it.
    running: Option<(u64, RunningObjectDelete, iggy_object_store::TierScope)>,
}

/// What the object-delete job in flight was started with.
enum RunningObjectDelete {
    /// A sweep, with the truncation watermark its pass came with.
    Sweep { committed_up_to: u64 },
    /// A delete of these segments, as they were queued when it started.
    Delete(Vec<(u64, RetiredSegment)>),
}

/// What one call to [`IggyPartition::remove_sealed_segments_up_to`] reclaimed.
///
/// `budget_spent` reports that the pass stopped on
/// `SEGMENT_REMOVAL_BUDGET_PER_PASS` rather than on the end of the removable
/// run, so a caller can re-stage immediately instead of leaving the rest until
/// its next interval tick. The budget itself stays private: the signal is what
/// callers need, and reading the number would invite them to rebuild the
/// comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentRemoval {
    pub segments: u64,
    pub messages: u64,
    pub budget_spent: bool,
}

/// What one [`IggyPartition::evict_segments`] call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentEviction {
    pub segments: u64,
    /// Segments the pass would evict are still waiting for the next call.
    pub budget_spent: bool,
}

/// How much of a tiered partition stays on local disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalEnvelope {
    /// Uploaded segments whose newest message is older than this leave the
    /// disk.
    pub age: IggyExpiry,
    /// Uploaded segments leave the disk, oldest first, until the sealed
    /// segments still local fit in this many bytes.
    pub size: Option<u64>,
}

/// Counter shortfalls from removing one segment, reported with its offset range.
#[derive(Debug, Default, Clone, Copy)]
struct CleanupShortfall {
    size_bytes: u64,
    segments: u32,
    messages: u64,
}

impl CleanupShortfall {
    /// Report one removed segment's shortfall and offset range. Silent when the
    /// counters covered its rollback.
    fn report(self, namespace: IggyNamespace, removed_offsets: Option<(u64, u64)>) {
        if self.size_bytes == 0 && self.segments == 0 && self.messages == 0 {
            return;
        }
        let (removed_from, removed_to) = removed_offsets.unwrap_or_default();
        warn!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = namespace.inner(),
            removed_from,
            removed_to,
            size_shortfall = self.size_bytes,
            segments_shortfall = self.segments,
            messages_shortfall = self.messages,
            "segment cleanup gave back more than the partition counters held; the parent \
             totals are now low by the shortfall until a rebuild or a restart"
        );
    }
}

/// Roll one cleaned-up segment out of the partition counters.
///
/// Returns the messages the segment held, which is what the caller reports as
/// removed, plus whatever the rollback could not cover. Retention is the
/// likeliest source of a clamped rollback: a partition on its way out keeps
/// serving cleanup passes after the delete already settled its counters into
/// the parents.
fn settle_cleaned_segment(stats: &PartitionStats, segment: &Segment) -> (u64, CleanupShortfall) {
    // The removal loop only reaches sealed segments, which always hold at least
    // one message, so the count is inclusive start..=end. A one-message sealed
    // segment has `start_offset == end_offset`, so the `+ 1` is required (a
    // `start == end -> 0` special case would undercount it).
    let messages = segment.end_offset - segment.start_offset + 1;
    let shortfall = CleanupShortfall {
        size_bytes: stats.decrement_size_bytes(segment.size.as_bytes_u64()),
        segments: stats.decrement_segments_count(1),
        messages: stats.decrement_messages_count(messages),
    };
    (messages, shortfall)
}

/// Highest `end_offset` among the leading run of expired sealed segments, or
/// `None` when none are expired. The last element is the active segment and is
/// never considered. `expiry` must be resolved; a `ServerDefault` expires
/// nothing (see [`Segment::is_expired`]).
fn leading_expired_end(
    segments: &[Segment],
    now: IggyTimestamp,
    expiry: IggyExpiry,
) -> Option<u64> {
    let last_idx = segments.len().saturating_sub(1);
    let mut up_to = None;
    for (idx, segment) in segments.iter().enumerate() {
        if idx == last_idx || !segment.is_expired(now, expiry) {
            break;
        }
        up_to = Some(segment.end_offset);
    }
    up_to
}

/// Index of the last of `segments` that starts at or below `offset`, or of the
/// first one when they all start above it.
///
/// Segments are ordered by start offset, and remote ones make the list as
/// long as total retention, so the lookup must not cost a step per segment.
fn segment_holding(segments: &[Segment], offset: u64) -> usize {
    segments
        .partition_point(|segment| segment.start_offset <= offset)
        .saturating_sub(1)
}

/// How many of `segments`, a poll's starting segment and the ones after it,
/// the poll's snapshot holds.
///
/// Every message after the starting segment matches the poll, so the walk
/// cannot use a segment past the one that completes `count` messages or
/// `byte_budget` bytes. The starting segment counts toward neither: how much
/// of it sits before the poll's first match is unknown until it is read.
fn walked_segments(segments: &[Segment], count: u32, byte_budget: usize) -> usize {
    let mut messages = 0_u64;
    let mut bytes = 0_u64;
    for (walked, segment) in segments.iter().enumerate().skip(1) {
        if messages >= u64::from(count) || bytes >= byte_budget as u64 {
            return walked;
        }
        let size = segment.size.as_bytes_u64();
        if size > 0 {
            messages = messages
                .saturating_add(segment.end_offset.saturating_sub(segment.start_offset) + 1);
            bytes = bytes.saturating_add(size);
        }
    }
    segments.len()
}

/// Highest `end_offset` to drop so the SEALED resident size falls to
/// `max_bytes`, or `None` when already under budget. The active segment (last
/// element) is never dropped. The budget is per-partition: the cluster has no
/// single owner of a topic-wide total, so each replica trims its own log.
///
/// The active segment's bytes are excluded from the running total, not merely
/// from the deletions. Counting bytes that can never be reclaimed lets them
/// evict the sealed history instead: at a budget near one segment the active
/// one alone exceeds it, and every sealed segment is dropped no matter how
/// small the log is.
fn leading_oversized_end(segments: &[Segment], max_bytes: u64) -> Option<u64> {
    let last_idx = segments.len().saturating_sub(1);
    let mut resident: u64 = segments
        .iter()
        .take(last_idx)
        .map(|segment| segment.size.as_bytes_u64())
        .sum();
    let mut up_to = None;
    for (idx, segment) in segments.iter().enumerate() {
        if idx == last_idx || !segment.sealed || resident <= max_bytes {
            break;
        }
        resident -= segment.size.as_bytes_u64();
        up_to = Some(segment.end_offset);
    }
    up_to
}

/// Highest `end_offset` retention removes from `segments`: the longer of the
/// expired and the over-budget leading runs, cut back to the last segment that
/// ends at or below `barrier`. `None` when retention removes nothing. The
/// active segment (last element) is never named.
fn retention_end(
    segments: &[Segment],
    now: IggyTimestamp,
    expiry: IggyExpiry,
    max_bytes: Option<u64>,
    barrier: Option<u64>,
) -> Option<u64> {
    let expired = leading_expired_end(segments, now, expiry);
    let oversized = max_bytes.and_then(|max_bytes| leading_oversized_end(segments, max_bytes));
    let up_to = expired.into_iter().chain(oversized).max()?;
    let through = barrier.map_or(up_to, |barrier| up_to.min(barrier));
    // `up_to` ends a sealed segment, so the walk stops before the active one.
    segments
        .iter()
        .take_while(|segment| segment.end_offset <= through)
        .map(|segment| segment.end_offset)
        .last()
}

/// `end_offset` of the `count`-th oldest sealed (non-active) segment of
/// `segments`, or `None` when there is no deletable sealed segment. Clamps to
/// the last sealed segment when fewer than `count` exist.
fn nth_oldest_sealed_end(segments: &[Segment], count: u32) -> Option<u64> {
    if count == 0 {
        return None;
    }
    // Exclude the active (last) segment, take the leading sealed run, then the
    // `count`-th of those (or the last available when fewer exist).
    let last_idx = segments.len().saturating_sub(1);
    segments
        .iter()
        .take(last_idx)
        .take_while(|segment| segment.sealed)
        .take(count as usize)
        .map(|segment| segment.end_offset)
        .last()
}

#[cfg(test)]
pub mod tests;
