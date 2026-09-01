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

use crate::iggy_index_writer::IggyIndexWriter;
use crate::journal::{MessageLookup, PartitionJournal, PartitionJournalMemStorage};
use crate::log::JournalInfo;
use crate::log::SegmentedLog;
use crate::messages_writer::MessagesWriter;
use crate::offset_storage::{
    PURGE_GENERATION_FILE, delete_persisted_offset, persist_offset, persist_offset_max,
    persist_purge_generation, read_purge_generation,
};
use crate::poll_plan::{
    AutoCommitCtx, AutoCommitTarget, DiskReadPlan, DiskSegment, LastPolledCtx,
    PartitionDirResolution, PollPlan, PollTier, ResidentTailSnapshot,
};
use crate::segment::Segment;
use crate::state_transfer::{PartitionTransferSession, PendingTransferRearm};
use crate::types::{FatalCommit, RepairConclusion, RepairSession};
use crate::{
    AppendResult, Partition, PartitionOffsets, PartitionsConfig, PollQueryResult, PollingArgs,
    PollingConsumer,
};
use consensus::{
    ClientTable, ClientTableMode, CommitLogEvent, Consensus, PartitionDiagEvent, PipelineEntry,
    PlaneKind, Project, ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus,
    ack_preflight, ack_quorum_reached, build_deny_reply_from_request, build_reply_from_request,
    build_reply_message, drain_committable_prefix, emit_namespace_progress_event,
    emit_partition_diag, emit_sim_event, fence_old_prepare_by_commit, repaired_frontier_update,
    replicate_frozen_to_next_in_chain, replicate_preflight, restamp_prepare_view,
    send_prepare_ok as send_prepare_ok_common, verify_prepare_integrity,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::responses::messages::{
    SendMessagesConfirmationResponse, SendMessagesResponse,
};
use iggy_binary_protocol::{
    AckLevel, GenericHeader, Operation, PrepareHeader, WireDecode, WireEncode, WireIdentifier,
};
use iggy_binary_protocol::{PrepareOkHeader, ReplyHeader, RoutedRequestHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyExpiry, IggyTimestamp, PartitionStats, PollingKind,
    TopicRuntimeOptions,
};
use journal::Journal as _;
use journal::local_gate::LocalGate;
use journal::superblock::{
    PingPongSuperblock, SUPERBLOCK_RETRY_BACKOFF_BASE_MICROS, SUPERBLOCK_RETRY_BACKOFF_MAX_MICROS,
    SUPERBLOCK_RETRY_BACKOFF_MAX_SHIFT, SuperblockStore,
};
use message_bus::{IggyMessageBus, MessageBus, is_auto_commit_client};
use server_common::{
    MESSAGE_ALIGN, Message, SegmentStorage,
    iobuf::{Frozen, Owned},
    send_messages::{
        BatchHeader, ChecksumMode, convert_request_message, decode_prepare_slice,
        decode_prepare_slice_trusted, stamp_prepare_for_persistence,
    },
    sharding::IggyNamespace,
};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, error, warn};

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
pub struct IggyPartition<B = IggyMessageBus, SB = PingPongSuperblock>
where
    B: MessageBus,
{
    consensus: VsrConsensus<B>,
    /// This group's slice of the VSR client table, run in
    /// [`ClientTableMode::PARTITION_SLICE`]: per-client request watermarks
    /// folded in at commit, so every replica derives the same slice from the
    /// same log. The mode turns off what this plane cannot use -- no reply ring
    /// (`SendMessages` has no result section, so a duplicate is answered by
    /// synthesizing the empty success its original earned), no epoch fence (a
    /// partition group never observes a `Register`), and no preallocated slot
    /// array (one table per group, where preallocating the cap would reserve
    /// hundreds of KiB per partition before a client connects).
    dedup: ClientTable,
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
    /// persisted): a fresh server treats a group as never-polled.
    pub last_polled_offsets: Arc<ConsumerGroupOffsets>,
    pub stats: Arc<PartitionStats>,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub should_increment_offset: bool,
    pub write_lock: Arc<TokioMutex<()>>,
    pub(crate) consumer_offsets_path: Option<String>,
    pub(crate) consumer_group_offsets_path: Option<String>,
    /// Canonical on-disk partition directory, set at construction by the
    /// server builder. Disk polls must not derive this from live writers:
    /// sealed segments drop their writer at rotation, so a writer-derived
    /// path transiently disappears and silently hides the disk tier.
    /// `None` only for in-memory (simulated) partitions.
    pub(crate) partition_dir: Option<String>,
    pub(crate) consumer_offset_enforce_fsync: bool,
    /// This topic's runtime knobs, resolved at topic admission and carried
    /// here by the builder. Every `None` field falls back to the shard-wide
    /// `PartitionsConfig` value (simulator and tests build partitions with
    /// no resolved options at all).
    pub(crate) runtime_options: TopicRuntimeOptions,
    /// In-flight journal repair:
    /// set when the recovery handshake finds this replica behind the group's
    /// commit frontier, cleared when `RepairDone` completes the walk.
    pub repair: Option<RepairSession>,
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
    fatal: Option<FatalCommit>,
    pub(crate) pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
    /// Committed-only mirror of each consumer's persisted offset file: the
    /// last value this replica durably wrote per (kind, consumer id). Fed
    /// exclusively by the file-writing paths (replicated commit-apply, the
    /// primary-local `NoAck` store, purge/delete/reclaim) and never by the
    /// eager poll-path in-memory apply, so both readers see committed state
    /// only: the auto-commit persist gate (skip or blind-write, no per-commit
    /// file read) and the submit-side coalesce gate
    /// ([`Self::is_auto_commit_offset_covered`]). A cold key (first touch
    /// after boot) folds against the file once via `persist_offset_max`, so
    /// the tracker rebuilds from disk lazily and deterministically.
    /// `RefCell`: mutated from `&self` paths on the single shard thread;
    /// borrows never cross an await.
    pub(crate) persisted_offsets: RefCell<HashMap<(ConsumerKind, u32), u64>>,
    pub(crate) observed_view: u32,
    /// Highest `PurgeTopic` generation this replica has locally applied (reset
    /// the partition to empty). The reconciler compares the committed metadata
    /// generation against this and resets only when it advances, so a redundant
    /// reconcile pass never re-wipes a partition already at this generation.
    pub(crate) applied_purge_generation: u64,
    /// `Partition::created_revision` of the metadata row this partition was
    /// built for (the reconciler's "epoch"). Keys the durable `purge.gen`
    /// record: a delete whose on-disk cleanup failed leaves the directory
    /// behind, and the recreated partition restarts its generations at 0, so
    /// the dead incarnation's record must not hydrate. `0` for partitions built
    /// without a metadata row (tests, in-memory storage).
    pub(crate) created_revision: u64,
    /// Highest consensus op assigned when the last purge ran. INVARIANT: every
    /// journal-apply path must no-op entries with `op <= purge_floor_op`. The
    /// purge keeps journal entries resident (consensus history for backups,
    /// repair and retransmission) while wiping the segments, so without the
    /// floor a pre-purge op committing after the purge would flush purged
    /// bytes back into a fresh segment or re-advance the reset offset. Not
    /// persisted: the in-memory journal dies with the process, so no resident
    /// pre-purge entry survives a restart.
    purge_floor_op: u64,
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
    superblock_lock: LocalGate,
    /// Consecutive failed superblock writes, and the clock reading after which
    /// the next attempt may run. A persistent `ENOSPC` / `EIO` would otherwise
    /// re-run a full `atomic_replace` on every 10 ms consensus tick. Reset on
    /// the first success. See [`Self::persist_superblock_if_needed`] for the
    /// terminal policy.
    superblock_write_failures: Cell<u64>,
    superblock_retry_after_micros: Cell<u64>,
    /// A committed purge this replica accepted but could not apply, because it
    /// could not record the frontier reset first. Withholds `PrepareOk` until
    /// the purge lands: the counter still names the PRE-purge offset space, so
    /// every op acked meanwhile would be stamped from a `base_offset` the peers
    /// that already purged do not share.
    ///
    /// The superblock persist gate cannot cover this on its own -- it fires on
    /// `(view, log_view)` changes, and a replica with a stable view and a full
    /// disk attempts no write, observes no failure, and fences nothing.
    pub(crate) purge_deferred: bool,
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
    /// cache alone never saves the pass. Swept against the live chain at build
    /// time; cleared wherever segment files are unlinked-and-recreated (purge,
    /// install, converge).
    pub(crate) segment_checksum_cache:
        RefCell<std::collections::HashMap<u64, crate::state_transfer::SegmentChecksumMemo>>,
    /// Receiving-side memo of the last staged-segment reuse scan, so a peer
    /// rotation against the same segment set does not re-read and re-walk every
    /// staged file. See [`crate::state_transfer::ReuseScanMemo`].
    pub(crate) reuse_scan_memo: RefCell<Option<crate::state_transfer::ReuseScanMemo>>,
    /// Serving-side offer cache, keyed by the `commit_op` it was built at, so
    /// simultaneous rejoiners share one manifest instead of re-reading every
    /// segment per requester. Invalidated by `purge` (same commit frontier,
    /// different bytes) and released by the shard's offer-expiry sweep.
    pub(crate) transfer_offer_cache:
        RefCell<Option<Rc<crate::state_transfer::PartitionStateTransferOffer>>>,
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
            .field("should_increment_offset", &self.should_increment_offset)
            .field("partition_dir", &self.partition_dir)
            .field("repair", &self.repair)
            .field("recovered_durable_offset", &self.recovered_durable_offset)
            .field("observed_view", &self.observed_view)
            .field("applied_purge_generation", &self.applied_purge_generation)
            .finish_non_exhaustive()
    }
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

/// Why a purge did not complete, split by whether it had already mutated.
///
/// The two need opposite handling, and conflating them is a data-loss bug:
/// fencing a partition whose purge failed before it touched anything
/// quarantines a complete healthy chain while the live counter still names the
/// pre-purge offset space, and the fence's own frontier write then stamps that
/// stale counter as durable truth.
#[derive(Debug)]
pub enum PurgeError {
    /// The frontier reset could not be recorded. NOTHING was mutated: the
    /// segments, the counters and `applied_purge_generation` are all untouched,
    /// so the reconciler's `committed > applied` gate re-issues this purge on
    /// its next pass. Retry, do not fence.
    ///
    /// Sets `purge_deferred`, which withholds `PrepareOk` for this
    /// group until the purge lands, so the replica goes quorum-invisible THERE
    /// while every other partition on the node keeps serving. Without that
    /// fence the counter would still name the pre-purge offset space and every
    /// op this replica acked would be stamped from a `base_offset` its purged
    /// peers do not share. The superblock persist gate does not cover it: that
    /// fires on `(view, log_view)` changes, and a stable view attempts no write
    /// and so observes no failure.
    ///
    /// Fencing the SEND rather than the whole partition is the point. The
    /// alternative was fencing a partition whose chain is still whole, which
    /// quarantines live data and rebuilds it at the pre-purge frontier.
    ///
    /// Carries no cause: the write path reports `bool`, and the underlying
    /// `ENOSPC` / `EIO` is logged by the superblock writer on the first failure
    /// and at every power-of-two thereafter.
    FrontierNotRecorded,
    /// The wipe ran and the fresh chain is planted, but the applied purge
    /// generation could not be recorded durably (`purge.gen`), so
    /// `applied_purge_generation` stays at its pre-purge value and the
    /// reconciler re-issues the purge. Retry, do not fence: the partition is
    /// serviceable and re-purging an already-empty chain is cheap.
    ///
    /// Sets `purge_deferred` for the same reason as
    /// [`Self::FrontierNotRecorded`]: an op acked between this failure and the
    /// retry would be wiped by that retry while every peer that recorded the
    /// generation keeps it.
    GenerationNotRecorded(IggyError),
    /// A step after the drain failed, so the partition holds no serviceable
    /// segment chain and its next append would panic on `active_segment()`.
    /// The caller must fence this group for rebuild.
    Unserviceable(IggyError),
}

impl fmt::Display for PurgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrontierNotRecorded => write!(
                f,
                "could not record the purge's offset-frontier reset; nothing was mutated"
            ),
            Self::GenerationNotRecorded(source) => write!(
                f,
                "purge reset the partition but could not record its applied generation; \
                 the purge will be re-issued: {source}"
            ),
            Self::Unserviceable(source) => write!(
                f,
                "purge left the partition without a serviceable chain: {source}"
            ),
        }
    }
}

impl std::error::Error for PurgeError {}

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
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PendingConsumerOffsetMutation {
    Upsert(u64),
    Delete,
}

impl PendingConsumerOffsetCommit {
    const fn upsert(kind: ConsumerKind, consumer_id: u32, offset: u64) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Upsert(offset),
            auto_commit: false,
        }
    }

    /// Monotone-apply variant for a server auto-commit op. See `auto_commit`.
    const fn upsert_auto_commit(kind: ConsumerKind, consumer_id: u32, offset: u64) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Upsert(offset),
            auto_commit: true,
        }
    }

    const fn delete(kind: ConsumerKind, consumer_id: u32) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Delete,
            auto_commit: false,
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

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    pub fn new(stats: Arc<PartitionStats>, consensus: VsrConsensus<B>) -> Self {
        let observed_view = consensus.view();
        let single_replica = consensus.replica_count() == 1;
        let partition = Self {
            consensus,
            dedup: ClientTable::with_mode(
                consensus::PARTITION_DEDUP_CLIENTS_MAX,
                ClientTableMode::PARTITION_SLICE,
            ),
            log: SegmentedLog::default(),
            offset: Arc::new(AtomicU64::new(0)),
            dirty_offset: AtomicU64::new(0),
            consumer_offsets: Arc::new(ConsumerOffsets::with_capacity(1)),
            consumer_group_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(1)),
            last_polled_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(1)),
            stats,
            created_at: IggyTimestamp::now(),
            revision_id: 0,
            should_increment_offset: false,
            write_lock: Arc::new(TokioMutex::new(())),
            consumer_offsets_path: None,
            consumer_group_offsets_path: None,
            partition_dir: None,
            consumer_offset_enforce_fsync: false,
            runtime_options: TopicRuntimeOptions::default(),
            repair: None,
            recovered_durable_offset: None,
            installed_frontier: None,
            fatal: None,
            pending_consumer_offset_commits: HashMap::new(),
            persisted_offsets: RefCell::new(HashMap::new()),
            observed_view,
            applied_purge_generation: 0,
            created_revision: 0,
            purge_floor_op: 0,
            superblock: None,
            superblock_lock: LocalGate::new(),
            superblock_write_failures: Cell::new(0),
            superblock_retry_after_micros: Cell::new(0),
            purge_deferred: false,
            durable_offset_frontier: Cell::new(0),
            transfer: None,
            transfer_attempts: 0,
            transfer_failures: 0,
            transfer_refusals: 0,
            transfer_rearm: None,
            segment_checksum_cache: RefCell::new(std::collections::HashMap::new()),
            reuse_scan_memo: RefCell::new(None),
            transfer_offer_cache: RefCell::new(None),
        };
        if single_replica {
            partition.log.journal().inner.set_repair_retention(false);
        }
        partition
    }

    #[must_use]
    pub const fn applied_purge_generation(&self) -> u64 {
        self.applied_purge_generation
    }

    /// See [`Self::purge_floor_op` field docs](#structfield.purge_floor_op).
    /// Exposed for the repair-serving path: a peer must not serve entries at
    /// or below this replica's floor.
    #[must_use]
    pub const fn purge_floor_op(&self) -> u64 {
        self.purge_floor_op
    }

    /// Record the metadata incarnation this partition was built for. Must run
    /// BEFORE [`Self::hydrate_applied_purge_generation`], which keys the
    /// durable record on it.
    pub const fn set_created_revision(&mut self, created_revision: u64) {
        self.created_revision = created_revision;
    }

    /// Seed [`Self::applied_purge_generation`] from the partition dir's
    /// `purge.gen` file at build time (both fresh create and recovery walk
    /// this). Absent file reads 0, so a partition that never purged and a
    /// repair-rebuilt dir both start below any committed generation and the
    /// reconciler re-applies the purge; a crash AFTER a purge's durable
    /// generation write correctly skips the re-wipe, keeping messages
    /// appended since. A record left by a PREVIOUS incarnation of this
    /// namespace reads 0 as well (see [`read_purge_generation`]). No-op
    /// without a partition dir (in-memory storage).
    ///
    /// # Errors
    /// Propagates a real I/O failure reading `purge.gen`: booting with the
    /// sentinel 0 instead would make the reconciler silently re-purge and
    /// destroy post-purge messages, so the boot fails loud.
    pub async fn hydrate_applied_purge_generation(&mut self) -> Result<(), IggyError> {
        if let Some(dir) = self.partition_dir() {
            let path = format!("{dir}/{PURGE_GENERATION_FILE}");
            self.applied_purge_generation =
                read_purge_generation(&path, self.created_revision).await?;
        }
        Ok(())
    }

    #[must_use]
    pub const fn consensus(&self) -> &VsrConsensus<B> {
        &self.consensus
    }

    /// This group's dedup slice. Read at admission to classify a request,
    /// written only from the commit path.
    #[must_use]
    pub const fn dedup(&self) -> &ClientTable {
        &self.dedup
    }

    /// Mutable slice, for the commit path and state-transfer install.
    pub(crate) const fn dedup_mut(&mut self) -> &mut ClientTable {
        &mut self.dedup
    }

    /// Size the dedup slice to `[partition] dedup_clients_max`. Boot-time:
    /// shrinking a live slice evicts its oldest-committed entries, which costs
    /// dedup coverage for those clients rather than correctness.
    pub fn set_dedup_clients_max(&mut self, clients_max: usize) {
        self.dedup.set_capacity(clients_max.max(1));
    }

    #[must_use]
    pub fn with_in_memory_storage(
        stats: Arc<PartitionStats>,
        consensus: VsrConsensus<B>,
        segment_size: IggyByteSize,
        consumer_offset_enforce_fsync: bool,
    ) -> Self {
        let mut partition = Self::new(stats, consensus);
        partition.consumer_offset_enforce_fsync = consumer_offset_enforce_fsync;
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
        partition.should_increment_offset = false;
        partition.stats.increment_segments_count(1);
        partition
    }

    pub fn set_partition_dir(&mut self, partition_dir: String) {
        self.partition_dir = Some(partition_dir);
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
        self.superblock = Some(superblock);
        self.durable_offset_frontier
            .set(recovered.map_or(0, |state| state.offset_frontier));
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
    /// only when a write was attempted and failed, and the caller must
    /// withhold the send. The in-memory view stays ahead of the durable one,
    /// which a crash safely rolls back, and the next tick retries.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn persist_superblock_if_needed(&self) -> bool {
        let Some(superblock) = self.superblock.as_ref() else {
            // No store (in-memory / simulated partitions): nothing can be
            // recorded, so keep the durable cells current instead. The
            // dispatch tripwire asserts `needs_superblock_persist()` is clear
            // on every view-scoped send, and for a storeless group "current"
            // is trivially true -- leaving the cells behind would trip it on
            // the first view change.
            self.consensus
                .mark_superblock_durable(self.consensus.view(), self.consensus.log_view());
            return true;
        };
        // Lock-free fast path: the steady state is an unchanged view with
        // nothing to write, and skipping the lock keeps every gated send off
        // it, notably `send_prepare_ok`, which runs this per prepare. Safe
        // because `view`/`log_view` advance only on this single-threaded
        // executor and no `.await` sits between the `Cell` read and the
        // return; a concurrent advance is caught by the re-check below.
        if !self.consensus.needs_superblock_persist() {
            return true;
        }
        // A write that keeps failing must not re-run a full `atomic_replace`
        // on every 10 ms tick. Back off first, while still reporting `false`
        // so the send stays withheld: fail-closed is the point of this gate,
        // and the backoff only bounds what the retry costs.
        if self.superblock_write_is_backed_off() {
            return false;
        }
        // Re-check needs-persist AFTER acquiring the lock so check and write
        // are atomic and a redundant caller coalesces, finding the state
        // already made durable by the writer it queued behind.
        let _superblock_guard = self.superblock_lock.acquire().await;
        if !self.consensus.needs_superblock_persist() {
            return true;
        }
        self.write_superblock(superblock.as_ref(), self.offset_frontier())
            .await
    }

    /// Write the current VSR state under [`Self::superblock_lock`].
    ///
    /// The caller must hold that lock. The state is captured HERE rather than
    /// passed in: with writes serialized and no await between the capture and
    /// the write, the last writer carries the freshest view, so the durable
    /// view cannot regress. `mark_superblock_durable` takes the WRITTEN
    /// values, never a re-read, because the in-memory view can advance across
    /// the write's `.await`.
    ///
    /// # Terminal policy
    /// There is none beyond staying fenced: a replica that cannot record the
    /// view it is in must not act in it, so it withholds every view-scoped
    /// send for this group, goes quiet, and its peers elect around it. Only
    /// THIS partition's group is fenced; the rest of the node keeps serving.
    #[allow(clippy::future_not_send)]
    async fn write_superblock(&self, superblock: &SB, offset_frontier: u64) -> bool {
        // ADVANCE direction: never below what this replica has already minted,
        // and never below what the record ALREADY holds. Both bounds are
        // needed and neither implies the other -- a failed install leaves the
        // counter behind the record it wrote before the swap, so maxing against
        // the counter alone lets the fence that follows lower the durable
        // frontier. The reset direction goes through `write_superblock_inner`.
        let advanced = offset_frontier
            .max(self.offset_frontier())
            .max(self.durable_offset_frontier.get());
        self.write_superblock_inner(superblock, advanced).await
    }

    /// The write itself; the advance and reset directions differ only in the
    /// frontier they hand in.
    #[allow(clippy::future_not_send)]
    async fn write_superblock_inner(&self, superblock: &SB, offset_frontier: u64) -> bool {
        // The pairing fields stay `(0, 0)` and `commit_max` is a dead write
        // on this plane: nothing reads either back (`restore_partition_view`
        // restores view/log_view only), because recovery re-derives the
        // install floor from the installed segments at boot -- a crash after
        // an install does not re-run the transfer. Written anyway so the
        // record shape matches the metadata plane's.
        //
        // `offset_frontier` is NOT dead: it is the only durable carrier of the
        // group's offset space once the segments that named it are gone. Every
        // write stamps the current counter, so whichever write lands last (a
        // view change, or the explicit persist an install issues) leaves a
        // lower bound boot can re-seed from.
        let mut state = self.consensus.vsr_state(0, 0);
        state.offset_frontier = offset_frontier;
        match superblock.write(&state.to_bytes()).await {
            Ok(()) => {
                self.consensus
                    .mark_superblock_durable(state.view, state.log_view);
                self.durable_offset_frontier.set(state.offset_frontier);
                self.superblock_write_failures.set(0);
                self.superblock_retry_after_micros.set(0);
                true
            }
            Err(error) => {
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
    /// because three paths leave a replica whose counter would otherwise
    /// restart at 0 while the group is at N (a transfer install of an all-GC'd
    /// origin, a crash inside the install's swap window, and the
    /// fence-and-rebuild path, which needs no crash at all). Restarting the
    /// counter is not a lag -- replicas re-stamp `base_offset` from it and
    /// recompute `batch_checksum` over the result, so the next replicated
    /// prepare would persist different bytes here than on every peer, silently.
    ///
    /// Lives HERE rather than in the server crate so the boot paths and the
    /// simulator share one implementation. A copy in the harness was a copy of
    /// the max rule that had lost the max, in the one place built to catch
    /// violations of it.
    pub fn restore_offset_frontier(&mut self, recovered: Option<&consensus::VsrState>) {
        let Some(frontier) = recovered
            .map(|state| state.offset_frontier)
            .filter(|&f| f > 0)
        else {
            return;
        };
        let recovered_end = frontier - 1;
        if self.should_increment_offset && self.offset.load(Ordering::Acquire) >= recovered_end {
            return;
        }
        tracing::info!(
            namespace_raw = self.consensus().group(),
            offset_frontier = frontier,
            "restored partition offset frontier from its superblock"
        );
        self.offset.store(recovered_end, Ordering::Release);
        self.dirty_offset.store(recovered_end, Ordering::Relaxed);
        self.should_increment_offset = true;
    }

    /// Whether this partition ever stamped an offset, i.e. whether its offset
    /// counters describe a real offset space rather than an untouched zero. The
    /// one bit separating a partition holding one message at offset 0 from one
    /// that never took a write: both report `(0, 0)`.
    #[cfg(any(test, feature = "simulator"))]
    pub const fn offset_space_used(&self) -> bool {
        self.should_increment_offset
    }

    /// Adopt a log carried over from a previous incarnation of this partition,
    /// standing in for what segment recovery reads off disk at boot.
    ///
    /// A real server loses nothing across the rebuild: its messages are in segment
    /// files and boot recovers the offset counter from them. The simulator's
    /// partitions are in-memory, so without this the rebuilt partition comes back
    /// empty and its `commit_offset` regresses to zero, which reads as a consensus
    /// regression rather than the harness having thrown the data away.
    ///
    /// `durable_offset` and `write_offset` are what the caller recovered, as
    /// `segment_recovery` derives them from segments. Applied as a MAX against
    /// whatever the superblock frontier already proved, for the same reason
    /// [`Self::restore_offset_frontier`] maxes: a recovered value behind the
    /// frontier must not lower it.
    #[cfg(any(test, feature = "simulator"))]
    pub fn adopt_retained_log(&mut self, state: crate::RetainedPartitionState) {
        let crate::RetainedPartitionState {
            log,
            durable_offset,
            write_offset,
            offset_space_used,
        } = state;
        self.log = log;
        // Empty carry-over: the previous incarnation never took a write, so there
        // is no offset space to restore and claiming one would make the next
        // prepare mint from a base no peer agrees on.
        //
        // Keyed on the RETIRED incarnation's flag, never on `(0, 0)` or on this
        // partition's own `should_increment_offset`. One message at offset 0 reports
        // the same two zeroes as an empty partition, and this instance is freshly
        // built so its own flag is always false. The arithmetic test would therefore
        // adopt the log, skip the counters, and let the next write stamp
        // `base_offset = 0` where peers stamp 1, with `batch_checksum` over it: two
        // logs, different bytes at the same op, silently.
        if !offset_space_used {
            return;
        }
        let durable = durable_offset.max(self.offset.load(Ordering::Acquire));
        let dirty = write_offset
            .max(durable)
            .max(self.dirty_offset.load(Ordering::Relaxed));
        self.offset.store(durable, Ordering::Release);
        self.dirty_offset.store(dirty, Ordering::Relaxed);
        self.should_increment_offset = true;
        // Everything carried over is already persisted as far as this replica is
        // concerned, so the flush and commit paths must not re-persist or re-count
        // it, the same contract boot gives a partition recovered from segments.
        self.recovered_durable_offset = Some(durable);
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

    /// The next message offset this replica will mint, `0` while the offset
    /// space is still empty. The value stamped into the durable record.
    #[must_use]
    pub fn offset_frontier(&self) -> u64 {
        if self.should_increment_offset {
            self.offset.load(Ordering::Acquire).saturating_add(1)
        } else {
            0
        }
    }

    /// Force the durable record to catch up with the current offset frontier,
    /// outside the view-change gate.
    ///
    /// [`Self::persist_superblock_if_needed`] fires on `(view, log_view)`
    /// changes only, which is the right trigger for the split-brain fence and
    /// the wrong one for the frontier: an install can move the counter by
    /// millions without touching the view. Called where the frontier changes
    /// with nothing else durable naming it -- after a state-transfer install
    /// and after the convergence that follows a failed one. Returns whether the
    /// record now holds it; a failure is logged by the writer and left to the
    /// ordinary retry, since the install itself already succeeded.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn persist_offset_frontier(&self) -> bool {
        self.persist_offset_frontier_at(self.offset_frontier())
            .await
    }

    /// Record a frontier that may be LOWER than the one already on disk.
    ///
    /// The frontier is conditionally monotone: it advances everywhere except a
    /// purge, which legitimately resets the offset space to 0. The advancing
    /// form cannot express that -- it maxes against the live counter -- and the
    /// distinction has to be explicit: a purge that leaves the old frontier
    /// recorded makes the next boot re-seed the counter to the state the purge
    /// just erased, and the following append stamps `base_offset` N where every
    /// peer stamps 0.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn reset_offset_frontier(&self) -> bool {
        self.reset_offset_frontier_at(self.offset_frontier()).await
    }

    /// [`Self::reset_offset_frontier`] for a frontier the live counter does not
    /// hold yet.
    ///
    /// Two callers need the value spelled out rather than read off the counter.
    /// A purge records its reset BEFORE it unlinks anything, while the counter
    /// still names the pre-purge space, so a crash mid-unlink cannot boot into
    /// a re-seed of the space the purge was erasing. An install under an
    /// advancing purge generation records the offer's frontier, which is
    /// legitimately below the local counter: the advancing form would max it
    /// straight back up and leave the pre-purge value on disk across the swap
    /// window.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn reset_offset_frontier_at(&self, frontier: u64) -> bool {
        let Some(superblock) = self.superblock.as_ref().map(Rc::clone) else {
            return true;
        };
        if self.superblock_write_is_backed_off() {
            return false;
        }
        let _superblock_guard = self.superblock_lock.acquire().await;
        self.write_superblock_inner(superblock.as_ref(), frontier)
            .await
    }

    /// Record the frontier immediately ahead of an irreversible quarantine,
    /// BYPASSING the retry backoff.
    ///
    /// The gate exists because the other writers' callers became retry loops,
    /// and skipping a doomed write costs them nothing. This caller is the
    /// opposite: it writes once and then moves the segments that are the
    /// record's only corroborating witness into `.fenced.N`, so a skip here is
    /// not deferred work, it is the last chance gone. A disk that recovered
    /// inside the backoff window would otherwise leave the rebuild re-seeding
    /// from a stale record with nothing left to take the max against.
    ///
    /// `intended` is the frontier the caller knows the group is at, written
    /// verbatim; `None` means the live counter is authoritative and the
    /// advancing form applies.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn record_frontier_before_quarantine(&self, intended: Option<u64>) -> bool {
        let Some(superblock) = self.superblock.as_ref().map(Rc::clone) else {
            return true;
        };
        let _superblock_guard = self.superblock_lock.acquire().await;
        match intended {
            Some(frontier) => {
                self.write_superblock_inner(superblock.as_ref(), frontier)
                    .await
            }
            None => {
                self.write_superblock(superblock.as_ref(), self.offset_frontier())
                    .await
            }
        }
    }

    /// Whether a recent write failure's backoff window is still open.
    ///
    /// The same gate [`Self::persist_superblock_if_needed`] applies before its
    /// own write, extended to the spelled-value writers because their callers
    /// became retry loops: a deferred purge is re-issued by the reconciler, and
    /// without this each pass re-runs a full `atomic_replace` against a disk
    /// that just refused one, as fast as `ENOSPC` returns.
    fn superblock_write_is_backed_off(&self) -> bool {
        self.consensus.clock_realtime_micros() < self.superblock_retry_after_micros.get()
    }

    /// [`Self::persist_offset_frontier`] for a frontier this replica has not
    /// reached yet.
    ///
    /// Used to record an INCOMING frontier before a destructive swap: the
    /// install unlinks the old chain and fsyncs that before the first staged
    /// rename lands, and boot sweeps `.log.staging` unconditionally, so a crash
    /// in that window otherwise leaves no copy of the frontier anywhere. Writing
    /// the claim first makes it a durable lower bound the whole way through, and
    /// over-claiming is harmless: the convergence that follows a failed install
    /// seeds the counter from the same artifact frontier.
    #[allow(clippy::future_not_send)]
    #[must_use = "the bool is the durability verdict; dropping it silently ignores a failed write"]
    pub async fn persist_offset_frontier_at(&self, frontier: u64) -> bool {
        let Some(superblock) = self.superblock.as_ref().map(Rc::clone) else {
            return true;
        };
        if self.superblock_write_is_backed_off() {
            return false;
        }
        let _superblock_guard = self.superblock_lock.acquire().await;
        self.write_superblock(superblock.as_ref(), frontier).await
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
    pub const fn set_runtime_options(&mut self, runtime_options: TopicRuntimeOptions) {
        self.runtime_options = runtime_options;
    }

    #[must_use]
    pub const fn runtime_options(&self) -> TopicRuntimeOptions {
        self.runtime_options
    }

    /// Segment size this partition rolls at: the per-topic value when the
    /// topic was created with one, else the shard-wide configured size.
    #[must_use]
    pub fn effective_segment_size(&self, config: &PartitionsConfig) -> IggyByteSize {
        self.runtime_options
            .segment_size
            .unwrap_or(config.segment_size)
    }

    /// Whether this partition's writes fsync.
    #[must_use]
    pub fn effective_enforce_fsync(&self, config: &PartitionsConfig) -> bool {
        self.runtime_options
            .enforce_fsync
            .unwrap_or(config.enforce_fsync)
    }

    /// Message-count threshold that flushes this partition's journal.
    #[must_use]
    pub fn effective_messages_required_to_save(&self, config: &PartitionsConfig) -> u32 {
        self.runtime_options
            .messages_required_to_save
            .unwrap_or(config.messages_required_to_save)
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

    pub fn configure_consumer_offset_storage(
        &mut self,
        consumer_offsets_path: String,
        consumer_group_offsets_path: String,
        consumer_offsets: ConsumerOffsets,
        consumer_group_offsets: ConsumerGroupOffsets,
        consumer_offset_enforce_fsync: bool,
    ) {
        self.consumer_offsets = Arc::new(consumer_offsets);
        self.consumer_group_offsets = Arc::new(consumer_group_offsets);
        self.consumer_offsets_path = Some(consumer_offsets_path);
        self.consumer_group_offsets_path = Some(consumer_group_offsets_path);
        self.consumer_offset_enforce_fsync = consumer_offset_enforce_fsync;
    }

    /// Stage a consumer offset upsert for the replicated op. The prepare
    /// must already have been appended to `self.log.journal` by the caller
    /// so `VsrAction::RetransmitPrepares` can recover it during a view
    /// change. The on-disk offset table is NOT touched here: persist runs
    /// from [`apply_staged_consumer_offset_commit`] at commit-time so a
    /// view-change rollback of the in-memory pending entry also rolls
    /// back the disk write (by never having performed it).
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
        self.pending_consumer_offset_commits.insert(op, pending);
    }

    /// Stage a consumer offset delete for the replicated op. See
    /// [`stage_consumer_offset_upsert`] for the ordering contract.
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
        self.pending_consumer_offset_commits.insert(op, pending);
    }

    pub(crate) async fn apply_staged_consumer_offset_commit(
        &mut self,
        op: u64,
    ) -> Result<(), IggyError> {
        // Peek (copy) instead of remove: if `persist_consumer_offset_commit`
        // fails (e.g. disk full, fd exhausted) the pending entry must remain
        // stageable for retry on the next apply. Removing first would strand
        // the op - not on disk AND not in memory.
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
        self.apply_consumer_offset_commit(pending)?;
        self.pending_consumer_offset_commits.remove(&op);
        Ok(())
    }

    async fn persist_consumer_offset_commit(
        &self,
        pending: PendingConsumerOffsetCommit,
    ) -> Result<(), IggyError> {
        let Some(path) = self.persisted_offset_path(pending.kind, pending.consumer_id) else {
            return Ok(());
        };
        let key = (pending.kind, pending.consumer_id);
        match pending.mutation {
            // A server auto-commit persists monotonically: its op offset can
            // trail the durably-recorded value (disk-tier polls replicate in
            // IO-completion order), so a plain overwrite would rewind the file
            // and re-deliver on restart. The `persisted_offsets` tracker keeps
            // the fold off the file: a covered offset skips the write, an
            // advancing one blind-writes, and only a cold key (first commit
            // after boot) reads the file once. Explicit client stores
            // overwrite, so a deliberate offset reset still holds. Mirrors the
            // in-memory `upsert_offset_max` vs `upsert_offset` split in the
            // commit-apply.
            PendingConsumerOffsetMutation::Upsert(offset) if pending.auto_commit => {
                let tracked = self.persisted_offsets.borrow().get(&key).copied();
                let persisted = match tracked {
                    Some(high_water) if offset <= high_water => return Ok(()),
                    Some(_) => {
                        persist_offset(&path, offset, self.consumer_offset_enforce_fsync).await?;
                        offset
                    }
                    None => {
                        persist_offset_max(&path, offset, self.consumer_offset_enforce_fsync)
                            .await?
                    }
                };
                self.persisted_offsets.borrow_mut().insert(key, persisted);
                Ok(())
            }
            PendingConsumerOffsetMutation::Upsert(offset) => {
                persist_offset(&path, offset, self.consumer_offset_enforce_fsync).await?;
                self.persisted_offsets.borrow_mut().insert(key, offset);
                Ok(())
            }
            PendingConsumerOffsetMutation::Delete => {
                delete_persisted_offset(&path).await?;
                self.persisted_offsets.borrow_mut().remove(&key);
                Ok(())
            }
        }
    }

    /// Whether the committed high-water for this consumer already covers
    /// `offset`, so a poll's auto-commit submit cannot advance it and may be
    /// skipped instead of burning a consensus op. Reads committed state only
    /// (the tracker is fed at commit-apply, never by the eager poll-path
    /// apply): an offset covered in memory but not yet committed keeps
    /// resubmitting until the covering op actually lands, so a dropped
    /// in-flight op self-heals on the next poll.
    #[must_use]
    pub fn is_auto_commit_offset_covered(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
    ) -> bool {
        self.persisted_offsets
            .borrow()
            .get(&(kind, consumer_id))
            .is_some_and(|&high_water| offset <= high_water)
    }

    fn apply_consumer_offset_commit(
        &self,
        pending: PendingConsumerOffsetCommit,
    ) -> Result<(), IggyError> {
        match pending.mutation {
            PendingConsumerOffsetMutation::Upsert(offset)
                if pending.kind == ConsumerKind::Consumer =>
            {
                let id = pending.consumer_id;
                let key = usize::try_from(id).expect("u32 consumer id must fit usize");
                let create = || {
                    self.consumer_offsets_path.as_deref().map_or_else(
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
                Ok(())
            }
            PendingConsumerOffsetMutation::Upsert(offset)
                if pending.kind == ConsumerKind::ConsumerGroup =>
            {
                let group_id = pending.consumer_id;
                let key = ConsumerGroupId(
                    usize::try_from(group_id).expect("u32 group id must fit usize"),
                );
                let create = || {
                    self.consumer_group_offsets_path.as_deref().map_or_else(
                        || {
                            ConsumerOffset::new(
                                ConsumerKind::ConsumerGroup,
                                group_id,
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
                Ok(())
            }
            // Commit-time apply keeps its invariant check on the PRIMARY:
            // admission verified the offset exists there, so a miss on the
            // primary is real divergence (log corruption / out-of-order apply)
            // and must surface rather than silently mask a split state. A
            // FOLLOWER may legitimately miss the offset: `AckLevel::NoAck`
            // stores apply on the primary only and are never replicated,
            // so a later quorum delete finds nothing on the backups -- erroring
            // there would fail the committed apply, panic the replica as
            // divergent, and crash-loop on every journal replay. The
            // prepare-time race is handled by not re-checking existence at
            // staging (see `stage_consumer_offset_delete`).
            PendingConsumerOffsetMutation::Delete if pending.kind == ConsumerKind::Consumer => {
                let id = pending.consumer_id;
                let guard = self.consumer_offsets.pin();
                let key = usize::try_from(id).expect("u32 consumer id must fit usize");
                let removed = guard.remove(&key).is_some();
                if !removed && !self.consensus.is_follower() {
                    return Err(IggyError::ConsumerOffsetNotFound(key));
                }
                Ok(())
            }
            PendingConsumerOffsetMutation::Delete
                if pending.kind == ConsumerKind::ConsumerGroup =>
            {
                let group_id = pending.consumer_id;
                let guard = self.consumer_group_offsets.pin();
                let key = ConsumerGroupId(
                    usize::try_from(group_id).expect("u32 group id must fit usize"),
                );
                let removed = guard.remove(&key).is_some();
                if !removed && !self.consensus.is_follower() {
                    return Err(IggyError::ConsumerOffsetNotFound(key.0));
                }
                Ok(())
            }
            _ => Ok(()),
        }
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

    /// Reclaim every stored consumer-group offset whose group id is no longer
    /// `is_live`, returning the owned persisted-file paths the caller must unlink.
    ///
    /// Fully synchronous (no `.await`): the in-memory papaya remove happens here,
    /// the disk unlink is deferred to the caller on owned `String` data so no
    /// borrow of `self` survives across the await. This is the only safe shape
    /// for the reconciler, which runs on a sibling task to the pump that may
    /// realloc the partitions vec during that await. The remove-then-unlink
    /// ordering matches the crash-safe GC invariant (monotonic, never-reused
    /// group ids mean a recreated group never reads a dead group's offset).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn reclaim_dead_group_offsets(&self, is_live: impl Fn(u64) -> bool) -> Vec<String> {
        let pinned = self.consumer_group_offsets.pin();
        let dead: Vec<u64> = pinned
            .keys()
            .map(|key| key.0 as u64)
            .filter(|group_id| !is_live(*group_id))
            .collect();
        let mut paths = Vec::with_capacity(dead.len());
        for group_id in dead {
            pinned.remove(&ConsumerGroupId(group_id as usize));
            self.persisted_offsets
                .borrow_mut()
                .remove(&(ConsumerKind::ConsumerGroup, group_id as u32));
            if let Some(path) =
                self.persisted_offset_path(ConsumerKind::ConsumerGroup, group_id as u32)
            {
                paths.push(path);
            }
        }
        paths
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

    /// Drop a group's ephemeral `last_polled` mark on this partition (residue of
    /// a since-removed member that a later join would misread as a live hold).
    #[allow(clippy::cast_possible_truncation)]
    pub fn clear_group_last_polled(&self, group_id: u64) {
        self.last_polled_offsets
            .pin()
            .remove(&ConsumerGroupId(group_id as usize));
    }

    /// `AckLevel::NoAck` fast path: persist, apply, send reply, no
    /// replication. Single-replica durability. Never recorded in the dedup
    /// slice: it does not replicate, so folding it in would fork the slice
    /// across replicas. Session lifecycle lives on metadata.
    #[allow(clippy::future_not_send)]
    async fn apply_consumer_offset_no_ack(
        &self,
        request_header: Box<RoutedRequestHeader>,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let pending = offset.map_or_else(
            || PendingConsumerOffsetCommit::delete(kind, consumer_id),
            |value| PendingConsumerOffsetCommit::upsert(kind, consumer_id, value),
        );

        if let Err(error) = self.persist_consumer_offset_commit(pending).await {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(self.diag_ctx(), "no_ack offset persist failed")
                    .with_operation(request_header.operation)
                    .with_error(error.to_string()),
            );
            return;
        }
        if let Err(error) = self.apply_consumer_offset_commit(pending) {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(self.diag_ctx(), "no_ack offset apply failed")
                    .with_operation(request_header.operation)
                    .with_error(error.to_string()),
            );
            return;
        }

        let reply = build_reply_from_request(
            &self.consensus,
            &request_header,
            committed_reply_body(request_header.operation),
        );
        // Same rule as the committed path: a submit's waiter takes the reply,
        // because `header.client` is then the VSR consensus id.
        if let Some(waiter) = waiter {
            let _ = waiter.send(reply);
            return;
        }
        let reply_buffers = reply.into_generic().into_frozen();
        if let Err(error) = self
            .consensus
            .message_bus()
            .send_to_client(request_header.client, reply_buffers)
            .await
        {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(self.diag_ctx(), "no_ack reply send failed")
                    .with_operation(request_header.operation)
                    .with_error(error.to_string()),
            );
        }
    }

    pub(crate) fn persisted_offset_path(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Option<String> {
        match kind {
            ConsumerKind::Consumer => self
                .consumer_offsets_path
                .as_ref()
                .map(|path| format!("{path}/{consumer_id}")),
            ConsumerKind::ConsumerGroup => self
                .consumer_group_offsets_path
                .as_ref()
                .map(|path| format!("{path}/{consumer_id}")),
        }
    }

    fn ensure_consumer_offset_exists(
        &self,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), IggyError> {
        let found = match kind {
            ConsumerKind::Consumer => {
                let key = usize::try_from(consumer_id).expect("u32 consumer id must fit usize");
                self.consumer_offsets.pin().contains_key(&key)
            }
            ConsumerKind::ConsumerGroup => {
                let key = ConsumerGroupId(
                    usize::try_from(consumer_id).expect("u32 group id must fit usize"),
                );
                self.consumer_group_offsets.pin().contains_key(&key)
            }
        };

        if found {
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

    fn clear_pending_consumer_offset_commits_if_view_changed(&mut self) {
        let current_view = self.consensus.view();
        if current_view == self.observed_view {
            return;
        }

        self.pending_consumer_offset_commits.clear();
        self.observed_view = current_view;
    }

    /// Build an owned [`PollPlan`] synchronously (no `.await`), so the caller
    /// can run the disk read + offset persist off the partition borrow. The
    /// in-memory journal tier is read here directly (mem reads never yield);
    /// the disk tier is captured as owned descriptors in [`DiskReadPlan`].
    #[allow(clippy::too_many_lines)]
    pub(crate) fn build_poll_plan(
        &mut self,
        consumer: PollingConsumer,
        args: &PollingArgs,
        validate_checksum: bool,
    ) -> PollPlan {
        // Reads the durable commit frontier (`self.offset`, stored only on
        // commit). Also used below as the poll's high-water bound: this function
        // is fully synchronous, so the single load cannot drift mid-plan.
        let commit_offset = self.offsets().commit_offset;
        if !self.should_increment_offset || args.count == 0 {
            return PollPlan {
                commit_offset,
                auto_commit: None,
                last_polled: None,
                tier: PollTier::Empty,
            };
        }

        let query = match args.strategy.kind {
            PollingKind::Timestamp => MessageLookup::Timestamp {
                timestamp: args.strategy.value,
                count: args.count,
                ceiling: commit_offset,
            },
            kind => {
                let start_offset = match kind {
                    PollingKind::Offset => args.strategy.value,
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
                        auto_commit: None,
                        last_polled: None,
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

        // Past the empty-return guards: only now build the auto-commit context,
        // whose offset-path `format!()` is wasted on the early returns above.
        let auto_commit = self.auto_commit_ctx(consumer, args.auto_commit);
        // Cooperative-rebalance: record the highest offset served to a group so
        // the drain reconciler can tell committed >= last-polled. Captured here
        // as an owned `Arc` and applied off the borrow in `PollPlan::execute`,
        // since the served offset is unknown until the poll completes.
        let last_polled = match consumer {
            PollingConsumer::ConsumerGroup(group_id, _) => Some(LastPolledCtx {
                offsets: self.last_polled_offsets.clone(),
                group_id,
            }),
            PollingConsumer::Consumer(..) => None,
        };

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
                Some((fragments, last_matching_offset)) => PollTier::Resident {
                    fragments,
                    last_matching_offset,
                },
                None => PollTier::Empty,
            };
            return PollPlan {
                commit_offset,
                auto_commit,
                last_polled,
                tier,
            };
        }

        let (start_segment, start_position) = self.disk_poll_start(&query);
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
        let segments = self.log.segments()[start_segment..]
            .iter()
            .zip(self.log.sealed_read_state()[start_segment..].iter())
            .map(|(segment, read_state)| DiskSegment {
                start_offset: segment.start_offset,
                persisted: segment.size.as_bytes_u64(),
                read_state: Rc::clone(read_state),
                sealed: segment.sealed,
            })
            .collect();
        let disk = DiskReadPlan {
            partition_dir: self.partition_dir_resolution(),
            segments,
            start_position,
            namespace_raw: self.namespace().inner(),
            validate_checksum,
        };
        // Snapshot the resident journal tail now (on the pump, under the
        // borrow) so the straddle splice runs off-task on owned data with no
        // partition reference. Point-in-time, so immune to a concurrent commit
        // evicting the run just past the disk match.
        let resident_tail = self.resident_tail_snapshot();
        PollPlan {
            commit_offset,
            auto_commit,
            last_polled,
            tier: PollTier::Disk {
                disk,
                query,
                resident_tail,
            },
        }
    }

    /// Capture the owned inputs for an auto-commit, if requested: the lock-free
    /// offset-map `Arc` and the target consumer/group id, so the in-memory apply
    /// runs off the partition borrow once the poll's served offset is known.
    /// Durability is not captured here: the poll no longer writes the offset
    /// file, the serving shard replicates the offset through consensus instead.
    fn auto_commit_ctx(
        &self,
        consumer: PollingConsumer,
        auto_commit: bool,
    ) -> Option<AutoCommitCtx> {
        if !auto_commit {
            return None;
        }
        let pending = PendingConsumerOffsetCommit::try_from_polling_consumer(consumer, 0).ok()?;
        let target = match pending.kind {
            ConsumerKind::Consumer => AutoCommitTarget::Consumer {
                offsets: self.consumer_offsets.clone(),
                consumer_id: pending.consumer_id,
                create_path: self.consumer_offsets_path.clone(),
            },
            ConsumerKind::ConsumerGroup => AutoCommitTarget::ConsumerGroup {
                offsets: self.consumer_group_offsets.clone(),
                group_id: pending.consumer_id,
                create_path: self.consumer_group_offsets_path.clone(),
            },
        };
        Some(AutoCommitCtx { target })
    }

    /// Synchronous in-memory journal poll, for the resident tier. Never awaits
    /// (see [`PartitionJournal::get_sync`]), so it is safe under a partition
    /// borrow.
    pub(crate) fn journal_get_sync(&self, query: &MessageLookup) -> Option<PollQueryResult<4096>> {
        self.log.journal().inner.get_sync(query)
    }

    /// Snapshot the resident journal tail (oldest resident offset + op-ascending
    /// entry clones) for the disk-tier straddle continuation. Taken
    /// synchronously under the partition borrow so the splice runs off-task on
    /// owned data; see [`ResidentTailSnapshot`].
    fn resident_tail_snapshot(&self) -> ResidentTailSnapshot {
        let journal = &self.log.journal().inner;
        let oldest_resident = journal.oldest_resident_offset();
        // Only clone the entries (a Vec + per-entry `Frozen` refcount bumps)
        // when a resident tail actually exists. A fully drained journal yields
        // `None`, and an empty `entries` makes `select_resident` return `None`
        // (empty poll) on both the straddle and retention-recovery paths.
        let entries = if oldest_resident.is_some() {
            journal.resident_entries()
        } else {
            Vec::new()
        };
        ResidentTailSnapshot {
            oldest_resident,
            entries,
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

    #[allow(clippy::cast_possible_truncation)]
    fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let pending = PendingConsumerOffsetCommit::try_from_polling_consumer(consumer, offset)?;
        self.apply_consumer_offset_commit(pending)?;
        Ok(())
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

        let dirty_offset = if self.should_increment_offset {
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
    fn namespace(&self) -> IggyNamespace {
        IggyNamespace::from_raw(self.consensus.group())
    }

    /// The commit fault that fenced this partition, if one has.
    #[must_use]
    pub const fn fatal(&self) -> Option<&FatalCommit> {
        self.fatal.as_ref()
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
                operation: Operation::SendMessages,
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
                storage.messages_reader.is_some() || storage.messages_writer.is_some()
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

    /// Starting `(segment index, byte position)` for a disk poll, resolved
    /// via each segment's sparse index cache. An index miss starts at the
    /// segment's first byte (the walk filters precisely).
    fn disk_poll_start(&self, query: &MessageLookup) -> (usize, u64) {
        let segments = self.log.segments();
        match query {
            MessageLookup::Offset { offset, .. } => {
                let segment_index = segments
                    .iter()
                    .rposition(|segment| segment.start_offset <= *offset)
                    .unwrap_or(0);
                let position = self
                    .log
                    .segment_indexes(segment_index)
                    .and_then(|cache| cache.offset_lower_bound(*offset))
                    .map_or(0, |index| index.position);
                (segment_index, position)
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
                (segment_index, position)
            }
        }
    }

    /// Project a client request into a prepare.
    ///
    /// A replay of a committed `(client, request)` is absorbed by this group's
    /// dedup slice; anything above the watermark projects into a prepare.
    /// Session lifecycle + eviction live on the metadata plane.
    ///
    /// # Panics
    /// Panics if called when this partition's consensus instance is not the
    /// primary, is not in normal status, or is currently syncing.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    /// `reply` is the in-process channel a `PartitionSubmit` carried in. When
    /// present the committed reply fires on it instead of going to the bus:
    /// the connection-owning shard writes it to the socket it holds, because
    /// `header.client` is the VSR consensus id and carries no home-shard
    /// routing. `None` keeps the bus path (auto-commit ops, tests).
    pub async fn on_request(
        &mut self,
        message: Message<RoutedRequestHeader>,
        reply: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        // Taken by whichever arm answers: the deny paths, the NoAck fast path,
        // or the pipeline entry that fires it at commit. Exactly one runs.
        let mut reply = reply;
        self.clear_pending_consumer_offset_commits_if_view_changed();
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

            // State-dependent admission belongs to the primary. A backup can
            // lag the committed offset table or message frontier and would
            // otherwise turn a routing artifact into a terminal 404 or 400.
            // Reject it first with the only response that proves the request
            // was never admitted, so the caller may safely retry elsewhere.
            if consensus.is_follower() || !consensus.is_normal() || consensus.is_transferring() {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "rejecting client request on non-primary partition replica",
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
            // A replay racing its own in-flight original is absorbed here: the
            // slice only knows committed ops, so it cannot yet see the copy
            // still in the pipeline. Keyed on the exact `(client, request)` --
            // matching any request from the client would serialize its
            // pipeline depth to one in-flight write per group.
            if !is_auto_commit_client(client_id) {
                if consensus.pipeline_has_message_from_client_request(client_id, request) {
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::TransientNotCommitted.as_code(),
                        "in-flight dedup transient reply send failed",
                        reply.take(),
                    )
                    .await;
                    return;
                }
                if self.dedup.is_duplicate(client_id, request) {
                    let committed = build_reply_from_request(
                        &self.consensus,
                        message.header(),
                        committed_reply_body(message.header().operation),
                    );
                    Self::answer_duplicate_or_log(
                        &self.consensus,
                        message.header(),
                        committed,
                        reply.take(),
                    )
                    .await;
                    return;
                }
            }

            if matches!(message.header().operation, Operation::DeleteConsumerOffset)
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
                && let Some((_, _, Some(requested_offset), _)) = consumer_offset
            {
                let current_offset = self.stats.current_offset();
                let partition_empty =
                    self.stats.messages_count_inconsistent() == 0 && current_offset == 0;
                if partition_empty || requested_offset > current_offset {
                    emit_partition_diag(
                        tracing::Level::WARN,
                        &PartitionDiagEvent::new(
                            ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                            "rejecting store_consumer_offset for out-of-range offset",
                        )
                        .with_operation(message.header().operation)
                        .with_error(IggyError::InvalidOffset(requested_offset).to_string()),
                    );
                    Self::send_partition_deny_or_log(
                        consensus,
                        message.header(),
                        IggyError::InvalidOffset(requested_offset).as_code(),
                        "store_consumer_offset deny reply send failed",
                        reply.take(),
                    )
                    .await;
                    return;
                }
            }

            // NoAck -> fast path. Quorum -> VSR pipeline.
            if let Some((kind, consumer_id, offset, AckLevel::NoAck)) = consumer_offset
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
                // Two-queue: prepare slot -> project+replicate; prepare full +
                // request room -> buffer; both full -> drop+warn (client retries
                // via read-timeout).
                if consensus.pipeline_is_full() {
                    let push_result = consensus.push_queued_request(
                        consensus::RequestEntry::with_sender(message, reply.take()),
                    );
                    if push_result.is_err() {
                        emit_partition_diag(
                            tracing::Level::WARN,
                            &PartitionDiagEvent::new(
                                ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                                "on_request: prepare and request queues both full, dropping",
                            ),
                        );
                    }
                    return;
                }

                let prepare = message.project(consensus);
                consensus.verify_pipeline();
                match reply.take() {
                    Some(sender) => consensus.pipeline_message_with_sender(
                        PlaneKind::Partitions,
                        &prepare,
                        sender,
                    ),
                    None => consensus.pipeline_message(PlaneKind::Partitions, &prepare),
                }
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

    /// Promote up to `slots_freed` buffered requests into prepares post-commit.
    ///
    /// Promotion runs no preflight: the request was classified at admission
    /// and the slice cannot have gained a higher watermark for it since (only
    /// a commit moves it, and this entry has not committed).
    ///
    /// Per-iteration `is_primary && is_normal && !is_transferring` asserts inlined
    /// (closure form's `&consensus` borrow conflicts with `&mut self`). Guards
    /// against view-change-reset flipping status across `on_replicate` await.
    ///
    /// View-change safety: `reset_view_change_state` calls
    /// [`consensus::Pipeline::clear_request_queue`]; resumed loop breaks via
    /// `else { break }`.
    ///
    /// # Panics
    /// On mid-iteration status flip. Reachable only if `clear_request_queue`
    /// is bypassed at view-change reset.
    #[allow(clippy::future_not_send)]
    pub async fn drain_request_queue_into_prepares(&mut self, slots_freed: usize) {
        for _ in 0..slots_freed {
            let req = self.consensus().pop_queued_request();
            let Some(req) = req else { break };

            let prepare = {
                let consensus = self.consensus();
                assert!(
                    !consensus.is_follower(),
                    "drain_request_queue_into_prepares: primary only"
                );
                assert!(
                    consensus.is_normal(),
                    "drain_request_queue_into_prepares: status must be normal"
                );
                assert!(
                    !consensus.is_transferring(),
                    "drain_request_queue_into_prepares: must not be transferring state"
                );
                let prepare = req.message.project(consensus);
                consensus.verify_pipeline();
                consensus.pipeline_message(PlaneKind::Partitions, &prepare);
                prepare
            };
            self.on_replicate(prepare).await;
        }
    }

    /// # Panics
    /// Panics on a primary when a prepare's op is ahead of the local
    /// sequencer: journaling it would make the next op assignment collide,
    /// which is unrecoverable in place.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn on_replicate(&mut self, message: Message<PrepareHeader>) {
        self.clear_pending_consumer_offset_commits_if_view_changed();
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
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "received old prepare (<= commit_min), skipping replication",
                )
                .with_operation(header.operation)
                .with_op(header.op),
            );
            // Fenced by commit_min: we've already executed this op, the
            // whole chain has it committed. Safe to drop entirely.
            return;
        }

        let journal_holds_op = self.log.journal().inner.header_by_op(header.op).is_some();
        if journal_holds_op {
            // Retransmit after downstream flap: durable here but commit
            // hasn't caught up. Re-forward + re-ACK so primary's view of
            // us is consistent. Both downstream and primary are idempotent
            // on duplicate (replica, op).
            emit_partition_diag(
                tracing::Level::DEBUG,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "journal already holds prepare, re-forwarding + re-acking",
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
            let consensus = self.consensus();
            if let Err(error) =
                replicate_frozen_to_next_in_chain(consensus, frozen_for_forward).await
            {
                let is_transport_error = error.is_transport();
                emit_partition_diag(
                    if is_transport_error {
                        tracing::Level::WARN
                    } else {
                        tracing::Level::ERROR
                    },
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to re-forward retransmitted prepare to next in chain",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
                if !is_transport_error {
                    return;
                }
            }
            self.send_prepare_ok(&header).await;
            return;
        }

        // Backup gap check; primary sequencer pre-advanced by
        // push_prepare_entry.
        //
        // The sequencer, deliberately, where `metadata::on_replicate` gates on its
        // journal. The two frontiers cannot drift apart on this plane:
        // `install_state_transfer` rewinds the sequencer to the offer's `commit_op`
        // (where the metadata install moves a durable snapshot floor and leaves the
        // WAL head behind it), and the repair ingest advances it by walking the
        // journal. Reading the journal here would answer 0 after every restart,
        // since this plane's journal is memory-only and starts empty however much
        // data sits on disk.
        let is_backup = self.consensus().is_follower();
        if is_backup {
            if header.op != current_op + 1 {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "dropping out-of-order prepare (gap)",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op),
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
            //   primary per view, the chain stops before the primary), so
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
        // Forward only after apply_replicated_operation journals the prepare.
        // The journal and network share the frozen allocation, so the bytes
        // retained for repair are exactly the bytes sent downstream.
        let replicated_result = if is_backup && header.operation == Operation::SendMessages {
            self.append_received_send_messages_to_journal(message).await
        } else {
            self.apply_replicated_operation(message).await
        };
        let frozen_for_forward = match replicated_result {
            Ok(frozen) => frozen,
            Err(error) => {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to apply replicated partition operation",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
                return;
            }
        };

        let consensus = self.consensus();
        // Backup only: advance sequencer + checksum after journal append.
        // Pre-advance on failing apply would leave consensus claiming op N
        // while the journal has nothing. Retransmit of N would silently drop
        // as is_old_prepare (header.op <= current_sequence). The primary does
        // not re-set here because push_prepare_entry already advanced it. A
        // sibling request pipelined during the apply await would otherwise be
        // rewound to a stale op + parent, projecting a duplicate next.
        if is_backup {
            consensus.sequencer().set_sequence(header.op);
            consensus.set_last_prepare_checksum(header.checksum);
            consensus.observe_prepare_timestamp(header.timestamp);
        }
        if let Err(error) = replicate_frozen_to_next_in_chain(consensus, frozen_for_forward).await {
            let is_transport_error = error.is_transport();
            emit_partition_diag(
                if is_transport_error {
                    tracing::Level::WARN
                } else {
                    tracing::Level::ERROR
                },
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "failed to replicate prepare to next in chain",
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

        self.send_prepare_ok(&header).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&mut self, message: Message<PrepareOkHeader>, config: &PartitionsConfig) {
        if self.fatal.is_some() {
            return;
        }
        self.clear_pending_consumer_offset_commits_if_view_changed();
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

        let drained = drain_committable_prefix(self.consensus());
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

    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&mut self, config: &PartitionsConfig) {
        if self.fatal.is_some() {
            return;
        }
        self.clear_pending_consumer_offset_commits_if_view_changed();

        // The primary commits inline via `on_ack` (it drains its own pipeline).
        // Backups never populate the pipeline - they journal replicated prepares
        // in `apply_replicated_operation` - so the pipeline drain is empty for
        // them. Fall back to the journal so backups durably persist committed
        // data. `commit_messages` then flushes only the committed prefix and
        // keeps the uncommitted tail journal-resident, so a later commit of that
        // tail still finds its headers here (no wedge). Pipeline-first keeps a
        // freshly promoted primary (rebuilt pipeline) draining there, avoiding a
        // double-count against `advance_commit_min`.
        let mut drained = drain_committable_prefix(self.consensus());
        if drained.is_empty() {
            drained = self.collect_committable_from_journal();
        }
        if drained.is_empty() {
            return;
        }

        self.handle_committed_entries(drained, config, false).await;
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

    /// Committable entries (ops `commit_min+1 ..= commit_max`) read from the
    /// journal, for a backup whose pipeline is empty. Stops at the first missing
    /// op: a replication gap must not be skipped, or `advance_commit_min`'s
    /// sequential contract breaks. Like the metadata plane's `commit_journal`,
    /// the journal keeps its committed entries until they are flushed
    /// (`commit_messages` drains only the committed prefix), so this read finds
    /// every committed op while the uncommitted tail stays resident.
    fn collect_committable_from_journal(&self) -> Vec<PipelineEntry> {
        let from_op = self.consensus.commit_min() + 1;
        let commit_max = self.consensus.commit_max();
        self.log
            .journal()
            .inner
            .committed_headers_from(from_op, commit_max)
            .into_iter()
            .map(PipelineEntry::new)
            .collect()
    }

    async fn apply_replicated_operation(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<Frozen<4096>, IggyError> {
        let header = *message.header();
        let replica_id = self.consensus.replica();
        let namespace_raw = self.consensus.group();

        match header.operation {
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
                // Replicated path is Quorum-only by construction; ack ignored.
                let (kind, consumer_id, offset, _ack) =
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
                self.log
                    .journal()
                    .inner
                    .append(frozen.clone())
                    .await
                    .map_err(|_| IggyError::CannotAppendMessage)?;

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
        let expected_offset = if self.should_increment_offset {
            self.dirty_offset
                .load(Ordering::Relaxed)
                .checked_add(1)
                .ok_or(IggyError::CannotAppendMessage)?
        } else {
            0
        };
        if (validated.base_offset, validated.base_timestamp) != (expected_offset, header.timestamp)
        {
            return Err(IggyError::CannotAppendMessage);
        }
        self.append_stamped_messages(message, validated)
            .await
            .map(|journaled| journaled.prepare)
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
        self.log
            .journal()
            .inner
            .append(frozen.clone())
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;

        self.should_increment_offset = true;
        self.dirty_offset
            .store(last_dirty_offset, Ordering::Relaxed);
        self.log.segments_mut()[segment_index].current_position = next_position;
        self.log.journal_mut().info = journal_info;

        Ok(JournaledMessages {
            result: AppendResult::new(batch.base_offset, last_dirty_offset, batch_messages_count),
            prepare: frozen,
        })
    }

    /// Drop an uncommitted view-divergent suffix and restore every append cursor
    /// from the retained prefix as one write-locked operation.
    ///
    /// # Errors
    ///
    /// Returns an error if a retained batch is invalid, the restored segment
    /// position overflows, or the journal cannot truncate the suffix.
    pub async fn truncate_uncommitted_from(&mut self, from_op: u64) -> Result<usize, IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let mut entries = self.log.journal().inner.resident_entries();
        entries.sort_unstable_by_key(peek_op);
        let mut retained_info = JournalInfo::default();
        let mut retained_next_offset = 0;
        let mut rewind_next_offset = None;
        for entry in &entries {
            if peek_operation(entry) != Operation::SendMessages {
                continue;
            }
            let batch = decode_prepare_slice_trusted(entry.as_slice())
                .map_err(|_| IggyError::InvalidCommand)?;
            if batch.message_count() == 0 {
                continue;
            }
            if peek_op(entry) >= from_op {
                rewind_next_offset = Some(
                    rewind_next_offset.map_or(batch.header.base_offset, |offset: u64| {
                        offset.min(batch.header.base_offset)
                    }),
                );
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
            .truncate_from(from_op)
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;

        self.log.journal_mut().info = retained_info;
        self.log.active_segment_mut().current_position = restored_position;
        if let Some(next_offset) = rewind_next_offset {
            let next_offset = next_offset.max(minimum_next_offset);
            self.dirty_offset
                .store(next_offset.saturating_sub(1), Ordering::Relaxed);
            self.should_increment_offset = next_offset > 0;
        }
        self.consensus.invalidate_local_dvc_suffix();
        Ok(removed)
    }

    async fn commit_messages(&mut self, config: &PartitionsConfig) -> Result<(), IggyError> {
        self.commit_messages_inner(config, false).await
    }

    /// Flush the committed journal prefix to segment storage regardless of
    /// the `messages_required_to_save` thresholds.
    ///
    /// Shutdown-path counterpart of the commit-time persist gate: a graceful
    /// stop must not lose committed messages that were still resident in the
    /// in-memory journal (consumer offsets are persisted eagerly, so losing
    /// the messages would fail recovery with an offset ahead of the data).
    ///
    /// # Errors
    ///
    /// Returns [`IggyError`] when writing the committed batches or their
    /// index entries to segment storage fails.
    pub async fn flush_committed_messages(
        &mut self,
        config: &PartitionsConfig,
    ) -> Result<(), IggyError> {
        self.commit_messages_inner(config, true).await
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_messages_inner(
        &mut self,
        config: &PartitionsConfig,
        force: bool,
    ) -> Result<(), IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let journal_info = self.log.journal().info;
        if journal_info.messages_count == 0 {
            if force {
                tracing::info!(
                    target: "iggy.partitions.diag",
                    namespace_raw = self.namespace().inner(),
                    "forced flush: journal counts zero messages, nothing to persist"
                );
            }
            return Ok(());
        }

        // `journal_info` counts the committed prefix PLUS the uncommitted tail
        // still resident in the journal, yet only the committed prefix is
        // flushed below. With `messages_required_to_save > 1` the tail bytes
        // count toward the trigger, so this threshold is not "committed bytes
        // only" - safe, since the flush still writes only committed bytes.
        let is_full = self.log.active_segment().is_full();
        let unsaved_messages_count_exceeded =
            journal_info.messages_count >= self.effective_messages_required_to_save(config);
        let unsaved_messages_size_exceeded = journal_info.size.as_bytes_u64()
            >= self.effective_size_of_messages_required_to_save(config);
        let should_persist =
            is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded;
        if !force && !should_persist {
            return Ok(());
        }

        // Read (do NOT yet evict) ONLY the committed prefix (op <= commit_max,
        // gap-stopped). A backup journals replicated prepares ahead of the
        // commit frontier; flushing the uncommitted tail would write
        // per-replica-timing bytes to its segment (cross-replica divergence) and
        // drop the headers those ops need when their own commit later lands
        // (commit_min wedge). Eviction is deferred until the bytes are durable,
        // so a persist failure retains the prefix rather than losing a committed
        // batch (a live-process I/O fault only; the in-memory journal does not
        // survive a crash). Only the transfer-offer flush survives to re-read it;
        // the commit path panics the shard pump instead. All segment range /
        // stats / durable-offset accounting below is computed from the committed
        // entries, not the resident-journal snapshot above.
        let commit_max = self.consensus.commit_max();
        let committed_entries = self.log.journal().inner.committed_prefix(commit_max);
        if committed_entries.is_empty() {
            if force {
                tracing::info!(
                    target: "iggy.partitions.diag",
                    namespace_raw = self.namespace().inner(),
                    commit_max,
                    journal_messages = journal_info.messages_count,
                    "forced flush: no committed entries resident"
                );
            }
            return Ok(());
        }
        // Persist the prefix in segment-sized chunks: a segment seals on the
        // first flush whose committed bytes reach OR EXCEED `max_size`, no
        // matter how many entries this flush happens to cover. The batch that
        // crosses the cap is appended whole, so a sealed segment lands
        // anywhere in `[max_size, max_size + one maximum batch)` and never
        // exactly at `max_size`. A backup commits in bursts behind the
        // primary, so any grouping- or timing-sensitive roll rule
        // (like keying rotation on the journal-position `is_full` above)
        // seals segments at per-replica offsets, and the offset-keyed segment
        // GC staged by the reconciler never converges across the cluster.
        let max_segment_size = self.log.active_segment().max_size.as_bytes_u64();
        let mut entries = committed_entries.into_iter().peekable();
        let mut durable_offset = None;
        // Entries whose bytes are durable but which are still resident in the
        // journal. Evicted ONCE after the loop: `evict_prefix` drains and
        // re-appends the whole retained tail, so a per-chunk call would
        // re-walk that tail once per segment crossed, quadratic in the flush
        // span -- all under the partition write lock. On an error mid-flush
        // the accumulated prefix is evicted before propagating, so any later
        // flush attempt re-reads only what did not land.
        let mut evictable = 0usize;
        while entries.peek().is_some() {
            // A recovered active segment can already sit at or past the cap
            // (crash between persist and rotation); seal it before appending.
            if self.log.active_segment().size.as_bytes_u64() >= max_segment_size
                && let Err(error) = self.rotate_segment(config).await
            {
                self.evict_committed_prefix(evictable).await;
                return Err(error);
            }

            let (frozen_batches, index_bytes, flush_index, batch_count, committed_info, chunk_len) = {
                let segment = self.log.active_segment();
                let mut file_position = segment.size.as_bytes_u64();
                let mut flush_index = None;
                let mut frozen = Vec::with_capacity(entries.len());
                let mut batch_count = 0u32;
                let mut committed_info = JournalInfo::default();
                let mut chunk_len = 0usize;

                for entry in entries.by_ref() {
                    chunk_len += 1;
                    // Consumer-offset ops are journaled in the same prefix but carry
                    // no segment bytes; they were applied when staged, so skip them.
                    if peek_operation(&entry) != Operation::SendMessages {
                        if force {
                            tracing::info!(
                                target: "iggy.partitions.diag",
                                operation = ?peek_operation(&entry),
                                "forced flush: skipping non-send entry"
                            );
                        }
                        continue;
                    }
                    // Purge floor: a pre-purge batch committing after the
                    // purge must not flush its (purged) bytes into the fresh
                    // segment. It still counts into `chunk_len`, so it joins
                    // the evictable prefix and commit_min advances normally.
                    if peek_op(&entry) <= self.purge_floor_op {
                        continue;
                    }
                    // Resident committed SendMessages entry: this node stamped it
                    // in `append_messages` (recomputing the batch checksum over these
                    // exact bytes), so a validating re-decode would only re-hash ~1
                    // MiB to confirm our own write. Trust the structural decode; the
                    // batch-checksum recompute belongs at network ingress (repair
                    // validation + the follower receive gate), not on locally-stamped
                    // bytes. Guard the invariant for a future disk read-back path that
                    // could make decode fallible.
                    let Ok(batch) = decode_prepare_slice_trusted(entry.as_slice()) else {
                        tracing::error!(
                            target: "iggy.partitions.diag",
                            namespace_raw = self.namespace().inner(),
                            entry_len = entry.as_slice().len(),
                            "resident committed SendMessages entry failed to decode"
                        );
                        continue;
                    };
                    let message_count = batch.message_count();
                    if message_count == 0 {
                        continue;
                    }
                    // A repaired batch at or below the boot-time recovered
                    // durable offset is already IN the segments this replica
                    // recovered; persisting it again would append duplicate
                    // bytes past the segment end. Evict it without writing.
                    // Live traffic always sits above the (immutable) line.
                    let batch_end = batch.header.base_offset + u64::from(message_count) - 1;
                    if let Some(durable) = self.recovered_durable_offset
                        && batch_end <= durable
                    {
                        continue;
                    }

                    if flush_index.is_none() {
                        // Record only; the in-mem cache insert is deferred until the
                        // batch + index are durable (see post-persist below).
                        flush_index = Some(crate::iggy_index::IggyIndex::new(
                            batch.header.base_offset,
                            batch.header.base_timestamp,
                            file_position,
                        ));
                    }
                    file_position += batch.header.total_size() as u64;
                    batch_count += message_count;
                    accumulate_committed_info(
                        &mut committed_info,
                        batch.header.base_offset,
                        batch.header.base_timestamp,
                        batch.header.total_size() as u64,
                        message_count,
                    );
                    frozen.push(entry);
                    if file_position >= max_segment_size {
                        break;
                    }
                }

                let index_bytes = flush_index
                    .as_ref()
                    .map(crate::iggy_index::IggyIndexCache::serialize);

                (
                    frozen,
                    index_bytes,
                    flush_index,
                    batch_count,
                    committed_info,
                    chunk_len,
                )
            };

            // No committed SendMessages batch was resident in this chunk (e.g.
            // a committed consumer-offset run that is not persisted to a
            // segment). Nothing to flush; no segment bytes are at risk, so the
            // entries just join the evictable prefix.
            let Some(index_bytes) = index_bytes else {
                evictable += chunk_len;
                continue;
            };

            // Persist BEFORE eviction so a write failure leaves the rest of the
            // committed prefix resident instead of dropping it. On the commit
            // path a failure fences the partition and stops the shard pump; the
            // server then shuts down non-zero after one final best-effort flush
            // of every partition. A shutdown-flush failure warns and moves to
            // the next namespace, while the transfer offer turns it into
            // `FlushFailed`. Recovery reopens each writer at the on-disk length
            // boot recovery validated.
            //
            // The ordering therefore buys a non-corrupting failure, not an
            // in-process one. Write cursors advance only once both the batch and the
            // index are durable, so a later attempt, including the final
            // shutdown flush, rewrites the same positions instead of appending
            // a duplicate. Chunks already durable are evicted before the error
            // propagates, so it cannot re-read them and write them past a
            // rotation.
            if let Err(error) = self
                .persist_frozen_batches_to_disk(frozen_batches, index_bytes, batch_count)
                .await
            {
                self.evict_committed_prefix(evictable).await;
                return Err(error);
            }
            // Insert the flushed sparse-index entry into the in-mem cache only now
            // that the batch + index are durable. Inserting in the build loop (before
            // persist) re-inserts a duplicate on the next flush after a persist
            // failure, which re-reads the same prefix. The active segment has not
            // rotated yet, so this targets the segment that received the batches.
            if let Some(index) = flush_index {
                self.log.ensure_indexes();
                let indexes = self.log.active_indexes_mut().expect("indexes must exist");
                indexes.insert(index.offset, index.timestamp, index.position);
            }
            evictable += chunk_len;

            // Stamp range metadata on the segment that received the batches
            // BEFORE rotating: rotation seals it and derives the next segment's
            // start offset from `end_offset`, so updating after rotation would
            // tag the fresh segment with the old range and shift every
            // subsequent segment boundary off the file contents.
            let segment_index = self.log.segments().len() - 1;
            let segment = &mut self.log.segments_mut()[segment_index];
            if segment.start_timestamp == 0 && committed_info.first_timestamp != 0 {
                segment.start_timestamp = committed_info.first_timestamp;
            }
            segment.end_timestamp = committed_info.end_timestamp;
            segment.max_timestamp = segment.max_timestamp.max(committed_info.max_timestamp);
            segment.end_offset = committed_info.current_offset;
            durable_offset = Some(committed_info.current_offset);

            // Seal eagerly once the committed bytes cross the cap so the
            // segment becomes removable (GC skips the active segment) without
            // waiting for the next flush.
            if self.log.active_segment().size.as_bytes_u64() >= max_segment_size
                && let Err(error) = self.rotate_segment(config).await
            {
                self.evict_committed_prefix(evictable).await;
                return Err(error);
            }
        }
        self.evict_committed_prefix(evictable).await;

        // Aggregate stats (`messages_count`/`size_bytes`) advance at commit in
        // `commit_partition_entry`, not here: this persist path is threshold-
        // gated, so counting here would leave the stats lagging the visible
        // offset until a flush and would double-count once it fires.
        if let Some(durable_offset) = durable_offset {
            self.offset.store(durable_offset, Ordering::Release);
            self.stats.set_current_offset(durable_offset);
        }
        Ok(())
    }

    /// Evict the committed prefix (the `count` front entries read by
    /// `committed_prefix`) and reset `journal.info` to reflect only the
    /// uncommitted tail left resident, so the next persist threshold counts that
    /// tail alone. Call once the prefix is durable, or when there is nothing to
    /// persist. The retained tail's accounting is folded from the meta
    /// `evict_prefix` surfaced during its re-append, so the tail is not decoded
    /// a second time.
    async fn evict_committed_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let retained = self.log.journal().inner.evict_prefix(count).await;
        let mut retained_info = JournalInfo::default();
        for (entry, meta) in &retained {
            // Purge floor: a retained pre-purge batch must not fold its
            // accounting back into `journal.info`, or the info would re-adopt
            // a pre-purge `current_offset` the purge just reset.
            if peek_op(entry) <= self.purge_floor_op {
                continue;
            }
            if let Some(meta) = meta {
                accumulate_committed_info(
                    &mut retained_info,
                    meta.base_offset,
                    meta.base_timestamp,
                    meta.total_size,
                    meta.message_count,
                );
            }
        }
        self.log.journal_mut().info = retained_info;
    }

    #[allow(clippy::too_many_lines)]
    async fn handle_committed_entries(
        &mut self,
        drained: Vec<PipelineEntry>,
        config: &PartitionsConfig,
        send_client_replies: bool,
    ) {
        let replica_id = self.consensus.replica();
        let namespace_raw = self.consensus.group();
        let drained_count = drained.len();
        if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id,
                first_op = first.header.op,
                last_op = last.header.op,
                drained_count,
                "draining committed partition ops"
            );
        }

        let mut failed_commit = false;
        // Must run BEFORE the commit loop: `commit_messages` evicts the
        // committed prefix, after which an entry survives only in the bounded
        // repair ring - and not even there on a single replica, which keeps no
        // ring at all. A miss degrades to a successful send carrying no
        // confirmation, a legal answer no client can tell from a real one.
        let committed_batch_stats = self.resolve_committed_visible_offsets(&drained);
        let mut messages_committed = false;

        for (mut entry, batch_stats) in drained.into_iter().zip(committed_batch_stats) {
            let prepare_header = entry.header;
            if !self
                .commit_partition_entry(
                    prepare_header,
                    &mut messages_committed,
                    batch_stats,
                    &mut failed_commit,
                    config,
                )
                .await
            {
                // Local commit failed but cluster committed (op came from
                // drain_committable_prefix). Replica diverged, can't serve
                // reads.
                //
                // `continue` is unsafe: failed op popped, commit_min not
                // advanced; next advance_commit_min(op+1) would assert
                // op+1 == commit_min + 1, panics cryptically.
                //
                // Fatal, but NOT by panicking: this runs on the shard's
                // message pump, and `compio::runtime::spawn` swallows a panic
                // there, leaving every partition on the shard unable to
                // commit, tick or reply while the process still reports
                // healthy and answers on its other shards. Fence the
                // partition and stop draining; the shard's tick picks the
                // fault up and takes the server down through the ordinary
                // shutdown path, so the remaining partitions flush and the
                // exit code is non-zero. Operator restarts; recovery+repair
                // re-syncs.
                error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id,
                    namespace_raw,
                    op = prepare_header.op,
                    operation = ?prepare_header.operation,
                    "partition local commit failed for a cluster-committed op; \
                     replica is divergent, fencing the partition and shutting down"
                );
                self.fatal = Some(FatalCommit {
                    namespace_raw,
                    op: prepare_header.op,
                    operation: prepare_header.operation,
                });
                return;
            }

            self.consensus.advance_commit_min(prepare_header.op);

            // Fold the committed request into this group's dedup slice. Runs on
            // EVERY replica, not just the one that replies, so a promoted
            // primary can absorb a replay of what its predecessor committed.
            // Auto-commit ops carry the reserved sentinel client and no client
            // ever replays them.
            if !is_auto_commit_client(prepare_header.client) {
                self.dedup.commit_request(
                    prepare_header.client,
                    prepare_header.request,
                    prepare_header.op,
                );
            }

            let pipeline_depth = self.consensus.pipeline_len();
            let event = CommitLogEvent {
                replica: ReplicaLogContext::from_consensus(&self.consensus, PlaneKind::Partitions),
                op: prepare_header.op,
                client_id: prepare_header.client,
                request_id: prepare_header.request,
                operation: prepare_header.operation,
                pipeline_depth,
            };
            emit_sim_event(SimEventKind::OperationCommitted, &event);
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &event.replica,
                prepare_header.op,
                pipeline_depth,
            );

            // No reply cache: an absorbed duplicate is answered by
            // synthesizing the same empty success at admission, so no committed
            // bytes need keeping. Only the primary delivers replies; backups
            // just advance commit and fold the slice. Session lifecycle is
            // metadata-only.
            //
            // A server-generated auto-commit op (a poll's `auto_commit`,
            // replicated for failover) carries the reserved
            // `AUTO_COMMIT_CLIENT_ID`: no client ever waits on it, so skip the
            // reply. Emitting it would push an unrequested frame onto a real
            // client's lockstep reply stream if the sentinel ever routed there.
            if send_client_replies && !is_auto_commit_client(prepare_header.client) {
                let body = match prepare_header.operation {
                    Operation::SendMessages => {
                        send_messages_reply_body(prepare_header.group, batch_stats)
                    }
                    operation => committed_reply_body(operation),
                };
                let reply = build_reply_message(&prepare_header, &body);
                emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

                // An in-process waiter takes the reply instead of the bus: it
                // arrived as a `PartitionSubmit`, so `header.client` is the VSR
                // consensus id and carries no home-shard routing. The awaiting
                // shard owns the socket. A dropped receiver is ignored -- the
                // client recovers on its own read-timeout.
                if let Some(sender) = entry.take_reply_sender() {
                    let _ = sender.send(reply);
                } else if let Err(error) = self
                    .consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, reply.into_generic().into_frozen())
                    .await
                {
                    tracing::error!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        client = prepare_header.client,
                        op = prepare_header.op,
                        namespace_raw,
                        %error,
                        "client reply forward failed, no retransmit path; client will time out",
                    );
                }
            }
        }

        if failed_commit {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id,
                namespace_raw,
                "partition failed local commit handling for one or more ops"
            );
        }

        // Each commit frees one prepare slot, promote up to drained_count
        // buffered requests so the pipeline stays busy.
        self.drain_request_queue_into_prepares(drained_count).await;
    }

    /// Batch stats for each drained entry, positionally parallel to `drained`.
    /// Every entry contributes exactly one slot (`None` for the operations that
    /// carry no batch), which is what makes the pairing correct by
    /// construction; keying on `op` instead would let a lookup miss attribute
    /// one batch's offsets to another entry's reply.
    fn resolve_committed_visible_offsets(
        &self,
        drained: &[PipelineEntry],
    ) -> Vec<Option<CommittedBatchStats>> {
        drained
            .iter()
            .map(|entry| {
                if entry.header.operation != Operation::SendMessages {
                    return None;
                }
                // Purge floor: a pre-purge send committing after the purge is
                // DELIBERATELY degraded to ZERO confirmations rather than
                // failed. Its messages are genuinely gone (the purge deleted
                // the segment they would have landed in) and no offset is left
                // to report, so the reply carries the established "committed,
                // no offsets to report" shape (`send_messages_reply_body`'s
                // empty confirmation list, byte-identical to what a send
                // without confirmation returns): the client sees success with
                // an empty confirmations list and re-sends if it needs the
                // offset. A typed transient status was the alternative and is
                // wrong here -- the op DID commit cluster-wide, so telling the
                // client to retry duplicates a committed send into the
                // post-purge offset space. `None` is also what keeps
                // `commit_partition_entry` from re-advancing the reset offset
                // and stats with pre-purge values.
                if entry.header.op <= self.purge_floor_op {
                    return None;
                }

                match self.committed_batch_stats_for_prepare(&entry.header) {
                    Ok(batch_stats) => batch_stats,
                    Err(error) => {
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            replica_id = self.consensus.replica(),
                            namespace_raw = self.namespace().inner(),
                            op = entry.header.op,
                            operation = ?entry.header.operation,
                            %error,
                            "failed to resolve committed visible offset for partition entry"
                        );
                        None
                    }
                }
            })
            .collect()
    }

    async fn commit_partition_entry(
        &mut self,
        prepare_header: PrepareHeader,
        messages_committed: &mut bool,
        batch_stats: Option<CommittedBatchStats>,
        failed_commit: &mut bool,
        config: &PartitionsConfig,
    ) -> bool {
        match prepare_header.operation {
            Operation::SendMessages => {
                if !*messages_committed {
                    if let Err(error) = self.commit_messages(config).await {
                        *failed_commit = true;
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            replica_id = self.consensus.replica(),
                            namespace_raw = self.namespace().inner(),
                            op = prepare_header.op,
                            operation = ?prepare_header.operation,
                            %error,
                            "failed to commit partition messages"
                        );
                        return false;
                    }
                    *messages_committed = true;
                }

                if let Some(batch_stats) = batch_stats {
                    let end_offset = batch_stats.end_offset();
                    // A repaired batch at or below the boot-time recovered
                    // durable offset was already counted (and persisted)
                    // before the restart; skip it. Live traffic always sits
                    // above the (immutable) line.
                    if self
                        .recovered_durable_offset
                        .is_none_or(|durable| end_offset > durable)
                    {
                        self.offset.store(end_offset, Ordering::Release);
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
                !*failed_commit
            }
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                self.commit_consumer_offset_entry(prepare_header, failed_commit)
                    .await
            }
            _ => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica_id = self.consensus.replica(),
                    op = prepare_header.op,
                    namespace_raw = self.namespace().inner(),
                    operation = ?prepare_header.operation,
                    "unexpected committed partition operation"
                );
                true
            }
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
    /// so nothing replicates.
    /// Answer an absorbed duplicate with the reply its original earned. Same
    /// delivery split as [`Self::send_partition_deny_or_log`]: the submit's
    /// channel when one is waiting, the bus otherwise.
    async fn answer_duplicate_or_log(
        consensus: &VsrConsensus<B>,
        header: &RoutedRequestHeader,
        reply: Message<ReplyHeader>,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        if let Some(waiter) = waiter {
            let _ = waiter.send(reply);
            return;
        }
        if let Err(send_error) = consensus
            .message_bus()
            .send_to_client(header.client, reply.into_generic().into_frozen())
            .await
        {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                    "duplicate reply send failed",
                )
                .with_operation(header.operation)
                .with_error(send_error.to_string()),
            );
        }
    }

    /// `waiter` is the submit's in-process channel, taken by the caller. When
    /// present the deny goes there: `header.client` is then the VSR consensus
    /// id, which the bus cannot route.
    async fn send_partition_deny_or_log(
        consensus: &VsrConsensus<B>,
        header: &RoutedRequestHeader,
        status: u32,
        send_fail_label: &'static str,
        waiter: Option<consensus::Sender<Message<ReplyHeader>>>,
    ) {
        let reply = build_deny_reply_from_request(consensus, header, status);
        if let Some(waiter) = waiter {
            let _ = waiter.send(reply);
            return;
        }
        if let Err(send_error) = consensus
            .message_bus()
            .send_to_client(header.client, reply.into_generic().into_frozen())
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
        // Deep copy: the journal buffer is shared and `Message::try_from`
        // wants an `Owned`; this path only runs on the post-view-change
        // fallback, never per-commit.
        let owned = Owned::<MESSAGE_ALIGN>::copy_from_slice(entry.as_slice());
        let message = Message::<GenericHeader>::try_from(owned)
            .map_err(|_| IggyError::InvalidCommand)?
            .try_into_typed::<PrepareHeader>()
            .map_err(|_| IggyError::InvalidCommand)?;
        let header = *message.header();
        let (kind, consumer_id, offset, _ack) =
            Self::parse_staged_consumer_offset_commit(header.operation, &message)?;
        match header.operation {
            Operation::StoreConsumerOffset => {
                let offset = offset.ok_or(IggyError::InvalidCommand)?;
                Ok(if is_auto_commit_client(header.client) {
                    PendingConsumerOffsetCommit::upsert_auto_commit(kind, consumer_id, offset)
                } else {
                    PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset)
                })
            }
            Operation::DeleteConsumerOffset => {
                Ok(PendingConsumerOffsetCommit::delete(kind, consumer_id))
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }

    fn parse_staged_consumer_offset_commit(
        operation: Operation,
        message: &Message<PrepareHeader>,
    ) -> Result<(ConsumerKind, u32, Option<u64>, AckLevel), IggyError> {
        let total_size =
            usize::try_from(message.header().size).map_err(|_| IggyError::InvalidCommand)?;
        let body = message
            .as_slice()
            .get(std::mem::size_of::<PrepareHeader>()..total_size)
            .ok_or(IggyError::InvalidCommand)?;
        Self::parse_consumer_offset_payload(operation, body)
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

    async fn commit_consumer_offset_entry(
        &mut self,
        prepare_header: PrepareHeader,
        failed_commit: &mut bool,
    ) -> bool {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        // Purge floor: the purge cleared the offset maps and files, so a
        // pre-purge store committing now must not resurrect its offset. An op
        // guard, not a bare staged-table clear at purge time:
        // `restage_consumer_offset_from_journal` re-derives pending commits
        // from the kept journal entries, so a cleared table alone would be
        // repopulated from the entry this guard is fencing.
        if prepare_header.op <= self.purge_floor_op {
            self.pending_consumer_offset_commits
                .remove(&prepare_header.op);
            return true;
        }

        if let Err(error) = self
            .apply_staged_consumer_offset_commit(prepare_header.op)
            .await
        {
            *failed_commit = true;
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id = self.consensus.replica(),
                op = prepare_header.op,
                namespace_raw = self.namespace().inner(),
                %error,
                "failed to apply staged consumer offset commit"
            );
            return false;
        }

        debug!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            replica_id = self.consensus.replica(),
            op = prepare_header.op,
            namespace_raw = self.namespace().inner(),
            "consumer offset committed"
        );
        true
    }

    async fn persist_frozen_batches_to_disk(
        &mut self,
        frozen_batches: Vec<Frozen<4096>>,
        index_bytes: Vec<u8>,
        batch_count: u32,
    ) -> Result<(), IggyError> {
        if batch_count == 0 {
            return Ok(());
        }

        if !self.log.has_segments() {
            return Ok(());
        }

        let stripped_batches: Vec<_> = frozen_batches
            .into_iter()
            .map(|batch| batch.slice(std::mem::size_of::<PrepareHeader>()..))
            .collect();
        let messages_writer = self
            .log
            .messages_writers()
            .last()
            .and_then(|writer| writer.as_ref())
            .cloned();
        let index_writer = self
            .log
            .index_writers()
            .last()
            .and_then(|writer| writer.as_ref())
            .cloned();

        if messages_writer.is_none() || index_writer.is_none() {
            let saved_bytes = stripped_batches.iter().map(Frozen::len).sum::<usize>();
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                batch_count,
                saved_bytes,
                "simulated in-memory batch persistence"
            );

            let segment_index = self.log.segments().len() - 1;
            let segment = &mut self.log.segments_mut()[segment_index];
            segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved_bytes as u64);
            return Ok(());
        }

        let messages_writer = messages_writer.expect("checked above");
        let index_writer = index_writer.expect("checked above");

        // Both writes are in flight before either completes, so under
        // `enforce_fsync` the two fdatasync round trips overlap instead of
        // serializing. `join` never cancels a half, so no write is dropped
        // mid-flight when the other one fails.
        let (log_result, index_result) = futures::future::join(
            messages_writer.save_frozen_batches(&stripped_batches),
            index_writer.save_indexes(index_bytes),
        )
        .await;

        // The match below collapses to the log's error (the durable record, the
        // index being derived from it). The halves write different files and can
        // fail for unrelated reasons, so name the index failure here instead of
        // letting the log's error stand for both.
        if let (Err(log_error), Err(index_error)) = (&log_result, &index_result) {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                batch_count,
                %log_error,
                %index_error,
                "failed to persist frozen batches: log and index both failed"
            );
        }

        let (saved, saved_index_bytes) = match (log_result, index_result) {
            (Ok(saved), Ok(saved_index_bytes)) => (saved, saved_index_bytes),
            (Err(error), _) | (Ok(_), Err(error)) => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    batch_count,
                    %error,
                    "failed to persist frozen batches"
                );
                return Err(error);
            }
        };

        // Advance both cursors only here, back to back with no await between.
        // They are the writers' next-write positions, so a half whose save
        // failed must keep its cursor: the pair then still describes one durable
        // prefix, which is what a writer re-opened by boot recovery asserts
        // against the length of the file it finds
        // (`SegmentSizeMismatchAtOpen`). In-process it also keeps a later flush
        // rewriting the same slot rather than appending a duplicate index entry
        // or leaving a hole in the segment.
        messages_writer.advance(saved.as_bytes_u64());
        index_writer.advance(saved_index_bytes);

        debug!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = self.namespace().inner(),
            batch_count,
            saved_bytes = saved.as_bytes_u64(),
            "persisted batches to disk"
        );

        let segment_index = self.log.segments().len() - 1;
        let segment = &mut self.log.segments_mut()[segment_index];
        segment.size = IggyByteSize::from(segment.size.as_bytes_u64() + saved.as_bytes_u64());

        Ok(())
    }

    async fn rotate_segment(&mut self, config: &PartitionsConfig) -> Result<(), IggyError> {
        let namespace = self.namespace();
        let old_segment_index = self.log.segments().len() - 1;
        let active_segment = self.log.active_segment_mut();
        active_segment.sealed = true;
        let start_offset = active_segment.end_offset + 1;

        let segment_size = self.effective_segment_size(config);
        let enforce_fsync = self.effective_enforce_fsync(config);
        let preallocate_segments = self.effective_preallocate_segments(config);
        let segment = Segment::new(start_offset, segment_size);
        // Prefer the active writer's location: a per-topic path override or a
        // config change after the initial segment was created must not scatter
        // one partition's segments across two directories. The config layout
        // only decides for a partition with no writer yet.
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
            |dir| {
                (
                    format!("{dir}/{start_offset:0>20}.log"),
                    format!("{dir}/{start_offset:0>20}.index"),
                )
            },
        );

        let storage = SegmentStorage::new(&messages_path, &index_path, 0, 0, false)
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
        let messages_size_bytes = storage
            .messages_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?
            .size_counter();
        let messages_writer = Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_bytes,
                enforce_fsync,
                false,
                preallocate_segments.then_some(segment_size),
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?,
        );
        let index_size_bytes = storage
            .index_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?
            .size_counter();
        let index_writer = Rc::new(
            IggyIndexWriter::new(&index_path, index_size_bytes, enforce_fsync, false)
                .await
                .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );

        let old_storage = &mut self.log.storages_mut()[old_segment_index];
        let _ = old_storage.shutdown();
        self.log.messages_writers_mut()[old_segment_index] = None;
        self.log.index_writers_mut()[old_segment_index] = None;
        // Drop the sealed segment's in-memory index cache: only the ACTIVE
        // segment's cache is ever read (the `commit_messages` flush staging),
        // so a sealed cache is dead weight.
        self.log.indexes_mut()[old_segment_index] = None;
        // The read fd cached while this segment was active is not counted by
        // the sealed LRU budget, so it must not survive the seal; the next
        // sealed poll re-fills the fresh slot under the LRU's rules.
        self.log.reset_read_state(old_segment_index);

        self.log
            .add_persisted_segment(segment, storage, Some(messages_writer), Some(index_writer));
        self.stats.increment_segments_count(1);

        debug!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = namespace.inner(),
            start_offset,
            "rotated to new segment"
        );
        Ok(())
    }

    /// Minimum committed offset across all consumers and consumer groups, with
    /// the holder's identity. `None` when nothing has been committed, in which
    /// case there is no deletion barrier.
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

    /// Time-expiry plus size-retention in one pass: remove the leading sealed
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
        let expired = leading_expired_end(self.log.segments(), now, message_expiry);
        let oversized =
            max_bytes.and_then(|max_bytes| leading_oversized_end(self.log.segments(), max_bytes));
        let Some(up_to) = expired.into_iter().chain(oversized).max() else {
            return SegmentRemoval::default();
        };
        self.remove_sealed_segments_up_to(up_to).await
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
    /// Holds `write_lock` to serialize against the commit/rotate path, which
    /// runs on the separate consensus-tick loop.
    pub async fn remove_sealed_segments_up_to(&mut self, up_to_offset: u64) -> SegmentRemoval {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let barrier = self.min_committed_offset();
        let namespace = self.namespace();
        let removable = {
            let segments = self.log.segments();
            let last_idx = segments.len().saturating_sub(1);
            let mut removable = 0usize;
            // One past the budget: the extra slot separates a run that ends
            // exactly on the budget from one with more still waiting.
            for (idx, segment) in segments
                .iter()
                .enumerate()
                .take(SEGMENT_REMOVAL_BUDGET_PER_PASS + 1)
            {
                if idx == last_idx || !segment.sealed || segment.end_offset > up_to_offset {
                    break;
                }
                if let Some((barrier_offset, kind, consumer_id)) = barrier
                    && segment.end_offset > barrier_offset
                {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = namespace.inner(),
                        start_offset = segment.start_offset,
                        end_offset = segment.end_offset,
                        barrier = barrier_offset,
                        %kind,
                        consumer_id,
                        "segment retained: blocked by committed consumer offset"
                    );
                    break;
                }
                removable += 1;
            }
            removable
        };

        let budget_spent = removable > SEGMENT_REMOVAL_BUDGET_PER_PASS;
        let removable = removable.min(SEGMENT_REMOVAL_BUDGET_PER_PASS);
        let mut removal = SegmentRemoval {
            budget_spent,
            ..SegmentRemoval::default()
        };
        for _ in 0..removable {
            // The removable run is always a prefix (oldest first), so the next
            // victim is the front once the previous one is gone.
            let Some((segment, mut storage)) = self.log.retire_front() else {
                break;
            };

            let (messages_path, index_path) = storage.segment_and_index_paths();
            let _ = storage.shutdown();
            drop(storage);

            for path in messages_path.into_iter().chain(index_path) {
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            namespace_raw = namespace.inner(),
                            path = %path,
                            %error,
                            "failed to unlink segment file during cleanup"
                        );
                    }
                }
            }

            let segment_size = segment.size.as_bytes_u64();
            // The removal loop above only reaches sealed segments, which always
            // hold at least one message, so the count is inclusive end..=start.
            // A one-message sealed segment has `start_offset == end_offset`, so
            // the `+ 1` is required (a `start == end -> 0` special case would
            // undercount it).
            let messages_in_segment = segment.end_offset - segment.start_offset + 1;
            self.stats.decrement_size_bytes(segment_size);
            self.stats.decrement_segments_count(1);
            self.stats.decrement_messages_count(messages_in_segment);

            removal.segments += 1;
            removal.messages += messages_in_segment;

            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                start_offset = segment.start_offset,
                end_offset = segment.end_offset,
                "deleted sealed segment during cleanup"
            );
        }

        removal
    }

    /// Build and install a fresh empty segment starting at `start_offset` with
    /// real on-disk writers. Paths are derived from the partition directory
    /// (see `rotate_segment`); falls back to the config-derived path for
    /// in-memory partitions with no directory.
    ///
    /// Both files are opened through `SegmentStorage::new` with
    /// `file_exists = false`, which TRUNCATES them. That is load-bearing, not
    /// incidental: this offset may already have an `.index` on disk (a crash
    /// between the state-transfer install's index-rename and log-rename loops
    /// leaves final-name indexes with no logs, and the boot sweep only reaches
    /// the ones still orphaned at startup). The `partitions`-side writers with
    /// the same names do NOT truncate, so a recreate path that opened them
    /// directly would read index entries from a previous generation.
    ///
    /// # Errors
    /// If the segment's log / index file cannot be created.
    pub(crate) async fn install_empty_segment(
        &mut self,
        config: &PartitionsConfig,
        start_offset: u64,
    ) -> Result<(), IggyError> {
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
            |dir| {
                (
                    format!("{dir}/{start_offset:0>20}.log"),
                    format!("{dir}/{start_offset:0>20}.index"),
                )
            },
        );
        let segment_size = self.effective_segment_size(config);
        let enforce_fsync = self.effective_enforce_fsync(config);
        let preallocate_segments = self.effective_preallocate_segments(config);
        let segment = Segment::new(start_offset, segment_size);
        let storage = SegmentStorage::new(&messages_path, &index_path, 0, 0, false)
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
        let messages_size_bytes = storage
            .messages_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?
            .size_counter();
        let messages_writer = Rc::new(
            MessagesWriter::new(
                &messages_path,
                messages_size_bytes,
                enforce_fsync,
                false,
                preallocate_segments.then_some(segment_size),
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?,
        );
        let index_size_bytes = storage
            .index_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?
            .size_counter();
        let index_writer = Rc::new(
            IggyIndexWriter::new(&index_path, index_size_bytes, enforce_fsync, false)
                .await
                .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );
        self.log
            .add_persisted_segment(segment, storage, Some(messages_writer), Some(index_writer));
        Ok(())
    }

    /// Record the purge's frontier reset BEFORE the purge touches anything.
    ///
    /// The unlinks are made durable by their own directory fsync, so a crash
    /// between them and a reset written afterwards boots a purged directory
    /// whose record still names the pre-purge offset space:
    /// `restore_offset_frontier` re-seeds the counter to it while every peer
    /// restarted at 0, and the first append stamps a `base_offset` and
    /// `batch_checksum` no peer shares. Writing 0 first inverts the window into
    /// a harmless one -- the record under-claims while the segments still
    /// exist, and boot takes the max of the record and what the segments prove.
    ///
    /// Spelled out rather than read off the counter, which still holds the
    /// pre-purge frontier at this point.
    ///
    /// # Errors
    /// [`PurgeError::FrontierNotRecorded`]. Refused rather than logged: nothing
    /// has been mutated yet, and a purge that cannot record its reset must not
    /// be the one that erases the data proving the old frontier. The caller
    /// RETRIES; it must not fence, since the chain is still whole and the live
    /// counter still names the pre-purge space.
    #[allow(clippy::future_not_send)]
    async fn record_purge_frontier_reset(&mut self, generation: u64) -> Result<(), PurgeError> {
        if self.reset_offset_frontier_at(0).await {
            self.purge_deferred = false;
            return Ok(());
        }
        self.purge_deferred = true;
        // The ONLY operator-visible signal for the withhold: `send_prepare_ok`
        // returns silently, correctly, since it runs per prepare. So this line
        // has to say that the replica is now out of quorum for this group, or
        // the symptom reads as a network fault. The consecutive count
        // correlates it with the superblock writer's own error log, which
        // carries the `ENOSPC` / `EIO` cause but is rate-limited to
        // power-of-two failures, while this deferral repeats per reconciler
        // pass.
        warn!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = self.namespace().inner(),
            generation,
            superblock_write_failures = self.superblock_write_failures.get(),
            "cannot record the purge's offset-frontier reset; deferring the purge so the \
             durable frontier cannot outlive the data it describes. This replica now \
             withholds PrepareOk for this partition until the purge lands, so it is \
             quorum-invisible there; its other partitions are unaffected"
        );
        Err(PurgeError::FrontierNotRecorded)
    }

    /// Reset the partition to a single empty segment at offset 0 and clear all
    /// consumer / consumer-group offsets (memory + disk). This is the local
    /// effect of a committed `PurgeTopic`: it wipes message data and offsets but
    /// preserves the partition and its consumer-group membership. Mirrors the
    /// legacy server's `purge_all_segments` + offset-file deletion.
    ///
    /// Records `generation` as the applied purge generation so the reconciler
    /// does not re-wipe a partition already purged at this generation (a later
    /// `PurgeTopic` advances the committed generation and triggers a fresh pass).
    ///
    /// # Errors
    /// [`PurgeError::FrontierNotRecorded`] before anything is mutated, which
    /// the caller RETRIES: the reconciler re-issues the purge while
    /// `committed > applied`, and fencing a partition that still holds its whole
    /// chain would quarantine live data behind a counter that still names the
    /// pre-purge offset space. [`PurgeError::Unserviceable`] once the drain has
    /// run, which the caller FENCES (quarantine + retire for the reconciler to
    /// rebuild), exactly as the state-transfer install's `ConvergeFailed` arm
    /// does, or the next append panics on `active_segment()`.
    #[allow(clippy::too_many_lines)]
    pub async fn purge(
        &mut self,
        config: &PartitionsConfig,
        generation: u64,
    ) -> Result<(), PurgeError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let namespace = self.namespace();

        self.record_purge_frontier_reset(generation).await?;

        // The purge recreates segment files at the paths it unlinks below, so
        // an in-flight poll's cached read fd would keep serving the unlinked
        // pre-purge inodes as live data. Wipe the shared read-state slots
        // first: the clones held by suspended walks observe the wipe and their
        // next segment resolve re-opens by path, seeing the fresh empty files.
        self.log.invalidate_sealed_read_state();

        // Drain every segment (including the active one) and unlink its files.
        let segment_count = self.log.segments().len();
        for _ in 0..segment_count {
            let Some((_, mut storage)) = self.log.retire_front() else {
                break;
            };

            let (messages_path, index_path) = storage.segment_and_index_paths();
            let _ = storage.shutdown();
            drop(storage);

            for path in messages_path.into_iter().chain(index_path) {
                match compio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        warn!(
                            target: "iggy.partitions.diag",
                            plane = "partitions",
                            namespace_raw = namespace.inner(),
                            path = %path,
                            %error,
                            "failed to unlink segment file during purge"
                        );
                    }
                }
            }
        }

        // An in-flight state transfer was pulling the PRE-purge state: its
        // staged segments hold data this purge just deleted, and letting the
        // session complete renames it back in -- durably, because the install
        // takes `max(offer generation, applied)` and this purge already stamped
        // the newer generation, so the reconciler's purge gate never re-fires
        // and the resurrected data outlives the process. Drop the session,
        // cancel the scheduled re-arm, release the transfer stage so the
        // ordinary triggers can arm a fresh one, and sweep the staged bytes.
        self.transfer = None;
        self.transfer_rearm = None;
        let consensus = self.consensus();
        if consensus.state_transfer_stage() != consensus::StateTransferStage::Idle {
            consensus.set_state_transfer_stage(consensus::StateTransferStage::Idle);
        }
        self.reuse_scan_memo.borrow_mut().take();
        if let Some(partition_dir) = self.partition_dir.clone() {
            crate::state_transfer::sweep_staging_except(&partition_dir, &HashSet::new()).await;
        }

        let start_offset = 0u64;
        // Counters reset BEFORE the fallible plant, not after: `?` on
        // `install_empty_segment` would otherwise leave the live counter at the
        // pre-purge value, which is what the router's purge-failure fence then
        // records and what a restart would re-seed. Safe to reorder --
        // `install_empty_segment` takes `start_offset` as a parameter and never
        // reads the counter, and the partition write lock is held across this
        // whole body.
        self.offset.store(start_offset, Ordering::Release);
        self.dirty_offset.store(start_offset, Ordering::Relaxed);
        self.should_increment_offset = false;

        // Recreate a fresh empty segment at offset 0 with real writers. Every
        // segment is drained by now, so a failure here is the fence case.
        self.install_empty_segment(config, start_offset)
            .await
            .map_err(PurgeError::Unserviceable)?;
        // Make the unlinks AND the replanted dirent durable together: without
        // this a crash can resurrect pre-purge segments until the boot re-purge
        // fires. Bounded and self-healing, so a failure is logged, not fenced:
        // the generation write below has not run yet, so a crash after a failed
        // fsync re-purges at boot anyway.
        if let Some(partition_dir) = self.partition_dir.clone()
            && let Err(error) = crate::state_transfer::fsync_dir(&partition_dir).await
        {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                generation,
                %error,
                "purge could not fsync the partition dir; a crash before the \
                 generation record re-purges at boot"
            );
        }
        // The boot-time durable line marks recovered bytes that must not be
        // re-persisted, but the purge just deleted those bytes and offsets
        // restart at 0. Keeping it would make every post-purge batch at or
        // below the old line evict silently without ever reaching a segment.
        // The installed frontier goes with it: the offset space genuinely
        // restarts, so nothing "stands in" below any floor anymore.
        self.recovered_durable_offset = None;
        self.installed_frontier = None;
        self.segment_checksum_cache.borrow_mut().clear();

        // Clear consumer + consumer-group offsets (memory + disk). Collect the
        // file paths before deleting so the map guard is not held across an
        // await.
        let consumer_paths: Vec<String> = {
            let guard = self.consumer_offsets.pin();
            let paths = guard
                .iter()
                .filter_map(|(key, _)| {
                    u32::try_from(*key)
                        .ok()
                        .and_then(|id| self.persisted_offset_path(ConsumerKind::Consumer, id))
                })
                .collect();
            guard.clear();
            paths
        };
        let group_paths: Vec<String> = {
            let guard = self.consumer_group_offsets.pin();
            let paths = guard
                .iter()
                .filter_map(|(key, _)| {
                    u32::try_from(key.0)
                        .ok()
                        .and_then(|id| self.persisted_offset_path(ConsumerKind::ConsumerGroup, id))
                })
                .collect();
            guard.clear();
            paths
        };
        // Sweep the directories too, not just the map-derived paths: a purge is a
        // full reset, and an offset file the live map never held -- a pre-purge
        // op re-persisted by journal repair on a restarted replica -- would
        // otherwise survive for boot to hydrate back.
        let strayed =
            crate::state_transfer::strayed_offset_files(self.consumer_offsets_path.as_deref(), &[])
                .into_iter()
                .chain(crate::state_transfer::strayed_offset_files(
                    self.consumer_group_offsets_path.as_deref(),
                    &[],
                ));
        for path in consumer_paths.into_iter().chain(group_paths).chain(strayed) {
            let _ = delete_persisted_offset(&path).await;
        }
        // Directory fsync so those unlinks stick, mirroring the install path: a
        // crash right after the purge otherwise resurrects the offset files at
        // boot, and while recovery clamps a resurrected offset down to the
        // rebuilt head, "consumed through 0" is not the intended "no entry at
        // all" -- that consumer skips the first post-purge message. Logged on
        // failure, sharper than the partition-dir fsync above: the generation
        // write below still runs, so a crash would resurrect these files with
        // no boot re-purge left to clear them.
        for dir in self
            .consumer_offsets_path
            .clone()
            .into_iter()
            .chain(self.consumer_group_offsets_path.clone())
        {
            if let Err(error) = crate::state_transfer::fsync_dir(&dir).await {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    generation,
                    dir = %dir,
                    %error,
                    "purge could not fsync an offsets dir; a crash may resurrect \
                     deleted offset files with the purge already recorded"
                );
            }
        }
        // The persisted-offset tracker mirrors the files unlinked above; a
        // stale entry would make a post-purge auto-commit skip its write and
        // lose the offset on restart.
        self.persisted_offsets.borrow_mut().clear();

        // Clear the ephemeral cooperative-rebalance tracking too: after the
        // reset to offset 0 a stale `last_polled` (a high pre-purge offset)
        // would make the reconciler's completion check `committed >= last_polled`
        // unsatisfiable, stalling a pending revocation until its timeout.
        self.last_polled_offsets.pin().clear();

        // Reset stats to a single empty segment.
        self.stats.zero_out_all();
        self.stats.increment_segments_count(1);

        // Fence the resident journal instead of clearing it: entries are
        // consensus history (backup commit walks, repair, retransmission), so
        // they stay, but every journal-apply path no-ops ops at or below this
        // floor (see `purge_floor_op`). The write lock held here is the same
        // one appends take, and the pump is single-threaded, so no op can be
        // assigned between reading the sequence and installing the floor.
        self.purge_floor_op = self.consensus.sequencer().current_sequence();
        // The journal's flush accounting and resident poll indexes describe
        // pre-purge bytes; reset them so the flush threshold counts only
        // post-purge appends and polls fall back to the (fresh, empty)
        // segments instead of resolving purged resident entries.
        self.log.journal_mut().info = JournalInfo::default();
        self.log
            .journal()
            .inner
            .clear_poll_index(self.purge_floor_op);
        // Hand the already-walked fenced prefix to the normal eviction path so
        // an idle purged partition does not pin it resident: the flush that
        // would otherwise evict it is gated on `journal.info.messages_count`,
        // which the reset above just zeroed, so with no post-purge traffic the
        // entries never leave. Repair semantics are unchanged -- `evict_prefix`
        // moves them into the evicted ring, still op-addressable by
        // `repair_entry`, and the serve path clamps `retained_from` above the
        // floor anyway. Bounded at `commit_min`, NOT `commit_max`: an op the
        // commit walk has not reached yet still needs its header resident, or
        // `committed_headers_from` stops at the hole and wedges `commit_min`.
        let fenced_prefix = self
            .log
            .journal()
            .inner
            .committed_prefix(self.consensus.commit_min().min(self.purge_floor_op))
            .len();
        self.evict_committed_prefix(fenced_prefix).await;

        // Last durable step: record the applied generation before the
        // in-memory marker advances. On a write failure the marker stays old,
        // the error propagates, and the reconciler retries the whole purge
        // (idempotent, the chain is already empty). The reverse order would
        // ack a purge that a crash then silently undoes: restart would
        // hydrate the old generation, yet the reconciler believes the purge
        // applied. Deferring PrepareOk mirrors the frontier-record failure:
        // an op acked now would be wiped by the retry purge while peers that
        // recorded the generation keep it.
        if let Some(dir) = self.partition_dir() {
            let path = format!("{dir}/{PURGE_GENERATION_FILE}");
            if let Err(error) =
                persist_purge_generation(&path, generation, self.created_revision).await
            {
                self.purge_deferred = true;
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = namespace.inner(),
                    generation,
                    %error,
                    "purge reset the partition but could not record its applied generation; \
                     deferring PrepareOk until the re-issued purge records it"
                );
                return Err(PurgeError::GenerationNotRecorded(error));
            }
        }
        self.applied_purge_generation = generation;
        // Same commit frontier, different (now empty) bytes: a cached offer
        // built pre-purge would advertise files the purge just unlinked.
        self.transfer_offer_cache.borrow_mut().take();
        // The reset itself already landed before the unlinks; this second write
        // only re-stamps the record now that the view-scoped fields and the
        // counter agree with it. A failure leaves the pre-unlink 0 on disk,
        // which is the safe direction, so it is logged rather than refused.
        if !self.reset_offset_frontier().await {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                generation,
                "purge could not re-stamp the superblock after resetting the partition; \
                 the frontier reset written before the unlinks still stands"
            );
        }
        Ok(())
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

    /// Ingest one repaired prepare: journal + stage it exactly like a live
    /// replicated op, minus the view fence, the gap check, and the ack (the
    /// op is already committed cluster-wide; there is nobody to ack to). The
    /// commit walk runs at `RepairDone`, after the floor is known.
    pub async fn apply_repaired_prepare(&mut self, message: Message<PrepareHeader>) {
        let header = *message.header();
        let Some(session) = self.repair else {
            return;
        };
        let consensus = self.consensus();
        if !consensus.is_normal() || consensus.view() != session.view {
            self.repair = None;
            return;
        }
        if header.op <= consensus.commit_min() || header.op > session.fetch_to_op {
            return;
        }
        let canonical_checksum = consensus
            .with_pending_view_log(|pending| {
                pending
                    .headers
                    .iter()
                    .find(|expected| expected.op == header.op)
                    .map(|expected| expected.checksum)
            })
            .flatten();
        if canonical_checksum.is_some_and(|expected| expected != header.checksum)
            || (header.op > session.commit_to_op && canonical_checksum.is_none())
        {
            return;
        }
        // Any in-window frame proves the stream is alive; only silence
        // should age the stall counter.
        if let Some(session) = self.repair.as_mut() {
            session.idle_ticks = 0;
        }
        if self.log.journal().inner.header_by_op(header.op).is_some() {
            return;
        }
        let applied = if header.operation == Operation::SendMessages {
            match self.append_repaired_send_messages(message).await {
                Ok(base_offset) => {
                    if header.op <= session.commit_to_op
                        && let (Some(base_offset), Some(session)) =
                            (base_offset, self.repair.as_mut())
                    {
                        session.first_batch_offset = Some(
                            session
                                .first_batch_offset
                                .map_or(base_offset, |first| first.min(base_offset)),
                        );
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            }
        } else {
            self.apply_replicated_operation(message).await.map(|_| ())
        };
        if let Err(error) = applied {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                op = header.op,
                %error,
                "failed to journal repaired prepare"
            );
            return;
        }
        // Advance the sequencer only along the CONTIGUOUS journaled
        // frontier. DVC advertises `op = sequencer.current_sequence()` and
        // elections pick the max, so bumping straight to a repaired op that
        // sits above an unfilled hole would let this replica win a view it
        // cannot walk. A dropped frame stalls the frontier here; the stall
        // retry refills the hole and the next apply resumes the advance
        // (walking over ops that were journaled out of order meanwhile).
        // The checksum moves only with the frontier and is read from the
        // journal header at the new head: a lower backfill must not rewind
        // the parent the next prepare chains onto.
        let previous_frontier = self.consensus().sequencer().current_sequence();
        let update = repaired_frontier_update(previous_frontier, |op| {
            self.log.journal().inner.header_by_op(op)
        });
        if let Some((frontier, frontier_checksum)) = update {
            let consensus = self.consensus();
            consensus.sequencer().set_sequence(frontier);
            consensus.set_last_prepare_checksum(frontier_checksum);
        }
    }

    /// Conclude a repair stream: settle the commit floor at the serving
    /// peer's eviction point (everything below it is represented by this
    /// replica's recovered segments + offset files) and walk the repaired
    /// window through the normal commit path.
    pub async fn complete_repair(&mut self, config: &PartitionsConfig) -> RepairConclusion {
        let Some(session) = self.repair else {
            return RepairConclusion::Done;
        };
        if !self.consensus().is_normal() || self.consensus().view() != session.view {
            self.repair = None;
            return RepairConclusion::Done;
        }
        if let Some(floor) = session.floor {
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
            let committed_shape = self
                .log
                .journal()
                .inner
                .repaired_window_shape(floor, session.commit_to_op);
            let connected = match (session.first_batch_offset, stand_in) {
                (Some(first), Some(bound)) => first <= bound,
                (Some(first), None) => first == 0,
                // No repaired batch arrived, so there is no offset anchor to
                // verify the floor's continuum claim against. `None` is only
                // safe when the served window itself proves it carried no
                // messages: every op in `(floor, to_op]` journaled and none
                // of them `SendMessages`. Anything less -- dropped frames, or
                // a fully evicted window -- is indistinguishable from a
                // message range below the floor that this replica does not
                // durably own, and accepting it would serve a holed log.
                (None, _) => {
                    floor < session.commit_to_op
                        && committed_shape.complete
                        && !committed_shape.holds_messages
                }
            };
            if !connected {
                tracing::error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    floor,
                    first_batch_offset = ?session.first_batch_offset,
                    recovered_durable_offset = ?durable_end,
                    "refusing commit floor: repaired window does not connect \
                     to recovered durable state (needs state transfer)"
                );
                self.commit_journal(config).await;
                // A refusal is DEFINITIVE only once the window itself is
                // fully present (or provably empty): until then more frames
                // can still lower `first_batch_offset` into connection, so
                // the session stays armed and the stall retry re-requests.
                // A complete window that still cannot connect will never
                // improve -- the peer retains nothing below the floor and
                // this replica holds nothing either -- and an EMPTY window
                // (everything evicted) re-raises identically every round.
                // Both are the state-transfer trigger; the session is
                // dropped here so the caller's arming funnel starts clean,
                // and a transfer-unavailable fallback re-arms repair fresh.
                if committed_shape.complete {
                    self.repair = None;
                    return RepairConclusion::FloorRefused {
                        floor,
                        to_op: session.commit_to_op,
                    };
                }
                return RepairConclusion::InProgress;
            }
            let commit_min = self.consensus().commit_min();
            if floor > commit_min {
                self.consensus().set_commit_floor(floor);
            }
        }
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
        let done = commit_min >= session.commit_to_op && fetch_complete;
        if done {
            self.repair = None;
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
    ) -> Result<Option<u64>, IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let op = message.header().op;
        let (base_offset, base_timestamp, total_size, message_count) = {
            let batch = decode_prepare_slice(message.as_slice())?;
            (
                batch.header.base_offset,
                batch.header.base_timestamp,
                batch.header.total_size() as u64,
                batch.message_count(),
            )
        };
        if message_count == 0 {
            return Err(IggyError::InvalidCommand);
        }

        // Purge floor: the same fence every other journal-apply path honors. A
        // repaired pre-purge batch is still journaled -- the commit walk stops
        // at the first missing op, so dropping it would wedge `commit_min` --
        // but it must not re-advance the reset counters or re-count purged
        // bytes. `None` also keeps it out of the session's
        // `first_batch_offset`: that anchors the floor-connect check, and
        // purged bytes cannot stand in for durable state.
        if op <= self.purge_floor_op {
            self.log
                .journal()
                .inner
                .append(message.into_frozen())
                .await
                .map_err(|_| IggyError::CannotAppendMessage)?;
            return Ok(None);
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
        self.log
            .journal()
            .inner
            .append(frozen)
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;

        self.should_increment_offset = true;
        self.dirty_offset
            .store(dirty.max(last_offset), Ordering::Relaxed);
        self.log.segments_mut()[segment_index].current_position = next_position;
        self.log.journal_mut().info = journal_info;
        Ok(Some(base_offset))
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        // Durable-before-send: a PrepareOk implies this replica's
        // (view, log_view), so it must not leave until they are durable, or a
        // crash could recover an older view than the one this ack helped
        // commit in, losing a committed op. Mirrors the view-change dispatch
        // gate; withhold on persist failure and let the primary's prepare
        // retransmit re-drive the ack once a later persist succeeds.
        if !self.persist_superblock_if_needed().await {
            return;
        }
        // Same fail-closed shape for a purge this replica accepted but has not
        // applied: its counter still names the pre-purge offset space, so an ack
        // now helps commit an op it will stamp differently from every peer that
        // did apply. The primary's retransmit re-drives the ack once the purge
        // lands. Local commits still apply -- this fences the SEND, exactly as
        // the durability gate above does.
        if self.purge_deferred {
            return;
        }
        // `VsrAction::RetransmitPrepares` reads from `self.log.journal`.
        // Both `SendMessages` (via `append_send_messages_to_journal`) and
        // consumer-offset ops (via `apply_replicated_operation`) append
        // to that journal before `send_prepare_ok` fires, so every op
        // that reaches here is journal-backed and ACKs as durable.
        // (`header_by_op` is a linear scan, so re-proving that here would
        // put O(journal) on every ack; the call-order invariant stands in.)
        send_prepare_ok_common(self.consensus(), header, true).await;
    }
}

/// Commit-apply an upserted offset into a lock-free offset map. A server
/// auto-commit already advanced this offset in memory on the serving poll and
/// this replicated commit can land behind a newer poll, so it must be
/// monotone (`fetch_max`) or it rewinds the map and re-serves consumed
/// messages. An explicit client store keeps the rewinding `store` (an offset
/// reset is a valid action).
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
        crate::poll_plan::upsert_offset_max(map, key, offset, create_on_miss);
    } else {
        crate::poll_plan::upsert_offset(map, key, offset, create_on_miss);
    }
}

/// The operation tag at the front of a journal entry. Every entry begins with a
/// `PrepareHeader`, so reading the tag is a cheap cast, not a full batch decode;
/// it tells a committed consumer-offset op (no segment bytes) apart from a
/// `SendMessages` batch without relying on a decode failure to do so.
fn peek_operation(entry: &Frozen<4096>) -> Operation {
    bytemuck::checked::try_from_bytes::<PrepareHeader>(
        &entry[..std::mem::size_of::<PrepareHeader>()],
    )
    .expect("journal entry must begin with a valid prepare header")
    .operation
}

/// The consensus op of a journal entry, same cheap header cast as
/// [`peek_operation`]. Used by the purge-floor guards to tell pre-purge
/// entries (op at or below the floor) from post-purge ones.
fn peek_op(entry: &Frozen<4096>) -> u64 {
    bytemuck::checked::try_from_bytes::<PrepareHeader>(
        &entry[..std::mem::size_of::<PrepareHeader>()],
    )
    .expect("journal entry must begin with a valid prepare header")
    .op
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
#[allow(clippy::cast_possible_truncation)]
fn send_messages_reply_body(
    namespace: u64,
    batch_stats: Option<CommittedBatchStats>,
) -> bytes::Bytes {
    let Some(stats) = batch_stats else {
        return bytes::Bytes::from_static(&[0, 0, 0, 0]);
    };
    let namespace = IggyNamespace::from_raw(namespace);
    SendMessagesResponse {
        confirmations: vec![SendMessagesConfirmationResponse {
            // Every field is narrower than a `u32` (widths compile-asserted in
            // `iggy_binary_protocol`), so each component fits by construction.
            stream_id: namespace.stream_id() as u32,
            topic_id: namespace.topic_id() as u32,
            partition_id: namespace.partition_id() as u32,
            base_offset: stats.base_offset,
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
mod tests {
    use super::*;
    use crate::iggy_index::{IGGY_INDEX_SIZE, IggyIndex, IggyIndexCache};
    use crate::iggy_index_reader::IggyIndexReader;
    use crate::poll_plan::{DiskReadOutcome, SealedSegmentHandle};
    use bytes::Bytes;
    use compio::io::AsyncWriteAtExt;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::{Command, ReplyHeader, WireConsumer, WireEncode};
    use message_bus::SendError;
    use server_common::MESSAGE_ALIGN;
    use server_common::send_messages::{
        COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    const TEST_CLUSTER: u128 = 1;

    pub(super) fn test_partition() -> IggyPartition<IggyMessageBus> {
        let namespace = IggyNamespace::new(1, 1, 0);
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            1,
            namespace.inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.init();
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
            false,
        )
    }

    /// Partition whose consensus already advanced to `(view, log_view)` with
    /// nothing marked durable, as after a view change and before the persist
    /// gate runs.
    fn partition_at_view(
        view: u32,
        log_view: u32,
    ) -> IggyPartition<IggyMessageBus, RecordingSuperblock> {
        let namespace = IggyNamespace::new(1, 1, 0);
        let mut consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            3,
            namespace.inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.set_view(view);
        consensus.set_log_view(log_view);
        consensus.init_as_backup();
        IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
            false,
        )
    }

    /// In-memory superblock double: records every payload, counts attempts,
    /// and injects write failures.
    #[derive(Default)]
    struct RecordingSuperblock {
        writes: RefCell<Vec<Vec<u8>>>,
        attempts: Cell<u32>,
        fail_writes: Cell<bool>,
    }

    impl journal::superblock::SuperblockStore for RecordingSuperblock {
        async fn write(&self, payload: &[u8]) -> std::io::Result<()> {
            self.attempts.set(self.attempts.get() + 1);
            if self.fail_writes.get() {
                return Err(std::io::Error::other("injected superblock write failure"));
            }
            self.writes.borrow_mut().push(payload.to_vec());
            Ok(())
        }

        async fn read_latest(&self) -> std::io::Result<journal::superblock::SuperblockContents> {
            Ok(self
                .writes
                .borrow()
                .last()
                .map_or(journal::superblock::SuperblockContents::Empty, |bytes| {
                    journal::superblock::SuperblockContents::Present(bytes.clone())
                }))
        }
    }

    #[compio::test]
    async fn given_storeless_partition_when_persist_gate_runs_should_mark_current_view_durable() {
        let partition = partition_at_view(2, 1);
        assert!(partition.consensus().needs_superblock_persist());

        assert!(partition.persist_superblock_if_needed().await);

        assert!(
            !partition.consensus().needs_superblock_persist(),
            "a storeless partition must record durable = current, or the dispatch \
             tripwire would fire on its first view-scoped send"
        );
    }

    #[compio::test]
    async fn given_advanced_view_when_persist_gate_runs_should_write_vsr_state_once() {
        let mut partition = partition_at_view(3, 2);
        let store = Rc::new(RecordingSuperblock::default());
        partition.set_superblock(store.clone(), None);

        assert!(partition.persist_superblock_if_needed().await);

        let state = consensus::VsrState::try_from(store.writes.borrow()[0].as_slice())
            .expect("recorded payload decodes as a VsrState");
        assert_eq!(state.cluster, TEST_CLUSTER);
        assert_eq!(state.view, 3);
        assert_eq!(state.log_view, 2);
        assert_eq!(
            (state.checkpoint_op, state.checkpoint_checksum),
            (0, 0),
            "no partition checkpoint exists yet, so the pairing fields stay zero"
        );
        assert!(!partition.consensus().needs_superblock_persist());

        assert!(partition.persist_superblock_if_needed().await);
        assert_eq!(
            store.attempts.get(),
            1,
            "an unchanged view must take the lock-free fast path, not rewrite"
        );
    }

    /// The `offset_frontier` of the most recent recorded write.
    fn last_recorded_frontier(store: &RecordingSuperblock) -> u64 {
        let writes = store.writes.borrow();
        let bytes = writes.last().expect("a superblock write landed");
        consensus::VsrState::try_from(bytes.as_slice())
            .expect("recorded payload decodes as a VsrState")
            .offset_frontier
    }

    /// The fence path persists the frontier while the live counter still sits
    /// at its pre-install value, so an advance that maxes against the counter
    /// alone erases the record and then quarantines the segments that were its
    /// only other witness. Boot re-mints from 0 against a group at N after that.
    #[compio::test]
    async fn given_record_above_live_counter_when_advancing_should_keep_the_record() {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        partition.set_superblock(store.clone(), None);

        assert!(partition.persist_offset_frontier_at(9_000).await);
        assert_eq!(last_recorded_frontier(&store), 9_000);
        assert_eq!(
            partition.offset_frontier(),
            0,
            "a partition that never minted reports a zero frontier, which is the \
             value the fence would otherwise persist"
        );

        assert!(partition.persist_offset_frontier().await);

        assert_eq!(
            last_recorded_frontier(&store),
            9_000,
            "the advance direction must not lower the durable frontier"
        );
    }

    /// Attaching a store seeds the last-written frontier from the record
    /// itself, so an advance maxes against what boot read off disk even before
    /// this replica has written anything. The sibling test reaches that state by
    /// WRITING first, which cannot catch an attach site that skips the seed.
    #[compio::test]
    async fn given_attached_record_when_advancing_should_keep_the_recorded_frontier() {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        let recovered = consensus::VsrState {
            cluster: TEST_CLUSTER,
            replica_id: 0,
            replica_count: 3,
            view: 1,
            log_view: 1,
            commit_max: 0,
            checkpoint_op: 0,
            checkpoint_checksum: 0,
            offset_frontier: 4_200,
        };
        partition.set_superblock(store.clone(), Some(&recovered));
        assert_eq!(partition.offset_frontier(), 0, "nothing minted locally");

        assert!(partition.persist_offset_frontier().await);

        assert_eq!(
            last_recorded_frontier(&store),
            4_200,
            "the first write after an attach must not lower the record it was attached to"
        );
    }

    /// The reset direction is the only way down, and it must actually go there:
    /// an install under an advancing purge generation records a frontier below
    /// the live counter on purpose.
    #[compio::test]
    async fn given_reset_below_live_counter_when_written_should_lower_the_record() {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        partition.set_superblock(store.clone(), None);
        partition.offset.store(9_000, Ordering::Release);
        partition.should_increment_offset = true;

        assert!(partition.persist_offset_frontier().await);
        assert_eq!(last_recorded_frontier(&store), 9_001);

        assert!(partition.reset_offset_frontier_at(12).await);

        assert_eq!(
            last_recorded_frontier(&store),
            12,
            "the reset must record the incoming frontier, not max back up to the \
             counter the install is about to replace"
        );
    }

    /// A purge records its reset before it unlinks anything, so a write it
    /// cannot make has to stop the purge while the data proving the old
    /// frontier is still on disk.
    #[compio::test]
    async fn given_failing_store_when_purge_records_its_reset_should_refuse_before_mutating() {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        partition.set_superblock(store.clone(), None);
        partition.offset.store(9_000, Ordering::Release);
        partition.should_increment_offset = true;

        store.fail_writes.set(true);
        assert!(
            matches!(
                partition.record_purge_frontier_reset(7).await,
                Err(PurgeError::FrontierNotRecorded)
            ),
            "the pre-mutation refusal must be distinguishable from a post-drain \
             failure: the caller retries this one and fences the other"
        );

        assert!(
            partition.purge_deferred,
            "a deferred purge must fence the ack path: the counter still names the \
             pre-purge offset space, and the view-change persist gate cannot see \
             this because a stable view attempts no write at all"
        );

        store.fail_writes.set(false);
        assert!(
            matches!(
                partition.record_purge_frontier_reset(7).await,
                Err(PurgeError::FrontierNotRecorded)
            ),
            "the failed write armed a backoff, and the retry must respect it rather \
             than re-running a full atomic_replace against a disk that just refused one"
        );
        assert_eq!(
            store.attempts.get(),
            1,
            "the backed-off retry must not reach the store at all"
        );

        // Backoff expiry, without a controllable clock in this fixture.
        partition.superblock_retry_after_micros.set(0);
        partition
            .record_purge_frontier_reset(7)
            .await
            .expect("a working store records the reset once the backoff elapses");
        assert!(
            !partition.purge_deferred,
            "recording the reset releases the fence"
        );
        assert_eq!(
            last_recorded_frontier(&store),
            0,
            "the reset is spelled out, not read off a counter still holding the \
             pre-purge frontier"
        );
    }

    #[compio::test]
    async fn given_undurable_view_when_sending_prepare_ok_should_withhold_until_persisted() {
        let bus = RecordingBus::default();
        let replica_frames = bus.sent_to_replicas.clone();
        let mut consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            3,
            IggyNamespace::new(1, 1, 0).inner(),
            bus,
            LocalPipeline::new(),
        );
        consensus.set_view(1);
        consensus.set_log_view(1);
        consensus.init_as_backup();
        let mut partition: IggyPartition<RecordingBus, RecordingSuperblock> =
            IggyPartition::with_in_memory_storage(
                Arc::new(PartitionStats::default()),
                consensus,
                IggyByteSize::from(1024 * 1024),
                false,
            );
        let store = Rc::new(RecordingSuperblock::default());
        store.fail_writes.set(true);
        partition.set_superblock(store.clone(), None);
        // The ack path drops an op past the local head, so the head must cover it.
        partition.consensus().sequencer().set_sequence(1);
        let size = std::mem::size_of::<PrepareHeader>();
        let prepare = Message::<PrepareHeader>::new(size).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.op = 1;
                // Current view: an older-view prepare is fenced as deposed-primary
                // traffic and would never reach the ack send under test.
                header.view = 1;
                header.size = u32::try_from(size).expect("prepare header size fits in u32");
            },
        );
        let header = *prepare.header();

        partition.send_prepare_ok(&header).await;

        assert!(
            replica_frames.borrow().is_empty(),
            "an ack must not leave while the advanced view is not durable"
        );
        assert_eq!(store.attempts.get(), 1);

        // Outwait the write-failure backoff (base 10 ms doubled once by the
        // first failure), then retry with the store healthy: the ack must
        // persist first and then go out.
        store.fail_writes.set(false);
        compio::time::sleep(std::time::Duration::from_millis(50)).await;
        partition.send_prepare_ok(&header).await;

        assert_eq!(
            replica_frames.borrow().len(),
            1,
            "the retried ack must go out once the view persisted"
        );
        assert!(!partition.consensus().needs_superblock_persist());
    }

    #[compio::test]
    async fn given_failing_superblock_when_persist_gate_runs_should_withhold_and_back_off() {
        let mut partition = partition_at_view(1, 1);
        let store = Rc::new(RecordingSuperblock::default());
        store.fail_writes.set(true);
        partition.set_superblock(store.clone(), None);

        assert!(
            !partition.persist_superblock_if_needed().await,
            "a failed write must withhold the send"
        );
        assert_eq!(store.attempts.get(), 1);

        assert!(
            !partition.persist_superblock_if_needed().await,
            "the backoff window must withhold without retrying the write"
        );
        assert_eq!(
            store.attempts.get(),
            1,
            "a call inside the backoff window must not touch the store"
        );
        assert!(
            partition.consensus().needs_superblock_persist(),
            "the view stays undurable until a write lands"
        );
    }

    /// Client-facing bus that records every `send_to_client` frame so tests
    /// can assert on reply bytes without a connection registry (whose slot
    /// guard would borrow the partition across `on_request(&mut self)`).
    #[derive(Debug, Default)]
    struct RecordingBus {
        sent_to_clients: Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>,
        sent_to_replicas: Rc<RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>>,
    }

    impl MessageBus for RecordingBus {
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

        async fn send_to_client(
            &self,
            client_id: u128,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.sent_to_clients.borrow_mut().push((client_id, data));
            Ok(())
        }

        async fn send_to_replica(
            &self,
            replica: u8,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.sent_to_replicas.borrow_mut().push((replica, data));
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
    }

    type SentFrames = Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>;

    fn recording_partition() -> (IggyPartition<RecordingBus>, SentFrames) {
        recording_partition_at(0, 1)
    }

    fn recording_partition_at(
        replica: u8,
        replica_count: u8,
    ) -> (IggyPartition<RecordingBus>, SentFrames) {
        let namespace = IggyNamespace::new(1, 1, 0);
        let bus = RecordingBus::default();
        let sent_to_clients = bus.sent_to_clients.clone();
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            replica,
            replica_count,
            namespace.inner(),
            bus,
            LocalPipeline::new(),
        );
        consensus.init();
        let partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            IggyByteSize::from(1024 * 1024),
            false,
        );
        (partition, sent_to_clients)
    }

    fn delete_offset_request(
        client_id: u128,
        request_id: u64,
        consumer_id: u32,
    ) -> Message<RoutedRequestHeader> {
        let body = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(consumer_id)),
            stream_id: WireIdentifier::Numeric(1),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(0),
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&body);
        message.transmute_header(|_, header: &mut RoutedRequestHeader| {
            header.command = Command::Request;
            header.operation = Operation::DeleteConsumerOffset;
            header.client = client_id;
            header.session = 1;
            header.request = request_id;
            header.group = IggyNamespace::new(1, 1, 0).inner();
            header.size = u32::try_from(total).expect("request size fits u32");
        })
    }

    /// Deleting a consumer offset that was never stored must answer with a
    /// typed deny reply (empty body, `status` = `ConsumerOffsetNotFound`,
    /// `op` 0) before consensus: nothing may enter the pipeline, and an
    /// awaited client write must fail fast instead of waiting out its reply
    /// timeout. Once the offset exists, the same request must pass the gate
    /// into the pipeline without a deny.
    #[compio::test]
    async fn on_request_delete_of_missing_offset_replies_typed_deny() {
        let (mut partition, sent_to_clients) = recording_partition();
        let client_id: u128 = 42;
        let consumer_id: u32 = 5;

        partition
            .on_request(delete_offset_request(client_id, 7, consumer_id), None)
            .await;

        {
            let sent = sent_to_clients.borrow();
            assert_eq!(sent.len(), 1, "exactly one deny reply");
            let (reply_client, frame) = &sent[0];
            assert_eq!(*reply_client, client_id);
            let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
            )
            .expect("deny frame starts with a valid reply header");
            assert_eq!(header.command, Command::Reply);
            assert_eq!(
                header.status,
                IggyError::ConsumerOffsetNotFound(0).as_code()
            );
            assert_eq!(header.op, 0, "a deny commits nothing");
            assert_eq!(header.request, 7);
            assert_eq!(
                header.size as usize,
                std::mem::size_of::<ReplyHeader>(),
                "deny reply body must be empty"
            );
        }
        assert_eq!(
            partition.consensus().pipeline_len(),
            0,
            "denied delete must not replicate"
        );
        assert!(partition.pending_consumer_offset_commits.is_empty());

        // Existing offset: the gate passes and the delete enters the pipeline.
        partition.consumer_offsets.pin().insert(
            consumer_id as usize,
            ConsumerOffset::new(ConsumerKind::Consumer, consumer_id, 3, String::new()),
        );
        partition
            .on_request(delete_offset_request(client_id, 8, consumer_id), None)
            .await;
        assert_eq!(
            partition.consensus().pipeline_len(),
            1,
            "existing offset delete must replicate"
        );
    }

    #[compio::test]
    async fn on_request_delete_on_stale_backup_replies_transient_before_not_found() {
        let (mut partition, sent_to_clients) = recording_partition_at(1, 3);
        let client_id = 42;

        partition
            .on_request(delete_offset_request(client_id, 7, 5), None)
            .await;

        let sent = sent_to_clients.borrow();
        assert_eq!(sent.len(), 1, "exactly one routing denial");
        let (reply_client, frame) = &sent[0];
        assert_eq!(*reply_client, client_id);
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("deny frame starts with a valid reply header");
        assert_eq!(header.status, IggyError::TransientNotAccepted.as_code());
        assert_eq!(header.op, 0, "the backup admitted nothing");
        assert_eq!(
            partition.consensus().pipeline_len(),
            0,
            "a backup must not replicate the delete"
        );
    }

    fn unique_temp_offset_dir() -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "iggy-offset-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        dir.to_string_lossy().into_owned()
    }

    /// A server auto-commit persists monotonically. Disk-tier polls replicate
    /// their offsets in IO-completion order, so the last committed op can carry
    /// a lower offset than an earlier one; the file must keep the max or a
    /// restart reloads the rewound value and re-delivers. An explicit client
    /// store still overwrites, so a deliberate offset reset holds.
    #[compio::test]
    async fn auto_commit_offset_persists_monotonically_explicit_store_rewinds() {
        let mut partition = test_partition();
        let dir = unique_temp_offset_dir();
        partition.consumer_offsets_path = Some(dir.clone());
        let consumer_id: u32 = 5;
        let path = format!("{dir}/{consumer_id}");
        let read_disk = |p: &str| -> u64 {
            let bytes = std::fs::read(p).expect("offset file exists");
            match crate::offset_storage::decode_offset_record(&bytes) {
                crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
                other => panic!("offset file must hold a readable value, got {other:?}"),
            }
        };

        // Reordered auto-commits: the later op (109) trails the earlier (114).
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                consumer_id,
                114,
            ))
            .await
            .expect("auto-commit persist 114");
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                consumer_id,
                109,
            ))
            .await
            .expect("auto-commit persist 109");
        assert_eq!(
            read_disk(&path),
            114,
            "auto-commit must not rewind the file on IO-completion reorder"
        );

        assert!(
            partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 114),
            "committed high-water covers the persisted offset"
        );
        assert!(
            !partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 115),
            "an advancing offset is not covered and must submit"
        );

        // An explicit client store may deliberately rewind.
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
                ConsumerKind::Consumer,
                consumer_id,
                109,
            ))
            .await
            .expect("explicit store persist 109");
        assert_eq!(read_disk(&path), 109, "explicit store may rewind the file");
        assert!(
            !partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 114),
            "explicit rewind lowers the high-water so a later auto-commit may re-advance"
        );

        // The accepted edge: an auto-commit racing the explicit rewind
        // re-advances the file past it.
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                consumer_id,
                114,
            ))
            .await
            .expect("auto-commit persist 114 after rewind");
        assert_eq!(
            read_disk(&path),
            114,
            "auto-commit re-advances past a rewind"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The persisted-offset tracker is cold after a restart; the first
    /// auto-commit folds against the file once (so a pre-existing higher value
    /// wins, exactly like the old per-commit read-modify-write) and warms the
    /// tracker with the on-disk value, not the op's. A delete drops both the
    /// file and the tracker entry so a later auto-commit starts a fresh fold.
    #[compio::test]
    async fn auto_commit_cold_key_folds_against_file_once() {
        let mut partition = test_partition();
        let dir = unique_temp_offset_dir();
        partition.consumer_offsets_path = Some(dir.clone());
        let consumer_id: u32 = 5;
        let path = format!("{dir}/{consumer_id}");
        let read_disk = |p: &str| -> u64 {
            let bytes = std::fs::read(p).expect("offset file exists");
            match crate::offset_storage::decode_offset_record(&bytes) {
                crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
                other => panic!("offset file must hold a readable value, got {other:?}"),
            }
        };

        // Simulate the previous process run: the file already holds 114.
        persist_offset(&path, 114, false)
            .await
            .expect("seed offset file");
        assert!(
            !partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 1),
            "a cold key is never covered; the first submit must go through"
        );

        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                consumer_id,
                109,
            ))
            .await
            .expect("auto-commit persist 109 on cold key");
        assert_eq!(
            read_disk(&path),
            114,
            "cold-key fold must not rewind the pre-existing on-disk value"
        );
        assert!(
            partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 114),
            "tracker warms with the on-disk value, not the trailing op offset"
        );

        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::delete(
                ConsumerKind::Consumer,
                consumer_id,
            ))
            .await
            .expect("delete persisted offset");
        assert!(!std::path::Path::new(&path).exists(), "file unlinked");
        assert!(
            !partition.is_auto_commit_offset_covered(ConsumerKind::Consumer, consumer_id, 1),
            "delete drops the tracker entry with the file"
        );

        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
                ConsumerKind::Consumer,
                consumer_id,
                7,
            ))
            .await
            .expect("auto-commit persist 7 after delete");
        assert_eq!(read_disk(&path), 7, "post-delete auto-commit starts fresh");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `reclaim_dead_group_offsets` must drop exactly the not-`is_live` groups
    /// from the in-memory map and hand back their owned persisted-file paths,
    /// leaving live groups untouched. The returned `Vec<String>` is what the
    /// reconciler unlinks off-borrow, so it carries no partition reference.
    ///
    /// Scope: the synchronous removal contract the off-borrow split relies on.
    /// The cross-task interleave it enables -- a pump mutating the partitions vec
    /// while a sibling task is parked mid-await -- is covered on the simulator's
    /// deterministic executor, against the debug borrow tripwire, by
    /// `simulator::tests::shell_detects_partition_borrow_held_across_await`
    /// (`swap_remove`) and
    /// `shell_detects_partition_borrow_held_across_a_pump_realloc` (a growing
    /// `push`, which relocates every element).
    #[compio::test]
    async fn reclaim_dead_group_offsets_drops_dead_keeps_live() {
        let mut partition = test_partition();
        let group_offsets_path = "/iggy-test-cg-offsets".to_owned();
        partition.consumer_group_offsets_path = Some(group_offsets_path.clone());

        let dead: u32 = 1;
        let live: u32 = 2;
        partition.consumer_group_offsets.pin().insert(
            ConsumerGroupId(dead as usize),
            ConsumerOffset::new(ConsumerKind::ConsumerGroup, dead, 7, String::new()),
        );
        partition.consumer_group_offsets.pin().insert(
            ConsumerGroupId(live as usize),
            ConsumerOffset::new(ConsumerKind::ConsumerGroup, live, 9, String::new()),
        );

        let paths = partition.reclaim_dead_group_offsets(|group_id| group_id == u64::from(live));

        assert_eq!(
            paths,
            vec![format!("{group_offsets_path}/{dead}")],
            "only the dead group's persisted path is returned for unlink"
        );
        let mut remaining = partition.consumer_group_offset_ids();
        remaining.sort_unstable();
        assert_eq!(
            remaining,
            vec![u64::from(live)],
            "dead group removed in-memory; live group retained"
        );
    }

    /// One-message segment record in on-disk layout `[256B command header][blob]`
    /// stamped at `base_offset`, with a valid batch checksum so it decodes
    /// through `decode_batch_slice` and matches an `Offset` poll.
    pub(super) fn build_segment_record(namespace: IggyNamespace, base_offset: u64) -> Vec<u8> {
        let mut batch = IggyMessages::with_capacity(1);
        batch.push(IggyMessage {
            header: IggyMessageHeader {
                payload_length: 8,
                ..Default::default()
            },
            payload: Bytes::from_static(b"abcdefgh"),
            user_headers: None,
        });
        let mut owned =
            SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");
        owned.header.base_offset = base_offset;
        owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);

        let mut record = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
        owned.header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
        record[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
        record
    }

    /// Fail-closed disk read: an unreadable EARLIER segment must stop the walk
    /// (return `Faulted`) rather than skip forward and serve a LATER segment's
    /// messages, which would punch a silent gap into the poll. The second
    /// segment holds a real, matchable batch at a higher offset; before the
    /// fix, a missing first segment did `continue` and the walk served that
    /// batch (offset 5 in response to an offset-0 poll) - the exact skip.
    #[compio::test]
    async fn read_disk_faults_closed_when_earlier_segment_unreadable() {
        let namespace = IggyNamespace::new(1, 1, 0);

        // Unique temp dir; the first segment file is deliberately never created.
        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-faulted-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        // Second segment starts at offset 5 and holds a valid batch there.
        let later_record = build_segment_record(namespace, 5);
        let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
        let later_len = later_record.len() as u64;
        {
            let mut file = compio::fs::File::create(&later_path)
                .await
                .expect("create later segment file");
            let (written, _) = file.write_all_at(later_record, 0).await.into();
            written.expect("write later segment record");
            file.sync_all().await.expect("flush later segment file");
        }

        // First segment claims persisted bytes but its file is absent, so the
        // open exhausts retries -> the walk must fault-close before segment two.
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir),
            validate_checksum: true,
            segments: vec![
                DiskSegment {
                    start_offset: 0,
                    persisted: 512,
                    read_state: SealedSegmentHandle::default(),
                    sealed: false,
                },
                DiskSegment {
                    start_offset: 5,
                    persisted: later_len,
                    read_state: SealedSegmentHandle::default(),
                    sealed: false,
                },
            ],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };

        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: u64::MAX,
            })
            .await;

        assert!(
            matches!(outcome, DiskReadOutcome::Faulted),
            "unreadable first segment must fault-close, not skip forward to the later segment",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fail-closed disk read on a CORRUPT (present-but-undecodable) batch in an
    /// EARLIER segment: like a missing/unreadable segment, the walk must stop
    /// (`Faulted`) rather than skip past the garbage and serve a LATER
    /// segment's valid batch at a higher offset, which would punch a silent gap
    /// into the poll. The first segment's file exists and claims persisted bytes
    /// but holds non-decodable data; the second segment holds a real batch at
    /// offset 5.
    #[compio::test]
    async fn read_disk_faults_closed_when_earlier_segment_corrupt() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-corrupt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        // First segment (start_offset 0): garbage bytes that never decode into a
        // complete batch.
        let corrupt_record = vec![0xABu8; 512];
        let corrupt_len = corrupt_record.len() as u64;
        let corrupt_path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&corrupt_path)
                .await
                .expect("create corrupt segment file");
            let (written, _) = file.write_all_at(corrupt_record, 0).await.into();
            written.expect("write corrupt segment record");
            file.sync_all().await.expect("flush corrupt segment file");
        }

        // Second segment (start_offset 5): a valid, matchable batch.
        let later_record = build_segment_record(namespace, 5);
        let later_path = format!("{partition_dir}/{:0>20}.log", 5u64);
        let later_len = later_record.len() as u64;
        {
            let mut file = compio::fs::File::create(&later_path)
                .await
                .expect("create later segment file");
            let (written, _) = file.write_all_at(later_record, 0).await.into();
            written.expect("write later segment record");
            file.sync_all().await.expect("flush later segment file");
        }

        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir),
            validate_checksum: true,
            segments: vec![
                DiskSegment {
                    start_offset: 0,
                    persisted: corrupt_len,
                    read_state: SealedSegmentHandle::default(),
                    sealed: false,
                },
                DiskSegment {
                    start_offset: 5,
                    persisted: later_len,
                    read_state: SealedSegmentHandle::default(),
                    sealed: false,
                },
            ],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };

        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: u64::MAX,
            })
            .await;

        assert!(
            matches!(outcome, DiskReadOutcome::Faulted),
            "corrupt earlier segment must fault-close, not skip forward to the later segment",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A segment whose bytes decode cleanly but do not match their own
    /// `batch_checksum`: bit rot at rest, not a torn write. Unverified, the batch is
    /// served and a consumer reads data provably not what was written.
    ///
    /// Detection only, per the operator knob: the poll fails closed and reports, with
    /// no attempt to repair.
    #[compio::test]
    async fn read_disk_faults_closed_on_batch_checksum_mismatch() {
        let namespace = IggyNamespace::new(1, 1, 0);
        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-bitrot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        // Structurally valid with one payload byte flipped, so every length and
        // offset still decodes and only the checksum disagrees.
        let mut record = build_segment_record(namespace, 0);
        let last = record.len() - 1;
        record[last] ^= 0x01;
        let record_len = record.len() as u64;
        let path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&path)
                .await
                .expect("create segment file");
            let (written, _) = file.write_all_at(record, 0).await.into();
            written.expect("write segment record");
            file.sync_all().await.expect("flush segment file");
        }

        let plan = |validate_checksum| DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let query = MessageLookup::Offset {
            offset: 0,
            count: 10,
            ceiling: u64::MAX,
        };

        let outcome = plan(true).read_disk(query).await;
        assert!(
            matches!(outcome, DiskReadOutcome::Faulted),
            "a batch that fails its own checksum must fault-close"
        );

        // What the opt-out costs. The shipped default is `true` because of it.
        let outcome = plan(false).read_disk(query).await;
        assert!(
            matches!(outcome, DiskReadOutcome::Matched { .. }),
            "verification off is an explicit opt-out: the corrupt batch is served"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A simulated (file-less) partition has no segment files by design, so a
    /// disk poll with no dir must stay `Empty`: the caller then serves the
    /// resident journal tier, the sim's only tier.
    #[compio::test]
    async fn read_disk_serves_journal_when_partition_has_no_files() {
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::NoFiles,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: 512,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            }],
            start_position: 0,
            namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
            validate_checksum: true,
        };

        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: u64::MAX,
            })
            .await;

        assert!(
            matches!(outcome, DiskReadOutcome::Empty),
            "file-less (simulated) storage must serve the journal tier, not fault",
        );
    }

    /// A live partition whose dir is transiently unresolvable (mid-rotation)
    /// may hold disk-resident data the walk cannot reach; the poll must
    /// fault-close instead of letting the journal-forward skip those offsets.
    #[compio::test]
    async fn read_disk_faults_closed_when_partition_dir_unresolvable() {
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Unresolvable,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: 512,
                read_state: SealedSegmentHandle::default(),
                sealed: false,
            }],
            start_position: 0,
            namespace_raw: IggyNamespace::new(1, 1, 0).inner(),
            validate_checksum: true,
        };

        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 10,
                ceiling: u64::MAX,
            })
            .await;

        assert!(
            matches!(outcome, DiskReadOutcome::Faulted),
            "unresolvable dir over file-backed data must fault-close, not serve the journal",
        );
    }

    /// A sealed-segment poll opens the file once and caches the read fd; a later
    /// poll of the same segment reuses the cached descriptor. Proven by
    /// unlinking the file after the first read: a fresh open-by-path would now
    /// fail, so a successful second read can only come from the cached fd (which
    /// reads the still-open, unlinked inode).
    #[compio::test]
    async fn read_disk_caches_and_reuses_sealed_segment_fd() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-fdcache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        let record = build_segment_record(namespace, 0);
        let record_len = record.len() as u64;
        let path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&path)
                .await
                .expect("create segment file");
            let (written, _) = file.write_all_at(record, 0).await.into();
            written.expect("write segment record");
            file.sync_all().await.expect("flush segment file");
        }

        let handle = SealedSegmentHandle::default();
        // The pump touches the poll's start segment before cloning its handle
        // into the plan, so a cache-eligible handle is always tracked.
        handle.tracked.set(true);
        assert!(handle.fd.borrow().is_none(), "fd cache slot starts empty");

        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let first = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(first, DiskReadOutcome::Matched { .. }),
            "first sealed poll must match the batch",
        );
        assert!(
            handle.fd.borrow().is_some(),
            "first sealed poll must populate the read-fd cache slot",
        );

        // Unlink the file: a fresh open-by-path would fail now, so the second
        // read succeeding proves the cached fd was reused.
        std::fs::remove_file(&path).expect("unlink segment file");

        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let second = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(second, DiskReadOutcome::Matched { .. }),
            "cached fd must serve the read after the segment path is unlinked",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An untracked handle (a sealed segment the walk crosses without being the
    /// poll's start segment, or a slot evicted mid-poll) opens its file
    /// transiently: the read succeeds but no fd is retained, so the sealed LRU
    /// cap stays a true bound on resident descriptors.
    #[compio::test]
    async fn read_disk_does_not_retain_fd_for_untracked_handle() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-untracked-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        let record = build_segment_record(namespace, 0);
        let record_len = record.len() as u64;
        let path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&path)
                .await
                .expect("create segment file");
            let (written, _) = file.write_all_at(record, 0).await.into();
            written.expect("write segment record");
            file.sync_all().await.expect("flush segment file");
        }

        let handle = SealedSegmentHandle::default();
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(outcome, DiskReadOutcome::Matched { .. }),
            "the transient open must still serve the read",
        );
        assert!(
            handle.fd.borrow().is_none(),
            "an untracked handle must not retain the fd",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A sealed-segment poll reloads the dropped sparse index from the `.index`
    /// file and resolves the start byte from it, skipping the full-segment scan.
    /// Proven by prefixing the `.log` with bytes a scan from position 0 would
    /// fault on: only an index that jumps straight to the batch reads it.
    #[compio::test]
    async fn read_disk_reloads_sealed_index_to_skip_scan() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-idxreload-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        // `.log`: an undecodable prefix (a scan from byte 0 faults on it) then a
        // valid batch at offset 5. `.index`: one sparse entry mapping offset 5
        // to the batch's byte position, so the poll jumps past the prefix.
        let prefix = vec![0xABu8; 512];
        let prefix_len = prefix.len() as u64;
        let batch = build_segment_record(namespace, 5);
        let mut log_bytes = prefix;
        log_bytes.extend_from_slice(&batch);
        let log_len = log_bytes.len() as u64;
        let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&log_path)
                .await
                .expect("create segment log");
            let (written, _) = file.write_all_at(log_bytes, 0).await.into();
            written.expect("write segment log");
            file.sync_all().await.expect("flush segment log");
        }

        let index_bytes = crate::iggy_index::IggyIndexCache::serialize(
            &crate::iggy_index::IggyIndex::new(5, 0, prefix_len),
        );
        let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
        {
            let mut file = compio::fs::File::create(&index_path)
                .await
                .expect("create segment index");
            let (written, _) = file.write_all_at(index_bytes, 0).await.into();
            written.expect("write segment index");
            file.sync_all().await.expect("flush segment index");
        }

        let handle = SealedSegmentHandle::default();
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: log_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            // Byte 0, exactly what disk_poll_start returns for a sealed segment
            // whose resident index was dropped.
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: 5,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(outcome, DiskReadOutcome::Matched { .. }),
            "the reloaded sparse index must skip the prefix; a scan from byte 0 would fault",
        );
        assert!(
            handle.index.borrow().is_some(),
            "the sealed poll must cache the reloaded sparse index",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A sparse index past `SEALED_INDEX_RESIDENT_MAX_BYTES` (a dense flush
    /// cadence can make it track every message, hundreds of MB per segment) is
    /// binary-searched on file instead of materialized: the poll still resolves
    /// the exact start byte (proven by the poison prefix a byte-0 scan would
    /// fault on) while the handle's index slot stays empty.
    #[compio::test]
    async fn read_disk_resolves_oversized_index_without_materializing() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-read-disk-bigidx-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        let index_size = crate::iggy_index::IGGY_INDEX_SIZE as u64;
        let entry_count = crate::poll_plan::SEALED_INDEX_RESIDENT_MAX_BYTES / index_size + 1;
        let target_offset = entry_count - 1;

        let prefix = vec![0xABu8; 512];
        let prefix_len = prefix.len() as u64;
        let batch = build_segment_record(namespace, target_offset);
        let mut log_bytes = prefix;
        log_bytes.extend_from_slice(&batch);
        let log_len = log_bytes.len() as u64;
        let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
        {
            let mut file = compio::fs::File::create(&log_path)
                .await
                .expect("create segment log");
            let (written, _) = file.write_all_at(log_bytes, 0).await.into();
            written.expect("write segment log");
            file.sync_all().await.expect("flush segment log");
        }

        // Every entry below the target points at byte 0 (the poison prefix),
        // so only an exact lower-bound hit on the last entry reads the batch.
        let mut index_bytes =
            Vec::with_capacity(usize::try_from(entry_count * index_size).expect("fits in usize"));
        for entry in 0..entry_count {
            index_bytes.extend_from_slice(&entry.to_le_bytes());
            index_bytes.extend_from_slice(&entry.to_le_bytes());
            let position = if entry == target_offset {
                prefix_len
            } else {
                0
            };
            index_bytes.extend_from_slice(&position.to_le_bytes());
        }
        let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
        {
            let mut file = compio::fs::File::create(&index_path)
                .await
                .expect("create segment index");
            let (written, _) = file.write_all_at(index_bytes, 0).await.into();
            written.expect("write segment index");
            file.sync_all().await.expect("flush segment index");
        }

        let handle = SealedSegmentHandle::default();
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: log_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let outcome = plan
            .read_disk(MessageLookup::Offset {
                offset: target_offset,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(outcome, DiskReadOutcome::Matched { .. }),
            "the on-file lower bound must resolve past the poison prefix",
        );
        assert!(
            handle.index.borrow().is_none(),
            "an index past the resident cap must never be materialized",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Purge unlinks every segment and recreates the same paths, so a poll
    /// suspended across the purge must not keep serving the old inodes through
    /// its cached read state. The wipe reaches the in-flight clone through the
    /// shared handle: the resumed walk re-opens by path and fails closed on
    /// the recreated empty segment instead of serving purged messages.
    #[compio::test]
    async fn purge_invalidates_sealed_read_state_held_by_in_flight_poll() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let dir = std::env::temp_dir().join(format!(
            "iggy-purge-readstate-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        let partition_dir = dir.to_string_lossy().into_owned();

        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());

        // Seal the boot segment and back it with real files so the purge
        // unlinks and recreates them at the same paths.
        let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
        let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
        partition.log.segments_mut()[0].sealed = true;
        partition.log.storages_mut()[0] = SegmentStorage::new(&log_path, &index_path, 0, 0, false)
            .await
            .expect("create segment storage");

        let record = build_segment_record(namespace, 0);
        let record_len = record.len() as u64;
        {
            let mut file = compio::fs::File::create(&log_path)
                .await
                .expect("open segment log");
            let (written, _) = file.write_all_at(record, 0).await.into();
            written.expect("write segment record");
            file.sync_all().await.expect("flush segment log");
        }

        partition.log.touch_sealed_read_state(0);
        let handle = Rc::clone(&partition.log.sealed_read_state()[0]);
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let before_purge = plan
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            matches!(before_purge, DiskReadOutcome::Matched { .. }),
            "the sealed poll must match before the purge",
        );
        assert!(
            handle.fd.borrow().is_some(),
            "the sealed poll must populate the fd cache slot",
        );

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        assert!(
            handle.fd.borrow().is_none(),
            "purge must clear the cached fd inside the shared state",
        );
        assert!(
            handle.index.borrow().is_none(),
            "purge must clear the cached index inside the shared state",
        );
        assert!(
            !handle.tracked.get(),
            "purge must untrack the handle so a resumed walk cannot re-cache",
        );

        // A walk resumed after the purge resolves through the same handle: it
        // must re-open by path and hit the recreated empty segment, never the
        // unlinked pre-purge inode.
        let resumed = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
            validate_checksum: true,
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Rc::clone(&handle),
                sealed: true,
            }],
            start_position: 0,
            namespace_raw: namespace.inner(),
        };
        let after_purge = resumed
            .read_disk(MessageLookup::Offset {
                offset: 0,
                count: 1,
                ceiling: u64::MAX,
            })
            .await;
        assert!(
            !matches!(after_purge, DiskReadOutcome::Matched { .. }),
            "a resumed walk must not serve purged messages through a stale fd",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    pub(super) fn repair_config() -> PartitionsConfig {
        PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
            enforce_fsync: false,
            validate_checksum: true,
            segment_size: IggyByteSize::from(1024 * 1024),
            preallocate_segments: false,
            encryptor: None,
            path_layout: crate::PartitionPathLayout::default(),
        }
    }

    fn armed_session(to_op: u64, floor: u64, first_batch_offset: Option<u64>) -> RepairSession {
        armed_fetch_session(to_op, to_op, floor, first_batch_offset)
    }

    fn armed_fetch_session(
        to_op: u64,
        fetch_to_op: u64,
        floor: u64,
        first_batch_offset: Option<u64>,
    ) -> RepairSession {
        RepairSession {
            nonce: 1,
            view: 0,
            commit_to_op: to_op,
            fetch_to_op,
            floor: Some(floor),
            peer: 0,
            first_batch_offset,
            idle_ticks: 0,
        }
    }

    async fn journal_prepare(
        partition: &IggyPartition<IggyMessageBus>,
        op: u64,
        operation: Operation,
    ) {
        let size = std::mem::size_of::<PrepareHeader>();
        let prepare = Message::<PrepareHeader>::new(size).transmute_header(
            |_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.op = op;
                header.operation = operation;
                header.size = u32::try_from(size).expect("prepare header size fits in u32");
            },
        );
        partition
            .log
            .journal()
            .inner
            .append(prepare.into_frozen())
            .await
            .expect("journal append");
    }

    /// A repaired `SendMessages` prepare with an explicit chain identity, as a
    /// serving peer ships it.
    fn repaired_send_prepare(op: u64, parent: u128, checksum: u128) -> Message<PrepareHeader> {
        let namespace = IggyNamespace::new(1, 1, 0);
        let record = build_segment_record(namespace, op);
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + record.len();
        let mut message = Message::<PrepareHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&record);
        message.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::SendMessages;
            header.op = op;
            header.parent = parent;
            header.checksum = checksum;
            header.group = namespace.inner();
            header.size = u32::try_from(total).expect("prepare size fits u32");
        })
    }

    #[compio::test]
    async fn given_lower_backfill_when_applying_repaired_prepare_should_not_rewind_parent() {
        // The call-site regression for the WAL frontier rewind: a repaired op
        // below the DVC-adopted head must leave BOTH halves of the frontier
        // alone. The old code left the sequencer at the head and rewound
        // `last_prepare_checksum` to the backfilled entry, so the next prepare
        // parented past a committed op and recovery refused the WAL.
        const CHECKSUM_1: u128 = 0x11;
        const CHECKSUM_2: u128 = 0x22;
        const CHECKSUM_3: u128 = 0x33;
        let mut partition = test_partition();
        partition.repair = Some(armed_session(3, 0, None));

        partition
            .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
            .await;
        partition
            .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
            .await;
        // The hole at op 2 stalls the frontier advance.
        assert_eq!(partition.consensus().sequencer().current_sequence(), 1);

        // The state a DoViewChange merge leaves: head 3, parented on its
        // checksum, with the hole at op 2 still unfilled locally.
        partition.consensus().sequencer().set_sequence(3);
        partition.consensus().set_last_prepare_checksum(CHECKSUM_3);

        partition
            .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
            .await;

        assert!(
            partition.log.journal().inner.header_by_op(2).is_some(),
            "the backfill must be journaled"
        );
        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        assert_eq!(
            partition.consensus().last_prepare_checksum(),
            CHECKSUM_3,
            "a lower backfill must not rewind the parent of the next prepare"
        );
    }

    #[compio::test]
    async fn given_gap_closure_when_applying_repaired_prepare_should_adopt_new_head_checksum() {
        // The frame that closes a gap is not the new head: the frontier walks
        // to the highest contiguous op and the checksum must come from THAT
        // journal entry, not from the repair frame that happened to arrive
        // last.
        const CHECKSUM_1: u128 = 0x11;
        const CHECKSUM_2: u128 = 0x22;
        const CHECKSUM_3: u128 = 0x33;
        let mut partition = test_partition();
        partition.repair = Some(armed_session(3, 0, None));

        partition
            .apply_repaired_prepare(repaired_send_prepare(1, 0, CHECKSUM_1))
            .await;
        partition
            .apply_repaired_prepare(repaired_send_prepare(3, CHECKSUM_2, CHECKSUM_3))
            .await;
        assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
        assert_eq!(partition.consensus().last_prepare_checksum(), CHECKSUM_1);

        partition
            .apply_repaired_prepare(repaired_send_prepare(2, CHECKSUM_1, CHECKSUM_2))
            .await;

        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        assert_eq!(
            partition.consensus().last_prepare_checksum(),
            CHECKSUM_3,
            "a gap closure must adopt the checksum of the new contiguous head"
        );
    }

    #[compio::test]
    async fn given_prior_view_repair_when_a_new_view_started_should_discard_it() {
        let mut partition = test_partition();
        partition.repair = Some(armed_fetch_session(0, 1, 0, None));
        partition.consensus.set_view(1);

        partition
            .apply_repaired_prepare(repaired_send_prepare(1, 0, 0x11))
            .await;

        assert!(
            partition.repair.is_none(),
            "the prior-view session is obsolete"
        );
        assert!(
            partition.log.journal().inner.header_by_op(1).is_none(),
            "a delayed prior-view body must not enter the new view's journal"
        );
    }

    #[compio::test]
    async fn given_session_remint_when_attempts_burned_should_survive_on_partition() {
        let mut partition = test_partition();
        for round in 0..consensus::STATE_TRANSFER_MAX_STALL_RETRIES {
            assert!(!partition.burn_transfer_attempt());
            // A re-minted session must not reset the budget: it lives on the
            // partition precisely because arming sites mint fresh sessions.
            partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
                nonce: u128::from(round),
                peer: 0,
                commit_op: 0,
                artifacts: Vec::new(),
                target_accepted: false,
                idle_ticks: 0,
            });
        }
        assert!(partition.burn_transfer_attempt(), "budget exhausts");
        partition.note_transfer_progress();
        assert!(!partition.burn_transfer_attempt(), "progress resets it");
    }

    #[compio::test]
    async fn given_repeated_failures_when_only_generation_advances_should_keep_counting() {
        let mut partition = test_partition();
        // A committing primary advances its generation every round; the
        // consecutive count must keep growing regardless, or a
        // deterministic local failure retries at network round-trip rate
        // forever. Only a completed install resets it.
        assert_eq!(partition.record_transfer_failure(), 1);
        assert_eq!(partition.record_transfer_failure(), 2);
        partition.note_transfer_progress();
        assert_eq!(
            partition.record_transfer_failure(),
            3,
            "received chunks are not install progress"
        );
        partition.note_transfer_installed();
        assert_eq!(partition.record_transfer_failure(), 1, "install resets");
    }

    #[compio::test]
    async fn given_no_repaired_batch_when_window_never_arrived_should_refuse_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        partition.repair = Some(armed_session(8, 5, None));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::InProgress,
            "an incomplete window is not a definitive refusal"
        );
        assert_eq!(partition.consensus().commit_min(), 0);
        assert!(
            partition.repair.is_some(),
            "session must stay armed for retry"
        );
    }

    #[compio::test]
    async fn given_no_repaired_batch_when_window_offsets_only_should_accept_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        // Any non-SendMessages operation exercises the offsets-only arm; the
        // commit walk no-ops operations it does not recognize, so the test
        // needs no on-disk offset directories.
        for op in 6..=8 {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
        partition.repair = Some(armed_session(8, 5, None));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(conclusion, RepairConclusion::Done);
        assert!(partition.consensus().commit_min() >= 5);
    }

    #[compio::test]
    async fn given_no_repaired_batch_when_window_holds_message_op_should_refuse_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        journal_prepare(&partition, 6, Operation::SendMessages).await;
        for op in 7..=8 {
            journal_prepare(&partition, op, Operation::CreateStream).await;
        }
        partition.repair = Some(armed_session(8, 5, None));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 5, to_op: 8 },
            "a complete window with an unanchored message op can never connect"
        );
        assert_eq!(partition.consensus().commit_min(), 0);
        assert!(
            partition.repair.is_none(),
            "a definitive refusal hands recovery to state transfer"
        );
    }

    #[compio::test]
    async fn given_no_repaired_batch_when_window_fully_evicted_should_refuse_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        partition.repair = Some(armed_session(8, 8, None));

        let conclusion = partition.complete_repair(&repair_config()).await;

        // Everything the peer retained was evicted: a retry re-raises the
        // identical empty window every round (the wedge state transfer
        // exists to break), so this refusal is definitive.
        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 8, to_op: 8 }
        );
        assert_eq!(partition.consensus().commit_min(), 0);
        assert!(partition.repair.is_none());
    }

    #[compio::test]
    async fn given_empty_committed_window_with_a_suffix_fetch_should_escape_to_state_transfer() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(5);
        partition.repair = Some(armed_fetch_session(5, 9, 5, None));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 5, to_op: 5 },
            "the uncommitted fetch ceiling must not postpone a definitive committed-floor refusal"
        );
        assert!(partition.repair.is_none());
    }

    /// A one-message `SendMessages` prepare for `op`, journaled through the
    /// replicated-apply path (which stamps offsets and re-checksums), with the
    /// sequencer advanced the way `on_replicate` does after a real append.
    pub(super) async fn journal_send_batch(partition: &mut IggyPartition<IggyMessageBus>, op: u64) {
        let namespace = IggyNamespace::new(1, 1, 0);
        let record = build_segment_record(namespace, 0);
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + record.len();
        let mut message = Message::<PrepareHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&record);
        let message = message.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::SendMessages;
            header.op = op;
            header.timestamp = op;
            header.group = namespace.inner();
            header.size = u32::try_from(total).expect("prepare size fits u32");
        });
        partition
            .apply_replicated_operation(message)
            .await
            .expect("journal send batch");
        partition.consensus().sequencer().set_sequence(op);
    }

    #[compio::test]
    async fn given_committed_suffix_evicted_when_completing_repair_should_close_the_session() {
        // A successful suffix repair is what lets the group commit past
        // `commit_to_op`, and the commit walk's flush then evicts exactly the
        // suffix headers. The completion verdict must survive that eviction:
        // judged from resident headers alone, the fully successful session
        // would report itself incomplete forever, stay armed, and block every
        // later re-arm for this partition until a view change.
        let mut partition = test_partition();
        for op in 1..=3 {
            journal_send_batch(&mut partition, op).await;
        }
        partition.consensus().advance_commit_max(3);
        partition.repair = Some(armed_fetch_session(2, 3, 0, Some(0)));
        partition.commit_journal(&repair_config()).await;
        let _ = partition.log.journal().inner.evict_prefix(3).await;

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(conclusion, RepairConclusion::Done);
        assert!(
            partition.repair.is_none(),
            "a fully committed suffix fetch must not stay armed after its \
             headers are flushed out of the resident journal"
        );
    }

    #[compio::test]
    async fn given_suffix_fetch_when_its_view_is_discarded_should_clear_the_session() {
        let mut partition = test_partition();
        partition.repair = Some(armed_fetch_session(0, 3, 0, None));
        partition.consensus.set_view(1);

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(conclusion, RepairConclusion::Done);
        assert!(
            partition.repair.is_none(),
            "a discarded view must not leave its suffix fetch blocking future repair"
        );
    }

    #[compio::test]
    async fn given_repaired_batch_above_durable_end_when_floor_arrives_should_refuse_commit_floor()
    {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        // No recovered segments (durable end None) and the served window's
        // first batch starts at offset 3: ops below the floor are neither
        // locally durable nor repaired.
        partition.repair = Some(armed_session(8, 5, Some(3)));

        let conclusion = partition.complete_repair(&repair_config()).await;

        assert_eq!(
            conclusion,
            RepairConclusion::InProgress,
            "with the window incomplete, later frames can still lower the \
             first batch offset into connection"
        );
        assert_eq!(partition.consensus().commit_min(), 0);
        assert!(partition.repair.is_some());
    }
    /// Temp partition directory for the state-transfer fence specs below.
    async fn transfer_fence_dir(label: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "iggy-transfer-fence-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir)
            .await
            .expect("create temp partition dir");
        dir.to_string_lossy().into_owned()
    }

    fn armed_transfer(peer: u8) -> crate::state_transfer::PartitionTransferSession {
        crate::state_transfer::PartitionTransferSession {
            nonce: 7,
            peer,
            commit_op: 12,
            artifacts: Vec::new(),
            target_accepted: true,
            idle_ticks: 0,
        }
    }

    /// A purge must not leave a transfer running: its staged segments hold
    /// PRE-purge data, and completing the install renames it back in durably
    /// (the install takes `max(offer generation, applied)`, and this purge
    /// already stamped the newer one, so the reconciler's purge gate never
    /// re-fires).
    #[compio::test]
    async fn given_armed_transfer_when_purged_should_abandon_session_and_rearm() {
        let partition_dir = transfer_fence_dir("purge-abandons").await;
        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());
        partition.transfer = Some(armed_transfer(1));
        partition.transfer_rearm = Some(crate::state_transfer::PendingTransferRearm {
            peer: 2,
            after_ticks: 5,
        });
        partition.consensus().begin_state_transfer_await();

        partition
            .purge(&repair_config(), 3)
            .await
            .expect("purge partition");

        assert!(
            partition.transfer.is_none(),
            "purge must drop the in-flight transfer session"
        );
        assert!(
            partition.transfer_rearm.is_none(),
            "purge must cancel the scheduled re-arm"
        );
        assert_eq!(
            partition.consensus().state_transfer_stage(),
            consensus::StateTransferStage::Idle,
            "purge must release the transfer stage so a later trigger can arm"
        );

        let _ = std::fs::remove_dir_all(&partition_dir);
    }

    /// An offer whose frontier sits below this replica's own offset counter is
    /// refused: installing it would rewind the counter, and the next replicated
    /// prepare is re-stamped from it, so this replica would persist different
    /// bytes (and a different `batch_checksum`) than the rest of the group.
    #[compio::test]
    async fn given_offer_below_local_counter_when_installed_should_refuse_rewind() {
        let partition_dir = transfer_fence_dir("rewind-refused").await;
        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());
        partition.should_increment_offset = true;
        partition.offset.store(99, Ordering::Release);

        let behind = crate::state_transfer::ConsumerOffsetsWire {
            purge_generation: 0,
            next_offset: 50,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: Vec::new(),
        };
        let refused = partition
            .install_state_transfer(&repair_config(), 12, Vec::new(), &behind.encode(), 0)
            .await;
        assert!(
            matches!(
                refused,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                        offer_next_offset: 50,
                        local_next_offset: 100,
                    }
                )
            ),
            "expected a rewind refusal, got {refused:?}"
        );

        // A purge at the origin is the one legitimate rewind, and the artifact
        // carries the generation that proves it: the same offer passes the fence
        // once its generation advances past the COMMITTED one the caller reads
        // off the metadata plane (0 here), not past this replica's applied
        // value, whose `purge.gen` hydration a kill-before-record leaves stale.
        let purged = crate::state_transfer::ConsumerOffsetsWire {
            purge_generation: 1,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: Vec::new(),
        };
        let accepted = partition
            .install_state_transfer(&repair_config(), 12, Vec::new(), &purged.encode(), 0)
            .await;
        assert!(
            !matches!(
                accepted,
                Err(crate::state_transfer::PartitionInstallError::OfferRewindsDurableData { .. })
            ),
            "a purge-advancing offer must pass the rewind fence, got {accepted:?}"
        );

        let _ = std::fs::remove_dir_all(&partition_dir);
    }

    /// The canonical post-restart rejoin: this replica applied a purge before
    /// the restart but was killed before the purge's `purge.gen` record step,
    /// so the metadata plane's COMMITTED generation is 1 while its own
    /// hydrated `applied_purge_generation` is back at 0. Gated on the local
    /// field, `offered(1) > applied(0)` reads as an advancing purge and
    /// disables the rewind refusal -- on the one path it exists to guard.
    #[compio::test]
    async fn given_restarted_replica_when_offer_matches_committed_purge_should_refuse_rewind() {
        let partition_dir = transfer_fence_dir("restart-purge-rewind").await;
        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());
        partition.should_increment_offset = true;
        partition.offset.store(99, Ordering::Release);
        assert_eq!(
            partition.applied_purge_generation(),
            0,
            "with no purge.gen record the hydrated generation starts at 0"
        );

        let offer = crate::state_transfer::ConsumerOffsetsWire {
            purge_generation: 1,
            next_offset: 50,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: Vec::new(),
        };
        let refused = partition
            .install_state_transfer(&repair_config(), 12, Vec::new(), &offer.encode(), 1)
            .await;

        assert!(
            matches!(
                refused,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                        offer_next_offset: 50,
                        local_next_offset: 100,
                    }
                )
            ),
            "an offer that merely matches the committed generation is not a purge \
             advancing past it, so the rewind fence must hold: got {refused:?}"
        );

        let _ = std::fs::remove_dir_all(&partition_dir);
    }

    /// A replica that missed the purge entirely: the metadata plane has it
    /// committed, this replica never applied it, so its frontier still measures
    /// the PRE-purge offset space. The reset offer is the only thing that can
    /// converge it, and journal repair cannot bridge the floor the purge moved,
    /// so refusing it strands the replica on pre-purge data for good.
    ///
    /// Distinguished from the lagging-origin case above by `next_offset == 0`:
    /// nothing has been appended since the purge, so there is no post-purge
    /// data for the offer to rewind.
    #[compio::test]
    async fn given_replica_that_missed_the_purge_when_offered_the_reset_should_install() {
        let partition_dir = transfer_fence_dir("missed-purge-reset").await;
        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());
        partition.should_increment_offset = true;
        partition.offset.store(99, Ordering::Release);
        assert_eq!(
            partition.applied_purge_generation(),
            0,
            "a replica that missed the purge has not recorded its generation"
        );

        let reset = crate::state_transfer::ConsumerOffsetsWire {
            purge_generation: 1,
            next_offset: 0,
            consumers: Vec::new(),
            groups: Vec::new(),
            dedup: Vec::new(),
        };
        let installed = partition
            .install_state_transfer(&repair_config(), 12, Vec::new(), &reset.encode(), 1)
            .await;

        assert!(
            !matches!(
                installed,
                Err(crate::state_transfer::PartitionInstallError::OfferRewindsDurableData { .. })
            ),
            "the reset for a purge this replica never applied must pass the rewind \
             fence, got {installed:?}"
        );

        let _ = std::fs::remove_dir_all(&partition_dir);
    }

    /// Primary-by-index at view 0 with nothing committed refuses to serve: an
    /// empty group is trivially "caught up", so this gate is the only thing
    /// separating a real primary from a phantom whose directory vanished, whose
    /// zero-segment offer at frontier 0 would make a data-holding receiver
    /// unlink its chain.
    #[compio::test]
    async fn given_nothing_committed_when_offer_requested_should_refuse() {
        let partition_dir = transfer_fence_dir("nothing-committed").await;
        let mut partition = test_partition();
        partition.set_partition_dir(partition_dir.clone());
        assert_eq!(partition.consensus().commit_max(), 0);

        let refused = partition.state_transfer_offer(&repair_config()).await;
        assert!(
            matches!(
                refused,
                Err(crate::state_transfer::PartitionTransferUnavailable::NothingCommitted)
            ),
            "expected a NothingCommitted refusal, got {refused:?}"
        );
        assert!(
            refused.is_err_and(|reason| reason.transient()),
            "the refusal must be transient: the requester rotates rather than \
             charging its failure count"
        );

        let _ = std::fs::remove_dir_all(&partition_dir);
    }

    fn batch_stats(base_offset: u64, message_count: u32) -> CommittedBatchStats {
        CommittedBatchStats {
            base_offset,
            message_count,
            size_bytes: 128,
        }
    }

    #[test]
    fn given_send_messages_when_offsets_resolved_should_confirm_base_offset() {
        let namespace = IggyNamespace::new(3, 7, 5);
        let stats = batch_stats(42, 3);

        let body = send_messages_reply_body(namespace.inner(), Some(stats));
        let (response, consumed) = SendMessagesResponse::decode(&body).unwrap();

        assert_eq!(consumed, body.len());
        assert_eq!(
            response.confirmations,
            vec![SendMessagesConfirmationResponse {
                stream_id: 3,
                topic_id: 7,
                partition_id: 5,
                base_offset: 42,
            }]
        );
    }

    #[test]
    fn given_send_messages_when_offsets_unavailable_should_reply_zero_confirmations() {
        let namespace = IggyNamespace::new(1, 1, 0);

        let body = send_messages_reply_body(namespace.inner(), None);

        assert_eq!(&body[..], &[0, 0, 0, 0]);
        let (response, _) = SendMessagesResponse::decode(&body).unwrap();
        assert!(response.confirmations.is_empty());
    }

    #[test]
    fn given_batch_stats_when_end_offset_derived_should_span_the_message_run() {
        assert_eq!(batch_stats(9, 1).end_offset(), 9);
        assert_eq!(batch_stats(9, 4).end_offset(), 12);
    }

    #[test]
    fn given_result_framed_operation_when_committed_should_reply_empty_result_section() {
        assert_eq!(
            &committed_reply_body(Operation::StoreConsumerOffset)[..],
            &[0, 0, 0, 0]
        );
    }

    #[test]
    fn given_unframed_operation_when_committed_should_reply_empty_body() {
        assert!(committed_reply_body(Operation::DeleteSegments).is_empty());
    }

    /// Every write to this device fails with `ENOSPC`, which is how the
    /// persist failure cases below inject a fault into one half of the flush
    /// without any production-side plumbing.
    #[cfg(target_os = "linux")]
    const DEV_FULL: &str = "/dev/full";

    const FIRST_PAYLOAD: &[u8] = b"first-chunk";
    const SECOND_PAYLOAD: &[u8] = b"second-chunk-is-longer";

    /// Partition whose active segment carries real writers over the given
    /// paths, both with fsync on. Point either path at [`DEV_FULL`] to make
    /// that half's save fail.
    struct PersistFixture {
        partition: IggyPartition<IggyMessageBus>,
        log_cursor: Rc<AtomicU64>,
        index_cursor: Rc<AtomicU64>,
    }

    impl PersistFixture {
        async fn new(log_path: &str, index_path: &str) -> Self {
            let log_cursor = Rc::new(AtomicU64::new(0));
            let index_cursor = Rc::new(AtomicU64::new(0));
            let messages_writer =
                MessagesWriter::new(log_path, log_cursor.clone(), true, false, None)
                    .await
                    .expect("open segment log writer");
            let index_writer = IggyIndexWriter::new(index_path, index_cursor.clone(), true, false)
                .await
                .expect("open segment index writer");

            let mut partition = test_partition();
            partition.log.add_persisted_segment(
                Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
                SegmentStorage::default(),
                Some(Rc::new(messages_writer)),
                Some(Rc::new(index_writer)),
            );

            Self {
                partition,
                log_cursor,
                index_cursor,
            }
        }

        /// Mirrors one `commit_messages` chunk: the sparse entry addresses the
        /// byte the batch is about to land on, taken from the segment size the
        /// way production takes it, so a second persist can only index
        /// correctly if the first one advanced the segment in step with the
        /// writer's cursor.
        async fn persist(&mut self, payload: &[u8], offset: u64) -> Result<(), IggyError> {
            let index = IggyIndex::new(offset, offset + 1, self.segment_size());
            self.partition
                .persist_frozen_batches_to_disk(
                    vec![prepare_framed(payload)],
                    IggyIndexCache::serialize(&index),
                    1,
                )
                .await
        }

        fn cursors(&self) -> (u64, u64) {
            (
                self.log_cursor.load(Ordering::Relaxed),
                self.index_cursor.load(Ordering::Relaxed),
            )
        }

        fn segment_size(&self) -> u64 {
            self.partition.log.active_segment().size.as_bytes_u64()
        }
    }

    /// Journaled entry in the shape the persist path expects: a `PrepareHeader`
    /// prefix it strips, followed by the bytes that reach the segment file.
    fn prepare_framed(payload: &[u8]) -> Frozen<4096> {
        let mut bytes = vec![0u8; size_of::<PrepareHeader>()];
        bytes.extend_from_slice(payload);
        Owned::<4096>::copy_from_slice(&bytes).into()
    }

    fn file_len(path: &std::path::Path) -> u64 {
        std::fs::metadata(path).expect("stat file").len()
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_index_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
     {
        let dir = tempfile::tempdir().expect("temp dir");
        let log_path = dir.path().join("segment.log");
        let mut fixture =
            PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;

        let result = fixture.persist(FIRST_PAYLOAD, 0).await;

        assert!(
            matches!(result, Err(IggyError::CannotSaveIndexToSegment)),
            "index save over {DEV_FULL} must fail, got {result:?}"
        );
        assert_eq!(
            file_len(&log_path),
            FIRST_PAYLOAD.len() as u64,
            "the segment bytes landed; only the cursor is withheld"
        );
        assert_eq!(
            fixture.cursors(),
            (0, 0),
            "neither cursor may advance when the index half failed"
        );
        assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_log_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
     {
        let dir = tempfile::tempdir().expect("temp dir");
        let index_path = dir.path().join("segment.index");
        let mut fixture =
            PersistFixture::new(DEV_FULL, index_path.to_str().expect("utf-8 path")).await;

        let result = fixture.persist(FIRST_PAYLOAD, 0).await;

        assert!(
            matches!(result, Err(IggyError::CannotWriteToFile)),
            "segment save over {DEV_FULL} must fail, got {result:?}"
        );
        assert_eq!(
            file_len(&index_path),
            IGGY_INDEX_SIZE as u64,
            "the index entry landed; only the cursor is withheld"
        );
        assert_eq!(
            fixture.cursors(),
            (0, 0),
            "neither cursor may advance when the segment half failed"
        );
        assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_both_saves_failing_when_persisting_should_leave_both_cursors_untouched() {
        let mut fixture = PersistFixture::new(DEV_FULL, DEV_FULL).await;

        let result = fixture.persist(FIRST_PAYLOAD, 0).await;

        assert!(
            matches!(result, Err(IggyError::CannotWriteToFile)),
            "a failed segment save must win over the index error, got {result:?}"
        );
        assert_eq!(fixture.cursors(), (0, 0), "no cursor may advance");
        assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
    }

    /// The two saves run concurrently, so each must read its own cursor
    /// without observing the other's advance: the second chunk lands exactly
    /// where the first one ended, and its index entry says so.
    #[compio::test]
    async fn given_two_successful_persists_when_reading_back_should_place_second_chunk_at_first_chunk_end()
     {
        let dir = tempfile::tempdir().expect("temp dir");
        let log_path = dir.path().join("segment.log");
        let index_path = dir.path().join("segment.index");
        let mut fixture = PersistFixture::new(
            log_path.to_str().expect("utf-8 path"),
            index_path.to_str().expect("utf-8 path"),
        )
        .await;

        fixture
            .persist(FIRST_PAYLOAD, 0)
            .await
            .expect("first persist");
        fixture
            .persist(SECOND_PAYLOAD, 7)
            .await
            .expect("second persist");

        let log_bytes = (FIRST_PAYLOAD.len() + SECOND_PAYLOAD.len()) as u64;
        let index_bytes = 2 * IGGY_INDEX_SIZE as u64;
        assert_eq!(
            fixture.cursors(),
            (log_bytes, index_bytes),
            "both cursors must cover both persists"
        );
        assert_eq!(file_len(&log_path), log_bytes, "segment file length");
        assert_eq!(file_len(&index_path), index_bytes, "index file length");
        assert_eq!(fixture.segment_size(), log_bytes, "segment size");

        let reader = IggyIndexReader::new(index_path.to_str().expect("utf-8 path"))
            .await
            .expect("open index reader");
        let last = reader
            .load_last()
            .await
            .expect("read last index entry")
            .expect("index entry present");
        assert_eq!(
            last,
            IggyIndex::new(7, 8, FIRST_PAYLOAD.len() as u64),
            "the second entry must address the first chunk's end"
        );
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_a_persist_failure_on_a_committed_op_should_fence_the_partition_not_panic() {
        // The op is cluster-committed and the local write cannot be made, so
        // the replica is divergent. This used to `panic!`, which the pump
        // task swallows: the shard stopped serving every partition it owned
        // while the process reported healthy. The partition must fence itself
        // instead, so the shard's tick can see the fault and stop the server.
        let dir = tempfile::tempdir().expect("temp dir");
        let log_path = dir.path().join("segment.log");
        let log_cursor = Rc::new(AtomicU64::new(0));
        let index_cursor = Rc::new(AtomicU64::new(0));
        let messages_writer = MessagesWriter::new(
            log_path.to_str().expect("utf-8 path"),
            log_cursor,
            true,
            false,
            None,
        )
        .await
        .expect("open segment log writer");
        // The index half writes to a device that is always full, so the
        // persist fails the way a full disk fails.
        let index_writer = IggyIndexWriter::new(DEV_FULL, index_cursor, true, false)
            .await
            .expect("open segment index writer");

        let mut partition = test_partition();
        partition.log.add_persisted_segment(
            Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
            SegmentStorage::default(),
            Some(Rc::new(messages_writer)),
            Some(Rc::new(index_writer)),
        );

        journal_send_batch(&mut partition, 1).await;
        partition.consensus().advance_commit_max(1);

        // Would abort the test process before the fence existed.
        partition.commit_journal(&repair_config()).await;

        let fault = partition
            .fatal()
            .expect("a failed commit of a cluster-committed op must fence the partition");
        assert_eq!(fault.op, 1);
        assert_eq!(fault.operation, Operation::SendMessages);

        // The fence holds: a fenced partition must not advance again, or the
        // pump's tail drain walks it into the `advance_commit_min` assert.
        let commit_min = partition.consensus().commit_min();
        partition.commit_journal(&repair_config()).await;
        assert_eq!(
            partition.consensus().commit_min(),
            commit_min,
            "a fenced partition must not advance on a later commit"
        );
    }

    #[compio::test]
    async fn given_a_shutdown_flush_failure_when_fencing_should_keep_an_earlier_commit_fault() {
        let mut partition = test_partition();

        // A flush failure on a healthy partition fences it, so the pump's
        // post-flush scan turns the exit non-zero instead of reporting a
        // clean shutdown over unpersisted cluster-committed data.
        partition.fence_flush_failure();
        let fault = partition
            .fatal()
            .expect("a failed shutdown flush must fence the partition");
        assert_eq!(fault.op, partition.consensus().commit_min());
        assert_eq!(fault.operation, Operation::SendMessages);

        // A partition the commit path already fenced keeps that fault: it
        // names the op that first diverged.
        let commit_fault = FatalCommit {
            namespace_raw: fault.namespace_raw,
            op: 42,
            operation: Operation::StoreConsumerOffset,
        };
        partition.fatal = Some(commit_fault);
        partition.fence_flush_failure();
        let kept = partition.fatal().expect("the fence must hold");
        assert_eq!(kept.op, 42);
        assert_eq!(kept.operation, Operation::StoreConsumerOffset);
    }

    /// A half that failed never advanced its cursor, so the retry rewrites
    /// the same positions: one copy of the batch, one index entry.
    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_failed_persist_when_retried_with_a_healthy_writer_should_overwrite_the_same_positions()
     {
        let dir = tempfile::tempdir().expect("temp dir");
        let log_path = dir.path().join("segment.log");
        let index_path = dir.path().join("segment.index");
        let mut fixture =
            PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;

        assert!(
            fixture.persist(FIRST_PAYLOAD, 0).await.is_err(),
            "index save over {DEV_FULL} must fail"
        );

        // The committed prefix stays resident, so the retry re-persists the
        // identical bytes; only the broken index writer is swapped out.
        let index_writer = IggyIndexWriter::new(
            index_path.to_str().expect("utf-8 path"),
            fixture.index_cursor.clone(),
            true,
            false,
        )
        .await
        .expect("open replacement index writer");
        let active = fixture.partition.log.index_writers().len() - 1;
        fixture.partition.log.index_writers_mut()[active] = Some(Rc::new(index_writer));

        fixture
            .persist(FIRST_PAYLOAD, 0)
            .await
            .expect("retry persist");

        assert_eq!(
            file_len(&log_path),
            FIRST_PAYLOAD.len() as u64,
            "the retry must overwrite the batch, not append a second copy"
        );
        assert_eq!(
            file_len(&index_path),
            IGGY_INDEX_SIZE as u64,
            "the retry must write exactly one index entry"
        );
        assert_eq!(
            fixture.cursors(),
            (FIRST_PAYLOAD.len() as u64, IGGY_INDEX_SIZE as u64),
            "both cursors must advance once the retry succeeded"
        );
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use iggy_common::IggyDuration;
    use std::time::Duration;

    fn segment(end_offset: u64, max_timestamp: u64, size: u64, sealed: bool) -> Segment {
        let mut segment = Segment::new(0, IggyByteSize::from(0u64));
        segment.end_offset = end_offset;
        segment.max_timestamp = max_timestamp;
        segment.size = IggyByteSize::from(size);
        segment.sealed = sealed;
        segment
    }

    fn one_second() -> IggyExpiry {
        IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)))
    }

    /// Shape of one sealed segment in the [`partition_with_sealed_run`] fixture.
    const MESSAGES_PER_SEGMENT: u64 = 10;
    const BYTES_PER_SEGMENT: u64 = 100;

    /// A topic's configured segment size, and the largest batch that can be
    /// appended to it.
    const SEGMENT_SIZE: u64 = 1_000;
    const MAX_BATCH_SIZE: u64 = 100;
    /// The size a sealed segment actually reaches. The batch that crosses
    /// `SEGMENT_SIZE` is appended whole, so a sealed segment closes somewhere
    /// in `[SEGMENT_SIZE, SEGMENT_SIZE + MAX_BATCH_SIZE)`. Retention asserted
    /// at exactly `SEGMENT_SIZE` would miss that entirely.
    const SEALED_SIZE: u64 = SEGMENT_SIZE + 40;
    /// What the cleaner enforces for a cap of one segment: the per-partition
    /// share, floored at the largest a sealed segment can be.
    const FLOORED_BUDGET: u64 = SEGMENT_SIZE + MAX_BATCH_SIZE;

    fn sealed_run_segment(start_offset: u64) -> Segment {
        let mut segment = segment(
            start_offset + MESSAGES_PER_SEGMENT - 1,
            1,
            BYTES_PER_SEGMENT,
            true,
        );
        segment.start_offset = start_offset;
        segment
    }

    /// Partition whose log is `sealed_count` sealed segments followed by the
    /// active one, with stats seeded to match. Every storage is the in-memory
    /// default (no reader, so no path), which is what lets retirement run its
    /// full body here without touching the filesystem.
    fn partition_with_sealed_run(sealed_count: u64) -> IggyPartition<IggyMessageBus> {
        let mut partition = super::tests::test_partition();
        // The fixture ships one unsealed segment: rewrite it as the head of the
        // run and append a fresh active segment last, where the removal walk
        // always stops.
        partition.log.segments_mut()[0] = sealed_run_segment(0);
        for index in 1..sealed_count {
            partition.log.add_persisted_segment(
                sealed_run_segment(index * MESSAGES_PER_SEGMENT),
                SegmentStorage::default(),
                None,
                None,
            );
        }
        partition.log.add_persisted_segment(
            Segment::new(
                sealed_count * MESSAGES_PER_SEGMENT,
                IggyByteSize::from(0u64),
            ),
            SegmentStorage::default(),
            None,
            None,
        );
        let stats = &partition.stats;
        stats.increment_segments_count(u32::try_from(sealed_count).expect("run fits a u32"));
        stats.increment_messages_count(sealed_count * MESSAGES_PER_SEGMENT);
        stats.increment_size_bytes(sealed_count * BYTES_PER_SEGMENT);
        partition
    }

    #[test]
    fn leading_expired_end_skips_active_and_returns_last_expired() {
        let segments = vec![
            segment(9, 1, 100, true),
            segment(19, 2, 100, true),
            segment(29, 3, 100, true),
            segment(39, 0, 100, false), // active: never considered
        ];
        assert_eq!(
            leading_expired_end(&segments, IggyTimestamp::now(), one_second()),
            Some(29)
        );
    }

    #[test]
    fn leading_expired_end_stops_at_first_unexpired() {
        let now = IggyTimestamp::now();
        let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_hours(1)));
        let segments = vec![
            segment(9, 1, 100, true),                // expired
            segment(19, now.as_micros(), 100, true), // recent: not expired, stops run
            segment(29, 1, 100, true),
            segment(39, 0, 100, false),
        ];
        assert_eq!(leading_expired_end(&segments, now, expiry), Some(9));
    }

    #[test]
    fn leading_expired_end_none_for_never_expire() {
        let segments = vec![segment(9, 1, 100, true), segment(19, 0, 100, false)];
        assert_eq!(
            leading_expired_end(&segments, IggyTimestamp::now(), IggyExpiry::NeverExpire),
            None
        );
    }

    #[test]
    fn leading_expired_end_none_for_lone_active_segment() {
        let segments = vec![segment(9, 1, 100, false)];
        assert_eq!(
            leading_expired_end(&segments, IggyTimestamp::now(), one_second()),
            None
        );
    }

    #[test]
    fn leading_oversized_end_trims_oldest_until_under_budget() {
        // 3 x 100 = 300 SEALED resident, the active segment's bytes excluded.
        // Budget 250: drop seg0 (200 <= 250, stop). up_to = seg0.end_offset.
        let segments = vec![
            segment(9, 1, 100, true),
            segment(19, 2, 100, true),
            segment(29, 3, 100, true),
            segment(39, 0, 100, false),
        ];
        assert_eq!(leading_oversized_end(&segments, 250), Some(9));
    }

    #[test]
    fn leading_oversized_end_none_when_under_budget() {
        let segments = vec![segment(9, 1, 100, true), segment(19, 0, 100, false)];
        assert_eq!(leading_oversized_end(&segments, 10_000), None);
    }

    #[test]
    fn leading_oversized_end_never_drops_active_segment() {
        let segments = vec![segment(9, 1, 1_000, false)];
        assert_eq!(leading_oversized_end(&segments, 10), None);
    }

    #[test]
    fn leading_oversized_end_retains_an_overshot_sealed_segment_at_the_floored_budget() {
        // The shape admission accepts at the floor: max_topic_size == one
        // segment. The sealed segment overshot, and the active one holds far
        // more than the budget. Counting the active segment, or dividing the
        // cap without a floor, deletes the only history this partition has.
        let segments = vec![
            segment(9, 1, SEALED_SIZE, true),
            segment(19, 0, SEGMENT_SIZE * 5, false),
        ];
        assert_eq!(leading_oversized_end(&segments, FLOORED_BUDGET), None);
        // The same segments against the UNFLOORED share, which is what a cap of
        // one segment divides into. It is under what a sealed segment reaches,
        // so the history goes -- which is why the budget carries a floor.
        assert_eq!(leading_oversized_end(&segments, SEGMENT_SIZE), Some(9));
    }

    #[test]
    fn leading_oversized_end_still_drops_the_oldest_once_two_sealed_segments_exceed_the_budget() {
        // Same budget, one sealed segment more: the cap is real, not disabled.
        let segments = vec![
            segment(9, 1, SEALED_SIZE, true),
            segment(19, 2, SEALED_SIZE, true),
            segment(29, 0, SEGMENT_SIZE * 5, false),
        ];
        assert_eq!(
            leading_oversized_end(&segments, FLOORED_BUDGET),
            Some(9),
            "the oldest sealed segment goes, the newest one stays"
        );
    }

    #[test]
    fn nth_oldest_sealed_end_resolves_count_to_offset() {
        let segments = vec![
            segment(9, 1, 100, true),
            segment(19, 2, 100, true),
            segment(29, 3, 100, true),
            segment(39, 0, 100, false), // active: excluded
        ];
        assert_eq!(nth_oldest_sealed_end(&segments, 1), Some(9));
        assert_eq!(nth_oldest_sealed_end(&segments, 2), Some(19));
        // More than available sealed: clamps to the last sealed segment.
        assert_eq!(nth_oldest_sealed_end(&segments, 10), Some(29));
        assert_eq!(nth_oldest_sealed_end(&segments, 0), None);
    }

    #[test]
    fn nth_oldest_sealed_end_stops_at_first_unsealed() {
        let segments = vec![
            segment(9, 1, 100, true),
            segment(19, 2, 100, false), // unsealed mid-run stops the count
            segment(29, 3, 100, true),
            segment(39, 0, 100, false),
        ];
        assert_eq!(nth_oldest_sealed_end(&segments, 5), Some(9));
    }

    #[test]
    fn nth_oldest_sealed_end_none_for_lone_active_segment() {
        let segments = vec![segment(9, 1, 100, false)];
        assert_eq!(nth_oldest_sealed_end(&segments, 1), None);
    }

    #[compio::test]
    async fn removal_spends_the_per_pass_budget_and_resumes_on_the_next_pass() {
        let budget = u64::try_from(SEGMENT_REMOVAL_BUDGET_PER_PASS).expect("budget fits a u64");
        let sealed_count = budget + 3;
        let mut partition = partition_with_sealed_run(sealed_count);
        // The whole run qualifies: every sealed segment ends at or below
        // `up_to`, and a partition nobody has committed against has no barrier.
        let up_to = sealed_count * MESSAGES_PER_SEGMENT - 1;
        let segments_len = |partition: &IggyPartition<IggyMessageBus>| {
            u64::try_from(partition.log.segments().len()).expect("log fits a u64")
        };

        let removal = partition.remove_sealed_segments_up_to(up_to).await;
        assert_eq!(removal.segments, budget, "one pass stops at the budget");
        assert_eq!(removal.messages, budget * MESSAGES_PER_SEGMENT);
        assert!(
            removal.budget_spent,
            "a pass that stopped on the budget must ask to be re-staged"
        );
        assert_eq!(segments_len(&partition), sealed_count + 1 - budget);
        assert_eq!(
            partition.log.segments()[0].start_offset,
            budget * MESSAGES_PER_SEGMENT,
            "the surviving run starts where the budget stopped"
        );

        let remainder = sealed_count - budget;
        let removal = partition.remove_sealed_segments_up_to(up_to).await;
        assert_eq!(
            removal.segments, remainder,
            "a later pass finishes the run it was handed"
        );
        assert_eq!(removal.messages, remainder * MESSAGES_PER_SEGMENT);
        assert!(
            !removal.budget_spent,
            "a pass that drained the run must not re-stage"
        );
        assert_eq!(
            segments_len(&partition),
            1,
            "only the active segment survives"
        );

        let stats = &partition.stats;
        assert_eq!(stats.segments_count_inconsistent(), 1);
        assert_eq!(stats.messages_count_inconsistent(), 0);
        assert_eq!(stats.size_bytes_inconsistent(), 0);
        assert_eq!(
            partition.remove_sealed_segments_up_to(up_to).await,
            SegmentRemoval::default(),
            "a converged partition removes nothing"
        );

        // A run that ends exactly on the budget is finished by the pass that
        // spends it; re-staging would hand the pump a no-op frame.
        let mut exact = partition_with_sealed_run(budget);
        let removal = exact
            .remove_sealed_segments_up_to(budget * MESSAGES_PER_SEGMENT - 1)
            .await;
        assert_eq!(removal.segments, budget, "the whole run goes in one pass");
        assert!(
            !removal.budget_spent,
            "a run that ends on the budget has nothing left to re-stage"
        );
        assert_eq!(segments_len(&exact), 1, "only the active segment survives");
    }
}

#[cfg(test)]
mod purge_floor_tests {
    use super::tests::{build_segment_record, journal_send_batch, repair_config, test_partition};
    use super::*;
    use iggy_binary_protocol::{Command, WireConsumer, WireEncode};

    /// Fresh temp dir wired as the partition dir, so `purge()` can recreate
    /// real segment files and write `purge.gen`.
    fn purge_test_partition(tag: &str) -> (IggyPartition<IggyMessageBus>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "iggy-purge-floor-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        std::fs::create_dir_all(&dir).expect("create temp partition dir");
        let mut partition = test_partition();
        partition.set_partition_dir(dir.to_string_lossy().into_owned());
        (partition, dir)
    }

    /// A `StoreConsumerOffset` prepare for `op`, journaled and staged through
    /// the replicated-apply path.
    async fn journal_store_offset(
        partition: &mut IggyPartition<IggyMessageBus>,
        op: u64,
        consumer_id: u32,
        offset: u64,
    ) {
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(consumer_id)),
            stream_id: WireIdentifier::Numeric(1),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(0),
            offset,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + body.len();
        let mut message = Message::<PrepareHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&body);
        let message = message.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::StoreConsumerOffset;
            header.op = op;
            header.group = IggyNamespace::new(1, 1, 0).inner();
            header.size = u32::try_from(total).expect("prepare size fits u32");
        });
        partition
            .apply_replicated_operation(message)
            .await
            .expect("journal store offset");
        partition.consensus().sequencer().set_sequence(op);
    }

    #[compio::test]
    async fn given_resident_batches_when_purged_should_seal_journal_polls() {
        let (mut partition, dir) = purge_test_partition("seal");
        journal_send_batch(&mut partition, 1).await;
        journal_send_batch(&mut partition, 2).await;
        assert!(
            partition
                .log
                .journal()
                .inner
                .oldest_resident_offset()
                .is_some(),
            "resident batches must be poll-resolvable before the purge"
        );

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        assert_eq!(
            partition.log.journal().inner.oldest_resident_offset(),
            None,
            "purge must seal the resident poll tier so polls fall back to \
             the (fresh, empty) segments"
        );
        assert_eq!(
            partition.log.journal().inner.resident_count(),
            2,
            "journal entries are consensus history and must survive the purge"
        );
        assert!(
            partition.log.journal().inner.header_by_op(1).is_some()
                && partition.log.journal().inner.header_by_op(2).is_some(),
            "repair and retransmission must still resolve pre-purge ops"
        );
        assert!(
            partition.log.journal().inner.resident_entries().is_empty(),
            "the poll view of the resident tier must exclude fenced entries, \
             even though they stay resident for consensus"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn given_pre_purge_ops_committed_after_purge_should_advance_commit_min_without_stale_flush()
     {
        let (mut partition, dir) = purge_test_partition("no-stale-flush");
        journal_send_batch(&mut partition, 1).await;
        journal_send_batch(&mut partition, 2).await;

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        // Both sends commit only now, after the purge fenced them.
        partition.consensus().advance_commit_max(2);
        partition.commit_journal(&repair_config()).await;

        assert_eq!(
            partition.consensus().commit_min(),
            2,
            "pre-purge ops must still commit (no wedge), just without effect"
        );
        assert_eq!(
            partition.offset.load(Ordering::Acquire),
            0,
            "purged sends must not re-advance the reset offset"
        );
        assert_eq!(
            partition.log.active_segment().size.as_bytes_u64(),
            0,
            "purged sends must not flush bytes into the fresh segment"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn given_post_purge_appends_when_committed_should_flush_from_offset_zero() {
        let (mut partition, dir) = purge_test_partition("post-appends");
        journal_send_batch(&mut partition, 1).await;

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        // A fresh append lands after the purge; its commit walks the journal
        // front where the fenced pre-purge entry still sits.
        journal_send_batch(&mut partition, 2).await;
        partition.consensus().advance_commit_max(2);
        partition.commit_journal(&repair_config()).await;

        assert_eq!(partition.consensus().commit_min(), 2);
        assert_eq!(
            partition.offset.load(Ordering::Acquire),
            0,
            "the single post-purge message flushes at offset 0"
        );
        // Exactly ONE record's bytes: both entries stamp base_offset 0 (the
        // pre-purge append was first, the post-purge one restarts at 0), so
        // an unfenced flush of the purged batch would double the size while
        // leaving every offset assert green.
        let one_record = build_segment_record(IggyNamespace::new(1, 1, 0), 0).len() as u64;
        assert_eq!(
            partition.log.active_segment().size.as_bytes_u64(),
            one_record,
            "only the post-purge batch may reach the fresh segment"
        );
        assert_eq!(
            partition.log.active_segment().start_offset,
            0,
            "post-purge storage restarts at offset 0"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn given_pre_purge_consumer_offset_op_when_committed_after_purge_should_not_resurrect_offset()
     {
        let (mut partition, dir) = purge_test_partition("offset-resurrect");
        journal_store_offset(&mut partition, 1, 7, 42).await;

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        partition.consensus().advance_commit_max(1);
        partition.commit_journal(&repair_config()).await;

        assert_eq!(
            partition.consensus().commit_min(),
            1,
            "the fenced offset op must still commit"
        );
        assert!(
            partition.consumer_offsets.pin().is_empty(),
            "a pre-purge store committing after the purge must not resurrect \
             the cleared consumer offset"
        );
        assert!(
            partition.pending_consumer_offset_commits.is_empty(),
            "the fenced op must not linger in the staged-commit table"
        );

        // A store admitted after the purge carries a higher op -- the primary
        // assigns them monotonically at admission -- so it lands above the
        // floor and applies normally.
        journal_store_offset(&mut partition, 2, 7, 4).await;
        partition.consensus().advance_commit_max(2);
        partition.commit_journal(&repair_config()).await;

        assert_eq!(
            partition.consumer_offsets.pin().len(),
            1,
            "a store admitted after the purge must survive the floor"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn given_purged_straggler_when_evicted_prefix_reappends_it_should_stay_poll_sealed() {
        // A flush evicts the committed prefix and re-appends the retained
        // tail (`evict_prefix`); without the poll floor that re-append
        // re-indexes a fenced pre-purge straggler, and resident polls serve
        // purged bytes once it commits.
        let (mut partition, dir) = purge_test_partition("evict-reappend");
        journal_send_batch(&mut partition, 1).await;
        journal_send_batch(&mut partition, 2).await;
        partition.consensus().advance_commit_max(1);

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        // The straggler flush path: evict the committed prefix (op 1), which
        // re-appends the retained op 2 through `append_with_meta`.
        let committed = partition.log.journal().inner.committed_prefix(1);
        assert_eq!(committed.len(), 1, "only op 1 is committed");
        partition.log.journal().inner.evict_prefix(1).await;

        assert_eq!(
            partition.log.journal().inner.oldest_resident_offset(),
            None,
            "evict re-append must not undo the purge's poll seal"
        );
        assert!(
            partition.log.journal().inner.header_by_op(2).is_some(),
            "the retained op stays consensus history"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The resident poll tier is sealed by the purge, but the first post-purge
    /// indexed append re-arms it. The snapshot handed to the straddle and
    /// retention-recovery walks matches on batch CONTENTS alone (no op), so
    /// without the fence those walks serve purged bytes again.
    #[compio::test]
    async fn given_post_purge_append_when_snapshotting_resident_tail_should_skip_fenced_entries() {
        let (mut partition, dir) = purge_test_partition("resident-fence");
        // Two pre-purge batches: offsets 0 and 1 (the counter advances).
        journal_send_batch(&mut partition, 1).await;
        journal_send_batch(&mut partition, 2).await;

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        // Re-arms the resident tier: this batch restarts at offset 0.
        journal_send_batch(&mut partition, 3).await;

        let snapshot = partition.resident_tail_snapshot();
        assert_eq!(
            snapshot.entries.len(),
            1,
            "only the post-purge entry may reach a poll"
        );
        // Offset 1 existed ONLY in the purged batch, so a resident poll there
        // must come up empty instead of serving the fenced entry.
        let purged_offset = crate::journal::select_resident(
            &snapshot.entries,
            MessageLookup::Offset {
                offset: 1,
                count: 10,
                ceiling: u64::MAX,
            },
        );
        assert!(
            purged_offset.is_none(),
            "a purged offset must not be servable from the resident tier"
        );
        assert!(
            crate::journal::select_resident(
                &snapshot.entries,
                MessageLookup::Offset {
                    offset: 0,
                    count: 10,
                    ceiling: u64::MAX,
                },
            )
            .is_some(),
            "the post-purge batch is still servable"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The flush that would evict the fenced prefix is gated on
    /// `journal.info.messages_count`, which the purge zeroes, so an idle purged
    /// partition would pin those entries resident forever. The purge hands them
    /// to the ordinary eviction path instead; repair still resolves them from
    /// the evicted ring.
    #[compio::test]
    async fn given_walked_prefix_when_purged_should_evict_fenced_entries_to_the_ring() {
        let (mut partition, dir) = purge_test_partition("fenced-evict");
        // Single-replica test partitions disable repair retention; the ring is
        // what makes eviction safe for repair, so exercise it.
        partition.log.journal().inner.set_repair_retention(true);
        journal_send_batch(&mut partition, 1).await;
        journal_send_batch(&mut partition, 2).await;
        partition.consensus().advance_commit_max(2);
        // Walked already (a settled repair floor does this without flushing),
        // so the entries are committed history that is still resident.
        partition.consensus().set_commit_floor(2);

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");

        assert_eq!(
            partition.log.journal().inner.resident_count(),
            0,
            "the walked fenced prefix must not stay pinned in resident storage"
        );
        assert!(
            partition.log.journal().inner.repair_entry(1).is_some()
                && partition.log.journal().inner.repair_entry(2).is_some(),
            "eviction moves them to the ring, where repair still resolves them"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `purge_floor_op` promises EVERY journal-apply path no-ops at or below the
    /// floor. The repaired-prepare path writes the dirty offset, the segment
    /// write cursor and `journal.info`, so it needs the same guard: only the
    /// peer-side serve clamp kept purged bytes out, and that clamp is the
    /// PEER's floor, not this replica's.
    #[compio::test]
    async fn given_repaired_send_at_or_below_floor_when_appended_should_not_mutate_state() {
        let (mut partition, dir) = purge_test_partition("repaired-fenced");
        journal_send_batch(&mut partition, 1).await;
        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");
        assert_eq!(partition.purge_floor_op(), 1, "the floor fences op 1");

        // A repaired pre-purge batch for the fenced op, carrying its original
        // (pre-purge) stamps exactly as the serving peer stored them.
        let namespace = IggyNamespace::new(1, 1, 0);
        let record = build_segment_record(namespace, 40);
        let header_size = std::mem::size_of::<PrepareHeader>();
        let total = header_size + record.len();
        let mut message = Message::<PrepareHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&record);
        let message = message.transmute_header(|_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::SendMessages;
            header.op = 1;
            header.group = namespace.inner();
            header.size = u32::try_from(total).expect("prepare size fits u32");
        });

        let base_offset = partition
            .append_repaired_send_messages(message)
            .await
            .expect("a fenced repaired prepare is journaled, not refused");

        assert_eq!(
            base_offset, None,
            "a purged batch must not anchor the repair floor's connect check"
        );
        assert_eq!(
            partition.dirty_offset.load(Ordering::Relaxed),
            0,
            "the reset counter must not jump to a purged offset"
        );
        let segment_index = partition.log.segments().len() - 1;
        assert_eq!(
            partition.log.segments()[segment_index].current_position,
            0,
            "no purged bytes may be reserved in the fresh segment"
        );
        assert_eq!(
            partition.log.journal().info.messages_count,
            0,
            "purged bytes must not re-enter the flush accounting"
        );
        assert!(
            partition.log.journal().inner.header_by_op(1).is_some(),
            "the entry is still journaled: dropping it would wedge commit_min"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Why the shard defers repair COMPLETION while a committed purge is
    /// unapplied: until the purge lands, `recovered_durable_offset` still names
    /// the pre-purge segments, and every repaired post-purge batch (offsets
    /// restart at 0) silently vanishes in the flush skip. The purge clears the
    /// line, and the same batch persists.
    #[compio::test]
    async fn given_stale_recovered_durable_offset_when_committing_should_drop_until_purge_applies()
    {
        let (mut partition, dir) = purge_test_partition("stale-durable");
        // A restart that recovered segments through offset 9.
        partition.recovered_durable_offset = Some(9);

        journal_send_batch(&mut partition, 1).await;
        partition.consensus().advance_commit_max(1);
        partition.commit_journal(&repair_config()).await;
        assert_eq!(
            partition.log.active_segment().size.as_bytes_u64(),
            0,
            "a batch at offset 0 is skipped as already-durable while the stale \
             recovered line stands"
        );

        partition
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");
        assert_eq!(
            partition.recovered_durable_offset, None,
            "the purge deleted those bytes, so the line must go with them"
        );

        journal_send_batch(&mut partition, 2).await;
        partition.consensus().advance_commit_max(2);
        partition.commit_journal(&repair_config()).await;
        assert_eq!(
            partition.log.active_segment().size.as_bytes_u64(),
            build_segment_record(IggyNamespace::new(1, 1, 0), 0).len() as u64,
            "after the purge the same offset-0 batch reaches the segment"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A delete whose on-disk cleanup failed leaves the partition directory
    /// (and `purge.gen`) behind. The recreated topic's rows restart their purge
    /// generations at 0, so hydrating the DEAD incarnation's generation would
    /// swallow the new topic's purges until the committed counter climbed past
    /// it.
    #[compio::test]
    async fn given_purge_gen_from_a_dead_incarnation_when_rebuilt_should_hydrate_zero() {
        let (mut partition, dir) = purge_test_partition("stale-incarnation");
        partition.set_created_revision(7);
        partition
            .purge(&repair_config(), 4)
            .await
            .expect("purge partition");
        assert_eq!(partition.applied_purge_generation(), 4);

        let rebuild = |created_revision: u64| {
            let mut rebuilt = test_partition();
            rebuilt.set_partition_dir(dir.to_string_lossy().into_owned());
            rebuilt.set_created_revision(created_revision);
            rebuilt
        };

        // Same incarnation (an ordinary restart): the generation still stands.
        let mut restarted = rebuild(7);
        restarted
            .hydrate_applied_purge_generation()
            .await
            .expect("hydrate purge generation");
        assert_eq!(restarted.applied_purge_generation(), 4);

        // New incarnation over the same directory.
        let mut recreated = rebuild(8);
        recreated
            .hydrate_applied_purge_generation()
            .await
            .expect("hydrate purge generation");
        assert_eq!(
            recreated.applied_purge_generation(),
            0,
            "a dead incarnation's record must not fence the recreated partition"
        );

        // So the new topic's first purge (generation 1) passes the reconciler's
        // `committed > applied` gate and re-keys the record.
        recreated
            .purge(&repair_config(), 1)
            .await
            .expect("purge partition");
        let mut after = rebuild(8);
        after
            .hydrate_applied_purge_generation()
            .await
            .expect("hydrate purge generation");
        assert_eq!(after.applied_purge_generation(), 1);
        let mut dead = rebuild(7);
        dead.hydrate_applied_purge_generation()
            .await
            .expect("hydrate purge generation");
        assert_eq!(
            dead.applied_purge_generation(),
            0,
            "re-keying leaves the dead incarnation with nothing to hydrate"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn purge_persists_generation_and_hydrates_it_back() {
        let (mut partition, dir) = purge_test_partition("generation");
        partition
            .purge(&repair_config(), 3)
            .await
            .expect("purge partition");
        assert_eq!(partition.applied_purge_generation(), 3);

        // A rebuilt partition over the same dir (restart) reads the durable
        // generation instead of resetting to 0 and re-wiping.
        let mut rebuilt = test_partition();
        rebuilt.set_partition_dir(dir.to_string_lossy().into_owned());
        rebuilt
            .hydrate_applied_purge_generation()
            .await
            .expect("hydrate purge generation");
        assert_eq!(
            rebuilt.applied_purge_generation(),
            3,
            "restart must hydrate the durably applied purge generation"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
