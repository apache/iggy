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
use crate::offset_storage::{delete_persisted_offset, persist_offset, persist_offset_max};
use crate::poll_plan::{
    AutoCommitCtx, AutoCommitTarget, DiskReadPlan, DiskSegment, LastPolledCtx, PollPlan, PollTier,
    ResidentTailSnapshot,
};
use crate::segment::Segment;
use crate::types::RepairSession;
use crate::{
    AppendResult, Partition, PartitionOffsets, PartitionsConfig, PollQueryResult, PollingArgs,
    PollingConsumer,
};
use consensus::{
    CommitLogEvent, Consensus, PartitionDiagEvent, Pipeline, PipelineEntry, PlaneKind, Project,
    ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus, ack_preflight,
    ack_quorum_reached, build_deny_reply_from_request, build_reply_from_request,
    build_reply_message, drain_committable_prefix, emit_namespace_progress_event,
    emit_partition_diag, emit_sim_event, fence_old_prepare_by_commit, replicate_preflight,
    replicate_to_next_in_chain, send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{
    AckLevel, GenericHeader, Operation, PrepareHeader, WireDecode, WireIdentifier,
};
use iggy_binary_protocol::{PrepareOkHeader, RequestHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyExpiry, IggyTimestamp, PartitionStats, PollingKind,
};
use journal::Journal as _;
use message_bus::{IggyMessageBus, MessageBus, is_auto_commit_client};
use server_common::{
    MESSAGE_ALIGN, Message, SegmentStorage,
    iobuf::{Frozen, Owned},
    send_messages2::{
        ChecksumMode, convert_request_message, decode_prepare_slice, decode_prepare_slice_trusted,
        stamp_prepare_for_persistence, verify_received_send_messages,
    },
    sharding::IggyNamespace,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, warn};

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
//
// Note: there is no per-client write dedup at the partition plane.
// `SendMessages` retries are at-least-once and may commit multiple times.
// Consumers handle duplicate messages via `server_common::MessageDeduplicator`
// (message-id based) if they care.
#[derive(Debug)]
pub struct IggyPartition<B = IggyMessageBus>
where
    B: MessageBus,
{
    consensus: VsrConsensus<B>,
    pub log: SegmentedLog<PartitionJournal<PartitionJournalMemStorage>, PartitionJournalMemStorage>,
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
    consumer_offsets_path: Option<String>,
    consumer_group_offsets_path: Option<String>,
    /// Canonical on-disk partition directory, set at construction by the
    /// server builder. Disk polls must not derive this from live writers:
    /// sealed segments drop their writer at rotation, so a writer-derived
    /// path transiently disappears and silently hides the disk tier.
    /// `None` only for in-memory (simulated) partitions.
    partition_dir: Option<String>,
    consumer_offset_enforce_fsync: bool,
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
    pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
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
    persisted_offsets: RefCell<HashMap<(ConsumerKind, u32), u64>>,
    observed_view: u32,
    /// Highest `PurgeTopic` generation this replica has locally applied (reset
    /// the partition to empty). The reconciler compares the committed metadata
    /// generation against this and resets only when it advances, so a redundant
    /// reconcile pass never re-wipes a partition already at this generation.
    applied_purge_generation: u64,
}

/// Post-preflight dispatch in `on_request`: replicate via VSR or take the
/// `NoAck` leader-local fast path. `RequestHeader` is boxed to avoid the
/// 277-byte inline variant tripping clippy's `large_enum_variant`.
enum Disposition {
    Replicate(Message<PrepareHeader>),
    NoAck {
        request_header: Box<RequestHeader>,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PendingConsumerOffsetCommit {
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

impl<B> IggyPartition<B>
where
    B: MessageBus,
{
    pub fn new(stats: Arc<PartitionStats>, consensus: VsrConsensus<B>) -> Self {
        let observed_view = consensus.view();
        let single_replica = consensus.replica_count() == 1;
        let partition = Self {
            consensus,
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
            repair: None,
            recovered_durable_offset: None,
            pending_consumer_offset_commits: HashMap::new(),
            persisted_offsets: RefCell::new(HashMap::new()),
            observed_view,
            applied_purge_generation: 0,
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

    #[must_use]
    pub const fn consensus(&self) -> &VsrConsensus<B> {
        &self.consensus
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
            // (v2) stores apply on the primary only and are never replicated,
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
    /// replication. Single-replica durability. No reply cache: partition
    /// plane is at-least-once; session lifecycle lives on metadata.
    #[allow(clippy::future_not_send)]
    async fn apply_consumer_offset_no_ack(
        &self,
        request_header: Box<RequestHeader>,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: Option<u64>,
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

    fn persisted_offset_path(&self, kind: ConsumerKind, consumer_id: u32) -> Option<String> {
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
        // the LRU keeps the hot set and drops the least-recently-used fd + index.
        let start_offset = self.log.segments()[start_segment].start_offset;
        self.log.touch_sealed_read_state(start_offset);
        // Snapshot only the segments the disk walk visits (`start_segment..`),
        // so `start_position` applies to the first snapshotted segment. A sealed
        // segment carries its shared read-state handle (fd + sparse index) so
        // the off-borrow read reuses (or fills) it; the active segment opens
        // fresh and resolves from its resident index.
        let segments = self.log.segments()[start_segment..]
            .iter()
            .zip(self.log.sealed_read_state()[start_segment..].iter())
            .map(|(segment, read_state)| DiskSegment {
                start_offset: segment.start_offset,
                persisted: segment.size.as_bytes_u64(),
                read_state: segment.sealed.then(|| Rc::clone(read_state)),
            })
            .collect();
        let disk = DiskReadPlan {
            partition_dir: self.partition_dir(),
            segments,
            start_position,
            namespace_raw: self.namespace().inner(),
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

impl<B> Partition for IggyPartition<B>
where
    B: MessageBus,
{
    async fn append_messages(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<AppendResult, IggyError> {
        let header = *message.header();
        if header.operation != Operation::SendMessages {
            return Err(IggyError::CannotAppendMessage);
        }

        let dirty_offset = if self.should_increment_offset {
            self.dirty_offset.load(Ordering::Relaxed) + 1
        } else {
            0
        };

        // Reuse the prepare's monotonic timestamp, assigned once by the primary
        // in `project()` (`next_monotonic_timestamp`) and replicated verbatim to
        // every backup. Sourcing it here instead of a fresh local `now()` makes
        // the persisted `base_timestamp` (and the `batch_checksum` derived from
        // it) byte-identical across replicas; a local `now()` diverges per node.
        let batch_timestamp = header.timestamp;
        let (message, batch, batch_messages_count) =
            stamp_prepare_for_persistence(message, dirty_offset, batch_timestamp)
                .map_err(|_| IggyError::CannotAppendMessage)?;

        if batch_messages_count == 0 {
            return Ok(AppendResult::new(0, 0, 0));
        }

        let batch_messages_size =
            u64::try_from(batch.total_size()).map_err(|_| IggyError::CannotAppendMessage)?;

        let last_dirty_offset = dirty_offset + u64::from(batch_messages_count) - 1;

        if !self.should_increment_offset {
            self.should_increment_offset = true;
        }
        self.dirty_offset
            .store(last_dirty_offset, Ordering::Relaxed);

        let segment_index = self.log.segments().len() - 1;
        let current_position = self.log.segments()[segment_index].current_position;
        self.log.segments_mut()[segment_index].current_position = current_position
            .checked_add(batch_messages_size)
            .ok_or(IggyError::CannotAppendMessage)?;

        let journal = self.log.journal_mut();
        journal.info.messages_count += batch_messages_count;
        journal.info.size += IggyByteSize::from(batch_messages_size);
        journal.info.current_offset = last_dirty_offset;
        if journal.info.first_timestamp == 0 {
            journal.info.first_timestamp = batch.base_timestamp;
        }
        journal.info.end_timestamp = batch.base_timestamp;
        journal.info.max_timestamp = journal.info.max_timestamp.max(batch.base_timestamp);
        journal
            .inner
            .append(message.into_frozen())
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;

        Ok(AppendResult::new(
            dirty_offset,
            last_dirty_offset,
            batch_messages_count,
        ))
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

impl<B> IggyPartition<B>
where
    B: MessageBus,
{
    #[must_use]
    fn namespace(&self) -> IggyNamespace {
        IggyNamespace::from_raw(self.consensus.namespace())
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
    /// At-least-once: no per-client dedup. `SendMessages` retry -> fresh
    /// prepare, may re-commit at new offset. Consumers handle dedup
    /// (message key / content / producer-id+seq). Session lifecycle +
    /// eviction live on metadata plane.
    ///
    /// # Panics
    /// Panics if called when this partition's consensus instance is not the
    /// primary, is not in normal status, or is currently syncing.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub async fn on_request(&mut self, message: Message<RequestHeader>) {
        self.clear_pending_consumer_offset_commits_if_view_changed();
        let namespace = IggyNamespace::from_raw(message.header().namespace);
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
                // batch (native v2, or the plane's pre-encrypt convert output)
                // returns early above, so Skip only affects the legacy
                // transcode, whose output goes straight to project/stamp.
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

            // Parse once for both the delete-existence check and AckLevel dispatch.
            let consumer_offset = match message.header().operation {
                Operation::StoreConsumerOffset
                | Operation::StoreConsumerOffset2
                | Operation::DeleteConsumerOffset
                | Operation::DeleteConsumerOffset2 => {
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

            if matches!(
                message.header().operation,
                Operation::DeleteConsumerOffset | Operation::DeleteConsumerOffset2
            ) && let Some((kind, consumer_id, _, _)) = consumer_offset
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
            if matches!(
                message.header().operation,
                Operation::StoreConsumerOffset | Operation::StoreConsumerOffset2
            ) && let Some((_, _, Some(requested_offset), _)) = consumer_offset
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
                    )
                    .await;
                    return;
                }
            }

            // A client op landing on a non-primary (or mid-view-change)
            // replica is a routing artifact -- e.g. the roster still points
            // here while this group's primaryship moved after a restart.
            // Answer the typed transient instead of asserting: the SDK
            // replays and its leader recheck re-routes, whereas a panic
            // kills the shard and a silent drop wedges the client until its
            // read timeout.
            if consensus.is_follower() || !consensus.is_normal() || consensus.is_syncing() {
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
                )
                .await;
                return;
            }

            // NoAck v2 -> fast path. Quorum + v1 -> VSR pipeline.
            if let Some((kind, consumer_id, offset, AckLevel::NoAck)) = consumer_offset
                && matches!(
                    message.header().operation,
                    Operation::StoreConsumerOffset2 | Operation::DeleteConsumerOffset2,
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
                if consensus.pipeline().borrow().is_full() {
                    let push_result = consensus
                        .pipeline()
                        .borrow_mut()
                        .push_request(consensus::RequestEntry::new(message));
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
                consensus.pipeline_message(PlaneKind::Partitions, &prepare);
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
                self.apply_consumer_offset_no_ack(request_header, kind, consumer_id, offset)
                    .await;
            }
        }
    }

    /// Promote up to `slots_freed` buffered requests into prepares post-commit.
    ///
    /// No preflight: partition plane is at-least-once with no `ClientTable`
    /// dedup. Buffered `SendMessages` retry commits at fresh offset; consumers
    /// dedup by message key / content / producer-id+seq.
    ///
    /// Per-iteration `is_primary && is_normal && !is_syncing` asserts inlined
    /// (closure form's `&consensus` borrow conflicts with `&mut self`). Guards
    /// against view-change-reset flipping status across `on_replicate` await.
    ///
    /// View-change safety: `reset_view_change_state` calls
    /// [`crate::Pipeline::clear_request_queue`]; resumed loop breaks via
    /// `else { break }`.
    ///
    /// # Panics
    /// On mid-iteration status flip. Reachable only if `clear_request_queue`
    /// is bypassed at view-change reset.
    #[allow(clippy::future_not_send)]
    pub async fn drain_request_queue_into_prepares(&mut self, slots_freed: usize) {
        for _ in 0..slots_freed {
            let req = self.consensus().pipeline().borrow_mut().pop_request();
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
                    !consensus.is_syncing(),
                    "drain_request_queue_into_prepares: must not be syncing"
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
            let clone_for_forward = message.clone();
            let consensus = self.consensus();
            if let Err(error) = replicate_to_next_in_chain(consensus, &clone_for_forward).await {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to re-forward retransmitted prepare to next in chain",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
            }
            self.send_prepare_ok(&header).await;
            return;
        }

        // Backup gap check; primary sequencer pre-advanced by
        // push_prepare_entry. See metadata::on_replicate.
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
            //   sequenced. Apply is keyed by `header.op` and the primary
            //   never advances its sequencer post-apply, so proceeding is
            //   idempotent-safe; log loudly for diagnosis.
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
        // First blob-integrity check on the replicated path. The consensus
        // layer never validates the body (PrepareHeader integrity fields are
        // inert zeros) and the batch checksum is recomputed locally at stamp,
        // so a follower must verify each message's stamp-invariant per-message
        // checksum before journaling transit bytes. Follower-only: the primary
        // (and single-node self-replicate) produced these bytes and already
        // checked the client batch at ingest, so they must not pay this pass.
        // Fail closed on mismatch - drop without journaling, forwarding, or
        // acking; the primary retransmits on prepare-timeout.
        if is_backup
            && header.operation == Operation::SendMessages
            && let Err(error) = verify_received_send_messages(message.as_slice())
        {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "rejecting replicated send_messages: per-message checksum mismatch",
                )
                .with_operation(header.operation)
                .with_op(header.op)
                .with_error(error.to_string()),
            );
            return;
        }

        // Durability-before-ack: clone for chain-replicate, forward only
        // AFTER apply_replicated_operation persists. Forward-first would
        // give downstream an op whose WAL entry we never wrote, that violates
        // tail-ahead-of-head. Clone is cheap (Arc bumps in common case).
        let clone_for_forward = message.clone();
        let replicated_result = self.apply_replicated_operation(message).await;
        if replicated_result.is_ok() {
            let consensus = self.consensus();
            // Backup only: advance sequencer + checksum after journal append.
            // Pre-advance on failing apply would leave consensus claiming op N
            // while journal has nothing; retransmit of N would silently drop
            // as is_old_prepare (header.op <= current_sequence). Primary must
            // NOT re-set here: push_prepare_entry already advanced, and a
            // sibling request pipelined during the apply await would be
            // rewound to a stale op + parent, projecting a duplicate next.
            if is_backup {
                consensus.sequencer().set_sequence(header.op);
                consensus.set_last_prepare_checksum(header.checksum);
                consensus.observe_prepare_timestamp(header.timestamp);
            }
            if let Err(error) = replicate_to_next_in_chain(consensus, &clone_for_forward).await {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        self.diag_ctx(),
                        "failed to replicate prepare to next in chain",
                    )
                    .with_operation(header.operation)
                    .with_op(header.op)
                    .with_error(error.to_string()),
                );
            }
        }

        if let Err(error) = replicated_result {
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

        {
            let consensus = self.consensus();
            emit_namespace_progress_event(
                SimEventKind::NamespaceProgressUpdated,
                &ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                header.op,
                consensus.pipeline().borrow().len(),
            );
        }

        self.send_prepare_ok(&header).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_ack(&mut self, message: Message<PrepareOkHeader>, config: &PartitionsConfig) {
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

            let pipeline = consensus.pipeline().borrow();
            if pipeline
                .entry_by_op_and_checksum(header.op, header.prepare_checksum)
                .is_none()
            {
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
                consensus.pipeline().borrow().len(),
            );
        }
    }

    #[allow(clippy::future_not_send)]
    pub async fn commit_journal(&mut self, config: &PartitionsConfig) {
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
                consensus.pipeline().borrow().len(),
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
    ) -> Result<(), IggyError> {
        let header = *message.header();
        let replica_id = self.consensus.replica();
        let namespace_raw = self.consensus.namespace();

        match header.operation {
            Operation::SendMessages => {
                self.append_send_messages_to_journal(message).await?;
                debug!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    replica = replica_id,
                    op = header.op,
                    namespace_raw,
                    operation = ?header.operation,
                    "replicated send_messages appended to partition journal"
                );
                Ok(())
            }
            Operation::StoreConsumerOffset
            | Operation::DeleteConsumerOffset
            | Operation::StoreConsumerOffset2
            | Operation::DeleteConsumerOffset2 => {
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
                self.log
                    .journal()
                    .inner
                    .append(message.clone().into_frozen())
                    .await
                    .map_err(|_| IggyError::CannotAppendMessage)?;

                match header.operation {
                    Operation::StoreConsumerOffset | Operation::StoreConsumerOffset2 => {
                        self.stage_consumer_offset_upsert(
                            header.op,
                            kind,
                            consumer_id,
                            offset.expect("store_consumer_offset must include offset"),
                            is_auto_commit_client(header.client),
                        );
                    }
                    Operation::DeleteConsumerOffset | Operation::DeleteConsumerOffset2 => {
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
                Ok(())
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
                Ok(())
            }
        }
    }

    async fn append_send_messages_to_journal(
        &mut self,
        message: Message<PrepareHeader>,
    ) -> Result<(), IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;
        self.append_messages(message).await.map(|_| ())
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
            journal_info.messages_count >= config.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_info.size.as_bytes_u64()
            >= config.size_of_messages_required_to_save.as_bytes_u64();
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
        // (commit_min wedge). Eviction is deferred until the bytes are durable:
        // on a persist failure the prefix stays resident so the next commit
        // re-reads it instead of losing a committed batch (a live-process I/O
        // fault only; the in-memory journal does not survive a crash). All
        // segment range / stats / durable-offset accounting below is computed
        // from the committed entries, not the resident-journal snapshot above.
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
        // Persist the prefix in segment-sized chunks: a segment seals exactly
        // when its committed bytes reach `max_size`, no matter how many
        // entries this flush happens to cover. A backup commits in bursts
        // behind the primary, so any grouping- or timing-sensitive roll rule
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
        // the accumulated prefix is evicted before propagating, so the retry
        // re-reads only what did not land.
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
            // committed prefix resident for retry. The persist is idempotent on
            // failure: a batch write that lands but whose index save then fails
            // rewinds the segment write cursor, so the retry overwrites those
            // bytes instead of appending a duplicate. Chunks already durable
            // are evicted before the error propagates, so the retry cannot
            // re-read them (and re-write them past a rotation).
            if let Err(error) = self
                .persist_frozen_batches_to_disk(frozen_batches, index_bytes, batch_count)
                .await
            {
                self.evict_committed_prefix(evictable).await;
                return Err(error);
            }
            // Insert the flushed sparse-index entry into the in-mem cache only now
            // that the batch + index are durable. Inserting in the build loop (before
            // persist) re-inserts a duplicate on a persist-failure retry, which
            // re-reads the same prefix. The active segment has not rotated yet, so
            // this targets the segment that received the batches.
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
        for (_, meta) in &retained {
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
        let namespace_raw = self.consensus.namespace();
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
        let committed_visible_offsets = self.resolve_committed_visible_offsets(&drained).await;
        let mut messages_committed = false;

        for entry in drained {
            let prepare_header = entry.header;
            if !self
                .commit_partition_entry(
                    prepare_header,
                    &mut messages_committed,
                    &committed_visible_offsets,
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
                // Fatal: better to suicide than serve stale or panic later.
                // Operator restarts; recovery+repair re-syncs.
                panic!(
                    "partition local commit failed at op={} ({:?}): replica is divergent from cluster commit; restart required",
                    prepare_header.op, prepare_header.operation
                );
            }

            self.consensus.advance_commit_min(prepare_header.op);

            let pipeline_depth = self.consensus.pipeline().borrow().len();
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

            // No reply cache: at-least-once means retries re-commit at new
            // offsets. Only primary delivers replies; backups just advance
            // commit. Session lifecycle is metadata-only.
            //
            // A server-generated auto-commit op (a poll's `auto_commit`,
            // replicated for failover) carries the reserved
            // `AUTO_COMMIT_CLIENT_ID`: no client ever waits on it, so skip the
            // reply. Emitting it would push an unrequested frame onto a real
            // client's lockstep reply stream if the sentinel ever routed there.
            if send_client_replies && !is_auto_commit_client(prepare_header.client) {
                let reply = build_reply_message(
                    &prepare_header,
                    &committed_reply_body(prepare_header.operation),
                );
                let reply_buffers = reply.into_generic().into_frozen();
                emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

                if let Err(error) = self
                    .consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, reply_buffers)
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

    async fn resolve_committed_visible_offsets(
        &self,
        drained: &[PipelineEntry],
    ) -> HashMap<u64, CommittedBatchStats> {
        let mut committed_visible_offsets = HashMap::new();

        for entry in drained {
            if entry.header.operation != Operation::SendMessages {
                continue;
            }

            match self.committed_batch_stats_for_prepare(&entry.header).await {
                Ok(Some(batch_stats)) => {
                    committed_visible_offsets.insert(entry.header.op, batch_stats);
                }
                Ok(None) => {}
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
                }
            }
        }

        committed_visible_offsets
    }

    async fn commit_partition_entry(
        &mut self,
        prepare_header: PrepareHeader,
        messages_committed: &mut bool,
        committed_visible_offsets: &HashMap<u64, CommittedBatchStats>,
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

                if let Some(batch_stats) = committed_visible_offsets.get(&prepare_header.op) {
                    // A repaired batch at or below the boot-time recovered
                    // durable offset was already counted (and persisted)
                    // before the restart; skip it. Live traffic always sits
                    // above the (immutable) line.
                    if self
                        .recovered_durable_offset
                        .is_none_or(|durable| batch_stats.end_offset > durable)
                    {
                        self.offset.store(batch_stats.end_offset, Ordering::Release);
                        self.stats.set_current_offset(batch_stats.end_offset);
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
            Operation::StoreConsumerOffset
            | Operation::DeleteConsumerOffset
            | Operation::StoreConsumerOffset2
            | Operation::DeleteConsumerOffset2 => {
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

    async fn committed_batch_stats_for_prepare(
        &self,
        prepare_header: &PrepareHeader,
    ) -> Result<Option<CommittedBatchStats>, IggyError> {
        let Some(entry) = self.log.journal().inner.entry(prepare_header).await else {
            return Err(IggyError::InvalidCommand);
        };
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
            end_offset: batch.header.base_offset + u64::from(message_count) - 1,
            message_count,
            size_bytes: batch.header.total_size() as u64,
        }))
    }

    fn parse_consumer_offset_request(
        operation: Operation,
        message: &Message<RequestHeader>,
    ) -> Result<(ConsumerKind, u32, Option<u64>, AckLevel), IggyError> {
        let total_size =
            usize::try_from(message.header().size).map_err(|_| IggyError::InvalidCommand)?;
        let body = message
            .as_slice()
            .get(std::mem::size_of::<RequestHeader>()..total_size)
            .ok_or(IggyError::InvalidCommand)?;
        Self::parse_consumer_offset_payload(operation, body)
    }

    /// Send `header`'s deny reply with `status` on `ReplyHeader.status` (empty
    /// body, op=0), logging a WARN under `send_fail_label` if the reply send
    /// fails. Callers deny on the primary, before the op enters the pipeline,
    /// so nothing replicates.
    async fn send_partition_deny_or_log(
        consensus: &VsrConsensus<B>,
        header: &RequestHeader,
        status: u32,
        send_fail_label: &'static str,
    ) {
        let reply = build_deny_reply_from_request(consensus, header, status);
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
            Operation::StoreConsumerOffset | Operation::StoreConsumerOffset2 => {
                let offset = offset.ok_or(IggyError::InvalidCommand)?;
                Ok(if is_auto_commit_client(header.client) {
                    PendingConsumerOffsetCommit::upsert_auto_commit(kind, consumer_id, offset)
                } else {
                    PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset)
                })
            }
            Operation::DeleteConsumerOffset | Operation::DeleteConsumerOffset2 => {
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
                (request.consumer, Some(request.offset), AckLevel::Quorum)
            }
            Operation::StoreConsumerOffset2 => {
                let request = StoreConsumerOffset2Request::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (request.consumer, Some(request.offset), request.ack)
            }
            Operation::DeleteConsumerOffset => {
                let request = DeleteConsumerOffsetRequest::decode_from(body)
                    .map_err(|_| IggyError::InvalidCommand)?;
                (request.consumer, None, AckLevel::Quorum)
            }
            Operation::DeleteConsumerOffset2 => {
                let request = DeleteConsumerOffset2Request::decode_from(body)
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
            self.log.clear_in_flight();
            return Ok(());
        }

        let messages_writer = messages_writer.expect("checked above");
        let index_writer = index_writer.expect("checked above");

        let saved = messages_writer
            .save_frozen_batches(&stripped_batches)
            .await
            .map_err(|error| {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    batch_count,
                    %error,
                    "failed to save frozen batches"
                );
                error
            })?;

        if let Err(error) = index_writer.save_indexes(index_bytes).await {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                batch_count,
                %error,
                "failed to save sparse indexes; rewinding segment write cursor"
            );
            // The batch bytes landed but the index did not, so the whole persist
            // fails and the committed prefix stays resident for retry. Rewind the
            // writer cursor by exactly what this call advanced so the retry
            // overwrites those bytes instead of appending a duplicate copy.
            messages_writer.rewind(saved.as_bytes_u64());
            return Err(error);
        }

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

        self.log.clear_in_flight();
        Ok(())
    }

    async fn rotate_segment(&mut self, config: &PartitionsConfig) -> Result<(), IggyError> {
        let namespace = self.namespace();
        let old_segment_index = self.log.segments().len() - 1;
        let active_segment = self.log.active_segment_mut();
        active_segment.sealed = true;
        let start_offset = active_segment.end_offset + 1;

        let segment = Segment::new(start_offset, config.segment_size);
        // `PartitionsConfig::get_messages_path` is a stub (`/tmp/iggy_stub`);
        // the partition's real directory is only known to the server config
        // that created the initial segment, so derive the rotated paths from
        // the active writer's location.
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

        let storage = SegmentStorage::new(
            &messages_path,
            &index_path,
            0,
            0,
            config.enforce_fsync,
            config.enforce_fsync,
            false,
        )
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
                config.enforce_fsync,
                false,
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
            IggyIndexWriter::new(&index_path, index_size_bytes, config.enforce_fsync, false)
                .await
                .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );

        let old_storage = &mut self.log.storages_mut()[old_segment_index];
        let _ = old_storage.shutdown();
        self.log.messages_writers_mut()[old_segment_index] = None;
        self.log.index_writers_mut()[old_segment_index] = None;
        // Drop the sealed segment's in-memory index cache: only the ACTIVE
        // segment's cache is ever read (the `commit_messages` flush staging),
        // so a sealed cache is dead weight -- and `ensure_indexes` preallocates
        // a 16 MiB-capacity `Vec` per segment, which under small-segment
        // workloads retains hundreds of MB across thousands of sealed segments.
        self.log.indexes_mut()[old_segment_index] = None;

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
    /// segments that have expired or that push the partition past `max_bytes`.
    /// Returns the `(segments, messages)` removed.
    pub async fn clean_expired_segments(
        &mut self,
        now: IggyTimestamp,
        message_expiry: IggyExpiry,
        max_bytes: Option<u64>,
    ) -> (u64, u64) {
        let expired = leading_expired_end(self.log.segments(), now, message_expiry);
        let oversized =
            max_bytes.and_then(|max_bytes| leading_oversized_end(self.log.segments(), max_bytes));
        let Some(up_to) = expired.into_iter().chain(oversized).max() else {
            return (0, 0);
        };
        self.remove_sealed_segments_up_to(up_to).await
    }

    /// Remove the oldest sealed segments whose `end_offset <= up_to_offset`,
    /// never the active segment and never past the consumer barrier (the
    /// minimum committed consumer/group offset). Unlinks the messages and
    /// index files and decrements partition stats. Idempotent: an offset below
    /// the oldest sealed segment removes nothing. Returns the
    /// `(segments, messages)` removed.
    ///
    /// Holds `write_lock` to serialize against the commit/rotate path, which
    /// runs on the separate consensus-tick loop.
    pub async fn remove_sealed_segments_up_to(&mut self, up_to_offset: u64) -> (u64, u64) {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let barrier = self.min_committed_offset();
        let namespace = self.namespace();
        let removable = {
            let segments = self.log.segments();
            let last_idx = segments.len().saturating_sub(1);
            let mut removable = 0usize;
            for (idx, segment) in segments.iter().enumerate() {
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

        let mut deleted_segments = 0u64;
        let mut deleted_messages = 0u64;
        for _ in 0..removable {
            // The removable run is always a prefix (oldest first), so the next
            // victim is index 0 once the previous one is gone.
            let segment = self.log.segments_mut().remove(0);
            let mut storage = self.log.storages_mut().remove(0);
            self.log.indexes_mut().remove(0);
            self.log.messages_writers_mut().remove(0);
            self.log.index_writers_mut().remove(0);
            // Drop the pump's read-state handle (fd + sparse index); an in-flight
            // poll holding a clone keeps it alive until it finishes (a cached fd
            // reads the unlinked inode).
            self.log.sealed_read_state_mut().remove(0);

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

            deleted_segments += 1;
            deleted_messages += messages_in_segment;

            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = namespace.inner(),
                start_offset = segment.start_offset,
                end_offset = segment.end_offset,
                "deleted sealed segment during cleanup"
            );
        }

        (deleted_segments, deleted_messages)
    }

    /// Build and install a fresh empty segment starting at `start_offset` with
    /// real on-disk writers. Paths are derived from the partition directory
    /// (see `rotate_segment`); falls back to the config-derived path for
    /// in-memory partitions with no directory.
    ///
    /// # Errors
    /// If the segment's log / index file cannot be created.
    async fn install_empty_segment(
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
        let segment = Segment::new(start_offset, config.segment_size);
        let storage = SegmentStorage::new(
            &messages_path,
            &index_path,
            0,
            0,
            config.enforce_fsync,
            config.enforce_fsync,
            false,
        )
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
                config.enforce_fsync,
                false,
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
            IggyIndexWriter::new(&index_path, index_size_bytes, config.enforce_fsync, false)
                .await
                .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );
        self.log
            .add_persisted_segment(segment, storage, Some(messages_writer), Some(index_writer));
        Ok(())
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
    /// If the replacement segment's log / index file cannot be created.
    pub async fn purge(
        &mut self,
        config: &PartitionsConfig,
        generation: u64,
    ) -> Result<(), IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let namespace = self.namespace();

        // Drain every segment (including the active one) and unlink its files.
        let segment_count = self.log.segments().len();
        for _ in 0..segment_count {
            self.log.segments_mut().remove(0);
            let mut storage = self.log.storages_mut().remove(0);
            self.log.indexes_mut().remove(0);
            self.log.messages_writers_mut().remove(0);
            self.log.index_writers_mut().remove(0);
            // Drop the pump's read-state handle in lockstep (see cleanup path).
            self.log.sealed_read_state_mut().remove(0);

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

        // Recreate a fresh empty segment at offset 0 with real writers.
        let start_offset = 0u64;
        self.install_empty_segment(config, start_offset).await?;

        // Reset the offset counters so new messages start at offset 0.
        self.offset.store(start_offset, Ordering::Release);
        self.dirty_offset.store(start_offset, Ordering::Relaxed);
        self.should_increment_offset = false;
        // The boot-time durable line marks recovered bytes that must not be
        // re-persisted, but the purge just deleted those bytes and offsets
        // restart at 0. Keeping it would make every post-purge batch at or
        // below the old line evict silently without ever reaching a segment.
        self.recovered_durable_offset = None;

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
        for path in consumer_paths.into_iter().chain(group_paths) {
            let _ = delete_persisted_offset(&path).await;
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

        self.applied_purge_generation = generation;
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
        let Some(session) = &self.repair else {
            return;
        };
        if header.op <= self.consensus().commit_min() || header.op > session.to_op {
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
                    if let (Some(base_offset), Some(session)) = (base_offset, self.repair.as_mut())
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
            self.apply_replicated_operation(message).await
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
        let mut frontier = self.consensus().sequencer().current_sequence();
        while self
            .log
            .journal()
            .inner
            .header_by_op(frontier + 1)
            .is_some()
        {
            frontier += 1;
        }
        let consensus = self.consensus();
        if frontier > consensus.sequencer().current_sequence() {
            consensus.sequencer().set_sequence(frontier);
        }
        consensus.set_last_prepare_checksum(header.checksum);
    }

    /// Conclude a repair stream: settle the commit floor at the serving
    /// peer's eviction point (everything below it is represented by this
    /// replica's recovered segments + offset files) and walk the repaired
    /// window through the normal commit path.
    pub async fn complete_repair(&mut self, config: &PartitionsConfig) {
        let Some(session) = self.repair else {
            return;
        };
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
            let connected = match (session.first_batch_offset, durable_end) {
                (Some(first), Some(durable)) => first <= durable.saturating_add(1),
                (Some(first), None) => first == 0,
                // No repaired batch arrived, so there is no offset anchor to
                // verify the floor's continuum claim against. `None` is only
                // safe when the served window itself proves it carried no
                // messages: every op in `(floor, to_op]` journaled and none
                // of them `SendMessages`. Anything less -- dropped frames, or
                // a fully evicted window -- is indistinguishable from a
                // message range below the floor that this replica does not
                // durably own, and accepting it would serve a holed log.
                (None, _) => self.repaired_window_is_offsets_only(floor, session.to_op),
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
                return;
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
        let done = commit_min >= session.to_op;
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
            to_op = session.to_op,
            done,
            "repair window commit walk finished"
        );
    }

    /// Whether the served repair window `(floor, to_op]` arrived complete and
    /// holds no `SendMessages` op. Only then may a commit floor be accepted
    /// without a batch anchor: the window demonstrably moved no messages, so
    /// the consumer-offset table on disk stands in below the floor. An empty
    /// window (`floor >= to_op`) carries no evidence at all and never
    /// qualifies.
    fn repaired_window_is_offsets_only(&self, floor: u64, to_op: u64) -> bool {
        if floor >= to_op {
            return false;
        }
        ((floor + 1)..=to_op).all(|op| {
            self.log
                .journal()
                .inner
                .header_by_op(op)
                .is_some_and(|header| header.operation != Operation::SendMessages)
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
    ) -> Result<Option<u64>, IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let (base_offset, base_timestamp, total_size, message_count) = {
            let batch =
                decode_prepare_slice(message.as_slice()).map_err(|_| IggyError::InvalidCommand)?;
            (
                batch.header.base_offset,
                batch.header.base_timestamp,
                batch.header.total_size() as u64,
                batch.message_count(),
            )
        };
        if message_count == 0 {
            return Ok(None);
        }
        let last_offset = base_offset + u64::from(message_count) - 1;

        self.should_increment_offset = true;
        let dirty = self.dirty_offset.load(Ordering::Relaxed);
        self.dirty_offset
            .store(dirty.max(last_offset), Ordering::Relaxed);

        let segment_index = self.log.segments().len() - 1;
        let current_position = self.log.segments()[segment_index].current_position;
        self.log.segments_mut()[segment_index].current_position = current_position
            .checked_add(total_size)
            .ok_or(IggyError::CannotAppendMessage)?;

        let journal = self.log.journal_mut();
        journal.info.messages_count += message_count;
        journal.info.size += IggyByteSize::from(total_size);
        journal.info.current_offset = last_offset;
        if journal.info.first_timestamp == 0 {
            journal.info.first_timestamp = base_timestamp;
        }
        journal.info.end_timestamp = base_timestamp;
        journal.info.max_timestamp = journal.info.max_timestamp.max(base_timestamp);
        journal
            .inner
            .append(message.into_frozen())
            .await
            .map_err(|_| IggyError::CannotAppendMessage)?;
        Ok(Some(base_offset))
    }

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        // `VsrAction::RetransmitPrepares` reads from `self.log.journal`.
        // Both `SendMessages` (via `append_send_messages_to_journal`) and
        // consumer-offset ops (via `apply_replicated_operation`) append
        // to that journal before `send_prepare_ok` fires, so every op
        // that reaches here is journal-backed and ACKs as durable.
        send_prepare_ok_common(self.consensus(), header, Some(true)).await;
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

/// Success reply body for a committed partition op. Result-framed ops
/// (`Operation::is_result_framed`; on this plane the consumer-offset ops,
/// whose rejections ship typed errors) must carry an explicit empty result
/// section (`[count = 0]`) so the SDK's framed decode does not misread the
/// payload; every other partition op replies with an empty body.
const fn committed_reply_body(operation: Operation) -> bytes::Bytes {
    if operation.is_result_framed() {
        bytes::Bytes::from_static(&[0, 0, 0, 0])
    } else {
        bytes::Bytes::new()
    }
}

/// Committed-batch accounting surfaced at commit time so the aggregate stats
/// (`messages_count`, `size_bytes`) advance with the visible offset rather than
/// waiting on the threshold-gated disk persist.
#[derive(Clone, Copy)]
struct CommittedBatchStats {
    end_offset: u64,
    message_count: u32,
    size_bytes: u64,
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

/// Highest `end_offset` to drop so the resident size falls to `max_bytes`, or
/// `None` when already under budget. The active segment (last element) is
/// never dropped. The budget is per-partition: the cluster has no single owner
/// of a topic-wide total, so each replica trims its own log.
fn leading_oversized_end(segments: &[Segment], max_bytes: u64) -> Option<u64> {
    let last_idx = segments.len().saturating_sub(1);
    let mut resident: u64 = segments
        .iter()
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
    use crate::poll_plan::{DiskReadOutcome, SealedSegmentHandle};
    use bytes::Bytes;
    use compio::io::AsyncWriteAtExt;
    use consensus::LocalPipeline;
    use iggy_binary_protocol::{Command2, ReplyHeader, WireConsumer, WireEncode};
    use message_bus::SendError;
    use server_common::MESSAGE_ALIGN;
    use server_common::send_messages2::{
        COMMAND_HEADER_SIZE, IggyMessage2, IggyMessage2Header, IggyMessages2, SendMessages2Owned,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    const TEST_CLUSTER: u128 = 1;

    fn test_partition() -> IggyPartition<IggyMessageBus> {
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

    /// Client-facing bus that records every `send_to_client` frame so tests
    /// can assert on reply bytes without a connection registry (whose slot
    /// guard would borrow the partition across `on_request(&mut self)`).
    #[derive(Debug, Default)]
    struct RecordingBus {
        sent_to_clients: Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>,
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
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
    }

    type SentFrames = Rc<RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>>;

    fn recording_partition() -> (IggyPartition<RecordingBus>, SentFrames) {
        let namespace = IggyNamespace::new(1, 1, 0);
        let bus = RecordingBus::default();
        let sent_to_clients = bus.sent_to_clients.clone();
        let consensus = VsrConsensus::new(
            TEST_CLUSTER,
            0,
            1,
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
    ) -> Message<RequestHeader> {
        let body = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(consumer_id)),
            stream_id: WireIdentifier::Numeric(1),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(0),
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let header_size = std::mem::size_of::<RequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RequestHeader>::new(total);
        message.as_mut_slice()[header_size..].copy_from_slice(&body);
        message.transmute_header(|_, header: &mut RequestHeader| {
            header.command = Command2::Request;
            header.operation = Operation::DeleteConsumerOffset2;
            header.client = client_id;
            header.session = 1;
            header.request = request_id;
            header.namespace = IggyNamespace::new(1, 1, 0).inner();
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
            .on_request(delete_offset_request(client_id, 7, consumer_id))
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
            assert_eq!(header.command, Command2::Reply);
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
            partition.consensus().pipeline().borrow().len(),
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
            .on_request(delete_offset_request(client_id, 8, consumer_id))
            .await;
        assert_eq!(
            partition.consensus().pipeline().borrow().len(),
            1,
            "existing offset delete must replicate"
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
            u64::from_le_bytes(bytes.try_into().expect("offset file is 8 bytes"))
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
            u64::from_le_bytes(bytes.try_into().expect("offset file is 8 bytes"))
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
    /// TODO: a true cross-task interleave (pump reallocs the partitions vec
    /// while the reconciler awaits the unlink) needs a two-future sim oracle
    /// that does not exist yet; this covers the synchronous removal contract
    /// the off-borrow split relies on.
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
    fn build_segment_record(namespace: IggyNamespace, base_offset: u64) -> Vec<u8> {
        let mut batch = IggyMessages2::with_capacity(1);
        batch.push(IggyMessage2 {
            header: IggyMessage2Header {
                payload_length: 8,
                ..Default::default()
            },
            payload: Bytes::from_static(b"abcdefgh"),
            user_headers: None,
        });
        let mut owned = SendMessages2Owned::from_messages(namespace, &batch)
            .expect("build send_messages batch");
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
            partition_dir: Some(partition_dir),
            segments: vec![
                DiskSegment {
                    start_offset: 0,
                    persisted: 512,
                    read_state: None,
                },
                DiskSegment {
                    start_offset: 5,
                    persisted: later_len,
                    read_state: None,
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
            partition_dir: Some(partition_dir),
            segments: vec![
                DiskSegment {
                    start_offset: 0,
                    persisted: corrupt_len,
                    read_state: None,
                },
                DiskSegment {
                    start_offset: 5,
                    persisted: later_len,
                    read_state: None,
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
        assert!(handle.fd.borrow().is_none(), "fd cache slot starts empty");

        let plan = DiskReadPlan {
            partition_dir: Some(partition_dir.clone()),
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Some(Rc::clone(&handle)),
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
            partition_dir: Some(partition_dir.clone()),
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: record_len,
                read_state: Some(Rc::clone(&handle)),
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
            partition_dir: Some(partition_dir.clone()),
            segments: vec![DiskSegment {
                start_offset: 0,
                persisted: log_len,
                read_state: Some(Rc::clone(&handle)),
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

    fn repair_config() -> PartitionsConfig {
        PartitionsConfig {
            messages_required_to_save: 1,
            size_of_messages_required_to_save: IggyByteSize::from(1024 * 1024),
            enforce_fsync: false,
            segment_size: IggyByteSize::from(1024 * 1024),
            encryptor: None,
        }
    }

    fn armed_session(to_op: u64, floor: u64, first_batch_offset: Option<u64>) -> RepairSession {
        RepairSession {
            nonce: 1,
            to_op,
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
                header.command = Command2::Prepare;
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

    #[compio::test]
    async fn given_no_repaired_batch_when_window_never_arrived_should_refuse_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        partition.repair = Some(armed_session(8, 5, None));

        partition.complete_repair(&repair_config()).await;

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

        partition.complete_repair(&repair_config()).await;

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

        partition.complete_repair(&repair_config()).await;

        assert_eq!(partition.consensus().commit_min(), 0);
    }

    #[compio::test]
    async fn given_no_repaired_batch_when_window_fully_evicted_should_refuse_commit_floor() {
        let mut partition = test_partition();
        partition.consensus().advance_commit_max(8);
        partition.repair = Some(armed_session(8, 8, None));

        partition.complete_repair(&repair_config()).await;

        assert_eq!(partition.consensus().commit_min(), 0);
        assert!(partition.repair.is_some());
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

        partition.complete_repair(&repair_config()).await;

        assert_eq!(partition.consensus().commit_min(), 0);
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
        // 4 x 100 = 400 resident, active excluded. Budget 250: drop seg0 (300
        // left) then seg1 (200 <= 250, stop). up_to = seg1.end_offset.
        let segments = vec![
            segment(9, 1, 100, true),
            segment(19, 2, 100, true),
            segment(29, 3, 100, true),
            segment(39, 0, 100, false),
        ];
        assert_eq!(leading_oversized_end(&segments, 250), Some(19));
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
}
