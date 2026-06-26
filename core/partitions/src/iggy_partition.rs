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
use crate::journal::{
    MessageLookup, PartitionJournal, PartitionJournalMemStorage, QueryableJournal,
    select_batch_slice,
};
use crate::log::JournalInfo;
use crate::log::SegmentedLog;
use crate::messages_writer::MessagesWriter;
use crate::offset_storage::{delete_persisted_offset, persist_offset};
use crate::segment::Segment;
use crate::types::Fragment;
use crate::{
    AppendResult, Partition, PartitionOffsets, PartitionsConfig, PollFragments, PollQueryResult,
    PollingArgs, PollingConsumer,
};
use compio::io::AsyncReadAtExt;
use consensus::{
    CommitLogEvent, Consensus, PartitionDiagEvent, Pipeline, PipelineEntry, PlaneKind, Project,
    ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus, ack_preflight,
    ack_quorum_reached, build_reply_from_request, build_reply_message, drain_committable_prefix,
    emit_namespace_progress_event, emit_partition_diag, emit_sim_event,
    fence_old_prepare_by_commit, replicate_preflight, replicate_to_next_in_chain,
    send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{AckLevel, Operation, PrepareHeader, WireDecode, WireIdentifier};
use iggy_binary_protocol::{PrepareOkHeader, RequestHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyTimestamp, PartitionStats, PollingKind,
};
use journal::Journal as _;
use message_bus::{IggyMessageBus, MessageBus};
use server_common::{
    Message, SegmentStorage,
    iobuf::{Frozen, Owned},
    send_messages2::{
        COMMAND_HEADER_SIZE, convert_request_message, decode_batch_slice, decode_prepare_slice,
        stamp_prepare_for_persistence,
    },
    sharding::IggyNamespace,
};
use std::collections::HashMap;
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
    pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
    observed_view: u32,
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
        }
    }

    const fn delete(kind: ConsumerKind, consumer_id: u32) -> Self {
        Self {
            kind,
            consumer_id,
            mutation: PendingConsumerOffsetMutation::Delete,
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
        Self {
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
            pending_consumer_offset_commits: HashMap::new(),
            observed_view,
        }
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
    ) {
        let pending = PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset);
        self.pending_consumer_offset_commits.insert(op, pending);
    }

    /// Stage a consumer offset delete for the replicated op. See
    /// [`stage_consumer_offset_upsert`] for the ordering contract.
    ///
    /// # Errors
    ///
    /// Returns `ConsumerOffsetNotFound` if the consumer or group has no
    /// existing on-disk / in-memory offset to delete.
    pub(crate) fn stage_consumer_offset_delete(
        &mut self,
        op: u64,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), IggyError> {
        self.ensure_consumer_offset_exists(kind, consumer_id)?;
        let pending = PendingConsumerOffsetCommit::delete(kind, consumer_id);
        self.pending_consumer_offset_commits.insert(op, pending);
        Ok(())
    }

    pub(crate) async fn apply_staged_consumer_offset_commit(
        &mut self,
        op: u64,
    ) -> Result<(), IggyError> {
        // Peek (copy) instead of remove: if `persist_consumer_offset_commit`
        // fails (e.g. disk full, fd exhausted) the pending entry must remain
        // stageable for retry on the next apply. Removing first would strand
        // the op - not on disk AND not in memory.
        let pending = *self
            .pending_consumer_offset_commits
            .get(&op)
            .ok_or(IggyError::InvalidCommand)?;
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
        match pending.mutation {
            PendingConsumerOffsetMutation::Upsert(offset) => {
                persist_offset(&path, offset, self.consumer_offset_enforce_fsync).await
            }
            PendingConsumerOffsetMutation::Delete => delete_persisted_offset(&path).await,
        }
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
                let guard = self.consumer_offsets.pin();
                let key = usize::try_from(id).expect("u32 consumer id must fit usize");
                if let Some(existing) = guard.get(&key) {
                    existing.offset.store(offset, Ordering::Relaxed);
                } else {
                    let created = self.consumer_offsets_path.as_deref().map_or_else(
                        || ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
                        |path| ConsumerOffset::default_for_consumer(id, path),
                    );
                    created.offset.store(offset, Ordering::Relaxed);
                    guard.insert(key, created);
                }
                Ok(())
            }
            PendingConsumerOffsetMutation::Upsert(offset)
                if pending.kind == ConsumerKind::ConsumerGroup =>
            {
                let group_id = pending.consumer_id;
                let guard = self.consumer_group_offsets.pin();
                let key = ConsumerGroupId(
                    usize::try_from(group_id).expect("u32 group id must fit usize"),
                );
                if let Some(existing) = guard.get(&key) {
                    existing.offset.store(offset, Ordering::Relaxed);
                } else {
                    let created = self.consumer_group_offsets_path.as_deref().map_or_else(
                        || {
                            ConsumerOffset::new(
                                ConsumerKind::ConsumerGroup,
                                group_id,
                                0,
                                String::new(),
                            )
                        },
                        |path| ConsumerOffset::default_for_consumer_group(key, path),
                    );
                    created.offset.store(offset, Ordering::Relaxed);
                    guard.insert(key, created);
                }
                Ok(())
            }
            PendingConsumerOffsetMutation::Delete if pending.kind == ConsumerKind::Consumer => {
                let id = pending.consumer_id;
                let guard = self.consumer_offsets.pin();
                let key = usize::try_from(id).expect("u32 consumer id must fit usize");
                let _ = guard
                    .remove(&key)
                    .ok_or(IggyError::ConsumerOffsetNotFound(key))?;
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
                let _ = guard
                    .remove(&key)
                    .ok_or(IggyError::ConsumerOffsetNotFound(key.0))?;
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

    /// Reclaim a deleted consumer group's offset on this partition (in-memory
    /// entry + persisted file). A no-op if the group has no stored offset here.
    ///
    /// # Errors
    /// Returns an I/O error if deleting the persisted offset file fails.
    #[allow(clippy::cast_possible_truncation)]
    pub async fn delete_consumer_group_offset(&self, group_id: u64) -> Result<(), IggyError> {
        self.consumer_group_offsets
            .pin()
            .remove(&ConsumerGroupId(group_id as usize));
        if let Some(path) = self.persisted_offset_path(ConsumerKind::ConsumerGroup, group_id as u32)
        {
            delete_persisted_offset(&path).await?;
        }
        Ok(())
    }

    async fn store_consumer_offset_and_persist(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let pending = PendingConsumerOffsetCommit::try_from_polling_consumer(consumer, offset)?;
        self.persist_consumer_offset_commit(pending).await?;
        self.apply_consumer_offset_commit(pending)?;
        Ok(())
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

        let reply = build_reply_from_request(&self.consensus, &request_header, bytes::Bytes::new());
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

    async fn poll_messages(
        &self,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> Result<PollQueryResult<4096>, IggyError> {
        if !self.should_increment_offset || args.count == 0 {
            return Ok((PollFragments::new(), None));
        }

        let write_offset = self.offset.load(Ordering::Acquire);

        let result = match args.strategy.kind {
            PollingKind::Timestamp => {
                self.lookup_messages(MessageLookup::Timestamp {
                    timestamp: args.strategy.value,
                    count: args.count,
                })
                .await
            }
            kind => {
                let start_offset = match kind {
                    PollingKind::Offset => args.strategy.value,
                    PollingKind::First => 0,
                    PollingKind::Last => write_offset.saturating_sub(u64::from(args.count) - 1),
                    PollingKind::Next => self
                        .get_consumer_offset(consumer)
                        .map_or(0, |offset| offset + 1),
                    PollingKind::Timestamp => unreachable!(),
                };

                if start_offset > write_offset {
                    return Ok((PollFragments::new(), None));
                }

                self.lookup_messages(MessageLookup::Offset {
                    offset: start_offset,
                    count: args.count,
                })
                .await
            }
        };

        let (fragments, last_matching_offset) =
            result.unwrap_or_else(|| (PollFragments::new(), None));

        // Record the highest offset served to a consumer group, so the
        // cooperative-rebalance reconciler knows when a pending-revoked
        // partition has been fully drained (committed >= last polled).
        if let (PollingConsumer::ConsumerGroup(group_id, _), Some(last_offset)) =
            (consumer, last_matching_offset)
        {
            let guard = self.last_polled_offsets.pin();
            let key = ConsumerGroupId(group_id);
            if let Some(existing) = guard.get(&key) {
                existing.offset.fetch_max(last_offset, Ordering::Relaxed);
            } else {
                let created = ConsumerOffset::new(
                    ConsumerKind::ConsumerGroup,
                    u32::try_from(group_id).unwrap_or(u32::MAX),
                    last_offset,
                    String::new(),
                );
                guard.insert(key, created);
            }
        }

        if args.auto_commit && !fragments.is_empty() {
            let last_offset =
                last_matching_offset.expect("non-empty poll result must have a last offset");
            if let Err(err) = self
                .store_consumer_offset_and_persist(consumer, last_offset)
                .await
            {
                // warning for now.
                warn!(
                    target: "iggy.partitions.diag",
                    consumer = ?consumer,
                    last_offset,
                    %err,
                    "poll_messages: failed to store consumer offset"
                );
            }
        }

        Ok((fragments, last_matching_offset))
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

    /// Resolve a poll query against the in-memory journal, falling back to
    /// the on-disk segments for ranges the journal no longer holds (the
    /// persist threshold drains committed batches to segment files).
    ///
    /// A query is served from exactly one tier per call: a poll that starts
    /// below the journal's oldest resident offset reads from disk only, and
    /// the client's next poll (advancing past what was returned) eventually
    /// crosses back into the resident range. Timestamp queries try disk
    /// first whenever segments hold persisted bytes -- older matches always
    /// live there -- and fall back to the journal when the disk has none.
    async fn lookup_messages(&self, query: MessageLookup) -> Option<PollQueryResult<4096>> {
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
            return self.log.journal().inner.get(&query).await;
        }
        match self.poll_from_disk(query).await {
            Some((mut fragments, last_matching_offset, matched)) => {
                // A poll can straddle the tiers: older messages already
                // drained to segments, the tail still journal-resident.
                // Continue past the last disk match by offset (timestamp
                // matches are contiguous from the first hit, so an offset
                // continuation is equivalent).
                let remaining = query.count().saturating_sub(matched);
                if remaining > 0
                    && let Some(last_offset) = last_matching_offset
                {
                    let continuation = MessageLookup::Offset {
                        offset: last_offset + 1,
                        count: remaining,
                    };
                    if let Some((journal_fragments, journal_last)) =
                        self.log.journal().inner.get(&continuation).await
                    {
                        fragments.extend(journal_fragments);
                        return Some((fragments, journal_last.or(last_matching_offset)));
                    }
                }
                Some((fragments, last_matching_offset))
            }
            // Nothing matched on disk (e.g. a timestamp newer than every
            // persisted batch): the match, if any, is journal-resident.
            None => self.log.journal().inner.get(&query).await,
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

    fn has_persisted_segment_bytes(&self) -> bool {
        self.log
            .segments()
            .iter()
            .any(|segment| segment.size.as_bytes_u64() > 0)
    }

    /// Serve a poll from the on-disk segment files.
    ///
    /// Picks the starting segment + byte position via the sparse index
    /// (one entry per persist flush; a miss falls back to the segment
    /// start), then walks stamped `[256B SendMessages2Header][blob]`
    /// batches in chunked reads, slicing fragments with the same selector
    /// the journal path uses. Batches split across a chunk boundary are
    /// re-read from their start in the next chunk.
    #[allow(clippy::cast_possible_truncation)]
    async fn poll_from_disk(
        &self,
        query: MessageLookup,
    ) -> Option<(PollFragments<4096>, Option<u64>, u32)> {
        const DISK_POLL_CHUNK: u64 = 1 << 20;

        let count = query.count();
        if count == 0 || !self.log.has_segments() {
            return None;
        }

        let (start_segment, mut position) = self.disk_poll_start(&query);

        let mut fragments = PollFragments::new();
        let mut last_matching_offset = None;
        let mut matched: u32 = 0;

        for segment_index in start_segment..self.log.segments().len() {
            if matched >= count {
                break;
            }
            let persisted = self.log.segments()[segment_index].size.as_bytes_u64();
            if persisted == 0 || position >= persisted {
                position = 0;
                continue;
            }
            // Sealed segments drop their writer at rotation, so resolve the
            // file from the partition directory (taken from any live writer)
            // plus the segment's start offset, mirroring the writer naming.
            let Some(partition_dir) = self.partition_dir() else {
                // Simulated in-memory persistence: no files to read. A live
                // partition hitting this means no writer was resolvable
                // (e.g. mid-rotation), which silently hides the disk tier.
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    segment_count = self.log.segments().len(),
                    "disk poll: no live writer to resolve partition dir; disk tier unreadable"
                );
                return None;
            };
            let start_offset = self.log.segments()[segment_index].start_offset;
            let path = format!("{partition_dir}/{start_offset:0>20}.log");
            let Some(file) = self.open_segment_with_retry(&path).await else {
                position = 0;
                continue;
            };

            let mut chunk_len = DISK_POLL_CHUNK;
            while matched < count && position < persisted {
                let len = (persisted - position).min(chunk_len) as usize;
                let Some(chunk) = self.read_chunk_with_retry(&file, position, len).await else {
                    break;
                };
                let consumed = walk_disk_chunk(
                    &chunk,
                    query,
                    count,
                    &mut matched,
                    &mut fragments,
                    &mut last_matching_offset,
                );
                if consumed == 0 {
                    if (len as u64) >= persisted - position {
                        // The whole remainder fit and still no complete
                        // batch decoded: corrupt tail; stop.
                        break;
                    }
                    // A single batch larger than the chunk: grow and
                    // re-read instead of spinning.
                    chunk_len = chunk_len.saturating_mul(4);
                    continue;
                }
                chunk_len = DISK_POLL_CHUNK;
                position += consumed as u64;
            }
            position = 0;
        }

        if fragments.is_empty() {
            None
        } else {
            Some((fragments, last_matching_offset, matched))
        }
    }

    /// Open a segment file for a disk poll, retrying transient IO failures
    /// (fd pressure under heavy parallel load) so one failed syscall does
    /// not silently collapse the poll into an empty result.
    async fn open_segment_with_retry(&self, path: &str) -> Option<compio::fs::File> {
        for attempt in 0..3u8 {
            match compio::fs::File::open(path).await {
                Ok(file) => return Some(file),
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.namespace().inner(),
                        path,
                        attempt,
                        %error,
                        "disk poll: failed to open segment file"
                    );
                    compio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
        None
    }

    /// Read one chunk for a disk poll, retrying transient IO failures.
    async fn read_chunk_with_retry(
        &self,
        file: &compio::fs::File,
        position: u64,
        len: usize,
    ) -> Option<Frozen<4096>> {
        for attempt in 0..3u8 {
            let buffer = Owned::<4096>::zeroed(len);
            let compio::BufResult(read, buffer) = file.read_exact_at(buffer, position).await;
            match read {
                Ok(()) => return Some(Frozen::from(buffer)),
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.namespace().inner(),
                        position,
                        attempt,
                        %error,
                        "disk poll: segment read failed"
                    );
                    compio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
        None
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
                for (segment_index, _) in segments.iter().enumerate() {
                    if let Some(index) = self
                        .log
                        .segment_indexes(segment_index)
                        .and_then(|cache| cache.timestamp_lower_bound(*timestamp))
                    {
                        return (segment_index, index.position);
                    }
                }
                (0, 0)
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
                match convert_request_message(namespace, message) {
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
                return;
            }

            assert!(!consensus.is_follower(), "on_request: primary only");
            assert!(consensus.is_normal(), "on_request: status must be normal");
            assert!(!consensus.is_syncing(), "on_request: must not be syncing");

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
                        );
                    }
                    Operation::DeleteConsumerOffset | Operation::DeleteConsumerOffset2 => {
                        self.stage_consumer_offset_delete(header.op, kind, consumer_id)?;
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

    #[allow(clippy::too_many_lines)]
    async fn commit_messages(&mut self, config: &PartitionsConfig) -> Result<(), IggyError> {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        let journal_info = self.log.journal().info;
        if journal_info.messages_count == 0 {
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
        if !should_persist {
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
            return Ok(());
        }
        let committed_count = committed_entries.len();

        let (frozen_batches, index_bytes, flush_index, batch_count, committed_info) = {
            let segment = self.log.active_segment();
            let mut file_position = segment.size.as_bytes_u64();
            let mut flush_index = None;
            let mut frozen = Vec::with_capacity(committed_entries.len());
            let mut batch_count = 0u32;
            let mut committed_info = JournalInfo::default();

            for entry in committed_entries {
                // Consumer-offset ops are journaled in the same prefix but carry
                // no segment bytes; they were applied when staged, so skip them.
                if peek_operation(&entry) != Operation::SendMessages {
                    continue;
                }
                // A resident committed SendMessages entry decoded once at append
                // (the offset index) with its checksum stamped over these exact
                // bytes, so it must decode again here. Guard the invariant for a
                // future disk read-back path that could make decode fallible.
                let Ok(batch) = decode_prepare_slice(entry.as_slice()) else {
                    debug_assert!(
                        false,
                        "resident committed SendMessages entry failed to decode"
                    );
                    continue;
                };
                let message_count = batch.message_count();
                if message_count == 0 {
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
            )
        };

        // No committed SendMessages batch was resident (e.g. only uncommitted
        // ops, or a committed consumer-offset prefix that is not persisted to a
        // segment). Nothing to flush, but the committed prefix must still be
        // evicted; no segment bytes are at risk, so evict directly.
        let Some(index_bytes) = index_bytes else {
            self.evict_committed_prefix(committed_count).await;
            return Ok(());
        };

        // Persist BEFORE eviction so a write failure leaves the committed prefix
        // resident for retry. The persist is idempotent on failure: a batch
        // write that lands but whose index save then fails rewinds the segment
        // write cursor, so the retry overwrites those bytes instead of appending
        // a duplicate. Only once the bytes are durable do we evict and rebuild
        // the tail accounting.
        self.persist_frozen_batches_to_disk(frozen_batches, index_bytes, batch_count)
            .await?;
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
        self.evict_committed_prefix(committed_count).await;

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

        if is_full {
            self.rotate_segment(config).await?;
        }

        self.stats
            .increment_size_bytes(committed_info.size.as_bytes_u64());
        self.stats
            .increment_messages_count(u64::from(committed_info.messages_count));

        let durable_offset = committed_info.current_offset;
        self.offset.store(durable_offset, Ordering::Release);
        self.stats.set_current_offset(durable_offset);
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

        for mut entry in drained {
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
            let reply = build_reply_message(&prepare_header, &bytes::Bytes::new());

            // TODO: no production caller yet. Partition has no in-process
            // subscriber (only metadata uses pipeline_message_with_subscriber);
            // wired for forward-compat. Fired AFTER local commit (slot-first
            // ordering analog). Dropped receiver ignored.
            if let Some(sender) = entry.take_reply_sender() {
                let _ = sender.send(reply.clone());
            }

            if send_client_replies {
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
    ) -> HashMap<u64, u64> {
        let mut committed_visible_offsets = HashMap::new();

        for entry in drained {
            if entry.header.operation != Operation::SendMessages {
                continue;
            }

            match self.committed_end_offset_for_prepare(&entry.header).await {
                Ok(Some(end_offset)) => {
                    committed_visible_offsets.insert(entry.header.op, end_offset);
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
        committed_visible_offsets: &HashMap<u64, u64>,
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

                if let Some(visible_offset) = committed_visible_offsets.get(&prepare_header.op) {
                    self.offset.store(*visible_offset, Ordering::Release);
                    self.stats.set_current_offset(*visible_offset);
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

    async fn committed_end_offset_for_prepare(
        &self,
        prepare_header: &PrepareHeader,
    ) -> Result<Option<u64>, IggyError> {
        let Some(entry) = self.log.journal().inner.entry(prepare_header).await else {
            return Err(IggyError::InvalidCommand);
        };
        let batch =
            decode_prepare_slice(entry.as_slice()).map_err(|_| IggyError::InvalidCommand)?;
        let message_count = batch.message_count();
        if message_count == 0 {
            return Ok(None);
        }

        Ok(Some(
            batch.header.base_offset + u64::from(message_count) - 1,
        ))
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
        let index_writer = Rc::new(
            IggyIndexWriter::new(
                &index_path,
                Rc::new(std::sync::atomic::AtomicU64::new(0)),
                config.enforce_fsync,
                false,
            )
            .await
            .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?,
        );

        let old_storage = &mut self.log.storages_mut()[old_segment_index];
        let _ = old_storage.shutdown();
        self.log.messages_writers_mut()[old_segment_index] = None;
        self.log.index_writers_mut()[old_segment_index] = None;

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

    async fn send_prepare_ok(&self, header: &PrepareHeader) {
        // `VsrAction::RetransmitPrepares` reads from `self.log.journal`.
        // Both `SendMessages` (via `append_send_messages_to_journal`) and
        // consumer-offset ops (via `apply_replicated_operation`) append
        // to that journal before `send_prepare_ok` fires, so every op
        // that reaches here is journal-backed and ACKs as durable.
        send_prepare_ok_common(self.consensus(), header, Some(true)).await;
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

/// Walk stamped `[256B SendMessages2Header][blob]` batches in one disk
/// chunk, pushing matching fragments. Returns bytes consumed: the start
/// of the first batch that did not fully fit in the chunk (the caller
/// re-reads from there), or the chunk end when everything decoded.
fn walk_disk_chunk(
    chunk: &Frozen<4096>,
    query: MessageLookup,
    count: u32,
    matched: &mut u32,
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
) -> usize {
    let bytes: &[u8] = chunk;
    let mut cursor = 0usize;

    while *matched < count && cursor + COMMAND_HEADER_SIZE <= bytes.len() {
        let Ok(batch) = decode_batch_slice(&bytes[cursor..]) else {
            // Incomplete tail batch (or corrupt data): hand the position
            // back so the caller can re-read or bail.
            break;
        };
        let total_size = batch.header.total_size();

        if let Some(selection) = select_batch_slice(&batch, query, *matched) {
            let full_body_selected = selection.start == 0 && selection.end == batch.blob().len();
            if full_body_selected {
                fragments.push(Fragment::slice(chunk.clone(), cursor, cursor + total_size));
            } else {
                let mut rewritten = batch.header;
                rewritten.batch_length =
                    u64::try_from(COMMAND_HEADER_SIZE + (selection.end - selection.start))
                        .expect("sliced batch length exceeds u64::MAX");
                rewritten.message_count = selection.matched_messages;
                rewritten.batch_checksum = rewritten.checksum_for_blob(
                    batch
                        .blob()
                        .get(selection.start..selection.end)
                        .expect("selected batch slice must stay within blob bounds"),
                );
                fragments.push(Fragment::whole(rewritten.into_frozen()));
                fragments.push(Fragment::slice(
                    chunk.clone(),
                    cursor + COMMAND_HEADER_SIZE + selection.start,
                    cursor + COMMAND_HEADER_SIZE + selection.end,
                ));
            }
            *last_matching_offset = Some(selection.last_matching_offset);
            *matched += selection.matched_messages;
        }

        cursor += total_size;
    }

    cursor.min(bytes.len())
}
