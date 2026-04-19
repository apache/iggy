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
};
use crate::log::JournalInfo;
use crate::log::SegmentedLog;
use crate::messages_writer::MessagesWriter;
use crate::offset_storage::{delete_persisted_offset, persist_offset};
use crate::segment::Segment;
use crate::{
    AppendResult, Partition, PartitionOffsets, PartitionsConfig, PollFragments, PollQueryResult,
    PollingArgs, PollingConsumer,
};
use consensus::{
    CommitLogEvent, Consensus, PartitionDiagEvent, Pipeline, PipelineEntry, PlaneKind, Project,
    ReplicaLogContext, RequestLogEvent, Sequencer, SimEventKind, VsrConsensus, ack_preflight,
    ack_quorum_reached, build_reply_message, drain_committable_prefix,
    emit_namespace_progress_event, emit_partition_diag, emit_sim_event,
    fence_old_prepare_by_commit, replicate_preflight, replicate_to_next_in_chain,
    request_preflight, send_prepare_ok as send_prepare_ok_common,
};
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{GenericHeader, PrepareOkHeader, RequestHeader};
use iggy_binary_protocol::{Message, Operation, PrepareHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyTimestamp, PartitionStats, PollingKind, SegmentStorage,
    send_messages2::stamp_prepare_for_persistence,
    send_messages2::{convert_request_message, decode_prepare_slice},
    sharding::IggyNamespace,
};
use journal::Journal as _;
use message_bus::{IggyMessageBus, MessageBus};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, warn};

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
//
// TODO: Fix op deduplication once we move to a consensus-per-partition design.
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
    pub stats: Arc<PartitionStats>,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub should_increment_offset: bool,
    pub write_lock: Arc<TokioMutex<()>>,
    consumer_offsets_path: Option<String>,
    consumer_group_offsets_path: Option<String>,
    consumer_offset_enforce_fsync: bool,
    pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
    observed_view: u32,
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
            stats,
            created_at: IggyTimestamp::now(),
            revision_id: 0,
            should_increment_offset: false,
            write_lock: Arc::new(TokioMutex::new(())),
            consumer_offsets_path: None,
            consumer_group_offsets_path: None,
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

    pub(crate) async fn persist_and_stage_consumer_offset_upsert(
        &mut self,
        op: u64,
        kind: ConsumerKind,
        consumer_id: u32,
        offset: u64,
    ) -> Result<(), IggyError> {
        let pending = PendingConsumerOffsetCommit::upsert(kind, consumer_id, offset);
        self.persist_consumer_offset_commit(pending).await?;
        self.pending_consumer_offset_commits.insert(op, pending);
        Ok(())
    }

    pub(crate) async fn persist_and_stage_consumer_offset_delete(
        &mut self,
        op: u64,
        kind: ConsumerKind,
        consumer_id: u32,
    ) -> Result<(), IggyError> {
        self.ensure_consumer_offset_exists(kind, consumer_id)?;
        let pending = PendingConsumerOffsetCommit::delete(kind, consumer_id);
        self.persist_consumer_offset_commit(pending).await?;
        self.pending_consumer_offset_commits.insert(op, pending);
        Ok(())
    }

    pub(crate) fn apply_staged_consumer_offset_commit(&mut self, op: u64) -> Result<(), IggyError> {
        let pending = self
            .pending_consumer_offset_commits
            .remove(&op)
            .ok_or(IggyError::InvalidCommand)?;
        self.apply_consumer_offset_commit(pending)?;
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

        // TODO: Replace this with monotonic broker timestamp assignment. If wall clock
        // time goes backwards, clamp to the partition/log max timestamp instead.
        let batch_timestamp = IggyTimestamp::now().as_micros();
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
                self.log
                    .journal()
                    .inner
                    .get(&MessageLookup::Timestamp {
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

                self.log
                    .journal()
                    .inner
                    .get(&MessageLookup::Offset {
                        offset: start_offset,
                        count: args.count,
                    })
                    .await
            }
        };

        let (fragments, last_matching_offset) =
            result.unwrap_or_else(|| (PollFragments::new(), None));

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
    B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
{
    #[must_use]
    fn namespace(&self) -> IggyNamespace {
        IggyNamespace::from_raw(self.consensus.namespace())
    }

    /// Handles a client request for this partition and turns it into a prepare.
    ///
    /// # Panics
    /// Panics if called when this partition's consensus instance is not the
    /// primary, is not in normal status, or is currently syncing.
    #[allow(clippy::future_not_send)]
    pub async fn on_request(&mut self, message: Message<RequestHeader>) {
        self.clear_pending_consumer_offset_commits_if_view_changed();
        let namespace = IggyNamespace::from_raw(message.header().namespace);
        let client_id = message.header().client;
        let session = message.header().session;
        let request = message.header().request;

        // TODO: Add a bounded request queue instead of dropping here.
        // When the prepare queue (8 max) is full, buffer incoming requests
        // in a request queue. On commit, pop the next request from the
        // request queue and begin preparing it. Only drop when both queues
        // are full.
        {
            let consensus = self.consensus();
            if consensus.pipeline().borrow().is_full() {
                emit_partition_diag(
                    tracing::Level::WARN,
                    &PartitionDiagEvent::new(
                        ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                        "on_request: pipeline full, dropping request",
                    )
                    .with_operation(message.header().operation),
                );
                return;
            }
        }

        let prepare = {
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

            if message.header().operation == Operation::DeleteConsumerOffset {
                match Self::parse_consumer_offset_request(message.header().operation, &message)
                    .and_then(|(kind, consumer_id, _)| {
                        self.ensure_consumer_offset_exists(kind, consumer_id)
                    }) {
                    Ok(()) => {}
                    Err(error) => {
                        emit_partition_diag(
                            tracing::Level::WARN,
                            &PartitionDiagEvent::new(
                                ReplicaLogContext::from_consensus(consensus, PlaneKind::Partitions),
                                "rejecting delete_consumer_offset for missing offset",
                            )
                            .with_operation(Operation::DeleteConsumerOffset)
                            .with_error(error.to_string()),
                        );
                        return;
                    }
                }
            }

            if request_preflight(consensus, client_id, session, request)
                .await
                .is_none()
            {
                return;
            }

            assert!(!consensus.is_follower(), "on_request: primary only");
            assert!(consensus.is_normal(), "on_request: status must be normal");
            assert!(!consensus.is_syncing(), "on_request: must not be syncing");

            let prepare = message.project(consensus);
            consensus.verify_pipeline();
            consensus.pipeline_message(PlaneKind::Partitions, &prepare);
            prepare
        };
        self.on_replicate(prepare).await;
    }

    #[allow(clippy::future_not_send)]
    pub async fn on_replicate(&mut self, message: Message<PrepareHeader>) {
        self.clear_pending_consumer_offset_commits_if_view_changed();
        let header = *message.header();
        let previous_commit = self.consensus.commit_max();
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
        let is_old_prepare = {
            let consensus = self.consensus();
            fence_old_prepare_by_commit(consensus, &header)
                || self.log.journal().inner.header_by_op(header.op).is_some()
        };
        let message = if is_old_prepare {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "received old prepare, skipping replication",
                )
                .with_operation(header.operation)
                .with_op(header.op),
            );
            message
        } else {
            let consensus = self.consensus();
            replicate_to_next_in_chain(consensus, message).await
        };

        if header.op != current_op + 1 {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(self.diag_ctx(), "dropping out-of-order prepare (gap)")
                    .with_operation(header.operation)
                    .with_op(header.op),
            );
            return;
        }
        {
            let consensus = self.consensus();
            consensus.sequencer().set_sequence(header.op);
            consensus.set_last_prepare_checksum(header.checksum);
        }
        let replicated_result = self.apply_replicated_operation(message).await;

        let commit = self.consensus.commit_max();
        if commit > previous_commit
            && let Err(error) = self.apply_committed_consumer_offset_commits_up_to(commit)
        {
            emit_partition_diag(
                tracing::Level::WARN,
                &PartitionDiagEvent::new(
                    self.diag_ctx(),
                    "failed to apply committed consumer offset updates after commit advanced",
                )
                .with_operation(header.operation)
                .with_op(header.op)
                .with_error(error.to_string()),
            );
            return;
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

        let drained = drain_committable_prefix(self.consensus());
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
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                let (kind, consumer_id, offset) =
                    Self::parse_staged_consumer_offset_commit(header.operation, &message)?;
                let write_lock = self.write_lock.clone();
                let _guard = write_lock.lock().await;
                match header.operation {
                    Operation::StoreConsumerOffset => {
                        self.persist_and_stage_consumer_offset_upsert(
                            header.op,
                            kind,
                            consumer_id,
                            offset.expect("store_consumer_offset must include offset"),
                        )
                        .await?;
                    }
                    Operation::DeleteConsumerOffset => {
                        self.persist_and_stage_consumer_offset_delete(header.op, kind, consumer_id)
                            .await?;
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
                    "replicated consumer offset persisted and staged"
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

        let (frozen_batches, index_bytes, batch_count) = {
            let entries = self.log.journal().inner.entries();
            let segment = self.log.active_segment();
            let mut file_position = segment.size.as_bytes_u64();
            self.log.ensure_indexes();
            let indexes = self.log.active_indexes_mut().expect("indexes must exist");
            let mut flush_index = None;
            let mut frozen = Vec::with_capacity(entries.len());
            let mut batch_count = 0u32;

            for entry in entries {
                let Ok(batch) = decode_prepare_slice(entry.as_slice()) else {
                    continue;
                };
                let message_count = batch.message_count();
                if message_count == 0 {
                    continue;
                }

                let index = crate::iggy_index::IggyIndex::new(
                    batch.header.base_offset,
                    batch.header.base_timestamp,
                    file_position,
                );
                if flush_index.is_none() {
                    indexes.insert(index.offset, index.timestamp, index.position);
                    flush_index = Some(index);
                }
                file_position += batch.header.total_size() as u64;
                batch_count += message_count;
                frozen.push(entry);
            }

            let index_bytes =
                flush_index.map(|index| crate::iggy_index::IggyIndexCache::serialize(&index));

            (frozen, index_bytes, batch_count)
        };

        let Some(index_bytes) = index_bytes else {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace().inner(),
                "failed to build sparse index entry from pending journal batches"
            );
            return Err(IggyError::InvalidCommand);
        };

        self.persist_frozen_batches_to_disk(frozen_batches, index_bytes, batch_count)
            .await?;

        if is_full {
            self.rotate_segment(config).await?;
        }

        let _ = self.log.journal_mut().inner.commit();
        self.log.journal_mut().info = JournalInfo::default();

        let segment_index = self.log.segments().len() - 1;
        let segment = &mut self.log.segments_mut()[segment_index];
        if segment.end_offset == 0 && journal_info.first_timestamp != 0 {
            segment.start_timestamp = journal_info.first_timestamp;
        }
        segment.end_timestamp = journal_info.end_timestamp;
        segment.max_timestamp = segment.max_timestamp.max(journal_info.max_timestamp);
        segment.end_offset = journal_info.current_offset;

        self.stats
            .increment_size_bytes(journal_info.size.as_bytes_u64());
        self.stats
            .increment_messages_count(u64::from(journal_info.messages_count));

        let durable_offset = journal_info.current_offset;
        self.offset.store(durable_offset, Ordering::Release);
        self.stats.set_current_offset(durable_offset);
        Ok(())
    }

    async fn handle_committed_entries(
        &mut self,
        drained: Vec<PipelineEntry>,
        config: &PartitionsConfig,
        send_client_replies: bool,
    ) {
        let replica_id = self.consensus.replica();
        let namespace_raw = self.consensus.namespace();
        if let (Some(first), Some(last)) = (drained.first(), drained.last()) {
            debug!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                replica_id,
                first_op = first.header.op,
                last_op = last.header.op,
                drained_count = drained.len(),
                "draining committed partition ops"
            );
        }

        let mut failed_commit = false;
        let committed_visible_offsets = self.resolve_committed_visible_offsets(&drained).await;
        let mut messages_committed = false;

        for PipelineEntry {
            header: prepare_header,
            ..
        } in drained
        {
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
                continue;
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

            // Cache reply in client_table (both primary and backups) to
            // preserve idempotency/dedup across view changes. Only the
            // primary actually sends the reply to the client.
            let reply = build_reply_message(&self.consensus, &prepare_header, bytes::Bytes::new());
            let session = self
                .consensus
                .client_table()
                .borrow()
                .get_session(prepare_header.client)
                .unwrap_or_else(|| {
                    panic!(
                        "handle_committed_entries: client {} not registered",
                        prepare_header.client
                    )
                });
            self.consensus.client_table().borrow_mut().commit_reply(
                prepare_header.client,
                session,
                reply.clone(),
            );

            if send_client_replies {
                let reply_buffers = reply.into_generic();
                emit_sim_event(SimEventKind::ClientReplyEmitted, &event);

                if let Err(error) = self
                    .consensus
                    .message_bus()
                    .send_to_client(prepare_header.client, reply_buffers)
                    .await
                {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        client = prepare_header.client,
                        op = prepare_header.op,
                        namespace_raw,
                        %error,
                        "failed to send reply to client"
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
    ) -> Result<(ConsumerKind, u32, Option<u64>), IggyError> {
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
    ) -> Result<(ConsumerKind, u32, Option<u64>), IggyError> {
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
    ) -> Result<(ConsumerKind, u32, Option<u64>), IggyError> {
        let consumer_kind = *body.first().ok_or(IggyError::InvalidCommand)?;
        let consumer_id = body
            .get(1..5)
            .ok_or(IggyError::InvalidCommand)
            .and_then(|bytes| {
                <[u8; 4]>::try_from(bytes)
                    .map(u32::from_le_bytes)
                    .map_err(|_| IggyError::InvalidCommand)
            })?;
        let kind = ConsumerKind::from_code(consumer_kind)?;
        match operation {
            Operation::StoreConsumerOffset => {
                let offset =
                    body.get(5..13)
                        .ok_or(IggyError::InvalidCommand)
                        .and_then(|bytes| {
                            <[u8; 8]>::try_from(bytes)
                                .map(u64::from_le_bytes)
                                .map_err(|_| IggyError::InvalidCommand)
                        })?;
                Ok((kind, consumer_id, Some(offset)))
            }
            Operation::DeleteConsumerOffset => Ok((kind, consumer_id, None)),
            _ => Err(IggyError::InvalidCommand),
        }
    }

    fn apply_committed_consumer_offset_commits_up_to(
        &mut self,
        commit: u64,
    ) -> Result<(), IggyError> {
        let mut committed_ops: Vec<_> = self
            .pending_consumer_offset_commits
            .keys()
            .copied()
            .filter(|op| *op <= commit)
            .collect();
        committed_ops.sort_unstable();

        for op in committed_ops {
            self.apply_staged_consumer_offset_commit(op)?;
        }

        Ok(())
    }

    async fn commit_consumer_offset_entry(
        &mut self,
        prepare_header: PrepareHeader,
        failed_commit: &mut bool,
    ) -> bool {
        let write_lock = self.write_lock.clone();
        let _guard = write_lock.lock().await;

        if let Err(error) = self.apply_staged_consumer_offset_commit(prepare_header.op) {
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

        index_writer
            .save_indexes(index_bytes)
            .await
            .map_err(|error| {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace().inner(),
                    batch_count,
                    %error,
                    "failed to save sparse indexes"
                );
                error
            })?;

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
        let messages_path = config.get_messages_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
        );
        let index_path = config.get_index_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
            start_offset,
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
        send_prepare_ok_common(self.consensus(), header, Some(true)).await;
    }
}
