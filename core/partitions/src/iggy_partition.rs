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

use crate::journal::{
    MessageLookup, PartitionJournal, PartitionJournalMemStorage, QueryableJournal,
};
use crate::log::SegmentedLog;
use crate::offset_storage::{delete_persisted_offset, persist_offset};
use crate::{
    AppendResult, Partition, PartitionOffsets, PollFragments, PollQueryResult, PollingArgs,
    PollingConsumer,
};
use iggy_binary_protocol::{Message, Operation, PrepareHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyTimestamp, PartitionStats, PollingKind,
    send_messages2::stamp_prepare_for_persistence,
};
use journal::Journal as _;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
//
// TODO: Fix op deduplication once we move to a consensus-per-partition design.
#[derive(Debug)]
pub struct IggyPartition {
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
    pending_consumer_offset_commits: HashMap<u64, PendingConsumerOffsetCommit>,
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

impl IggyPartition {
    pub fn new(stats: Arc<PartitionStats>) -> Self {
        Self {
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
            pending_consumer_offset_commits: HashMap::new(),
        }
    }

    pub fn configure_consumer_offset_storage(
        &mut self,
        consumer_offsets_path: String,
        consumer_group_offsets_path: String,
        consumer_offsets: ConsumerOffsets,
        consumer_group_offsets: ConsumerGroupOffsets,
    ) {
        self.consumer_offsets = Arc::new(consumer_offsets);
        self.consumer_group_offsets = Arc::new(consumer_group_offsets);
        self.consumer_offsets_path = Some(consumer_offsets_path);
        self.consumer_group_offsets_path = Some(consumer_group_offsets_path);
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
        self.apply_consumer_offset_commit(pending);
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
            PendingConsumerOffsetMutation::Upsert(offset) => persist_offset(&path, offset).await,
            PendingConsumerOffsetMutation::Delete => delete_persisted_offset(&path).await,
        }
    }

    fn apply_consumer_offset_commit(&self, pending: PendingConsumerOffsetCommit) {
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
            }
            PendingConsumerOffsetMutation::Delete if pending.kind == ConsumerKind::Consumer => {
                let id = pending.consumer_id;
                let guard = self.consumer_offsets.pin();
                let key = usize::try_from(id).expect("u32 consumer id must fit usize");
                let _ = guard.remove(&key);
            }
            PendingConsumerOffsetMutation::Delete
                if pending.kind == ConsumerKind::ConsumerGroup =>
            {
                let group_id = pending.consumer_id;
                let guard = self.consumer_group_offsets.pin();
                let key = ConsumerGroupId(
                    usize::try_from(group_id).expect("u32 group id must fit usize"),
                );
                let _ = guard.remove(&key);
            }
            _ => {}
        }
    }

    async fn store_consumer_offset_and_persist(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let pending = PendingConsumerOffsetCommit::try_from_polling_consumer(consumer, offset)?;
        self.persist_consumer_offset_commit(pending).await?;
        self.apply_consumer_offset_commit(pending);
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
}

impl Partition for IggyPartition {
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
        self.apply_consumer_offset_commit(pending);
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
