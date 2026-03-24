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
use crate::{
    AppendResult, Partition, PartitionOffsets, PolledBatches, PollingArgs, PollingConsumer,
    send_messages2::stamp_prepare_for_persistence,
};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyTimestamp, PartitionStats, PollingKind,
    header::{Operation, PrepareHeader},
    message::Message,
};
use journal::Journal as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
#[derive(Debug)]
pub struct IggyPartition {
    pub log: SegmentedLog<PartitionJournal<PartitionJournalMemStorage>, PartitionJournalMemStorage>,
    /// Committed offset — advanced only after quorum ack.
    pub offset: Arc<AtomicU64>,
    /// Dirty offset — advanced on every prepare (before commit).
    /// Used to assign offsets during `prepare_for_persistence`.
    pub dirty_offset: AtomicU64,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    pub stats: Arc<PartitionStats>,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub should_increment_offset: bool,
    pub write_lock: Arc<TokioMutex<()>>,
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
            u32::try_from(batch.total_size()).map_err(|_| IggyError::CannotAppendMessage)?;

        let last_dirty_offset = if batch_messages_count == 0 {
            dirty_offset
        } else {
            dirty_offset + u64::from(batch_messages_count) - 1
        };

        if !self.should_increment_offset {
            self.should_increment_offset = true;
        }
        self.dirty_offset
            .store(last_dirty_offset, Ordering::Relaxed);

        let segment_index = self.log.segments().len() - 1;
        self.log.segments_mut()[segment_index].current_position += batch_messages_size;

        let journal = self.log.journal_mut();
        journal.info.messages_count += batch_messages_count;
        journal.info.size += IggyByteSize::from(u64::from(batch_messages_size));
        journal.info.current_offset = last_dirty_offset;
        if journal.info.first_timestamp == 0 {
            journal.info.first_timestamp = batch.base_timestamp;
        }
        journal.info.end_timestamp = batch.base_timestamp;
        journal.info.max_timestamp = journal.info.max_timestamp.max(batch.base_timestamp);
        journal.inner.append(message).await;

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
    ) -> Result<PolledBatches<4096>, IggyError> {
        if !self.should_increment_offset || args.count == 0 {
            return Ok(PolledBatches::empty());
        }

        let write_offset = self.dirty_offset.load(Ordering::Relaxed);

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
                    return Ok(PolledBatches::empty());
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

        let messages = result.unwrap_or_else(PolledBatches::empty);

        if args.auto_commit && !messages.is_empty() {
            let last_offset = messages
                .last_matching_offset
                .expect("non-empty poll result must have a last offset");
            if let Err(err) = self.store_consumer_offset(consumer, last_offset) {
                // warning for now.
                warn!(
                    consumer = ?consumer,
                    last_offset,
                    %err,
                    "poll_messages: failed to store consumer offset"
                );
            }
        }

        Ok(messages)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn store_consumer_offset(
        &self,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        match consumer {
            PollingConsumer::Consumer(id, _) => {
                let guard = self.consumer_offsets.pin();
                if let Some(existing) = guard.get(&id) {
                    existing.offset.store(offset, Ordering::Relaxed);
                } else {
                    guard.insert(
                        id,
                        ConsumerOffset::new(
                            ConsumerKind::Consumer,
                            id as u32,
                            offset,
                            String::new(),
                        ),
                    );
                }
            }
            PollingConsumer::ConsumerGroup(group_id, _) => {
                let guard = self.consumer_group_offsets.pin();
                let key = ConsumerGroupId(group_id);
                if let Some(existing) = guard.get(&key) {
                    existing.offset.store(offset, Ordering::Relaxed);
                } else {
                    guard.insert(
                        key,
                        ConsumerOffset::new(
                            ConsumerKind::ConsumerGroup,
                            group_id as u32,
                            offset,
                            String::new(),
                        ),
                    );
                }
            }
        }
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
            self.offset.load(Ordering::Relaxed),
            self.dirty_offset.load(Ordering::Relaxed),
        )
    }
}
