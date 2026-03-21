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
    AppendResult, Partition, PartitionOffsets, PollingArgs, PollingConsumer,
    decode_send_messages_batch,
};
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
    IggyByteSize, IggyError, IggyMessagesBatchMut, IggyMessagesBatchSet, IggyTimestamp,
    PartitionStats, PollingKind,
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
    fn prepare_message_from_batch(
        mut header: PrepareHeader,
        batch: &IggyMessagesBatchMut,
    ) -> Message<PrepareHeader> {
        let indexes = batch.indexes();
        let count = batch.count();
        let body_len = 4 + indexes.len() + batch.len();
        let total_size = std::mem::size_of::<PrepareHeader>() + body_len;
        header.size = u32::try_from(total_size)
            .expect("prepare_message_from_batch: batch size exceeds u32::MAX");

        let message = Message::<PrepareHeader>::new(total_size).transmute_header(|_old, new| {
            *new = header;
        });

        let mut bytes = message
            .into_inner()
            .try_into_mut()
            .expect("prepare_message_from_batch: expected unique bytes buffer");
        let header_size = std::mem::size_of::<PrepareHeader>();
        bytes[header_size..header_size + 4].copy_from_slice(&count.to_le_bytes());
        let mut position = header_size + 4;
        bytes[position..position + indexes.len()].copy_from_slice(indexes);
        position += indexes.len();
        bytes[position..position + batch.len()].copy_from_slice(batch);

        Message::<PrepareHeader>::from_bytes(bytes.freeze())
            .expect("prepare_message_from_batch: invalid prepared message bytes")
    }

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

        let mut batch = decode_send_messages_batch(message.body_bytes())
            .ok_or(IggyError::CannotAppendMessage)?;

        if batch.count() == 0 {
            return Ok(AppendResult::new(0, 0, 0));
        }

        let dirty_offset = if self.should_increment_offset {
            self.dirty_offset.load(Ordering::Relaxed) + 1
        } else {
            0
        };

        let segment = self.log.active_segment();
        let segment_start_offset = segment.start_offset;
        let current_position = segment.current_position;

        batch
            .prepare_for_persistence(segment_start_offset, dirty_offset, current_position, None)
            .await;

        let batch_messages_count = batch.count();
        let batch_messages_size = batch.size();

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
        if let Some(ts) = batch.first_timestamp()
            && journal.info.first_timestamp == 0
        {
            journal.info.first_timestamp = ts;
        }
        if let Some(ts) = batch.last_timestamp() {
            journal.info.end_timestamp = ts;
        }

        let message = Self::prepare_message_from_batch(header, &batch);
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
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if !self.should_increment_offset || args.count == 0 {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let committed_offset = self.offset.load(Ordering::Relaxed);

        let start_offset = match args.strategy.kind {
            PollingKind::Offset => args.strategy.value,
            PollingKind::First => 0,
            PollingKind::Last => committed_offset.saturating_sub(u64::from(args.count) - 1),
            PollingKind::Timestamp => {
                let result = self
                    .log
                    .journal()
                    .inner
                    .get(&MessageLookup::Timestamp {
                        timestamp: args.strategy.value,
                        count: args.count,
                    })
                    .await;
                let batch_set = result.unwrap_or_else(IggyMessagesBatchSet::empty);
                if let Some(first) = batch_set.first_offset() {
                    if first > committed_offset {
                        return Ok(IggyMessagesBatchSet::empty());
                    }
                    let max_count = u32::try_from(committed_offset - first + 1).unwrap_or(u32::MAX);
                    return Ok(batch_set.get_by_offset(first, batch_set.count().min(max_count)));
                }
                return Ok(batch_set);
            }
            PollingKind::Next => self
                .get_consumer_offset(consumer)
                .map_or(0, |offset| offset + 1),
        };

        if start_offset > committed_offset {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let max_count = u32::try_from(committed_offset - start_offset + 1).unwrap_or(u32::MAX);
        let count = args.count.min(max_count);

        let result = self
            .log
            .journal()
            .inner
            .get(&MessageLookup::Offset {
                offset: start_offset,
                count,
            })
            .await;

        let batch_set = result.unwrap_or_else(IggyMessagesBatchSet::empty);

        if args.auto_commit && !batch_set.is_empty() {
            let last_offset = start_offset + u64::from(batch_set.count()) - 1;
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

        Ok(batch_set)
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
