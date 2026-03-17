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

use crate::journal::{PartitionJournal, PartitionJournalMemStorage};
use crate::log::SegmentedLog;
use crate::{AppendResult, Partition, decode_send_messages_batch};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, IggyError, IggyMessagesBatchMut,
    IggyTimestamp, PartitionStats,
    header::{Operation, PrepareHeader},
    message::Message,
};
use journal::Journal as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;

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
}
