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

use crate::journal::{Noop, PartitionJournal};
use crate::log::SegmentedLog;
use crate::{AppendResult, Partition};
use iggy_common::{
    ConsumerGroupOffsets, ConsumerOffsets, IggyByteSize, IggyError, IggyMessagesBatchMut,
    IggyTimestamp, PartitionStats,
};
use journal::Journal as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;

// This struct aliases in terms of the code contained the `LocalPartition from `core/server/src/streaming/partitions/local_partition.rs`.
#[derive(Debug)]
pub struct IggyPartition {
    pub log: SegmentedLog<PartitionJournal, Noop>,
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
        mut batch: IggyMessagesBatchMut,
    ) -> Result<AppendResult, IggyError> {
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
            dirty_offset + batch_messages_count as u64 - 1
        };

        if self.should_increment_offset {
            self.dirty_offset
                .store(last_dirty_offset, Ordering::Relaxed);
        } else {
            self.should_increment_offset = true;
            self.dirty_offset
                .store(last_dirty_offset, Ordering::Relaxed);
        }

        let segment_index = self.log.segments().len() - 1;
        self.log.segments_mut()[segment_index].current_position += batch_messages_size;

        let journal = self.log.journal_mut();
        journal.info.messages_count += batch_messages_count;
        journal.info.size += IggyByteSize::from(batch_messages_size as u64);
        journal.info.current_offset = last_dirty_offset;
        if let Some(ts) = batch.first_timestamp()
            && journal.info.first_timestamp == 0
        {
            journal.info.first_timestamp = ts;
        }
        if let Some(ts) = batch.last_timestamp() {
            journal.info.end_timestamp = ts;
        }

        journal.inner.append(batch).await;

        Ok(AppendResult::new(
            dirty_offset,
            last_dirty_offset,
            batch_messages_count,
        ))
    }
}
