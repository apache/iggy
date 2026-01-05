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

use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    shard::system::messages::PollingArgs,
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            journal::{Journal, MemoryMessageJournal},
            log::SegmentedLog,
            partition::{ConsumerGroupOffsets, ConsumerOffsets},
        },
        polling_consumer::PollingConsumer,
        segments::IggyMessagesBatchSet,
        stats::PartitionStats,
    },
};
use err_trail::ErrContext;
use iggy_common::{IggyError, IggyTimestamp, PollingKind};
use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Per-partition data stored in the shard's local store.
///
/// This struct holds all the mutable state for a single partition:
/// - The segmented log containing messages
/// - Current offset tracking
/// - Consumer and consumer group offsets
/// - Optional message deduplication state
/// - Statistics reference from SharedStatsStore
#[derive(Debug)]
pub struct PartitionData {
    /// The segmented log containing messages
    pub log: SegmentedLog<MemoryMessageJournal>,

    /// Current message offset (monotonically increasing)
    pub offset: Arc<AtomicU64>,

    /// Per-consumer offset tracking
    pub consumer_offsets: Arc<ConsumerOffsets>,

    /// Per-consumer-group offset tracking
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,

    /// Optional message deduplicator (when deduplication is enabled)
    pub message_deduplicator: Option<Arc<MessageDeduplicator>>,

    /// Reference to partition stats in SharedStatsStore
    pub stats: Arc<PartitionStats>,

    /// When this partition data was created
    pub created_at: IggyTimestamp,

    /// Whether offset should be incremented on append
    pub should_increment_offset: bool,
}

impl PartitionData {
    /// Create new partition data with the given components.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log: SegmentedLog<MemoryMessageJournal>,
        offset: Arc<AtomicU64>,
        consumer_offsets: Arc<ConsumerOffsets>,
        consumer_group_offsets: Arc<ConsumerGroupOffsets>,
        message_deduplicator: Option<Arc<MessageDeduplicator>>,
        stats: Arc<PartitionStats>,
        created_at: IggyTimestamp,
        should_increment_offset: bool,
    ) -> Self {
        Self {
            log,
            offset,
            consumer_offsets,
            consumer_group_offsets,
            message_deduplicator,
            stats,
            created_at,
            should_increment_offset,
        }
    }

    /// Get current offset value.
    #[inline]
    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Purges all data from this partition, resetting it to empty state.
    /// Called when purge_topic is executed to reset partition_store data.
    pub fn purge(&mut self) {
        // Reset offset to 0
        self.offset.store(0, Ordering::Relaxed);

        // Clear the journal (in-memory messages not yet persisted)
        self.log.journal_mut().clear();

        // Clear the log (segments, storage, indexes)
        self.log.segments_mut().clear();
        self.log.storages_mut().clear();
        self.log.indexes_mut().clear();

        // Clear consumer offsets
        self.consumer_offsets.pin().clear();
        self.consumer_group_offsets.pin().clear();

        // Reset should_increment_offset since partition is now empty
        self.should_increment_offset = false;

        // Zero out stats
        self.stats.zero_out_all();
    }

    /// Find segment indices containing messages starting from the given offset.
    fn get_segment_range_by_offset(&self, offset: u64) -> Range<usize> {
        let segments = self.log.segments();

        if segments.is_empty() {
            return 0..0;
        }

        let start = segments
            .iter()
            .rposition(|segment| segment.start_offset <= offset)
            .unwrap_or(0);

        let end = segments.len();
        start..end
    }

    /// Find segment indices containing messages starting from the given timestamp.
    fn get_segment_range_by_timestamp(&self, timestamp: u64) -> Result<Range<usize>, IggyError> {
        let segments = self.log.segments();

        if segments.is_empty() {
            return Ok(0..0);
        }

        let start = segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.end_timestamp >= timestamp)
            .map(|(index, _)| index)
            .next()
            .ok_or(IggyError::TimestampOutOfRange(timestamp))?;

        let end = segments.len();
        Ok(start..end)
    }

    /// Get messages by offset from this partition.
    pub async fn get_messages_by_offset(
        &self,
        offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        let current_offset = self.current_offset();
        if offset > current_offset {
            return Ok(IggyMessagesBatchSet::default());
        }

        let range = self.get_segment_range_by_offset(offset);

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();
        let mut current_offset_pos = offset;

        for idx in range {
            if remaining_count == 0 {
                break;
            }

            let segment = &self.log.segments()[idx];
            let segment_start_offset = segment.start_offset;
            let segment_end_offset = segment.end_offset;

            let read_offset = if current_offset_pos < segment_start_offset {
                segment_start_offset
            } else {
                current_offset_pos
            };

            let end_offset =
                std::cmp::min(read_offset + remaining_count as u64 - 1, segment_end_offset);

            let segment_batches = self
                .get_messages_by_offset_base(
                    idx,
                    read_offset,
                    end_offset,
                    remaining_count,
                    segment_start_offset,
                )
                .await?;

            let batch_count = segment_batches.count();
            if batch_count > 0 {
                remaining_count = remaining_count.saturating_sub(batch_count);
                current_offset_pos = segment_end_offset + 1;
                batches.add_batch_set(segment_batches);
            }
        }

        Ok(batches)
    }

    /// Get messages from a specific segment, handling journal/disk split.
    async fn get_messages_by_offset_base(
        &self,
        idx: usize,
        offset: u64,
        end_offset: u64,
        count: u32,
        segment_start_offset: u64,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let journal = self.log.journal();
        let is_journal_empty = journal.is_empty();
        let journal_first_offset = journal.inner().base_offset;
        let journal_last_offset = journal.inner().current_offset;

        // Case 0: Journal is empty, all messages on disk
        if is_journal_empty {
            return self
                .load_messages_from_disk_by_offset(idx, offset, count, segment_start_offset)
                .await;
        }

        // Case 1: All messages in journal
        if offset >= journal_first_offset && end_offset <= journal_last_offset {
            let batches = journal.get(|batches| batches.get_by_offset(offset, count));
            return Ok(batches);
        }

        // Case 2: All messages on disk
        if end_offset < journal_first_offset {
            return self
                .load_messages_from_disk_by_offset(idx, offset, count, segment_start_offset)
                .await;
        }

        // Case 3: Messages span disk and journal boundary
        let disk_count = if offset < journal_first_offset {
            ((journal_first_offset - offset) as u32).min(count)
        } else {
            0
        };

        let mut combined_batch_set = IggyMessagesBatchSet::empty();

        // Load from disk if needed
        if disk_count > 0 {
            let disk_messages = self
                .load_messages_from_disk_by_offset(idx, offset, disk_count, segment_start_offset)
                .await
                .error(|e: &IggyError| {
                    format!(
                        "Failed to load messages from disk, start offset: {offset}, count: {disk_count}, error: {e}"
                    )
                })?;

            if !disk_messages.is_empty() {
                combined_batch_set.add_batch_set(disk_messages);
            }
        }

        // Load remaining from journal
        let remaining_count = count - combined_batch_set.count();
        if remaining_count > 0 {
            let journal_start_offset = std::cmp::max(offset, journal_first_offset);
            let journal_messages =
                journal.get(|batches| batches.get_by_offset(journal_start_offset, remaining_count));

            if !journal_messages.is_empty() {
                combined_batch_set.add_batch_set(journal_messages);
            }
        }

        Ok(combined_batch_set)
    }

    /// Load messages from disk by offset.
    async fn load_messages_from_disk_by_offset(
        &self,
        idx: usize,
        start_offset: u64,
        count: u32,
        segment_start_offset: u64,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let relative_start_offset = (start_offset - segment_start_offset) as u32;

        let storage = &self.log.storages()[idx];
        let index_reader = storage
            .index_reader
            .as_ref()
            .expect("Index reader not initialized");
        let messages_reader = storage
            .messages_reader
            .as_ref()
            .expect("Messages reader not initialized");

        // Try to use cached indexes first
        let indexes = self.log.indexes()[idx].as_ref().and_then(|indexes| {
            let sliced = indexes.slice_by_offset(relative_start_offset, count);
            sliced.filter(|s| !s.is_empty())
        });

        let indexes_to_read = match indexes {
            Some(indexes) => Some(indexes),
            None => {
                index_reader
                    .load_from_disk_by_offset(relative_start_offset, count)
                    .await?
            }
        };

        let Some(indexes_to_read) = indexes_to_read else {
            return Ok(IggyMessagesBatchSet::empty());
        };

        let batch = messages_reader
            .load_messages_from_disk(indexes_to_read)
            .await
            .error(|e: &IggyError| format!("Failed to load messages from disk: {e}"))?;

        batch
            .validate_checksums_and_offsets(start_offset)
            .error(|e: &IggyError| {
                format!("Failed to validate messages read from disk! error: {e}")
            })?;

        Ok(IggyMessagesBatchSet::from(batch))
    }

    /// Get messages by timestamp from this partition.
    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        let Ok(range) = self.get_segment_range_by_timestamp(timestamp) else {
            return Ok(IggyMessagesBatchSet::default());
        };

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();

        for idx in range {
            if remaining_count == 0 {
                break;
            }

            let segment = &self.log.segments()[idx];
            if segment.end_timestamp < timestamp {
                continue;
            }

            let messages = self
                .get_messages_by_timestamp_base(idx, timestamp, remaining_count)
                .await?;

            let messages_count = messages.count();
            if messages_count == 0 {
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);
            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    /// Get messages from a specific segment by timestamp.
    async fn get_messages_by_timestamp_base(
        &self,
        idx: usize,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        let journal = self.log.journal();
        let is_journal_empty = journal.is_empty();
        let journal_first_timestamp = journal.inner().first_timestamp;
        let journal_last_timestamp = journal.inner().end_timestamp;

        // Case 0: Journal is empty, all on disk
        if is_journal_empty {
            return self
                .load_messages_from_disk_by_timestamp(idx, timestamp, count)
                .await;
        }

        // Case 1: Timestamp after journal ends
        if timestamp > journal_last_timestamp {
            return Ok(IggyMessagesBatchSet::empty());
        }

        // Case 1b: Timestamp within journal range
        if timestamp >= journal_first_timestamp {
            let batches = journal.get(|batches| batches.get_by_timestamp(timestamp, count));
            return Ok(batches);
        }

        // Case 2: Load from disk first
        let disk_messages = self
            .load_messages_from_disk_by_timestamp(idx, timestamp, count)
            .await?;

        if disk_messages.count() >= count {
            return Ok(disk_messages);
        }

        // Case 3: Span disk and journal
        let remaining_count = count - disk_messages.count();
        let journal_messages =
            journal.get(|batches| batches.get_by_timestamp(timestamp, remaining_count));

        let mut combined_batch_set = disk_messages;
        if !journal_messages.is_empty() {
            combined_batch_set.add_batch_set(journal_messages);
        }
        Ok(combined_batch_set)
    }

    /// Load messages from disk by timestamp.
    async fn load_messages_from_disk_by_timestamp(
        &self,
        idx: usize,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let storage = &self.log.storages()[idx];
        let index_reader = storage
            .index_reader
            .as_ref()
            .expect("Index reader not initialized");
        let messages_reader = storage
            .messages_reader
            .as_ref()
            .expect("Messages reader not initialized");

        // Try cached indexes first
        let indexes = self.log.indexes()[idx].as_ref().and_then(|indexes| {
            let sliced = indexes.slice_by_timestamp(timestamp, count);
            sliced.filter(|s| !s.is_empty())
        });

        let indexes_to_read = match indexes {
            Some(indexes) => Some(indexes),
            None => {
                index_reader
                    .load_from_disk_by_timestamp(timestamp, count)
                    .await?
            }
        };

        let Some(indexes_to_read) = indexes_to_read else {
            return Ok(IggyMessagesBatchSet::empty());
        };

        let batch = messages_reader
            .load_messages_from_disk(indexes_to_read)
            .await
            .error(|e: &IggyError| {
                format!("Failed to load messages from disk by timestamp: {e}")
            })?;

        Ok(IggyMessagesBatchSet::from(batch))
    }

    /// Poll messages from this partition using the given consumer and polling strategy.
    pub async fn poll_messages(
        &self,
        partition_id: usize,
        consumer: PollingConsumer,
        args: PollingArgs,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        // Use shared stats offset for cross-shard consistency (purge updates this)
        let current_offset = self.stats.current_offset();
        let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);
        let count = args.count;
        let strategy = args.strategy;
        let value = strategy.value;

        let batches = match strategy.kind {
            PollingKind::Offset => {
                let offset = value;
                if offset > current_offset {
                    return Ok((metadata, IggyMessagesBatchSet::default()));
                }
                self.get_messages_by_offset(offset, count).await?
            }
            PollingKind::Timestamp => {
                let timestamp = value;
                self.get_messages_by_timestamp(timestamp, count).await?
            }
            PollingKind::First => {
                let first_offset = self
                    .log
                    .segments()
                    .first()
                    .map(|segment| segment.start_offset)
                    .unwrap_or(0);
                self.get_messages_by_offset(first_offset, count).await?
            }
            PollingKind::Last => {
                let mut requested_count = count as u64;
                if requested_count > current_offset + 1 {
                    requested_count = current_offset + 1;
                }
                let start_offset = 1 + current_offset - requested_count;
                self.get_messages_by_offset(start_offset, requested_count as u32)
                    .await?
            }
            PollingKind::Next => {
                let consumer_offset = match consumer {
                    PollingConsumer::Consumer(consumer_id, _) => self
                        .consumer_offsets
                        .pin()
                        .get(&consumer_id)
                        .map(|item| item.offset.load(Ordering::Relaxed)),
                    PollingConsumer::ConsumerGroup(consumer_group_id, _) => self
                        .consumer_group_offsets
                        .pin()
                        .get(&consumer_group_id)
                        .map(|item| item.offset.load(Ordering::Relaxed)),
                };

                match consumer_offset {
                    None => self.get_messages_by_offset(0, count).await?,
                    Some(offset) => {
                        let next_offset = offset + 1;
                        self.get_messages_by_offset(next_offset, count).await?
                    }
                }
            }
        };

        Ok((metadata, batches))
    }

    /// Store consumer offset for the given consumer.
    pub async fn store_consumer_offset(
        &self,
        _config: &crate::configs::system::SystemConfig,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        use crate::streaming::partitions::consumer_offset::ConsumerOffset;
        use iggy_common::ConsumerKind;

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                let consumer_offsets = self.consumer_offsets.pin();
                if let Some(entry) = consumer_offsets.get(&consumer_id) {
                    entry.offset.store(offset, Ordering::Relaxed);
                } else {
                    drop(consumer_offsets);
                    self.consumer_offsets.pin().insert(
                        consumer_id,
                        ConsumerOffset::new(
                            ConsumerKind::Consumer,
                            consumer_id as u32,
                            offset,
                            String::new(), // Path not used for in-memory offsets
                        ),
                    );
                }
            }
            PollingConsumer::ConsumerGroup(group_id, _) => {
                let group_offsets = self.consumer_group_offsets.pin();
                if let Some(entry) = group_offsets.get(&group_id) {
                    entry.offset.store(offset, Ordering::Relaxed);
                } else {
                    drop(group_offsets);
                    self.consumer_group_offsets.pin().insert(
                        group_id,
                        ConsumerOffset::new(
                            ConsumerKind::ConsumerGroup,
                            group_id.0 as u32,
                            offset,
                            String::new(), // Path not used for in-memory offsets
                        ),
                    );
                }
            }
        }
        Ok(())
    }

    /// Persist messages from journal to disk.
    pub async fn persist_messages(
        &mut self,
        _reason: &str,
        config: &crate::configs::system::SystemConfig,
    ) -> Result<u32, IggyError> {
        let batch_set = self.log.journal_mut().commit();
        if batch_set.is_empty() {
            return Ok(0);
        }

        let batch_count = batch_set.count();
        self.persist_messages_to_disk(batch_set, config).await?;
        Ok(batch_count)
    }

    /// Persist committed batches to disk.
    async fn persist_messages_to_disk(
        &mut self,
        batch_set: IggyMessagesBatchSet,
        _config: &crate::configs::system::SystemConfig,
    ) -> Result<(), IggyError> {
        if batch_set.is_empty() {
            return Ok(());
        }

        let storage = self.log.active_storage();

        // Write messages to disk
        if let Some(writer) = storage.messages_writer.as_ref() {
            writer.save_batch_set(batch_set).await?;
        }

        Ok(())
    }

    /// Fsync all messages to disk.
    pub async fn fsync_all_messages(&self) -> Result<(), IggyError> {
        let storage = self.log.active_storage();
        if let Some(writer) = storage.messages_writer.as_ref() {
            writer.fsync().await?;
        }
        if let Some(index_writer) = storage.index_writer.as_ref() {
            index_writer.fsync().await?;
        }
        Ok(())
    }
}
