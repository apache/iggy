/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::streaming::partitions::COMPONENT;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy_common::{Confirmation, IggyError, IggyTimestamp, Sizeable};
use std::sync::atomic::Ordering;
use tracing::trace;

impl Partition {
    /// Retrieves messages by timestamp (up to a specified count).
    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: IggyTimestamp,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        trace!(
            "Getting {count} messages by timestamp: {} for partition: {}...",
            timestamp.as_micros(),
            self.partition_id
        );

        if self.segments.is_empty() || count == 0 {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let query_ts = timestamp.as_micros();

        let filtered_segments: Vec<&Segment> = self
            .segments
            .iter()
            .filter(|segment| segment.end_timestamp() >= query_ts)
            .collect();

        Self::get_messages_from_segments_by_timestamp(filtered_segments, query_ts, count).await
    }

    // Retrieves messages by offset (up to a specified count).
    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        trace!(
            "Getting {count} messages for start offset: {start_offset} for partition: {}, current offset: {}...",
            self.partition_id, self.current_offset
        );

        if self.segments.is_empty() || start_offset > self.current_offset || count == 0 {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let start_segment_idx = match self
            .segments
            .iter()
            .rposition(|segment| segment.start_offset() <= start_offset)
        {
            Some(idx) => idx,
            None => return Ok(IggyMessagesBatchSet::empty()),
        };

        let relevant_segments: Vec<&Segment> = self.segments[start_segment_idx..].iter().collect();

        Self::get_messages_from_segments(relevant_segments, start_offset, count).await
    }

    // Retrieves the first messages (up to a specified count).
    pub async fn get_first_messages(&self, count: u32) -> Result<IggyMessagesBatchSet, IggyError> {
        if self.segments.is_empty() {
            return Ok(IggyMessagesBatchSet::empty());
        }
        let oldest_available_offset = self.segments[0].start_offset();
        self.get_messages_by_offset(oldest_available_offset, count)
            .await
    }

    // Retrieves the last messages (up to a specified count).
    pub async fn get_last_messages(&self, count: u32) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut requested_count = count as u64;
        if requested_count > self.current_offset + 1 {
            requested_count = self.current_offset + 1
        }
        let start_offset = 1 + self.current_offset - requested_count;
        self.get_messages_by_offset(start_offset, requested_count as u32)
            .await
    }

    // Retrieves the next messages for a polling consumer (up to a specified count).
    pub async fn get_next_messages(
        &self,
        consumer: PollingConsumer,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => (&self.consumer_offsets, consumer_id),
            PollingConsumer::ConsumerGroup(group_id, _) => (&self.consumer_group_offsets, group_id),
        };

        let consumer_offset = consumer_offsets.get(&consumer_id);
        if consumer_offset.is_none() {
            trace!(
                "Consumer: {} hasn't stored offset for partition: {}, returning the first messages...",
                consumer_id, self.partition_id
            );
            return self.get_first_messages(count).await;
        }

        let consumer_offset = consumer_offset.unwrap();
        if consumer_offset.offset == self.current_offset {
            trace!(
                "Consumer: {} has the latest offset: {} for partition: {}, returning empty messages...",
                consumer_id, consumer_offset.offset, self.partition_id
            );
            return Ok(IggyMessagesBatchSet::empty());
        }

        let offset = consumer_offset.offset + 1;
        trace!(
            "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
            consumer_id, self.partition_id, offset
        );

        self.get_messages_by_offset(offset, count).await
    }

    /// Retrieves messages from multiple segments.
    async fn get_messages_from_segments(
        segments: Vec<&Segment>,
        offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut remaining_count = count;
        let mut current_offset = offset;
        let mut batches = IggyMessagesBatchSet::empty();

        for segment in segments {
            if remaining_count == 0 {
                break;
            }

            let messages = segment
            .get_messages_by_offset(current_offset, remaining_count)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get messages from segment, segment: {segment}, \
                     offset: {current_offset}, count: {remaining_count}"
                )
            })?;

            let messages_count = messages.count();
            if messages_count == 0 {
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);

            if let Some(last_offset) = messages.last_offset() {
                current_offset = last_offset + 1;
            } else if messages_count > 0 {
                current_offset += messages_count as u64;
            }

            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    /// Retrieves messages from multiple segments by timestamp.
    async fn get_messages_from_segments_by_timestamp(
        segments: Vec<&Segment>,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();

        for segment in segments {
            if remaining_count == 0 {
                break;
            }

            let messages = segment
                .get_messages_by_timestamp(timestamp, remaining_count)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to get messages from segment by timestamp, \
                         segment: {segment}, timestamp: {timestamp}, count: {remaining_count}"
                    )
                })?;

            let messages_count = messages.count();
            remaining_count = remaining_count.saturating_sub(messages_count);

            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    pub async fn append_messages(
        &mut self,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        if batch.count() == 0 {
            return Ok(());
        }

        trace!(
            "Appending {} messages of size {} to partition with ID: {}...",
            batch.count(),
            batch.get_size_bytes(),
            self.partition_id
        );

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        if last_segment.is_closed() {
            let start_offset = last_segment.end_offset() + 1;
            trace!(
                "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                start_offset, self.partition_id
            );
            self.add_persisted_segment(start_offset).await.with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to add persisted segment, partition: {self}, start offset: {start_offset}",
                ))?
        }

        let current_offset = if !self.should_increment_offset {
            0
        } else {
            self.current_offset + 1
        };

        let batch_messages_count = batch.count();
        let batch_messages_size = batch.get_size_bytes();

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        last_segment
            .append_batch(current_offset, batch, self.message_deduplicator.as_ref())
                 .await
                 .with_error_context(|error| {
                     format!(
                         "{COMPONENT} (error: {error}) - failed to append batch into last segment: {last_segment}",
                     )
                 })?;

        // Handle the case when messages_count is 0 to avoid integer underflow
        let last_offset = if batch_messages_count == 0 {
            current_offset
        } else {
            current_offset + batch_messages_count as u64 - 1
        };

        if self.should_increment_offset {
            self.current_offset = last_offset;
        } else {
            self.should_increment_offset = true;
            self.current_offset = last_offset;
        }

        self.unsaved_messages_count += batch_messages_count;
        self.unsaved_messages_size += batch_messages_size;

        let unsaved_messages_count_exceeded =
            self.unsaved_messages_count >= self.config.partition.messages_required_to_save;
        let unsaved_messages_size_exceeded =
            self.unsaved_messages_size >= self.config.partition.size_of_messages_required_to_save;

        if unsaved_messages_count_exceeded
            || unsaved_messages_size_exceeded
            || last_segment.is_full().await
        {
            trace!(
                "Segment with start offset: {} for partition with ID: {} will be persisted on disk because {}...",
                last_segment.start_offset(),
                self.partition_id,
                if unsaved_messages_count_exceeded {
                    format!(
                        "unsaved messages count exceeded: {}, max from config: {}",
                        self.unsaved_messages_count,
                        self.config.partition.messages_required_to_save
                    )
                } else if unsaved_messages_size_exceeded {
                    format!(
                        "unsaved messages size exceeded: {}, max from config: {}",
                        self.unsaved_messages_size,
                        self.config.partition.size_of_messages_required_to_save
                    )
                } else {
                    format!(
                        "segment is full, current size: {}, max from config: {}",
                        last_segment.get_messages_size(),
                        self.config.segment.size
                    )
                }
            );

            last_segment.persist_messages().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to persist messages, partition id: {}, start offset: {}",
                    self.partition_id, last_segment.start_offset()
                )
            })?;
            self.unsaved_messages_count = 0;
            self.unsaved_messages_size = 0.into();
        }

        Ok(())
    }

    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    pub async fn flush_unsaved_buffer(&mut self, fsync: bool) -> Result<(), IggyError> {
        let _fsync = fsync;
        if self.unsaved_messages_count == 0 {
            return Ok(());
        }

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        trace!(
            "Segment with start offset: {} for partition with ID: {} will be forcefully persisted on disk...",
            last_segment.start_offset(),
            self.partition_id
        );

        last_segment.persist_messages().await.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to persist messages, partition id: {}, start offset: {}",
                self.partition_id, last_segment.start_offset()
            )
        })?;

        self.unsaved_messages_count = 0;
        self.unsaved_messages_size = 0.into();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{MessageDeduplicationConfig, SystemConfig};
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use crate::streaming::utils::MemoryPool;
    use bytes::Bytes;
    use iggy_common::{IggyExpiry, IggyMessage};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use tempfile::TempDir;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(false).await;
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.count(), messages_count);
    }

    #[tokio::test]
    async fn given_enabled_message_deduplication_only_messages_with_unique_id_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(true).await;
        let messages = create_messages();
        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
        let messages_count = messages.len() as u32;
        assert_eq!(batch.count(), messages_count);
        let unique_messages_count = 3;

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.count(), unique_messages_count);
    }

    #[tokio::test]
    async fn duplicates_at_beginning_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // First and second messages are duplicates
        let messages = vec![
            create_message(1, "message 1"),
            create_message(1, "message 1 - duplicate"),
            create_message(2, "message 2"),
            create_message(3, "message 3"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check first message (should be the first occurrence of ID 1)
        let first_message = loaded_messages.get(0).unwrap();
        assert_eq!(first_message.header().id(), 1);
        assert_eq!(first_message.payload(), b"message 1");
    }

    #[tokio::test]
    async fn duplicates_in_middle_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Middle messages (ID 2) are duplicates
        let messages = vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(2, "message 2 - duplicate"),
            create_message(3, "message 3"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check second message (should be the first occurrence of ID 2)
        let second_message = loaded_messages.get(1).unwrap();
        assert_eq!(second_message.header().id(), 2);
        assert_eq!(second_message.payload(), b"message 2");
    }

    #[tokio::test]
    async fn duplicates_at_end_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Last message is a duplicate
        let messages = vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(3, "message 3"),
            create_message(3, "message 3 - duplicate"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check last message (should be the first occurrence of ID 3)
        let last_message = loaded_messages.get(2).unwrap();
        assert_eq!(last_message.header().id(), 3);
        assert_eq!(last_message.payload(), b"message 3");
    }

    #[tokio::test]
    async fn interleaved_duplicates_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Every other message is a duplicate
        let messages = vec![
            create_message(1, "message 1"),
            create_message(1, "message 1 - duplicate"),
            create_message(2, "message 2"),
            create_message(2, "message 2 - duplicate"),
            create_message(3, "message 3"),
            create_message(3, "message 3 - duplicate"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check message content and order
        let first_message = loaded_messages.get(0).unwrap();
        assert_eq!(first_message.header().id(), 1);
        assert_eq!(first_message.payload(), b"message 1");

        let second_message = loaded_messages.get(1).unwrap();
        assert_eq!(second_message.header().id(), 2);
        assert_eq!(second_message.payload(), b"message 2");

        let third_message = loaded_messages.get(2).unwrap();
        assert_eq!(third_message.header().id(), 3);
        assert_eq!(third_message.payload(), b"message 3");
    }

    #[tokio::test]
    async fn all_duplicate_messages_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Add some initial messages
        let initial_messages = vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(3, "message 3"),
        ];

        let initial_size = initial_messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let initial_batch = IggyMessagesBatchMut::from_messages(&initial_messages, initial_size);
        partition
            .append_messages(initial_batch)
            .await
            .unwrap();

        // Now try to add only duplicates
        let duplicate_messages = vec![
            create_message(1, "message 1 - duplicate"),
            create_message(2, "message 2 - duplicate"),
            create_message(3, "message 3 - duplicate"),
        ];

        let duplicate_size = duplicate_messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let duplicate_batch =
            IggyMessagesBatchMut::from_messages(&duplicate_messages, duplicate_size);
        partition
            .append_messages(duplicate_batch)
            .await
            .unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Still only 3 unique messages should be stored (the originals)
        assert_eq!(loaded_messages.count(), 3);
    }

    #[tokio::test]
    async fn multiple_consecutive_duplicates_should_be_filtered() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Multiple consecutive duplicates of the same ID
        let messages = vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(2, "message 2 - duplicate 1"),
            create_message(2, "message 2 - duplicate 2"),
            create_message(2, "message 2 - duplicate 3"),
            create_message(3, "message 3"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check second message (should be the first occurrence of ID 2)
        let second_message = loaded_messages.get(1).unwrap();
        assert_eq!(second_message.header().id(), 2);
        assert_eq!(second_message.payload(), b"message 2");
    }

    #[tokio::test]
    async fn deduplication_across_multiple_append_operations() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // First batch
        let batch1 = vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
        ];

        let batch1_size = batch1
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch1 = IggyMessagesBatchMut::from_messages(&batch1, batch1_size);
        partition.append_messages(batch1).await.unwrap();

        // Second batch with mix of new and duplicate messages
        let batch2 = vec![
            create_message(2, "message 2 - duplicate"), // Duplicate
            create_message(3, "message 3"),             // New
            create_message(1, "message 1 - duplicate"), // Duplicate
        ];

        let batch2_size = batch2
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch2 = IggyMessagesBatchMut::from_messages(&batch2, batch2_size);
        partition.append_messages(batch2).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // Only 3 unique messages should be stored
        assert_eq!(loaded_messages.count(), 3);

        // Check the message order and content
        let first_message = loaded_messages.get(0).unwrap();
        assert_eq!(first_message.header().id(), 1);
        assert_eq!(first_message.payload(), b"message 1");

        let second_message = loaded_messages.get(1).unwrap();
        assert_eq!(second_message.header().id(), 2);
        assert_eq!(second_message.payload(), b"message 2");

        let third_message = loaded_messages.get(2).unwrap();
        assert_eq!(third_message.header().id(), 3);
        assert_eq!(third_message.payload(), b"message 3");
    }

    #[tokio::test]
    async fn zero_id_messages_should_not_be_deduplicated() {
        let (mut partition, _tempdir) = create_partition(true).await;

        // Messages with ID 0 (should not be deduplicated as 0 is a special case)
        let messages = vec![
            create_message(0, "message with zero ID 1"),
            create_message(0, "message with zero ID 2"),
            create_message(1, "message 1"),
            create_message(0, "message with zero ID 3"),
        ];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch).await.unwrap();

        let loaded_messages = partition.get_messages_by_offset(0, 10).await.unwrap();

        // All 4 messages should be stored (ID 0 messages are not deduplicated)
        // Note: This assumes the current behavior where ID 0 is treated specially.
        // If that's not the case, this test needs adjustment.
        assert_eq!(loaded_messages.count(), 4);
    }

    async fn create_partition(deduplication_enabled: bool) -> (Partition, TempDir) {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let with_segment = true;
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: temp_dir.path().to_path_buf().to_str().unwrap().to_string(),
            message_deduplication: MessageDeduplicationConfig {
                enabled: deduplication_enabled,
                ..Default::default()
            },
            ..Default::default()
        });
        let storage = Rc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        MemoryPool::init_pool(config.clone());

        (
            Partition::create(
                stream_id,
                topic_id,
                partition_id,
                with_segment,
                config,
                storage,
                IggyExpiry::NeverExpire,
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU32::new(0)),
                IggyTimestamp::now(),
            )
            .await,
            temp_dir,
        )
    }

    fn create_messages() -> Vec<IggyMessage> {
        vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(3, "message 3"),
            create_message(2, "message 3.2"),
            create_message(1, "message 1.2"),
            create_message(3, "message 3.3"),
        ]
    }

    fn create_message(id: u128, payload: &str) -> IggyMessage {
        IggyMessage::builder()
            .id(id)
            .payload(Bytes::from(payload.to_string()))
            .build()
            .expect("Failed to create message with ID")
    }
}
