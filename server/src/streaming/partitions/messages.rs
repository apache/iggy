use crate::streaming::batching::appendable_batch_info::AppendableBatchInfo;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::partitions::COMPONENT;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::messages::send_messages::Message;
use iggy::models::batch::IggyMutableBatch;
use iggy::utils::timestamp::IggyTimestamp;
use iggy::{confirmation::Confirmation, models::batch::IggyBatch};
use std::sync::{atomic::Ordering, Arc};
use tracing::{trace, warn};

impl Partition {
    /// Retrieves messages by timestamp (up to a specified count).
    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: IggyTimestamp,
        count: u32,
    ) -> Result<Vec<Arc<()>>, IggyError> {
        //TODO: Fix me
        /*
        trace!(
            "Getting messages by timestamp: {} for partition: {}...",
            timestamp,
            self.partition_id
        );

        if self.segments.is_empty() || count == 0 {
            return Ok(Vec::new());
        }

        let query_ts = timestamp.as_micros();
        let mut messages = Vec::new();
        let mut remaining = count as usize;

        for segment in &self.segments {
            if segment.end_timestamp < query_ts {
                continue;
            }

            let segment_messages = segment
                .get_messages_by_timestamp(query_ts, remaining)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to get messages from segment, \
                        partition: {}, segment start: {}, end: {}",
                        self, segment.start_offset, segment.end_offset
                    )
                })?;

            let num_messages = segment_messages.len();
            messages.extend(segment_messages);
            remaining -= num_messages;

            if remaining == 0 {
                break;
            }
        }

        Ok(messages)
        */
        todo!()
    }

    // Retrieves messages by offset (up to a specified count).
    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyBatchFetchResult, IggyError> {
        trace!(
            "Getting messages for start offset: {start_offset} for partition: {}, current offset: {}...",
            self.partition_id,
            self.current_offset
        );
        //TODO: Fix me
        /*
        if self.segments.is_empty() || start_offset > self.current_offset {
            return Ok(Vec::new());
        }
        */

        let end_offset = self.get_end_offset(start_offset, count);
        // TODO: Most likely don't need to find the specific range of segments, just find the first segment containing the first offset
        // and during reads roll to the next one, when the first is exhausted.
        let segments = self.filter_segments_by_offsets(start_offset, end_offset);
        match segments.len() {
            0 => panic!("TODO"),
            1 => {
                let slices = segments[0]
                    .get_messages_by_offset(start_offset, count)
                    .await?;
                Ok(slices.into())
            }
            _ => {
                let slices =
                    Self::get_messages_from_segments(segments, start_offset, count).await?;
                Ok(slices.into())
            }
        }
    }

    // Retrieves the first messages (up to a specified count).
    pub async fn get_first_messages(&self, count: u32) -> Result<IggyBatchFetchResult, IggyError> {
        self.get_messages_by_offset(0, count).await
    }

    // Retrieves the last messages (up to a specified count).
    pub async fn get_last_messages(&self, count: u32) -> Result<IggyBatchFetchResult, IggyError> {
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
    ) -> Result<IggyBatchFetchResult, IggyError> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => (&self.consumer_offsets, consumer_id),
            PollingConsumer::ConsumerGroup(group_id, _) => (&self.consumer_group_offsets, group_id),
        };

        let consumer_offset = consumer_offsets.get(&consumer_id);
        if consumer_offset.is_none() {
            trace!(
                "Consumer: {} hasn't stored offset for partition: {}, returning the first messages...",
                consumer_id,
                self.partition_id
            );
            return self.get_first_messages(count).await;
        }

        let consumer_offset = consumer_offset.unwrap();
        if consumer_offset.offset == self.current_offset {
            trace!(
                "Consumer: {} has the latest offset: {} for partition: {}, returning empty messages...",
                consumer_id,
                consumer_offset.offset,
                self.partition_id
            );
            //TODO: Fix me
            //return Ok(Vec::new());
        }

        let offset = consumer_offset.offset + 1;
        trace!(
            "Getting next messages for {} for partition: {} from offset: {}...",
            consumer_id,
            self.partition_id,
            offset
        );

        self.get_messages_by_offset(offset, count).await
    }

    fn get_end_offset(&self, offset: u64, count: u32) -> u64 {
        let mut end_offset = offset + (count - 1) as u64;
        let segment = self.segments.last().unwrap();
        let max_offset = segment.current_offset;
        if end_offset > max_offset {
            end_offset = max_offset;
        }

        end_offset
    }

    fn filter_segments_by_offsets(&self, start_offset: u64, end_offset: u64) -> Vec<&Segment> {
        let slice_start = self
            .segments
            .iter()
            .rposition(|segment| segment.start_offset <= start_offset)
            .unwrap_or(0);

        self.segments[slice_start..]
            .iter()
            .filter(|segment| segment.start_offset <= end_offset)
            .collect()
    }

    // Retrieves messages from multiple segments.
    async fn get_messages_from_segments(
        segments: Vec<&Segment>,
        offset: u64,
        count: u32,
    ) -> Result<Vec<IggyBatchSlice>, IggyError> {
        //TODO: Figure out how to flatten those IggyBatchFetchResults.
        let mut results = Vec::new();
        let mut remaining_count = count;
        for segment in segments {
            if remaining_count == 0 {
                break;
            }
            let slices = segment
                .get_messages_by_offset(offset, remaining_count)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to get messages from segment, segment: {}, \
                         offset: {}, count: {}",
                        segment, offset, remaining_count
                    )
                })?;
            let messages_count = slices
                .iter()
                .map(|slice| slice.header.last_offset_delta)
                .sum();
            remaining_count = remaining_count.saturating_sub(messages_count);
            results.extend(slices);
        }
        Ok(results)
    }

    // Tries to retrieve messages from the in-memory cache.
    fn try_get_messages_from_cache(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Option<Vec<Arc<()>>> {
        //TODO: Fix me
        /*
        let cache = self.cache.as_ref()?;
        if cache.is_empty() || start_offset > end_offset || end_offset > self.current_offset {
            return None;
        }

        let first_buffered_offset = cache[0].offset;
        trace!(
            "First buffered offset: {} for partition: {}",
            first_buffered_offset,
            self.partition_id
        );

        if start_offset >= first_buffered_offset {
            cache.record_hit();
            return Some(self.load_messages_from_cache(start_offset, end_offset));
        }
        cache.record_miss();
        None
        */
        todo!()
    }

    pub async fn get_newest_messages_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<Arc<()>>, IggyError> {
        //TODO: Fix me
        /*
        trace!(
            "Getting messages for size: {} bytes for partition: {}...",
            size_bytes,
            self.partition_id
        );

        if self.segments.is_empty() {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        let mut remaining_size = size_bytes;
        let mut batches = Vec::new();
        for segment in self.segments.iter().rev() {
            let segment_size_bytes = segment.size_bytes.as_bytes_u64();
            if segment_size_bytes == 0 {
                break;
            }
            if segment_size_bytes > remaining_size {
                let partial_batches = segment
                    .get_newest_batches_by_size(remaining_size)
                    .await
                    .with_error_context(|error| format!(
                        "{COMPONENT} (error: {error}) - failed to get newest batches by size, segment: {}, remaining size: {}",
                        segment, remaining_size,
                    ))?
                    .into_iter()
                    .map(Arc::new);
                batches.splice(..0, partial_batches);
                break;
            }

            let segment_batches = segment
                .get_all_batches()
                .await
                .with_error_context(|error| {
                    format!("{COMPONENT} (error: {error}) - failed to retrieve all batches from segment: {segment}",)
                })?
                .into_iter()
                .map(Arc::new);
            batches.splice(..0, segment_batches);
            remaining_size = remaining_size.saturating_sub(segment_size_bytes);
            if remaining_size == 0 {
                break;
            }
        }
        let mut retained_messages = Vec::new();
        for batch in batches {
            let messages = batch.into_messages_iter().map(Arc::new).collect::<Vec<_>>();
            retained_messages.extend(messages);
        }
        Ok(retained_messages)
        */
        todo!()
    }

    fn load_messages_from_cache(&self, start_offset: u64, end_offset: u64) -> Vec<Arc<()>> {
        /*
        trace!(
            "Loading messages from cache, start offset: {}, end offset: {}...",
            start_offset,
            end_offset
        );

        if self.cache.is_none() || start_offset > end_offset {
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        let cache = self.cache.as_ref().unwrap();
        if cache.is_empty() {
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        let first_offset = cache[0].offset;
        let start_index = (start_offset - first_offset) as usize;
        let end_index = usize::min(cache.len(), (end_offset - first_offset + 1) as usize);
        let expected_messages_count = end_index - start_index;

        let mut messages = Vec::with_capacity(expected_messages_count);
        for i in start_index..end_index {
            messages.push(cache[i].clone());
        }

        if messages.len() != expected_messages_count {
            warn!(
                "Loaded {} messages from cache, expected {}.",
                messages.len(),
                expected_messages_count
            );
            return EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        }

        trace!(
            "Loaded {} messages from cache, start offset: {}, end offset: {}...",
            messages.len(),
            start_offset,
            end_offset
        );

        messages
        */
        todo!()
    }

    pub async fn append_messages(
        &mut self,
        appendable_batch_info: AppendableBatchInfo,
        batch: IggyMutableBatch,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            if last_segment.is_closed {
                let start_offset = last_segment.end_offset + 1;
                trace!(
                    "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                    start_offset, self.partition_id
                );
                self.add_persisted_segment(start_offset).await.with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to add persisted segment, partition: {}, start offset: {}",
                    self, start_offset,
                ))?;
            }
        }

        let mut batch = batch;
        let batch_size = appendable_batch_info.batch_size;
        let current_offset = if !self.should_increment_offset {
            0
        } else {
            self.current_offset + 1
        };

        // TODO: Fix me
        // Use a sequence number on the batch and hold a stream local collection of the last few sequence numbers.
        /*
        let mut retained_messages = Vec::with_capacity(messages.len());
        if let Some(message_deduplicator) = &self.message_deduplicator {
            for message in messages {
                if !message_deduplicator.try_insert(&message.id).await {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                    continue;
                }
                let now = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, now, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        } else {
            for message in messages {
                let now = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, now, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        }
        if messages_count == 0 {
            return Ok(());
        }
        */

        // Why are we even doing it this way ? XD
        // What are those scopes
        let messages_count = {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            let messages_count = last_segment
                 .append_batch(current_offset, batch)
                 .await
                 .with_error_context(|error| {
                     format!(
                         "{COMPONENT} (error: {error}) - failed to append batch into last segment: {last_segment}",
                     )
                 })?;
            messages_count
        };
        let last_offset = current_offset + messages_count as u64 - 1;
        if self.should_increment_offset {
            self.current_offset = last_offset;
        } else {
            self.should_increment_offset = true;
            self.current_offset = last_offset;
        }
        /*
        if let Some(cache) = &mut self.cache {
            cache.extend(retained_messages);
        }
        */

        self.unsaved_messages_count += messages_count;
        {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            if self.unsaved_messages_count >= self.config.partition.messages_required_to_save
                || last_segment.is_full().await
            {
                trace!(
                    "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
                    last_segment.start_offset,
                    self.partition_id
                );

                last_segment.persist_messages(confirmation).await.unwrap();
                self.unsaved_messages_count = 0;
            }
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
            last_segment.start_offset,
            self.partition_id
        );

        // Make sure all of the messages from the accumulator are persisted
        // no leftover from one round trip.
        while last_segment.unsaved_messages.is_some() {
            last_segment.persist_messages(None).await.unwrap();
        }
        self.unsaved_messages_count = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO: Fix me
    /*
    use iggy::utils::byte_size::IggyByteSize;
    use iggy::utils::expiry::IggyExpiry;
    use iggy::utils::sizeable::Sizeable;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use tempfile::TempDir;

    use super::*;
    use crate::configs::system::{MessageDeduplicationConfig, SystemConfig};
    use crate::streaming::partitions::create_messages;
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(false).await;
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let appendable_batch_info = AppendableBatchInfo {
            batch_size: messages
                .iter()
                .map(|m| m.get_size_bytes())
                .sum::<IggyByteSize>(),
            partition_id: partition.partition_id,
        };
        partition
            .append_messages(appendable_batch_info, messages, None)
            .await
            .unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.len(), messages_count as usize);
    }

    #[tokio::test]
    async fn given_enabled_message_deduplication_only_messages_with_unique_id_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(true).await;
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let unique_messages_count = 3;
        let appendable_batch_info = AppendableBatchInfo {
            batch_size: messages
                .iter()
                .map(|m| m.get_size_bytes())
                .sum::<IggyByteSize>(),
            partition_id: partition.partition_id,
        };
        partition
            .append_messages(appendable_batch_info, messages, None)
            .await
            .unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.len(), unique_messages_count);
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
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));

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
    */
}
