use super::IggyMessagesBatchMut;
use crate::streaming::segments::segment::Segment;
use crate::{
    configs::cache_indexes::CacheIndexesConfig,
    streaming::deduplication::message_deduplicator::MessageDeduplicator,
};
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use std::sync::atomic::Ordering;
use tracing::{info, trace};

impl Segment {
    pub async fn append_batch(
        &mut self,
        current_offset: u64,
        messages: IggyMessagesBatchMut,
        deduplicator: Option<&MessageDeduplicator>,
    ) -> Result<(), IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }
        let batch_messages_size = messages.size();
        let batch_messages_count = messages.count();

        let messages_accumulator = &mut self.accumulator;
        messages_accumulator
            .coalesce_batch(
                self.start_offset,
                current_offset,
                self.last_index_position,
                messages,
                deduplicator,
            )
            .await;

        if self.end_offset == 0 {
            self.start_timestamp = messages_accumulator.first_timestamp();
        }
        self.end_timestamp = messages_accumulator.last_timestamp();
        self.end_offset = messages_accumulator.last_offset();

        self.update_counters(batch_messages_size as u64, batch_messages_count as u64);

        Ok(())
    }

    pub async fn persist_messages(
        &mut self,
        confirmation: Option<Confirmation>,
    ) -> Result<usize, IggyError> {
        if self.accumulator.is_empty() {
            return Ok(0);
        }

        let unsaved_messages_count = self.accumulator.unsaved_messages_count();
        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_count,
            self.start_offset,
            self.partition_id
        );

        let accumulator = std::mem::take(&mut self.accumulator);

        let mut batches = accumulator.into_batch_set();
        let confirmation = match confirmation {
            Some(val) => val,
            None => self.config.segment.server_confirmation,
        };

        let batch_size = batches.size();
        let batch_count = batches.count();

        let saved_bytes = self
            .messages_writer
            .as_mut()
            .expect("Messages writer not initialized")
            .save_batch_set(&batches, confirmation)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to save batch of {batch_count} messages ({batch_size} bytes) to {self}. {error}",
                )
            })?;

        self.last_index_position += saved_bytes.as_bytes_u64() as u32;

        batches.extract_indexes_to(&mut self.indexes);

        let unsaved_indexes_slice = self.indexes.unsaved_slice();
        self.index_writer
            .as_mut()
            .expect("Index writer not initialized")
            .save_indexes(unsaved_indexes_slice)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to save index of {} indexes to {self}. {error}",
                    unsaved_indexes_slice.len()
                )
            })?;

        self.indexes.mark_saved();

        if self.config.segment.cache_indexes == CacheIndexesConfig::None {
            self.indexes.clear();
        }

        self.check_and_handle_segment_full().await?;

        let saved_messages_count = unsaved_messages_count;

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            saved_messages_count,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        Ok(saved_messages_count)
    }

    fn update_counters(&mut self, messages_size: u64, messages_count: u64) {
        self.size_of_parent_stream
            .fetch_add(messages_size, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(messages_size, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(messages_size, Ordering::AcqRel);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count, Ordering::SeqCst);
    }

    async fn check_and_handle_segment_full(&mut self) -> Result<(), IggyError> {
        if self.is_full().await {
            let max_segment_size_from_config = self.config.segment.size;
            let current_segment_size = self.get_messages_size();
            assert!(
                current_segment_size >= max_segment_size_from_config,
                "Current segment size: {} is greater than max segment size: {}",
                current_segment_size,
                max_segment_size_from_config
            );
            if self.config.segment.cache_indexes == CacheIndexesConfig::OpenSegment {
                self.drop_indexes();
            }
            self.shutdown_writing().await;
            info!(
                "Closed segment with start offset: {}, end offset: {}, size: {} for partition with ID: {}.",
                self.start_offset, self.end_offset, self.get_messages_size(), self.partition_id
            );
            self.is_closed = true;
        }
        Ok(())
    }
}
