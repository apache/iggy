use super::indexes::*;
use super::IggyMessagesMut;
use crate::streaming::segments::segment::Segment;
use bytes::BufMut;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use std::sync::atomic::Ordering;
use tracing::{info, trace};

impl Segment {
    pub async fn append_batch(
        &mut self,
        current_offset: u64,
        messages: IggyMessagesMut,
    ) -> Result<u32, IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }
        let messages_size = messages.size();
        let messages_accumulator = self.unsaved_messages.get_or_insert_with(Default::default);
        let messages_count =
            messages_accumulator.coalesce_batch(current_offset, self.last_index_position, messages);

        if self.current_offset == 0 {
            self.start_timestamp = messages_accumulator.batch_base_timestamp();
        }
        self.end_timestamp = messages_accumulator.batch_max_timestamp();
        self.current_offset = messages_accumulator.max_offset();
        self.size_bytes += IggyByteSize::from(messages_size as u64);

        self.size_of_parent_stream
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count as u64, Ordering::SeqCst);

        Ok(messages_count)
    }

    fn store_offset_and_timestamp_index_for_batch(
        &mut self,
        batch_last_offset: u64,
        batch_max_timestamp: u64,
    ) -> Index {
        let relative_offset = (batch_last_offset - self.start_offset) as u32;
        trace!(
            "Storing index for relative_offset: {relative_offset}, start_offset: {}",
            self.start_offset
        );
        let index = Index {
            offset: relative_offset,
            position: self.last_index_position,
            timestamp: batch_max_timestamp,
        };
        if let Some(indexes) = &mut self.indexes {
            indexes.push(index);
        }
        index
    }

    pub async fn persist_messages(
        &mut self,
        confirmation: Option<Confirmation>,
    ) -> Result<usize, IggyError> {
        if self.unsaved_messages.is_none() {
            return Ok(0);
        }

        let messages_accumulator = self.unsaved_messages.take().unwrap();
        if messages_accumulator.is_empty() {
            return Ok(0);
        }
        // let batch_max_offset = messages_accumulator.max_offset();
        // let batch_max_timestamp = messages_accumulator.batch_max_timestamp();
        // let index =
        //     self.store_offset_and_timestamp_index_for_batch(batch_max_offset, batch_max_timestamp);

        let unsaved_messages_number = messages_accumulator.unsaved_messages_count();
        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id
        );

        let (messages, indexes) = messages_accumulator.materialize();
        let confirmation = match confirmation {
            Some(val) => val,
            None => self.config.segment.server_confirmation,
        };

        let saved_bytes = self
            .messages_writer
            .as_mut()
            .unwrap()
            .save_batches(messages, confirmation)
            .await
            .with_error_context(|error| format!("Failed to save batch for {self}. {error}",))?;

        self.last_index_position += saved_bytes.as_bytes_u64() as u32;

        self.index_writer
            .as_mut()
            .unwrap()
            .save_indexes(&indexes)
            .await
            .with_error_context(|error| format!("Failed to save index for {self}. {error}"))?;

        // TODO(hubcio): fix this
        // self.size_bytes += IggyByteSize::from(IGGY_BATCH_OVERHEAD);
        // self.size_of_parent_stream
        //     .fetch_add(IGGY_BATCH_OVERHEAD, Ordering::AcqRel);
        // self.size_of_parent_topic
        //     .fetch_add(IGGY_BATCH_OVERHEAD, Ordering::AcqRel);
        // self.size_of_parent_partition
        //     .fetch_add(IGGY_BATCH_OVERHEAD, Ordering::AcqRel);

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_messages = None;
            self.shutdown_writing().await;
            info!(
                "Closed segment with start offset: {}, end offset: {} for partition with ID: {}.",
                self.start_offset, self.end_offset, self.partition_id
            );
        }
        Ok(unsaved_messages_number)
    }
}
