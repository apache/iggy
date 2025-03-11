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

use super::indexes::*;
use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::batching::message_batch::RETAINED_BATCH_HEADER_LEN;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::segments::segment::Segment;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::sizeable::Sizeable;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, trace};

impl Segment {
    pub async fn append_batch(
        &mut self,
        batch_size: IggyByteSize,
        messages_count: u32,
        batch: &[Arc<RetainedMessage>],
    ) -> Result<(), IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }
        let messages_cap = std::cmp::max(
            self.config.partition.messages_required_to_save as usize,
            batch.len(),
        );
        if self.current_offset == 0 {
            self.start_timestamp = batch.first().unwrap().timestamp;
        }
        let batch_base_offset = batch.first().unwrap().offset;
        let batch_accumulator = self
            .unsaved_messages
            .get_or_insert_with(|| BatchAccumulator::new(batch_base_offset, messages_cap));
        batch_accumulator.append(batch_size, batch);
        self.end_timestamp = batch_accumulator.batch_max_timestamp();
        let curr_offset = batch_accumulator.batch_max_offset();

        self.current_offset = curr_offset;
        self.size_bytes += batch_size;
        let batch_size = batch_size.as_bytes_u64();
        self.size_of_parent_stream
            .fetch_add(batch_size, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(batch_size, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(batch_size, Ordering::AcqRel);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count as u64, Ordering::SeqCst);

        Ok(())
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

        let mut batch_accumulator = self.unsaved_messages.take().unwrap();
        if batch_accumulator.is_empty() {
            return Ok(0);
        }
        let batch_max_offset = batch_accumulator.batch_max_offset();
        let batch_max_timestamp = batch_accumulator.batch_max_timestamp();
        let index =
            self.store_offset_and_timestamp_index_for_batch(batch_max_offset, batch_max_timestamp);

        let unsaved_messages_number = batch_accumulator.unsaved_messages_count();
        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id
        );

        let batch = batch_accumulator.materialize_batch_and_update_state();
        let batch_size = batch.get_size_bytes();
        if batch_size > 0 {
            self.unsaved_messages = Some(batch_accumulator);
        }
        let confirmation = match confirmation {
            Some(val) => val,
            None => self.config.segment.server_confirmation,
        };
        let saved_bytes = self
            .log_writer
            .as_mut()
            .unwrap()
            .save_batches(batch, confirmation)
            .await
            .with_error_context(|error| {
                format!("Failed to save batch of size {batch_size} for {self}. {error}",)
            })?;

        self.index_writer
            .as_mut()
            .unwrap()
            .save_index(index)
            .await
            .with_error_context(|error| format!("Failed to save index for {self}. {error}"))?;

        self.last_index_position += batch_size.as_bytes_u64() as u32;
        self.size_bytes += IggyByteSize::from(RETAINED_BATCH_HEADER_LEN);
        self.size_of_parent_stream
            .fetch_add(RETAINED_BATCH_HEADER_LEN, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(RETAINED_BATCH_HEADER_LEN, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(RETAINED_BATCH_HEADER_LEN, Ordering::AcqRel);

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
