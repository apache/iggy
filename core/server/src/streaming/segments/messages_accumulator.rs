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

use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;

use super::types::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use tracing::trace;

/// A container that accumulates messages in memory before they are written to disk.
///
/// The accumulator serves as a staging area for messages, allowing them to be
/// collected and prepared for persistence. It maintains metadata like offsets,
/// timestamps, and positions to ensure correct ordering and indexing.
#[derive(Debug, Default)]
pub struct MessagesAccumulator {
    /// Base offset of the first message in the accumulator
    base_offset: u64,

    /// Current (latest) offset in the accumulator
    current_offset: u64,

    /// Current (latest) byte position for the next message in the segment, also size of all messages in the accumulator
    current_position: u32,

    /// Collection of all message batches in the accumulator
    batches: IggyMessagesBatchSet,

    /// Total number of messages in the accumulator
    messages_count: u32,
}

impl MessagesAccumulator {
    /// Adds a batch of messages to the accumulator and prepares them for persistence.
    ///
    /// This method assigns offsets, timestamps, and positions to the messages in the batch.
    /// It updates internal state to track the current offset and byte position,
    /// ensuring that subsequent batches will have correct sequential values.
    /// The offset management logic handles both initial batch insertion and
    /// maintaining appropriate offsets for follow-up batches.
    ///
    /// # Arguments
    ///
    /// * segment_start_offset - The segment's starting offset
    /// * segment_current_offset - The suggested starting offset for this batch
    /// * segment_current_position - The current byte position in the segment
    /// * batch - The batch of messages to add
    /// * deduplicator - Optional message deduplicator to handle duplicate messages
    ///
    pub async fn coalesce_batch(
        &mut self,
        segment_start_offset: u64,
        segment_current_offset: u64,
        segment_current_position: u32,
        mut batch: IggyMessagesBatchMut,
        deduplicator: Option<&MessageDeduplicator>,
    ) {
        let batch_messages_count = batch.count();
        if batch_messages_count == 0 {
            return;
        }

        trace!(
            "Coalescing batch with base_offset: {}, segment_current_offset: {}, self.messages_count: {}, batch.count: {}",
            self.base_offset, segment_current_offset, self.messages_count, batch_messages_count
        );

        self.initialize_or_update_offsets(segment_current_offset, segment_current_position);

        batch
            .prepare_for_persistence(
                segment_start_offset,
                self.current_offset,
                self.current_position,
                deduplicator,
            )
            .await;

        let batch_size = batch.size();

        self.batches.add_batch(batch);

        self.messages_count += batch_messages_count;
        self.current_offset = self.base_offset + self.messages_count as u64 - 1;
        self.current_position += batch_size;
    }

    /// Initialize accumulator state for the first batch or update offsets for subsequent batches
    fn initialize_or_update_offsets(&mut self, current_offset: u64, current_position: u32) {
        if self.batches.is_empty() {
            self.base_offset = current_offset;
            self.current_offset = current_offset;
            self.current_position = current_position;
        } else {
            let next_expected_offset = self.current_offset + 1;
            self.current_offset = current_offset.max(next_expected_offset);
        }
    }

    /// Retrieves messages from the accumulator based on start offset and count.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The starting offset to retrieve messages from
    /// * `count` - Maximum number of messages to retrieve
    ///
    /// # Returns
    ///
    /// A batch set containing the requested messages
    pub fn get_messages_by_offset(&self, start_offset: u64, count: u32) -> IggyMessagesBatchSet {
        trace!(
            "Getting {count} messages from accumulator by offset {start_offset}, current_offset: {}, current_position: {}",
            self.current_offset, self.current_position
        );
        self.batches.get_by_offset(start_offset, count)
    }

    /// Retrieves messages from the accumulator based on start timestamp and count.
    ///
    /// # Arguments
    ///
    /// * `start_timestamp` - The earliest timestamp to retrieve messages from
    /// * `count` - Maximum number of messages to retrieve
    ///
    /// # Returns
    ///
    /// A batch set containing the requested messages
    pub fn get_messages_by_timestamp(
        &self,
        start_timestamp: u64,
        count: u32,
    ) -> IggyMessagesBatchSet {
        self.batches.get_by_timestamp(start_timestamp, count)
    }

    /// Checks if the accumulator is empty (has no messages).
    pub fn is_empty(&self) -> bool {
        self.messages_count == 0
    }

    /// Returns the number of messages in the accumulator that have not been persisted.
    pub fn unsaved_messages_count(&self) -> usize {
        self.messages_count as usize
    }

    /// Returns the highest offset in the accumulator.
    pub fn last_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the timestamp of the last message in the accumulator.
    pub fn last_timestamp(&self) -> u64 {
        self.batches.last_timestamp().unwrap_or(0)
    }

    /// Returns the size of the last message in the accumulator.
    pub fn unsaved_messages_size(&self) -> u32 {
        self.current_position
    }

    /// Returns the starting offset of the first message in the accumulator.
    pub fn first_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the timestamp of the first message in the accumulator.
    pub fn first_timestamp(&self) -> u64 {
        self.batches.first_timestamp().unwrap_or(0)
    }

    /// Consumes the accumulator and returns the contained message batches.
    ///
    /// This is typically called when it's time to persist the accumulated messages to disk.
    pub fn into_batch_set(self) -> IggyMessagesBatchSet {
        self.batches
    }

    /// Gets the size of the accumulated messages in bytes
    pub fn size(&self) -> usize {
        self.batches.size() as usize
    }
}
