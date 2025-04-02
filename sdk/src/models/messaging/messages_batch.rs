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

use super::{IggyIndexes, IggyMessage, IggyMessageView, IggyMessageViewIterator};
use crate::{
    error::IggyError,
    messages::MAX_PAYLOAD_SIZE,
    models::messaging::INDEX_SIZE,
    prelude::{BytesSerializable, IggyByteSize, Sizeable, Validatable},
};
use bytes::{BufMut, Bytes, BytesMut};
use std::ops::{Deref, Index};

/// An immutable messages container that holds a buffer of messages
#[derive(Clone, Debug, PartialEq)]
pub struct IggyMessagesBatch {
    /// The number of messages in the batch
    count: u32,
    /// The byte-indexes of messages in the buffer, represented as array of u32's. Offsets are relative.
    /// Each index consists of offset, position (byte offset in the buffer) and timestamp.
    indexes: IggyIndexes,
    /// The buffer containing the messages
    messages: Bytes,
}

impl IggyMessagesBatch {
    /// Create a batch from indexes buffer and messages buffer
    pub fn new(indexes: IggyIndexes, messages: Bytes, count: u32) -> Self {
        Self {
            count,
            indexes,
            messages,
        }
    }

    /// Creates a empty messages batch
    pub fn empty() -> Self {
        Self::new(IggyIndexes::empty(), BytesMut::new().freeze(), 0)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.messages.len() as u32
    }

    /// Get access to the underlying buffer
    pub fn buffer(&self) -> &[u8] {
        &self.messages
    }

    /// Get the indexes slice
    pub fn indexes_slice(&self) -> &[u8] {
        &self.indexes
    }

    /// Take the indexes from the batch
    pub fn take_indexes(&mut self) -> IggyIndexes {
        std::mem::take(&mut self.indexes)
    }

    /// Decompose the batch into its components
    pub fn decompose(self) -> (u32, IggyIndexes, Bytes) {
        (self.count, self.indexes, self.messages)
    }

    /// Get index of first message
    pub fn first_offset(&self) -> Option<u64> {
        self.iter().next().map(|msg| msg.header().offset())
    }

    /// Get timestamp of first message
    pub fn first_timestamp(&self) -> Option<u64> {
        self.iter().next().map(|msg| msg.header().timestamp())
    }

    /// Get offset of last message
    pub fn last_offset(&self) -> Option<u64> {
        self.iter().last().map(|msg| msg.header().offset())
    }

    /// Get timestamp of last message
    pub fn last_timestamp(&self) -> Option<u64> {
        self.iter().last().map(|msg| msg.header().timestamp())
    }

    /// Calculates the start position of a message at the given index in the buffer
    fn message_start_position(&self, index: usize) -> usize {
        if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize - self.indexes.base_position() as usize
        }
    }

    /// Calculates the end position of a message at the given index in the buffer
    fn message_end_position(&self, index: usize) -> usize {
        if index >= self.count as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize - self.indexes.base_position() as usize
        }
    }

    /// Gets the byte range for a message at the given index
    fn get_message_boundaries(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.count as usize {
            return None;
        }

        let start = self.message_start_position(index);
        let end = self.message_end_position(index);

        if start > self.messages.len() || end > self.messages.len() || start > end {
            return None;
        }

        Some((start, end))
    }

    /// Helper method to read a position (u32) from the byte array at the given index
    fn position_at(&self, position_index: u32) -> u32 {
        if let Some(index) = self.indexes.get(position_index) {
            index.position()
        } else {
            0
        }
    }

    /// Returns a contiguous slice (as a new `IggyMessagesBatch`) of up to `count` messages
    /// whose message headers have an offset greater than or equal to the provided `start_offset`.
    pub fn slice_by_offset(&self, start_offset: u64, count: u32) -> Option<Self> {
        if self.is_empty() || count == 0 {
            return None;
        }

        let first_offset = self.first_offset()?;

        if start_offset < first_offset {
            return self.slice_by_index(0, count);
        }

        let last_offset = self.last_offset()?;
        if start_offset > last_offset {
            return None;
        }

        let offset_diff = start_offset - first_offset;
        let first_message_index = offset_diff as usize;

        if first_message_index >= self.count() as usize {
            return None;
        }

        self.slice_by_index(first_message_index as u32, count)
    }

    /// Helper method to slice the batch starting from a specific index
    fn slice_by_index(&self, start_index: u32, count: u32) -> Option<Self> {
        if start_index >= self.count() {
            return None;
        }

        let last_message_index =
            std::cmp::min((start_index + count) as usize, self.count() as usize);

        let sub_indexes = self.indexes.slice_by_offset(
            start_index,
            (last_message_index - start_index as usize) as u32,
        )?;

        let first_message_position = self.message_start_position(start_index as usize);
        let last_message_position = self.message_end_position(last_message_index - 1);

        let sub_buffer = self
            .messages
            .slice(first_message_position..last_message_position);

        Some(IggyMessagesBatch {
            count: (last_message_index - start_index as usize) as u32,
            indexes: sub_indexes,
            messages: sub_buffer,
        })
    }

    /// Returns a contiguous slice (as a new `IggyMessagesBatch`) of up to `count` messages
    /// whose message headers have a timestamp greater than or equal to the provided `timestamp`.
    ///
    /// If no messages meet the criteria, returns `None`.
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<Self> {
        if self.is_empty() || count == 0 {
            return None;
        }

        // Use binary search to find the first message with timestamp >= the target
        let first_message_index = self.binary_search_timestamp(timestamp)?;

        self.slice_by_index(first_message_index, count)
    }

    /// Find the position of the index with timestamp closest to (but not exceeding) the target
    fn binary_search_timestamp(&self, target_timestamp: u64) -> Option<u32> {
        if self.count() == 0 {
            return None;
        }

        let last_timestamp = self.get(self.count() as usize - 1)?.header().timestamp();
        if target_timestamp > last_timestamp {
            return Some(self.count() - 1);
        }

        let first_timestamp = self.get(0)?.header().timestamp();
        if target_timestamp <= first_timestamp {
            return Some(0);
        }

        let mut low = 0;
        let mut high = self.count() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_index = self.get(mid as usize)?;
            let mid_timestamp = mid_index.header().timestamp();

            match mid_timestamp.cmp(&target_timestamp) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }
        }

        Some(low)
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
        if let Some((start, end)) = self.get_message_boundaries(index) {
            Some(IggyMessageView::new(&self.messages[start..end]))
        } else {
            None
        }
    }

    /// Consumes the batch and returns a vector of messages.
    pub fn into_messages(self) -> Vec<IggyMessage> {
        if self.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.count as usize);

        for i in 0..self.count as usize {
            if let Some((start, end)) = self.get_message_boundaries(i) {
                if let Ok(message) = IggyMessage::from_bytes(self.messages.slice(start..end)) {
                    result.push(message);
                }
            }
        }

        result
    }

    /// Validates that all messages in batch have correct checksums.
    pub fn validate_checksums(&self) -> Result<(), IggyError> {
        for message in self.iter() {
            let calculated_checksum = message.calculate_checksum();
            let actual_checksum = message.header().checksum();
            let offset = message.header().offset();
            if calculated_checksum != actual_checksum {
                return Err(IggyError::InvalidMessageChecksum(
                    actual_checksum,
                    calculated_checksum,
                    offset,
                ));
            }
        }
        Ok(())
    }

    /// Validates that all messages have correct checksums and offsets.
    /// This function should be called after messages have been read from disk.
    ///
    /// # Arguments
    ///
    /// * `absolute_start_offset` - The absolute offset of the first message in the batch.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all messages have correct checksums and offsets.
    /// * `Err(IggyError)` - If any message has an invalid checksum or offset.
    pub fn validate_checksums_and_offsets(
        &self,
        absolute_start_offset: u64,
    ) -> Result<(), IggyError> {
        let mut current_offset = absolute_start_offset;
        for message in self.iter() {
            let calculated_checksum = message.calculate_checksum();
            let actual_checksum = message.header().checksum();
            let offset = message.header().offset();
            if offset != current_offset {
                return Err(IggyError::InvalidOffset(offset));
            }
            if calculated_checksum != actual_checksum {
                return Err(IggyError::InvalidMessageChecksum(
                    actual_checksum,
                    calculated_checksum,
                    offset,
                ));
            }
            current_offset += 1;
        }
        Ok(())
    }
}

impl IntoIterator for IggyMessagesBatch {
    type Item = IggyMessage;
    type IntoIter = std::vec::IntoIter<IggyMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_messages().into_iter()
    }
}

impl Index<usize> for IggyMessagesBatch {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.count as usize {
            panic!(
                "Index out of bounds: the len is {} but the index is {}",
                self.count, index
            );
        }

        let (start, end) = self
            .get_message_boundaries(index)
            .expect("Invalid message boundaries");

        &self.messages[start..end]
    }
}
impl BytesSerializable for IggyMessagesBatch {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used");
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("don't use");
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.count);
        buf.put_slice(&self.indexes);
        buf.put_slice(&self.messages);
    }

    fn get_buffer_size(&self) -> usize {
        4 + self.indexes.len() + self.messages.len()
    }
}

impl Validatable<IggyError> for IggyMessagesBatch {
    fn validate(&self) -> Result<(), IggyError> {
        if self.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        let indexes_count = self.indexes.count();
        let indexes_size = self.indexes.size();

        if indexes_size % INDEX_SIZE as u32 != 0 {
            tracing::error!(
                "Indexes size {} is not a multiple of index size {}",
                indexes_size,
                INDEX_SIZE
            );
            return Err(IggyError::InvalidIndexesByteSize(indexes_size));
        }

        if indexes_count != self.count() {
            tracing::error!(
                "Indexes count {} does not match messages count {}",
                indexes_count,
                self.count()
            );
            return Err(IggyError::InvalidIndexesCount(indexes_count, self.count()));
        }

        let mut messages_count = 0;
        let mut messages_size = 0;

        for i in 0..self.count() {
            if let Some(index_view) = self.indexes.get(i) {
                if index_view.offset() != 0 {
                    tracing::error!("Non-zero offset {} at index: {}", index_view.offset(), i);
                    return Err(IggyError::NonZeroOffset(index_view.offset() as u64, i));
                }
                if index_view.timestamp() != 0 {
                    tracing::error!(
                        "Non-zero timestamp {} at index: {}",
                        index_view.timestamp(),
                        i
                    );
                    return Err(IggyError::NonZeroTimestamp(index_view.timestamp(), i));
                }
            } else {
                tracing::error!("Index {} is missing", i);
                return Err(IggyError::MissingIndex(i));
            }

            if let Some(message) = self.get(i as usize) {
                if message.payload().len() as u32 > MAX_PAYLOAD_SIZE {
                    tracing::error!(
                        "Message payload size {} exceeds maximum payload size {}",
                        message.payload().len(),
                        MAX_PAYLOAD_SIZE
                    );
                    return Err(IggyError::TooBigMessagePayload);
                }

                messages_size += message.size();
                messages_count += 1;
            } else {
                tracing::error!("Missing index {}", i);
                return Err(IggyError::MissingIndex(i));
            }
        }

        if indexes_count != messages_count {
            tracing::error!(
                "Indexes count {} does not match messages count {}",
                indexes_count,
                messages_count
            );
            return Err(IggyError::InvalidMessagesCount);
        }

        if messages_size != self.messages.len() {
            tracing::error!(
                "Messages size {} does not match messages buffer size {}",
                messages_size,
                self.messages.len() as u64
            );
            return Err(IggyError::InvalidMessagesSize(
                messages_size as u32,
                self.messages.len() as u32,
            ));
        }

        Ok(())
    }
}

impl Sizeable for IggyMessagesBatch {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages.len() as u64)
    }
}

impl Deref for IggyMessagesBatch {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}

/// Converts a slice of IggyMessage objects into an IggyMessagesBatch.
///
/// This trait implementation enables idiomatic conversion from message slices:
/// `let batch = IggyMessagesBatch::from(messages_slice);`
///
/// 1. Messages are serialized into a contiguous buffer
/// 2. Index entries are created for each message with:
///    - offset: Set to 0 (will be filled by the server during append)
///    - position: Cumulative byte position of each message in the buffer
///      Subsequent indexes point to the next message in the buffer
///    - timestamp: Set to 0 (will be filled by the server during append)
///
/// # Performance note
///
/// This layout is optimized for server-side processing. The server can efficiently:
/// - Allocate offsets sequentially
/// - Assign timestamps
/// - Write the entire message batch and index data to disk without additional allocations
/// - Update the offset and timestamp fields in-place before persistence
impl From<&[IggyMessage]> for IggyMessagesBatch {
    fn from(messages: &[IggyMessage]) -> Self {
        if messages.is_empty() {
            return Self::empty();
        }

        let messages_count = messages.len() as u32;
        let mut total_size = 0;
        for msg in messages.iter() {
            total_size += msg.get_size_bytes().as_bytes_usize();
        }

        let mut messages_buffer = BytesMut::with_capacity(total_size);
        let mut indexes_buffer = BytesMut::with_capacity(messages_count as usize * INDEX_SIZE);
        let mut current_position = 0;

        for message in messages.iter() {
            message.write_to_buffer(&mut messages_buffer);

            let msg_size = message.get_size_bytes().as_bytes_u32();
            current_position += msg_size;

            indexes_buffer.put_u32_le(0);
            indexes_buffer.put_u32_le(current_position);
            indexes_buffer.put_u64_le(0);
        }

        let indexes = IggyIndexes::new(indexes_buffer.freeze(), 0);

        Self {
            count: messages_count,
            indexes,
            messages: messages_buffer.freeze(),
        }
    }
}

/// Converts a reference to Vec<IggyMessage> into an IggyMessagesBatch.
///
/// This implementation delegates to the slice implementation via `as_slice()`.
/// It's provided for convenience so it's possible to use `&messages` without
/// explicit slice conversion.
impl From<&Vec<IggyMessage>> for IggyMessagesBatch {
    fn from(messages: &Vec<IggyMessage>) -> Self {
        Self::from(messages.as_slice())
    }
}

/// Converts a Vec<IggyMessage> into an IggyMessagesBatch.
impl From<Vec<IggyMessage>> for IggyMessagesBatch {
    fn from(messages: Vec<IggyMessage>) -> Self {
        Self::from(messages.as_slice())
    }
}
