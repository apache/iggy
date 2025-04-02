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

use super::message_view_mut::IggyMessageViewMutIterator;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::segments::indexes::IggyIndexesMut;
use crate::streaming::utils::random_id;
use bytes::{BufMut, BytesMut};
use iggy::messages::MAX_PAYLOAD_SIZE;
use iggy::models::messaging::{IggyIndexView, IggyMessagesBatch, INDEX_SIZE};
use iggy::prelude::*;
use iggy::utils::timestamp::IggyTimestamp;
use lending_iterator::prelude::*;
use std::ops::Deref;
use tracing::{error, warn};

/// A container for mutable messages that are being prepared for persistence.
///
/// `IggyMessagesBatchMut` holds both the raw message data in a `BytesMut` buffer
/// and the corresponding index data that allows for efficient message lookup.
#[derive(Debug, Default)]
pub struct IggyMessagesBatchMut {
    /// The index data for all messages in the buffer
    indexes: IggyIndexesMut,

    /// The buffer containing the serialized message data
    messages: BytesMut,
}

impl Sizeable for IggyMessagesBatchMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages.len() as u64)
    }
}

impl IggyMessagesBatchMut {
    /// Creates an empty messages container with the specified capacity to avoid reallocations.
    ///
    /// # Arguments
    ///
    /// * `bytes_capacity` - The expected total size of all messages in bytes
    pub fn with_capacity(bytes_capacity: usize) -> Self {
        let index_capacity = bytes_capacity / INDEX_SIZE + 1; // Add 1 to avoid rounding down to 0
        Self {
            indexes: IggyIndexesMut::with_capacity(index_capacity, 0),
            messages: BytesMut::with_capacity(bytes_capacity),
        }
    }

    /// Creates a new messages container from existing index and message buffers.
    ///
    /// # Arguments
    ///
    /// * `indexes` - Preprocessed index data
    /// * `messages` - Serialized message data
    pub fn from_indexes_and_messages(indexes: IggyIndexesMut, messages: BytesMut) -> Self {
        Self { indexes, messages }
    }

    /// Creates a new messages container from a slice of IggyMessage objects.
    ///
    /// # Note
    /// This function should be used only for testing purposes, because it creates
    /// deep copies of the messages.
    ///
    /// # Arguments
    ///
    /// * `messages` - Slice of message objects to store
    /// * `messages_size` - Total size of all messages in bytes
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut messages_buffer = BytesMut::with_capacity(messages_size as usize);
        let mut indexes_buffer = IggyIndexesMut::with_capacity(messages.len(), 0);
        let mut position = 0;

        for message in messages {
            let bytes = message.to_bytes();
            messages_buffer.put_slice(&bytes);
            position += message.get_size_bytes().as_bytes_u32();
            indexes_buffer.insert(0, position, 0);
        }

        Self::from_indexes_and_messages(indexes_buffer, messages_buffer)
    }

    /// Creates a lending iterator that yields mutable views of messages.
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.messages)
    }

    /// Creates an iterator that yields immutable views of messages.
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Returns the number of messages in the batch.
    pub fn count(&self) -> u32 {
        self.indexes.len() as u32 / INDEX_SIZE as u32
    }

    /// Returns the total size of all messages in bytes.
    pub fn size(&self) -> u32 {
        self.messages.len() as u32
    }

    /// Prepares all messages in the batch for persistence by setting their offsets,
    /// timestamps, and other necessary fields.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The starting offset of the segment
    /// * `base_offset` - The base offset for this batch of messages
    /// * `current_position` - The current position in the segment
    ///
    /// # Returns
    ///
    /// An immutable `IggyMessagesBatch` ready for persistence
    pub async fn prepare_for_persistence(
        self,
        start_offset: u64,
        base_offset: u64,
        current_position: u32,
        deduplicator: Option<&MessageDeduplicator>,
    ) -> IggyMessagesBatch {
        let messages_count = self.count();
        if messages_count == 0 {
            return IggyMessagesBatch::empty();
        }

        let mut curr_abs_offset = base_offset;
        let mut curr_position = current_position;
        let mut curr_rel_offset: u32 = 0;

        // Prepare invalid messages indexes if deduplicator is provided, this
        // way we avoid creating a new vector if we don't need it.
        // The less allocation the better.
        let mut invalid_messages_indexes =
            deduplicator.map(|_| Vec::with_capacity(messages_count as usize));

        let (mut indexes, mut messages) = self.decompose();
        indexes.set_base_position(current_position);
        let mut iter = IggyMessageViewMutIterator::new(&mut messages);
        let timestamp = IggyTimestamp::now().as_micros();

        while let Some(mut message) = iter.next() {
            message.header_mut().set_offset(curr_abs_offset);
            message.header_mut().set_timestamp(timestamp);
            if message.header().id() == 0 {
                message.header_mut().set_id(random_id::get_uuid());
            }

            if let Some(deduplicator) = deduplicator {
                if !deduplicator.try_insert(message.header().id()).await {
                    warn!(
                        "Detected duplicate message ID {}, removing...",
                        message.header().id()
                    );
                    invalid_messages_indexes
                        .as_mut()
                        .unwrap()
                        .push(curr_rel_offset);
                }
            }

            message.update_checksum();

            let message_size = message.size() as u32;
            curr_position += message_size;

            let relative_offset = (curr_abs_offset - start_offset) as u32;
            indexes.set_offset_at(curr_rel_offset, relative_offset);
            indexes.set_position_at(curr_rel_offset, curr_position);
            indexes.set_timestamp_at(curr_rel_offset, timestamp);

            curr_abs_offset += 1;
            curr_rel_offset += 1;
        }

        if let Some(invalid_messages_indexes) = invalid_messages_indexes {
            if invalid_messages_indexes.is_empty() {
                return IggyMessagesBatch::new(
                    indexes.make_immutable(),
                    messages.freeze(),
                    messages_count,
                );
            }
            let batch = IggyMessagesBatchMut::from_indexes_and_messages(indexes, messages)
                .remove_messages(&invalid_messages_indexes, current_position);

            let messages_count = batch.count();

            let (indexes, messages) = batch.decompose();

            return IggyMessagesBatch::new(
                indexes.make_immutable(),
                messages.freeze(),
                messages_count,
            );
        }

        IggyMessagesBatch::new(indexes.make_immutable(), messages.freeze(), messages_count)
    }

    /// Returns the first timestamp in the batch
    pub fn first_timestamp(&self) -> u64 {
        IggyMessageView::new(&self.messages).header().timestamp()
    }

    /// Returns the last timestamp in the batch
    pub fn last_timestamp(&self) -> u64 {
        if self.is_empty() {
            return 0;
        }

        let last_index = self.count() as usize - 1;
        self.get_message_boundaries(last_index)
            .map(|(start, _)| {
                IggyMessageView::new(&self.messages[start..])
                    .header()
                    .timestamp()
            })
            .unwrap_or(0)
    }

    /// Checks if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Decomposes the batch into its constituent parts.
    pub fn decompose(self) -> (IggyIndexesMut, BytesMut) {
        (self.indexes, self.messages)
    }

    /// Get message position from the indexes at the given index
    pub fn position_at(&self, index: u32) -> Option<u32> {
        self.indexes.get(index).map(|index| index.position())
    }

    /// Calculates the start position of a message at the given index in the buffer
    fn message_start_position(&self, index: usize) -> Option<usize> {
        if index >= self.count() as usize {
            return None;
        }

        if index == 0 {
            Some(0)
        } else {
            self.position_at(index as u32 - 1)
                .map(|pos| (pos - self.indexes.base_position()) as usize)
        }
    }

    /// Calculates the end position of a message at the given index in the buffer
    fn message_end_position(&self, index: usize) -> Option<usize> {
        if index >= self.count() as usize {
            return None;
        }

        if index == self.count() as usize - 1 {
            Some(self.messages.len())
        } else {
            self.position_at(index as u32)
                .map(|pos| (pos - self.indexes.base_position()) as usize)
        }
    }

    /// Gets the byte range for a message at the given index
    fn get_message_boundaries(&self, index: usize) -> Option<(usize, usize)> {
        let start = self.message_start_position(index)?;
        let end = self.message_end_position(index)?;

        if start > self.messages.len() || end > self.messages.len() || start > end {
            return None;
        }

        Some((start, end))
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds or the message cannot be found.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
        self.get_message_boundaries(index)
            .map(|(start, end)| IggyMessageView::new(&self.messages[start..end]))
    }

    /// This helper function is used to parse newly appended chunks in the `new_buffer`.
    /// The function iterates over the range `[chunk_start..chunk_start + chunk_len]`,
    /// constructing `IggyMessageView` instances to compute message sizes. For each message,
    /// a corresponding index entry is created in `new_indexes`. The `offset_in_new_buffer`
    /// is incremented by each message’s size to preserve the correct offsets for
    /// subsequent messages in the new buffer.
    #[allow(clippy::too_many_arguments)]
    fn rebuild_indexes_for_chunk(
        new_buffer: &BytesMut,
        new_indexes: &mut IggyIndexesMut,
        offset_in_new_buffer: &mut u32,
        chunk_start: usize,
        chunk_len: usize,
    ) {
        let chunk_end = chunk_start + chunk_len;
        let mut current = chunk_start;

        while current < chunk_end {
            let view = IggyMessageView::new(&new_buffer[current..]);
            let msg_size = view.size();
            *offset_in_new_buffer += msg_size as u32;
            new_indexes.insert(0, *offset_in_new_buffer, 0);

            current += msg_size;
        }
    }

    /// Removes messages at the specified indexes and returns a new batch.
    ///
    /// This function efficiently creates a new `IggyMessagesBatchMut` by copying only the
    /// messages that should be kept, and rebuilding the index entries. Note that `put()`
    /// can be memmove underneath due to the way memory is handled.
    ///
    /// # Arguments
    ///
    /// * `indexes_to_remove` - A slice of message indexes (0-based) to remove
    ///
    /// # Returns
    ///
    /// A new `IggyMessagesBatchMut` with the specified messages removed
    pub fn remove_messages(self, indexes_to_remove: &[u32], current_position: u32) -> Self {
        /*
            A temporary list of message boundaries is first collected for each index
            that should be removed. Chunks of data that are not removed are appended
            to a new buffer, and indexes are rebuilt to reflect the shifted positions.
            In this process, split_to() is used to carve out slices from the source
            buffer, and those slices are either copied or discarded, depending on
            whether they are part of the messages that are to be removed.
            This allows for avoiding copying unnecessary data and ensures that indexes
            match the newly constructed buffer.
        */

        if indexes_to_remove.is_empty() || self.is_empty() {
            return self;
        }

        let msg_count = self.count() as usize;
        if indexes_to_remove.len() >= msg_count {
            return IggyMessagesBatchMut::default();
        }

        let current_size = self.size();
        let mut size_to_remove = 0;
        let boundaries_to_remove: Vec<(usize, usize)> = indexes_to_remove
            .iter()
            .filter_map(|&idx| {
                self.get_message_boundaries(idx as usize)
                    .inspect(|boundaries| {
                        size_to_remove += (boundaries.1 - boundaries.0) as u32;
                    })
            })
            .collect();

        assert_eq!(
            boundaries_to_remove.len(),
            indexes_to_remove.len(),
            "Could not retrieve valid boundaries for some message indexes: {:?}, boundaries: {:?}",
            indexes_to_remove,
            boundaries_to_remove
        );

        let new_size = current_size - size_to_remove;
        let new_message_count = msg_count as u32 - indexes_to_remove.len() as u32;

        let mut new_buffer = BytesMut::with_capacity(new_size as usize);
        let mut new_indexes =
            IggyIndexesMut::with_capacity(new_message_count as usize, current_position);

        let mut source = self.messages;
        let mut last_pos = 0_usize;
        let mut new_pos = current_position;

        for &(start, end) in &boundaries_to_remove {
            if start > last_pos {
                let keep_len = start - last_pos;
                let chunk = source.split_to(keep_len);
                let chunk_start_in_new_buffer = new_buffer.len();
                new_buffer.put(chunk);

                Self::rebuild_indexes_for_chunk(
                    &new_buffer,
                    &mut new_indexes,
                    &mut new_pos,
                    chunk_start_in_new_buffer,
                    keep_len,
                );
            }

            let removed_message_size = end - start;
            if removed_message_size > 0 {
                let _ = source.split_to(removed_message_size);
            }

            last_pos = end;
        }

        if !source.is_empty() {
            let chunk_start_in_new_buffer = new_buffer.len();
            let chunk_len = source.len();
            new_buffer.put(source);
            Self::rebuild_indexes_for_chunk(
                &new_buffer,
                &mut new_indexes,
                &mut new_pos,
                chunk_start_in_new_buffer,
                chunk_len,
            );
        }

        IggyMessagesBatchMut::from_indexes_and_messages(new_indexes, new_buffer)
    }

    /// Validates the structure of the indexes (sizes, counts, etc.)
    fn validate_indexes_structure(&self) -> Result<(), IggyError> {
        let indexes_count = self.indexes.count();
        let indexes_size = self.indexes.size();

        if indexes_size % INDEX_SIZE as u32 != 0 {
            error!(
                "Indexes size {} is not a multiple of index size {}",
                indexes_size, INDEX_SIZE
            );
            return Err(IggyError::InvalidIndexesByteSize(indexes_size));
        }

        if indexes_count != self.count() {
            error!(
                "Indexes count {} does not match messages count {}",
                indexes_count,
                self.count()
            );
            return Err(IggyError::InvalidIndexesCount(indexes_count, self.count()));
        }

        Ok(())
    }

    fn validate_message_contents(&self) -> Result<(), IggyError> {
        let mut messages_count = 0;
        let mut messages_size = 0;
        let mut prev_offset = 0;
        let mut prev_position = 0;

        for i in 0..self.count() {
            let index = self.validate_index_at(i)?;
            let message = self.validate_message_at(i)?;

            if message.header().offset() < prev_offset {
                error!(
                "Offset of previous message: {} is smaller than current message {} at offset {}",
                prev_offset,
                message.header().offset(),
                i
            );
                return Err(IggyError::InvalidOffset(message.header().offset()));
            }

            if index.position() < prev_position {
                error!(
                "Position of previous message: {} is smaller than current message {} at offset {}",
                prev_position,
                index.position(),
                i
            );
                return Err(IggyError::CannotReadIndexPosition);
            }

            prev_offset = message.header().offset();
            prev_position = index.position();
            messages_size += message.size();
            messages_count += 1;
        }

        let indexes_count = self.indexes.count();
        if indexes_count != messages_count {
            error!(
                "Indexes count {} does not match messages count {}",
                indexes_count, messages_count
            );
            return Err(IggyError::InvalidMessagesCount);
        }

        if messages_size != self.messages.len() {
            error!(
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

    /// Validates a specific index entry
    fn validate_index_at(&self, i: u32) -> Result<IggyIndexView, IggyError> {
        let index_view = match self.indexes.get(i) {
            Some(view) => view,
            None => {
                error!("Index {} is missing", i);
                return Err(IggyError::MissingIndex(i));
            }
        };

        if index_view.offset() != 0 {
            error!(
                "Non-zero offset {} at index: {}, messages coming from network always have offset 0",
                index_view.offset(),
                i
            );
            return Err(IggyError::NonZeroOffset(index_view.offset() as u64, i));
        }

        if index_view.timestamp() != 0 {
            error!(
                "Non-zero timestamp {} at index: {}, messages coming from network always have timestamp 0",
                index_view.timestamp(),
                i
            );
            return Err(IggyError::NonZeroTimestamp(index_view.timestamp(), i));
        }

        Ok(index_view)
    }

    /// Validates a specific message
    fn validate_message_at(&self, i: u32) -> Result<IggyMessageView, IggyError> {
        let message = match self.get(i as usize) {
            Some(msg) => msg,
            None => {
                error!("Message at index {} is missing", i);
                return Err(IggyError::MissingIndex(i));
            }
        };

        if message.payload().len() as u32 > MAX_PAYLOAD_SIZE {
            error!(
                "Message payload size {} B exceeds maximum payload size {} B",
                message.payload().len(),
                MAX_PAYLOAD_SIZE
            );
            return Err(IggyError::TooBigMessagePayload);
        }

        if message.size() < IGGY_MESSAGE_HEADER_SIZE {
            error!(
                "Message size {} B is less than minimum message size {} B (header)",
                message.size(),
                IGGY_MESSAGE_HEADER_SIZE
            );
            return Err(IggyError::TooSmallMessage(
                message.size() as u32,
                IGGY_MESSAGE_HEADER_SIZE as u32,
            ));
        }

        if let Some(user_headers) = message.user_headers() {
            if user_headers.len() as u32 > MAX_USER_HEADERS_SIZE {
                error!(
                    "Message user headers size {} B exceeds maximum size {} B",
                    user_headers.len(),
                    MAX_USER_HEADERS_SIZE
                );
                return Err(IggyError::TooBigUserHeaders);
            }
        }

        Ok(message)
    }
}

impl Validatable<IggyError> for IggyMessagesBatchMut {
    fn validate(&self) -> Result<(), IggyError> {
        if self.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        self.validate_indexes_structure()?;
        self.validate_message_contents()
    }
}

impl Deref for IggyMessagesBatchMut {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}

/// Iterator over messages batch that yields immutable message views
pub struct IggyMessageViewIterator<'a> {
    buffer: &'a BytesMut,
    position: usize,
}

impl<'a> IggyMessageViewIterator<'a> {
    pub fn new(buffer: &'a BytesMut) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<'a> Iterator for IggyMessageViewIterator<'a> {
    type Item = IggyMessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let remaining = &self.buffer[self.position..];
        if remaining.len() < IGGY_MESSAGE_HEADER_SIZE {
            error!(
                "Buffer too small for message header at position {}, buffer len: {}",
                self.position,
                self.buffer.len()
            );
            return None;
        }

        let header_view = IggyMessageHeaderView::new(remaining);
        let message_size = header_view.payload_length()
            + header_view.user_headers_length()
            + IGGY_MESSAGE_HEADER_SIZE;

        if message_size > remaining.len() {
            error!(
                "Message size {} exceeds remaining buffer size {} at position {}",
                message_size,
                remaining.len(),
                self.position
            );
            return None;
        }

        let message_view =
            IggyMessageView::new(&self.buffer[self.position..self.position + message_size]);
        self.position += message_size;

        Some(message_view)
    }
}
