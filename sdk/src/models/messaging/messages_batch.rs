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

    pub fn indexes_slice(&self) -> &[u8] {
        &self.indexes
    }

    /// Decompose the batch into its components
    pub fn decompose(self) -> (u32, IggyIndexes, Bytes) {
        (self.count, self.indexes, self.messages)
    }

    /// Get index of first message
    pub fn first_offset(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.header().offset())
            .unwrap_or(0)
    }

    /// Get timestamp of first message
    pub fn first_timestamp(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.header().timestamp())
            .unwrap_or(0)
    }

    /// Get offset of last message
    pub fn last_offset(&self) -> u64 {
        self.iter()
            .last()
            .map(|msg| msg.header().offset())
            .unwrap_or(0)
    }

    /// Get timestamp of last message
    pub fn last_timestamp(&self) -> u64 {
        self.iter()
            .last()
            .map(|msg| msg.header().timestamp())
            .unwrap_or(0)
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

        // TODO(hubcio): this can be optimized via lookup
        let first_message_index = self
            .iter()
            .position(|msg| msg.header().offset() >= start_offset);

        first_message_index?;

        let first_message_index = first_message_index.unwrap();
        let last_message_index =
            std::cmp::min(first_message_index + count as usize, self.count() as usize);

        let sub_indexes = self.indexes.slice_by_offset(
            first_message_index as u32,
            (last_message_index - first_message_index) as u32,
        )?;

        let first_message_position = if first_message_index == 0 {
            0
        } else {
            self.position_at(first_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        let last_message_position = if last_message_index >= self.count() as usize {
            self.messages.len()
        } else {
            self.position_at(last_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        debug_assert!(
            first_message_position <= self.messages.len(),
            "First message position {} exceeds buffer length {}",
            first_message_position,
            self.messages.len()
        );
        debug_assert!(
            last_message_position <= self.messages.len(),
            "Last message position {} exceeds buffer length {}",
            last_message_position,
            self.messages.len()
        );

        let sub_buffer = self
            .messages
            .slice(first_message_position..last_message_position);

        Some(IggyMessagesBatch {
            count: (last_message_index - first_message_index) as u32,
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

        // TODO(hubcio): this can be optimized via binary search
        let first_message_index = self
            .iter()
            .position(|msg| msg.header().timestamp() >= timestamp);

        first_message_index?;

        let first_message_index = first_message_index.unwrap();
        let last_message_index =
            std::cmp::min(first_message_index + count as usize, self.count() as usize);

        let sub_indexes = self.indexes.slice_by_offset(
            first_message_index as u32,
            (last_message_index - first_message_index) as u32,
        )?;

        let first_message_position = if first_message_index == 0 {
            0
        } else {
            self.position_at(first_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        let last_message_position = if last_message_index >= self.count() as usize {
            self.messages.len()
        } else {
            self.position_at(last_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        debug_assert!(
            first_message_position <= self.messages.len(),
            "First message position {} exceeds buffer length {}",
            first_message_position,
            self.messages.len()
        );
        debug_assert!(
            last_message_position <= self.messages.len(),
            "Last message position {} exceeds buffer length {}",
            last_message_position,
            self.messages.len()
        );

        let sub_buffer = self
            .messages
            .slice(first_message_position..last_message_position);

        Some(IggyMessagesBatch {
            count: (last_message_index - first_message_index) as u32,
            indexes: sub_indexes,
            messages: sub_buffer,
        })
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
        if index >= self.count as usize {
            return None;
        }

        // TODO: refactor this
        let start_position = if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize - self.indexes.base_position() as usize
        };

        let end_position = if index == self.count as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize - self.indexes.base_position() as usize
        };

        debug_assert!(
            start_position <= self.messages.len(),
            "Start message position {} exceeds buffer length {}, index: {}, self.count: {}, self.base_position: {}",
            start_position,
            self.messages.len(),
            index,
            self.count,
            self.indexes.base_position()
        );
        debug_assert!(
            end_position <= self.messages.len(),
            "End message position {} exceeds buffer length {}, index: {}, self.count: {}, self.base_position: {}",
            end_position,
            self.messages.len(),
            index,
            self.count,
            self.indexes.base_position()
        );

        Some(IggyMessageView::new(
            &self.messages[start_position..end_position],
        ))
    }

    /// Consumes the batch and returns a vector of messages.
    pub fn into_messages(self) -> Vec<IggyMessage> {
        if self.is_empty() {
            return Vec::new();
        }

        let (count, indexes, buffer) = self.decompose();
        let mut result = Vec::with_capacity(count as usize);
        let base_position = indexes.base_position();

        for i in 0..count {
            let start_position = if i == 0 {
                0
            } else {
                let prev_idx = indexes.get(i - 1).map(|idx| idx.position()).unwrap_or(0);
                prev_idx as usize - base_position as usize
            };

            let end_position = if i == count - 1 {
                buffer.len()
            } else {
                let curr_idx = indexes.get(i).map(|idx| idx.position()).unwrap_or(0);
                curr_idx as usize - base_position as usize
            };

            if start_position < buffer.len()
                && end_position <= buffer.len()
                && start_position < end_position
            {
                if let Ok(message) =
                    IggyMessage::from_bytes(buffer.slice(start_position..end_position))
                {
                    result.push(message);
                }
            }
        }

        result
    }

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

    /// Get the message bytes at the specified index
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds (>= count)
    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.count as usize {
            panic!(
                "Index out of bounds: the len is {} but the index is {}",
                self.count, index
            );
        }

        let start_position = if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize
        };

        let end_position = if index == self.count as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize
        };

        &self.messages[start_position..end_position]
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
