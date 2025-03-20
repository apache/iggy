use super::{IggyIndexes, IggyMessageHeaderView, IggyMessageViewIterator};
use crate::{
    error::IggyError,
    models::messaging::INDEX_SIZE,
    prelude::{BytesSerializable, IggyByteSize, Sizeable},
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// An immutable messages container that holds a buffer of messages
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessagesBatch {
    /// The number of messages in the batch
    count: u32,
    /// The byte-indexes of messages in the buffer, represented as array of u32's
    /// Offsets are relative
    /// Each index position points to the END position of a message (start of next message)
    indexes: IggyIndexes,
    /// The buffer containing the messages
    messages: Bytes,
}

impl IggyMessagesBatch {
    /// Create a new messages container from a buffer
    pub fn new(indexes: IggyIndexes, messages: Bytes, count: u32) -> Self {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(indexes.len() % INDEX_SIZE, 0);
            let indexes_count = indexes.len() / INDEX_SIZE;
            debug_assert_eq!(indexes_count, count as usize);

            if count > 0 {
                let first_position = indexes.get(0).unwrap().position();

                let iter = IggyMessageViewIterator::new(&messages);
                let mut current_position = first_position;

                for (i, message) in iter.enumerate().skip(1) {
                    if i < count as usize {
                        current_position += message.size();
                        let expected_position = indexes.get(i as u32).unwrap().position();
                        debug_assert_eq!(
                            current_position, expected_position,
                            "Message {} position mismatch: {} != {}",
                            i, current_position, expected_position
                        );
                    }
                }
            }
        }

        Self {
            count,
            indexes,
            messages,
        }
    }

    /// Creates a empty messages container
    pub fn empty() -> Self {
        Self::new(IggyIndexes::empty(), BytesMut::new().freeze(), 0)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        debug_assert_eq!(self.indexes.len() % INDEX_SIZE, 0);

        // For N messages, we might have either N or N+1 indexes
        // N+1 indexes define the boundaries of N messages (fence post pattern)
        let indexes_count = self.indexes.len() / INDEX_SIZE;
        debug_assert!(
            self.count as usize == indexes_count || self.count as usize == indexes_count - 1,
            "Mismatch between message count and indexes count"
        );

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

    /// Get index of first message
    pub fn first_index(&self) -> u64 {
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

    /// Helper method to read a position (u32) from the byte array at the given index
    fn read_position_at(&self, position_index: u32) -> u32 {
        let idx = position_index * INDEX_SIZE as u32;
        self.indexes.get(idx).unwrap().position()
    }

    /// Returns a contiguous slice (as a new `IggyMessagesBatch`) of up to `count` messages
    /// whose message headers have an offset greater than or equal to the provided `start_offset`.
    pub fn slice_by_offset(&self, start_offset: u64, count: u32) -> Option<Self> {
        if self.is_empty() || count == 0 {
            return None;
        }

        let indexes_count = self.indexes.len() / INDEX_SIZE;
        let mut pos = 0;

        for i in 0..indexes_count {
            let msg_start = if i == 0 {
                0
            } else {
                self.read_position_at(i as u32 - 1) as usize
            };

            let msg_offset = IggyMessageHeaderView::new(&self.messages[msg_start..]).offset();

            if msg_offset >= start_offset {
                pos = i;
                break;
            }

            if i == indexes_count - 1 {
                return None;
            }
        }

        let end_idx = std::cmp::min(pos + count as usize, indexes_count);

        let first_byte = if pos == 0 {
            0
        } else {
            self.read_position_at(pos as u32 - 1) as usize
        };

        let last_byte = self.read_position_at(end_idx as u32 - 1) as usize;
        let sub_buffer = self.messages.slice(first_byte..last_byte);

        let sub_indexes = self
            .indexes
            .slice_by_offset(pos as u32, end_idx as u32)
            .unwrap();

        Some(IggyMessagesBatch {
            count: (end_idx - pos) as u32,
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

        let indexes_count = self.indexes.len() / 4;
        let mut pos = None;

        for i in 0..indexes_count {
            let msg_start = if i == 0 {
                0
            } else {
                self.read_position_at(i as u32 - 1) as usize
            };

            let msg_ts = IggyMessageHeaderView::new(&self.messages[msg_start..]).timestamp();

            if msg_ts >= timestamp {
                pos = Some(i);
                break;
            }
        }

        let pos = pos?;

        let end_idx = std::cmp::min(pos + count as usize, indexes_count);

        let first_byte = if pos == 0 {
            0
        } else {
            self.read_position_at(pos as u32 - 1) as usize
        };

        let last_byte = self.read_position_at(end_idx as u32 - 1) as usize;

        let sub_buffer = self.messages.slice(first_byte..last_byte);

        let sub_indexes = self
            .indexes
            .slice_by_offset(pos as u32, end_idx as u32)
            .unwrap();

        Some(IggyMessagesBatch {
            count: (end_idx - pos) as u32,
            indexes: sub_indexes,
            messages: sub_buffer,
        })
    }
}

impl BytesSerializable for IggyMessagesBatch {
    fn to_bytes(&self) -> Bytes {
        self.messages.clone()
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("don't use");
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.count);
        buf.put_slice(&self.indexes);
        buf.put_slice(&self.messages);
    }

    fn get_buffer_size(&self) -> u32 {
        4 + self.indexes.len() as u32 + self.messages.len() as u32
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
