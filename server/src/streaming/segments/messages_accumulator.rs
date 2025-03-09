use super::{IggyMessagesMut, Index};
use bytes::{Bytes, BytesMut};
use iggy::prelude::{IggyMessageView, IggyMessages};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::mem;

/// A container that accumulates messages before they are written to disk
#[derive(Debug, Default)]
pub struct MessagesAccumulator {
    /// Base offset of the first message
    base_offset: u64,

    /// Base timestamp of the first message
    base_timestamp: u64,

    /// Current size of all accumulated messages
    current_size: IggyByteSize,

    /// Current maximum offset
    current_offset: u64,

    /// Current maximum timestamp
    current_timestamp: u64,

    /// A single buffer containing all accumulated messages
    messages: Vec<u8>,

    /// Indexes
    indexes: Vec<Index>,

    /// Number of messages in the accumulator
    messages_count: u32,
}

impl MessagesAccumulator {
    /// Coalesces a batch of messages into the accumulator
    /// This method also prepares the messages for persistence by setting their
    /// offset, timestamp, record size, and checksum fields
    /// Returns the number of messages in the batch
    pub fn coalesce_batch(
        &mut self,
        current_offset: u64,
        current_position: u32,
        mut batch: IggyMessagesMut,
    ) -> u32 {
        // TODO(hubcio): add capacity
        let batch_messages_count = batch.count();

        if batch_messages_count == 0 {
            return 0;
        }

        if self.messages.is_empty() {
            self.base_offset = current_offset;
            self.base_timestamp = IggyTimestamp::now().as_micros();
            self.current_offset = current_offset;
            self.current_timestamp = self.base_timestamp;
        }

        batch.prepare_for_persistence(self.base_offset, self.current_timestamp);

        let batch_size = batch.size();
        let batch = batch.make_immutable();
        let mut current_position = current_position;
        for message in batch.iter() {
            let msg_size = message.as_ref().unwrap().size() as u32;

            let offset = message.as_ref().unwrap().msg_header().unwrap().offset();
            let relative_offset = (offset - self.base_offset) as u32;
            let position = current_position;
            let timestamp = message.as_ref().unwrap().msg_header().unwrap().timestamp();

            self.indexes.push(Index {
                offset: relative_offset,
                position,
                timestamp,
            });

            current_position += msg_size;
        }
        let messages: Vec<u8> = batch.into_inner().into();

        self.messages.extend(messages);

        self.messages_count += batch_messages_count;
        self.current_size += IggyByteSize::from(batch_size as u64);
        self.current_offset = self.base_offset + self.messages_count as u64 - 1;
        self.current_timestamp = IggyTimestamp::now().as_micros();

        batch_messages_count
    }

    pub fn get_messages_by_offset(&self, start_offset: u64, end_offset: u64) -> IggyMessages {
        let relative_start = start_offset - self.base_offset;
        let relative_end = end_offset - self.base_offset;
        let mut pos = 0;
        let mut msg_index = 0;
        let mut start_byte = None;
        let mut end_byte = None;
        while pos < self.messages.len() {
            let view = IggyMessageView::new(&self.messages[pos..]).unwrap();
            if msg_index == relative_start {
                start_byte = Some(pos);
            }
            pos += view.size();
            msg_index += 1;
            if msg_index == relative_end + 1 {
                end_byte = Some(pos);
                break;
            }
        }
        let start_byte = start_byte.unwrap_or(0);
        let end_byte = end_byte.unwrap_or(self.messages.len());
        let slice = &self.messages[start_byte..end_byte];
        let count = if relative_end >= relative_start {
            relative_end - relative_start + 1
        } else {
            0
        };
        IggyMessages::new(Bytes::copy_from_slice(slice), count as u32)
    }

    /// Checks if the accumulator is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() || self.messages_count == 0
    }

    /// Returns the number of unsaved messages
    pub fn unsaved_messages_count(&self) -> usize {
        self.messages_count as usize
    }

    /// Returns the maximum offset in the accumulator
    pub fn max_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the maximum timestamp in the accumulator
    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    /// Returns the base offset of the accumulator
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the base timestamp of the accumulator
    pub fn batch_base_timestamp(&self) -> u64 {
        self.base_timestamp
    }

    /// Takes ownership of the accumulator and returns the messages
    pub fn materialize(self) -> (IggyMessages, Vec<Index>) {
        let messages = IggyMessages::new(Bytes::from(self.messages), self.messages_count);
        let indexes = self.indexes;
        (messages, indexes)
    }
}
