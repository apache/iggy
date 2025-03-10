use super::{IggyMessagesMut, Index};
use crate::streaming::segments::types::IggyMessagesSlice;
use iggy::prelude::IggyMessages;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;

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

    /// A multiple buffers containing all accumulated messages
    messages: Vec<IggyMessages>,

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
            let msg_size = message.size() as u32;

            let offset = message.msg_header().offset();
            let relative_offset = (offset - self.base_offset) as u32;
            let position = current_position;
            let timestamp = message.msg_header().timestamp();

            self.indexes.push(Index {
                offset: relative_offset,
                position,
                timestamp,
            });

            current_position += msg_size;
        }
        self.messages.push(batch);

        self.messages_count += batch_messages_count;
        self.current_size += IggyByteSize::from(batch_size as u64);
        self.current_offset = self.base_offset + self.messages_count as u64 - 1;
        self.current_timestamp = IggyTimestamp::now().as_micros();

        batch_messages_count
    }
    pub fn get_messages_by_offset(&self, start_offset: u64, end_offset: u64) -> IggyMessagesSlice {
        let start_idx = (start_offset - self.base_offset) as usize;
        let end_idx = (end_offset - self.base_offset) as usize; // inclusive
        if self.indexes.is_empty() || start_idx >= self.indexes.len() {
            return IggyMessagesSlice::empty();
        }
        let start_byte = self.indexes[start_idx].position as usize;
        let end_byte = if end_idx + 1 < self.indexes.len() {
            self.indexes[end_idx + 1].position as usize
        } else {
            self.current_size.as_bytes_usize()
        };

        let mut cumulative = Vec::with_capacity(self.messages.len());
        let mut sum = 0usize;
        for msg in &self.messages {
            sum += msg.buffer().len();
            cumulative.push(sum);
        }

        let mut buffers = Vec::new();
        for (i, &cum) in cumulative.iter().enumerate() {
            let batch_start = if i == 0 { 0 } else { cumulative[i - 1] };
            let batch_end = cum;
            if batch_end <= start_byte {
                continue;
            }
            if batch_start >= end_byte {
                break;
            }
            let slice_start = if start_byte > batch_start {
                start_byte - batch_start
            } else {
                0
            };
            let slice_end = if end_byte < batch_end {
                end_byte - batch_start
            } else {
                batch_end - batch_start
            };
            buffers.push(
                self.messages[i]
                    .shallow_copy()
                    .slice(slice_start..slice_end),
            );
        }

        let count = if end_idx >= start_idx {
            (end_idx - start_idx + 1) as u32
        } else {
            0
        };

        IggyMessagesSlice::from_buffers(buffers, count)
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

    /// Takes ownership of the accumulator and returns the messages and indexes
    pub fn materialize(self) -> (Vec<IggyMessages>, Vec<Index>) {
        (self.messages, self.indexes)
    }
}
