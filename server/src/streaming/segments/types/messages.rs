use bytes::{Bytes, BytesMut};
use iggy::prelude::*;
use std::ops::Deref;

/// An immutable messages container that holds a buffer of messages
#[derive(Clone, Debug)]
pub struct IggyMessages {
    /// The number of messages in the buffer
    count: u32,
    /// The buffer containing the messages
    buffer: Bytes,
}

impl IggyMessages {
    /// Create a new messages container from a buffer
    pub fn new(buffer: Bytes, count: u32) -> Self {
        Self { buffer, count }
    }

    /// Creates a empty messages container
    pub fn empty() -> Self {
        Self::new(BytesMut::new().freeze(), 0)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Get access to the underlying buffer
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Get index of first message
    pub fn first_index(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.msg_header().offset())
            .unwrap_or(0)
    }

    /// Get timestamp of first message
    pub fn first_timestamp(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.msg_header().timestamp())
            .unwrap_or(0)
    }

    /// Returns a contiguous slice (as a new `IggyMessages`) of up to `count` messages
    /// whose message headers have an offset greater than or equal to the provided `start_offset`.
    ///
    /// If no messages meet the criteria, returns `None`.
    pub fn slice_by_offset(&self, start_offset: u64, count: u32) -> Option<Self> {
        // Return an empty container if there are no messages or if count is zero.
        if self.count == 0 || count == 0 {
            return Some(Self::empty());
        }

        // Build metadata for each message: (offset, byte offset, message size)
        let mut meta: Vec<(u64, usize, usize)> = Vec::with_capacity(self.count as usize);
        let mut byte_offset: usize = 0;
        for msg in self.iter() {
            let offset = msg.msg_header().offset();
            let size = msg.size();
            meta.push((offset, byte_offset, size));
            byte_offset += size;
        }

        // Use partition_point to find the first message with offset >= target.
        let pos = meta.partition_point(|&(offset, _, _)| offset < start_offset);
        if pos == meta.len() {
            // No message meets the offset criteria.
            return None;
        }

        // Determine the range of messages to include (at most `count` messages).
        let end_idx = (pos + count as usize).min(meta.len());
        let start_byte = meta[pos].1;

        // Calculate the end offset: last message's starting position plus its size.
        let last = meta[end_idx - 1];
        let end_byte = last.1 + last.2;

        // Create a new IggyMessages from the selected byte range.
        Some(Self::new(
            self.buffer.slice(start_byte..end_byte),
            (end_idx - pos) as u32,
        ))
    }

    /// Returns a contiguous slice (as a new `IggyMessages`) of up to `count` messages
    /// whose message headers have a timestamp greater than or equal to the provided `timestamp`.
    ///
    /// If no messages meet the criteria, returns `None`.
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<Self> {
        // Return an empty container if there are no messages or if count is zero.
        if self.count == 0 || count == 0 {
            return Some(Self::empty());
        }

        // Build metadata for each message: (timestamp, byte offset, message size)
        let mut meta: Vec<(u64, usize, usize)> = Vec::with_capacity(self.count as usize);
        let mut offset: usize = 0;
        for msg in self.iter() {
            let ts = msg.msg_header().timestamp();
            let size = msg.size();
            meta.push((ts, offset, size));
            offset += size;
        }

        // Find the first message with timestamp >= target using linear search
        // Linear search is more reliable than binary search when timestamps might be identical
        let mut pos = 0;
        while pos < meta.len() && meta[pos].0 < timestamp {
            pos += 1;
        }

        if pos == meta.len() {
            // No message meets the timestamp criteria.
            return None;
        }

        // Determine the range of messages to include (at most `count` messages).
        let end_idx = (pos + count as usize).min(meta.len());
        let start_byte = meta[pos].1;

        // Calculate the end offset: last message's starting offset plus its size.
        let last = meta[end_idx - 1];
        let end_byte = last.1 + last.2;

        // Create a new IggyMessages from the selected byte range.
        Some(Self::new(
            self.buffer.slice(start_byte..end_byte),
            (end_idx - pos) as u32,
        ))
    }

    /// Extract a subset of messages based on index range
    ///
    /// # Arguments
    /// * `start_idx` - The index of the first message to include (0-based)
    /// * `end_idx` - The index of the last message to include (0-based, inclusive)
    ///
    /// # Returns
    /// A new IggyMessages instance containing only the specified message range
    pub fn slice(&self, start_idx: usize, end_idx: usize) -> Option<Self> {
        if self.count == 0 || start_idx > end_idx || start_idx >= self.count as usize {
            return None;
        }

        let clamped_end = end_idx.min(self.count as usize - 1);
        let num_messages = clamped_end - start_idx + 1;

        if num_messages == 0 {
            return Some(Self::empty());
        }

        if start_idx == 0 && clamped_end == self.count as usize - 1 {
            return Some(self.clone());
        }

        let iter = self.iter();
        let mut start_pos = 0;
        let mut end_pos = 0;
        let mut current_pos = 0;

        for (current_idx, message) in iter.enumerate() {
            let message_size = message.size();

            if current_idx == start_idx {
                start_pos = current_pos;
            }

            current_pos += message_size;

            if current_idx == clamped_end {
                end_pos = current_pos;
                break;
            }
        }

        if end_pos > start_pos {
            let slice_buffer = self.buffer.slice(start_pos..end_pos);
            return Some(Self::new(slice_buffer, num_messages as u32));
        }

        None
    }
}

impl Sizeable for IggyMessages {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl Deref for IggyMessages {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}
