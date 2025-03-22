use super::{message_view_mut::IggyMessageViewMutIterator, IggyMessages};
use crate::streaming::segments::indexes::IggyIndexesMut;
use bytes::{BufMut, BytesMut};
use iggy::prelude::*;
use lending_iterator::prelude::*;
use std::ops::Deref;

/// A container for mutable messages
#[derive(Debug, Default)]
pub struct IggyMessagesMut {
    /// Number of messages in the buffer
    count: u32,
    /// The buffer containing all messages
    buffer: BytesMut,
}

impl Sizeable for IggyMessagesMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl IggyMessagesMut {
    /// Create a new messages container with a specified capacity
    pub fn with_capacity(capacity: u32) -> Self {
        Self {
            count: 0,
            buffer: BytesMut::with_capacity(capacity as usize),
        }
    }

    /// Create a new messages container from a existing buffer of bytes
    pub fn from_bytes(bytes: BytesMut, messages_count: u32) -> Self {
        Self {
            count: messages_count,
            buffer: bytes,
        }
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut buffer = BytesMut::with_capacity(messages_size as usize);
        for message in messages {
            let bytes = message.to_bytes();
            buffer.put_slice(&bytes);
        }

        Self::from_bytes(buffer, messages.len() as u32)
    }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Prepares all messages in the batch for persistence by setting their offsets, timestamps,
    /// and other necessary fields. This method should be called before the messages are written to disk.
    /// Returns the prepared, immutable messages.
    pub fn prepare_for_persistence(
        mut self,
        base_offset: u64,
        timestamp: u64,
        current_position: u32,
        indexes: &mut IggyIndexesMut,
    ) -> IggyMessages {
        let mut current_offset = base_offset;
        let mut current_position = current_position;
        let mut iter = self.iter_mut();

        while let Some(mut message) = iter.next() {
            message.msg_header_mut().set_offset(current_offset);
            message.msg_header_mut().set_timestamp(timestamp);
            message.update_checksum();

            indexes.add_unsaved_index(
                (current_offset - base_offset) as u32,
                current_position,
                timestamp,
            );

            current_offset += 1;
            current_position += message.size() as u32;
        }

        let buffer = self.buffer.freeze();
        IggyMessages::new(buffer, self.count)
    }

    /// Returns true if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Deref for IggyMessagesMut {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl From<&[IggyMessage]> for IggyMessagesMut {
    fn from(messages: &[IggyMessage]) -> Self {
        let messages_size: u32 = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
            .sum();
        Self::from_messages(messages, messages_size)
    }
}
