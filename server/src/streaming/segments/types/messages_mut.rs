use super::message_view_mut::IggyMessageViewMutIterator;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::prelude::*;
use lending_iterator::prelude::*;
use serde::{Deserialize, Serialize};

/// A container for mutable messages
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessagesMut {
    /// The number of messages in the buffer
    #[serde(skip)]
    count: u32,

    /// The buffer containing the messages
    buffer: BytesMut,
}

impl BytesSerializable for IggyMessagesMut {
    fn to_bytes(&self) -> Bytes {
        self.buffer.clone().freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Self {
            buffer: BytesMut::from(&bytes[..]),
            count: 0,
        })
    }
}

impl Sizeable for IggyMessagesMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl IggyMessagesMut {
    /// Create a new messages container from a buffer
    pub fn new(buffer: BytesMut) -> Self {
        Self { buffer, count: 0 }
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut buffer = BytesMut::with_capacity(messages_size as usize);
        for message in messages {
            buffer.extend_from_slice(&message.to_bytes());
        }
        let mut messages_mut = Self::new(buffer);
        messages_mut.count = messages.len() as u32;
        messages_mut
    }

    /// Extends the messages container with another set of mutable messages by consuming them
    pub fn extend(&mut self, messages: IggyMessagesMut) {
        let count = messages.count();
        let buffer = messages.into_inner();
        self.buffer.put(buffer);
        self.count += count;
    }

    /// Sets the capacity of the messages container
    pub fn set_capacity(&mut self, capacity: u32) {
        self.buffer.reserve(capacity as usize);
    }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.buffer)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Set the message count
    pub fn set_count(&mut self, count: u32) {
        self.count = count;
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Get the buffer
    pub fn into_inner(self) -> BytesMut {
        self.buffer
    }

    /// Prepares all messages in the batch for persistence by setting their offsets, timestamps,
    /// and other necessary fields. This method should be called before the messages are written to disk.
    /// Returns the number of messages processed.
    pub fn prepare_for_persistence(&mut self, base_offset: u64, timestamp: u64) -> u32 {
        let mut processed_count = 0;
        let mut current_offset = base_offset;

        let mut iterator = self.iter_mut();
        while let Some(message_result) = LendingIterator::next(&mut iterator) {
            if let Ok(mut message) = message_result {
                message.msg_header_mut().set_offset(current_offset);

                if message.msg_header().timestamp() == 0 {
                    message.msg_header_mut().set_timestamp(timestamp);
                }

                message.update_checksum();

                current_offset += 1;
                processed_count += 1;
            }
        }

        processed_count
    }

    pub fn make_immutable(self) -> IggyMessages {
        IggyMessages::new(self.buffer.freeze(), self.count)
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
