use super::{message_view::IggyMessageViewIterator, IggyMessage};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// An immutable messages container that holds a buffer of messages
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessages {
    /// The number of messages in the buffer
    #[serde(skip)]
    count: u32,
    /// The buffer containing the messages
    buffer: Bytes,
}

impl IggyMessages {
    /// Create a new messages container from a buffer
    pub fn new(buffer: Bytes, count: u32) -> Self {
        Self { buffer, count }
    }

    /// Creates a empty messages container with capacity
    pub fn with_capacity(capacity: u32) -> Self {
        Self::new(BytesMut::with_capacity(capacity as usize).freeze(), 0)
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage]) -> Self {
        let mut buffer = BytesMut::new();
        for message in messages {
            buffer.extend_from_slice(&message.to_bytes());
        }
        Self::new(buffer.freeze(), messages.len() as u32)
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

    /// Get access to the underlying buffer shallow  copy
    pub fn shallow_copy(&self) -> Bytes {
        self.buffer.clone()
    }

    pub fn into_inner(self) -> Bytes {
        self.buffer
    }

    /// Convert to messages
    pub fn to_messages(self) -> Vec<IggyMessage> {
        let mut messages = Vec::with_capacity(self.count as usize);
        let mut position = 0;

        while position < self.buffer.len() {
            let remaining = self.buffer.slice(position..);
            if let Ok(message) = IggyMessage::from_bytes(remaining) {
                let message_size = message.get_size_bytes().as_bytes_usize();
                position += message_size;
                messages.push(message);
            } else {
                break;
            }
        }

        messages
    }
}

impl BytesSerializable for IggyMessages {
    fn to_bytes(&self) -> Bytes {
        self.buffer.clone()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut messages_count = 0;
        let iterator = IggyMessageViewIterator::new(&bytes);

        for result in iterator {
            result;
            messages_count += 1;
        }

        Ok(Self {
            buffer: bytes,
            count: messages_count,
        })
    }
}

impl Sizeable for IggyMessages {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl Default for IggyMessages {
    fn default() -> Self {
        Self::new(BytesMut::new().freeze(), 0)
    }
}
