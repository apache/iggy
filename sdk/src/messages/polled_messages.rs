use crate::{
    error::IggyError,
    prelude::{BytesSerializable, IggyMessage, IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::error;

/// The wrapper on top of the collection of messages that are polled from the partition.
/// It consists of the following fields:
/// - `partition_id`: the identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `messages`: the collection of messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    /// The identifier of the partition. If it's '0', then there's no partition assigned to the consumer group member.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The count of messages.
    pub count: u32,
    /// The collection of messages.
    pub messages: Vec<IggyMessage>,
}

impl PolledMessages {
    pub fn empty() -> Self {
        Self {
            partition_id: 0,
            current_offset: 0,
            count: 0,
            messages: Vec::new(),
        }
    }
}

impl BytesSerializable for PolledMessages {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let partition_id = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let current_offset = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let messages = messages_from_bytes_and_count(bytes.slice(16..), count)?;

        Ok(Self {
            partition_id,
            current_offset,
            count,
            messages,
        })
    }
}

/// Convert Bytes to messages
fn messages_from_bytes_and_count(buffer: Bytes, count: u32) -> Result<Vec<IggyMessage>, IggyError> {
    let mut messages = Vec::with_capacity(count as usize);
    let mut position = 0;
    let buf_len = buffer.len();

    while position < buf_len {
        if position + IGGY_MESSAGE_HEADER_SIZE as usize > buf_len {
            break;
        }
        let header_bytes = buffer.slice(position..position + IGGY_MESSAGE_HEADER_SIZE as usize);
        let header = match IggyMessageHeader::from_bytes(header_bytes) {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to deserialize message header: {}", e);
                return Err(e);
            }
        };
        position += IGGY_MESSAGE_HEADER_SIZE as usize;

        let payload_end = position + header.payload_length as usize;
        if payload_end > buf_len {
            break;
        }
        let payload = buffer.slice(position..payload_end);
        position = payload_end;

        let headers: Option<Bytes> = if header.user_headers_length > 0 {
            Some(buffer.slice(position..position + header.user_headers_length as usize))
        } else {
            None
        };
        position += header.user_headers_length as usize;

        messages.push(IggyMessage {
            header,
            payload,
            user_headers: headers,
        });
    }

    Ok(messages)
}
