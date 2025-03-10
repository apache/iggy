use super::{Partitioning, PartitioningKind};
use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, SEND_MESSAGES_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::{MAX_HEADERS_SIZE, MAX_PAYLOAD_SIZE};
use crate::models::messaging::{HeaderKey, HeaderValue};
use crate::models::messaging::{IggyMessage, IggyMessageHeader, IggyMessageViewIterator};
use crate::prelude::IGGY_MESSAGE_HEADER_SIZE;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::utils::varint::IggyVarInt;
use crate::validatable::Validatable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use uuid::Uuid;

/// `SendMessages` command is used to send messages to a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitioning` - to which partition the messages should be sent - either provided by the client or calculated by the server.
/// - `messages` - collection of messages to be sent using zero-copy message views.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SendMessages {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// To which partition the messages should be sent - either provided by the client or calculated by the server.
    pub partitioning: Partitioning,
    /// Number of messages to be sent.
    pub messages_count: u32,
    /// Messages collection
    pub messages: Bytes,
}

impl SendMessages {
    pub fn as_bytes(
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &[IggyMessage],
    ) -> Bytes {
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let partitioning_bytes = partitioning.to_bytes();
        let messages_count = messages.len() as u32;

        let total_size = stream_id_bytes.len()
        + topic_id_bytes.len()
        + partitioning_bytes.len()
        + 4  // For messages_count (u32)
        + messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_usize())
            .sum::<usize>();

        let mut bytes = BytesMut::with_capacity(total_size);

        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&partitioning_bytes);
        bytes.put_u32_le(messages_count);

        for message in messages {
            message.write_to_bytes_mut(&mut bytes);
        }

        bytes.freeze()
    }
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::default(),
            messages: Bytes::new(),
            messages_count: 0,
        }
    }
}

impl Command for SendMessages {
    fn code(&self) -> u32 {
        SEND_MESSAGES_CODE
    }
}

impl Validatable<IggyError> for SendMessages {
    fn validate(&self) -> Result<(), IggyError> {
        if self.messages.is_empty() || self.messages_count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        if self.partitioning.value.len() > 255
            || (self.partitioning.kind != PartitioningKind::Balanced
                && self.partitioning.value.is_empty())
        {
            return Err(IggyError::InvalidKeyValueLength);
        }

        let mut payload_size = 0;
        let mut message_count = 0;

        for message in IggyMessageViewIterator::new(&self.messages) {
            message_count += 1;

            // TODO(hubcio): IMHO validation of headers should be purely on SDK side
            // if let Some(headers) = message.headers() {
            //     for value in headers.values() {
            //         headers_size += value.value.len() as u32;
            //         if headers_size > MAX_HEADERS_SIZE {
            //             return Err(IggyError::TooBigHeadersPayload);
            //         }
            //     }
            // }

            let message_payload = message.payload();
            payload_size += message_payload.len() as u32;
            if payload_size > MAX_PAYLOAD_SIZE {
                return Err(IggyError::TooBigMessagePayload);
            }

            if message_payload.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
                return Err(IggyError::InvalidMessagePayloadLength);
            }
        }

        if message_count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        if payload_size == 0 {
            return Err(IggyError::EmptyMessagePayload);
        }

        Ok(())
    }
}

impl BytesSerializable for SendMessages {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let partitioning_bytes = self.partitioning.to_bytes();

        let metadata_len = stream_id_bytes.len()
            + topic_id_bytes.len()
            + partitioning_bytes.len()
            + std::mem::size_of::<u32>();

        let total_len = metadata_len + self.messages.len();

        let mut bytes = BytesMut::with_capacity(total_len);

        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&partitioning_bytes);
        bytes.put_u32_le(self.messages_count);
        bytes.put_slice(&self.messages);

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<SendMessages, IggyError> {
        if bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();

        if bytes.len() <= position {
            return Err(IggyError::InvalidCommand);
        }

        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();

        if bytes.len() <= position {
            return Err(IggyError::InvalidCommand);
        }

        let partitioning = Partitioning::from_bytes(bytes.slice(position..))?;
        position += partitioning.get_size_bytes().as_bytes_usize();

        if bytes.len() < position + 4 {
            return Err(IggyError::InvalidCommand);
        }

        let messages_count = u32::from_le_bytes(
            bytes
                .slice(position..position + 4)
                .as_ref()
                .try_into()
                .unwrap(),
        );
        position += 4;

        let messages = bytes.slice(position..);

        Ok(SendMessages {
            stream_id,
            topic_id,
            partitioning,
            messages_count,
            messages,
        })
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.messages_count;
        write!(
            f,
            "{}|{}|{}|messages.len:{}|messages_size:{}",
            self.stream_id,
            self.topic_id,
            self.partitioning,
            count,
            self.messages.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    //     //TODO: Fix me, fix those tests.
    //     #[test]
    //     fn should_be_serialized_as_bytes() {
    //         let message_1 = Message::from_str("hello 1").unwrap();
    //         let message_2 = Message::new(Some(2), "hello 2".into(), None);
    //         let message_3 = Message::new(Some(3), "hello 3".into(), None);
    //         let messages = vec![message_1, message_2, message_3];
    //         let command = SendMessages {
    //             stream_id: Identifier::numeric(1).unwrap(),
    //             topic_id: Identifier::numeric(2).unwrap(),
    //             partitioning: Partitioning::partition_id(4),
    //             messages,
    //         };

    //         let bytes = command.to_bytes();

    //         let mut position = 0;
    //         let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
    //         position += stream_id.get_size_bytes().as_bytes_usize();
    //         let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
    //         position += topic_id.get_size_bytes().as_bytes_usize();
    //         let key = Partitioning::from_bytes(bytes.slice(position..)).unwrap();
    //         position += key.get_size_bytes().as_bytes_usize();
    //         let messages = bytes.slice(position..);
    //         let command_messages = command
    //             .messages
    //             .iter()
    //             .fold(BytesMut::new(), |mut bytes_mut, message| {
    //                 bytes_mut.put(message.to_bytes());
    //                 bytes_mut
    //             })
    //             .freeze();

    //         assert!(!bytes.is_empty());
    //         assert_eq!(stream_id, command.stream_id);
    //         assert_eq!(topic_id, command.topic_id);
    //         assert_eq!(key, command.partitioning);
    //         assert_eq!(messages, command_messages);
    //     }

    //     #[test]
    //     fn should_be_deserialized_from_bytes() {
    //         let stream_id = Identifier::numeric(1).unwrap();
    //         let topic_id = Identifier::numeric(2).unwrap();
    //         let key = Partitioning::partition_id(4);

    //         let message_1 = Message::from_str("hello 1").unwrap();
    //         let message_2 = Message::new(Some(2), "hello 2".into(), None);
    //         let message_3 = Message::new(Some(3), "hello 3".into(), None);
    //         let messages = [
    //             message_1.to_bytes(),
    //             message_2.to_bytes(),
    //             message_3.to_bytes(),
    //         ]
    //         .concat();

    //         let key_bytes = key.to_bytes();
    //         let stream_id_bytes = stream_id.to_bytes();
    //         let topic_id_bytes = topic_id.to_bytes();
    //         let current_position = stream_id_bytes.len() + topic_id_bytes.len() + key_bytes.len();
    //         let mut bytes = BytesMut::with_capacity(current_position);
    //         bytes.put_slice(&stream_id_bytes);
    //         bytes.put_slice(&topic_id_bytes);
    //         bytes.put_slice(&key_bytes);
    //         bytes.put_slice(&messages);
    //         let bytes = bytes.freeze();
    //         let command = SendMessages::from_bytes(bytes.clone());
    //         assert!(command.is_ok());

    //         let messages_payloads = bytes.slice(current_position..);
    //         let mut position = 0;
    //         let mut messages = Vec::new();
    //         while position < messages_payloads.len() {
    //             let message = Message::from_bytes(messages_payloads.slice(position..)).unwrap();
    //             position += message.get_size_bytes().as_bytes_usize();
    //             messages.push(message);
    //         }

    //         let command = command.unwrap();
    //         assert_eq!(command.stream_id, stream_id);
    //         assert_eq!(command.topic_id, topic_id);
    //         assert_eq!(command.partitioning, key);
    //         for (index, message) in command.messages.iter().enumerate() {
    //             let command_message = &command.messages[index];
    //             assert_eq!(command_message.id, message.id);
    //             assert_eq!(command_message.length, message.length);
    //             assert_eq!(command_message.payload, message.payload);
    //         }
    //     }

    #[test]
    fn key_of_type_balanced_should_have_empty_value() {
        let key = Partitioning::balanced();
        assert_eq!(key.kind, PartitioningKind::Balanced);
        assert_eq!(key.length, 0);
        assert!(key.value.is_empty());
        assert_eq!(
            PartitioningKind::from_code(1).unwrap(),
            PartitioningKind::Balanced
        );
    }

    #[test]
    fn key_of_type_partition_should_have_value_of_const_length_4() {
        let partition_id = 1234u32;
        let key = Partitioning::partition_id(partition_id);
        assert_eq!(key.kind, PartitioningKind::PartitionId);
        assert_eq!(key.length, 4);
        assert_eq!(key.value, partition_id.to_le_bytes());
        assert_eq!(
            PartitioningKind::from_code(2).unwrap(),
            PartitioningKind::PartitionId
        );
    }

    #[test]
    fn key_of_type_messages_key_should_have_value_of_dynamic_length() {
        let messages_key = "hello world";
        let key = Partitioning::messages_key_str(messages_key).unwrap();
        assert_eq!(key.kind, PartitioningKind::MessagesKey);
        assert_eq!(key.length, messages_key.len() as u8);
        assert_eq!(key.value, messages_key.as_bytes());
        assert_eq!(
            PartitioningKind::from_code(3).unwrap(),
            PartitioningKind::MessagesKey
        );
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_0_should_fail() {
        let messages_key = "";
        let key = Partitioning::messages_key_str(messages_key);
        assert!(key.is_err());
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_greater_than_255_should_fail() {
        let messages_key = "a".repeat(256);
        let key = Partitioning::messages_key_str(&messages_key);
        assert!(key.is_err());
    }
}
