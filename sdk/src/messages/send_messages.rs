use super::{Partitioning, PartitioningKind};
use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, SEND_MESSAGES_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::messaging::{IggyMessage, IggyMessagesBatch, INDEX_SIZE};
use crate::prelude::IggyMessageView;
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// `SendMessages` command is used to send messages to a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitioning` - to which partition the messages should be sent - either provided by the client or calculated by the server.
/// - `batch` - collection of messages to be sent.
#[derive(Debug, PartialEq)]
pub struct SendMessages {
    /// Unique stream ID (numeric or name).
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    pub topic_id: Identifier,
    /// To which partition the messages should be sent - either provided by the client or calculated by the server.
    pub partitioning: Partitioning,
    /// Messages collection
    pub batch: IggyMessagesBatch,
}

impl SendMessages {
    pub fn as_bytes(
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &[IggyMessage],
    ) -> Bytes {
        let stream_id_size = stream_id.get_buffer_size();
        let topic_id_size = topic_id.get_buffer_size();
        let partitioning_size = partitioning.get_buffer_size();
        let messages_count = messages.len();
        let messages_count_size = std::mem::size_of::<u32>();
        let indexes_size = messages_count * INDEX_SIZE;

        let total_size = stream_id_size
            + topic_id_size
            + partitioning_size
            + messages_count_size
            + indexes_size
            + messages
                .iter()
                .map(|m| m.get_size_bytes().as_bytes_usize())
                .sum::<usize>();

        let mut bytes = BytesMut::with_capacity(total_size);

        stream_id.write_to_buffer(&mut bytes);
        topic_id.write_to_buffer(&mut bytes);
        partitioning.write_to_buffer(&mut bytes);
        bytes.put_u32_le(messages_count as u32);

        let mut current_position = bytes.len();

        bytes.put_bytes(0, indexes_size);

        let mut msgs_size: u32 = 0;
        for message in messages.iter() {
            message.write_to_buffer(&mut bytes);
            msgs_size += message.get_size_bytes().as_bytes_u64() as u32;
            write_value_at(&mut bytes, 0u64.to_le_bytes(), current_position);
            write_value_at(&mut bytes, msgs_size.to_le_bytes(), current_position + 4);
            write_value_at(&mut bytes, 0u64.to_le_bytes(), current_position + 8);
            current_position += INDEX_SIZE;
        }

        let out = bytes.freeze();

        debug_assert_eq!(
            total_size,
            out.len(),
            "Calculated SendMessages command byte size doesn't match actual command size",
        );

        out
    }
}

fn write_value_at<const N: usize>(slice: &mut [u8], value: [u8; N], position: usize) {
    let slice = &mut slice[position..position + N];
    let ptr = slice.as_mut_ptr();
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, N);
    }
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::default(),
            batch: IggyMessagesBatch::empty(),
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
        if self.partitioning.value.len() > 255
            || (self.partitioning.kind != PartitioningKind::Balanced
                && self.partitioning.value.is_empty())
        {
            return Err(IggyError::InvalidKeyValueLength);
        }

        self.batch.validate()?;

        Ok(())
    }
}

impl BytesSerializable for SendMessages {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(_bytes: Bytes) -> Result<SendMessages, IggyError> {
        panic!("should not be used")
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let messages_count = self.batch.count();
        let messages_size = self.batch.size();
        write!(
            f,
            "{}|{}|{}|messages_count:{}|messages_size:{}",
            self.stream_id, self.topic_id, self.partitioning, messages_count, messages_size
        )
    }
}

impl Serialize for SendMessages {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // In HTTP API, we expose:
        // - partitioning (kind, value)
        // - messages as an array of {id, payload, headers}
        // We don't expose stream_id and topic_id via JSON as they're in URL path

        let messages: Vec<HashMap<&str, serde_json::Value>> = self
            .batch
            .iter()
            .map(|msg_view: IggyMessageView<'_>| {
                let mut map = HashMap::with_capacity(self.batch.count() as usize);
                map.insert("id", serde_json::to_value(msg_view.header().id()).unwrap());

                let payload_base64 = BASE64.encode(msg_view.payload());
                map.insert("payload", serde_json::to_value(payload_base64).unwrap());

                if let Ok(Some(headers)) = msg_view.user_headers_map() {
                    map.insert("headers", serde_json::to_value(&headers).unwrap());
                }

                map
            })
            .collect();

        let mut state = serializer.serialize_struct("SendMessages", 2)?;
        state.serialize_field("partitioning", &self.partitioning)?;
        state.serialize_field("messages", &messages)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SendMessages {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Partitioning,
            Messages,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                        formatter.write_str("`partitioning` or `messages`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "partitioning" => Ok(Field::Partitioning),
                            "messages" => Ok(Field::Messages),
                            _ => Err(de::Error::unknown_field(
                                value,
                                &["partitioning", "messages"],
                            )),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct SendMessagesVisitor;

        impl<'de> Visitor<'de> for SendMessagesVisitor {
            type Value = SendMessages;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("struct SendMessages")
            }

            fn visit_map<V>(self, mut map: V) -> Result<SendMessages, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut partitioning = None;
                let mut messages = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Partitioning => {
                            if partitioning.is_some() {
                                return Err(de::Error::duplicate_field("partitioning"));
                            }
                            partitioning = Some(map.next_value()?);
                        }
                        Field::Messages => {
                            if messages.is_some() {
                                return Err(de::Error::duplicate_field("messages"));
                            }

                            let message_data: Vec<serde_json::Value> = map.next_value()?;
                            let mut iggy_messages = Vec::new();

                            for msg in message_data {
                                let id =
                                    msg.get("id").and_then(|v| v.as_u64()).unwrap_or(0) as u128;

                                let payload = msg
                                    .get("payload")
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| de::Error::missing_field("payload"))?;
                                let payload_bytes = BASE64
                                    .decode(payload)
                                    .map_err(|_| de::Error::custom("Invalid base64 payload"))?;

                                let headers_map = if let Some(headers) = msg.get("headers") {
                                    if headers.is_null() {
                                        None
                                    } else {
                                        Some(serde_json::from_value(headers.clone()).map_err(
                                            |_| de::Error::custom("Invalid headers format"),
                                        )?)
                                    }
                                } else {
                                    None
                                };

                                let iggy_message = if let Some(headers) = headers_map {
                                    IggyMessage::with_id_and_headers(id, payload_bytes, headers)
                                } else {
                                    IggyMessage::with_id(id, payload_bytes)
                                };

                                iggy_messages.push(iggy_message);
                            }

                            messages = Some(iggy_messages);
                        }
                    }
                }

                let partitioning =
                    partitioning.ok_or_else(|| de::Error::missing_field("partitioning"))?;
                let messages = messages.ok_or_else(|| de::Error::missing_field("messages"))?;

                let batch = IggyMessagesBatch::from(&messages);

                Ok(SendMessages {
                    stream_id: Identifier::default(),
                    topic_id: Identifier::default(),
                    partitioning,
                    batch,
                })
            }
        }

        deserializer.deserialize_struct(
            "SendMessages",
            &["partitioning", "messages"],
            SendMessagesVisitor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
