/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::BytesSerializable;
use crate::Identifier;
use crate::IggyMessageView;
use crate::PartitioningKind;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::types::message::partitioning::Partitioning;
use crate::{Command, SEND_MESSAGES_CODE};
use crate::{INDEX_SIZE, IggyMessage, IggyMessagesBatch};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::{BufMut, Bytes, BytesMut};
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    /// Length of stream_id, topic_id, partitioning and messages_count (4 bytes)
    pub metadata_length: u32,
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
    pub fn bytes(
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &[IggyMessage],
    ) -> Bytes {
        let stream_id_field_size = stream_id.get_buffer_size();
        let topic_id_field_size = topic_id.get_buffer_size();
        let partitioning_field_size = partitioning.get_buffer_size();
        let metadata_length_field_size = size_of::<u32>();
        let messages_count = messages.len();
        let messages_count_field_size = size_of::<u32>();
        let metadata_length = stream_id_field_size
            + topic_id_field_size
            + partitioning_field_size
            + messages_count_field_size;
        let indexes_size = messages_count * INDEX_SIZE;
        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_usize())
            .sum::<usize>();

        let total_size = metadata_length_field_size
            + stream_id_field_size
            + topic_id_field_size
            + partitioning_field_size
            + messages_count_field_size
            + indexes_size
            + messages_size;

        let mut bytes = BytesMut::with_capacity(total_size);

        bytes.put_u32_le(metadata_length as u32);
        stream_id.write_to_buffer(&mut bytes);
        topic_id.write_to_buffer(&mut bytes);
        partitioning.write_to_buffer(&mut bytes);
        bytes.put_u32_le(messages_count as u32);

        let mut current_position = bytes.len();

        bytes.put_bytes(0, indexes_size);

        let mut msg_size: u32 = 0;
        for message in messages.iter() {
            message.write_to_buffer(&mut bytes);
            msg_size += message.get_size_bytes().as_bytes_u64() as u32;
            write_value_at(&mut bytes, 0u64.to_le_bytes(), current_position);
            write_value_at(&mut bytes, msg_size.to_le_bytes(), current_position + 4);
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
            metadata_length: 0,
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
                                let id = parse_message_id(msg.get("id")).map_err(|errror| {
                                    de::Error::custom(format!("Invalid message ID: {errror}"))
                                })?;

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
                                    IggyMessage::builder()
                                        .id(id)
                                        .payload(payload_bytes.into())
                                        .user_headers(headers)
                                        .build()
                                        .map_err(|e| {
                                            de::Error::custom(format!(
                                                "Failed to create message with headers: {e}"
                                            ))
                                        })?
                                } else {
                                    IggyMessage::builder()
                                        .id(id)
                                        .payload(payload_bytes.into())
                                        .build()
                                        .map_err(|e| {
                                            de::Error::custom(format!(
                                                "Failed to create message: {e}"
                                            ))
                                        })?
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
                    metadata_length: 0, // this field is used only for TCP/QUIC
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

fn parse_message_id(value: Option<&serde_json::Value>) -> Result<u128, String> {
    let value = match value {
        Some(v) => v,
        None => return Ok(0),
    };

    match value {
        serde_json::Value::Number(id) => id
            .as_u64()
            .map(|v| v as u128)
            .ok_or_else(|| "ID must be a positive integer".to_string()),
        serde_json::Value::String(id) => {
            if let Ok(id) = id.parse::<u128>() {
                return Ok(id);
            }

            let hex_str = id.replace('-', "");
            if hex_str.len() == 32 && hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
                u128::from_str_radix(&hex_str, 16)
                    .map_err(|error| format!("Invalid UUID format: {error}"))
            } else {
                Err(format!(
                    "Invalid ID string: '{id}' - must be a decimal number or UUID hex format",
                ))
            }
        }
        _ => Err("ID must be a number or string".to_string()),
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

    #[test]
    fn parse_message_id_from_number() {
        let value = serde_json::json!(12345);
        let id = parse_message_id(Some(&value)).unwrap();
        assert_eq!(id, 12345u128);
    }

    #[test]
    fn parse_message_id_from_large_number_string() {
        let value = serde_json::json!("340282366920938463463374607431768211455");
        let id = parse_message_id(Some(&value)).unwrap();
        assert_eq!(id, 340282366920938463463374607431768211455u128);
    }

    #[test]
    fn parse_message_id_from_uuid_with_dashes() {
        let value = serde_json::json!("af362865-042c-4000-0000-000000000000");
        let id = parse_message_id(Some(&value)).unwrap();
        assert_eq!(id, 0xaf362865042c40000000000000000000u128);
    }

    #[test]
    fn parse_message_id_from_uuid_without_dashes() {
        let value = serde_json::json!("af362865042c40000000000000000000");
        let id = parse_message_id(Some(&value)).unwrap();
        assert_eq!(id, 0xaf362865042c40000000000000000000u128);
    }

    #[test]
    fn parse_message_id_defaults_to_zero_when_missing() {
        let id = parse_message_id(None).unwrap();
        assert_eq!(id, 0u128);
    }

    #[test]
    fn parse_message_id_rejects_invalid_string() {
        let value = serde_json::json!("not-a-valid-id");
        let result = parse_message_id(Some(&value));
        assert!(result.is_err());
    }
}
