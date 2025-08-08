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

use super::MAX_NAME_LENGTH;
use crate::BytesSerializable;
use crate::CompressionAlgorithm;
use crate::Identifier;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use crate::{Command, UPDATE_TOPIC_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `UpdateTopic` command is used to update a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `message_expiry` - message expiry, if `NeverExpire` then messages will never expire.
/// - `max_topic_size` - maximum size of the topic in bytes, if `Unlimited` then topic size is unlimited.
///   Can't be lower than segment size in the config.
/// - `replication_factor` - replication factor for the topic.
/// - `name` - unique topic name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UpdateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Compression algorithm for the topic.
    pub compression_algorithm: CompressionAlgorithm,
    /// Message expiry, if `NeverExpire` then messages will never expire.
    pub message_expiry: IggyExpiry,
    /// Max topic size, if `Unlimited` then topic size is unlimited.
    /// Can't be lower than segment size in the config.
    pub max_topic_size: MaxTopicSize,
    /// Replication factor for the topic.
    pub replication_factor: Option<u8>,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl Command for UpdateTopic {
    fn code(&self) -> u32 {
        UPDATE_TOPIC_CODE
    }
}

impl Default for UpdateTopic {
    fn default() -> Self {
        UpdateTopic {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            compression_algorithm: Default::default(),
            message_expiry: IggyExpiry::NeverExpire,
            max_topic_size: MaxTopicSize::ServerDefault,
            replication_factor: None,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<IggyError> for UpdateTopic {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidTopicName);
        }

        if let Some(replication_factor) = self.replication_factor
            && replication_factor == 0
        {
            return Err(IggyError::InvalidReplicationFactor);
        }

        Ok(())
    }
}

impl BytesSerializable for UpdateTopic {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            19 + stream_id_bytes.len() + topic_id_bytes.len() + self.name.len(),
        );
        bytes.put_slice(&stream_id_bytes.clone());
        bytes.put_slice(&topic_id_bytes.clone());
        bytes.put_u8(self.compression_algorithm.as_code());
        bytes.put_u64_le(self.message_expiry.into());
        bytes.put_u64_le(self.max_topic_size.into());
        match self.replication_factor {
            Some(replication_factor) => bytes.put_u8(replication_factor),
            None => bytes.put_u8(0),
        }
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<UpdateTopic, IggyError> {
        if bytes.len() < 21 {
            return Err(IggyError::InvalidCommand);
        }
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let compression_algorithm = CompressionAlgorithm::from_code(bytes[position])?;
        position += 1;
        let message_expiry = u64::from_le_bytes(
            bytes[position..position + 8]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let message_expiry: IggyExpiry = message_expiry.into();
        let max_topic_size = u64::from_le_bytes(
            bytes[position + 8..position + 16]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let max_topic_size: MaxTopicSize = max_topic_size.into();
        let replication_factor = match bytes[position + 16] {
            0 => None,
            factor => Some(factor),
        };
        let name_length = bytes[position + 17];
        let name = from_utf8(&bytes[position + 18..(position + 18 + name_length as usize)])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        if name.len() != name_length as usize {
            return Err(IggyError::InvalidCommand);
        }
        let command = UpdateTopic {
            stream_id,
            topic_id,
            compression_algorithm,
            message_expiry,
            max_topic_size,
            replication_factor,
            name,
        };
        Ok(command)
    }
}

impl Display for UpdateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.message_expiry,
            self.max_topic_size,
            self.replication_factor.unwrap_or(0),
            self.name,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::byte_size::IggyByteSize;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdateTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            compression_algorithm: CompressionAlgorithm::None,
            message_expiry: IggyExpiry::NeverExpire,
            max_topic_size: MaxTopicSize::ServerDefault,
            replication_factor: Some(1),
            name: "test".to_string(),
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let compression_algorithm = CompressionAlgorithm::from_code(bytes[position]).unwrap();
        position += 1;
        let message_expiry = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let message_expiry: IggyExpiry = message_expiry.into();
        let max_topic_size =
            u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let max_topic_size: MaxTopicSize = max_topic_size.into();
        let replication_factor = bytes[position + 16];
        let name_length = bytes[position + 17];
        let name = from_utf8(&bytes[position + 18..position + 18 + name_length as usize])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(compression_algorithm, command.compression_algorithm);
        assert_eq!(message_expiry, command.message_expiry);
        assert_eq!(max_topic_size, command.max_topic_size);
        assert_eq!(replication_factor, command.replication_factor.unwrap());
        assert_eq!(name.len() as u8, command.name.len() as u8);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let compression_algorithm = CompressionAlgorithm::None;
        let name = "test".to_string();
        let message_expiry = IggyExpiry::NeverExpire;
        let max_topic_size = MaxTopicSize::Custom(IggyByteSize::from(100));
        let replication_factor = 1;

        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let mut bytes =
            BytesMut::with_capacity(19 + stream_id_bytes.len() + topic_id_bytes.len() + name.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_u8(compression_algorithm.as_code());
        bytes.put_u64_le(message_expiry.into());
        bytes.put_u64_le(max_topic_size.as_bytes_u64());
        bytes.put_u8(replication_factor);

        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());

        let command = UpdateTopic::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.compression_algorithm, compression_algorithm);
        assert_eq!(command.message_expiry, message_expiry);
        assert_eq!(command.max_topic_size, max_topic_size);
        assert_eq!(command.replication_factor, Some(replication_factor));
        assert_eq!(command.name, name);
    }
}
