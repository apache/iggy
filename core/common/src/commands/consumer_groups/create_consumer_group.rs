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
use crate::Identifier;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::{CREATE_CONSUMER_GROUP_CODE, Command};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `CreateConsumerGroup` command creates a new consumer group for the topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `group_id` - unique consumer group ID.
/// - `name` - unique consumer group name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateConsumerGroup {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique consumer group ID (numeric), if None is provided then the server will automatically assign it.
    pub group_id: Option<u32>,
    /// Unique consumer group name, max length is 255 characters.
    pub name: String,
}

impl Command for CreateConsumerGroup {
    fn code(&self) -> u32 {
        CREATE_CONSUMER_GROUP_CODE
    }
}

impl Default for CreateConsumerGroup {
    fn default() -> Self {
        CreateConsumerGroup {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            group_id: None,
            name: "consumer_group_1".to_string(),
        }
    }
}

impl Validatable<IggyError> for CreateConsumerGroup {
    fn validate(&self) -> Result<(), IggyError> {
        if let Some(group_id) = self.group_id
            && group_id == 0
        {
            return Err(IggyError::InvalidConsumerGroupId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidConsumerGroupName);
        }

        Ok(())
    }
}

impl BytesSerializable for CreateConsumerGroup {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            4 + stream_id_bytes.len() + topic_id_bytes.len() + self.name.len(),
        );
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_u32_le(self.group_id.unwrap_or(0));
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<CreateConsumerGroup, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let group_id = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let group_id = if group_id == 0 { None } else { Some(group_id) };
        let name_length = bytes[position + 4];
        let name = from_utf8(&bytes[position + 5..position + 5 + name_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        let command = CreateConsumerGroup {
            stream_id,
            topic_id,
            group_id,
            name,
        };
        Ok(command)
    }
}

impl Display for CreateConsumerGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.group_id.unwrap_or(0),
            self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateConsumerGroup {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            group_id: Some(3),
            name: "test".to_string(),
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let group_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());

        let name_length = bytes[position + 4];
        let name = from_utf8(&bytes[position + 5..position + 5 + name_length as usize]).unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(group_id, command.group_id.unwrap());
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let group_id = 3u32;
        let name = "test".to_string();
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let mut bytes =
            BytesMut::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len() + name.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_u32_le(group_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());
        let command = CreateConsumerGroup::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.group_id.unwrap(), group_id);
        assert_eq!(command.name, name);
    }
}
