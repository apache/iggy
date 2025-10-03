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

use super::MAX_PARTITIONS_COUNT;
use crate::BytesSerializable;
use crate::Identifier;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::{Command, DELETE_PARTITIONS_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `DeletePartitions` command is used to delete partitions from a topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitions_count` - number of partitions in the topic to delete, max value is 1000.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeletePartitions {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Number of partitions in the topic to delete, max value is 1000.
    pub partitions_count: usize,
}

impl Command for DeletePartitions {
    fn code(&self) -> u32 {
        DELETE_PARTITIONS_CODE
    }
}

impl Default for DeletePartitions {
    fn default() -> Self {
        DeletePartitions {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitions_count: 1,
        }
    }
}

impl Validatable<IggyError> for DeletePartitions {
    fn validate(&self) -> Result<(), IggyError> {
        if !(1..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(IggyError::TooManyPartitions);
        }

        Ok(())
    }
}

impl BytesSerializable for DeletePartitions {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&self.partitions_count.to_le_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> std::result::Result<DeletePartitions, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partitions_count = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        let command = DeletePartitions {
            stream_id,
            topic_id,
            partitions_count,
        };
        Ok(command)
    }
}

impl Display for DeletePartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}",
            self.stream_id, self.topic_id, self.partitions_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeletePartitions {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partitions_count: 3,
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partitions_count =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partitions_count, command.partitions_count);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partitions_count = 3u32;
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(4 + stream_id_bytes.len() + topic_id_bytes.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_u32_le(partitions_count);
        let command = DeletePartitions::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
    }
}
