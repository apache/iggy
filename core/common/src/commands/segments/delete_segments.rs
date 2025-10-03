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
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::{Command, DELETE_SEGMENTS_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `DeleteSegments` command is used to delete segments from a partition.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partition_id` - unique partition ID (numeric or name).
/// - `segments_count` - number of segments in the partition to delete.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeleteSegments {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique partition ID (numeric or name).
    #[serde(skip)]
    pub partition_id: usize,
    /// Number of segments in the partition to delete, max value is 1000.
    pub segments_count: u32,
}

impl Command for DeleteSegments {
    fn code(&self) -> u32 {
        DELETE_SEGMENTS_CODE
    }
}

impl Default for DeleteSegments {
    fn default() -> Self {
        DeleteSegments {
            segments_count: 1,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: usize::default(),
        }
    }
}

impl Validatable<IggyError> for DeleteSegments {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for DeleteSegments {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            std::mem::size_of::<u32>()
                + std::mem::size_of::<u32>()
                + stream_id_bytes.len()
                + topic_id_bytes.len(),
        );
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&self.partition_id.to_le_bytes());
        bytes.put_u32_le(self.segments_count);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> std::result::Result<DeleteSegments, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partition_id = u32::from_le_bytes(
            bytes[position..position + std::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        position += std::mem::size_of::<u32>();
        let segments_count = u32::from_le_bytes(
            bytes[position..position + std::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let command = DeleteSegments {
            stream_id,
            topic_id,
            partition_id,
            segments_count,
        };
        Ok(command)
    }
}

impl Display for DeleteSegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id, self.topic_id, self.partition_id, self.segments_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteSegments {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partition_id: 3,
            segments_count: 3,
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partition_id = u32::from_le_bytes(
            bytes[position..position + std::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)
                .unwrap(),
        );
        position += std::mem::size_of::<u32>();
        let segments_count = u32::from_le_bytes(
            bytes[position..position + std::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)
                .unwrap(),
        );

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(segments_count, command.segments_count);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partition_id = 3;
        let segments_count = 3u32;
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            std::mem::size_of::<u32>()
                + std::mem::size_of::<u32>()
                + stream_id_bytes.len()
                + topic_id_bytes.len(),
        );
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_u32_le(partition_id);
        bytes.put_u32_le(segments_count);
        let command = DeleteSegments::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(segments_count, command.segments_count);
    }
}
