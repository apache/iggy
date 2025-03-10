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

use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, PURGE_TOPIC_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `PurgeTopic` command is used to purge topic data (its messages in all the partitions) from a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct PurgeTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
}

impl Command for PurgeTopic {
    fn code(&self) -> u32 {
        PURGE_TOPIC_CODE
    }
}

impl Validatable<IggyError> for PurgeTopic {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for PurgeTopic {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(stream_id_bytes.len() + topic_id_bytes.len());
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<PurgeTopic, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        let command = PurgeTopic {
            stream_id,
            topic_id,
        };
        Ok(command)
    }
}

impl Display for PurgeTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id, self.topic_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = PurgeTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&stream_id.to_bytes());
        bytes.put_slice(&topic_id.to_bytes());
        let command = PurgeTopic::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
    }
}
