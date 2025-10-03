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

use crate::{
    BytesSerializable, Identifier, Validatable,
    error::IggyError,
    {Command, FLUSH_UNSAVED_BUFFER_CODE},
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `FlushUnsavedBuffer` command is used to force a flush of `unsaved_buffer` to disk for specific stream -> topic -> partition.
/// - `stream_id` - stream identifier
/// - `topic_id` - topic identifier
/// - `partition_id` - partition identifier
/// - `fsync` - if `true` then the data is flushed to disk and fsynced, if `false` then the data is only flushed to disk.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct FlushUnsavedBuffer {
    pub stream_id: Identifier,
    pub topic_id: Identifier,
    pub partition_id: usize,
    pub fsync: bool,
}

impl FlushUnsavedBuffer {
    fn fsync_stringified(&self) -> &'static str {
        if self.fsync { "f" } else { "n" }
    }
}

impl Display for FlushUnsavedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.fsync_stringified()
        )
    }
}

impl Command for FlushUnsavedBuffer {
    fn code(&self) -> u32 {
        FLUSH_UNSAVED_BUFFER_CODE
    }
}

impl BytesSerializable for FlushUnsavedBuffer {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let mut bytes =
            BytesMut::with_capacity(stream_id_bytes.len() + topic_id_bytes.len() + 4 + 1);
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&self.partition_id.to_le_bytes());
        bytes.put_u8(if self.fsync { 1 } else { 0 });
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.to_bytes().len();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.to_bytes().len();
        let partition_id = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        ) as usize;
        position += 4;
        let fsync = bytes[position] == 1;
        Ok(FlushUnsavedBuffer {
            stream_id,
            topic_id,
            partition_id,
            fsync,
        })
    }
}

impl Validatable<IggyError> for FlushUnsavedBuffer {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::FlushUnsavedBuffer;
    use crate::BytesSerializable;
    use crate::Identifier;

    #[test]
    fn test_flush_unsaved_buffer_serialization() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(1).unwrap();
        let flush_unsaved_buffer = super::FlushUnsavedBuffer {
            stream_id,
            topic_id,
            partition_id: 1,
            fsync: false,
        };
        let bytes = flush_unsaved_buffer.to_bytes();
        let deserialized_flush_unsaved_buffer = FlushUnsavedBuffer::from_bytes(bytes).unwrap();
        assert_eq!(flush_unsaved_buffer, deserialized_flush_unsaved_buffer);
    }
}
