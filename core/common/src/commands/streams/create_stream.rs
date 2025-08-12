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
use crate::Validatable;
use crate::error::IggyError;
use crate::{CREATE_STREAM_CODE, Command};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `CreateStream` command is used to create a new stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric)
/// - `name` - unique stream name (string), max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateStream {
    /// Unique stream ID (numeric), if None is provided then the server will automatically assign it.
    pub stream_id: Option<u32>,
    /// Unique stream name (string), max length is 255 characters.
    pub name: String,
}

impl Command for CreateStream {
    fn code(&self) -> u32 {
        CREATE_STREAM_CODE
    }
}

impl Default for CreateStream {
    fn default() -> Self {
        CreateStream {
            stream_id: Some(1),
            name: "stream".to_string(),
        }
    }
}

impl Validatable<IggyError> for CreateStream {
    fn validate(&self) -> Result<(), IggyError> {
        if let Some(stream_id) = self.stream_id
            && stream_id == 0
        {
            return Err(IggyError::InvalidStreamId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(IggyError::InvalidStreamName);
        }

        Ok(())
    }
}

impl BytesSerializable for CreateStream {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5 + self.name.len());
        bytes.put_u32_le(self.stream_id.unwrap_or(0));
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<CreateStream, IggyError> {
        if bytes.len() < 6 {
            return Err(IggyError::InvalidCommand);
        }

        let stream_id = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let stream_id = if stream_id == 0 {
            None
        } else {
            Some(stream_id)
        };
        let name_length = bytes[4];
        let name = from_utf8(&bytes[5..5 + name_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        if name.len() != name_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        let command = CreateStream { stream_id, name };
        Ok(command)
    }
}

impl Display for CreateStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.stream_id.unwrap_or(0), self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateStream {
            stream_id: Some(1),
            name: "test".to_string(),
        };

        let bytes = command.to_bytes();
        let stream_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let name_length = bytes[4];
        let name = from_utf8(&bytes[5..5 + name_length as usize]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id.unwrap());
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = 1u32;
        let name = "test".to_string();
        let mut bytes = BytesMut::new();
        bytes.put_u32_le(stream_id);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());
        let command = CreateStream::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id.unwrap(), stream_id);
        assert_eq!(command.name, name);
    }
}
