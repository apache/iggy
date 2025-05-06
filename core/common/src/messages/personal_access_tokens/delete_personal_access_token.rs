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

use crate::defaults::*;
use crate::error::IggyError;
use crate::Command;
use crate::Validatable;
use crate::{BytesSerializable, DELETE_PERSONAL_ACCESS_TOKEN_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

/// `DeletePersonalAccessToken` command is used to delete a personal access token for the authenticated user.
/// It has additional payload:
/// - `name` - unique name of the token, must be between 3 and 30 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeletePersonalAccessToken {
    /// Unique name of the token, must be between 3 and 30 characters long.
    pub name: String,
}

impl Command for DeletePersonalAccessToken {
    fn code(&self) -> u32 {
        DELETE_PERSONAL_ACCESS_TOKEN_CODE
    }
}

impl Default for DeletePersonalAccessToken {
    fn default() -> Self {
        DeletePersonalAccessToken {
            name: "token".to_string(),
        }
    }
}

impl Validatable<IggyError> for DeletePersonalAccessToken {
    fn validate(&self) -> Result<(), IggyError> {
        if self.name.is_empty()
            || self.name.len() > MAX_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
            || self.name.len() < MIN_PERSONAL_ACCESS_TOKEN_NAME_LENGTH
        {
            return Err(IggyError::InvalidPersonalAccessTokenName);
        }

        Ok(())
    }
}

impl BytesSerializable for DeletePersonalAccessToken {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5 + self.name.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<DeletePersonalAccessToken, IggyError> {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        if name.len() != name_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        let command = DeletePersonalAccessToken { name };
        Ok(command)
    }
}

impl Display for DeletePersonalAccessToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeletePersonalAccessToken {
            name: "test".to_string(),
        };

        let bytes = command.to_bytes();
        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize]).unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let name = "test";
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());

        let command = DeletePersonalAccessToken::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
    }
}
