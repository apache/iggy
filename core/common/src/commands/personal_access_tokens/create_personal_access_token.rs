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
use crate::Validatable;
use crate::defaults::*;
use crate::error::IggyError;
use crate::utils::expiry::IggyExpiry;
use crate::{CREATE_PERSONAL_ACCESS_TOKEN_CODE, Command};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

/// `CreatePersonalAccessToken` command is used to create a new personal access token for the authenticated user.
/// It has additional payload:
/// - `name` - unique name of the token, must be between 3 and 30 characters long.
/// - `expiry` - expiry of the token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreatePersonalAccessToken {
    /// Unique name of the token, must be between 3 and 30 characters long.
    pub name: String,
    /// Expiry of the token.
    pub expiry: IggyExpiry,
}

impl Command for CreatePersonalAccessToken {
    fn code(&self) -> u32 {
        CREATE_PERSONAL_ACCESS_TOKEN_CODE
    }
}

impl Default for CreatePersonalAccessToken {
    fn default() -> Self {
        CreatePersonalAccessToken {
            name: "token".to_string(),
            expiry: IggyExpiry::NeverExpire,
        }
    }
}

impl Validatable<IggyError> for CreatePersonalAccessToken {
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

impl BytesSerializable for CreatePersonalAccessToken {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5 + self.name.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.put_slice(self.name.as_bytes());
        bytes.put_u64_le(self.expiry.into());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<CreatePersonalAccessToken, IggyError> {
        if bytes.len() < 12 {
            return Err(IggyError::InvalidCommand);
        }

        let name_length = *bytes.first().ok_or(IggyError::InvalidCommand)? as usize;
        let name = from_utf8(
            bytes
                .get(1..1 + name_length)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

        let position = 1 + name_length;
        let expiry = u64::from_le_bytes(
            bytes
                .get(position..position + 8)
                .ok_or(IggyError::InvalidCommand)?
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let expiry: IggyExpiry = expiry.into();

        let command = CreatePersonalAccessToken { name, expiry };
        Ok(command)
    }
}

impl Display for CreatePersonalAccessToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.name, self.expiry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreatePersonalAccessToken {
            name: "test".to_string(),
            expiry: IggyExpiry::NeverExpire,
        };

        let bytes = command.to_bytes();
        let name_length = bytes[0];
        let name = from_utf8(&bytes[1..1 + name_length as usize]).unwrap();
        let expiry = u64::from_le_bytes(
            bytes[1 + name_length as usize..9 + name_length as usize]
                .try_into()
                .unwrap(),
        );
        let expiry: IggyExpiry = expiry.into();
        assert!(!bytes.is_empty());
        assert_eq!(name, command.name);
        assert_eq!(expiry, command.expiry);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let name = "test";
        let expiry = IggyExpiry::NeverExpire;
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.put_slice(name.as_bytes());
        bytes.put_u64_le(expiry.into());

        let command = CreatePersonalAccessToken::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.name, name);
        assert_eq!(command.expiry, expiry);
    }
}
