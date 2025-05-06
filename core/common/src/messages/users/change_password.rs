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

use super::defaults::*;
use crate::error::IggyError;
use crate::BytesSerializable;
use crate::Identifier;
use crate::Sizeable;
use crate::Validatable;
use crate::{Command, CHANGE_PASSWORD_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `ChangePassword` command is used to change a user's password.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `current_password` - current password, must be between 3 and 100 characters long.
/// - `new_password` - new password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ChangePassword {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// Current password, must be between 3 and 100 characters long.
    pub current_password: String,
    /// New password, must be between 3 and 100 characters long.
    pub new_password: String,
}

impl Command for ChangePassword {
    fn code(&self) -> u32 {
        CHANGE_PASSWORD_CODE
    }
}

impl Default for ChangePassword {
    fn default() -> Self {
        ChangePassword {
            user_id: Identifier::default(),
            current_password: "secret".to_string(),
            new_password: "topsecret".to_string(),
        }
    }
}

impl Validatable<IggyError> for ChangePassword {
    fn validate(&self) -> Result<(), IggyError> {
        if self.current_password.is_empty()
            || self.current_password.len() > MAX_PASSWORD_LENGTH
            || self.current_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        if self.new_password.is_empty()
            || self.new_password.len() > MAX_PASSWORD_LENGTH
            || self.new_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}

impl BytesSerializable for ChangePassword {
    fn to_bytes(&self) -> Bytes {
        let user_id_bytes = self.user_id.to_bytes();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id_bytes);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.current_password.len() as u8);
        bytes.put_slice(self.current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.new_password.len() as u8);
        bytes.put_slice(self.new_password.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<ChangePassword, IggyError> {
        if bytes.len() < 9 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes.clone())?;
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let current_password_length = bytes[position];
        position += 1;
        let current_password =
            from_utf8(&bytes[position..position + current_password_length as usize])
                .map_err(|_| IggyError::InvalidUtf8)?
                .to_string();
        position += current_password_length as usize;
        let new_password_length = bytes[position];
        position += 1;
        let new_password = from_utf8(&bytes[position..position + new_password_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();

        let command = ChangePassword {
            user_id,
            current_password,
            new_password,
        };
        Ok(command)
    }
}

impl Display for ChangePassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|******|******", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = ChangePassword {
            user_id: Identifier::numeric(1).unwrap(),
            current_password: "user".to_string(),
            new_password: "secret".to_string(),
        };

        let bytes = command.to_bytes();
        let user_id = Identifier::from_bytes(bytes.clone()).unwrap();
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let current_password_length = bytes[position];
        position += 1;
        let current_password =
            from_utf8(&bytes[position..position + current_password_length as usize]).unwrap();
        position += current_password_length as usize;
        let new_password_length = bytes[position];
        position += 1;
        let new_password =
            from_utf8(&bytes[position..position + new_password_length as usize]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(current_password, command.current_password);
        assert_eq!(new_password, command.new_password);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let current_password = "secret";
        let new_password = "topsecret";
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id.to_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(current_password.len() as u8);
        bytes.put_slice(current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(new_password.len() as u8);
        bytes.put_slice(new_password.as_bytes());

        let command = ChangePassword::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.current_password, current_password);
        assert_eq!(command.new_password, new_password);
    }
}
