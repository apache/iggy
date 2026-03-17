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
use crate::BytesSerializable;
use crate::Identifier;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::{CHANGE_PASSWORD_CODE, Command};
use bytes::{BufMut, Bytes, BytesMut};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `ChangePassword` command is used to change a user's password.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `current_password` - current password, must be between 3 and 100 characters long.
/// - `new_password` - new password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChangePassword {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// Current password, must be between 3 and 100 characters long.
    #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
    pub current_password: SecretString,
    /// New password, must be between 3 and 100 characters long.
    #[serde(serialize_with = "crate::utils::serde_secret::serialize_secret")]
    pub new_password: SecretString,
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
            current_password: SecretString::from("secret"),
            new_password: SecretString::from("topsecret"),
        }
    }
}

impl Validatable<IggyError> for ChangePassword {
    fn validate(&self) -> Result<(), IggyError> {
        let current_password = self.current_password.expose_secret();
        if current_password.is_empty()
            || current_password.len() > MAX_PASSWORD_LENGTH
            || current_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        let new_password = self.new_password.expose_secret();
        if new_password.is_empty()
            || new_password.len() > MAX_PASSWORD_LENGTH
            || new_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}

impl BytesSerializable for ChangePassword {
    fn to_bytes(&self) -> Bytes {
        let user_id_bytes = self.user_id.to_bytes();
        let current_password = self.current_password.expose_secret();
        let new_password = self.new_password.expose_secret();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id_bytes);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(current_password.len() as u8);
        bytes.put_slice(current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(new_password.len() as u8);
        bytes.put_slice(new_password.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<ChangePassword, IggyError> {
        if bytes.len() < 9 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes.clone())?;
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let current_password_length = *bytes.get(position).ok_or(IggyError::InvalidCommand)?;
        position += 1;
        let current_password = from_utf8(
            bytes
                .get(position..position + current_password_length as usize)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
        position += current_password_length as usize;
        let new_password_length = *bytes.get(position).ok_or(IggyError::InvalidCommand)?;
        position += 1;
        let new_password = from_utf8(
            bytes
                .get(position..position + new_password_length as usize)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

        let command = ChangePassword {
            user_id,
            current_password: SecretString::from(current_password),
            new_password: SecretString::from(new_password),
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
            current_password: SecretString::from("user"),
            new_password: SecretString::from("secret"),
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
        assert_eq!(current_password, command.current_password.expose_secret());
        assert_eq!(new_password, command.new_password.expose_secret());
    }

    #[test]
    fn from_bytes_should_fail_on_empty_input() {
        assert!(ChangePassword::from_bytes(Bytes::new()).is_err());
    }

    #[test]
    fn from_bytes_should_fail_on_truncated_input() {
        let command = ChangePassword::default();
        let bytes = command.to_bytes();
        for i in 0..bytes.len() - 1 {
            let truncated = bytes.slice(..i);
            assert!(
                ChangePassword::from_bytes(truncated).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
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
        assert_eq!(command.current_password.expose_secret(), current_password);
        assert_eq!(command.new_password.expose_secret(), new_password);
    }

    #[test]
    fn should_fail_validation_for_invalid_current_password() {
        for password in ["", "ab"] {
            let command = ChangePassword {
                current_password: SecretString::from(password),
                ..ChangePassword::default()
            };
            assert!(
                command.validate().is_err(),
                "expected validation error for current_password: {password:?}"
            );
        }
    }

    #[test]
    fn should_fail_validation_for_empty_new_password() {
        let command = ChangePassword {
            new_password: SecretString::from(""),
            ..ChangePassword::default()
        };
        assert!(command.validate().is_err());
    }

    #[test]
    fn should_pass_validation_for_valid_command() {
        let command = ChangePassword::default();
        assert!(command.validate().is_ok());
    }
}
