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
use crate::UserStatus;
use crate::Validatable;
use crate::error::IggyError;
use crate::{Command, UPDATE_USER_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `UpdateUser` command is used to update a user's username and status.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `username` - new username (optional), if provided, must be between 3 and 50 characters long.
/// - `status` - new status (optional)
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct UpdateUser {
    #[serde(skip)]
    pub user_id: Identifier,
    pub username: Option<String>,
    pub status: Option<UserStatus>,
}

impl Command for UpdateUser {
    fn code(&self) -> u32 {
        UPDATE_USER_CODE
    }
}

impl Validatable<IggyError> for UpdateUser {
    fn validate(&self) -> Result<(), IggyError> {
        if self.username.is_none() {
            return Ok(());
        }

        let username = self.username.as_ref().unwrap();
        if username.is_empty()
            || username.len() > MAX_USERNAME_LENGTH
            || username.len() < MIN_USERNAME_LENGTH
        {
            return Err(IggyError::InvalidUsername);
        }

        Ok(())
    }
}

impl BytesSerializable for UpdateUser {
    fn to_bytes(&self) -> Bytes {
        let user_id_bytes = self.user_id.to_bytes();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id_bytes);
        if let Some(username) = &self.username {
            bytes.put_u8(1);
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u8(username.len() as u8);
            bytes.put_slice(username.as_bytes());
        } else {
            bytes.put_u8(0);
        }
        if let Some(status) = &self.status {
            bytes.put_u8(1);
            bytes.put_u8(status.as_code());
        } else {
            bytes.put_u8(0);
        }

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<UpdateUser, IggyError> {
        if bytes.len() < 5 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes.clone())?;
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let has_username = *bytes.get(position).ok_or(IggyError::InvalidCommand)?;
        if has_username > 1 {
            return Err(IggyError::InvalidCommand);
        }

        position += 1;
        let username = if has_username == 1 {
            let username_length = *bytes.get(position).ok_or(IggyError::InvalidCommand)?;
            position += 1;
            let username = from_utf8(
                bytes
                    .get(position..position + username_length as usize)
                    .ok_or(IggyError::InvalidCommand)?,
            )
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
            position += username_length as usize;
            Some(username)
        } else {
            None
        };

        let has_status = *bytes.get(position).ok_or(IggyError::InvalidCommand)?;
        if has_status > 1 {
            return Err(IggyError::InvalidCommand);
        }

        let status = if has_status == 1 {
            position += 1;
            let status =
                UserStatus::from_code(*bytes.get(position).ok_or(IggyError::InvalidCommand)?)?;
            Some(status)
        } else {
            None
        };

        let command = UpdateUser {
            user_id,
            username,
            status,
        };
        Ok(command)
    }
}

impl Display for UpdateUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let username = self.username.as_deref().unwrap_or("");
        let status = self
            .status
            .as_ref()
            .map_or_else(String::new, |s| s.to_string());
        write!(f, "{}|{username}|{status}", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdateUser {
            user_id: Identifier::numeric(1).unwrap(),
            username: Some("user".to_string()),
            status: Some(UserStatus::Active),
        };

        let bytes = command.to_bytes();
        let user_id = Identifier::from_bytes(bytes.clone()).unwrap();
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let has_username = bytes[position];
        position += 1;
        let username_length = bytes[position];
        position += 1;
        let username = from_utf8(&bytes[position..position + username_length as usize]).unwrap();
        position += username_length as usize;
        let has_status = bytes[position];
        position += 1;
        let status = UserStatus::from_code(bytes[position]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(has_username, 1);
        assert_eq!(username, command.username.unwrap());
        assert_eq!(has_status, 1);
        assert_eq!(status, command.status.unwrap());
    }

    #[test]
    fn from_bytes_should_fail_on_empty_input() {
        assert!(UpdateUser::from_bytes(Bytes::new()).is_err());
    }

    #[test]
    fn from_bytes_should_fail_on_truncated_input() {
        let command = UpdateUser {
            user_id: Identifier::numeric(1).unwrap(),
            username: Some("user".to_string()),
            status: Some(UserStatus::Active),
        };
        let bytes = command.to_bytes();
        for i in 0..bytes.len() - 1 {
            let truncated = bytes.slice(..i);
            assert!(
                UpdateUser::from_bytes(truncated).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let username = "user";
        let status = UserStatus::Active;
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id.to_bytes());
        bytes.put_u8(1);
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        bytes.put_u8(1);
        bytes.put_u8(status.as_code());

        let command = UpdateUser::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.username.unwrap(), username);
        assert_eq!(command.status.unwrap(), status);
    }
}
