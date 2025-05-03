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
use crate::command::{Command, CREATE_USER_CODE};
use crate::error::IggyError;
use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::users::defaults::*;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `CreateUser` command is used to create a new user.
/// It has additional payload:
/// - `username` - unique name of the user, must be between 3 and 50 characters long.
/// - `password` - password of the user, must be between 3 and 100 characters long.
/// - `status` - status of the user, can be either `active` or `inactive`.
/// - `permissions` - optional permissions of the user. If not provided, user will have no permissions.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateUser {
    /// Unique name of the user, must be between 3 and 50 characters long.
    pub username: String,
    /// Password of the user, must be between 3 and 100 characters long.
    pub password: String,
    /// Status of the user, can be either `active` or `inactive`.
    pub status: UserStatus,
    /// Optional permissions of the user. If not provided, user will have no permissions.
    pub permissions: Option<Permissions>,
}

impl Command for CreateUser {
    fn code(&self) -> u32 {
        CREATE_USER_CODE
    }
}

impl Default for CreateUser {
    fn default() -> Self {
        CreateUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: None,
        }
    }
}

impl Validatable<IggyError> for CreateUser {
    fn validate(&self) -> Result<(), IggyError> {
        if self.username.is_empty()
            || self.username.len() > MAX_USERNAME_LENGTH
            || self.username.len() < MIN_USERNAME_LENGTH
        {
            return Err(IggyError::InvalidUsername);
        }

        if self.password.is_empty()
            || self.password.len() > MAX_PASSWORD_LENGTH
            || self.password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}

impl BytesSerializable for CreateUser {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.username.len() + self.password.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.username.len() as u8);
        bytes.put_slice(self.username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.password.len() as u8);
        bytes.put_slice(self.password.as_bytes());
        bytes.put_u8(self.status.as_code());
        if let Some(permissions) = &self.permissions {
            bytes.put_u8(1);
            let permissions = permissions.to_bytes();
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(permissions.len() as u32);
            bytes.put_slice(&permissions);
        } else {
            bytes.put_u8(0);
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<CreateUser, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..1 + username_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        if username.len() != username_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 1 + username_length as usize;
        let password_length = bytes[position];
        position += 1;
        let password = from_utf8(&bytes[position..position + password_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        if password.len() != password_length as usize {
            return Err(IggyError::InvalidCommand);
        }

        position += password_length as usize;
        let status = UserStatus::from_code(bytes[position])?;
        position += 1;
        let has_permissions = bytes[position];
        if has_permissions > 1 {
            return Err(IggyError::InvalidCommand);
        }

        position += 1;
        let permissions = if has_permissions == 1 {
            let permissions_length = u32::from_le_bytes(
                bytes[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            position += 4;
            Some(Permissions::from_bytes(
                bytes.slice(position..position + permissions_length as usize),
            )?)
        } else {
            None
        };

        let command = CreateUser {
            username,
            password,
            status,
            permissions,
        };
        Ok(command)
    }
}

impl Display for CreateUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(
            f,
            "{}|******|{}|{}",
            self.username, self.status, permissions
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::permissions::GlobalPermissions;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            status: UserStatus::Active,
            permissions: Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: true,
                    manage_users: false,
                    read_users: true,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        };

        let bytes = command.to_bytes();
        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..1 + username_length as usize]).unwrap();
        let mut position = 1 + username_length as usize;
        let password_length = bytes[position];
        position += 1;
        let password = from_utf8(&bytes[position..position + password_length as usize]).unwrap();
        position += password_length as usize;
        let status = UserStatus::from_code(bytes[position]).unwrap();
        position += 1;
        let has_permissions = bytes[3 + username_length as usize + password_length as usize];
        position += 1;

        let permissions_length =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        let permissions =
            Permissions::from_bytes(bytes.slice(position..position + permissions_length as usize))
                .unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(username, command.username);
        assert_eq!(password, command.password);
        assert_eq!(status, command.status);
        assert_eq!(has_permissions, 1);
        assert_eq!(permissions, command.permissions.unwrap());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let username = "user";
        let password = "secret";
        let status = UserStatus::Active;
        let has_permissions = 1u8;
        let permissions = Permissions {
            global: GlobalPermissions {
                manage_servers: false,
                read_servers: true,
                manage_users: false,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: true,
            },
            streams: None,
        };
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.put_slice(password.as_bytes());
        bytes.put_u8(status.as_code());
        bytes.put_u8(has_permissions);
        let permissions_bytes = permissions.to_bytes();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u32_le(permissions_bytes.len() as u32);
        bytes.put_slice(&permissions_bytes);

        let command = CreateUser::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
        assert_eq!(command.status, status);
        assert!(command.permissions.is_some());
        assert_eq!(command.permissions.unwrap(), permissions);
    }
}
