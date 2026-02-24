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
use crate::Validatable;
use crate::error::IggyError;
use crate::{Command, LOGIN_USER_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `LoginUser` command is used to login a user by username and password.
/// It has additional payload:
/// - `username` - username, must be between 3 and 50 characters long.
/// - `password` - password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct LoginUser {
    /// Username, must be between 3 and 50 characters long.
    pub username: String,
    /// Password, must be between 3 and 100 characters long.
    pub password: String,
    // Version metadata added by SDK.
    pub version: Option<String>,
    // Context metadata added by SDK.
    pub context: Option<String>,
}

impl Command for LoginUser {
    fn code(&self) -> u32 {
        LOGIN_USER_CODE
    }
}

impl Default for LoginUser {
    fn default() -> Self {
        LoginUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            version: None,
            context: None,
        }
    }
}

impl Validatable<IggyError> for LoginUser {
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

impl BytesSerializable for LoginUser {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.username.len() + self.password.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.username.len() as u8);
        bytes.put_slice(self.username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.password.len() as u8);
        bytes.put_slice(self.password.as_bytes());
        match &self.version {
            Some(version) => {
                bytes.put_u32_le(version.len() as u32);
                bytes.put_slice(version.as_bytes());
            }
            None => {
                bytes.put_u32_le(0);
            }
        }
        match &self.context {
            Some(context) => {
                bytes.put_u32_le(context.len() as u32);
                bytes.put_slice(context.as_bytes());
            }
            None => {
                bytes.put_u32_le(0);
            }
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<LoginUser, IggyError> {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let username_length = *bytes.first().ok_or(IggyError::InvalidCommand)? as usize;
        let username = from_utf8(
            bytes
                .get(1..1 + username_length)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

        let pos = 1 + username_length;
        let password_length = *bytes.get(pos).ok_or(IggyError::InvalidCommand)? as usize;
        let password = from_utf8(
            bytes
                .get(pos + 1..pos + 1 + password_length)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

        let mut position = pos + 1 + password_length;

        // Version and context fields are optional for backward compatibility
        // with older SDKs (e.g. v0.8.0) that don't send them.
        // However, 1-3 trailing bytes (incomplete u32 length prefix) are rejected
        // as they indicate a corrupt payload rather than a valid old-SDK format.
        let remaining = bytes.len() - position;
        let version = if remaining == 0 {
            None
        } else if remaining < 4 {
            return Err(IggyError::InvalidCommand);
        } else {
            let version_length = u32::from_le_bytes(
                bytes
                    .get(position..position + 4)
                    .ok_or(IggyError::InvalidCommand)?
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            position += 4;
            match version_length {
                0 => None,
                _ => {
                    let version = from_utf8(
                        bytes
                            .get(position..position + version_length as usize)
                            .ok_or(IggyError::InvalidCommand)?,
                    )
                    .map_err(|_| IggyError::InvalidUtf8)?
                    .to_string();
                    position += version_length as usize;
                    Some(version)
                }
            }
        };

        let remaining = bytes.len() - position;
        let context = if remaining == 0 {
            None
        } else if remaining < 4 {
            return Err(IggyError::InvalidCommand);
        } else {
            let context_length = u32::from_le_bytes(
                bytes
                    .get(position..position + 4)
                    .ok_or(IggyError::InvalidCommand)?
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            );
            position += 4;
            match context_length {
                0 => None,
                _ => {
                    let context = from_utf8(
                        bytes
                            .get(position..position + context_length as usize)
                            .ok_or(IggyError::InvalidCommand)?,
                    )
                    .map_err(|_| IggyError::InvalidUtf8)?
                    .to_string();
                    Some(context)
                }
            }
        };

        let command = LoginUser {
            username,
            password,
            version,
            context,
        };
        Ok(command)
    }
}

impl Display for LoginUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|******", self.username)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = LoginUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            version: Some("1.0.0".to_string()),
            context: Some("test".to_string()),
        };

        let bytes = command.to_bytes();
        let username_length = bytes[0];
        let username = from_utf8(&bytes[1..=(username_length as usize)]).unwrap();
        let password_length = bytes[1 + username_length as usize];
        let password = from_utf8(
            &bytes[2 + username_length as usize
                ..2 + username_length as usize + password_length as usize],
        )
        .unwrap();
        let position = 2 + username_length as usize + password_length as usize;
        let version_length = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let version = Some(
            from_utf8(&bytes[position + 4..position + 4 + version_length as usize])
                .unwrap()
                .to_string(),
        );
        let position = position + 4 + version_length as usize;
        let context_length = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let context = Some(
            from_utf8(&bytes[position + 4..position + 4 + context_length as usize])
                .unwrap()
                .to_string(),
        );

        assert!(!bytes.is_empty());
        assert_eq!(username, command.username);
        assert_eq!(password, command.password);
        assert_eq!(version, command.version);
        assert_eq!(context, command.context);
    }

    #[test]
    fn from_bytes_should_fail_on_empty_input() {
        assert!(LoginUser::from_bytes(Bytes::new()).is_err());
    }

    #[test]
    fn from_bytes_should_fail_on_truncated_input() {
        let command = LoginUser {
            username: "user".to_string(),
            password: "secret".to_string(),
            version: Some("1.0.0".to_string()),
            context: Some("test".to_string()),
        };
        let bytes = command.to_bytes();
        // Truncate at every position up to (but not including) the version field.
        // Positions within username/password must error; positions at or past the
        // version boundary are valid old-SDK payloads.
        let version_offset = 2 + command.username.len() + command.password.len();
        for i in 0..version_offset {
            let truncated = bytes.slice(..i);
            assert!(
                LoginUser::from_bytes(truncated).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn from_bytes_should_fail_on_corrupted_username_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(255); // username_length = 255
        buf.put_slice(b"short");
        assert!(LoginUser::from_bytes(buf.freeze()).is_err());
    }

    #[test]
    fn from_bytes_should_fail_on_trailing_bytes() {
        let username = "user";
        let password = "secret";
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.put_slice(password.as_bytes());

        // 1-3 trailing bytes (incomplete u32 length prefix) must be rejected
        for extra in 1..=3u8 {
            let mut buf = bytes.clone();
            for i in 0..extra {
                buf.put_u8(i);
            }
            assert!(
                LoginUser::from_bytes(buf.freeze()).is_err(),
                "expected error for {extra} trailing byte(s)"
            );
        }
    }

    #[test]
    fn from_bytes_should_accept_old_sdk_format_without_version_context() {
        let username = "user";
        let password = "secret";
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.put_slice(password.as_bytes());

        let command = LoginUser::from_bytes(bytes.freeze()).unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
        assert_eq!(command.version, None);
        assert_eq!(command.context, None);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let username = "user";
        let password = "secret";
        let version = "1.0.0".to_string();
        let context = "test".to_string();
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(username.len() as u8);
        bytes.put_slice(username.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(password.len() as u8);
        bytes.put_slice(password.as_bytes());
        bytes.put_u32_le(version.len() as u32);
        bytes.put_slice(version.as_bytes());
        bytes.put_u32_le(context.len() as u32);
        bytes.put_slice(context.as_bytes());
        let command = LoginUser::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.username, username);
        assert_eq!(command.password, password);
        assert_eq!(command.version, Some(version));
        assert_eq!(command.context, Some(context));
    }
}
