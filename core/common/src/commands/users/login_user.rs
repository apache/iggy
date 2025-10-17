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
use crate::wire::auth::{decode_login_auth, encode_login_auth};
use crate::{Command, LOGIN_USER_CODE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

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
        // Delegate to wire::auth module for centralized binary protocol handling
        encode_login_auth(&self.username, &self.password, &self.version, &self.context)
    }

    fn from_bytes(bytes: Bytes) -> Result<LoginUser, IggyError> {
        // Delegate to wire::auth module for centralized binary protocol handling
        let (username, password, version, context) = decode_login_auth(bytes)?;
        Ok(LoginUser {
            username,
            password,
            version,
            context,
        })
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
    use bytes::{BufMut, BytesMut};
    use std::str::from_utf8;

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
