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
use crate::{Command, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

/// `LoginWithPersonalAccessToken` command is used to login the user with a personal access token, instead of the username and password.
/// It has additional payload:
/// - `token` - personal access token
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct LoginWithPersonalAccessToken {
    /// Personal access token
    pub token: String,
}

impl Command for LoginWithPersonalAccessToken {
    fn code(&self) -> u32 {
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
    }
}

impl Default for LoginWithPersonalAccessToken {
    fn default() -> Self {
        LoginWithPersonalAccessToken {
            token: "token".to_string(),
        }
    }
}

impl Validatable<IggyError> for LoginWithPersonalAccessToken {
    fn validate(&self) -> Result<(), IggyError> {
        if self.token.is_empty() || self.token.len() > MAX_PAT_LENGTH {
            return Err(IggyError::InvalidPersonalAccessToken);
        }

        Ok(())
    }
}

impl BytesSerializable for LoginWithPersonalAccessToken {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(5 + self.token.len());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.token.len() as u8);
        bytes.put_slice(self.token.as_bytes());
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<LoginWithPersonalAccessToken, IggyError> {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let token_length = *bytes.first().ok_or(IggyError::InvalidCommand)? as usize;
        let token = from_utf8(
            bytes
                .get(1..1 + token_length)
                .ok_or(IggyError::InvalidCommand)?,
        )
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

        let command = LoginWithPersonalAccessToken { token };
        Ok(command)
    }
}

impl Display for LoginWithPersonalAccessToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = LoginWithPersonalAccessToken {
            token: "test".to_string(),
        };

        let bytes = command.to_bytes();
        let token_length = bytes[0];
        let token = from_utf8(&bytes[1..1 + token_length as usize]).unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(token, command.token);
    }

    #[test]
    fn from_bytes_should_fail_on_empty_input() {
        assert!(LoginWithPersonalAccessToken::from_bytes(Bytes::new()).is_err());
    }

    #[test]
    fn from_bytes_should_fail_on_truncated_input() {
        let command = LoginWithPersonalAccessToken::default();
        let bytes = command.to_bytes();
        for i in 0..bytes.len() - 1 {
            let truncated = bytes.slice(..i);
            assert!(
                LoginWithPersonalAccessToken::from_bytes(truncated).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn from_bytes_should_fail_on_corrupted_token_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(255);
        buf.put_slice(b"short");
        assert!(LoginWithPersonalAccessToken::from_bytes(buf.freeze()).is_err());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let token = "test";
        let mut bytes = BytesMut::new();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(token.len() as u8);
        bytes.put_slice(token.as_bytes());

        let command = LoginWithPersonalAccessToken::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.token, token);
    }
}
