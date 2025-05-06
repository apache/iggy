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

use crate::error::IggyError;
use crate::BytesSerializable;
use crate::Identifier;
use crate::Validatable;
use crate::{Command, GET_USER_CODE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetUser` command is used to retrieve the information about a user by unique ID.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct GetUser {
    #[serde(skip)]
    /// Unique user ID (numeric or name).
    pub user_id: Identifier,
}

impl Command for GetUser {
    fn code(&self) -> u32 {
        GET_USER_CODE
    }
}

impl Validatable<IggyError> for GetUser {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetUser {
    fn to_bytes(&self) -> Bytes {
        self.user_id.to_bytes()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetUser, IggyError> {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let command = GetUser { user_id };
        Ok(command)
    }
}

impl Display for GetUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetUser {
            user_id: Identifier::numeric(1).unwrap(),
        };

        let bytes = command.to_bytes();
        let user_id = Identifier::from_bytes(bytes.clone()).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let bytes = user_id.to_bytes();
        let command = GetUser::from_bytes(bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
    }
}
