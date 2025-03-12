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
use crate::command::{Command, GET_PERSONAL_ACCESS_TOKENS_CODE};
use crate::error::IggyError;
use crate::validatable::Validatable;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetPersonalAccessTokens` command is used to get all personal access tokens for the authenticated user.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct GetPersonalAccessTokens {}

impl Command for GetPersonalAccessTokens {
    fn code(&self) -> u32 {
        GET_PERSONAL_ACCESS_TOKENS_CODE
    }
}

impl Validatable<IggyError> for GetPersonalAccessTokens {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for GetPersonalAccessTokens {
    fn to_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetPersonalAccessTokens, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        Ok(GetPersonalAccessTokens {})
    }
}

impl Display for GetPersonalAccessTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = GetPersonalAccessTokens {};
        let bytes = command.to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let command = GetPersonalAccessTokens::from_bytes(Bytes::new());
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let command = GetPersonalAccessTokens::from_bytes(Bytes::from_static(&[0]));
        assert!(command.is_err());
    }
}
