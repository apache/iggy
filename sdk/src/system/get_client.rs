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
use crate::command::{Command, GET_CLIENT_CODE};
use crate::error::IggyError;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `GetClient` command is used to get the information about a specific client by unique ID.
/// It has additional payload:
/// - `client_id` - unique ID (numeric) of the client.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct GetClient {
    /// Unique ID (numeric) of the client.
    pub client_id: u32,
}

impl Command for GetClient {
    fn code(&self) -> u32 {
        GET_CLIENT_CODE
    }
}

impl Default for GetClient {
    fn default() -> Self {
        GetClient { client_id: 1 }
    }
}

impl Validatable<IggyError> for GetClient {
    fn validate(&self) -> Result<(), IggyError> {
        if self.client_id == 0 {
            return Err(IggyError::InvalidClientId);
        }

        Ok(())
    }
}

impl BytesSerializable for GetClient {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32_le(self.client_id);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<GetClient, IggyError> {
        if bytes.len() != 4 {
            return Err(IggyError::InvalidCommand);
        }

        let client_id = u32::from_le_bytes(
            bytes
                .as_ref()
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let command = GetClient { client_id };
        Ok(command)
    }
}

impl Display for GetClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = GetClient { client_id: 1 };

        let bytes = command.to_bytes();
        let client_id = u32::from_le_bytes(bytes[..4].try_into().unwrap());

        assert!(!bytes.is_empty());
        assert_eq!(client_id, command.client_id);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let client_id = 1u32;
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32_le(client_id);
        let command = GetClient::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.client_id, client_id);
    }
}
