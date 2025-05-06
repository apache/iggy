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
use crate::Validatable;
use crate::{Command, PING_CODE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `Ping` command is used to check if the server is alive.
/// It has no additional payload.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Ping {}

impl Command for Ping {
    fn code(&self) -> u32 {
        PING_CODE
    }
}

impl Validatable<IggyError> for Ping {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for Ping {
    fn to_bytes(&self) -> Bytes {
        Bytes::new()
    }

    fn from_bytes(bytes: Bytes) -> Result<Ping, IggyError> {
        if !bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        let command = Ping {};
        Ok(command)
    }
}

impl Display for Ping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let command = Ping {};
        let bytes = command.to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let command = Ping::from_bytes(Bytes::new());
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let command = Ping::from_bytes(Bytes::from_static(&[0]));
        assert!(command.is_err());
    }
}
