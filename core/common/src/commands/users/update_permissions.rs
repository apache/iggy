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
use crate::Identifier;
use crate::Permissions;
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::{Command, UPDATE_PERMISSIONS_CODE};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `UpdatePermissions` command is used to update a user's permissions.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `permissions` - new permissions (optional)
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct UpdatePermissions {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// New permissions if `None` is provided, then the existing user's permissions will be removed.
    pub permissions: Option<Permissions>,
}

impl Command for UpdatePermissions {
    fn code(&self) -> u32 {
        UPDATE_PERMISSIONS_CODE
    }
}

impl Validatable<IggyError> for UpdatePermissions {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for UpdatePermissions {
    fn to_bytes(&self) -> Bytes {
        let user_id_bytes = self.user_id.to_bytes();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id_bytes);
        if let Some(permissions) = &self.permissions {
            bytes.put_u8(1);
            bytes.put_u32_le(permissions.to_bytes().len() as u32);
            bytes.put_slice(&permissions.to_bytes());
        } else {
            bytes.put_u8(0);
        }

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<UpdatePermissions, IggyError> {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes.clone())?;
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let has_permissions = bytes[position];
        if has_permissions > 1 {
            return Err(IggyError::InvalidCommand);
        }

        position += 1;
        let permissions = if has_permissions == 1 {
            let permissions_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
            position += 4;
            let permissions = Permissions::from_bytes(
                bytes.slice(position..position + permissions_length as usize),
            )?;
            Some(permissions)
        } else {
            None
        };

        let command = UpdatePermissions {
            user_id,
            permissions,
        };
        Ok(command)
    }
}

impl Display for UpdatePermissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permissions = if let Some(permissions) = &self.permissions {
            permissions.to_string()
        } else {
            "no_permissions".to_string()
        };
        write!(f, "{}|{}", self.user_id, permissions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GlobalPermissions;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdatePermissions {
            user_id: Identifier::numeric(1).unwrap(),
            permissions: Some(get_permissions()),
        };
        let bytes = command.to_bytes();
        let user_id = Identifier::from_bytes(bytes.clone()).unwrap();
        let mut position = user_id.get_size_bytes().as_bytes_usize();
        let has_permissions = bytes[position];
        position += 1;
        let permissions_length =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        let permissions =
            Permissions::from_bytes(bytes.slice(position..position + permissions_length as usize))
                .unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(has_permissions, 1);
        assert_eq!(permissions, command.permissions.unwrap());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let permissions = get_permissions();
        let permissions_bytes = permissions.to_bytes();
        let mut bytes = BytesMut::new();
        bytes.put_slice(&user_id.to_bytes());
        bytes.put_u8(1);
        bytes.put_u32_le(permissions_bytes.len() as u32);
        bytes.put_slice(&permissions_bytes);

        let command = UpdatePermissions::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.permissions.unwrap(), permissions);
    }

    fn get_permissions() -> Permissions {
        Permissions {
            global: GlobalPermissions {
                manage_servers: true,
                read_servers: true,
                manage_users: true,
                read_users: true,
                manage_streams: false,
                read_streams: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
            },
            streams: None,
        }
    }
}
