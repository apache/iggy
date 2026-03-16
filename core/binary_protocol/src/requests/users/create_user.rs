// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_str, read_u8, read_u32_le};
use crate::primitives::identifier::WireName;
use crate::primitives::permissions::WirePermissions;
use bytes::{BufMut, BytesMut};

/// `CreateUser` request.
///
/// Wire format:
/// `[username_len:u8][username:N][password_len:u8][password:N][status:u8]
///  [has_permissions:u8][permissions_len:u32_le?][permissions:M?]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUserRequest {
    pub username: WireName,
    pub password: String,
    pub status: u8,
    pub permissions: Option<WirePermissions>,
}

impl WireEncode for CreateUserRequest {
    fn encoded_size(&self) -> usize {
        self.username.encoded_size()
            + 1
            + self.password.len()
            + 1 // status
            + 1 // has_permissions
            + 4 // permissions_len (always present)
            + self.permissions.as_ref().map_or(0, WireEncode::encoded_size)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.username.encode(buf);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.password.len() as u8);
        buf.put_slice(self.password.as_bytes());
        buf.put_u8(self.status);
        if let Some(perms) = &self.permissions {
            buf.put_u8(1);
            let perm_bytes = perms.to_bytes();
            #[allow(clippy::cast_possible_truncation)]
            buf.put_u32_le(perm_bytes.len() as u32);
            buf.put_slice(&perm_bytes);
        } else {
            buf.put_u8(0);
            buf.put_u32_le(0);
        }
    }
}

impl WireDecode for CreateUserRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (username, mut pos) = WireName::decode(buf)?;

        let password_len = read_u8(buf, pos)? as usize;
        pos += 1;
        let password = read_str(buf, pos, password_len)?;
        pos += password_len;

        let status = read_u8(buf, pos)?;
        pos += 1;

        let has_permissions = read_u8(buf, pos)?;
        pos += 1;

        let perm_len = read_u32_le(buf, pos)? as usize;
        pos += 4;

        let permissions = if has_permissions == 1 && perm_len > 0 {
            let (perms, consumed) = WirePermissions::decode(&buf[pos..])?;
            if consumed != perm_len {
                return Err(WireError::Validation(format!(
                    "permissions length mismatch: header says {perm_len}, decoded {consumed}"
                )));
            }
            pos += consumed;
            Some(perms)
        } else {
            None
        };

        Ok((
            Self {
                username,
                password,
                status,
                permissions,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::permissions::{WireGlobalPermissions, WirePermissions};

    fn sample_permissions() -> WirePermissions {
        WirePermissions {
            global: WireGlobalPermissions {
                manage_servers: true,
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
            streams: vec![],
        }
    }

    #[test]
    fn roundtrip_without_permissions() {
        let req = CreateUserRequest {
            username: WireName::new("testuser").unwrap(),
            password: "secret123".to_string(),
            status: 1,
            permissions: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_with_permissions() {
        let req = CreateUserRequest {
            username: WireName::new("admin").unwrap(),
            password: "p@ssw0rd".to_string(),
            status: 2,
            permissions: Some(sample_permissions()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = CreateUserRequest {
            username: WireName::new("user").unwrap(),
            password: "pw".to_string(),
            status: 0,
            permissions: Some(sample_permissions()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = CreateUserRequest {
            username: WireName::new("user").unwrap(),
            password: "pass".to_string(),
            status: 1,
            permissions: None,
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                CreateUserRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn none_permissions_wire_layout() {
        let req = CreateUserRequest {
            username: WireName::new("u").unwrap(),
            password: "p".to_string(),
            status: 0,
            permissions: None,
        };
        let bytes = req.to_bytes();
        // username: [1, b'u'] + password: [1, b'p'] + status: [0]
        // + has_perm: [0] + perm_len: [0,0,0,0]
        let expected: &[u8] = &[1, b'u', 1, b'p', 0, 0, 0, 0, 0, 0];
        assert_eq!(&bytes[..], expected);
    }
}
