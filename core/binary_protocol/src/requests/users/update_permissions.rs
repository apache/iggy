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
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le};
use crate::primitives::permissions::WirePermissions;
use bytes::{BufMut, BytesMut};
use std::borrow::Cow;

/// `UpdatePermissions` request.
///
/// Wire format:
/// `[user_id:WireIdentifier][has_permissions:u8][permissions_len:u32_le?][permissions:M?]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePermissionsRequest {
    pub user_id: WireIdentifier,
    pub permissions: Option<WirePermissions>,
}

impl WireEncode for UpdatePermissionsRequest {
    fn encoded_size(&self) -> usize {
        self.user_id.encoded_size()
            + 1 // has_permissions
            + self
                .permissions
                .as_ref()
                .map_or(0, |p| 4 + p.encoded_size())
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user_id.encode(buf);
        if let Some(perms) = &self.permissions {
            buf.put_u8(1);
            let perm_bytes = perms.to_bytes();
            #[allow(clippy::cast_possible_truncation)]
            buf.put_u32_le(perm_bytes.len() as u32);
            buf.put_slice(&perm_bytes);
        } else {
            buf.put_u8(0);
        }
    }
}

impl WireDecode for UpdatePermissionsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (user_id, mut pos) = WireIdentifier::decode(buf)?;

        let has_permissions = read_u8(buf, pos)?;
        pos += 1;

        let permissions = if has_permissions == 1 {
            let perm_len = read_u32_le(buf, pos)? as usize;
            pos += 4;
            let (perms, consumed) = WirePermissions::decode(&buf[pos..])?;
            if consumed != perm_len {
                return Err(WireError::Validation(Cow::Owned(format!(
                    "permissions length mismatch: header says {perm_len}, decoded {consumed}"
                ))));
            }
            pos += consumed;
            Some(perms)
        } else {
            None
        };

        Ok((
            Self {
                user_id,
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
                manage_users: true,
                read_users: true,
                manage_streams: false,
                read_streams: false,
                manage_topics: false,
                read_topics: false,
                poll_messages: true,
                send_messages: true,
            },
            streams: vec![],
        }
    }

    #[test]
    fn roundtrip_with_permissions() {
        let req = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(1),
            permissions: Some(sample_permissions()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdatePermissionsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_without_permissions() {
        let req = UpdatePermissionsRequest {
            user_id: WireIdentifier::named("admin").unwrap(),
            permissions: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdatePermissionsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(42),
            permissions: Some(sample_permissions()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(1),
            permissions: Some(sample_permissions()),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdatePermissionsRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
