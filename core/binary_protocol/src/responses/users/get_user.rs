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
use crate::codec::{WireDecode, WireEncode, read_bytes, read_u8, read_u32_le};
use crate::primitives::permissions::WirePermissions;
use crate::responses::users::user_response::UserResponse;
use bytes::{BufMut, BytesMut};

/// `GetUser` response: user header followed by optional permissions.
///
/// Wire format:
/// ```text
/// [UserResponse]
/// If no permissions:  [0x00 0x00 0x00 0x00]     (4 bytes)
/// If has permissions:  [0x01][len:4][perms:N]    (1 + 4 + N bytes)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserDetailsResponse {
    pub user: UserResponse,
    pub permissions: Option<WirePermissions>,
}

impl WireEncode for UserDetailsResponse {
    fn encoded_size(&self) -> usize {
        self.user.encoded_size()
            + self
                .permissions
                .as_ref()
                .map_or(4, |perms| 1 + 4 + perms.encoded_size())
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user.encode(buf);
        match &self.permissions {
            Some(perms) => {
                buf.put_u8(1);
                let perm_bytes = perms.to_bytes();
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(perm_bytes.len() as u32);
                buf.put_slice(&perm_bytes);
            }
            None => {
                buf.put_u32_le(0);
            }
        }
    }
}

impl WireDecode for UserDetailsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (user, mut pos) = UserResponse::decode(buf)?;
        let flag = read_u8(buf, pos)?;
        pos += 1;

        let permissions = if flag == 0 {
            // No-permissions encoding: first byte of u32_le(0) was 0x00,
            // validate and skip remaining 3 padding bytes.
            let _ = read_bytes(buf, pos, 3)?;
            pos += 3;
            None
        } else {
            let perm_len = read_u32_le(buf, pos)? as usize;
            pos += 4;
            let perm_buf = read_bytes(buf, pos, perm_len)?;
            let (perms, _) = WirePermissions::decode(perm_buf)?;
            pos += perm_len;
            Some(perms)
        };

        Ok((Self { user, permissions }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;
    use crate::primitives::permissions::{
        WireGlobalPermissions, WireStreamPermissions, WireTopicPermissions,
    };

    fn sample_user() -> UserResponse {
        UserResponse {
            id: 1,
            created_at: 1_710_000_000_000,
            status: 1,
            username: WireName::new("admin").unwrap(),
        }
    }

    fn make_global(all: bool) -> WireGlobalPermissions {
        WireGlobalPermissions {
            manage_servers: all,
            read_servers: all,
            manage_users: all,
            read_users: all,
            manage_streams: all,
            read_streams: all,
            manage_topics: all,
            read_topics: all,
            poll_messages: all,
            send_messages: all,
        }
    }

    #[test]
    fn roundtrip_no_permissions() {
        let resp = UserDetailsResponse {
            user: sample_user(),
            permissions: None,
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = UserDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_with_permissions() {
        let perms = WirePermissions {
            global: make_global(true),
            streams: vec![WireStreamPermissions {
                stream_id: 1,
                manage_stream: true,
                read_stream: true,
                manage_topics: false,
                read_topics: true,
                poll_messages: true,
                send_messages: false,
                topics: vec![WireTopicPermissions {
                    topic_id: 10,
                    manage_topic: true,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: true,
                }],
            }],
        };
        let resp = UserDetailsResponse {
            user: sample_user(),
            permissions: Some(perms),
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = UserDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn no_permissions_wire_format() {
        let resp = UserDetailsResponse {
            user: sample_user(),
            permissions: None,
        };
        let bytes = resp.to_bytes();
        let user_size = sample_user().encoded_size();
        // Last 4 bytes should be all zeros (u32_le(0))
        assert_eq!(&bytes[user_size..user_size + 4], &[0, 0, 0, 0]);
    }

    #[test]
    fn has_permissions_wire_flag() {
        let perms = WirePermissions {
            global: make_global(false),
            streams: vec![],
        };
        let resp = UserDetailsResponse {
            user: sample_user(),
            permissions: Some(perms),
        };
        let bytes = resp.to_bytes();
        let user_size = sample_user().encoded_size();
        assert_eq!(bytes[user_size], 1);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = UserDetailsResponse {
            user: sample_user(),
            permissions: None,
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UserDetailsResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
