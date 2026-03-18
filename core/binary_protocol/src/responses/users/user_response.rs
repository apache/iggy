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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// User header on the wire. Used in both single-user and multi-user responses.
///
/// Wire format (13 + `username_len` bytes):
/// ```text
/// [id:4][created_at:8][status:1][username_len:1][username:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserResponse {
    pub id: u32,
    pub created_at: u64,
    pub status: u8,
    pub username: WireName,
}

impl UserResponse {
    const FIXED_SIZE: usize = 4 + 8 + 1; // 13
}

impl WireEncode for UserResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.username.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u8(self.status);
        self.username.encode(buf);
    }
}

impl WireDecode for UserResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let created_at = read_u64_le(buf, 4)?;
        let status = read_u8(buf, 12)?;
        let (username, name_consumed) = WireName::decode(&buf[13..])?;
        let consumed = 13 + name_consumed;

        Ok((
            Self {
                id,
                created_at,
                status,
                username,
            },
            consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> UserResponse {
        UserResponse {
            id: 1,
            created_at: 1_710_000_000_000,
            status: 1,
            username: WireName::new("admin").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), UserResponse::FIXED_SIZE + 1 + 5);
        let (decoded, consumed) = UserResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = sample();
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UserResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn multiple_users_sequential() {
        let u1 = UserResponse {
            id: 1,
            username: WireName::new("alice").unwrap(),
            ..sample()
        };
        let u2 = UserResponse {
            id: 2,
            username: WireName::new("bob").unwrap(),
            ..sample()
        };
        let mut buf = BytesMut::new();
        u1.encode(&mut buf);
        u2.encode(&mut buf);
        let bytes = buf.freeze();

        let (d1, pos1) = UserResponse::decode(&bytes).unwrap();
        let (d2, pos2) = UserResponse::decode(&bytes[pos1..]).unwrap();
        assert_eq!(d1, u1);
        assert_eq!(d2, u2);
        assert_eq!(pos1 + pos2, bytes.len());
    }
}
