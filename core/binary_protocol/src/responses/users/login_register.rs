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
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use bytes::{BufMut, BytesMut};

/// Combined login + register response for server-ng.
///
/// Returns the authenticated user's ID and the consensus session number
/// (commit op number from the Register operation).
///
/// Wire format (12 bytes):
/// ```text
/// [user_id:4 LE][session:8 LE]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginRegisterResponse {
    pub user_id: u32,
    pub session: u64,
}

impl WireEncode for LoginRegisterResponse {
    fn encoded_size(&self) -> usize {
        12
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        buf.put_u64_le(self.session);
    }
}

impl WireDecode for LoginRegisterResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let user_id = read_u32_le(buf, 0)?;
        let session = read_u64_le(buf, 4)?;
        Ok((Self { user_id, session }, 12))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let resp = LoginRegisterResponse {
            user_id: 42,
            session: 100,
        };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 12);
        let (decoded, consumed) = LoginRegisterResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 12);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = LoginRegisterResponse {
            user_id: 1,
            session: 1,
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LoginRegisterResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_layout() {
        let resp = LoginRegisterResponse {
            user_id: 0x0102_0304,
            session: 0x0506_0708_090A_0B0C,
        };
        let bytes = resp.to_bytes();
        assert_eq!(
            u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            0x0102_0304
        );
        assert_eq!(
            u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            0x0506_0708_090A_0B0C
        );
    }
}
