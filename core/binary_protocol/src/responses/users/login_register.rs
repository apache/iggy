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
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// Combined login + register response for server-ng.
///
/// Returns the authenticated user's ID, the consensus session number
/// (commit op number from the Register operation), and the server's
/// protocol version + build version so both sides know what they talk to.
///
/// Wire format:
/// ```text
/// [user_id:u32 LE][session:u64 LE][server_protocol_version:u32 LE]
/// [server_version_len:u8][server_version:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginRegisterResponse {
    pub user_id: u32,
    pub session: u64,
    pub server_protocol_version: u32,
    pub server_version: WireName,
}

impl WireEncode for LoginRegisterResponse {
    fn encoded_size(&self) -> usize {
        16 + self.server_version.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.user_id);
        buf.put_u64_le(self.session);
        buf.put_u32_le(self.server_protocol_version);
        self.server_version.encode(buf);
    }
}

impl WireDecode for LoginRegisterResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let user_id = read_u32_le(buf, 0)?;
        let session = read_u64_le(buf, 4)?;
        let server_protocol_version = read_u32_le(buf, 12)?;
        let (server_version, server_version_len) = WireName::decode(&buf[16..])?;
        Ok((
            Self {
                user_id,
                session,
                server_protocol_version,
                server_version,
            },
            16 + server_version_len,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> LoginRegisterResponse {
        LoginRegisterResponse {
            user_id: 42,
            session: 100,
            server_protocol_version: 1,
            server_version: WireName::new("0.8.0").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let resp = sample();
        let bytes = resp.to_bytes();
        let (decoded, consumed) = LoginRegisterResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn encoded_size_matches_output() {
        let resp = sample();
        assert_eq!(resp.encoded_size(), resp.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let bytes = sample().to_bytes();
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
            server_protocol_version: 0x0D0E_0F10,
            server_version: WireName::new("v").unwrap(),
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
        assert_eq!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            0x0D0E_0F10
        );
        assert_eq!(bytes[16], 1);
        assert_eq!(bytes[17], b'v');
    }
}
