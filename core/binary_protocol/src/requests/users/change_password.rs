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
use crate::codec::{WireDecode, WireEncode, read_str, read_u8};
use bytes::{BufMut, BytesMut};

/// `ChangePassword` request.
///
/// Wire format:
/// `[user_id:WireIdentifier][current_password_len:u8][current_password:N]
///  [new_password_len:u8][new_password:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePasswordRequest {
    pub user_id: WireIdentifier,
    pub current_password: String,
    pub new_password: String,
}

impl WireEncode for ChangePasswordRequest {
    fn encoded_size(&self) -> usize {
        self.user_id.encoded_size() + 1 + self.current_password.len() + 1 + self.new_password.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user_id.encode(buf);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.current_password.len() as u8);
        buf.put_slice(self.current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.new_password.len() as u8);
        buf.put_slice(self.new_password.as_bytes());
    }
}

impl WireDecode for ChangePasswordRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (user_id, mut pos) = WireIdentifier::decode(buf)?;

        let current_len = read_u8(buf, pos)? as usize;
        pos += 1;
        let current_password = read_str(buf, pos, current_len)?;
        pos += current_len;

        let new_len = read_u8(buf, pos)? as usize;
        pos += 1;
        let new_password = read_str(buf, pos, new_len)?;
        pos += new_len;

        Ok((
            Self {
                user_id,
                current_password,
                new_password,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = ChangePasswordRequest {
            user_id: WireIdentifier::numeric(1),
            current_password: "old-pass".to_string(),
            new_password: "new-pass-123".to_string(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = ChangePasswordRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_user() {
        let req = ChangePasswordRequest {
            user_id: WireIdentifier::named("admin").unwrap(),
            current_password: "root".to_string(),
            new_password: "s3cure!".to_string(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = ChangePasswordRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = ChangePasswordRequest {
            user_id: WireIdentifier::numeric(42),
            current_password: "abc".to_string(),
            new_password: "xyz123".to_string(),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = ChangePasswordRequest {
            user_id: WireIdentifier::numeric(1),
            current_password: "old".to_string(),
            new_password: "new".to_string(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ChangePasswordRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
