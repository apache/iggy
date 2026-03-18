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
use crate::codec::{WireDecode, WireEncode, read_u8};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// `UpdateUser` request.
///
/// Wire format:
/// `[user_id:WireIdentifier][has_username:u8][username_len:u8?][username:N?]
///  [has_status:u8][status:u8?]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserRequest {
    pub user_id: WireIdentifier,
    pub username: Option<WireName>,
    pub status: Option<u8>,
}

impl WireEncode for UpdateUserRequest {
    fn encoded_size(&self) -> usize {
        self.user_id.encoded_size()
            + 1 // has_username
            + self.username.as_ref().map_or(0, WireEncode::encoded_size)
            + 1 // has_status
            + self.status.map_or(0, |_| 1)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.user_id.encode(buf);
        match &self.username {
            Some(name) => {
                buf.put_u8(1);
                name.encode(buf);
            }
            None => {
                buf.put_u8(0);
            }
        }
        match self.status {
            Some(s) => {
                buf.put_u8(1);
                buf.put_u8(s);
            }
            None => {
                buf.put_u8(0);
            }
        }
    }
}

impl WireDecode for UpdateUserRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (user_id, mut pos) = WireIdentifier::decode(buf)?;

        let has_username = read_u8(buf, pos)?;
        pos += 1;
        let username = if has_username == 1 {
            let (name, consumed) = WireName::decode(&buf[pos..])?;
            pos += consumed;
            Some(name)
        } else {
            None
        };

        let has_status = read_u8(buf, pos)?;
        pos += 1;
        let status = if has_status == 1 {
            let s = read_u8(buf, pos)?;
            pos += 1;
            Some(s)
        } else {
            None
        };

        Ok((
            Self {
                user_id,
                username,
                status,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_both_present() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("new-name").unwrap()),
            status: Some(2),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_both_none() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(5),
            username: None,
            status: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_username_only() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::named("admin").unwrap(),
            username: Some(WireName::new("super-admin").unwrap()),
            status: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_status_only() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(10),
            username: None,
            status: Some(0),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("test").unwrap()),
            status: Some(1),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("name").unwrap()),
            status: Some(1),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdateUserRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
