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
use crate::primitives::options::WireOptions;
use bytes::{BufMut, BytesMut};

/// `UpdateUser` request.
///
/// Wire format:
/// `[user_id:WireIdentifier][has_username:u8][username_len:u8?][username:N?]
///  [has_status:u8][status:u8?][options TLV to end]`
///
/// The options block mirrors `CreateUser`'s, so a user setting added to the
/// catalog is updatable without another layout change. Keys absent from the
/// block are left alone rather than reset: an update patches the stored map,
/// so a client built before a key existed cannot erase it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateUserRequest {
    pub user_id: WireIdentifier,
    pub username: Option<WireName>,
    pub status: Option<u8>,
    pub options: WireOptions,
}

impl WireEncode for UpdateUserRequest {
    fn encoded_size(&self) -> usize {
        self.user_id.encoded_size()
            + 1 // has_username
            + self.username.as_ref().map_or(0, WireEncode::encoded_size)
            + 1 // has_status
            + self.status.map_or(0, |_| 1)
            + self.options.encoded_size()
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
        self.options.encode(buf);
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

        let options = WireOptions::from_slice(&buf[pos..])?;
        Ok((
            Self {
                user_id,
                username,
                status,
                options,
            },
            buf.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    fn sample_options() -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(&[(2, b"future_key", 2, b"value")], &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }

    #[test]
    fn roundtrip_both_present() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("new-name").unwrap()),
            status: Some(2),
            options: WireOptions::empty(),
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
            options: WireOptions::empty(),
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
            options: WireOptions::empty(),
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
            options: WireOptions::empty(),
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
            options: WireOptions::empty(),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_options_return_error() {
        // One pair, so any strict-interior truncation is invalid. Cutting at
        // the block boundary is a legitimate update carrying no options.
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("name").unwrap()),
            status: Some(1),
            options: sample_options(),
        };
        let bytes = req.to_bytes();
        let fixed_end = bytes.len() - req.options.encoded_size();
        for i in fixed_end + 1..bytes.len() {
            assert!(
                UpdateUserRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn truncated_returns_error() {
        let req = UpdateUserRequest {
            user_id: WireIdentifier::numeric(1),
            username: Some(WireName::new("name").unwrap()),
            status: Some(1),
            options: WireOptions::empty(),
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
