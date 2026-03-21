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
use bytes::{BufMut, BytesMut};

/// `LoginUser` request.
///
/// Wire format:
/// `[username_len:u8][username:N][password_len:u8][password:N]
///  [version_len:u32_le][version:N?][context_len:u32_le][context:N?]`
///
/// Both `version_len` and `context_len` are always present on the wire.
/// A length of 0 means the field is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginUserRequest {
    pub username: WireName,
    pub password: String,
    pub version: Option<String>,
    pub context: Option<String>,
}

impl WireEncode for LoginUserRequest {
    fn encoded_size(&self) -> usize {
        self.username.encoded_size()
            + 1
            + self.password.len()
            + 4
            + self.version.as_ref().map_or(0, String::len)
            + 4
            + self.context.as_ref().map_or(0, String::len)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.username.encode(buf);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.password.len() as u8);
        buf.put_slice(self.password.as_bytes());
        match &self.version {
            Some(v) => {
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(v.len() as u32);
                buf.put_slice(v.as_bytes());
            }
            None => {
                buf.put_u32_le(0);
            }
        }
        match &self.context {
            Some(c) => {
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(c.len() as u32);
                buf.put_slice(c.as_bytes());
            }
            None => {
                buf.put_u32_le(0);
            }
        }
    }
}

impl WireDecode for LoginUserRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (username, mut pos) = WireName::decode(buf)?;

        let password_len = read_u8(buf, pos)? as usize;
        pos += 1;
        let password = read_str(buf, pos, password_len)?;
        pos += password_len;

        let version_len = read_u32_le(buf, pos)? as usize;
        pos += 4;
        let version = if version_len > 0 {
            let v = read_str(buf, pos, version_len)?;
            pos += version_len;
            Some(v)
        } else {
            None
        };

        let context_len = read_u32_le(buf, pos)? as usize;
        pos += 4;
        let context = if context_len > 0 {
            let c = read_str(buf, pos, context_len)?;
            pos += context_len;
            Some(c)
        } else {
            None
        };

        Ok((
            Self {
                username,
                password,
                version,
                context,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_full() {
        let req = LoginUserRequest {
            username: WireName::new("admin").unwrap(),
            password: "secret".to_string(),
            version: Some("1.0.0".to_string()),
            context: Some("rust-sdk".to_string()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_no_version_no_context() {
        let req = LoginUserRequest {
            username: WireName::new("user").unwrap(),
            password: "pass".to_string(),
            version: None,
            context: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_version_only() {
        let req = LoginUserRequest {
            username: WireName::new("user").unwrap(),
            password: "pw".to_string(),
            version: Some("2.3.4".to_string()),
            context: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_context_only() {
        let req = LoginUserRequest {
            username: WireName::new("user").unwrap(),
            password: "pw".to_string(),
            version: None,
            context: Some("test-ctx".to_string()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginUserRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = LoginUserRequest {
            username: WireName::new("admin").unwrap(),
            password: "p".to_string(),
            version: Some("v1".to_string()),
            context: Some("ctx".to_string()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = LoginUserRequest {
            username: WireName::new("u").unwrap(),
            password: "p".to_string(),
            version: Some("v".to_string()),
            context: Some("c".to_string()),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LoginUserRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_layout_no_optionals() {
        let req = LoginUserRequest {
            username: WireName::new("u").unwrap(),
            password: "p".to_string(),
            version: None,
            context: None,
        };
        let bytes = req.to_bytes();
        // username: [1, b'u'] + password: [1, b'p'] + version_len: [0,0,0,0] + context_len: [0,0,0,0]
        let expected: &[u8] = &[1, b'u', 1, b'p', 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(&bytes[..], expected);
    }
}
