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
use crate::codec::{WireDecode, WireEncode, read_str, read_u8, read_u32_le, read_u128_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};
use secrecy::{ExposeSecret, SecretString};

/// Combined login + register request for server-ng.
///
/// The client sends credentials and its ephemeral `client_id`. The server
/// verifies credentials locally, then submits `Operation::Register` through
/// consensus. The response carries `user_id` + `session` (commit op number).
///
/// Wire format:
/// ```text
/// [client_id:16 LE][username_len:u8][username:N][password_len:u8][password:N]
/// [version_len:u32_le][version:N?][context_len:u32_le][context:N?]
/// ```
#[derive(Debug, Clone)]
pub struct LoginRegisterRequest {
    pub client_id: u128,
    pub username: WireName,
    pub password: SecretString,
    pub version: Option<String>,
    pub client_context: Option<String>,
}

impl WireEncode for LoginRegisterRequest {
    fn encoded_size(&self) -> usize {
        16 + self.username.encoded_size()
            + 1
            + self.password.expose_secret().len()
            + 4
            + self.version.as_ref().map_or(0, String::len)
            + 4
            + self.client_context.as_ref().map_or(0, String::len)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
        self.username.encode(buf);
        let password = self.password.expose_secret();
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(password.len() as u8);
        buf.put_slice(password.as_bytes());
        match &self.version {
            Some(v) => {
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(v.len() as u32);
                buf.put_slice(v.as_bytes());
            }
            None => buf.put_u32_le(0),
        }
        match &self.client_context {
            Some(c) => {
                #[allow(clippy::cast_possible_truncation)]
                buf.put_u32_le(c.len() as u32);
                buf.put_slice(c.as_bytes());
            }
            None => buf.put_u32_le(0),
        }
    }
}

impl WireDecode for LoginRegisterRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let client_id = read_u128_le(buf, 0)?;
        let mut pos = 16;

        let (username, name_len) = WireName::decode(&buf[pos..])?;
        pos += name_len;

        let password_len = read_u8(buf, pos)? as usize;
        pos += 1;
        let password = SecretString::from(read_str(buf, pos, password_len)?);
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

        let client_context_len = read_u32_le(buf, pos)? as usize;
        pos += 4;
        let client_context = if client_context_len > 0 {
            let c = read_str(buf, pos, client_context_len)?;
            pos += client_context_len;
            Some(c)
        } else {
            None
        };

        Ok((
            Self {
                client_id,
                username,
                password,
                version,
                client_context,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_req_eq(a: &LoginRegisterRequest, b: &LoginRegisterRequest) {
        assert_eq!(a.client_id, b.client_id);
        assert_eq!(a.username, b.username);
        assert_eq!(a.password.expose_secret(), b.password.expose_secret());
        assert_eq!(a.version, b.version);
        assert_eq!(a.client_context, b.client_context);
    }

    #[test]
    fn roundtrip_full() {
        let req = LoginRegisterRequest {
            client_id: 0xDEAD_BEEF_CAFE_BABE_1234_5678_9ABC_DEF0,
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("secret"),
            version: Some("1.0.0".to_string()),
            client_context: Some("rust-sdk".to_string()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginRegisterRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_req_eq(&decoded, &req);
    }

    #[test]
    fn roundtrip_no_optionals() {
        let req = LoginRegisterRequest {
            client_id: 42,
            username: WireName::new("user").unwrap(),
            password: SecretString::from("pass"),
            version: None,
            client_context: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginRegisterRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_req_eq(&decoded, &req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = LoginRegisterRequest {
            client_id: 1,
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("p"),
            version: Some("v1".to_string()),
            client_context: Some("ctx".to_string()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = LoginRegisterRequest {
            client_id: 1,
            username: WireName::new("u").unwrap(),
            password: SecretString::from("p"),
            version: Some("v".to_string()),
            client_context: Some("c".to_string()),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LoginRegisterRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_layout_client_id_first() {
        let req = LoginRegisterRequest {
            client_id: 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
            username: WireName::new("u").unwrap(),
            password: SecretString::from("p"),
            version: None,
            client_context: None,
        };
        let bytes = req.to_bytes();
        // First 16 bytes are client_id in LE.
        let client_id = u128::from_le_bytes(bytes[..16].try_into().unwrap());
        assert_eq!(client_id, req.client_id);
        // Then username: [1, b'u'], password: [1, b'p'], version: [0,0,0,0], client_context: [0,0,0,0]
        assert_eq!(bytes[16], 1); // username len
        assert_eq!(bytes[17], b'u');
    }
}
