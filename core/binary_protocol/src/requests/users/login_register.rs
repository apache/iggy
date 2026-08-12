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
use crate::version::ClientVersionInfo;
use bytes::{BufMut, BytesMut};
use secrecy::{ExposeSecret, SecretString};

/// Combined login + register request for server-ng.
///
/// The server gates on `version_info.protocol_version` (see
/// [`crate::version::is_protocol_compatible`]), verifies credentials
/// locally, then submits `Operation::Register` through consensus. The
/// response carries `user_id` + `session` (commit op number) plus the
/// server's own protocol version and build version. The `client_id` is
/// carried in the VSR `RequestHeader.client` field (populated by the SDK
/// at encode time); the body no longer duplicates it.
///
/// Wire format:
/// ```text
/// [ClientVersionInfo]
/// [username_len:u8][username:N][password_len:u8][password:N]
/// [context_len:u32_le][context:N?]
/// ```
///
/// `ClientVersionInfo` is encoded first so the server can always parse the
/// version prefix and reject incompatible clients (with an
/// `EvictionReason::IncompatibleProtocol` frame carrying the accepted
/// range) before touching the rest of the body.
///
/// # Cross-version compatibility
///
/// This wire shape is gated by the `vsr` cargo feature and lives under
/// `LOGIN_REGISTER_CODE`. The legacy `LOGIN_USER_CODE` shape (still in use
/// by non-`vsr` builds against the legacy `iggy-server`) is untouched.
/// server-ng speaks VSR framing only; a non-`vsr` SDK cannot log in to
/// server-ng. Foreign-language SDKs (C++, C#, Python, Go, Java) adopt this
/// shape, with their own `sdk_name`, when they wire VSR framing. Bump
/// [`crate::version::IGGY_PROTOCOL_VERSION`] on any wire-incompatible
/// change.
#[derive(Debug, Clone)]
pub struct LoginRegisterRequest {
    pub version_info: ClientVersionInfo,
    pub username: WireName,
    pub password: SecretString,
    pub client_context: Option<String>,
}

impl WireEncode for LoginRegisterRequest {
    fn encoded_size(&self) -> usize {
        self.version_info.encoded_size()
            + self.username.encoded_size()
            + 1
            + self.password.expose_secret().len()
            + 4
            + self.client_context.as_ref().map_or(0, String::len)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.version_info.encode(buf);
        self.username.encode(buf);
        let password = self.password.expose_secret();
        debug_assert!(
            u8::try_from(password.len()).is_ok(),
            "password exceeds u8 length prefix; callers must validate before encoding"
        );
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(password.len() as u8);
        buf.put_slice(password.as_bytes());
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

impl LoginRegisterRequest {
    /// Decode the body after the [`ClientVersionInfo`] prefix has already
    /// been consumed; the server-side login gate decodes the prefix once
    /// for both login-register shapes. Returns the bytes consumed from
    /// `tail`.
    ///
    /// # Errors
    /// [`WireError`] when `tail` is truncated or a field is malformed.
    pub fn decode_after_prefix(
        version_info: ClientVersionInfo,
        tail: &[u8],
    ) -> Result<(Self, usize), WireError> {
        let (username, name_len) = WireName::decode(tail)?;
        let mut pos = name_len;

        let password_len = read_u8(tail, pos)? as usize;
        pos += 1;
        let password = SecretString::from(read_str(tail, pos, password_len)?);
        pos += password_len;

        let client_context_len = read_u32_le(tail, pos)? as usize;
        pos += 4;
        let client_context = if client_context_len > 0 {
            let c = read_str(tail, pos, client_context_len)?;
            pos += client_context_len;
            Some(c)
        } else {
            None
        };

        Ok((
            Self {
                version_info,
                username,
                password,
                client_context,
            },
            pos,
        ))
    }
}

impl WireDecode for LoginRegisterRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (version_info, prefix_len) = ClientVersionInfo::decode(buf)?;
        let (request, body_len) = Self::decode_after_prefix(version_info, &buf[prefix_len..])?;
        Ok((request, prefix_len + body_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version_info() -> ClientVersionInfo {
        ClientVersionInfo {
            protocol_version: 1,
            sdk_name: WireName::new("rust-sdk").unwrap(),
            sdk_version: WireName::new("1.0.0").unwrap(),
        }
    }

    fn assert_req_eq(a: &LoginRegisterRequest, b: &LoginRegisterRequest) {
        assert_eq!(a.version_info, b.version_info);
        assert_eq!(a.username, b.username);
        assert_eq!(a.password.expose_secret(), b.password.expose_secret());
        assert_eq!(a.client_context, b.client_context);
    }

    #[test]
    fn roundtrip_full() {
        let req = LoginRegisterRequest {
            version_info: version_info(),
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("secret"),
            client_context: Some("rust-sdk".to_string()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginRegisterRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_req_eq(&decoded, &req);
    }

    #[test]
    fn roundtrip_no_context() {
        let req = LoginRegisterRequest {
            version_info: version_info(),
            username: WireName::new("user").unwrap(),
            password: SecretString::from("pass"),
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
            version_info: version_info(),
            username: WireName::new("admin").unwrap(),
            password: SecretString::from("p"),
            client_context: Some("ctx".to_string()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = LoginRegisterRequest {
            version_info: version_info(),
            username: WireName::new("u").unwrap(),
            password: SecretString::from("p"),
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
    fn wire_layout_version_info_first() {
        let req = LoginRegisterRequest {
            version_info: version_info(),
            username: WireName::new("u").unwrap(),
            password: SecretString::from("p"),
            client_context: None,
        };
        let bytes = req.to_bytes();
        // protocol_version u32 LE, then sdk_name [8, b"rust-sdk"].
        assert_eq!(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 1);
        assert_eq!(bytes[4], 8);
        assert_eq!(&bytes[5..13], b"rust-sdk");
        // The version prefix alone must decode even when the rest is garbage.
        let prefix_len = req.version_info.encoded_size();
        let (prefix, consumed) = ClientVersionInfo::decode(&bytes).unwrap();
        assert_eq!(consumed, prefix_len);
        assert_eq!(prefix, req.version_info);
    }
}
