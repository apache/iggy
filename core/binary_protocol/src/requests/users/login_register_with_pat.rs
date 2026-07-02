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
use crate::version::ClientVersionInfo;
use bytes::{BufMut, BytesMut};
use secrecy::{ExposeSecret, SecretString};

/// Combined login-with-PAT + register request for server-ng.
///
/// Shares the `ClientVersionInfo` prefix with `LoginRegisterRequest` so the
/// server gates on the protocol version once before attempting either body
/// shape. The server verifies the token locally, then submits
/// `Operation::Register` through consensus. The `client_id` is carried in
/// the VSR `RequestHeader.client` field (populated by the SDK at encode
/// time); the body no longer duplicates it.
///
/// Wire format:
/// ```text
/// [ClientVersionInfo]
/// [token_len:u8][token:N]
/// [context_len:u32_le][context:N?]
/// ```
#[derive(Debug, Clone)]
pub struct LoginRegisterWithPatRequest {
    pub version_info: ClientVersionInfo,
    pub token: SecretString,
    pub client_context: Option<String>,
}

impl WireEncode for LoginRegisterWithPatRequest {
    fn encoded_size(&self) -> usize {
        self.version_info.encoded_size()
            + 1
            + self.token.expose_secret().len()
            + 4
            + self.client_context.as_ref().map_or(0, String::len)
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.version_info.encode(buf);
        let token = self.token.expose_secret();
        debug_assert!(
            u8::try_from(token.len()).is_ok(),
            "token exceeds u8 length prefix; callers must validate before encoding"
        );
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(token.len() as u8);
        buf.put_slice(token.as_bytes());
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

impl LoginRegisterWithPatRequest {
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
        let token_len = read_u8(tail, 0)? as usize;
        let mut pos = 1;
        let token = SecretString::from(read_str(tail, pos, token_len)?);
        pos += token_len;

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
                token,
                client_context,
            },
            pos,
        ))
    }
}

impl WireDecode for LoginRegisterWithPatRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (version_info, prefix_len) = ClientVersionInfo::decode(buf)?;
        let (request, body_len) = Self::decode_after_prefix(version_info, &buf[prefix_len..])?;
        Ok((request, prefix_len + body_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::identifier::WireName;

    fn version_info() -> ClientVersionInfo {
        ClientVersionInfo {
            protocol_version: 1,
            sdk_name: WireName::new("rust-sdk").unwrap(),
            sdk_version: WireName::new("1.0.0").unwrap(),
        }
    }

    fn assert_req_eq(a: &LoginRegisterWithPatRequest, b: &LoginRegisterWithPatRequest) {
        assert_eq!(a.version_info, b.version_info);
        assert_eq!(a.token.expose_secret(), b.token.expose_secret());
        assert_eq!(a.client_context, b.client_context);
    }

    #[test]
    fn roundtrip_full() {
        let req = LoginRegisterWithPatRequest {
            version_info: version_info(),
            token: SecretString::from("pat-abc123def456"),
            client_context: Some("rust-sdk".to_string()),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginRegisterWithPatRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_req_eq(&decoded, &req);
    }

    #[test]
    fn roundtrip_no_context() {
        let req = LoginRegisterWithPatRequest {
            version_info: version_info(),
            token: SecretString::from("tok"),
            client_context: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginRegisterWithPatRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_req_eq(&decoded, &req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = LoginRegisterWithPatRequest {
            version_info: version_info(),
            token: SecretString::from("t"),
            client_context: Some("ctx".to_string()),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = LoginRegisterWithPatRequest {
            version_info: version_info(),
            token: SecretString::from("t"),
            client_context: Some("c".to_string()),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LoginRegisterWithPatRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_layout_version_info_first() {
        let req = LoginRegisterWithPatRequest {
            version_info: version_info(),
            token: SecretString::from("t"),
            client_context: None,
        };
        let bytes = req.to_bytes();
        assert_eq!(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 1);
        assert_eq!(bytes[4], 8);
        assert_eq!(&bytes[5..13], b"rust-sdk");
    }
}
