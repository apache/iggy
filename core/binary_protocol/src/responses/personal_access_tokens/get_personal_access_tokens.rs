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
use crate::codec::{WireDecode, WireEncode, read_u64_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// Single personal access token entry on the wire.
///
/// Wire format:
/// ```text
/// [name_len:1][name:N][expiry_at:8]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalAccessTokenResponse {
    pub name: WireName,
    pub expiry_at: u64,
}

impl WireEncode for PersonalAccessTokenResponse {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size() + 8
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.name.encode(buf);
        buf.put_u64_le(self.expiry_at);
    }
}

impl WireDecode for PersonalAccessTokenResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (name, name_consumed) = WireName::decode(buf)?;
        let expiry_at = read_u64_le(buf, name_consumed)?;
        let consumed = name_consumed + 8;

        Ok((Self { name, expiry_at }, consumed))
    }
}

/// `GetPersonalAccessTokens` response: sequential token entries.
///
/// Wire format:
/// ```text
/// [PersonalAccessTokenResponse]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPersonalAccessTokensResponse {
    pub tokens: Vec<PersonalAccessTokenResponse>,
}

impl WireEncode for GetPersonalAccessTokensResponse {
    fn encoded_size(&self) -> usize {
        self.tokens.iter().map(WireEncode::encoded_size).sum()
    }

    fn encode(&self, buf: &mut BytesMut) {
        for token in &self.tokens {
            token.encode(buf);
        }
    }
}

impl WireDecode for GetPersonalAccessTokensResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut tokens = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (token, consumed) = PersonalAccessTokenResponse::decode(&buf[pos..])?;
            pos += consumed;
            tokens.push(token);
        }
        Ok((Self { tokens }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token(name: &str, expiry: u64) -> PersonalAccessTokenResponse {
        PersonalAccessTokenResponse {
            name: WireName::new(name).unwrap(),
            expiry_at: expiry,
        }
    }

    #[test]
    fn token_roundtrip() {
        let tok = sample_token("my-token", 1_710_000_000_000);
        let bytes = tok.to_bytes();
        assert_eq!(bytes.len(), 1 + 8 + 8);
        let (decoded, consumed) = PersonalAccessTokenResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, tok);
    }

    #[test]
    fn token_truncated_returns_error() {
        let tok = sample_token("t", 0);
        let bytes = tok.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                PersonalAccessTokenResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn list_roundtrip_empty() {
        let resp = GetPersonalAccessTokensResponse { tokens: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetPersonalAccessTokensResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn list_roundtrip_multiple() {
        let resp = GetPersonalAccessTokensResponse {
            tokens: vec![sample_token("token-a", 100), sample_token("token-b", 0)],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetPersonalAccessTokensResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }
}
