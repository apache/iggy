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
use crate::codec::{WireDecode, WireEncode};
use crate::primitives::identifier::WireName;
use bytes::BytesMut;

/// Response for `CreatePersonalAccessToken`: the raw token string.
///
/// Wire format:
/// ```text
/// [token_len:1][token:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPersonalAccessTokenResponse {
    pub token: WireName,
}

impl WireEncode for RawPersonalAccessTokenResponse {
    fn encoded_size(&self) -> usize {
        self.token.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.token.encode(buf);
    }
}

impl WireDecode for RawPersonalAccessTokenResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (token, consumed) = WireName::decode(buf)?;
        Ok((Self { token }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let resp = RawPersonalAccessTokenResponse {
            token: WireName::new("raw-secret-token-value").unwrap(),
        };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 1 + 22);
        let (decoded, consumed) = RawPersonalAccessTokenResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = RawPersonalAccessTokenResponse {
            token: WireName::new("tok").unwrap(),
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                RawPersonalAccessTokenResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
