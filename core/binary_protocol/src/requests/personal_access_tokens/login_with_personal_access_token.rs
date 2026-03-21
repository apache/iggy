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

/// `LoginWithPersonalAccessToken` request. Wire format: `[token_len:u8][token:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginWithPersonalAccessTokenRequest {
    pub token: WireName,
}

impl WireEncode for LoginWithPersonalAccessTokenRequest {
    fn encoded_size(&self) -> usize {
        self.token.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.token.encode(buf);
    }
}

impl WireDecode for LoginWithPersonalAccessTokenRequest {
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
        let req = LoginWithPersonalAccessTokenRequest {
            token: WireName::new("abc123def456").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LoginWithPersonalAccessTokenRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = LoginWithPersonalAccessTokenRequest {
            token: WireName::new("token-value").unwrap(),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = LoginWithPersonalAccessTokenRequest {
            token: WireName::new("tok").unwrap(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LoginWithPersonalAccessTokenRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
