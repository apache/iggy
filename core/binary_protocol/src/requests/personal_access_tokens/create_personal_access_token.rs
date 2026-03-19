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

/// `CreatePersonalAccessToken` request.
///
/// Wire format: `[name_len:u8][name:N][expiry:u64_le]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePersonalAccessTokenRequest {
    pub name: WireName,
    pub expiry: u64,
}

impl WireEncode for CreatePersonalAccessTokenRequest {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size() + 8
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.name.encode(buf);
        buf.put_u64_le(self.expiry);
    }
}

impl WireDecode for CreatePersonalAccessTokenRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (name, mut pos) = WireName::decode(buf)?;
        let expiry = read_u64_le(buf, pos)?;
        pos += 8;
        Ok((Self { name, expiry }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = CreatePersonalAccessTokenRequest {
            name: WireName::new("my-token").unwrap(),
            expiry: 3600,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreatePersonalAccessTokenRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_zero_expiry() {
        let req = CreatePersonalAccessTokenRequest {
            name: WireName::new("permanent").unwrap(),
            expiry: 0,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreatePersonalAccessTokenRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_max_expiry() {
        let req = CreatePersonalAccessTokenRequest {
            name: WireName::new("t").unwrap(),
            expiry: u64::MAX,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreatePersonalAccessTokenRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = CreatePersonalAccessTokenRequest {
            name: WireName::new("test-token").unwrap(),
            expiry: 86400,
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = CreatePersonalAccessTokenRequest {
            name: WireName::new("tok").unwrap(),
            expiry: 100,
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                CreatePersonalAccessTokenRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
