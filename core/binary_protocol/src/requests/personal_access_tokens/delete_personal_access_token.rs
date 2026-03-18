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

/// `DeletePersonalAccessToken` request. Wire format: `[name_len:u8][name:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePersonalAccessTokenRequest {
    pub name: WireName,
}

impl WireEncode for DeletePersonalAccessTokenRequest {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.name.encode(buf);
    }
}

impl WireDecode for DeletePersonalAccessTokenRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (name, consumed) = WireName::decode(buf)?;
        Ok((Self { name }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = DeletePersonalAccessTokenRequest {
            name: WireName::new("my-token").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeletePersonalAccessTokenRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = DeletePersonalAccessTokenRequest {
            name: WireName::new("test").unwrap(),
        };
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }

    #[test]
    fn truncated_returns_error() {
        let req = DeletePersonalAccessTokenRequest {
            name: WireName::new("token").unwrap(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                DeletePersonalAccessTokenRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
