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
use bytes::BytesMut;

/// `GetPersonalAccessTokens` request. Wire format: empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPersonalAccessTokensRequest;

impl WireEncode for GetPersonalAccessTokensRequest {
    fn encoded_size(&self) -> usize {
        0
    }

    fn encode(&self, _buf: &mut BytesMut) {}
}

impl WireDecode for GetPersonalAccessTokensRequest {
    fn decode(_buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((Self, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = GetPersonalAccessTokensRequest;
        let bytes = req.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetPersonalAccessTokensRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, req);
    }
}
