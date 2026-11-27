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

/// `RetireGeoOrigin` request: the geo peer `origin` never returns, so this
/// cluster stops following it. Wire format: `[origin_len:u8][origin:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireGeoOriginRequest {
    pub origin: WireName,
}

impl WireEncode for RetireGeoOriginRequest {
    fn encoded_size(&self) -> usize {
        self.origin.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.origin.encode(buf);
    }
}

impl WireDecode for RetireGeoOriginRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (origin, consumed) = WireName::decode(buf)?;
        Ok((Self { origin }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_request_when_encoded_should_decode_to_the_same_request() {
        let request = RetireGeoOriginRequest {
            origin: WireName::new("dc-a").unwrap(),
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = RetireGeoOriginRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(request.encoded_size(), bytes.len());
    }

    #[test]
    fn given_a_truncated_request_when_decoded_should_fail() {
        let bytes = RetireGeoOriginRequest {
            origin: WireName::new("dc-a").unwrap(),
        }
        .to_bytes();
        for end in 0..bytes.len() {
            assert!(RetireGeoOriginRequest::decode(&bytes[..end]).is_err());
        }
    }
}
