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
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode};
use bytes::BytesMut;

/// `GetTopics` request. Wire format: `[stream_id:WireIdentifier]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicsRequest {
    pub stream_id: WireIdentifier,
}

impl WireEncode for GetTopicsRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
    }
}

impl WireDecode for GetTopicsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, consumed) = WireIdentifier::decode(buf)?;
        Ok((Self { stream_id }, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_numeric() {
        let req = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(42),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetTopicsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named() {
        let req = GetTopicsRequest {
            stream_id: WireIdentifier::named("my-stream").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = GetTopicsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }
}
