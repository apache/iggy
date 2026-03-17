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
use crate::identifier::WireName;
use bytes::BytesMut;

/// `UpdateStream` request. Wire format: `[identifier][name_len:1][name:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStreamRequest {
    pub stream_id: WireIdentifier,
    pub name: WireName,
}

impl WireEncode for UpdateStreamRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.name.encode(buf);
    }
}

impl WireDecode for UpdateStreamRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (name, consumed) = WireName::decode(&buf[pos..])?;
        pos += consumed;
        Ok((Self { stream_id, name }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = UpdateStreamRequest {
            stream_id: WireIdentifier::named("old-name").unwrap(),
            name: WireName::new("new-name").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateStreamRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = UpdateStreamRequest {
            stream_id: WireIdentifier::numeric(1),
            name: WireName::new("test").unwrap(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdateStreamRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
