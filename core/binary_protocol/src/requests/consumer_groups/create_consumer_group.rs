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
use crate::primitives::identifier::WireName;
use bytes::BytesMut;

/// `CreateConsumerGroup` request.
///
/// Wire format: `[stream_id][topic_id][name_len:1][name:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub name: WireName,
}

impl WireEncode for CreateConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.name.encode(buf);
    }
}

impl WireDecode for CreateConsumerGroupRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut pos = 0;
        let (stream_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (name, n) = WireName::decode(&buf[pos..])?;
        pos += n;
        Ok((
            Self {
                stream_id,
                topic_id,
                name,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let req = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            name: WireName::new("my-group").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateConsumerGroupRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            name: WireName::new("consumer-group-1").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateConsumerGroupRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            name: WireName::new("grp").unwrap(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                CreateConsumerGroupRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_compat_byte_layout() {
        let req = CreateConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            name: WireName::new("grp").unwrap(),
        };
        let bytes = req.to_bytes();
        // stream_id: [1,4, 1,0,0,0] + topic_id: [1,4, 2,0,0,0] + name: [3, g,r,p]
        assert_eq!(
            &bytes[..],
            &[1, 4, 1, 0, 0, 0, 1, 4, 2, 0, 0, 0, 3, b'g', b'r', b'p']
        );
    }
}
