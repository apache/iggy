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

/// `LeaveConsumerGroup` request.
///
/// Wire format: `[stream_id][topic_id][group_id]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveConsumerGroupRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub group_id: WireIdentifier,
}

impl WireEncode for LeaveConsumerGroupRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size() + self.group_id.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        self.group_id.encode(buf);
    }
}

impl WireDecode for LeaveConsumerGroupRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut pos = 0;
        let (stream_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (group_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        Ok((
            Self {
                stream_id,
                topic_id,
                group_id,
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
        let req = LeaveConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            group_id: WireIdentifier::numeric(3),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LeaveConsumerGroupRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named() {
        let req = LeaveConsumerGroupRequest {
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            group_id: WireIdentifier::named("group-1").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = LeaveConsumerGroupRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = LeaveConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            group_id: WireIdentifier::numeric(3),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                LeaveConsumerGroupRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
