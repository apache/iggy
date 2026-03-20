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

/// `DeleteTopic` request. Wire format: `[stream_id:WireIdentifier][topic_id:WireIdentifier]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
}

impl WireEncode for DeleteTopicRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + self.topic_id.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
    }
}

impl WireDecode for DeleteTopicRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, consumed) = WireIdentifier::decode(&buf[pos..])?;
        pos += consumed;
        Ok((
            Self {
                stream_id,
                topic_id,
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
        let req = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(5),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named() {
        let req = DeleteTopicRequest {
            stream_id: WireIdentifier::named("stream").unwrap(),
            topic_id: WireIdentifier::named("topic").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                DeleteTopicRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
