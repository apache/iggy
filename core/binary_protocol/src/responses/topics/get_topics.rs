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
use crate::responses::streams::get_stream::TopicHeader;
use bytes::BytesMut;

/// `GetTopics` response: sequential topic headers.
///
/// Wire format:
/// ```text
/// [TopicHeader]*
/// ```
///
/// Empty payload means zero topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicsResponse {
    pub topics: Vec<TopicHeader>,
}

impl WireEncode for GetTopicsResponse {
    fn encoded_size(&self) -> usize {
        self.topics.iter().map(WireEncode::encoded_size).sum()
    }

    fn encode(&self, buf: &mut BytesMut) {
        for topic in &self.topics {
            topic.encode(buf);
        }
    }
}

impl WireDecode for GetTopicsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut topics = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (topic, consumed) = TopicHeader::decode(&buf[pos..])?;
            pos += consumed;
            topics.push(topic);
        }
        Ok((Self { topics }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;

    fn sample_topic(id: u32, name: &str) -> TopicHeader {
        TopicHeader {
            id,
            created_at: 1_710_000_000_000,
            partitions_count: 3,
            message_expiry: 0,
            compression_algorithm: 1,
            max_topic_size: 0,
            replication_factor: 1,
            size_bytes: 1024,
            messages_count: 100,
            name: WireName::new(name).unwrap(),
        }
    }

    #[test]
    fn roundtrip_empty() {
        let resp = GetTopicsResponse { topics: vec![] };
        let bytes = resp.to_bytes();
        assert!(bytes.is_empty());
        let (decoded, consumed) = GetTopicsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_multiple() {
        let resp = GetTopicsResponse {
            topics: vec![sample_topic(1, "events"), sample_topic(2, "logs")],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetTopicsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn truncated_returns_error() {
        let resp = GetTopicsResponse {
            topics: vec![sample_topic(1, "t")],
        };
        let bytes = resp.to_bytes();
        for i in 1..bytes.len() {
            assert!(
                GetTopicsResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
