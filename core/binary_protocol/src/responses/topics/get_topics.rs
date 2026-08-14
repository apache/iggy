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
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::responses::streams::get_stream::TopicHeader;
use bytes::{BufMut, BytesMut};

/// `GetTopics` response: count-prefixed topic headers.
///
/// Wire format:
/// ```text
/// [topics_count:u32_le][TopicHeader]*
/// ```
///
/// The count prefix is what lets a decoder pre-size its `Vec`: an element
/// carries two variable-length option blocks, so the payload length alone says
/// nothing useful about how many elements are in it. Detecting a short read
/// does not need it - the element decoder errors on one, which is how
/// `GetStreams` and `GetUsers` still run their loops to end-of-payload. This
/// response is the odd one out of the three, and every SDK special-cases it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicsResponse {
    pub topics: Vec<TopicHeader>,
}

impl WireEncode for GetTopicsResponse {
    fn encoded_size(&self) -> usize {
        4 + self
            .topics
            .iter()
            .map(WireEncode::encoded_size)
            .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(self.topics.len() as u32);
        for topic in &self.topics {
            topic.encode(buf);
        }
    }
}

/// Smallest a topic header can encode as: the fixed ids, timestamps, sizes,
/// counts and name length, plus the shortest name `WireName` accepts (one byte,
/// since it rejects an empty one) and two empty length-prefixed option blocks.
const MIN_TOPIC_HEADER_SIZE: usize = TopicHeader::FIXED_SIZE + 1 + 4 + 4;

impl WireDecode for GetTopicsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let topics_count = read_u32_le(buf, 0)? as usize;
        let mut pos = 4;
        let mut topics = Vec::with_capacity(crate::codec::bounded_capacity(
            topics_count,
            buf.len().saturating_sub(pos),
            MIN_TOPIC_HEADER_SIZE,
        ));
        for _ in 0..topics_count {
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
    use crate::{WireName, WireOptions};

    fn sample_topic(id: u32, name: &str) -> TopicHeader {
        TopicHeader {
            id,
            created_at: 1_710_000_000_000,
            partitions_count: 3,
            message_expiry: 0,
            compression_algorithm: 1,
            max_topic_size: 0,
            size_bytes: 1024,
            messages_count: 100,
            name: WireName::new(name).unwrap(),
            options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
        }
    }

    #[test]
    fn roundtrip_empty() {
        let resp = GetTopicsResponse { topics: vec![] };
        let bytes = resp.to_bytes();
        assert_eq!(bytes.len(), 4);
        let (decoded, consumed) = GetTopicsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
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
