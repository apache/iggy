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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
use crate::primitives::identifier::WireName;
use crate::primitives::options::{
    WireOptions, decode_options_prefixed, encode_options_prefixed, options_prefixed_size,
};
use crate::responses::streams::StreamResponse;
use bytes::{BufMut, BytesMut};

/// Topic header within a `GetStream` response.
///
/// Wire format (50 + `name_len` + options bytes):
/// ```text
/// [id:4][created_at:8][partitions_count:4][message_expiry:8]
/// [compression_algorithm:1][max_topic_size:8]
/// [size_bytes:8][messages_count:8][name_len:1][name:N]
/// [explicit_options_len:4][explicit options TLV]
/// [derived_options_len:4][derived options TLV]
/// ```
///
/// `options` are the keys the client pinned at create; `derived_options` are
/// the defaults the admitting primary resolved. A round-tripping client
/// re-sends only `options`; the effective configuration is the union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicHeader {
    pub id: u32,
    pub created_at: u64,
    pub partitions_count: u32,
    pub message_expiry: u64,
    pub compression_algorithm: u8,
    pub max_topic_size: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub name: WireName,
    pub options: WireOptions,
    pub derived_options: WireOptions,
}

impl TopicHeader {
    pub(crate) const FIXED_SIZE: usize = 4 + 8 + 4 + 8 + 1 + 8 + 8 + 8 + 1; // 50
}

impl WireEncode for TopicHeader {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE
            + self.name.len()
            + options_prefixed_size(&self.options)
            + options_prefixed_size(&self.derived_options)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u32_le(self.partitions_count);
        buf.put_u64_le(self.message_expiry);
        buf.put_u8(self.compression_algorithm);
        buf.put_u64_le(self.max_topic_size);
        buf.put_u64_le(self.size_bytes);
        buf.put_u64_le(self.messages_count);
        self.name.encode(buf);
        encode_options_prefixed(&self.options, buf);
        encode_options_prefixed(&self.derived_options, buf);
    }
}

impl WireDecode for TopicHeader {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let created_at = read_u64_le(buf, 4)?;
        let partitions_count = read_u32_le(buf, 12)?;
        let message_expiry = read_u64_le(buf, 16)?;
        let compression_algorithm = read_u8(buf, 24)?;
        let max_topic_size = read_u64_le(buf, 25)?;
        let size_bytes = read_u64_le(buf, 33)?;
        let messages_count = read_u64_le(buf, 41)?;
        let (name, name_consumed) = WireName::decode(&buf[49..])?;
        let mut consumed = 49 + name_consumed;
        let (options, options_consumed) = decode_options_prefixed(buf, consumed)?;
        consumed += options_consumed;
        let (derived_options, derived_consumed) = decode_options_prefixed(buf, consumed)?;
        consumed += derived_consumed;

        Ok((
            Self {
                id,
                created_at,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                size_bytes,
                messages_count,
                name,
                options,
                derived_options,
            },
            consumed,
        ))
    }
}

/// `GetStream` response: stream header followed by topic headers.
///
/// Wire format:
/// ```text
/// [StreamResponse][TopicHeader]*
/// ```
///
/// The number of topics is determined by `stream.topics_count` or by
/// consuming remaining bytes (topics are packed sequentially).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStreamResponse {
    pub stream: StreamResponse,
    pub topics: Vec<TopicHeader>,
}

impl WireEncode for GetStreamResponse {
    fn encoded_size(&self) -> usize {
        self.stream.encoded_size()
            + self
                .topics
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream.encode(buf);
        for topic in &self.topics {
            topic.encode(buf);
        }
    }
}

/// Smallest a topic header can encode as: the fixed ids, timestamps, sizes,
/// counts and name length, plus the shortest name `WireName` accepts (one byte,
/// since it rejects an empty one) and two empty length-prefixed option blocks.
const MIN_TOPIC_HEADER_SIZE: usize = TopicHeader::FIXED_SIZE + 1 + 4 + 4;

impl WireDecode for GetStreamResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream, mut pos) = StreamResponse::decode(buf)?;
        // Count-driven: a topic element carries variable-length options
        // blocks, so "consume until the buffer ends" no longer delimits it.
        let mut topics = Vec::with_capacity(crate::codec::bounded_capacity(
            stream.topics_count as usize,
            buf.len().saturating_sub(pos),
            MIN_TOPIC_HEADER_SIZE,
        ));
        for _ in 0..stream.topics_count {
            let (topic, consumed) = TopicHeader::decode(&buf[pos..])?;
            pos += consumed;
            topics.push(topic);
        }
        Ok((Self { stream, topics }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stream() -> StreamResponse {
        StreamResponse {
            id: 1,
            created_at: 1_710_000_000_000,
            topics_count: 2,
            size_bytes: 2048,
            messages_count: 200,
            name: WireName::new("my-stream").unwrap(),
            options: WireOptions::empty(),
        }
    }

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
    fn roundtrip_with_topics() {
        let resp = GetStreamResponse {
            stream: sample_stream(),
            topics: vec![sample_topic(1, "topic-a"), sample_topic(2, "topic-b")],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetStreamResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_no_topics() {
        let resp = GetStreamResponse {
            stream: StreamResponse {
                topics_count: 0,
                ..sample_stream()
            },
            topics: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetStreamResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn topic_header_roundtrip() {
        let topic = sample_topic(5, "events");
        let bytes = topic.to_bytes();
        assert_eq!(bytes.len(), TopicHeader::FIXED_SIZE + 6 + 4 + 4);
        let (decoded, consumed) = TopicHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, topic);
    }

    #[test]
    fn min_topic_header_size_matches_the_shortest_encoding() {
        let bytes = sample_topic(1, "t").to_bytes();
        assert_eq!(bytes.len(), MIN_TOPIC_HEADER_SIZE);
    }

    #[test]
    fn topic_header_truncated_returns_error() {
        let topic = sample_topic(1, "t");
        let bytes = topic.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                TopicHeader::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
