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
use crate::identifier::WireName;
use crate::responses::streams::StreamResponse;
use bytes::{BufMut, BytesMut};

/// Topic header within a `GetStream` response.
///
/// Wire format (51 + `name_len` bytes):
/// ```text
/// [id:4][created_at:8][partitions_count:4][message_expiry:8]
/// [compression_algorithm:1][max_topic_size:8][replication_factor:1]
/// [size_bytes:8][messages_count:8][name_len:1][name:N]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicHeader {
    pub id: u32,
    pub created_at: u64,
    pub partitions_count: u32,
    pub message_expiry: u64,
    pub compression_algorithm: u8,
    pub max_topic_size: u64,
    pub replication_factor: u8,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub name: WireName,
}

impl TopicHeader {
    const FIXED_SIZE: usize = 4 + 8 + 4 + 8 + 1 + 8 + 1 + 8 + 8 + 1; // 51
}

impl WireEncode for TopicHeader {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.name.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u32_le(self.partitions_count);
        buf.put_u64_le(self.message_expiry);
        buf.put_u8(self.compression_algorithm);
        buf.put_u64_le(self.max_topic_size);
        buf.put_u8(self.replication_factor);
        buf.put_u64_le(self.size_bytes);
        buf.put_u64_le(self.messages_count);
        self.name.encode(buf);
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
        let replication_factor = read_u8(buf, 33)?;
        let size_bytes = read_u64_le(buf, 34)?;
        let messages_count = read_u64_le(buf, 42)?;
        let (name, name_consumed) = WireName::decode(&buf[50..])?;
        let consumed = 50 + name_consumed;

        Ok((
            Self {
                id,
                created_at,
                partitions_count,
                message_expiry,
                compression_algorithm,
                max_topic_size,
                replication_factor,
                size_bytes,
                messages_count,
                name,
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

impl WireDecode for GetStreamResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream, mut pos) = StreamResponse::decode(buf)?;
        let mut topics = Vec::new();
        while pos < buf.len() {
            let (topic, consumed) = TopicHeader::decode(&buf[pos..])?;
            pos += consumed;
            topics.push(topic);
        }
        if topics.len() != stream.topics_count as usize {
            return Err(WireError::Validation(format!(
                "stream.topics_count={} but decoded {} topics",
                stream.topics_count,
                topics.len()
            )));
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
            replication_factor: 1,
            size_bytes: 1024,
            messages_count: 100,
            name: WireName::new(name).unwrap(),
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
        assert_eq!(bytes.len(), TopicHeader::FIXED_SIZE + 6);
        let (decoded, consumed) = TopicHeader::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, topic);
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
