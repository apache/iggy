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
use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::responses::streams::get_stream::TopicHeader;
use bytes::{BufMut, BytesMut};

/// Partition details within a `GetTopic` response.
///
/// Wire format (40 bytes fixed):
/// ```text
/// [id:4][created_at:8][segments_count:4][current_offset:8][size_bytes:8][messages_count:8]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResponse {
    pub id: u32,
    pub created_at: u64,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
}

impl PartitionResponse {
    const FIXED_SIZE: usize = 4 + 8 + 4 + 8 + 8 + 8; // 40
}

impl WireEncode for PartitionResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u32_le(self.segments_count);
        buf.put_u64_le(self.current_offset);
        buf.put_u64_le(self.size_bytes);
        buf.put_u64_le(self.messages_count);
    }
}

impl WireDecode for PartitionResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let created_at = read_u64_le(buf, 4)?;
        let segments_count = read_u32_le(buf, 12)?;
        let current_offset = read_u64_le(buf, 16)?;
        let size_bytes = read_u64_le(buf, 24)?;
        let messages_count = read_u64_le(buf, 32)?;

        Ok((
            Self {
                id,
                created_at,
                segments_count,
                current_offset,
                size_bytes,
                messages_count,
            },
            Self::FIXED_SIZE,
        ))
    }
}

/// `GetTopic` response: topic header followed by partition details.
///
/// Wire format:
/// ```text
/// [TopicHeader][PartitionResponse]*
/// ```
///
/// The number of partitions must match `topic.partitions_count`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetTopicResponse {
    pub topic: TopicHeader,
    pub partitions: Vec<PartitionResponse>,
}

impl WireEncode for GetTopicResponse {
    fn encoded_size(&self) -> usize {
        self.topic.encoded_size()
            + self
                .partitions
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.topic.encode(buf);
        for partition in &self.partitions {
            partition.encode(buf);
        }
    }
}

impl WireDecode for GetTopicResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (topic, mut pos) = TopicHeader::decode(buf)?;
        let mut partitions = Vec::new();
        while pos < buf.len() {
            let (partition, consumed) = PartitionResponse::decode(&buf[pos..])?;
            pos += consumed;
            partitions.push(partition);
        }
        if partitions.len() != topic.partitions_count as usize {
            return Err(WireError::Validation(format!(
                "topic.partitions_count={} but decoded {} partitions",
                topic.partitions_count,
                partitions.len()
            )));
        }
        Ok((Self { topic, partitions }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;

    fn sample_topic(partitions_count: u32) -> TopicHeader {
        TopicHeader {
            id: 1,
            created_at: 1_710_000_000_000,
            partitions_count,
            message_expiry: 0,
            compression_algorithm: 1,
            max_topic_size: 0,
            replication_factor: 1,
            size_bytes: 2048,
            messages_count: 200,
            name: WireName::new("my-topic").unwrap(),
        }
    }

    fn sample_partition(id: u32) -> PartitionResponse {
        PartitionResponse {
            id,
            created_at: 1_710_000_000_000,
            segments_count: 2,
            current_offset: 99,
            size_bytes: 1024,
            messages_count: 100,
        }
    }

    #[test]
    fn partition_roundtrip() {
        let p = sample_partition(1);
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), PartitionResponse::FIXED_SIZE);
        let (decoded, consumed) = PartitionResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, PartitionResponse::FIXED_SIZE);
        assert_eq!(decoded, p);
    }

    #[test]
    fn partition_truncated_returns_error() {
        let p = sample_partition(1);
        let bytes = p.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                PartitionResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn roundtrip_with_partitions() {
        let resp = GetTopicResponse {
            topic: sample_topic(2),
            partitions: vec![sample_partition(1), sample_partition(2)],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetTopicResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn roundtrip_no_partitions() {
        let resp = GetTopicResponse {
            topic: sample_topic(0),
            partitions: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = GetTopicResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn partition_count_mismatch_returns_error() {
        let resp = GetTopicResponse {
            topic: sample_topic(3),
            partitions: vec![sample_partition(1)],
        };
        let bytes = resp.to_bytes();
        assert!(GetTopicResponse::decode(&bytes).is_err());
    }

    #[test]
    fn truncated_returns_error() {
        let resp = GetTopicResponse {
            topic: sample_topic(1),
            partitions: vec![sample_partition(1)],
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                GetTopicResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
