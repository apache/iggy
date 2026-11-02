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
use crate::primitives::partition_history::PartitionContext;
use crate::responses::streams::get_stream::TopicHeader;
use bytes::{BufMut, BytesMut};

/// Partition details within a `GetTopic` response.
///
/// Wire format, 66 bytes plus one per listed node:
/// ```text
/// [id:4][created_at:8][segments_count:4][current_offset:8][size_bytes:8][messages_count:8][context:24]
/// [members_count:1][members:members_count][in_sync_count:1][in_sync:in_sync_count]
/// ```
/// Each node list is a `u8` count followed by that many `u8` node ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResponse {
    pub id: u32,
    pub created_at: u64,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub context: PartitionContext,
    /// Node ids that store the partition, in member order.
    pub members: Vec<u8>,
    /// Node ids among `members` that the partition's primary last reported
    /// in sync and that are up. Empty until the primary first reports.
    pub in_sync_members: Vec<u8>,
}

impl PartitionResponse {
    const COUNTERS_SIZE: usize = 4 + 8 + 4 + 8 + 8 + 8 + PartitionContext::ENCODED_SIZE;
    /// Smallest encoding: the counters and two empty node lists.
    const FIXED_SIZE: usize = Self::COUNTERS_SIZE + 2;
}
const _: () = assert!(PartitionResponse::COUNTERS_SIZE == 64);

impl WireEncode for PartitionResponse {
    fn encoded_size(&self) -> usize {
        Self::FIXED_SIZE + self.members.len() + self.in_sync_members.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u64_le(self.created_at);
        buf.put_u32_le(self.segments_count);
        buf.put_u64_le(self.current_offset);
        buf.put_u64_le(self.size_bytes);
        buf.put_u64_le(self.messages_count);
        self.context.encode(buf);
        encode_nodes(buf, &self.members);
        encode_nodes(buf, &self.in_sync_members);
    }
}

/// A node list as a `u8` count then the ids. A partition has at most 32
/// members, so the count always fits.
fn encode_nodes(buf: &mut BytesMut, nodes: &[u8]) {
    let listed = &nodes[..nodes.len().min(usize::from(u8::MAX))];
    buf.put_u8(u8::try_from(listed.len()).unwrap_or(u8::MAX));
    buf.put_slice(listed);
}

fn decode_nodes(buf: &[u8], offset: usize) -> Result<(Vec<u8>, usize), WireError> {
    let count = usize::from(read_u8(buf, offset)?);
    let start = offset + 1;
    let nodes = buf
        .get(start..start + count)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset: start,
            need: count,
            have: buf.len().saturating_sub(start),
        })?;
    Ok((nodes.to_vec(), start + count))
}

impl WireDecode for PartitionResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let created_at = read_u64_le(buf, 4)?;
        let segments_count = read_u32_le(buf, 12)?;
        let current_offset = read_u64_le(buf, 16)?;
        let size_bytes = read_u64_le(buf, 24)?;
        let messages_count = read_u64_le(buf, 32)?;
        let (context, _) = PartitionContext::decode(&buf[40..])?;
        let (members, pos) = decode_nodes(buf, Self::COUNTERS_SIZE)?;
        let (in_sync_members, pos) = decode_nodes(buf, pos)?;

        Ok((
            Self {
                id,
                created_at,
                segments_count,
                current_offset,
                size_bytes,
                messages_count,
                context,
                members,
                in_sync_members,
            },
            pos,
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
        // Count-driven so the element stays delimited even when embedded in a
        // larger payload; the header's variable-length options blocks removed
        // the old "everything after the header is partitions" property.
        let mut partitions = Vec::with_capacity(crate::codec::bounded_capacity(
            topic.partitions_count as usize,
            buf.len().saturating_sub(pos),
            PartitionResponse::FIXED_SIZE,
        ));
        for _ in 0..topic.partitions_count {
            let (partition, consumed) = PartitionResponse::decode(&buf[pos..])?;
            pos += consumed;
            partitions.push(partition);
        }
        Ok((Self { topic, partitions }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WireName, WireOptions};
    use std::fmt::Write;

    fn sample_topic(partitions_count: u32) -> TopicHeader {
        TopicHeader {
            id: 1,
            created_at: 1_710_000_000_000,
            partitions_count,
            size_bytes: 2048,
            messages_count: 200,
            name: WireName::new("my-topic").unwrap(),
            options: WireOptions::empty(),
            derived_options: WireOptions::empty(),
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
            context: PartitionContext {
                incarnation: 23,
                owner_generation: 0,
                metadata_op: 101,
            },
            members: vec![1, 3, 4],
            in_sync_members: vec![1, 4],
        }
    }

    #[test]
    fn partition_roundtrip() {
        let p = sample_partition(1);
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), PartitionResponse::FIXED_SIZE + 5);
        assert_eq!(&bytes[64..], &[3, 1, 3, 4, 2, 1, 4]);
        let (decoded, consumed) = PartitionResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, p);
    }

    /// Shared with the SDK decoder tests, so every SDK reads the same bytes.
    #[test]
    fn given_sample_partition_when_encoding_should_match_the_pinned_bytes() {
        let hex = "01000000004cf1238e01000002000000630000000000000000040000000000006400000000000000\
                   17000000000000000000000000000000650000000000000003010304020104";
        let bytes = sample_partition(1).to_bytes();
        let encoded = bytes.iter().fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
        assert_eq!(encoded, hex);
    }

    #[test]
    fn given_empty_node_lists_when_encoding_should_take_one_count_byte_each() {
        let p = PartitionResponse {
            members: Vec::new(),
            in_sync_members: Vec::new(),
            ..sample_partition(1)
        };
        let bytes = p.to_bytes();
        assert_eq!(bytes.len(), PartitionResponse::FIXED_SIZE);
        assert_eq!(PartitionResponse::decode(&bytes).unwrap(), (p, bytes.len()));
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
