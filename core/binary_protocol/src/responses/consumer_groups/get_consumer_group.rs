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

use super::consumer_group_response::ConsumerGroupResponse;
use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use bytes::{BufMut, BytesMut};

/// A consumer group member with its assigned partitions.
///
/// Wire format:
/// ```text
/// [id:4][partitions_count:4][partition_id:4]*
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMemberResponse {
    pub id: u32,
    pub partitions_count: u32,
    pub partitions: Vec<u32>,
}

impl WireEncode for ConsumerGroupMemberResponse {
    fn encoded_size(&self) -> usize {
        4 + 4 + self.partitions.len() * 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.id);
        buf.put_u32_le(self.partitions_count);
        for &partition_id in &self.partitions {
            buf.put_u32_le(partition_id);
        }
    }
}

impl WireDecode for ConsumerGroupMemberResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let id = read_u32_le(buf, 0)?;
        let partitions_count = read_u32_le(buf, 4)?;
        let mut partitions = Vec::with_capacity(partitions_count as usize);
        let mut offset = 8;
        for _ in 0..partitions_count {
            let partition_id = read_u32_le(buf, offset)?;
            partitions.push(partition_id);
            offset += 4;
        }
        Ok((
            Self {
                id,
                partitions_count,
                partitions,
            },
            offset,
        ))
    }
}

/// `GetConsumerGroup` response: group header followed by member details.
///
/// Wire format:
/// ```text
/// [ConsumerGroupResponse][ConsumerGroupMemberResponse]*
/// ```
///
/// The number of members must match `group.members_count`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupDetailsResponse {
    pub group: ConsumerGroupResponse,
    pub members: Vec<ConsumerGroupMemberResponse>,
}

impl WireEncode for ConsumerGroupDetailsResponse {
    fn encoded_size(&self) -> usize {
        self.group.encoded_size()
            + self
                .members
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.group.encode(buf);
        for member in &self.members {
            member.encode(buf);
        }
    }
}

impl WireDecode for ConsumerGroupDetailsResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (group, mut pos) = ConsumerGroupResponse::decode(buf)?;
        let mut members = Vec::new();
        for _ in 0..group.members_count {
            let (member, consumed) = ConsumerGroupMemberResponse::decode(&buf[pos..])?;
            pos += consumed;
            members.push(member);
        }
        Ok((Self { group, members }, pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireName;

    fn sample_group(members_count: u32) -> ConsumerGroupResponse {
        ConsumerGroupResponse {
            id: 1,
            partitions_count: 6,
            members_count,
            name: WireName::new("my-group").unwrap(),
        }
    }

    fn sample_member(id: u32, partitions: &[u32]) -> ConsumerGroupMemberResponse {
        ConsumerGroupMemberResponse {
            id,
            #[allow(clippy::cast_possible_truncation)]
            partitions_count: partitions.len() as u32,
            partitions: partitions.to_vec(),
        }
    }

    #[test]
    fn member_roundtrip() {
        let m = sample_member(1, &[0, 1, 2]);
        let bytes = m.to_bytes();
        assert_eq!(bytes.len(), 4 + 4 + 3 * 4);
        let (decoded, consumed) = ConsumerGroupMemberResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, m);
    }

    #[test]
    fn member_no_partitions_roundtrip() {
        let m = sample_member(5, &[]);
        let bytes = m.to_bytes();
        assert_eq!(bytes.len(), 8);
        let (decoded, consumed) = ConsumerGroupMemberResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(decoded, m);
    }

    #[test]
    fn member_truncated_returns_error() {
        let m = sample_member(1, &[0, 1]);
        let bytes = m.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ConsumerGroupMemberResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn details_roundtrip_with_members() {
        let resp = ConsumerGroupDetailsResponse {
            group: sample_group(2),
            members: vec![sample_member(1, &[0, 1, 2]), sample_member(2, &[3, 4, 5])],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ConsumerGroupDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn details_roundtrip_no_members() {
        let resp = ConsumerGroupDetailsResponse {
            group: sample_group(0),
            members: vec![],
        };
        let bytes = resp.to_bytes();
        let (decoded, consumed) = ConsumerGroupDetailsResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, resp);
    }

    #[test]
    fn details_truncated_returns_error() {
        let resp = ConsumerGroupDetailsResponse {
            group: sample_group(1),
            members: vec![sample_member(1, &[0])],
        };
        let bytes = resp.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                ConsumerGroupDetailsResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
