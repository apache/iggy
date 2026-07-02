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
use crate::codec::{WireDecode, WireEncode, capped_capacity, read_u32_le, read_u64_le};
use bytes::{BufMut, BytesMut};

/// `SyncConsumerGroup` response.
///
/// The requesting member's current partition assignment plus the group
/// generation it belongs to. The client caches this and re-syncs when the
/// generation advances (rebalance) or a poll is fenced.
///
/// Wire format: `[generation:8][partitions_count:4][partition_id:4]*`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConsumerGroupResponse {
    pub generation: u64,
    pub partitions: Vec<u32>,
}

impl WireEncode for SyncConsumerGroupResponse {
    fn encoded_size(&self) -> usize {
        8 + 4 + self.partitions.len() * 4
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.generation);
        buf.put_u32_le(self.partitions.len() as u32);
        for &partition_id in &self.partitions {
            buf.put_u32_le(partition_id);
        }
    }
}

impl WireDecode for SyncConsumerGroupResponse {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let generation = read_u64_le(buf, 0)?;
        let partitions_count = read_u32_le(buf, 8)?;
        let remaining = buf.len().saturating_sub(12);
        let mut partitions =
            Vec::with_capacity(capped_capacity(partitions_count as usize, remaining, 4));
        let mut offset = 12;
        for _ in 0..partitions_count {
            partitions.push(read_u32_le(buf, offset)?);
            offset += 4;
        }
        Ok((
            Self {
                generation,
                partitions,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let response = SyncConsumerGroupResponse {
            generation: 7,
            partitions: vec![0, 2, 4],
        };
        let bytes = response.to_bytes();
        let (decoded, consumed) = SyncConsumerGroupResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, response);
    }

    #[test]
    fn roundtrip_empty_assignment() {
        let response = SyncConsumerGroupResponse {
            generation: 1,
            partitions: vec![],
        };
        let bytes = response.to_bytes();
        let (decoded, consumed) = SyncConsumerGroupResponse::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, response);
    }

    #[test]
    fn truncated_returns_error() {
        let response = SyncConsumerGroupResponse {
            generation: 7,
            partitions: vec![1, 2],
        };
        let bytes = response.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                SyncConsumerGroupResponse::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
