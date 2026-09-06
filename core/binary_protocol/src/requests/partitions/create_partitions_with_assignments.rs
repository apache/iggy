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
use crate::primitives::partition_assignment::CreatedPartitionAssignment;
use crate::requests::partitions::CreatePartitionsRequest;
use bytes::{BufMut, BytesMut};

fn usize_to_u32(value: usize, context: &str) -> u32 {
    u32::try_from(value).unwrap_or_else(|_| panic!("{context} exceeds u32"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsWithAssignmentsRequest {
    pub request: CreatePartitionsRequest,
    pub partitions: Vec<CreatedPartitionAssignment>,
    /// View the admitting primary minted this create in. Rides the body, which
    /// `checksum_body` seals, so unlike the header's `view` it is never restamped
    /// by a post-view-change retransmit: every replica decodes the same value.
    pub created_view: u32,
}

impl WireEncode for CreatePartitionsWithAssignmentsRequest {
    fn encoded_size(&self) -> usize {
        4 + self.request.encoded_size()
            + 4
            + self
                .partitions
                .iter()
                .map(WireEncode::encoded_size)
                .sum::<usize>()
            + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(usize_to_u32(
            self.request.encoded_size(),
            "create partitions request size",
        ));
        self.request.encode(buf);
        buf.put_u32_le(usize_to_u32(
            self.partitions.len(),
            "create partitions partition count",
        ));
        for partition in &self.partitions {
            partition.encode(buf);
        }
        buf.put_u32_le(self.created_view);
    }
}

impl WireDecode for CreatePartitionsWithAssignmentsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let request_size = read_u32_le(buf, 0)? as usize;
        let request_start = 4;
        let request_end = request_start + request_size;
        if buf.len() < request_end {
            return Err(WireError::UnexpectedEof {
                offset: request_start,
                need: request_size,
                have: buf.len().saturating_sub(request_start),
            });
        }

        let request = CreatePartitionsRequest::decode_from(&buf[request_start..request_end])?;
        let partitions_count = read_u32_le(buf, request_end)? as usize;
        let mut offset = request_end + 4;
        let mut partitions = Vec::with_capacity(partitions_count);
        for _ in 0..partitions_count {
            let (partition, consumed) = CreatedPartitionAssignment::decode(&buf[offset..])?;
            offset += consumed;
            partitions.push(partition);
        }

        // Trailing field: entries journaled before it existed end here and read
        // 0, the view every plane starts in. 1-3 leftover bytes are corruption,
        // not an old shape, so they still error.
        let created_view = if offset < buf.len() {
            let view = read_u32_le(buf, offset)?;
            offset += 4;
            view
        } else {
            0
        };

        Ok((
            Self {
                request,
                partitions,
                created_view,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CreatePartitionsWithAssignmentsRequest;
    use crate::WireIdentifier;
    use crate::codec::{WireDecode, WireEncode};
    use crate::primitives::partition_assignment::CreatedPartitionAssignment;
    use crate::requests::partitions::CreatePartitionsRequest;

    #[test]
    fn roundtrip() {
        let request = CreatePartitionsWithAssignmentsRequest {
            request: CreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
                partitions_count: 2,
            },
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 3,
                    consensus_group_id: 11,
                },
                CreatedPartitionAssignment {
                    partition_id: 4,
                    consensus_group_id: 12,
                },
            ],
            created_view: 9,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreatePartitionsWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }

    #[test]
    fn decode_without_trailing_view_reads_zero() {
        let request = CreatePartitionsWithAssignmentsRequest {
            request: CreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 3,
                consensus_group_id: 11,
            }],
            created_view: 9,
        };
        // Bytes journaled before the field existed: same shape, no trailing u32.
        let bytes = request.to_bytes();
        let old_bytes = &bytes[..bytes.len() - 4];
        let (decoded, consumed) =
            CreatePartitionsWithAssignmentsRequest::decode(old_bytes).unwrap();
        assert_eq!(consumed, old_bytes.len());
        assert_eq!(decoded.created_view, 0);
        assert_eq!(decoded.partitions, request.partitions);
        assert_eq!(decoded.request, request.request);
    }
}
