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
    }
}

impl WireDecode for CreatePartitionsWithAssignmentsRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing create partitions assignment bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let request_size = read_u32_le(buf, 0)? as usize;
        let request_start: usize = 4;
        let request_end =
            request_start
                .checked_add(request_size)
                .ok_or_else(|| WireError::UnexpectedEof {
                    offset: request_start,
                    need: request_size,
                    have: buf.len().saturating_sub(request_start),
                })?;
        if buf.len() < request_end {
            return Err(WireError::UnexpectedEof {
                offset: request_start,
                need: request_size,
                have: buf.len().saturating_sub(request_start),
            });
        }

        let (request, consumed) =
            CreatePartitionsRequest::decode(&buf[request_start..request_end])?;
        if consumed != request_size {
            return Err(WireError::Validation(
                "trailing create partitions request bytes".into(),
            ));
        }
        let partitions_count = read_u32_le(buf, request_end)? as usize;
        if partitions_count != request.partitions_count as usize {
            return Err(WireError::Validation(
                "partition assignment count mismatch".into(),
            ));
        }
        let mut offset = request_end + 4;
        let mut partitions = Vec::with_capacity(crate::codec::bounded_capacity(
            partitions_count,
            buf.len().saturating_sub(offset),
            CreatedPartitionAssignment::ENCODED_SIZE,
        ));
        for _ in 0..partitions_count {
            let (partition, consumed) = CreatedPartitionAssignment::decode(&buf[offset..])?;
            offset += consumed;
            partitions.push(partition);
        }

        Ok((
            Self {
                request,
                partitions,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CreatePartitionsWithAssignmentsRequest;
    use crate::codec::{WireDecode, WireEncode};
    use crate::primitives::partition_assignment::CreatedPartitionAssignment;
    use crate::requests::partitions::CreatePartitionsRequest;
    use crate::{WireError, WireIdentifier};

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
                    initial_view: 9,
                    partition_id: 3,
                    consensus_group_id: 11,
                },
                CreatedPartitionAssignment {
                    initial_view: 10,
                    partition_id: 4,
                    consensus_group_id: 12,
                },
            ],
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreatePartitionsWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(bytes.len(), request.encoded_size());
    }

    #[test]
    fn given_malformed_assignments_when_decoding_should_reject() {
        let mut request = CreatePartitionsWithAssignmentsRequest {
            request: CreatePartitionsRequest {
                stream_id: WireIdentifier::numeric(1),
                topic_id: WireIdentifier::numeric(2),
                partitions_count: 1,
            },
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 9,
                partition_id: 3,
                consensus_group_id: 11,
            }],
        };
        let bytes = request.to_bytes();
        for truncated in 0..bytes.len() {
            assert!(
                CreatePartitionsWithAssignmentsRequest::decode_from(&bytes[..truncated]).is_err()
            );
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(CreatePartitionsWithAssignmentsRequest::decode_from(&trailing).is_err());

        let request_end = size_of::<u32>() + request.request.encoded_size();
        let mut embedded_trailing = bytes.to_vec();
        embedded_trailing.insert(request_end, 0);
        embedded_trailing[..4].copy_from_slice(
            &u32::try_from(request.request.encoded_size() + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(CreatePartitionsWithAssignmentsRequest::decode_from(&embedded_trailing).is_err());
        assert!(matches!(
            CreatePartitionsWithAssignmentsRequest::decode_from(&u32::MAX.to_le_bytes()),
            Err(WireError::UnexpectedEof { .. })
        ));

        for count in [0, 2, u32::MAX] {
            request.request.partitions_count = count;
            assert!(matches!(
                CreatePartitionsWithAssignmentsRequest::decode_from(&request.to_bytes()),
                Err(WireError::Validation(_))
            ));
        }
        let mut huge_count = request.to_bytes().to_vec();
        huge_count[request_end..request_end + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            CreatePartitionsWithAssignmentsRequest::decode_from(&huge_count),
            Err(WireError::UnexpectedEof { .. })
        ));
    }
}
