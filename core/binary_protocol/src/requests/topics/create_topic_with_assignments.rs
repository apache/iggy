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
use crate::primitives::options::{WireOptions, decode_options_prefixed};
use crate::primitives::partition_assignment::CreatedPartitionAssignment;
use crate::requests::topics::CreateTopicRequest;
use bytes::{BufMut, BytesMut};

fn usize_to_u32(value: usize, context: &str) -> u32 {
    u32::try_from(value).unwrap_or_else(|_| panic!("{context} exceeds u32"))
}

/// Internal create-topic form the admitting primary replicates.
///
/// `request` carries the client's explicit options verbatim; `derived_options`
/// carries the values the primary resolved from server defaults for keys the
/// client did not send. Splitting the two preserves per-key provenance across
/// replication, so `GetTopic` can distinguish client-pinned settings from
/// defaults filled at creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicWithAssignmentsRequest {
    pub request: CreateTopicRequest,
    pub derived_options: WireOptions,
    pub partitions: Vec<CreatedPartitionAssignment>,
    /// View the admitting primary minted this create in. Rides the body, which
    /// `checksum_body` seals, so unlike the header's `view` it is never restamped
    /// by a post-view-change retransmit: every replica decodes the same value.
    pub created_view: u32,
}

impl WireEncode for CreateTopicWithAssignmentsRequest {
    fn encoded_size(&self) -> usize {
        4 + self.request.encoded_size()
            + 4
            + self.derived_options.encoded_size()
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
            "create topic request size",
        ));
        self.request.encode(buf);
        buf.put_u32_le(usize_to_u32(
            self.derived_options.encoded_size(),
            "create topic derived options size",
        ));
        self.derived_options.encode(buf);
        buf.put_u32_le(usize_to_u32(
            self.partitions.len(),
            "create topic partition count",
        ));
        for partition in &self.partitions {
            partition.encode(buf);
        }
        buf.put_u32_le(self.created_view);
    }
}

impl WireDecode for CreateTopicWithAssignmentsRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let request_size = read_u32_le(buf, 0)? as usize;
        let request_start: usize = 4;
        // `checked_add`: on a 32-bit target a wire-supplied length wraps, the
        // truncation guard below then passes, and the slice panics instead.
        // This decode runs on journal replay, so that panic would take a
        // replica down rather than fail one client request.
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

        let request = CreateTopicRequest::decode_from(&buf[request_start..request_end])?;

        let (derived_options, derived_consumed) = decode_options_prefixed(buf, request_end)?;
        let derived_end = request_end + derived_consumed;

        let partitions_count = read_u32_le(buf, derived_end)? as usize;
        let mut offset = derived_end + 4;
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
                derived_options,
                partitions,
                created_view,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CreateTopicWithAssignmentsRequest;
    use crate::WireIdentifier;
    use crate::codec::{WireDecode, WireEncode};
    use crate::primitives::identifier::WireName;
    use crate::primitives::options::WireOptions;
    use crate::primitives::partition_assignment::CreatedPartitionAssignment;
    use crate::primitives::user_headers::encode_user_headers;
    use crate::requests::topics::CreateTopicRequest;
    use bytes::BytesMut;

    fn options_block(entries: &[(u8, &[u8], u8, &[u8])]) -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(entries, &mut buf);
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }

    #[test]
    fn roundtrip() {
        let request = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(1),
                partitions_count: 2,
                name: WireName::new("events").unwrap(),
                options: options_block(&[(2, b"message_expiry", 2, b"7 days")]),
            },
            derived_options: options_block(&[(2, b"max_topic_size", 12, &1024u64.to_le_bytes())]),
            partitions: vec![
                CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id: 1,
                },
                CreatedPartitionAssignment {
                    partition_id: 1,
                    consensus_group_id: 2,
                },
            ],
            created_view: 7,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreateTopicWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }

    #[test]
    fn decode_without_trailing_view_reads_zero() {
        let request = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(1),
                partitions_count: 1,
                name: WireName::new("events").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
            created_view: 7,
        };
        // Bytes journaled before the field existed: same shape, no trailing u32.
        let bytes = request.to_bytes();
        let old_bytes = &bytes[..bytes.len() - 4];
        let (decoded, consumed) = CreateTopicWithAssignmentsRequest::decode(old_bytes).unwrap();
        assert_eq!(consumed, old_bytes.len());
        assert_eq!(decoded.created_view, 0);
        assert_eq!(decoded.partitions, request.partitions);
        assert_eq!(decoded.request, request.request);
    }

    #[test]
    fn roundtrip_with_empty_blocks() {
        let request = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(7),
                partitions_count: 0,
                name: WireName::new("bare").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![],
            created_view: 0,
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreateTopicWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }
}
