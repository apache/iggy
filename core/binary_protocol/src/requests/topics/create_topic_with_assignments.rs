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
    }
}

impl WireDecode for CreateTopicWithAssignmentsRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing create topic assignment bytes".into(),
            ));
        }
        Ok(request)
    }

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

        let (request, consumed) = CreateTopicRequest::decode(&buf[request_start..request_end])?;
        if consumed != request_size {
            return Err(WireError::Validation(
                "trailing create topic request bytes".into(),
            ));
        }

        let (derived_options, derived_consumed) = decode_options_prefixed(buf, request_end)?;
        let derived_end = request_end + derived_consumed;

        let partitions_count = read_u32_le(buf, derived_end)? as usize;
        if partitions_count != request.partitions_count as usize {
            return Err(WireError::Validation(
                "partition assignment count mismatch".into(),
            ));
        }
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

        Ok((
            Self {
                request,
                derived_options,
                partitions,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::CreateTopicWithAssignmentsRequest;
    use crate::codec::{WireDecode, WireEncode};
    use crate::primitives::identifier::WireName;
    use crate::primitives::options::WireOptions;
    use crate::primitives::partition_assignment::CreatedPartitionAssignment;
    use crate::primitives::user_headers::encode_user_headers;
    use crate::requests::topics::CreateTopicRequest;
    use crate::{WireError, WireIdentifier};
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
                    initial_view: 7,
                    partition_id: 0,
                    consensus_group_id: 1,
                },
                CreatedPartitionAssignment {
                    initial_view: 8,
                    partition_id: 1,
                    consensus_group_id: 2,
                },
            ],
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreateTopicWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(bytes.len(), request.encoded_size());
    }

    #[test]
    fn given_malformed_assignments_when_decoding_should_reject() {
        let mut request = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(1),
                partitions_count: 1,
                name: WireName::new("events").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 7,
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        let bytes = request.to_bytes();
        for truncated in 0..bytes.len() {
            assert!(CreateTopicWithAssignmentsRequest::decode_from(&bytes[..truncated]).is_err());
        }
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(CreateTopicWithAssignmentsRequest::decode_from(&trailing).is_err());

        let request_end = size_of::<u32>() + request.request.encoded_size();
        let mut embedded_trailing = bytes.to_vec();
        embedded_trailing.insert(request_end, 0);
        embedded_trailing[..4].copy_from_slice(
            &u32::try_from(request.request.encoded_size() + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(CreateTopicWithAssignmentsRequest::decode_from(&embedded_trailing).is_err());
        assert!(matches!(
            CreateTopicWithAssignmentsRequest::decode_from(&u32::MAX.to_le_bytes()),
            Err(WireError::UnexpectedEof { .. })
        ));

        for count in [0, 2, u32::MAX] {
            request.request.partitions_count = count;
            assert!(matches!(
                CreateTopicWithAssignmentsRequest::decode_from(&request.to_bytes()),
                Err(WireError::Validation(_))
            ));
        }
        let mut huge_count = request.to_bytes().to_vec();
        let count_offset = huge_count.len() - CreatedPartitionAssignment::ENCODED_SIZE - 4;
        huge_count[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            CreateTopicWithAssignmentsRequest::decode_from(&huge_count),
            Err(WireError::UnexpectedEof { .. })
        ));
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
        };
        let bytes = request.to_bytes();
        let (decoded, consumed) = CreateTopicWithAssignmentsRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
    }
}
