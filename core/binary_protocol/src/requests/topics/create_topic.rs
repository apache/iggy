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
use crate::WireIdentifier;
use crate::codec::{WireDecode, WireEncode, read_u32_le};
use crate::primitives::identifier::WireName;
use crate::primitives::options::WireOptions;
use bytes::{BufMut, BytesMut};

/// `CreateTopic` request.
///
/// Wire format:
/// `[stream_id:WireIdentifier][partitions_count:u32_le][name_len:u8][name:N][options TLV to end]`
///
/// The fixed fields are the shape of the operation itself: which stream, how
/// many partitions to allocate, and what to call the topic. `partitions_count`
/// is an argument, not a setting -- admission consumes it to compute the
/// partition assignments and it is deliberately never persisted as an option
/// (`CreatePartitions` would make a stored count stale).
///
/// Every actual topic SETTING -- expiry, size caps, segment sizing, durability
/// and any future knob -- rides the options block, so adding a knob never
/// changes this layout. The block runs to the end of the payload; its
/// validator requires exact consumption, so no length prefix is needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequest {
    pub stream_id: WireIdentifier,
    pub partitions_count: u32,
    pub name: WireName,
    pub options: WireOptions,
}

impl WireEncode for CreateTopicRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size() + 4 + self.name.encoded_size() + self.options.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        buf.put_u32_le(self.partitions_count);
        self.name.encode(buf);
        self.options.encode(buf);
    }
}

impl WireDecode for CreateTopicRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let partitions_count = read_u32_le(buf, pos)?;
        pos += 4;
        let (name, consumed) = WireName::decode(&buf[pos..])?;
        pos += consumed;
        let options = WireOptions::from_slice(&buf[pos..])?;
        Ok((
            Self {
                stream_id,
                partitions_count,
                name,
                options,
            },
            buf.len(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::user_headers::encode_user_headers;

    fn sample_options() -> WireOptions {
        let mut buf = BytesMut::new();
        encode_user_headers(
            &[
                (2, b"message_expiry", 2, b"7 days"),
                (2, b"max_topic_size", 12, &1024u64.to_le_bytes()),
            ],
            &mut buf,
        );
        WireOptions::from_bytes(buf.freeze()).unwrap()
    }

    fn sample_request() -> CreateTopicRequest {
        CreateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            partitions_count: 3,
            name: WireName::new("orders").unwrap(),
            options: sample_options(),
        }
    }

    #[test]
    fn roundtrip() {
        let req = sample_request();
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_without_options() {
        let req = CreateTopicRequest {
            stream_id: WireIdentifier::named("my-stream").unwrap(),
            partitions_count: 1,
            name: WireName::new("events").unwrap(),
            options: WireOptions::empty(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = CreateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_options_return_error() {
        // A single key-value pair: any strict-interior truncation of the
        // block is structurally invalid (a multi-pair block truncates
        // cleanly at pair boundaries).
        let mut buf = BytesMut::new();
        encode_user_headers(&[(2, b"message_expiry", 2, b"7 days")], &mut buf);
        let req = CreateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            partitions_count: 2,
            name: WireName::new("orders").unwrap(),
            options: WireOptions::from_bytes(buf.freeze()).unwrap(),
        };
        let bytes = req.to_bytes();
        let fixed_end = bytes.len() - req.options.encoded_size();
        for i in fixed_end + 1..bytes.len() {
            assert!(
                CreateTopicRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
        let (decoded, _) = CreateTopicRequest::decode(&bytes[..fixed_end]).unwrap();
        assert!(decoded.options.is_empty());
    }

    #[test]
    fn truncated_fixed_fields_return_error() {
        let req = CreateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            partitions_count: 1,
            name: WireName::new("orders").unwrap(),
            options: WireOptions::empty(),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                CreateTopicRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn encoded_size_matches_output() {
        let req = sample_request();
        assert_eq!(req.encoded_size(), req.to_bytes().len());
    }
}
