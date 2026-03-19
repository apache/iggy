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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u64_le};
use crate::primitives::identifier::WireName;
use bytes::{BufMut, BytesMut};

/// `UpdateTopic` request.
///
/// Wire format:
/// `[stream_id:WireIdentifier][topic_id:WireIdentifier][compression_algorithm:u8]
///  [message_expiry:u64_le][max_topic_size:u64_le][replication_factor:u8][name_len:u8][name:N]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTopicRequest {
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub compression_algorithm: u8,
    pub message_expiry: u64,
    pub max_topic_size: u64,
    pub replication_factor: u8,
    pub name: WireName,
}

const FIXED_FIELDS_SIZE: usize = 1 + 8 + 8 + 1; // 18 bytes

impl WireEncode for UpdateTopicRequest {
    fn encoded_size(&self) -> usize {
        self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + FIXED_FIELDS_SIZE
            + self.name.encoded_size()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u8(self.compression_algorithm);
        buf.put_u64_le(self.message_expiry);
        buf.put_u64_le(self.max_topic_size);
        buf.put_u8(self.replication_factor);
        self.name.encode(buf);
    }
}

impl WireDecode for UpdateTopicRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (stream_id, mut pos) = WireIdentifier::decode(buf)?;
        let (topic_id, consumed) = WireIdentifier::decode(&buf[pos..])?;
        pos += consumed;
        let compression_algorithm = read_u8(buf, pos)?;
        pos += 1;
        let message_expiry = read_u64_le(buf, pos)?;
        pos += 8;
        let max_topic_size = read_u64_le(buf, pos)?;
        pos += 8;
        let replication_factor = read_u8(buf, pos)?;
        pos += 1;
        let (name, name_consumed) = WireName::decode(&buf[pos..])?;
        pos += name_consumed;
        Ok((
            Self {
                stream_id,
                topic_id,
                compression_algorithm,
                message_expiry,
                max_topic_size,
                replication_factor,
                name,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> UpdateTopicRequest {
        UpdateTopicRequest {
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(2),
            compression_algorithm: 1,
            message_expiry: 7200,
            max_topic_size: 500_000,
            replication_factor: 2,
            name: WireName::new("updated-topic").unwrap(),
        }
    }

    #[test]
    fn roundtrip() {
        let req = sample_request();
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = UpdateTopicRequest {
            stream_id: WireIdentifier::named("stream-a").unwrap(),
            topic_id: WireIdentifier::named("topic-b").unwrap(),
            compression_algorithm: 0,
            message_expiry: 0,
            max_topic_size: u64::MAX,
            replication_factor: 1,
            name: WireName::new("new-name").unwrap(),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = UpdateTopicRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn truncated_returns_error() {
        let req = sample_request();
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                UpdateTopicRequest::decode(&bytes[..i]).is_err(),
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
