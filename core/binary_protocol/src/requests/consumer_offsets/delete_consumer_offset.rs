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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le};
use crate::primitives::consumer::WireConsumer;
use bytes::{BufMut, BytesMut};

/// `DeleteConsumerOffset` request.
///
/// Wire format:
/// ```text
/// [consumer][stream_id][topic_id][partition_flag:1][partition_id:4 LE]
/// ```
///
/// `partition_id` encoding: a u8 flag (1=Some, 0=None) followed by 4 bytes
/// for the u32 value (0 when None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerOffsetRequest {
    pub consumer: WireConsumer,
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: Option<u32>,
}

impl WireEncode for DeleteConsumerOffsetRequest {
    fn encoded_size(&self) -> usize {
        self.consumer.encoded_size()
            + self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + 1
            + 4
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.consumer.encode(buf);
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        if let Some(pid) = self.partition_id {
            buf.put_u8(1);
            buf.put_u32_le(pid);
        } else {
            buf.put_u8(0);
            buf.put_u32_le(0);
        }
    }
}

impl WireDecode for DeleteConsumerOffsetRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let mut pos = 0;
        let (consumer, n) = WireConsumer::decode(&buf[pos..])?;
        pos += n;
        let (stream_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let partition_flag = read_u8(buf, pos)?;
        pos += 1;
        let partition_raw = read_u32_le(buf, pos)?;
        pos += 4;
        let partition_id = if partition_flag == 1 {
            Some(partition_raw)
        } else {
            None
        };
        Ok((
            Self {
                consumer,
                stream_id,
                topic_id,
                partition_id,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_with_partition() {
        let req = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(10),
            topic_id: WireIdentifier::numeric(20),
            partition_id: Some(5),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffsetRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_without_partition() {
        let req = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(3)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: None,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffsetRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::named("my-consumer").unwrap()),
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            partition_id: Some(0),
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffsetRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn partition_none_encodes_zero_bytes() {
        let req = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: None,
        };
        let bytes = req.to_bytes();
        let partition_offset = req.consumer.encoded_size()
            + req.stream_id.encoded_size()
            + req.topic_id.encoded_size();
        assert_eq!(bytes[partition_offset], 0);
        assert_eq!(
            &bytes[partition_offset + 1..partition_offset + 5],
            &[0, 0, 0, 0]
        );
    }

    #[test]
    fn truncated_returns_error() {
        let req = DeleteConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(1),
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                DeleteConsumerOffsetRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
