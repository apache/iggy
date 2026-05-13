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
use crate::primitives::ack_level::AckLevel;
use crate::primitives::consumer::WireConsumer;
use bytes::{BufMut, BytesMut};

/// `DeleteConsumerOffset` v2 request.
///
/// Adds an `ack` byte: `NoAck` = leader-local fast path, `Quorum` = VSR
/// pipeline.
///
/// Wire format:
/// ```text
/// [consumer][stream_id][topic_id][partition_flag:1][partition_id:4 LE][ack:1]
/// ```
///
/// `partition_id` encoding: a u8 flag (1=Some, 0=None) followed by 4 bytes
/// for the u32 value (0 when None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteConsumerOffset2Request {
    pub consumer: WireConsumer,
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: Option<u32>,
    pub ack: AckLevel,
}

impl WireEncode for DeleteConsumerOffset2Request {
    fn encoded_size(&self) -> usize {
        self.consumer.encoded_size()
            + self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + 1
            + 4
            + 1
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
        buf.put_u8(self.ack.as_u8());
    }
}

impl WireDecode for DeleteConsumerOffset2Request {
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
        let ack_code = read_u8(buf, pos)?;
        pos += 1;
        let ack = AckLevel::from_code(ack_code)?;
        Ok((
            Self {
                consumer,
                stream_id,
                topic_id,
                partition_id,
                ack,
            },
            pos,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_with_partition_quorum() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(10),
            topic_id: WireIdentifier::numeric(20),
            partition_id: Some(5),
            ack: AckLevel::Quorum,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffset2Request::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_without_partition_no_ack() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(3)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: None,
            ack: AckLevel::NoAck,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffset2Request::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::named("my-consumer").unwrap()),
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            partition_id: Some(0),
            ack: AckLevel::Quorum,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = DeleteConsumerOffset2Request::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn ack_byte_is_last() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(0),
            ack: AckLevel::NoAck,
        };
        let bytes = req.to_bytes();
        assert_eq!(*bytes.last().unwrap(), AckLevel::NoAck.as_u8());
    }

    #[test]
    fn unknown_ack_rejected() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(0),
            ack: AckLevel::Quorum,
        };
        let mut bytes = req.to_bytes().to_vec();
        let last = bytes.len() - 1;
        bytes[last] = 0xFF;
        assert!(DeleteConsumerOffset2Request::decode(&bytes).is_err());
    }

    #[test]
    fn truncated_returns_error() {
        let req = DeleteConsumerOffset2Request {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(1),
            ack: AckLevel::Quorum,
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                DeleteConsumerOffset2Request::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
