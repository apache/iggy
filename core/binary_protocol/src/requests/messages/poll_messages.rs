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
use crate::primitives::polling_strategy::WirePollingStrategy;
use bytes::{BufMut, BytesMut};

/// `PollMessages` request.
///
/// Wire format:
/// ```text
/// [consumer][stream_id][topic_id][partition_flag:1][partition_id:4 LE]
/// [strategy:9][count:4 LE][auto_commit:1]
/// ```
///
/// `partition_id` encoding: a u8 flag (1=Some, 0=None) followed by 4 bytes
/// for the u32 value (0 when None).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollMessagesRequest {
    pub consumer: WireConsumer,
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub partition_id: Option<u32>,
    pub strategy: WirePollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

const PARTITION_FLAG_SIZE: usize = 1;
const PARTITION_VALUE_SIZE: usize = 4;
const STRATEGY_SIZE: usize = 9;
const COUNT_SIZE: usize = 4;
const AUTO_COMMIT_SIZE: usize = 1;

impl WireEncode for PollMessagesRequest {
    fn encoded_size(&self) -> usize {
        self.consumer.encoded_size()
            + self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + PARTITION_FLAG_SIZE
            + PARTITION_VALUE_SIZE
            + STRATEGY_SIZE
            + COUNT_SIZE
            + AUTO_COMMIT_SIZE
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
        self.strategy.encode(buf);
        buf.put_u32_le(self.count);
        buf.put_u8(u8::from(self.auto_commit));
    }
}

impl WireDecode for PollMessagesRequest {
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

        let (strategy, n) = WirePollingStrategy::decode(&buf[pos..])?;
        pos += n;
        let count = read_u32_le(buf, pos)?;
        pos += 4;
        let auto_commit = read_u8(buf, pos)? != 0;
        pos += 1;

        Ok((
            Self {
                consumer,
                stream_id,
                topic_id,
                partition_id,
                strategy,
                count,
                auto_commit,
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
        let req = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(10),
            topic_id: WireIdentifier::numeric(20),
            partition_id: Some(5),
            strategy: WirePollingStrategy::offset(100),
            count: 50,
            auto_commit: true,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = PollMessagesRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_without_partition() {
        let req = PollMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(3)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: None,
            strategy: WirePollingStrategy::first(),
            count: 10,
            auto_commit: false,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = PollMessagesRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn roundtrip_named_identifiers() {
        let req = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::named("my-consumer").unwrap()),
            stream_id: WireIdentifier::named("stream-1").unwrap(),
            topic_id: WireIdentifier::named("topic-1").unwrap(),
            partition_id: Some(0),
            strategy: WirePollingStrategy::offset(0),
            count: 1,
            auto_commit: false,
        };
        let bytes = req.to_bytes();
        let (decoded, consumed) = PollMessagesRequest::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, req);
    }

    #[test]
    fn partition_none_encodes_zero_bytes() {
        let req = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: None,
            strategy: WirePollingStrategy::first(),
            count: 1,
            auto_commit: false,
        };
        let bytes = req.to_bytes();
        // After consumer(7) + stream_id(6) + topic_id(6) = offset 19
        let partition_offset = req.consumer.encoded_size()
            + req.stream_id.encoded_size()
            + req.topic_id.encoded_size();
        assert_eq!(bytes[partition_offset], 0); // flag = 0
        assert_eq!(
            &bytes[partition_offset + 1..partition_offset + 5],
            &[0, 0, 0, 0]
        );
    }

    #[test]
    fn truncated_returns_error() {
        let req = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::numeric(1),
            partition_id: Some(1),
            strategy: WirePollingStrategy::offset(0),
            count: 1,
            auto_commit: false,
        };
        let bytes = req.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                PollMessagesRequest::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
