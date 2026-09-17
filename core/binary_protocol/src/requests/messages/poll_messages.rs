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
use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
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

/// Finite protocol ceiling; servers may configure a lower admission limit.
pub const MAX_DEFERRED_POLL_WAIT_US: u64 = 600_000_000;
pub const MAX_DEFERRED_POLL_TIMEOUT_US: u64 = MAX_DEFERRED_POLL_WAIT_US + 30_000_000;

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

/// Readiness-based polling with explicit message and response-byte limits.
/// New command codes prevent older servers from silently ignoring the wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredPollMessagesRequest {
    pub poll: PollMessagesRequest,
    pub wait_us: u64,
    pub min_count: u32,
    pub max_bytes: u32,
    pub request_timeout_us: u64,
}

impl WireEncode for DeferredPollMessagesRequest {
    fn encoded_size(&self) -> usize {
        self.poll.encoded_size() + 2 * size_of::<u64>() + 2 * size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.poll.encode(buf);
        buf.put_u64_le(self.wait_us);
        buf.put_u32_le(self.min_count);
        buf.put_u32_le(self.max_bytes);
        buf.put_u64_le(self.request_timeout_us);
    }
}

impl WireDecode for DeferredPollMessagesRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (poll, consumed) = PollMessagesRequest::decode(buf)?;
        if buf.len() - consumed != 2 * size_of::<u64>() + 2 * size_of::<u32>() {
            return Err(WireError::Validation(
                "deferred poll requires wait, minimum count, byte limit and request timeout".into(),
            ));
        }
        let wait_us = read_u64_le(buf, consumed)?;
        let min_count = read_u32_le(buf, consumed + size_of::<u64>())?;
        let max_bytes = read_u32_le(buf, consumed + size_of::<u64>() + size_of::<u32>())?;
        let request_timeout_us =
            read_u64_le(buf, consumed + size_of::<u64>() + 2 * size_of::<u32>())?;
        if wait_us > MAX_DEFERRED_POLL_WAIT_US
            || min_count == 0
            || min_count > poll.count
            || request_timeout_us == 0
            || request_timeout_us < wait_us
            || request_timeout_us > MAX_DEFERRED_POLL_TIMEOUT_US
            || (max_bytes as usize)
                < crate::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE
        {
            return Err(WireError::Validation(
                "invalid deferred poll wait, message counts or byte limit".into(),
            ));
        }
        Ok((
            Self {
                poll,
                wait_us,
                min_count,
                max_bytes,
                request_timeout_us,
            },
            buf.len(),
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
    #[test]
    fn deferred_preserves_legacy_prefix_and_validates_limits() {
        let request = DeferredPollMessagesRequest {
            poll: PollMessagesRequest {
                consumer: WireConsumer::consumer_group(WireIdentifier::named("group").unwrap()),
                stream_id: WireIdentifier::named("stream").unwrap(),
                topic_id: WireIdentifier::numeric(2),
                partition_id: Some(3),
                strategy: WirePollingStrategy::offset(42),
                count: 5,
                auto_commit: true,
            },
            wait_us: MAX_DEFERRED_POLL_WAIT_US,
            min_count: 1,
            max_bytes: 1024,
            request_timeout_us: MAX_DEFERRED_POLL_TIMEOUT_US,
        };
        for wait_us in [0, 1, MAX_DEFERRED_POLL_WAIT_US] {
            let request = DeferredPollMessagesRequest {
                wait_us,
                ..request.clone()
            };
            let bytes = request.to_bytes();
            let prefix = request.poll.to_bytes();
            assert_eq!(&bytes[..prefix.len()], prefix.as_ref());
            assert_eq!(bytes.len(), prefix.len() + 24);
            assert_eq!(
                &bytes[prefix.len()..prefix.len() + 8],
                &wait_us.to_le_bytes()
            );
            assert_eq!(
                DeferredPollMessagesRequest::decode(&bytes).unwrap(),
                (request, bytes.len())
            );
            for end in 0..bytes.len() {
                assert!(DeferredPollMessagesRequest::decode(&bytes[..end]).is_err());
            }
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(DeferredPollMessagesRequest::decode(&extra).is_err());
        }
        for invalid in [
            DeferredPollMessagesRequest {
                wait_us: MAX_DEFERRED_POLL_WAIT_US + 1,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                min_count: 0,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                min_count: 6,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                max_bytes: 15,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                request_timeout_us: 0,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                request_timeout_us: MAX_DEFERRED_POLL_WAIT_US - 1,
                ..request.clone()
            },
            DeferredPollMessagesRequest {
                request_timeout_us: MAX_DEFERRED_POLL_TIMEOUT_US + 1,
                ..request
            },
        ] {
            assert!(
                DeferredPollMessagesRequest::decode(&invalid.to_bytes()).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
