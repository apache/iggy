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

impl PollMessagesRequest {
    /// Read the strategy and auto-commit flag without allocating identifiers.
    ///
    /// # Errors
    /// Validates the same request bytes as [`WireDecode::decode_from`], including
    /// the fields before the flags. Trailing bytes are accepted by both paths.
    pub fn peek_progress(buf: &[u8]) -> Result<(WirePollingStrategy, bool), WireError> {
        let mut pos = WireConsumer::skip(buf)?;
        pos += WireIdentifier::skip(&buf[pos..])?;
        pos += WireIdentifier::skip(&buf[pos..])?;
        read_u8(buf, pos)?;
        pos += PARTITION_FLAG_SIZE;
        read_u32_le(buf, pos)?;
        pos += PARTITION_VALUE_SIZE;
        let (strategy, consumed) = WirePollingStrategy::decode(&buf[pos..])?;
        pos += consumed;
        read_u32_le(buf, pos)?;
        pos += COUNT_SIZE;
        let auto_commit = read_u8(buf, pos)? != 0;
        Ok((strategy, auto_commit))
    }
}

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

    mod progress_peek_encodings {
        use super::*;

        #[test]
        fn given_numeric_consumer_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::consumer(WireIdentifier::numeric(
                u32::MAX,
            )));
        }

        #[test]
        fn given_named_consumer_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::consumer(
                WireIdentifier::named("named-é").unwrap(),
            ));
        }

        #[test]
        fn given_numeric_consumer_group_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::consumer_group(
                WireIdentifier::numeric(u32::MAX),
            ));
        }

        #[test]
        fn given_named_consumer_group_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::consumer_group(
                WireIdentifier::named("named-é").unwrap(),
            ));
        }

        #[test]
        fn given_numeric_external_group_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::external_group(
                WireIdentifier::numeric(u32::MAX),
            ));
        }

        #[test]
        fn given_named_external_group_when_peeking_progress_should_match_full_decode() {
            assert_poll_encodings_peek_matches(&WireConsumer::external_group(
                WireIdentifier::named("named-é").unwrap(),
            ));
        }

        fn assert_poll_encodings_peek_matches(consumer: &WireConsumer) {
            let identifiers = [
                WireIdentifier::numeric(u32::MAX),
                WireIdentifier::named("named-é").unwrap(),
            ];
            for stream_id in &identifiers {
                for topic_id in &identifiers {
                    for strategy in [
                        WirePollingStrategy::offset(0),
                        WirePollingStrategy::offset(u64::MAX),
                        WirePollingStrategy::timestamp(u64::MAX),
                        WirePollingStrategy::first(),
                        WirePollingStrategy::last(),
                        WirePollingStrategy::next(),
                    ] {
                        for partition_id in [None, Some(u32::MAX)] {
                            for auto_commit in [false, true] {
                                for count in [0, u32::MAX] {
                                    let request = PollMessagesRequest {
                                        consumer: consumer.clone(),
                                        stream_id: stream_id.clone(),
                                        topic_id: topic_id.clone(),
                                        partition_id,
                                        strategy: strategy.clone(),
                                        count,
                                        auto_commit,
                                    };
                                    let mut bytes = request.to_bytes().to_vec();
                                    assert_eq!(
                                        PollMessagesRequest::decode_from(&bytes).unwrap(),
                                        request
                                    );
                                    for end in 0..=bytes.len() {
                                        assert_progress_peek_matches(&bytes[..end]);
                                    }
                                    bytes.extend_from_slice(b"trailing\0\xff");
                                    assert_progress_peek_matches(&bytes);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    mod progress_peek_mutations {
        use super::*;

        #[test]
        fn given_mutated_consumer_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Consumer);
        }

        #[test]
        fn given_mutated_stream_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Stream);
        }

        #[test]
        fn given_mutated_topic_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Topic);
        }

        #[test]
        fn given_mutated_partition_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Partition);
        }

        #[test]
        fn given_mutated_strategy_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Strategy);
        }

        #[test]
        fn given_mutated_count_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::Count);
        }

        #[test]
        fn given_mutated_auto_commit_when_peeking_progress_should_match_full_decode() {
            assert_mutated_poll_field_peek_matches(PollField::AutoCommit);
        }

        #[derive(Clone, Copy)]
        enum PollField {
            Consumer,
            Stream,
            Topic,
            Partition,
            Strategy,
            Count,
            AutoCommit,
        }

        fn assert_mutated_poll_field_peek_matches(field: PollField) {
            for identifier in [
                WireIdentifier::numeric(1),
                WireIdentifier::named("café").unwrap(),
            ] {
                let request = PollMessagesRequest {
                    consumer: WireConsumer::consumer(identifier.clone()),
                    stream_id: identifier.clone(),
                    topic_id: identifier,
                    partition_id: Some(0),
                    strategy: WirePollingStrategy::offset(0),
                    count: 0,
                    auto_commit: false,
                };
                let mut bytes = request.to_bytes().to_vec();
                let consumer_end = request.consumer.encoded_size();
                let stream_end = consumer_end + request.stream_id.encoded_size();
                let topic_end = stream_end + request.topic_id.encoded_size();
                let partition_end = topic_end + PARTITION_FLAG_SIZE + PARTITION_VALUE_SIZE;
                let strategy_end = partition_end + STRATEGY_SIZE;
                let count_end = strategy_end + COUNT_SIZE;
                let offsets = match field {
                    PollField::Consumer => 0..consumer_end,
                    PollField::Stream => consumer_end..stream_end,
                    PollField::Topic => stream_end..topic_end,
                    PollField::Partition => topic_end..partition_end,
                    PollField::Strategy => partition_end..strategy_end,
                    PollField::Count => strategy_end..count_end,
                    PollField::AutoCommit => count_end..bytes.len(),
                };
                for offset in offsets {
                    let original = bytes[offset];
                    for value in u8::MIN..=u8::MAX {
                        bytes[offset] = value;
                        assert_progress_peek_matches(&bytes);
                    }
                    bytes[offset] = original;
                }
            }
        }
    }

    fn assert_progress_peek_matches(bytes: &[u8]) {
        let decoded = PollMessagesRequest::decode_from(bytes)
            .map(|request| (request.strategy, request.auto_commit));
        assert_eq!(
            PollMessagesRequest::peek_progress(bytes),
            decoded,
            "{bytes:?}"
        );
    }

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
