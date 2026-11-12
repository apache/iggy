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

use crate::codec::{WireDecode, WireEncode, read_u8, read_u32_le, read_u64_le};
use crate::primitives::consumer::WireConsumer;
use crate::primitives::polling_strategy::WirePollingStrategy;
use crate::requests::messages::poll_messages::{
    MAX_DEFERRED_POLL_TIMEOUT_US, MAX_DEFERRED_POLL_WAIT_US, MIN_DEFERRED_POLL_MAX_BYTES,
    MIN_DEFERRED_POLL_REPLY_MARGIN_US, STRATEGY_SIZE,
};
use crate::responses::messages::fetch_messages::FetchMessagesResponse;
use crate::{MAX_PARTITIONS_PER_REQUEST, WireError, WireIdentifier};
use bytes::{BufMut, BytesMut};
use std::collections::HashSet;

/// Offsets in the fields between the topic and the targets: the wait, the
/// request timeout, `min_count`, `count`, `max_bytes`, `auto_commit` and the
/// count of targets.
const MAX_BYTES_OFFSET: usize = 2 * size_of::<u64>() + 2 * size_of::<u32>();
const TARGETS_COUNT_OFFSET: usize = MAX_BYTES_OFFSET + size_of::<u32>() + size_of::<u8>();
const LIMITS_SIZE: usize = TARGETS_COUNT_OFFSET + size_of::<u32>();
const TARGET_SIZE: usize = 2 * size_of::<u32>() + STRATEGY_SIZE + 2 * size_of::<u64>();
/// A split `count` would loop or misread the other start points, so the client
/// resolves them with a deferred poll first.
const FETCH_STRATEGY_KINDS: [u8; 2] = [
    WirePollingStrategy::offset(0).kind,
    WirePollingStrategy::next().kind,
];

/// Waits until the targets hold `min_count` messages, then reads them and
/// answers with one section per read target.
///
/// Wire format:
/// ```text
/// [consumer][stream_id][topic_id][wait_us:8 LE][request_timeout_us:8 LE]
/// [min_count:4 LE][count:4 LE][max_bytes:4 LE][auto_commit:1]
/// [targets_count:4 LE] targets_count x
///   [partition_id:4 LE][strategy:9][max_bytes:4 LE][incarnation:8 LE][owner_generation:8 LE]
/// ```
///
/// Partition ids are unique, and there are 1 to `MAX_PARTITIONS_PER_REQUEST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMessagesRequest {
    pub consumer: WireConsumer,
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub wait_us: u64,
    pub request_timeout_us: u64,
    /// The wait ends once the targets hold this many messages, 1 to `count`.
    pub min_count: u32,
    /// Caps the messages of the whole reply.
    pub count: u32,
    /// Caps the records of all sections together.
    pub max_bytes: u32,
    pub auto_commit: bool,
    pub targets: Vec<FetchMessagesTarget>,
}

/// One partition to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMessagesTarget {
    pub partition_id: u32,
    /// An offset, or the next one after the stored offset.
    pub strategy: WirePollingStrategy,
    /// Caps the records of this section, at most the request `max_bytes`.
    pub max_bytes: u32,
    /// With `owner_generation`, the partition fence that a deferred poll
    /// carries in its request header.
    pub incarnation: u64,
    pub owner_generation: u64,
}

impl WireEncode for FetchMessagesRequest {
    fn encoded_size(&self) -> usize {
        self.consumer.encoded_size()
            + self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + LIMITS_SIZE
            + self.targets.len() * TARGET_SIZE
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer.encode(buf);
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u64_le(self.wait_us);
        buf.put_u64_le(self.request_timeout_us);
        buf.put_u32_le(self.min_count);
        buf.put_u32_le(self.count);
        buf.put_u32_le(self.max_bytes);
        buf.put_u8(u8::from(self.auto_commit));
        buf.put_u32_le(self.targets.len() as u32);
        for target in &self.targets {
            buf.put_u32_le(target.partition_id);
            target.strategy.encode(buf);
            buf.put_u32_le(target.max_bytes);
            buf.put_u64_le(target.incarnation);
            buf.put_u64_le(target.owner_generation);
        }
    }
}

impl WireDecode for FetchMessagesRequest {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (consumer, mut pos) = WireConsumer::decode(buf)?;
        let (stream_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let (topic_id, n) = WireIdentifier::decode(&buf[pos..])?;
        pos += n;
        let wait_us = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        let request_timeout_us = read_u64_le(buf, pos)?;
        pos += size_of::<u64>();
        let min_count = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let count = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let max_bytes = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        let auto_commit = read_u8(buf, pos)? != 0;
        pos += size_of::<u8>();
        let targets_count = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        // Bound the allocation before `validate` sees the targets.
        if targets_count > MAX_PARTITIONS_PER_REQUEST
            || buf.len() - pos != targets_count as usize * TARGET_SIZE
        {
            return Err(WireError::Validation("invalid fetch targets".into()));
        }
        let mut targets = Vec::with_capacity(targets_count as usize);
        for _ in 0..targets_count {
            let partition_id = read_u32_le(buf, pos)?;
            pos += size_of::<u32>();
            let (strategy, n) = WirePollingStrategy::decode(&buf[pos..])?;
            pos += n;
            let max_bytes = read_u32_le(buf, pos)?;
            pos += size_of::<u32>();
            let incarnation = read_u64_le(buf, pos)?;
            pos += size_of::<u64>();
            let owner_generation = read_u64_le(buf, pos)?;
            pos += size_of::<u64>();
            targets.push(FetchMessagesTarget {
                partition_id,
                strategy,
                max_bytes,
                incarnation,
                owner_generation,
            });
        }
        let request = Self {
            consumer,
            stream_id,
            topic_id,
            wait_us,
            request_timeout_us,
            min_count,
            count,
            max_bytes,
            auto_commit,
            targets,
        };
        request.validate()?;
        Ok((request, pos))
    }
}

impl FetchMessagesRequest {
    /// The limits that decode enforces, for a sender that builds the request.
    ///
    /// # Errors
    /// Returns `WireError::Validation` for a wait or request timeout out of
    /// range, a request timeout less than `MIN_DEFERRED_POLL_REPLY_MARGIN_US`
    /// after the wait (also for a zero wait), a `min_count` outside 1 to
    /// `count`, a byte limit below `MIN_DEFERRED_POLL_MAX_BYTES` or a target
    /// limit above the request limit, no targets, too many targets, a
    /// duplicate partition, or a start point other than an offset or the next
    /// one.
    pub fn validate(&self) -> Result<(), WireError> {
        if !timers_are_valid(self.wait_us, self.request_timeout_us)
            || self.min_count == 0
            || self.min_count > self.count
            || (self.max_bytes as usize) < MIN_DEFERRED_POLL_MAX_BYTES
            || self.targets.is_empty()
            || self.targets.len() > MAX_PARTITIONS_PER_REQUEST as usize
        {
            return Err(WireError::Validation(
                "invalid fetch wait, request timeout, message counts, byte limit or targets".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(self.targets.len());
        for target in &self.targets {
            if !FETCH_STRATEGY_KINDS.contains(&target.strategy.kind)
                || (target.max_bytes as usize) < MIN_DEFERRED_POLL_MAX_BYTES
                || target.max_bytes > self.max_bytes
            {
                return Err(WireError::Validation(
                    "invalid fetch target start point or byte limit".into(),
                ));
            }
            if !seen.insert(target.partition_id) {
                return Err(WireError::Validation("duplicate partition in fetch".into()));
            }
        }
        Ok(())
    }

    /// The largest reply this request can get: the section count, one table
    /// entry per target and `max_bytes` of records. A client reads no larger
    /// reply.
    #[must_use]
    pub const fn reply_limit(&self) -> usize {
        reply_limit_of(self.targets.len(), self.max_bytes)
    }

    /// `request_timeout_us` read from the fields before the targets, without
    /// allocating identifiers or decoding targets, for a transport that bounds
    /// its reply wait before the request reaches the server.
    ///
    /// `None` for a truncated or malformed prefix, or for timers that
    /// [`Self::validate`] refuses.
    #[must_use]
    pub fn peek_request_timeout_us(body: &[u8]) -> Option<u64> {
        let pos = limits_position(body)?;
        let wait_us = read_u64_le(body, pos).ok()?;
        let request_timeout_us = read_u64_le(body, pos + size_of::<u64>()).ok()?;
        timers_are_valid(wait_us, request_timeout_us).then_some(request_timeout_us)
    }

    /// [`Self::reply_limit`] read from the fields before the targets, without
    /// allocating identifiers or decoding targets, for a client that bounds
    /// the reply to each request it sends.
    ///
    /// `None` for a truncated or malformed prefix, or for a byte limit or a
    /// target count that [`Self::validate`] refuses.
    #[must_use]
    pub fn peek_reply_limit(body: &[u8]) -> Option<usize> {
        let pos = limits_position(body)?;
        let max_bytes = read_u32_le(body, pos + MAX_BYTES_OFFSET).ok()?;
        let targets_count = read_u32_le(body, pos + TARGETS_COUNT_OFFSET).ok()?;
        if (max_bytes as usize) < MIN_DEFERRED_POLL_MAX_BYTES
            || !(1..=MAX_PARTITIONS_PER_REQUEST).contains(&targets_count)
        {
            return None;
        }
        Some(reply_limit_of(targets_count as usize, max_bytes))
    }
}

/// Where the fields between the topic and the targets start, found without
/// allocating the consumer or the identifiers.
fn limits_position(body: &[u8]) -> Option<usize> {
    let mut pos = WireConsumer::skip(body).ok()?;
    pos += WireIdentifier::skip(&body[pos..]).ok()?;
    pos += WireIdentifier::skip(&body[pos..]).ok()?;
    Some(pos)
}

const fn reply_limit_of(targets: usize, max_bytes: u32) -> usize {
    FetchMessagesResponse::head_size(targets).saturating_add(max_bytes as usize)
}

/// Unlike 106, a zero wait also needs the reply margin, because the
/// server ends the partition reads two `FETCH_REPLY_MARGIN_US` before the
/// request timeout.
const fn timers_are_valid(wait_us: u64, request_timeout_us: u64) -> bool {
    wait_us <= MAX_DEFERRED_POLL_WAIT_US
        && request_timeout_us >= wait_us.saturating_add(MIN_DEFERRED_POLL_REPLY_MARGIN_US)
        && request_timeout_us <= MAX_DEFERRED_POLL_TIMEOUT_US
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::cast_possible_truncation)]
    const MIN_BYTES: u32 = MIN_DEFERRED_POLL_MAX_BYTES as u32;
    const MAX_BYTES: u32 = 4 * MIN_BYTES;

    fn request(targets: Vec<FetchMessagesTarget>) -> FetchMessagesRequest {
        FetchMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::named("group").unwrap()),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::named("topic").unwrap(),
            wait_us: 1_000,
            request_timeout_us: 1_000 + MIN_DEFERRED_POLL_REPLY_MARGIN_US,
            min_count: 1,
            count: 100,
            max_bytes: MAX_BYTES,
            auto_commit: true,
            targets,
        }
    }

    fn target(partition_id: u32) -> FetchMessagesTarget {
        FetchMessagesTarget {
            partition_id,
            strategy: WirePollingStrategy::offset(u64::from(partition_id) * 10),
            max_bytes: MIN_BYTES,
            incarnation: u64::from(partition_id) + 1,
            owner_generation: 7,
        }
    }

    fn golden() -> FetchMessagesRequest {
        FetchMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(7)),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::named("orders").unwrap(),
            wait_us: 5_000_000,
            request_timeout_us: 6_000_000,
            min_count: 1,
            count: 100,
            max_bytes: 1_048_576,
            auto_commit: true,
            targets: vec![
                FetchMessagesTarget {
                    partition_id: 1,
                    strategy: WirePollingStrategy::offset(41),
                    max_bytes: 524_288,
                    incarnation: 3,
                    owner_generation: 9,
                },
                FetchMessagesTarget {
                    partition_id: 2,
                    strategy: WirePollingStrategy::next(),
                    max_bytes: 524_288,
                    incarnation: 3,
                    owner_generation: 9,
                },
            ],
        }
    }

    fn accepted(request: &FetchMessagesRequest) -> bool {
        FetchMessagesRequest::decode(&request.to_bytes()).is_ok()
    }

    #[test]
    fn roundtrip_and_reject_truncation() {
        let request = request(vec![
            target(0),
            FetchMessagesTarget {
                strategy: WirePollingStrategy::next(),
                ..target(7)
            },
        ]);
        let bytes = request.to_bytes();
        assert_eq!(bytes.len(), request.encoded_size());
        assert_eq!(
            FetchMessagesRequest::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
        for end in 0..bytes.len() {
            assert!(
                FetchMessagesRequest::decode(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = BytesMut::from(&request(vec![target(1)]).to_bytes()[..]);
        bytes.put_u8(0);
        assert!(FetchMessagesRequest::decode(&bytes).is_err());
    }

    #[test]
    fn target_count_limits() {
        assert!(accepted(&request(
            (0..MAX_PARTITIONS_PER_REQUEST).map(target).collect()
        )));
        for targets in [
            Vec::new(),
            (0..=MAX_PARTITIONS_PER_REQUEST).map(target).collect(),
            vec![target(3), target(4), target(3)],
        ] {
            let count = targets.len();
            assert!(!accepted(&request(targets)), "accepted {count} targets");
        }
    }

    #[test]
    fn only_offset_and_next_targets_are_fetched() {
        for (strategy, valid) in [
            (WirePollingStrategy::offset(0), true),
            (WirePollingStrategy::next(), true),
            (WirePollingStrategy::timestamp(1), false),
            (WirePollingStrategy::first(), false),
            (WirePollingStrategy::last(), false),
        ] {
            let request = request(vec![
                target(1),
                FetchMessagesTarget {
                    strategy: strategy.clone(),
                    ..target(2)
                },
            ]);
            assert_eq!(accepted(&request), valid, "{strategy:?}");
        }
    }

    #[test]
    fn wait_and_timeout_limits() {
        for (wait_us, request_timeout_us, valid) in [
            (0, MIN_DEFERRED_POLL_REPLY_MARGIN_US, true),
            (
                MAX_DEFERRED_POLL_WAIT_US,
                MAX_DEFERRED_POLL_TIMEOUT_US,
                true,
            ),
            (0, MIN_DEFERRED_POLL_REPLY_MARGIN_US - 1, false),
            (0, 0, false),
            (2, 1, false),
            (1, 1 + MIN_DEFERRED_POLL_REPLY_MARGIN_US, true),
            (1, MIN_DEFERRED_POLL_REPLY_MARGIN_US, false),
            (
                MAX_DEFERRED_POLL_WAIT_US + 1,
                MAX_DEFERRED_POLL_TIMEOUT_US,
                false,
            ),
            (1, MAX_DEFERRED_POLL_TIMEOUT_US + 1, false),
        ] {
            let request = FetchMessagesRequest {
                wait_us,
                request_timeout_us,
                ..request(vec![target(1)])
            };
            assert_eq!(
                accepted(&request),
                valid,
                "wait {wait_us}, timeout {request_timeout_us}"
            );
            assert_eq!(
                FetchMessagesRequest::peek_request_timeout_us(&request.to_bytes()).is_some(),
                valid,
                "peek of wait {wait_us}, timeout {request_timeout_us}"
            );
        }
    }

    #[test]
    fn message_count_limits() {
        for (min_count, count, valid) in [
            (1, 1, true),
            (5, 5, true),
            (1, u32::MAX, true),
            (0, 5, false),
            (6, 5, false),
            (0, 0, false),
            (1, 0, false),
        ] {
            let request = FetchMessagesRequest {
                min_count,
                count,
                ..request(vec![target(1)])
            };
            assert_eq!(
                accepted(&request),
                valid,
                "min_count {min_count}, count {count}"
            );
        }
    }

    #[test]
    fn byte_limits() {
        for (max_bytes, target_max_bytes, valid) in [
            (MIN_BYTES, MIN_BYTES, true),
            (MAX_BYTES, MAX_BYTES, true),
            (MIN_BYTES - 1, MIN_BYTES - 1, false),
            (MAX_BYTES, MIN_BYTES - 1, false),
            (MAX_BYTES, MAX_BYTES + 1, false),
        ] {
            let request = FetchMessagesRequest {
                max_bytes,
                ..request(vec![
                    target(1),
                    FetchMessagesTarget {
                        max_bytes: target_max_bytes,
                        ..target(2)
                    },
                ])
            };
            assert_eq!(
                accepted(&request),
                valid,
                "max_bytes {max_bytes}, target max_bytes {target_max_bytes}"
            );
        }
    }

    #[test]
    fn reply_limit_counts_the_table_and_the_records() {
        let request = request(vec![target(1), target(2)]);
        assert_eq!(
            request.reply_limit(),
            4 + 2 * 56 + MAX_BYTES as usize,
            "the reply layout is part of the protocol"
        );
    }

    #[test]
    fn peek_reads_the_request_timeout_from_the_prefix() {
        let request = golden();
        let bytes = request.to_bytes();
        let prefix = request.consumer.encoded_size()
            + request.stream_id.encoded_size()
            + request.topic_id.encoded_size()
            + 2 * size_of::<u64>();
        for end in 0..=bytes.len() {
            let expected = (end >= prefix).then_some(request.request_timeout_us);
            assert_eq!(
                FetchMessagesRequest::peek_request_timeout_us(&bytes[..end]),
                expected,
                "peek of the first {end} bytes"
            );
        }
    }

    #[test]
    fn peek_reads_the_reply_limit_from_the_prefix() {
        let request = golden();
        let bytes = request.to_bytes();
        let prefix = request.consumer.encoded_size()
            + request.stream_id.encoded_size()
            + request.topic_id.encoded_size()
            + LIMITS_SIZE;
        for end in 0..=bytes.len() {
            let expected = (end >= prefix).then_some(request.reply_limit());
            assert_eq!(
                FetchMessagesRequest::peek_reply_limit(&bytes[..end]),
                expected,
                "peek of the first {end} bytes"
            );
        }
    }

    #[test]
    fn peek_refuses_the_reply_limits_that_validate_refuses() {
        for request in [
            request(vec![target(1)]),
            request((0..MAX_PARTITIONS_PER_REQUEST).map(target).collect()),
            request(Vec::new()),
            request((0..=MAX_PARTITIONS_PER_REQUEST).map(target).collect()),
            FetchMessagesRequest {
                max_bytes: MIN_BYTES - 1,
                ..request(vec![target(1)])
            },
        ] {
            assert_eq!(
                FetchMessagesRequest::peek_reply_limit(&request.to_bytes()),
                accepted(&request).then_some(request.reply_limit()),
                "peek of {} targets, max_bytes {}",
                request.targets.len(),
                request.max_bytes
            );
        }
    }

    #[test]
    fn golden_request() {
        let hex = golden()
            .to_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .concat();
        let expected = [
            "02",
            "0104",
            "07000000",
            "0104",
            "01000000",
            "0206",
            "6f7264657273",
            "404b4c0000000000",
            "808d5b0000000000",
            "01000000",
            "64000000",
            "00001000",
            "01",
            "02000000",
            "01000000",
            "01",
            "2900000000000000",
            "00000800",
            "0300000000000000",
            "0900000000000000",
            "02000000",
            "05",
            "0000000000000000",
            "00000800",
            "0300000000000000",
            "0900000000000000",
        ]
        .concat();
        assert_eq!(hex, expected);
    }
}
