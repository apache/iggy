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

use crate::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use crate::primitives::consumer::WireConsumer;
use crate::primitives::polling_strategy::WirePollingStrategy;
use crate::requests::messages::poll_messages::{
    MAX_DEFERRED_POLL_TIMEOUT_US, MAX_DEFERRED_POLL_WAIT_US, STRATEGY_SIZE,
    deferred_timeout_covers_wait,
};
use crate::{MAX_PARTITIONS_PER_REQUEST, WireError, WireIdentifier};
use bytes::{BufMut, BytesMut};
use std::collections::HashSet;

const TARGET_SIZE: usize = size_of::<u32>() + STRATEGY_SIZE;

/// Waits until at least one partition holds a message at its start point.
/// Reads and commits nothing.
///
/// Wire format:
/// ```text
/// [consumer][stream_id][topic_id][wait_us:8 LE][request_timeout_us:8 LE]
/// [partitions_count:4 LE] partitions_count x [partition_id:4 LE][strategy:9]
/// ```
///
/// Partition ids are unique, and there are 1 to `MAX_PARTITIONS_PER_REQUEST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitForMessagesRequest {
    pub consumer: WireConsumer,
    pub stream_id: WireIdentifier,
    pub topic_id: WireIdentifier,
    pub wait_us: u64,
    pub request_timeout_us: u64,
    pub partitions: Vec<WaitForMessagesTarget>,
}

/// The server accepts the offset and next strategies only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitForMessagesTarget {
    pub partition_id: u32,
    pub strategy: WirePollingStrategy,
}

impl WireEncode for WaitForMessagesRequest {
    fn encoded_size(&self) -> usize {
        self.consumer.encoded_size()
            + self.stream_id.encoded_size()
            + self.topic_id.encoded_size()
            + 2 * size_of::<u64>()
            + size_of::<u32>()
            + self.partitions.len() * TARGET_SIZE
    }

    #[allow(clippy::cast_possible_truncation)]
    fn encode(&self, buf: &mut BytesMut) {
        self.consumer.encode(buf);
        self.stream_id.encode(buf);
        self.topic_id.encode(buf);
        buf.put_u64_le(self.wait_us);
        buf.put_u64_le(self.request_timeout_us);
        buf.put_u32_le(self.partitions.len() as u32);
        for target in &self.partitions {
            buf.put_u32_le(target.partition_id);
            target.strategy.encode(buf);
        }
    }
}

impl WireDecode for WaitForMessagesRequest {
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
        let count = read_u32_le(buf, pos)?;
        pos += size_of::<u32>();
        // Bound the allocation before `validate` sees the partitions.
        if count > MAX_PARTITIONS_PER_REQUEST || buf.len() - pos != count as usize * TARGET_SIZE {
            return Err(WireError::Validation(
                "invalid readiness wait partitions".into(),
            ));
        }
        let mut partitions = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let partition_id = read_u32_le(buf, pos)?;
            let (strategy, n) = WirePollingStrategy::decode(&buf[pos + size_of::<u32>()..])?;
            pos += size_of::<u32>() + n;
            partitions.push(WaitForMessagesTarget {
                partition_id,
                strategy,
            });
        }
        let request = Self {
            consumer,
            stream_id,
            topic_id,
            wait_us,
            request_timeout_us,
            partitions,
        };
        request.validate()?;
        Ok((request, pos))
    }
}

impl WaitForMessagesRequest {
    /// The limits that decode enforces, for a sender that builds the request.
    ///
    /// # Errors
    /// Returns `WireError::Validation` for a wait or request timeout out of
    /// range, no partitions, too many partitions or a duplicate partition.
    pub fn validate(&self) -> Result<(), WireError> {
        if self.wait_us > MAX_DEFERRED_POLL_WAIT_US
            || self.request_timeout_us == 0
            || !deferred_timeout_covers_wait(self.wait_us, self.request_timeout_us)
            || self.request_timeout_us > MAX_DEFERRED_POLL_TIMEOUT_US
            || self.partitions.is_empty()
            || self.partitions.len() > MAX_PARTITIONS_PER_REQUEST as usize
        {
            return Err(WireError::Validation(
                "invalid readiness wait, request timeout or partitions".into(),
            ));
        }
        let mut seen = HashSet::with_capacity(self.partitions.len());
        if !self
            .partitions
            .iter()
            .all(|target| seen.insert(target.partition_id))
        {
            return Err(WireError::Validation(
                "duplicate partition in readiness wait".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::messages::poll_messages::MIN_DEFERRED_POLL_REPLY_MARGIN_US;

    fn request(partitions: Vec<WaitForMessagesTarget>) -> WaitForMessagesRequest {
        WaitForMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::named("group").unwrap()),
            stream_id: WireIdentifier::numeric(1),
            topic_id: WireIdentifier::named("topic").unwrap(),
            wait_us: 1_000,
            request_timeout_us: 1_000 + MIN_DEFERRED_POLL_REPLY_MARGIN_US,
            partitions,
        }
    }

    fn target(partition_id: u32) -> WaitForMessagesTarget {
        WaitForMessagesTarget {
            partition_id,
            strategy: WirePollingStrategy::offset(u64::from(partition_id) * 10),
        }
    }

    #[test]
    fn roundtrip_and_reject_truncation() {
        let request = request(vec![
            target(0),
            WaitForMessagesTarget {
                partition_id: 7,
                strategy: WirePollingStrategy::next(),
            },
        ]);
        let bytes = request.to_bytes();
        assert_eq!(bytes.len(), request.encoded_size());
        assert_eq!(
            WaitForMessagesRequest::decode(&bytes).unwrap(),
            (request, bytes.len())
        );
        for end in 0..bytes.len() {
            assert!(
                WaitForMessagesRequest::decode(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
    }

    #[test]
    fn partition_count_limits() {
        let full = request(
            (0..MAX_PARTITIONS_PER_REQUEST)
                .map(target)
                .collect::<Vec<_>>(),
        );
        assert!(WaitForMessagesRequest::decode(&full.to_bytes()).is_ok());
        for partitions in [
            Vec::new(),
            (0..=MAX_PARTITIONS_PER_REQUEST).map(target).collect(),
            vec![target(3), target(3)],
        ] {
            let count = partitions.len();
            assert!(
                WaitForMessagesRequest::decode(&request(partitions).to_bytes()).is_err(),
                "accepted {count} partitions"
            );
        }
    }

    #[test]
    fn wait_and_timeout_limits() {
        for (wait_us, request_timeout_us, valid) in [
            (0, 1, true),
            (
                MAX_DEFERRED_POLL_WAIT_US,
                MAX_DEFERRED_POLL_TIMEOUT_US,
                true,
            ),
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
            let mut request = request(vec![target(1)]);
            request.wait_us = wait_us;
            request.request_timeout_us = request_timeout_us;
            assert_eq!(
                WaitForMessagesRequest::decode(&request.to_bytes()).is_ok(),
                valid,
                "wait {wait_us}, timeout {request_timeout_us}"
            );
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = BytesMut::from(&request(vec![target(1)]).to_bytes()[..]);
        bytes.put_u8(0);
        assert!(WaitForMessagesRequest::decode(&bytes).is_err());
    }
}
