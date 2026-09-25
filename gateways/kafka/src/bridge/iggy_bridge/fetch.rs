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

//! Fetch-side bridge calls.
//!
//! Separate from `produce.rs` so the two never edit one file.

use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{
    Consumer, Identifier, IggyError, MessageClient, Partition, PolledMessages, PollingStrategy,
    TopicClient,
};
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio::time::{Instant, timeout, timeout_at};

use super::offsets::high_watermark;
use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

/// How long a poll may hold its slot: the SDK read deadline (30 s) plus a reconnect (15 s).
const POLL_LIMIT: Duration = Duration::from_secs(45);

/// One partition as a Fetch sees it before it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionProbe {
    /// Same value `high_watermarks` reports.
    pub high_watermark: u64,
    /// Stored bytes per retained message. 0 when the partition retains none.
    pub average_size: u64,
    /// 0 when retention removed every message, or when the server has no stats for the partition.
    pub messages_count: u64,
}

impl From<&Partition> for PartitionProbe {
    fn from(partition: &Partition) -> Self {
        Self {
            high_watermark: high_watermark(partition),
            average_size: partition
                .size
                .as_bytes_u64()
                .checked_div(partition.messages_count)
                .unwrap_or(0),
            messages_count: partition.messages_count,
        }
    }
}

/// Every partition of one topic, by id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicProbe {
    partitions: Vec<(u32, PartitionProbe)>,
}

impl TopicProbe {
    /// The probe of partition `id`, if the topic has it.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<PartitionProbe> {
        self.partitions
            .binary_search_by_key(&id, |&(found, _)| found)
            .ok()
            .map(|found| self.partitions[found].1)
    }

    #[must_use]
    pub fn partitions_count(&self) -> u32 {
        u32::try_from(self.partitions.len()).unwrap_or(u32::MAX)
    }
}

impl FromIterator<(u32, PartitionProbe)> for TopicProbe {
    fn from_iter<I: IntoIterator<Item = (u32, PartitionProbe)>>(partitions: I) -> Self {
        let mut partitions: Vec<_> = partitions.into_iter().collect();
        partitions.sort_unstable_by_key(|&(id, _)| id);
        Self { partitions }
    }
}

impl IggyBridge {
    /// Probes every partition of the topic `kafka_topic` maps to, with one `get_topic` call.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's rules.
    /// [`BridgeError::Iggy`] if the stream or topic is missing, or the call fails.
    /// [`BridgeError::Timeout`] past `REQUEST_TIMEOUT`.
    pub async fn probe(&self, kafka_topic: &str) -> Result<TopicProbe, BridgeError> {
        let (stream_id, topic_id) = self.iggy_ids(kafka_topic)?;
        let details = with_request_timeout(self.client.get_topic(&stream_id, &topic_id))
            .await?
            // A missing stream reads as `None` too. Both answer the same Kafka code.
            .ok_or_else(|| {
                BridgeError::Iggy(IggyError::TopicNameNotFound(
                    topic_id.to_string(),
                    stream_id.to_string(),
                ))
            })?;
        Ok(details
            .partitions
            .iter()
            .map(|partition| (partition.id, PartitionProbe::from(partition)))
            .collect())
    }

    /// Reads up to `count` messages of `partition`, from `offset` on, and waits until `deadline`.
    ///
    /// The poll runs in a task that holds `slot` until the Iggy call ends, then hands it back.
    /// The SDK cannot cancel a call, so a poll given up on keeps its slot: the next poll waits for
    /// the slot, not in the SDK queue.
    ///
    /// A plain consumer without auto commit, so Iggy stores no offset. Always an explicit
    /// offset, never `Next` (see `docs/OFFSET_STORAGE.md`).
    ///
    /// `None` once `deadline` passes. Errors as [`Self::probe`]. A partition the topic lacks is
    /// [`BridgeError::Iggy`] here.
    pub async fn poll(
        self: Arc<Self>,
        slot: OwnedSemaphorePermit,
        kafka_topic: &str,
        partition: u32,
        offset: u64,
        count: u32,
        deadline: Instant,
    ) -> Option<(Result<PolledMessages, BridgeError>, OwnedSemaphorePermit)> {
        let (stream_id, topic_id) = match self.iggy_ids(kafka_topic) {
            Ok(ids) => ids,
            Err(error) => return Some((Err(error), slot)),
        };
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let (consumer, strategy) = (Consumer::default(), PollingStrategy::offset(offset));
            let poll = self.client.poll_messages(
                &stream_id,
                &topic_id,
                Some(partition),
                &consumer,
                &strategy,
                count,
                false,
            );
            let polled = match timeout(POLL_LIMIT, poll).await {
                Ok(polled) => polled.map_err(BridgeError::Iggy),
                Err(_elapsed) => Err(BridgeError::Timeout),
            };
            // The caller may have stopped waiting. The slot then goes back here.
            let _ = sender.send((polled, slot));
        });
        timeout_at(deadline, receiver).await.ok()?.ok()
    }

    /// Checks Kafka's name rules, then names the Iggy stream and topic `kafka_topic` maps to.
    fn iggy_ids(&self, kafka_topic: &str) -> Result<(Identifier, Identifier), BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        Ok((
            Identifier::named(stream_name)?,
            Identifier::named(topic_name)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::{IggyByteSize, IggyTimestamp};

    use super::*;

    fn partition(current_offset: u64, messages_count: u64, size: u64) -> Partition {
        Partition {
            id: 0,
            created_at: IggyTimestamp::zero(),
            segments_count: 1,
            current_offset,
            size: IggyByteSize::from(size),
            messages_count,
        }
    }

    #[test]
    fn given_stored_messages_when_probed_should_give_watermark_and_average_size() {
        let probe = PartitionProbe::from(&partition(9, 10, 1000));
        assert_eq!(probe.high_watermark, 10);
        assert_eq!(probe.average_size, 100);
        assert_eq!(probe.messages_count, 10);
    }

    #[test]
    fn given_an_empty_partition_when_probed_should_give_zero_for_both() {
        let probe = PartitionProbe::from(&partition(0, 0, 0));
        assert_eq!(probe.high_watermark, 0);
        assert_eq!(probe.average_size, 0);
    }

    #[test]
    fn given_every_message_expired_when_probed_should_keep_the_watermark() {
        // Retention lowers the count, not the offset.
        let probe = PartitionProbe::from(&partition(41, 0, 0));
        assert_eq!(probe.high_watermark, 42);
        assert_eq!(probe.average_size, 0, "no retained message to measure");
        assert_eq!(probe.messages_count, 0);
    }

    #[test]
    fn given_partitions_in_any_order_when_looked_up_should_find_each_by_id() {
        let probe = |high_watermark| PartitionProbe {
            high_watermark,
            average_size: 1,
            messages_count: high_watermark,
        };
        let topic: TopicProbe = [(2, probe(20)), (0, probe(0)), (5, probe(50))]
            .into_iter()
            .collect();
        assert_eq!(topic.get(0), Some(probe(0)));
        assert_eq!(topic.get(2), Some(probe(20)));
        assert_eq!(topic.get(5), Some(probe(50)));
        assert_eq!(topic.get(3), None);
        assert_eq!(topic.partitions_count(), 3);
    }
}
