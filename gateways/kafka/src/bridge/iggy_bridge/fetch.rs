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

use std::sync::{Arc, Mutex, PoisonError};

use iggy::prelude::{
    Client, Consumer, IggyClient, IggyError, MessageClient, Partition, PolledMessages,
    PollingStrategy, TopicClient,
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout_at};

use super::offsets::high_watermark;
use super::{IggyBridge, TopicTarget, connect_client, in_slot, with_request_timeout};
use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

/// Fetch reads that poll at once. Each has its own Iggy client, so a poll that Iggy holds up
/// stalls only its own slot.
const FETCH_SLOTS: usize = 4;

/// The Fetch read slots, with a client for each.
pub(super) struct FetchPool {
    permits: Arc<Semaphore>,
    /// The clients of the free slots.
    idle: Mutex<Vec<FetchClient>>,
    /// Every slot's client, for `close`.
    all: Vec<FetchClient>,
}

impl FetchPool {
    pub(super) fn new() -> Self {
        let all: Vec<FetchClient> = (0..FETCH_SLOTS).map(|_| FetchClient::default()).collect();
        Self {
            permits: Arc::new(Semaphore::new(FETCH_SLOTS)),
            idle: Mutex::new(all.clone()),
            all,
        }
    }

    /// A free slot. `None` if none frees by `deadline`.
    async fn take(self: &Arc<Self>, deadline: Instant) -> Option<FetchSlot> {
        let permit = timeout_at(deadline, Arc::clone(&self.permits).acquire_owned())
            .await
            .ok()?
            .ok()?;
        let client = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()?;
        Some(FetchSlot {
            client,
            pool: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Shuts down each client that connected.
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        for client in &self.all {
            if let Some(client) = client.0.get() {
                with_request_timeout(client.shutdown()).await?;
            }
        }
        Ok(())
    }
}

/// The Iggy client of one read slot. It connects at the slot's first poll, so a bridge that never
/// fetches opens no extra connection.
#[derive(Clone, Default)]
struct FetchClient(Arc<OnceCell<IggyClient>>);

impl FetchClient {
    async fn connected(&self, config: &IggyBridgeConfig) -> Result<&IggyClient, BridgeError> {
        self.0.get_or_try_init(|| connect_client(config)).await
    }
}

/// One Fetch read slot. Dropping it frees the slot.
pub struct FetchSlot {
    client: FetchClient,
    pool: Arc<FetchPool>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for FetchSlot {
    fn drop(&mut self) {
        // Before the permit goes, so the next holder finds a client.
        self.pool
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.client.clone());
    }
}

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

impl PartitionProbe {
    /// Iggy counts the messages of a partition it loads before it restores the offset, so the high
    /// watermark reads too low until the load ends.
    ///
    /// A commit between Iggy's reads of the offset and the count looks the same for one probe.
    /// [`IggyBridge::probe`] reads twice to tell them apart.
    #[must_use]
    pub const fn is_loading(&self) -> bool {
        self.messages_count > self.high_watermark
    }
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
    /// A Fetch read slot, with its own client. `None` if none frees by `deadline`.
    pub async fn fetch_slot(&self, deadline: Instant) -> Option<FetchSlot> {
        self.fetch_pool.take(deadline).await
    }

    /// Probes every partition of the topic `kafka_topic` maps to, with one `get_topic` call, or
    /// two when a partition reads as loading.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's rules.
    /// [`BridgeError::Iggy`] if the stream or topic is missing, or the call fails.
    /// [`BridgeError::Timeout`] past `REQUEST_TIMEOUT`.
    pub async fn probe(&self, kafka_topic: &str) -> Result<TopicProbe, BridgeError> {
        let target = self.topic_target(kafka_topic)?;
        let probe = self.probe_target(&target).await?;
        // A load lasts. A commit that raced the read does not.
        if probe
            .partitions
            .iter()
            .any(|(_, partition)| partition.is_loading())
        {
            return self.probe_target(&target).await;
        }
        Ok(probe)
    }

    async fn probe_target(&self, target: &TopicTarget) -> Result<TopicProbe, BridgeError> {
        let details =
            with_request_timeout(self.client.get_topic(&target.stream_id, &target.topic_id))
                .await?
                // A missing stream reads as `None` too. Both answer the same Kafka code.
                .ok_or_else(|| {
                    BridgeError::Iggy(IggyError::TopicNameNotFound(
                        target.topic_id.to_string(),
                        target.stream_id.to_string(),
                    ))
                })?;
        Ok(details
            .partitions
            .iter()
            .map(|partition| (partition.id, PartitionProbe::from(partition)))
            .collect())
    }

    /// Reads up to `count` messages of `partition`, from `offset` on, with the client of `slot`.
    /// Hands the slot back with the result. `None` if no result comes by `deadline`.
    ///
    /// A plain consumer without auto commit, so Iggy stores no offset. Always an explicit
    /// offset, never `Next` (see `docs/OFFSET_STORAGE.md`).
    ///
    /// The errors are those of [`Self::probe`]. A partition the topic lacks is
    /// [`BridgeError::Iggy`] here. The poll stops at `deadline` too, so a result that comes then
    /// is [`BridgeError::Timeout`].
    pub async fn poll(
        self: &Arc<Self>,
        slot: FetchSlot,
        kafka_topic: &str,
        partition: u32,
        offset: u64,
        count: u32,
        deadline: Instant,
    ) -> Option<(Result<PolledMessages, BridgeError>, FetchSlot)> {
        let (bridge, client, topic) = (
            Arc::clone(self),
            slot.client.clone(),
            kafka_topic.to_owned(),
        );
        let poll = async move {
            bridge
                .poll_with(&client, &topic, partition, offset, count, deadline)
                .await
        };
        in_slot(slot, deadline, poll).await
    }

    async fn poll_with(
        &self,
        client: &FetchClient,
        kafka_topic: &str,
        partition: u32,
        offset: u64,
        count: u32,
        deadline: Instant,
    ) -> Result<PolledMessages, BridgeError> {
        let target = self.topic_target(kafka_topic)?;
        let (consumer, strategy) = (Consumer::default(), PollingStrategy::offset(offset));
        let poll = async {
            client
                .connected(&self.config)
                .await?
                .poll_messages(
                    &target.stream_id,
                    &target.topic_id,
                    Some(partition),
                    &consumer,
                    &strategy,
                    count,
                    false,
                )
                .await
                .map_err(BridgeError::Iggy)
        };
        // The SDK replays a poll that Iggy refuses for up to 30 s. Stop with the request, so the
        // slot frees.
        match timeout_at(deadline, poll).await {
            Ok(polled) => polled,
            Err(_elapsed) => Err(BridgeError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iggy::prelude::{IggyByteSize, IggyTimestamp};

    use super::*;

    #[tokio::test]
    async fn given_every_slot_taken_when_one_is_dropped_should_hand_its_client_to_the_next_read() {
        let pool = Arc::new(FetchPool::new());
        let soon = || Instant::now() + Duration::from_millis(20);
        let mut taken = Vec::new();
        for _ in 0..FETCH_SLOTS {
            taken.push(pool.take(soon()).await.expect("a free slot"));
        }
        for (index, slot) in taken.iter().enumerate() {
            for other in &taken[index + 1..] {
                assert!(
                    !Arc::ptr_eq(&slot.client.0, &other.client.0),
                    "each slot has its own client"
                );
            }
        }
        assert!(pool.take(soon()).await.is_none(), "every slot is taken");

        let freed = taken.pop().expect("a slot to free");
        let client = freed.client.clone();
        drop(freed);
        let next = pool.take(soon()).await.expect("the freed slot");
        assert!(
            Arc::ptr_eq(&next.client.0, &client.0),
            "with the freed client"
        );
        drop((next, taken));
    }

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
