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

//! Kafka group offsets, kept as Iggy external group offsets. See `docs/OFFSET_STORAGE.md`.

use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use futures::future::join_all;
use iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient, IggyError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::info;

use super::{IggyBridge, KafkaTopicMetadata, LazyClient, SLOT_LIMIT, TopicTarget};
use crate::bridge::error::BridgeError;

/// Prefix of the Iggy consumer group that holds a Kafka group's offsets. It keeps a Kafka group
/// apart from a native Iggy group of the same name.
pub const OFFSET_GROUP_PREFIX: &str = "kafka.cg.";

/// Offset calls that run at once, each slot on a client of its own, so offset calls never wait
/// behind a Produce send.
const OFFSET_SLOTS: usize = 4;

/// The slot of a call that belongs to no partition. Any slot will do.
const LISTING_SLOT: usize = 0;

impl IggyBridge {
    /// Stores each `(topic, partition, offset)` of `commits` as a committed offset of Kafka group
    /// `group`. One result per commit, in order.
    ///
    /// A negative offset deletes the key instead. Kafka consumers read any negative offset as
    /// none, and Iggy cannot store one. A key or a group that is not there counts as deleted. A
    /// store that finds no Iggy group for the offsets creates it. A replay that Iggy already
    /// applied counts as done.
    ///
    /// Each commit starts once its slot is free. Commits on one slot start in the order given.
    ///
    /// # Errors
    ///
    /// Per commit: [`BridgeError::Iggy`] if Iggy refuses it or `group` names no valid Iggy group,
    /// and [`BridgeError::Timeout`] if it does not end by `deadline`. A commit that has not
    /// started by then never starts.
    pub async fn commit_group_offsets(
        &self,
        group: &str,
        commits: &[(&TopicTarget, u32, i64)],
        deadline: Instant,
    ) -> Vec<Result<(), BridgeError>> {
        let names = match OffsetNames::new(group) {
            Ok(names) => Arc::new(names),
            Err(error) => return refused_all(commits.len(), &error),
        };
        // `join_all` polls each call once, in order, before any again, so calls on one slot
        // queue for it in order.
        join_all(commits.iter().map(|&(topic, partition, offset)| {
            let (names, stream_id, topic_id) = (
                Arc::clone(&names),
                topic.stream_id.clone(),
                topic.topic_id.clone(),
            );
            let commit = move |client: Arc<IggyClient>| async move {
                commit_on(&client, &names, &stream_id, &topic_id, partition, offset).await
            };
            self.commit_in_slot(slot_of(topic, partition), deadline, commit)
        }))
        .await
    }

    /// The committed offset of Kafka group `group` for each `(topic, partition)` of `reads`, in
    /// order. `None` when there is no key or no group. The reads run at once.
    ///
    /// # Errors
    ///
    /// As [`Self::commit_group_offsets`]. A missing topic is [`BridgeError::Iggy`] too.
    pub async fn fetch_group_offsets(
        &self,
        group: &str,
        reads: &[(&TopicTarget, u32)],
        deadline: Instant,
    ) -> Vec<Result<Option<u64>, BridgeError>> {
        let names = match OffsetNames::new(group) {
            Ok(names) => names,
            Err(error) => return refused_all(reads.len(), &error),
        };
        let consumer = &names.consumer;
        join_all(reads.iter().map(|&(topic, partition)| {
            let read = move |client: Arc<IggyClient>| async move {
                let info = client
                    .get_consumer_offset(
                        consumer,
                        &topic.stream_id,
                        &topic.topic_id,
                        Some(partition),
                    )
                    .await
                    .map_err(BridgeError::Iggy)?;
                Ok(info.map(|info| info.stored_offset))
            };
            self.read_in_slot(slot_of(topic, partition), deadline, read)
        }))
        .await
    }

    /// Whether Kafka group `group` holds offsets on `topic`.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Iggy`] if Iggy refuses the call, for example for a missing topic, or `group`
    /// names no valid Iggy group. [`BridgeError::Timeout`] past `deadline`.
    pub async fn holds_offset_group(
        &self,
        group: &str,
        topic: &TopicTarget,
        deadline: Instant,
    ) -> Result<bool, BridgeError> {
        let group_id = OffsetNames::new(group)
            .map_err(BridgeError::Iggy)?
            .consumer
            .id;
        let lookup = |client: Arc<IggyClient>| async move {
            client
                .get_consumer_group(&topic.stream_id, &topic.topic_id, &group_id)
                .await
                .map(|found| found.is_some())
                .map_err(BridgeError::Iggy)
        };
        // Any slot will do. Partition 0's keeps the choice fixed.
        self.read_in_slot(slot_of(topic, 0), deadline, lookup).await
    }

    /// [`Self::list_kafka_topics`] on an offset slot's client, so it never waits behind Produce.
    ///
    /// # Errors
    ///
    /// As [`Self::list_kafka_topics`]. [`BridgeError::Timeout`] past `deadline`.
    pub async fn list_kafka_topics_until(
        &self,
        deadline: Instant,
    ) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        let listing =
            |client: Arc<IggyClient>| async move { self.list_kafka_topics_on(&client).await };
        self.read_in_slot(LISTING_SLOT, deadline, listing).await
    }

    /// Runs `call` with the client of `slot`, and waits for it until `deadline`. The call keeps
    /// the slot until it ends, even after its caller stops waiting, so the next call on the slot
    /// cannot overtake it. A call that has not started by `deadline` never starts.
    async fn commit_in_slot<F>(
        &self,
        slot: usize,
        deadline: Instant,
        call: impl FnOnce(Arc<IggyClient>) -> F,
    ) -> Result<(), BridgeError>
    where
        F: Future<Output = Result<(), BridgeError>> + Send + 'static,
    {
        let Some((turn, client)) = self.offset_pool.take(slot, deadline).await else {
            return Err(BridgeError::Timeout);
        };
        let client = match timeout_at(deadline, client.connected(&self.config)).await {
            Ok(connected) => connected?,
            Err(_elapsed) => return Err(BridgeError::Timeout),
        };
        match timeout_at(deadline, spawn_holding(turn, call(client))).await {
            Ok(Ok(ended)) => ended,
            // A task that ends with no answer leaves the outcome unknown too.
            Ok(Err(_)) | Err(_) => Err(BridgeError::Timeout),
        }
    }

    /// Runs `call` with the client of `slot`, and frees the slot when it ends or at `deadline`.
    /// A read that `deadline` cuts off leaves its client behind, so the next call on the slot
    /// does not wait behind it in the SDK. A call that has not started by `deadline` never starts.
    async fn read_in_slot<T, F>(
        &self,
        slot: usize,
        deadline: Instant,
        call: impl FnOnce(Arc<IggyClient>) -> F,
    ) -> Result<T, BridgeError>
    where
        F: Future<Output = Result<T, BridgeError>>,
    {
        let Some((_turn, client)) = self.offset_pool.take(slot, deadline).await else {
            return Err(BridgeError::Timeout);
        };
        let read = async { call(client.connected(&self.config).await?).await };
        client.until(deadline, read).await
    }
}

/// The offset slots. A partition always takes the same slot. A commit keeps its slot until the
/// SDK lets go of it, so the commits of one partition reach Iggy in the order they started, and a
/// read that comes after them waits for them. A read frees its slot by its deadline.
pub(super) struct OffsetPool {
    slots: Vec<OffsetSlot>,
}

struct OffsetSlot {
    client: LazyClient,
    /// One call at a time.
    turn: Arc<Semaphore>,
}

impl OffsetPool {
    pub(super) fn new() -> Self {
        Self {
            slots: (0..OFFSET_SLOTS)
                .map(|_| OffsetSlot {
                    client: LazyClient::default(),
                    turn: Arc::new(Semaphore::new(1)),
                })
                .collect(),
        }
    }

    /// Slot `slot`, with its client. `None` if it is not free by `deadline`.
    async fn take(
        &self,
        slot: usize,
        deadline: Instant,
    ) -> Option<(OwnedSemaphorePermit, &LazyClient)> {
        let slot = &self.slots[slot];
        match timeout_at(deadline, Arc::clone(&slot.turn).acquire_owned()).await {
            Ok(Ok(turn)) if Instant::now() < deadline => Some((turn, &slot.client)),
            _ => None,
        }
    }

    /// Shuts down each client that connected, and returns the first error. A client that fails to
    /// shut down stops none of the others.
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        let mut closed = Ok(());
        for slot in &self.slots {
            let shutdown = slot.client.close().await;
            closed = closed.and(shutdown);
        }
        closed
    }
}

/// The slot of `partition` of `topic`, the same at every call.
fn slot_of(topic: &TopicTarget, partition: u32) -> usize {
    let mut hasher = DefaultHasher::new();
    (&topic.stream_id, &topic.topic_id, partition).hash(&mut hasher);
    usize::from(hasher.finish().to_le_bytes()[0]) % OFFSET_SLOTS
}

/// Runs `call` in a task that keeps `turn` until `call` ends, even after its caller stops
/// waiting, so the next call on the slot cannot overtake it.
fn spawn_holding<T: Send + 'static>(
    turn: OwnedSemaphorePermit,
    call: impl Future<Output = Result<T, BridgeError>> + Send + 'static,
) -> oneshot::Receiver<Result<T, BridgeError>> {
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let ended = timeout(SLOT_LIMIT, call)
            .await
            .unwrap_or(Err(BridgeError::Timeout));
        // The caller may have stopped waiting.
        let _ = sender.send(ended);
        drop(turn);
    });
    receiver
}

/// One refusal per call, for a group id that names no valid Iggy group.
fn refused_all<T>(count: usize, error: &IggyError) -> Vec<Result<T, BridgeError>> {
    (0..count)
        .map(|_| Err(BridgeError::Iggy(error.clone())))
        .collect()
}

/// The Iggy names of one Kafka group's offsets.
struct OffsetNames {
    /// `kafka.cg.<group>`, the Iggy group that holds the offsets.
    group: String,
    /// The key of each offset: the external group kind of that group.
    consumer: Consumer,
}

impl OffsetNames {
    fn new(group: &str) -> Result<Self, IggyError> {
        let name = format!("{OFFSET_GROUP_PREFIX}{group}");
        let id = Identifier::named(&name)?;
        Ok(Self {
            group: name,
            consumer: Consumer::external_group(id),
        })
    }
}

/// One commit on `client`. A store that finds no Iggy group creates it and stores again.
async fn commit_on(
    client: &IggyClient,
    names: &OffsetNames,
    stream_id: &Identifier,
    topic_id: &Identifier,
    partition: u32,
    offset: i64,
) -> Result<(), BridgeError> {
    let consumer = &names.consumer;
    let Ok(offset) = u64::try_from(offset) else {
        return match client
            .delete_consumer_offset(consumer, stream_id, topic_id, Some(partition))
            .await
        {
            Err(
                IggyError::ConsumerOffsetNotFound(_) | IggyError::ConsumerGroupNameNotFound(..),
            ) => Ok(()),
            deleted => landed(deleted),
        };
    };
    let store =
        || client.store_consumer_offset(consumer, stream_id, topic_id, Some(partition), offset);
    match store().await {
        Err(IggyError::ConsumerGroupNameNotFound(..)) => {
            create_offset_group(client, stream_id, topic_id, &names.group).await?;
            landed(store().await)
        }
        stored => landed(stored),
    }
}

/// A write whose replay Iggy refuses because the first try landed is done.
fn landed(result: Result<(), IggyError>) -> Result<(), BridgeError> {
    match result {
        Ok(()) | Err(IggyError::RequestAlreadyApplied) => Ok(()),
        Err(error) => Err(BridgeError::Iggy(error)),
    }
}

/// Creates the Iggy group `name` on the topic. A group that exists counts as created, since
/// another commit or another gateway can create it first.
async fn create_offset_group(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    name: &str,
) -> Result<(), BridgeError> {
    match client
        .create_consumer_group(stream_id, topic_id, name)
        .await
    {
        Ok(_) => {
            // `{name:?}`: a client picks the group id, and a newline in it would forge a line.
            info!("created Iggy consumer group {name:?} for Kafka group offsets");
            Ok(())
        }
        Err(IggyError::ConsumerGroupNameAlreadyExists(..) | IggyError::RequestAlreadyApplied) => {
            Ok(())
        }
        Err(error) => Err(BridgeError::Iggy(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use iggy::prelude::{ConsumerKind, IggyClientBuilder};
    use secrecy::SecretString;

    use super::*;
    use crate::bridge::iggy_bridge::FetchPool;
    use crate::bridge::{DEFAULT_MAX_MESSAGE_SIZE, IggyBridgeConfig, TopicMapping};

    fn target(topic: &str) -> TopicTarget {
        TopicTarget {
            stream_id: Identifier::named("kafka").unwrap(),
            topic_id: Identifier::named(topic).unwrap(),
        }
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_millis(20)
    }

    /// A client that never dialed and holds no credentials, so each call fails at once.
    fn offline_client() -> Arc<IggyClient> {
        let client = IggyClientBuilder::new()
            .with_tcp()
            .with_server_address("127.0.0.1:1".to_string())
            .build()
            .expect("building a client dials nothing");
        Arc::new(client)
    }

    /// A bridge that never dials. Each offset slot holds the client at its index.
    fn offline_bridge(slot_clients: [Arc<IggyClient>; OFFSET_SLOTS]) -> IggyBridge {
        IggyBridge {
            client: offline_client(),
            config: IggyBridgeConfig {
                address: "127.0.0.1:1".to_string(),
                username: "iggy".to_string(),
                password: SecretString::from("iggy"),
                topic_mapping: TopicMapping::new("kafka".to_string(), HashMap::new()).unwrap(),
                max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            },
            send_slot: Arc::new(Semaphore::new(1)),
            fetch_pool: Arc::new(FetchPool::new()),
            probe_client: LazyClient::default(),
            offset_pool: OffsetPool {
                slots: slot_clients
                    .into_iter()
                    .map(|client| OffsetSlot {
                        client: LazyClient::holding(client),
                        turn: Arc::new(Semaphore::new(1)),
                    })
                    .collect(),
            },
        }
    }

    /// The first partition of `topic` whose slot is not `slot`.
    fn partition_off_slot(topic: &TopicTarget, slot: usize) -> u32 {
        (0..64)
            .find(|&partition| slot_of(topic, partition) != slot)
            .expect("the partitions of one topic spread over the slots")
    }

    #[test]
    fn given_a_kafka_group_the_key_should_be_its_prefixed_external_group() {
        let names = OffsetNames::new("orders").unwrap();

        assert_eq!(names.group, "kafka.cg.orders");
        assert_eq!(names.consumer.kind, ConsumerKind::ExternalGroup);
        assert_eq!(
            names.consumer.id,
            Identifier::named("kafka.cg.orders").unwrap()
        );
    }

    /// 246 bytes of group id plus the prefix is the 255 byte cap of an Iggy name.
    #[test]
    fn given_the_longest_kafka_group_id_the_key_should_still_be_a_valid_name() {
        let longest = "g".repeat(crate::group::MAX_GROUP_ID_BYTES);

        assert!(OffsetNames::new(&longest).is_ok());
        assert!(OffsetNames::new(&format!("{longest}g")).is_err());
    }

    #[test]
    fn given_the_partitions_of_a_topic_should_pin_each_to_one_slot_and_use_several() {
        let orders = target("orders");
        let slots: Vec<usize> = (0..64)
            .map(|partition| slot_of(&orders, partition))
            .collect();

        for (partition, &slot) in (0..64).zip(&slots) {
            assert_eq!(slot_of(&target("orders"), partition), slot);
        }
        assert!(
            slots.iter().any(|&slot| slot != slots[0]),
            "the partitions of one topic must spread over the slots"
        );
    }

    #[tokio::test]
    async fn given_a_taken_slot_when_the_deadline_passes_should_not_start_the_call() {
        let pool = OffsetPool::new();
        let held = pool.take(0, soon()).await.expect("a free slot");

        assert!(pool.take(0, soon()).await.is_none());
        drop(held);
        assert!(pool.take(0, soon()).await.is_some());
    }

    /// A commit given up on can still land, so the next call on its slot must wait for it, or a
    /// newer offset could land first and lose to the older one.
    #[tokio::test]
    async fn given_a_commit_given_up_on_when_the_next_call_comes_should_wait_for_it_to_end() {
        let pool = OffsetPool::new();
        let (release, released) = oneshot::channel::<()>();
        let (turn, _) = pool.take(0, soon()).await.expect("a free slot");
        let stuck = spawn_holding(turn, async move {
            let _ = released.await;
            Ok(1)
        });

        assert!(
            timeout_at(soon(), stuck).await.is_err(),
            "the caller gives up"
        );
        assert!(
            pool.take(0, soon()).await.is_none(),
            "the call still holds its slot"
        );

        release.send(()).expect("the call waits for its release");
        let later = Instant::now() + Duration::from_secs(5);
        assert!(pool.take(0, later).await.is_some());
    }

    /// A busy slot must cost only its own partitions, wherever they sit in the request.
    #[tokio::test]
    async fn given_a_busy_slot_when_committing_should_still_commit_on_the_free_slots() {
        let bridge = offline_bridge(std::array::from_fn(|_| offline_client()));
        let orders = target("orders");
        let busy = slot_of(&orders, 0);
        let free = partition_off_slot(&orders, busy);
        let _held = bridge
            .offset_pool
            .take(busy, soon())
            .await
            .expect("a free slot");
        let deadline = Instant::now() + Duration::from_millis(200);

        let results = bridge
            .commit_group_offsets("g", &[(&orders, 0, 5), (&orders, free, 7)], deadline)
            .await;

        assert!(matches!(results[0], Err(BridgeError::Timeout)));
        assert!(
            matches!(results[1], Err(BridgeError::Iggy(_))),
            "the commit on the free slot reached its client: {:?}",
            results[1]
        );
    }

    /// A Java consumer retries a timed out `OffsetFetch`. A read that kept its slot, or its client,
    /// would make the retry wait behind it.
    #[tokio::test]
    async fn given_a_read_cut_off_by_its_deadline_should_free_its_slot_and_drop_its_client() {
        let clients: [Arc<IggyClient>; OFFSET_SLOTS] = std::array::from_fn(|_| offline_client());
        let bridge = offline_bridge(clients.clone());

        let read = bridge
            .read_in_slot(1, soon(), |_client| {
                std::future::pending::<Result<(), BridgeError>>()
            })
            .await;

        assert!(matches!(read, Err(BridgeError::Timeout)));
        assert!(
            bridge.offset_pool.take(1, soon()).await.is_some(),
            "the slot is free"
        );
        assert_eq!(
            Arc::strong_count(&clients[1]),
            1,
            "the slot let go of its client, so the next call connects a new one"
        );
    }

    #[test]
    fn given_a_replay_that_already_landed_the_write_should_count_as_done() {
        assert!(landed(Ok(())).is_ok());
        assert!(landed(Err(IggyError::RequestAlreadyApplied)).is_ok());
        assert!(matches!(
            landed(Err(IggyError::TooManyConsumerOffsets)),
            Err(BridgeError::Iggy(IggyError::TooManyConsumerOffsets))
        ));
    }
}
