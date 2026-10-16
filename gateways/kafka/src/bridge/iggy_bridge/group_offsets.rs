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

use std::collections::BTreeSet;
use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyClient, IggyError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{debug, error, info};

use super::{
    IggyBridge, KafkaTopicMetadata, LazyClient, OffsetGroups, SLOT_LIMIT, TopicTarget, in_slot,
    permit_by,
};
use crate::bridge::error::BridgeError;
use crate::group::is_valid_group_id;
use crate::protocol::api::ERROR_UNKNOWN_TOPIC_OR_PARTITION;

/// Prefix of the Iggy consumer group that holds a Kafka group's offsets. It keeps a Kafka group
/// apart from a native Iggy group of the same name.
pub const OFFSET_GROUP_PREFIX: &str = "kafka.cg.";

/// How long one scan for the groups that hold offsets may take. It keeps running after the request
/// that started it stops waiting, on a client of its own, so no offset call waits behind it.
const GROUP_SCAN_LIMIT: Duration = Duration::from_secs(30);

/// The first pause, and the longest, between two checks of whether Iggy finished deleting a group.
const DELETE_CHECK_FIRST: Duration = Duration::from_millis(5);
const DELETE_CHECK_MAX: Duration = Duration::from_millis(100);

/// Offset calls that run at once, each slot on clients of its own, so offset calls never wait
/// behind a Produce send.
const OFFSET_SLOTS: usize = 4;

/// The slot of a call that belongs to no partition. Any slot will do.
const LISTING_SLOT: usize = 0;

/// The offset calls of one Kafka request. A request queues at most one call per slot, so a
/// request with many partitions cannot hold up the others.
pub struct OffsetCalls {
    deadline: Instant,
    /// The request's place in the queue of each slot.
    queued: [Arc<Semaphore>; OFFSET_SLOTS],
}

impl OffsetCalls {
    /// Calls that give up at `deadline`. A call that has not started by then never starts.
    #[must_use]
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            queued: std::array::from_fn(|_| Arc::new(Semaphore::new(1))),
        }
    }

    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The turn of `lane`, a lane of `slot`, once no other call of this request waits there.
    /// Keep both permits until the call ends. `None` if a turn does not come by the deadline.
    async fn take(
        &self,
        slot: usize,
        lane: &Lane,
    ) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let queued = permit_by(&self.queued[slot], self.deadline).await?;
        let turn = permit_by(&lane.turn, self.deadline).await?;
        Some((queued, turn))
    }
}

impl IggyBridge {
    /// Stores each `(topic, partition, offset)` of `commits` as a committed offset of Kafka group
    /// `group`. One result per commit, in order.
    ///
    /// A negative offset deletes the key instead. Kafka consumers read any negative offset as
    /// none, and Iggy cannot store one. A key or a group that is not there counts as deleted. A
    /// store that finds no Iggy group for the offsets creates it. A replay that Iggy already
    /// applied counts as done.
    ///
    /// Commits on one slot start one at a time, in the order given. A stored offset adds `group`
    /// to [`Self::offset_groups`] at once.
    ///
    /// # Errors
    ///
    /// Per commit: [`BridgeError::Iggy`] if Iggy refuses it or `group` names no valid Iggy group,
    /// and [`BridgeError::Timeout`] if it does not end by the deadline of `calls`. A commit that
    /// has not started by then never starts.
    pub async fn commit_group_offsets(
        &self,
        group: &str,
        commits: &[(&TopicTarget, u32, i64)],
        calls: &OffsetCalls,
    ) -> Vec<Result<(), BridgeError>> {
        let names = match OffsetNames::new(group) {
            Ok(names) => Arc::new(names),
            Err(error) => return refused_all(commits.len(), &error),
        };
        // `join_all` polls each call once, in order, before any again, so calls on one slot
        // queue for it in order.
        let results = join_all(commits.iter().map(|&(topic, partition, offset)| {
            let (names, stream_id, topic_id) = (
                Arc::clone(&names),
                topic.stream_id.clone(),
                topic.topic_id.clone(),
            );
            let commit = move |client: Arc<IggyClient>| async move {
                commit_on(&client, &names, &stream_id, &topic_id, partition, offset).await
            };
            self.commit_in_slot(slot_of(topic, partition), calls, commit)
        }))
        .await;
        let stored = commits
            .iter()
            .zip(&results)
            .any(|(&(_, _, offset), result)| offset >= 0 && result.is_ok());
        if stored {
            self.group_index.note(group);
        }
        results
    }

    /// The committed offset of Kafka group `group` for each `(topic, partition)` of `reads`, in
    /// order. `None` when there is no key or no group. Reads on different slots run at once.
    ///
    /// # Errors
    ///
    /// As [`Self::commit_group_offsets`]. A missing topic is [`BridgeError::Iggy`] too.
    pub async fn fetch_group_offsets(
        &self,
        group: &str,
        reads: &[(&TopicTarget, u32)],
        calls: &OffsetCalls,
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
            self.read_in_slot(slot_of(topic, partition), calls, read)
        }))
        .await
    }

    /// Whether Kafka group `group` holds offsets on `topic`.
    ///
    /// # Errors
    ///
    /// [`BridgeError::Iggy`] if Iggy refuses the call, for example for a missing topic, or `group`
    /// names no valid Iggy group. [`BridgeError::Timeout`] past the deadline of `calls`.
    pub async fn holds_offset_group(
        &self,
        group: &str,
        topic: &TopicTarget,
        calls: &OffsetCalls,
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
        self.read_in_slot(slot_of(topic, 0), calls, lookup).await
    }

    /// Deletes the Iggy group that holds the offsets of each Kafka group of `groups`, on every
    /// Kafka topic. Iggy deletes the offsets with it. One result per group, in order: whether any
    /// topic held the group.
    ///
    /// Lists the topics, then the Iggy groups of each topic, so it costs one call per topic, plus
    /// at least two per deletion. Iggy deletes a group on each partition of the topic, and lists
    /// it until every partition did, so a deletion waits for that. A topic deleted since the
    /// listing holds none. A group whose deletions all ended leaves [`Self::offset_groups`] at
    /// once.
    ///
    /// # Errors
    ///
    /// The outer error holds for every group, and nothing was deleted: the listing failed, or
    /// [`IggyError::Unauthorized`] when the bridge user may not list the groups of a Kafka topic,
    /// or may not read an override topic, which the listing then leaves out. A group can hold
    /// offsets there. Per group, as [`Self::commit_group_offsets`], and [`BridgeError::Timeout`]
    /// also when Iggy has not finished a deletion by the deadline of `calls`.
    pub async fn delete_offset_groups(
        &self,
        groups: &[&str],
        calls: &OffsetCalls,
    ) -> Result<Vec<Result<bool, BridgeError>>, BridgeError> {
        let listing =
            |client: Arc<IggyClient>| async move { self.kafka_topic_listing_on(&client).await };
        let listing = self.read_in_slot(LISTING_SLOT, calls, listing).await?;
        let listings = join_all(
            listing
                .topics
                .iter()
                .map(|topic| self.offset_groups_on(&topic.kafka_topic, calls)),
        )
        .await;
        let mut unexamined = listing.unreadable;
        // The index in `groups` and the Kafka topic of each Iggy group to delete.
        let mut deletions = Vec::new();
        for (topic, listed) in listing.topics.iter().zip(listings) {
            let found = match listed {
                Ok(found) => found,
                Err(error) if topic_deleted(&error) => continue,
                Err(BridgeError::Iggy(IggyError::Unauthorized)) => {
                    unexamined.push(topic.kafka_topic.clone());
                    continue;
                }
                Err(error) => return Err(error),
            };
            for (at, group) in groups.iter().enumerate() {
                if found.iter().any(|name| name == group) {
                    deletions.push((at, topic.kafka_topic.as_str()));
                }
            }
        }
        if !unexamined.is_empty() {
            error!(
                kafka_topics = ?unexamined,
                "the bridge user may not list the Iggy groups of these Kafka topics, so DeleteGroups \
                 deletes nothing"
            );
            return Err(BridgeError::Iggy(IggyError::Unauthorized));
        }
        let deleted =
            join_all(deletions.iter().map(|&(at, kafka_topic)| {
                self.delete_offset_group_on(groups[at], kafka_topic, calls)
            }))
            .await;
        let mut results: Vec<Result<bool, BridgeError>> =
            groups.iter().map(|_| Ok(false)).collect();
        for (&(at, _), result) in deletions.iter().zip(deleted) {
            match result {
                Ok(()) if results[at].is_ok() => results[at] = Ok(true),
                Ok(()) => {}
                Err(error) => results[at] = Err(error),
            }
        }
        for (group, result) in groups.iter().zip(&results) {
            if result.is_ok() {
                self.group_index.forget(group);
            }
        }
        Ok(results)
    }

    /// The Iggy topic of `kafka_topic` and its partition count, read on an offset slot. `None`
    /// when the topic does not exist.
    ///
    /// # Errors
    ///
    /// As [`Self::get_kafka_topic`]. [`BridgeError::Timeout`] past the deadline of `calls`.
    pub async fn kafka_topic_partitions(
        &self,
        kafka_topic: &str,
        calls: &OffsetCalls,
    ) -> Result<Option<(TopicTarget, u32)>, BridgeError> {
        let topic = self.topic_target(kafka_topic)?;
        let lookup = |client: Arc<IggyClient>| async move {
            let details = self.get_kafka_topic_on(&client, kafka_topic).await?;
            Ok(details.map(|details| details.partitions_count))
        };
        // Any slot will do. Partition 0's keeps the choice fixed.
        let count = self.read_in_slot(slot_of(&topic, 0), calls, lookup).await?;
        Ok(count.map(|count| (topic, count)))
    }

    /// [`Self::list_kafka_topics`] on an offset slot's read client, so it never waits behind
    /// Produce.
    ///
    /// # Errors
    ///
    /// As [`Self::list_kafka_topics`]. [`BridgeError::Timeout`] past the deadline of `calls`.
    pub async fn list_kafka_topics_in_slot(
        &self,
        calls: &OffsetCalls,
    ) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        let listing =
            |client: Arc<IggyClient>| async move { self.list_kafka_topics_on(&client).await };
        self.read_in_slot(LISTING_SLOT, calls, listing).await
    }

    /// The Kafka groups that hold offsets in Iggy, from a scan that began less than 10 s ago, plus
    /// the groups that this gateway stored offsets for since. A scan lists the Kafka topics, then
    /// the Iggy groups of each topic, so it costs one call per topic. It runs on a client of its
    /// own, one call at a time, so no offset call waits behind it.
    ///
    /// Callers that come while a scan runs wait for it, until `deadline`. The scan keeps running
    /// after that and serves the next caller. A scan that fails, or does not end in time, leaves
    /// the groups of the last scan that found them as the answer. `None` when no scan found them
    /// yet.
    pub async fn offset_groups(self: &Arc<Self>, deadline: Instant) -> Option<Arc<OffsetGroups>> {
        let bridge = Arc::clone(self);
        let scan = move || async move { bridge.scan_offset_groups().await };
        self.group_index.groups(deadline, scan).await
    }

    /// Every Kafka group with offsets on a Kafka topic. A topic deleted since the listing holds
    /// none. A topic whose groups the bridge user may not list is left out, so that one such
    /// topic does not hide the groups of every other.
    async fn scan_offset_groups(&self) -> Result<BTreeSet<String>, BridgeError> {
        let scan = async {
            let client = self.scan_client.connected(&self.config).await?;
            let mut groups = BTreeSet::new();
            for topic in self.list_kafka_topics_on(&client).await? {
                let target = self.topic_target(&topic.kafka_topic)?;
                match kafka_groups_on(&client, &target).await {
                    Ok(found) => groups.extend(found),
                    Err(error)
                        if topic_deleted(&error)
                            || matches!(error, BridgeError::Iggy(IggyError::Unauthorized)) =>
                    {
                        debug!(
                            topic = topic.kafka_topic.as_str(),
                            %error,
                            "skipping a topic in the scan for groups"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(groups)
        };
        self.scan_client
            .until(Instant::now() + GROUP_SCAN_LIMIT, scan)
            .await
    }

    /// The Kafka groups with offsets on `kafka_topic`, listed on an offset slot.
    async fn offset_groups_on(
        &self,
        kafka_topic: &str,
        calls: &OffsetCalls,
    ) -> Result<Vec<String>, BridgeError> {
        let topic = self.topic_target(kafka_topic)?;
        // Any slot will do. Partition 0's spreads the topics over the slots.
        let slot = slot_of(&topic, 0);
        let listing =
            |client: Arc<IggyClient>| async move { kafka_groups_on(&client, &topic).await };
        self.read_in_slot(slot, calls, listing).await
    }

    /// Deletes the Iggy group of Kafka group `group` on `kafka_topic`, then waits until the topic
    /// no longer lists it. A group that is not there counts as deleted.
    async fn delete_offset_group_on(
        &self,
        group: &str,
        kafka_topic: &str,
        calls: &OffsetCalls,
    ) -> Result<(), BridgeError> {
        let group_id = OffsetNames::new(group)
            .map_err(BridgeError::Iggy)?
            .consumer
            .id;
        let topic = self.topic_target(kafka_topic)?;
        let (stream_id, topic_id) = (topic.stream_id.clone(), topic.topic_id.clone());
        let delete = move |client: Arc<IggyClient>| async move {
            match client
                .delete_consumer_group(&stream_id, &topic_id, &group_id)
                .await
            {
                Err(
                    IggyError::ConsumerGroupIdNotFound(..)
                    | IggyError::ConsumerGroupNameNotFound(..),
                ) => Ok(()),
                deleted => landed(deleted),
            }
        };
        // Partition 0's slot, so the delete follows the commits of that partition.
        self.commit_in_slot(slot_of(&topic, 0), calls, delete)
            .await?;
        let mut pause = DELETE_CHECK_FIRST;
        loop {
            match self.holds_offset_group(group, &topic, calls).await {
                Ok(false) => return Ok(()),
                Ok(true) => {}
                Err(error) if topic_deleted(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
            if Instant::now() + pause >= calls.deadline() {
                return Err(BridgeError::Timeout);
            }
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(DELETE_CHECK_MAX);
        }
    }

    /// Runs `call` with the commit client of `slot`, and waits for it until the deadline of
    /// `calls`. The call keeps the slot until it ends, or for at most `SLOT_LIMIT`, even after its
    /// caller stops waiting. A call that has not started by the deadline never starts.
    async fn commit_in_slot<F>(
        &self,
        slot: usize,
        calls: &OffsetCalls,
        call: impl FnOnce(Arc<IggyClient>) -> F,
    ) -> Result<(), BridgeError>
    where
        F: Future<Output = Result<(), BridgeError>> + Send + 'static,
    {
        let lane = &self.offset_pool.slots[slot].commits;
        let Some((_queued, turn)) = calls.take(slot, lane).await else {
            return Err(BridgeError::Timeout);
        };
        let client = match timeout_at(calls.deadline, lane.client.connected(&self.config)).await {
            Ok(connected) => connected?,
            Err(_elapsed) => return Err(BridgeError::Timeout),
        };
        // The turn comes back only to be dropped. A commit that outlives its caller frees it in
        // its task.
        match in_slot(turn, calls.deadline, timeout(SLOT_LIMIT, call(client))).await {
            Some((Ok(ended), _)) => ended,
            // Still running at the deadline, or cut off at `SLOT_LIMIT`: it may still land.
            Some((Err(_), _)) | None => Err(BridgeError::Timeout),
        }
    }

    /// Runs `call` with the read client of `slot`, and frees the slot when it ends or at the
    /// deadline of `calls`. A read that the deadline cuts off leaves its client behind, so the
    /// next read on the slot does not wait behind it in the SDK. A read that has not started by
    /// the deadline never starts.
    async fn read_in_slot<T, F>(
        &self,
        slot: usize,
        calls: &OffsetCalls,
        call: impl FnOnce(Arc<IggyClient>) -> F,
    ) -> Result<T, BridgeError>
    where
        F: Future<Output = Result<T, BridgeError>>,
    {
        let lane = &self.offset_pool.slots[slot].reads;
        let Some((_queued, _turn)) = calls.take(slot, lane).await else {
            return Err(BridgeError::Timeout);
        };
        let read = async { call(lane.client.connected(&self.config).await?).await };
        lane.client.until(calls.deadline, read).await
    }
}

/// The offset slots. A partition always takes the same slot. A slot has a lane for commits and
/// one for reads, each with its own client, so only commits wait on commits. The commits of a
/// partition share one client, which sends one call at a time, so they reach Iggy in order.
pub(super) struct OffsetPool {
    slots: Vec<OffsetSlot>,
}

struct OffsetSlot {
    commits: Lane,
    reads: Lane,
}

/// A client, and one call on it at a time.
struct Lane {
    client: LazyClient,
    turn: Arc<Semaphore>,
}

impl OffsetPool {
    pub(super) fn new() -> Self {
        Self {
            slots: (0..OFFSET_SLOTS)
                .map(|_| OffsetSlot {
                    commits: Lane::new(LazyClient::default()),
                    reads: Lane::new(LazyClient::default()),
                })
                .collect(),
        }
    }

    /// Closes each client that connected. See [`LazyClient::close_all`].
    pub(super) async fn close(&self) -> Result<(), BridgeError> {
        let clients = self
            .slots
            .iter()
            .flat_map(|slot| [&slot.commits.client, &slot.reads.client]);
        LazyClient::close_all(clients).await
    }
}

impl Lane {
    fn new(client: LazyClient) -> Self {
        Self {
            client,
            turn: Arc::new(Semaphore::new(1)),
        }
    }
}

/// The slot of `partition` of `topic`, the same at every call. The partitions of a topic take the
/// slots in turn, so a topic with one partition per slot uses every slot.
fn slot_of(topic: &TopicTarget, partition: u32) -> usize {
    let mut hasher = DefaultHasher::new();
    (&topic.stream_id, &topic.topic_id).hash(&mut hasher);
    let first = usize::from(hasher.finish().to_le_bytes()[0]);
    let step = usize::try_from(partition).map_or(0, |partition| partition % OFFSET_SLOTS);
    (first + step) % OFFSET_SLOTS
}

/// Whether a call on a listed topic failed because the topic was deleted since the listing. Iggy
/// deleted its groups with it.
const fn topic_deleted(error: &BridgeError) -> bool {
    error.to_kafka_error_code() == ERROR_UNKNOWN_TOPIC_OR_PARTITION
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

/// The Kafka groups with offsets on `topic`, listed on `client`.
async fn kafka_groups_on(
    client: &IggyClient,
    topic: &TopicTarget,
) -> Result<Vec<String>, BridgeError> {
    let groups = client
        .get_consumer_groups(&topic.stream_id, &topic.topic_id)
        .await
        .map_err(BridgeError::Iggy)?;
    Ok(groups
        .iter()
        .filter_map(|group| kafka_group_of(&group.name))
        .collect())
}

/// The Kafka group whose offsets the Iggy group `name` holds. `None` for any other Iggy group.
fn kafka_group_of(name: &str) -> Option<String> {
    name.strip_prefix(OFFSET_GROUP_PREFIX)
        .filter(|group| is_valid_group_id(group))
        .map(str::to_owned)
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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use iggy::prelude::{ConsumerKind, IggyClientBuilder};
    use tokio::sync::oneshot;

    use super::*;

    fn target(topic: &str) -> TopicTarget {
        TopicTarget {
            stream_id: Identifier::named("kafka").unwrap(),
            topic_id: Identifier::named(topic).unwrap(),
        }
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_millis(20)
    }

    fn later() -> Instant {
        Instant::now() + Duration::from_secs(5)
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

    fn offline_clients() -> [Arc<IggyClient>; OFFSET_SLOTS] {
        std::array::from_fn(|_| offline_client())
    }

    /// A bridge that never dials. Each offset slot holds the clients at its index.
    fn offline_bridge(
        commit_clients: [Arc<IggyClient>; OFFSET_SLOTS],
        read_clients: [Arc<IggyClient>; OFFSET_SLOTS],
    ) -> IggyBridge {
        IggyBridge {
            offset_pool: OffsetPool {
                slots: commit_clients
                    .into_iter()
                    .zip(read_clients)
                    .map(|(commits, reads)| OffsetSlot {
                        commits: Lane::new(LazyClient::holding(commits)),
                        reads: Lane::new(LazyClient::holding(reads)),
                    })
                    .collect(),
            },
            ..IggyBridge::offline()
        }
    }

    /// The first partition of `topic` whose slot is not `slot`.
    fn partition_off_slot(topic: &TopicTarget, slot: usize) -> u32 {
        (0..64)
            .find(|&partition| slot_of(topic, partition) != slot)
            .expect("the partitions of one topic spread over the slots")
    }

    /// A commit on `slot` that holds its commit lane until `released` fires or drops.
    async fn stuck_commit(bridge: &IggyBridge, slot: usize, released: oneshot::Receiver<()>) {
        let commit = bridge
            .commit_in_slot(slot, &OffsetCalls::new(soon()), |_client| async move {
                let _ = released.await;
                Ok(())
            })
            .await;
        assert!(
            matches!(commit, Err(BridgeError::Timeout)),
            "the caller gives up"
        );
    }

    #[test]
    fn given_an_iggy_group_name_only_an_offset_group_should_name_a_kafka_group() {
        assert_eq!(kafka_group_of("kafka.cg.orders").as_deref(), Some("orders"));
        assert_eq!(kafka_group_of("orders"), None, "a native Iggy group");
        assert_eq!(kafka_group_of("kafka.cg."), None, "an empty Kafka group id");
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

    /// Hashing the partition index too would put two partitions of most such topics on one slot.
    #[test]
    fn given_a_topic_with_one_partition_per_slot_should_give_each_partition_its_own_slot() {
        let partitions = u32::try_from(OFFSET_SLOTS).unwrap();
        for name in ["orders", "payments", "audit", "clicks"] {
            let topic = target(name);
            let mut slots: Vec<usize> = (0..partitions)
                .map(|partition| slot_of(&topic, partition))
                .collect();
            slots.sort_unstable();

            assert_eq!(slots, (0..OFFSET_SLOTS).collect::<Vec<_>>(), "{name}");
            assert_eq!(slot_of(&topic, partitions), slot_of(&topic, 0), "{name}");
        }
    }

    /// A commit given up on can still land, so the next commit on its slot must wait for it, or a
    /// newer offset could land first and lose to the older one.
    #[tokio::test]
    async fn given_a_commit_given_up_on_when_the_next_commit_comes_should_wait_for_it_to_end() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let (release, released) = oneshot::channel::<()>();
        stuck_commit(&bridge, 0, released).await;

        let started = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&started);
        let next = bridge
            .commit_in_slot(0, &OffsetCalls::new(soon()), |_client| async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(matches!(next, Err(BridgeError::Timeout)));
        assert!(
            !started.load(Ordering::SeqCst),
            "the stuck commit still holds the slot"
        );

        release
            .send(())
            .expect("the stuck commit waits for its release");
        let after = bridge
            .commit_in_slot(0, &OffsetCalls::new(later()), |_client| async { Ok(()) })
            .await;
        assert!(after.is_ok(), "the slot frees when the stuck commit ends");
    }

    /// Only commits wait on commits. A read on the slot of a stuck commit goes ahead.
    #[tokio::test]
    async fn given_a_stuck_commit_when_reading_on_its_slot_should_not_wait_for_it() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let (_release, released) = oneshot::channel::<()>();
        stuck_commit(&bridge, 0, released).await;

        let read = bridge
            .read_in_slot(0, &OffsetCalls::new(soon()), |_client| async { Ok(7) })
            .await;

        assert_eq!(read.unwrap(), 7);
    }

    /// A busy slot must cost only its own partitions, wherever they sit in the request.
    #[tokio::test]
    async fn given_a_busy_slot_when_committing_should_still_commit_on_the_free_slots() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let orders = target("orders");
        let busy = slot_of(&orders, 0);
        let free = partition_off_slot(&orders, busy);
        let _held = OffsetCalls::new(soon())
            .take(busy, &bridge.offset_pool.slots[busy].commits)
            .await
            .expect("a free slot");
        let calls = OffsetCalls::new(Instant::now() + Duration::from_millis(200));

        let results = bridge
            .commit_group_offsets("g", &[(&orders, 0, 5), (&orders, free, 7)], &calls)
            .await;

        assert!(matches!(results[0], Err(BridgeError::Timeout)));
        assert!(
            matches!(results[1], Err(BridgeError::Iggy(_))),
            "the commit on the free slot reached its client: {:?}",
            results[1]
        );
    }

    /// A request queues one call per slot, so another request waits behind one of its calls, not
    /// behind all of them.
    #[tokio::test]
    async fn given_a_request_with_many_reads_on_a_slot_should_let_another_request_in_after_one() {
        let bridge = offline_bridge(offline_clients(), offline_clients());
        let busy = OffsetCalls::new(later())
            .take(0, &bridge.offset_pool.slots[0].reads)
            .await
            .expect("a free slot");
        let (large, small) = (OffsetCalls::new(later()), OffsetCalls::new(later()));
        let order = Mutex::new(Vec::new());
        let read = async |calls: &OffsetCalls, name: &'static str| {
            let order = &order;
            bridge
                .read_in_slot(0, calls, move |_client| async move {
                    order.lock().unwrap().push(name);
                    Ok(())
                })
                .await
        };

        let (first, second, third, other, ()) = tokio::join!(
            read(&large, "large 1"),
            read(&large, "large 2"),
            read(&large, "large 3"),
            read(&small, "small"),
            async {
                tokio::task::yield_now().await;
                drop(busy);
            },
        );

        assert!(first.is_ok() && second.is_ok() && third.is_ok() && other.is_ok());
        assert_eq!(
            *order.lock().unwrap(),
            ["large 1", "small", "large 2", "large 3"]
        );
    }

    /// A Java consumer retries a timed out `OffsetFetch`. A read that kept its slot, or its client,
    /// would make the retry wait behind it. The commit client stays, so commits keep their order.
    #[tokio::test]
    async fn given_a_read_cut_off_by_its_deadline_should_free_its_slot_and_drop_only_its_client() {
        let (commit_clients, read_clients) = (offline_clients(), offline_clients());
        let bridge = offline_bridge(commit_clients.clone(), read_clients.clone());

        let read = bridge
            .read_in_slot(1, &OffsetCalls::new(soon()), |_client| {
                std::future::pending::<Result<(), BridgeError>>()
            })
            .await;

        assert!(matches!(read, Err(BridgeError::Timeout)));
        assert!(
            OffsetCalls::new(soon())
                .take(1, &bridge.offset_pool.slots[1].reads)
                .await
                .is_some(),
            "the slot is free"
        );
        assert_eq!(
            Arc::strong_count(&read_clients[1]),
            1,
            "the read client is gone, so the next read connects a new one"
        );
        assert_eq!(
            Arc::strong_count(&commit_clients[1]),
            2,
            "the commit client stays"
        );
    }

    /// A scan may take 30 s, so it runs on a client of its own. On an offset read lane, one slow
    /// scan call would make the `OffsetFetch` of that lane time out behind it.
    #[tokio::test]
    async fn given_every_offset_read_lane_busy_a_scan_should_still_run() {
        let bridge = Arc::new(IggyBridge {
            scan_client: LazyClient::holding(offline_client()),
            ..offline_bridge(offline_clients(), offline_clients())
        });
        let held = OffsetCalls::new(later());
        let mut lanes = Vec::with_capacity(OFFSET_SLOTS);
        for slot in 0..OFFSET_SLOTS {
            let turn = held
                .take(slot, &bridge.offset_pool.slots[slot].reads)
                .await
                .expect("a free lane");
            lanes.push(turn);
        }

        let scanned = timeout(Duration::from_secs(1), bridge.offset_groups(later())).await;

        assert!(
            matches!(scanned, Ok(None)),
            "the scan ended without the offset lanes, and an offline client finds no groups"
        );
        drop(lanes);
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
